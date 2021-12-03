#include "include.h"

extern char *default_null;

static Oid conf_data(Work *work) {
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s, data = %s", work->str.user, work->str.data);
    set_ps_display_my("data");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), work->quote.data, work->quote.user);
    names = stringToQualifiedNameList(work->quote.data);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)work->str.data;
#if PG_VERSION_NUM >= 100000
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)work->str.user), -1));
#else
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)work->str.user)));
#endif
        pstate->p_sourcetext = src.data;
#if PG_VERSION_NUM >= 100000
        oid = createdb(pstate, stmt);
#else
        oid = createdb(stmt);
#endif
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

static Oid conf_user(Work *work) {
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s", work->str.user);
    set_ps_display_my("user");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE USER %s), work->quote.user);
    if (work->str.partman) appendStringInfoString(&src, " SUPERUSER");
    names = stringToQualifiedNameList(work->quote.user);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    oid = get_role_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

void conf_work(BackgroundWorker *worker) {
    BackgroundWorkerHandle *handle;
    pid_t pid;
    set_ps_display_my("work");
    if (!RegisterDynamicBackgroundWorker(worker, &handle)) E("!RegisterDynamicBackgroundWorker");
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: E("WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED"); break;
        case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED"); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED"); break;
    }
    pfree(handle);
}

static void conf_check(void) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH j AS (
                SELECT  COALESCE(COALESCE(j.user, data), current_setting('pg_work.default_user', false)) AS user,
                        COALESCE(COALESCE(data, j.user), current_setting('pg_work.default_data', false)) AS data,
                        COALESCE(schema, current_setting('pg_work.default_schema', false)) AS schema,
                        COALESCE(j.table, current_setting('pg_work.default_table', false)) AS table,
                        COALESCE(timeout, current_setting('pg_work.default_timeout', false)::integer) AS timeout,
                        COALESCE(count, current_setting('pg_work.default_count', false)::integer) AS count,
                        EXTRACT(epoch FROM COALESCE(live, current_setting('pg_work.default_live', false)::interval))::bigint AS live,
                        NULLIF(COALESCE(partman, current_setting('pg_work.default_partman', true)), '%1$s') AS partman
                FROM    json_populate_recordset(NULL::record, current_setting('pg_task.json', false)::json) AS j ("user" text, data text, schema text, "table" text, timeout integer, count integer, live interval, partman text)
            ) SELECT    DISTINCT j.* FROM j
        ), "");
        appendStringInfo(&src, SQL(%1$s
            LEFT JOIN   pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text)
            WHERE       pid IS NULL
        ), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        BackgroundWorker worker = {0};
        size_t len = 0;
        Work *work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
        work->count =  DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "count", false));
        work->live =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "live", false));
        work->str.data = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        work->str.partman = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "partman", true));
        work->str.schema = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", false));
        work->str.table = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false));
        work->str.user = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        work->timeout =  DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        D1("row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, partman = %s", row, work->str.user, work->str.data, work->str.schema, work->str.table, work->timeout, work->count, work->live, work->str.partman ? work->str.partman : default_null);
        set_ps_display_my("row");
        work->quote.data = (char *)quote_identifier(work->str.data);
        if (work->str.partman) work->quote.partman = (char *)quote_identifier(work->str.partman);
        work->quote.schema = (char *)quote_identifier(work->str.schema);
        work->quote.table = (char *)quote_identifier(work->str.table);
        work->quote.user = (char *)quote_identifier(work->str.user);
        work->oid.user = conf_user(work);
        work->oid.data = conf_data(work);
        if (strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name)) >= sizeof(worker.bgw_function_name)) E("strlcpy");
        if (strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name)) >= sizeof(worker.bgw_library_name)) E("strlcpy");
        if (snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %i", work->str.user, work->str.data, work->str.schema, work->str.table, work->timeout) >= sizeof(worker.bgw_name) - 1) E("snprintf");
#if PG_VERSION_NUM >= 110000
        if (strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type)) >= sizeof(worker.bgw_type)) E("strlcpy");
#endif
#define X(name, serialize, deserialize) serialize(name);
        WORK
#undef X
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_notify_pid = MyProcPid;
        worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        if (init_check_ascii_all(&worker)) E("init_check_ascii_all");
        conf_work(&worker);
        if (work->quote.data != work->str.data) pfree(work->quote.data);
        if (work->str.partman && work->quote.partman != work->str.partman) pfree(work->quote.partman);
        if (work->quote.schema != work->str.schema) pfree(work->quote.schema);
        if (work->quote.table != work->str.table) pfree(work->quote.table);
        if (work->quote.user != work->str.user) pfree(work->quote.user);
        pfree(work->str.data);
        if (work->str.partman) pfree(work->str.partman);
        pfree(work->str.schema);
        pfree(work->str.table);
        pfree(work->str.user);
        pfree(work);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
}

void conf_main(Datum main_arg) {
    set_config_option("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM >= 110000
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
#else
    BackgroundWorkerInitializeConnection("postgres", "postgres");
#endif
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    conf_check();
}
