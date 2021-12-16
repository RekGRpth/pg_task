#include "include.h"

extern char *default_null;

static Oid conf_data(Work *work) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", work->str.user, work->str.data);
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
    elog(DEBUG1, "user = %s", work->str.user);
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

static void conf_work(BackgroundWorker *worker) {
    BackgroundWorkerHandle *handle;
    pid_t pid;
    set_ps_display_my("work");
    if (!RegisterDynamicBackgroundWorker(worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    pfree(handle);
}

static void conf_check(void) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, init_check(), "");
        appendStringInfo(&src, SQL(%1$sLEFT JOIN pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text) WHERE pid IS NULL), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        BackgroundWorker worker = {0};
        size_t len = 0;
        Work *work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
        work->reset =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "reset", false));
        work->str.data = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        work->str.partman = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "partman", true));
        work->str.schema = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", false));
        work->str.table = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false));
        work->str.user = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        work->timeout =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li, partman = %s", row, work->str.user, work->str.data, work->str.schema, work->str.table, work->timeout, work->reset, work->str.partman ? work->str.partman : default_null);
        set_ps_display_my("row");
        work->quote.data = (char *)quote_identifier(work->str.data);
        if (work->str.partman) work->quote.partman = (char *)quote_identifier(work->str.partman);
        work->quote.schema = (char *)quote_identifier(work->str.schema);
        work->quote.table = (char *)quote_identifier(work->str.table);
        work->quote.user = (char *)quote_identifier(work->str.user);
        work->oid.user = conf_user(work);
        work->oid.data = conf_data(work);
        if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
        if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
        if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", work->str.user, work->str.data, work->str.schema, work->str.table, work->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
        if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
        len = 0;
#define X(name, serialize, deserialize) serialize(name);
        WORK
#undef X
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_notify_pid = MyProcPid;
        worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
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
