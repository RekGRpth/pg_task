#include "include.h"

extern char *default_null;

static Oid conf_data(Conf *conf) {
    const char *data_quote = quote_identifier(conf->str.data);
    const char *user_quote = quote_identifier(conf->str.user);
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s, data = %s", conf->str.user, conf->str.data);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), data_quote, user_quote);
    names = stringToQualifiedNameList(data_quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)conf->str.data;
#if PG_VERSION_NUM >= 100000
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)conf->str.user), -1));
#else
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)conf->str.user)));
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
    if (user_quote != conf->str.user) pfree((void *)user_quote);
    if (data_quote != conf->str.data) pfree((void *)data_quote);
    pfree(src.data);
    return oid;
}

static Oid conf_user(Conf *conf) {
    const char *user_quote = quote_identifier(conf->str.user);
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s", conf->str.user);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE USER %s), user_quote);
    if (conf->str.partman) appendStringInfoString(&src, " SUPERUSER");
    names = stringToQualifiedNameList(user_quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    oid = get_role_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != conf->str.user) pfree((void *)user_quote);
    pfree(src.data);
    return oid;
}

void conf_work(Conf *conf) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    pid_t pid;
    size_t len = 0;
    D1("user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, partman = %s", conf->str.user, conf->str.data, conf->str.schema, conf->str.table, conf->timeout, conf->count, conf->live, conf->str.partman ? conf->str.partman : default_null);
    conf->oid.user = conf_user(conf);
    conf->oid.data = conf_data(conf);
    MemSet(&worker, 0, sizeof(worker));
    if (strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name)) >= sizeof(worker.bgw_function_name)) E("strlcpy");
    if (strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name)) >= sizeof(worker.bgw_library_name)) E("strlcpy");
    if (snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %i", conf->str.user, conf->str.data, conf->str.schema, conf->str.table, conf->timeout) >= sizeof(worker.bgw_name) - 1) E("snprintf");
#if PG_VERSION_NUM >= 110000
    if (strlcpy(worker.bgw_type, worker.bgw_name + strlen(conf->str.user) + 1 + strlen(conf->str.data) + 1, sizeof(worker.bgw_type)) >= sizeof(worker.bgw_type)) E("strlcpy");
#endif
#define X(name, serialize, deserialize) serialize(conf->name);
    CONF
#undef X
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (init_check_ascii_all(&worker)) E("init_check_ascii_all");
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) E("!RegisterDynamicBackgroundWorker");
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: E("WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED"); break;
        case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED"); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED"); break;
    }
    pfree(handle);
}

static void conf_check(void) {
    static SPI_plan *plan = NULL;
    static const char *command = SQL(
        WITH j AS (
            SELECT  COALESCE(COALESCE(j.user, data), current_setting('pg_task.default_user', false)) AS user,
                    COALESCE(COALESCE(data, j.user), current_setting('pg_task.default_data', false)) AS data,
                    COALESCE(schema, current_setting('pg_task.default_schema', false)) AS schema,
                    COALESCE(j.table, current_setting('pg_task.default_table', false)) AS table,
                    COALESCE(timeout, current_setting('pg_task.default_timeout', false)::int4) AS timeout,
                    COALESCE(count, current_setting('pg_task.default_count', false)::int4) AS count,
                    EXTRACT(epoch FROM COALESCE(live, current_setting('pg_task.default_live', false)::interval))::int8 AS live,
                    COALESCE(partman, current_setting('pg_task.default_partman', false)) AS partman
            FROM    json_populate_recordset(NULL::record, current_setting('pg_task.json', false)::json) AS j ("user" text, data text, schema text, "table" text, timeout int4, count int4, live interval, partman text)
        ) SELECT    COALESCE(pid, 0) AS pid, j.* FROM j
        LEFT JOIN   pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text) AND pid != pg_backend_pid()
    );
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Conf conf = {
            .count =  DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "count", false)),
            .live =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "live", false)),
            .str.data = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false)),
            .str.partman = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "partman", true)),
            .str.schema = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", false)),
            .str.table = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false)),
            .str.user = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false)),
            .timeout =  DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false))
        };
        int32 pid = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "pid", false));
        D1("row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, pid = %i, partman = %s", row, conf.str.user, conf.str.data, conf.str.schema, conf.str.table, conf.timeout, conf.count, conf.live, pid, conf.str.partman ? conf.str.partman : default_null);
        if (!pid) conf_work(&conf);
        pfree(conf.str.data);
        if (conf.str.partman) pfree(conf.str.partman);
        pfree(conf.str.schema);
        pfree(conf.str.table);
        pfree(conf.str.user);
    }
    SPI_finish_my();
}

void conf_main(Datum main_arg) {
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM >= 110000
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
#else
    BackgroundWorkerInitializeConnection("postgres", "postgres");
#endif
    pgstat_report_appname("pg_conf");
    process_session_preload_libraries();
    conf_check();
}
