#include "include.h"

extern char *default_null;

static Oid conf_data(const char *user, const char *data) {
    const char *data_quote = quote_identifier(data);
    const char *user_quote = quote_identifier(user);
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s, data = %s", user, data);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), data_quote, user_quote);
    names = stringToQualifiedNameList(data_quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)data;
#if PG_VERSION_NUM >= 100000
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)user), -1));
#else
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)user)));
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
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(src.data);
    return oid;
}

static Oid conf_user(const char *user) {
    const char *user_quote = quote_identifier(user);
    List *names;
    Oid oid;
    StringInfoData src;
    D1("user = %s", user);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE ROLE %s WITH LOGIN), user_quote);
    names = stringToQualifiedNameList(user_quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_role_oid(strVal(linitial(names)), true))) {
        CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->role = (char *)user;
#if PG_VERSION_NUM >= 100000
        stmt->options = list_make1(makeDefElem("canlogin", (Node *)makeInteger(1), -1));
#else
        stmt->options = list_make1(makeDefElem("canlogin", (Node *)makeInteger(1)));
#endif
        pstate->p_sourcetext = src.data;
#if PG_VERSION_NUM >= 100000
        oid = CreateRole(pstate, stmt);
#else
        oid = CreateRole(stmt);
#endif
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(src.data);
    return oid;
}

void conf_work(const Conf *conf, const char *data, const char *user) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    pid_t pid;
    size_t len = 0;
    D1("user_oid = %i, data_oid = %i, user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, partman = %s", conf->user, conf->data, user, data, conf->schema, conf->table, conf->timeout, conf->count, conf->live, conf->partman ? conf->partman : default_null);
    MemSet(&worker, 0, sizeof(worker));
    if (strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name)) >= sizeof(worker.bgw_function_name)) E("strlcpy");
    if (strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name)) >= sizeof(worker.bgw_library_name)) E("strlcpy");
    if (snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %i", user, data, conf->schema, conf->table, conf->timeout) >= sizeof(worker.bgw_name) - 1) E("snprintf");
#if PG_VERSION_NUM >= 110000
    if (strlcpy(worker.bgw_type, worker.bgw_name + strlen(user) + 1 + strlen(data) + 1, sizeof(worker.bgw_type)) >= sizeof(worker.bgw_type)) E("strlcpy");
#endif
#define X(type, name, get, serialize, deserialize) serialize(conf->name);
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
        ) SELECT    DISTINCT COALESCE(u.usesysid, 0) AS user_oid, COALESCE(oid, 0) AS data_oid, COALESCE(pid, 0) AS pid, j.* FROM j
        LEFT JOIN   pg_user AS u ON usename = j.user
        LEFT JOIN   pg_database AS d ON datname = data AND NOT datistemplate AND datallowconn AND (usesysid IS NULL OR usesysid = datdba)
        LEFT JOIN   pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text) AND pid != pg_backend_pid()
    );
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Conf conf = {
#define X(type, name, get, serialize, deserialize) .name = get(name),
    CONF
#undef X
        };
        char *data = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        char *user = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        int32 pid = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "pid", false));
        conf.data = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data_oid", false));
        conf.user = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user_oid", false));
        D1("row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, user_oid = %i, data_oid = %i, pid = %i, partman = %s", row, user, data, conf.schema, conf.table, conf.timeout, conf.count, conf.live, conf.user, conf.data, pid, conf.partman ? conf.partman : default_null);
        if (!pid) {
            if (!conf.user) conf.user = conf_user(user);
            if (!conf.data) conf.data = conf_data(user, data);
            conf_work(&conf, data, user);
        }
        pfree(user);
        pfree(data);
        pfree(conf.schema);
        pfree(conf.table);
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
