#include "include.h"

extern char *default_null;

static void conf_data(const char *user, const char *data) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    List *names;
    D1("user = %s, data = %s", user, data);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    names = stringToQualifiedNameList(data_quote);
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)data;
        stmt->options = list_make1(makeDefElem("owner", (Node *)makeString((char *)user), -1));
        pstate->p_sourcetext = buf.data;
        createdb(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void conf_user(const char *user) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    List *names;
    D1("user = %s", user);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "CREATE ROLE %s WITH LOGIN", user_quote);
    names = stringToQualifiedNameList(user_quote);
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) {
        CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->role = (char *)user;
        stmt->options = list_make1(makeDefElem("canlogin", (Node *)makeInteger(1), -1));
        pstate->p_sourcetext = buf.data;
        CreateRole(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(buf.data);
}

void conf_work(const char *user, const char *data, const char *schema, const char *table, const int reset, const int timeout, const int count) {
    BackgroundWorkerHandle *handle;
    pid_t pid;
    StringInfoData buf;
    int user_len = strlen(user), data_len = strlen(data), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), reset_len = sizeof(reset), timeout_len = sizeof(timeout), count_len = sizeof(count);
    BackgroundWorker worker;
    char *p = worker.bgw_extra;
    D1("user = %s, data = %s, schema = %s, table = %s, reset = %i, timeout = %i, count = %i", user, data, schema ? schema : default_null, table, reset, timeout, count);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "work_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s %i %i", schema ? schema : "", schema ? " " : "", table, reset, timeout);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s %s", user, data, worker.bgw_type);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + reset_len + timeout_len + count_len > BGW_EXTRALEN) E("%i > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + reset_len + timeout_len + count_len);
    p = (char *)memcpy(p, user, user_len) + user_len + 1;
    p = (char *)memcpy(p, data, data_len) + data_len + 1;
    p = (char *)memcpy(p, schema, schema_len) + schema_len + 1;
    p = (char *)memcpy(p, table, table_len) + table_len + 1;
    p = (char *)memcpy(p, &reset, reset_len) + reset_len;
    p = (char *)memcpy(p, &timeout, timeout_len) + timeout_len;
    p = (char *)memcpy(p, &count, count_len) + count_len;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) E("!RegisterDynamicBackgroundWorker");
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: E("WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED"); break;
        case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED"); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED"); break;
    }
    pfree(handle);
}

static void conf_check(Work *work) {
    static SPI_plan *plan = NULL;
    static const char *command =
        "WITH s AS (\n"
        "SELECT      COALESCE(COALESCE(usename, \"user\"), data)::TEXT AS user,\n"
        "            COALESCE(datname, data)::text AS data,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.default_table', false)) AS table,\n"
        "            COALESCE(reset, current_setting('pg_task.default_reset', false)::int4) AS reset,\n"
        "            COALESCE(timeout, current_setting('pg_task.default_timeout', false)::int4) AS timeout,\n"
        "            COALESCE(count, current_setting('pg_task.default_count', false)::int4) AS count\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.json', false)::json) AS s (\"user\" text, data text, schema text, \"table\" text, reset int4, timeout int4, count int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT DISTINCT s.*, u.usesysid IS NOT NULL AS user_exists, d.oid IS NOT NULL AS data_exists, pid IS NOT NULL AS active FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.usename = \"user\" AND a.datname = data AND ((application_name = concat_ws(' ', 'pg_task', schema, \"table\", reset::text, timeout::text) AND pid != pg_backend_pid()) OR (application_name = 'pg_task conf' AND pid = pg_backend_pid()))\n"
        "LEFT JOIN   pg_database AS d ON d.datname = data AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON u.usename = \"user\"";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    work->conf = false;
    for (uint64 row = 0; row < SPI_tuptable->numvals; row++) {
        char *user = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        char *data = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        char *schema = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", true));
        char *table = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false));
        int reset = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "reset", false));
        int timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        int count = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "count", false));
        bool user_exists = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user_exists", false));
        bool data_exists = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data_exists", false));
        bool active = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "active", false));
        D1("row = %lu, user = %s, data = %s, schema = %s, table = %s, reset = %i, timeout = %i, count = %i, user_exists = %s, data_exists = %s, active = %s", row, user, data, schema ? schema : default_null, table, reset, timeout, count, user_exists ? "true" : "false", data_exists ? "true" : "false", active ? "true" : "false");
        if (!strcmp(user, "postgres") && !strcmp(data, "postgres") && !schema && !strcmp(table, "task")) {
            work->conf = true;
            work->count = count;
            work->data = "postgres";
            work->reset = reset;
            work->schema = NULL;
            work->table = "task";
            work->timeout = timeout;
            work->user = "postgres";
            if (!work->oid) work_conf(work);
        } else if (!active) {
            if (!user_exists) conf_user(user);
            if (!data_exists) conf_data(user, data);
            conf_work(user, data, schema, table, reset, timeout, count);
        }
        pfree(user);
        pfree(data);
        if (schema) pfree(schema);
        pfree(table);
    }
    SPI_finish_my();
    if (!work->conf) work->timeout = -1;
}

static void conf_run(void) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    pid_t pid;
    MemSet(&worker, 0, sizeof(worker));
    init_work(&worker);
    worker.bgw_notify_pid = MyProcPid;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) E("!RegisterDynamicBackgroundWorker");
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: E("WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED"); break;
        case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED"); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED"); break;
    }
    pfree(handle);
}

static void conf_exit(int code, Datum arg) {
    Work *work = (Work *)DatumGetPointer(arg);
    D1("code = %i, oid = %i", code, work->oid);
    if (code || ShutdownRequestPending) return;
    if (work->conf) conf_run();
}

static void conf_init(Work *work) {
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    on_proc_exit(conf_exit, PointerGetDatum(work));
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    work->timeout = -1;
    LIST_INIT(&work->tasks);
    conf_check(work);
}

static void conf_reload(Work *work) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    conf_check(work);
}

static void conf_latch(Work *work) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) conf_reload(work);
}

void conf_worker(Datum main_arg) {
    instr_time cur_time;
    instr_time start_time;
    long cur_timeout = -1;
    Work *work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
    conf_init(work);
    while (!ShutdownRequestPending) {
        Task *task, *_;
        int nevents = 2;
        WaitEvent *events;
        WaitEventSet *set;
        if (cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = work->timeout;
        }
        LIST_FOREACH_SAFE(task, &work->tasks, item, _) {
            if (PQstatus(task->conn) == CONNECTION_BAD) { work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessage(task->conn), true); continue; }
            if (PQsocket(task->conn) < 0) { work_error(task, "PQsocket < 0", PQerrorMessage(task->conn), true); continue; }
            nevents++;
        }
        events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
        LIST_FOREACH_SAFE(task, &work->tasks, item, _) {
            if (task->events & WL_SOCKET_WRITEABLE) switch (PQflush(task->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, task->events & WL_SOCKET_MASK, PQsocket(task->conn), NULL, task);
        }
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) conf_latch(work);
            if (event->events & WL_SOCKET_MASK) work_socket(event->user_data);
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
        }
        if (work->timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = work->timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout <= 0) work_timeout(work);
        }
        FreeWaitEventSet(set);
        pfree(events);
        if (work->count && work->_count >= work->count) break;
    }
    work_fini(work);
    pfree(work);
}
