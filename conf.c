#include "include.h"

extern char *default_null;

static void conf_user(const char *user) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    List *names;
    D1("user = %s", user);
    initStringInfo(&buf);
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
    } else D1("user %s already exists", user_quote);
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(buf.data);
}

static void conf_data(const char *user, const char *data) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    List *names;
    D1("user = %s, data = %s", user, data);
    initStringInfo(&buf);
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
    } else D1("database %s already exists", data_quote);
    SPI_commit_my();
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void conf_work(const char *user, const char *data, const char *schema, const char *table, const int reset, const int timeout) {
    BackgroundWorkerHandle *handle;
    pid_t pid;
    StringInfoData buf;
    int user_len = strlen(user), data_len = strlen(data), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), reset_len = sizeof(reset), timeout_len = sizeof(timeout);
    BackgroundWorker worker;
    char *p = worker.bgw_extra;
    D1("user = %s, data = %s, schema = %s, table = %s, reset = %i, timeout = %i", user, data, schema ? schema : default_null, table, reset, timeout);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
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
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + reset_len + timeout_len > BGW_EXTRALEN) E("%i > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + reset_len + timeout_len);
    p = (char *)memcpy(p, user, user_len) + user_len + 1;
    p = (char *)memcpy(p, data, data_len) + data_len + 1;
    p = (char *)memcpy(p, schema, schema_len) + schema_len + 1;
    p = (char *)memcpy(p, table, table_len) + table_len + 1;
    p = (char *)memcpy(p, &reset, reset_len) + reset_len;
    p = (char *)memcpy(p, &timeout, timeout_len) + timeout_len;
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
        "            COALESCE(timeout, current_setting('pg_task.default_timeout', false)::int4) AS timeout\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.json', false)::json) AS s (\"user\" text, data text, schema text, \"table\" text, reset int4, timeout int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT DISTINCT s.* FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.usename = \"user\" AND a.datname = data AND application_name = concat_ws(' ', 'pg_task', schema, \"table\", reset::text, timeout::text) AND pid != pg_backend_pid()\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        char *user = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        char *data = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        char *schema = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", true));
        char *table = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false));
        int reset = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "reset", false));
        int timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        MemoryContextSwitchTo(oldMemoryContext);
        D1("row = %lu, user = %s, data = %s, schema = %s, table = %s, reset = %i, timeout = %i", row, user, data, schema ? schema : default_null, table, reset, timeout);
        if (!strcmp(user, "postgres") && !strcmp(data, "postgres") && !schema && !strcmp(table, "task")) {
            work->user = "postgres";
            work->data = "postgres";
            work->schema = NULL;
            work->table = "task";
            work->reset = reset;
            work->timeout = timeout;
            if (work_init(work)) work->timeout = -1;
        } else {
            conf_user(user);
            conf_data(user, data);
            work->timeout = -1;
            conf_work(user, data, schema, table, reset, timeout);
        }
        pfree(user);
        pfree(data);
        if (schema) pfree(schema);
        pfree(table);
    }
    SPI_finish_my();
}

static void conf_init(Work *work) {
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    work->timeout = -1;
    queue_init(&work->queue);
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
    TimestampTz stop = GetCurrentTimestamp(), start = stop;
    Work work;
    MemSet(&work, 0, sizeof(work));
    conf_init(&work);
    while (!ShutdownRequestPending) {
        WaitEvent *events;
        WaitEventSet *set;
        int nevents = 2;
        queue_each(&work.queue, queue) {
            Task *task = queue_data(queue, Task, queue);
            if (PQsocket(task->conn) < 0) { work_error(task, "PQsocket < 0"); continue; }
            nevents++;
        }
        events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&work.queue, queue) {
            Task *task = queue_data(queue, Task, queue);
            int fd = PQsocket(task->conn);
            if (fd < 0) continue;
            if (task->events & WL_SOCKET_WRITEABLE) switch (PQflush(task->conn)) {
                case 0: /*D1("PQflush = 0");*/ break;
                case 1: D1("PQflush = 1"); break;
                default: D1("PQflush = default"); break;
            }
            AddWaitEventToSet(set, task->events & WL_SOCKET_MASK, fd, NULL, task);
        }
        nevents = WaitEventSetWait(set, work.timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) conf_latch(&work);
            if (event->events & WL_SOCKET_MASK) work_socket(event->user_data);
        }
        stop = GetCurrentTimestamp();
        if (work.timeout > 0 && (TimestampDifferenceExceeds(start, stop, work.timeout) || !nevents)) {
            work_timeout(&work);
            start = stop;
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
}
