#include "include.h"

extern volatile sig_atomic_t sighup;
extern volatile sig_atomic_t sigterm;

static void conf_user(const char *user) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    List *names;
    L("user = %s", user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE ROLE %s WITH LOGIN", user_quote);
    if (!(names = stringToQualifiedNameList(user_quote))) E("!stringToQualifiedNameList");
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) {
        CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->role = (char *)user;
        stmt->options = lappend(stmt->options, makeDefElem("canlogin", (Node *)makeInteger(1), -1));
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

static void conf_data(const char *user, const char *data) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    List *names;
    L("user = %s, data = %s", user, data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    if (!(names = stringToQualifiedNameList(data_quote))) E("!stringToQualifiedNameList");
    SPI_start_transaction_my(buf.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)data;
        stmt->options = lappend(stmt->options, makeDefElem("owner", (Node *)makeString((char *)user), -1));
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

static void conf_tick(const char *user, const char *data, const char *schema, const char *table, const int period) {
    StringInfoData buf;
    int user_len = strlen(user), data_len = strlen(data), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), period_len = sizeof(period);
    BackgroundWorker worker;
    char *p = worker.bgw_extra;
    L("user = %s, data = %s, schema = %s, table = %s, period = %i", user, data, schema ? schema : "(null)", table, period);
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
    appendStringInfoString(&buf, "tick_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s", schema ? schema : "", schema ? " " : "", table);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s %s", user, data, worker.bgw_type);
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len > BGW_EXTRALEN) E("%i > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len);
    p = (char *)memcpy(p, user, user_len) + user_len + 1;
    p = (char *)memcpy(p, data, data_len) + data_len + 1;
    p = (char *)memcpy(p, schema, schema_len) + schema_len + 1;
    p = (char *)memcpy(p, table, table_len) + table_len + 1;
    p = (char *)memcpy(p, &period, period_len) + period_len;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static bool conf_check(Work *work) {
    bool exit = false;
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS (\n"
        "SELECT      COALESCE(COALESCE(usename, \"user\"), data)::TEXT AS user,\n"
        "            usename,\n"
        "            COALESCE(datname, data)::text AS data,\n"
        "            datname,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::int4) AS period\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.config', false)::json) AS s (\"user\" text, data text, schema text, \"table\" text, period int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT DISTINCT s.* FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.usename = \"user\" AND a.datname = data AND application_name = concat_ws(' ', 'pg_task', schema, \"table\") AND pid != pg_backend_pid()\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool user_isnull, data_isnull, schema_isnull, table_isnull, period_isnull, usename_isnull, datname_isnull;
        char *user = TextDatumGetCStringMy(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "user"), &user_isnull));
        char *data = TextDatumGetCStringMy(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "data"), &data_isnull));
        char *schema = TextDatumGetCStringMy(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schema"), &schema_isnull));
        char *table = TextDatumGetCStringMy(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "table"), &table_isnull));
        int period = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        if (user_isnull) E("user_isnull");
        if (data_isnull) E("data_isnull");
        if (table_isnull) E("table_isnull");
        if (period_isnull) E("period_isnull");
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        L("row = %lu, user = %s, data = %s, schema = %s, table = %s, period = %i, usename_isnull = %s, datname_isnull = %s", row, user, data, schema ? schema : "(null)", table, period, usename_isnull ? "true" : "false", datname_isnull ? "true" : "false");
        if (usename_isnull) conf_user(user);
        if (datname_isnull) conf_data(user, data);
        if (!pg_strncasecmp(user, "postgres", sizeof("postgres") - 1) && !pg_strncasecmp(data, "postgres", sizeof("postgres") - 1) && !schema && !pg_strncasecmp(table, "task", sizeof("task") - 1)) {
            work->user = "postgres";
            work->data = "postgres";
            work->schema = NULL;
            work->table = "task";
            work->period = period;
            exit = tick_init_work(work);
        } else {
            work->period = -1;
            conf_tick(user, data, schema, table, period);
        }
        pfree(user);
        pfree(data);
        if (schema) pfree(schema);
        pfree(table);
    }
    SPI_finish_my();
    return exit;
}

static void conf_init(Work *work) {
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    SetConfigOptionMy("application_name", MyBgworkerEntry->bgw_type);
    pqsignal(SIGHUP, sighup_my);
    pqsignal(SIGTERM, sigterm_my);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    work->period = -1;
    queue_init(&work->queue);
}

static void conf_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static bool conf_reload(Work *work) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
    return conf_check(work);
}

void conf_worker(Datum main_arg); void conf_worker(Datum main_arg) {
    TimestampTz stop = GetCurrentTimestamp(), start = stop;
    Work work;
    MemSet(&work, 0, sizeof(work));
    conf_init(&work);
    sigterm = conf_check(&work);
    while (!sigterm) {
        int count = queue_count(&work.queue) + 2;
        WaitEvent *events;
        WaitEventSet *set;
        if (!(events = palloc0(count * sizeof(*events)))) E("!palloc0");
        if (!(set = CreateWaitEventSet(CurrentMemoryContext, count))) E("!CreateWaitEventSet");
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        queue_each(&work.queue, queue) {
            Task *task = queue_data(queue, Task, queue);
            AddWaitEventToSet(set, task->events & WL_SOCKET_MASK, task->fd, NULL, task);
        }
        if (!(count = WaitEventSetWait(set, work.period, events, count, PG_WAIT_EXTENSION))) {
            if (work.period >= 0) tick_timeout(&work);
        } else {
            for (int i = 0; i < count; i++) {
                WaitEvent *event = &events[i];
                if (event->events & WL_LATCH_SET) conf_latch();
                if (sighup) sigterm = conf_reload(&work);
                if (event->events & WL_SOCKET_MASK) tick_socket(event->user_data);
            }
            if (TimestampDifferenceExceeds(start, stop = GetCurrentTimestamp(), work.period) && work.period >= 0) tick_timeout(&work);
        }
        FreeWaitEventSet(set);
        pfree(events);
        start = stop;
    }
}
