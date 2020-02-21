#include "include.h"

extern char *pg_task_task;
static volatile sig_atomic_t sighup = false;
static volatile sig_atomic_t sigterm = false;

static void conf_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void conf_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void conf_user(const char *user) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    List *names;
    L("user = %s", user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE ROLE %s WITH LOGIN", user_quote);
    names = stringToQualifiedNameList(user_quote);
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
    SPI_commit_my(buf.data);
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
    names = stringToQualifiedNameList(data_quote);
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
    SPI_commit_my(buf.data);
    list_free_deep(names);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void tick_work(Conf *conf) {
    StringInfoData buf;
    int data_len = strlen(conf->data), user_len = strlen(conf->user), schema_len = conf->schema ? strlen(conf->schema) : 0, table_len = strlen(conf->table), period_len = sizeof(conf->period);
    BackgroundWorker worker;
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, conf->period);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "tick_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s", conf->schema ? conf->schema : "", conf->schema ? " " : "", conf->table);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s %s", conf->user, conf->data, worker.bgw_type);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len > BGW_EXTRALEN) E("%u > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len);
    conf->p = worker.bgw_extra;
    memcpy(conf->p, conf->user, user_len);
    conf->p += user_len + 1;
    memcpy(conf->p, conf->data, data_len);
    conf->p += data_len + 1;
    memcpy(conf->p, conf->schema, schema_len);
    conf->p += schema_len + 1;
    memcpy(conf->p, conf->table, table_len);
    conf->p += table_len + 1;
    *(typeof(conf->period) *)conf->p = conf->period;
    conf->p += period_len;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static bool conf_check(Event *event) {
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
        "LEFT JOIN   pg_stat_activity AS a ON a.usename = \"user\" AND a.datname = data AND application_name = concat_ws(' ', 'pg_task', schema, \"table\", period::text) AND pid != pg_backend_pid()\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    event->events &= ~WL_TIMEOUT;
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT);
    SPI_commit_my(command);
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool period_isnull, usename_isnull, datname_isnull;
        Conf conf = {
            .user = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "user")),
            .data = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "data")),
            .schema = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schema")),
            .table = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "table")),
            .period = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull))
        };
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        L("row = %lu, user = %s, data = %s, schema = %s, table = %s, period = %d, usename_isnull = %s, datname_isnull = %s", row, conf.user, conf.data, conf.schema ? conf.schema : "(null)", conf.table, conf.period, usename_isnull ? "true" : "false", datname_isnull ? "true" : "false");
        if (period_isnull) E("period_isnull");
        if (usename_isnull) conf_user(conf.user);
        if (datname_isnull) conf_data(conf.user, conf.data);
        if (!pg_strncasecmp(conf.user, "postgres", sizeof("postgres") - 1) && !pg_strncasecmp(conf.data, "postgres", sizeof("postgres") - 1) && !conf.schema && !pg_strcasecmp(conf.table, pg_task_task)) {
            event->timeout = conf.period;
            event->events |= WL_TIMEOUT;
        } else tick_work(&conf);
        pfree(conf.user);
        pfree(conf.data);
        if (conf.schema) pfree(conf.schema);
        pfree(conf.table);
    }
    SPI_finish_my(command);
    if (event->events & WL_TIMEOUT) {
        Work *work = event->work;
        Conf *conf;
        if (!work && !(work = palloc0(sizeof(work)))) E("!palloc0");
        conf = &work->conf;
        conf->user = "postgres";
        conf->data = "postgres";
        conf->schema = NULL;
        conf->table = pg_task_task;
        conf->period = event->timeout;
        exit = tick_init_work(true, work);
    } else {
        Work *work = event->work;
        event->timeout = -1L;
        if (work) pfree(work);
        event->work = NULL;
    }
    return exit;
}

static void conf_init(void) {
    if (!MyProcPort && !(MyProcPort = (Port *)calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    SetConfigOptionMy("application_name", MyBgworkerEntry->bgw_type);
    pqsignal(SIGHUP, conf_sighup);
    pqsignal(SIGTERM, conf_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
}

static void conf_reset(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static bool conf_reload(Event *event) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
    return conf_check(event);
}

void conf_worker(Datum main_arg); void conf_worker(Datum main_arg) {
    Event event = {.events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, .timeout = -1L, .work = NULL};
    conf_init();
    sigterm = conf_check(&event);
    while (!sigterm) {
        int rc = WaitLatch(MyLatch, event.events, event.timeout, PG_WAIT_EXTENSION);
        if (rc & WL_LATCH_SET) conf_reset();
        if (sighup) sigterm = conf_reload(&event);
        if (rc & WL_TIMEOUT && event.work) tick_loop(event.work);
    }
}
