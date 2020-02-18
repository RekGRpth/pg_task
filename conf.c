#include "include.h"

extern char *pg_task_task;
static bool renamed = false;
static int events = WL_LATCH_SET | WL_POSTMASTER_DEATH;
static long timeout = LONG_MAX;
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

static void update_ps_display(bool conf) {
    StringInfoData buf;
    initStringInfo(&buf);
    if (!conf) appendStringInfoString(&buf, "postgres postgres pg_task conf");
    else appendStringInfo(&buf, "postgres postgres pg_task %s %ld", pg_task_task, timeout);
    init_ps_display(buf.data, "", "", "");
    resetStringInfo(&buf);
    if (!conf) appendStringInfoString(&buf, "pg_task conf");
    else appendStringInfo(&buf, "pg_task %s %ld", pg_task_task, timeout);
    SetConfigOptionMy("application_name", buf.data);
    pgstat_report_appname(buf.data);
    pfree(buf.data);
    renamed = true;
}

static void conf_user(const char *user) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    ParseState *pstate = make_parsestate(NULL);
    List *options = NIL;
    CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
    L("user = %s", user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE USER %s WITH LOGIN", user_quote);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("canlogin", (Node *)makeInteger(1), -1));
    stmt->role = (char *)user;
    stmt->options = options;
    SPI_begin_my(buf.data);
    CreateRole(pstate, stmt);
    SPI_commit_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(buf.data);
}

static void conf_data(const char *user, const char *data) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    ParseState *pstate = make_parsestate(NULL);
    List *options = NIL;
    CreatedbStmt *stmt = makeNode(CreatedbStmt);
    L("user = %s, data = %s", user, data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("owner", (Node *)makeString((char *)user), -1));
    stmt->dbname = (char *)data;
    stmt->options = options;
    SPI_begin_my(buf.data);
    createdb(pstate, stmt);
    SPI_commit_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void tick_worker(const char *user, const char *data, const char *schema, const char *table, const int period) {
    StringInfoData buf;
    int data_len = strlen(data), user_len = strlen(user), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), period_len = sizeof(period);
    BackgroundWorker worker;
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", user, data, schema ? schema : "(null)", table, period);
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
    appendStringInfo(&buf, "pg_task %s%s%s", schema ? schema : "", schema ? " " : "", table);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %d", user, data, schema ? schema : "", schema ? " " : "", table, period);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len > BGW_EXTRALEN) E("%u > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + period_len);
    memcpy(worker.bgw_extra, user, user_len);
    memcpy(worker.bgw_extra + user_len + 1, data, data_len);
    memcpy(worker.bgw_extra + user_len + 1 + data_len + 1, schema, schema_len);
    memcpy(worker.bgw_extra + user_len + 1 +  data_len + 1 + schema_len + 1, table, table_len);
    *(typeof(period + 0) *)(worker.bgw_extra + user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1) = period;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static void conf_check(void) {
    uint64 SPI_processed_my;
    typedef struct Tick {
        char *user;
        char *data;
        char *schema;
        char *table;
        int period;
        bool usename_isnull;
        bool datname_isnull;
    } Tick;
    Tick *tick;
    MemoryContext oldMemoryContext;
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
    events &= ~WL_TIMEOUT;
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT);
    oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    SPI_processed_my = SPI_processed;
    if (!(tick = palloc(SPI_processed_my * sizeof(tick)))) E("!palloc");
    for (uint64 row = 0; row < SPI_processed_my; row++) {
        bool period_isnull;
        tick[row].user = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "user"));
        tick[row].data = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "data"));
        tick[row].schema = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schema"));
        tick[row].table = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "table"));
        tick[row].period = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &tick[row].usename_isnull);
        SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &tick[row].datname_isnull);
        L("data = %s, user = %s, schema = %s, table = %s, period = %d, usename_isnull = %s, datname_isnull = %s", tick[row].data, tick[row].user, tick[row].schema ? tick[row].schema : "(null)", tick[row].table, tick[row].period, tick[row].usename_isnull ? "true" : "false", tick[row].datname_isnull ? "true" : "false");
        if (period_isnull) E("period_isnull");
    }
    MemoryContextSwitchTo(oldMemoryContext);
    SPI_commit_my(command);
    for (uint64 row = 0; row < SPI_processed_my; row++) {
        L("data = %s, user = %s, schema = %s, table = %s, period = %d, usename_isnull = %s, datname_isnull = %s", tick[row].data, tick[row].user, tick[row].schema ? tick[row].schema : "(null)", tick[row].table, tick[row].period, tick[row].usename_isnull ? "true" : "false", tick[row].datname_isnull ? "true" : "false");
        if (tick[row].usename_isnull) conf_user(tick[row].user);
        if (tick[row].datname_isnull) conf_data(tick[row].user, tick[row].data);
        if (!pg_strncasecmp(tick[row].data, "postgres", sizeof("postgres") - 1) && !pg_strncasecmp(tick[row].user, "postgres", sizeof("postgres") - 1) && !tick[row].schema && !pg_strcasecmp(tick[row].table, pg_task_task)) {
            timeout = tick[row].period;
            events |= WL_TIMEOUT;
        } else tick_worker(tick[row].user, tick[row].data, tick[row].schema, tick[row].table, tick[row].period);
        pfree(tick[row].user);
        pfree(tick[row].data);
        if (tick[row].schema) pfree(tick[row].schema);
        pfree(tick[row].table);
    }
    pfree(tick);
    if (events & WL_TIMEOUT) {
        update_ps_display(true);
        tick_init(true, "postgres", "postgres", NULL, pg_task_task, timeout);
    } else if (renamed) {
        timeout = LONG_MAX;
        update_ps_display(false);
    }
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
    conf_check();
}

static void conf_reset(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static void conf_reload(void) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
    conf_check();
}

void conf_worker(Datum main_arg); void conf_worker(Datum main_arg) {
    conf_init();
    while (!sigterm) {
        int rc = WaitLatch(MyLatch, events, timeout, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) break;
        if (rc & WL_LATCH_SET) conf_reset();
        if (sighup) conf_reload();
        if (rc & WL_TIMEOUT) tick_loop();
    }
}
