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
    SetConfigOption("application_name", buf.data, PGC_USERSET, PGC_S_OVERRIDE);
    pgstat_report_appname(buf.data);
    pfree(buf.data);
    renamed = true;
}

static void conf_user(const char *user) {
    int rc;
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    ereport(LOG, (errhidestmt(true), errhidecontext(true), errmsg("%s(%s:%d): user = %s", __func__, __FILE__, __LINE__, user)));
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE USER %s", user_quote);
    SPI_start_my(buf.data);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(buf.data);
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
    ereport(LOG, (errhidestmt(true), errhidecontext(true), errmsg("%s(%s:%d): user = %s, data = %s", __func__, __FILE__, __LINE__, user, data)));
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("owner", (Node *)makeString((char *)user), -1));
    stmt->dbname = (char *)data;
    stmt->options = options;
    SPI_start_my(buf.data);
    createdb(pstate, stmt);
    SPI_commit_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void tick_worker(const char *data, const char *user, const char *schema, const char *table, int period) {
    StringInfoData buf;
    int data_len = strlen(data), user_len = strlen(user), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), period_len = sizeof(period);
    BackgroundWorker worker;
    ereport(LOG, (errhidestmt(true), errhidecontext(true), errmsg("%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period)));
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "tick_worker");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s", schema ? schema : "", schema ? " " : "", table);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %d", user, data, schema ? schema : "", schema ? " " : "", table, period);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + period_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + period_len)));
    memcpy(worker.bgw_extra, data, data_len);
    memcpy(worker.bgw_extra + data_len + 1, user, user_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1, schema, schema_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1, table, table_len);
    *(typeof(period) *)(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1) = period;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static void conf_unlock(void) {
    int rc;
    static const char *command = "SELECT pg_advisory_unlock(current_setting('pg_task.lock', true)::int8) AS unlock";
    SPI_start_my(command);
    if ((rc = SPI_execute(command, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(command);
}

static void conf_check(void) {
    int rc;
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS (\n"
        "SELECT      COALESCE(datname, data)::text AS data,\n"
        "            datname,\n"
        "            COALESCE(COALESCE(usename, \"user\"), data)::TEXT AS user,\n"
        "            usename,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::int4) AS period\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.config', false)::json) AS s (data text, \"user\" text, schema text, \"table\" text, period int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT s.* FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.datname = data AND a.usename = \"user\" AND application_name = concat_ws(' ', 'pg_task', schema, \"table\", period::text) AND pid != pg_backend_pid()\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    events &= ~WL_TIMEOUT;
    SPI_start_my(command);
    if (!plan) {
        if (!(plan = SPI_prepare(command, 0, NULL))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, NULL, NULL, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool period_isnull, datname_isnull, usename_isnull;
        const char *data = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "data"));
        const char *user = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "user"));
        const char *schema = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schema"));
        const char *table = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "table"));
        const int period = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        ereport(LOG, (errhidestmt(true), errhidecontext(true), errmsg("%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %d, datname_isnull = %s, usename_isnull = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period, datname_isnull ? "true" : "false", usename_isnull ? "true" : "false")));
        if (period_isnull) ereport(ERROR, (errmsg("%s(%s:%d): period_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) conf_user(user);
        if (datname_isnull) conf_data(user, data);
        if (!pg_strncasecmp(data, "postgres", sizeof("postgres") - 1) && !pg_strncasecmp(user, "postgres", sizeof("postgres") - 1) && !schema && !pg_strcasecmp(table, pg_task_task)) {
            timeout = period;
            events |= WL_TIMEOUT;
        } else tick_worker(data, user, schema, table, period);
        pfree((void *)data);
        pfree((void *)user);
        if (schema) pfree((void *)schema);
        pfree((void *)table);
    }
    SPI_commit_my(command);
    if (events & WL_TIMEOUT) {
        update_ps_display(true);
        conf_unlock();
        tick_init(true, "postgres", "postgres", NULL, pg_task_task, timeout);
    } else if (renamed) {
        timeout = LONG_MAX;
        update_ps_display(false);
    }
}

static void conf_init(void) {
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) ereport(ERROR, (errmsg("%s(%s:%d): !calloc", __func__, __FILE__, __LINE__)));
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    SetConfigOption("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_OVERRIDE);
    pqsignal(SIGHUP, conf_sighup);
    pqsignal(SIGTERM, conf_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(MyProcPort->database_name, MyProcPort->user_name, 0);
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
