#include "include.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void create_user(const char *user) {
    int rc;
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    elog(LOG, "%s(%s:%d): user = %s", __func__, __FILE__, __LINE__, user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE USER %s", user_quote);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    if (user_quote != user) pfree((void *)user_quote);
    pfree(buf.data);
}

static void create_data(const char *user, const char *data) {
    StringInfoData buf;
    const char *user_quote = quote_identifier(user);
    const char *data_quote = quote_identifier(data);
    ParseState *pstate = make_parsestate(NULL);
    List *options = NIL;
    CreatedbStmt *stmt = makeNode(CreatedbStmt);
    elog(LOG, "%s(%s:%d): user = %s, data = %s", __func__, __FILE__, __LINE__, user, data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", data_quote, user_quote);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("owner", (Node *)makeString((char *)user), -1));
    stmt->dbname = (char *)data;
    stmt->options = options;
    SPI_connect_my(buf.data, StatementTimeout);
    createdb(pstate, stmt);
    SPI_commit();
    SPI_finish_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (user_quote != user) pfree((void *)user_quote);
    if (data_quote != data) pfree((void *)data_quote);
    pfree(buf.data);
}

static void register_tick_worker(const char *data, const char *user, const char *schema, const char *table, uint32 period) {
    StringInfoData buf;
    uint32 data_len = strlen(data), user_len = strlen(user), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), period_len = sizeof(period);
    BackgroundWorker worker;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %u", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period);
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
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %u", user, data, schema ? schema : "", schema ? " " : "", table, period);
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

static void check(void) {
    int rc;
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS (\n"
        "SELECT      COALESCE(datname, data)::TEXT AS data,\n"
        "            datname,\n"
        "            COALESCE(COALESCE(usename, \"user\"), data)::TEXT AS user,\n"
        "            usename,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::INT) AS period\n"
        "FROM        json_populate_recordset(NULL::RECORD, current_setting('pg_task.config', false)::JSON) AS s (data TEXT, \"user\" TEXT, schema TEXT, \"table\" TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT s.* FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.datname = data AND a.usename = \"user\" AND application_name = concat_ws(' ', 'pg_task', schema, \"table\", period::TEXT)\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    SPI_connect_my(command, StatementTimeout);
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
        const uint32 period = DatumGetUInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %u, datname_isnull = %s, usename_isnull = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period, datname_isnull ? "true" : "false", usename_isnull ? "true" : "false");
        if (period_isnull) ereport(ERROR, (errmsg("%s(%s:%d): period_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) create_user(user);
        if (datname_isnull) create_data(user, data);
        register_tick_worker(data, user, schema, table, period);
        pfree((void *)data);
        pfree((void *)user);
        if (schema) pfree((void *)schema);
        pfree((void *)table);
    }
    SPI_commit();
    SPI_finish_my(command);
}

static void init(void) {
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) ereport(ERROR, (errmsg("%s(%s:%d): !calloc", __func__, __FILE__, __LINE__)));
    if (!MyProcPort->user_name) MyProcPort->user_name = "postgres";
    if (!MyProcPort->database_name) MyProcPort->database_name = "postgres";
    SetConfigOption("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_OVERRIDE);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(MyProcPort->database_name, MyProcPort->user_name, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    check();
}

void conf_worker(Datum main_arg); void conf_worker(Datum main_arg) {
    init();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) break;
        if (rc & WL_LATCH_SET) { ResetLatch(MyLatch); CHECK_FOR_INTERRUPTS(); }
        if (got_sighup) { got_sighup = false; ProcessConfigFile(PGC_SIGHUP); check(); }
        if (got_sigterm) break;
    } while (!got_sigterm);
}
