#include "include.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

extern char *pg_task_config;
extern char *pg_task_taskname;
extern uint32 pg_task_period;

static void create_username(const char *username) {
    int rc;
    StringInfoData buf;
    const char *username_q = quote_identifier(username);
    elog(LOG, "%s(%s:%d): username = %s", __func__, __FILE__, __LINE__, username);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE USER %s", username_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    if (username_q != username) pfree((void *)username_q);
    pfree(buf.data);
}

static void create_dataname(const char *username, const char *dataname) {
    StringInfoData buf;
    const char *username_q = quote_identifier(username);
    const char *dataname_q = quote_identifier(dataname);
    ParseState *pstate = make_parsestate(NULL);
    List *options = NIL;
    CreatedbStmt *stmt = makeNode(CreatedbStmt);
    elog(LOG, "%s(%s:%d): username = %s, dataname = %s", __func__, __FILE__, __LINE__, username, dataname);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", dataname_q, username_q);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("owner", (Node *)makeString((char *)username), -1));
    stmt->dbname = (char *)dataname;
    stmt->options = options;
    SPI_connect_my(buf.data, StatementTimeout);
    createdb(pstate, stmt);
    SPI_commit();
    SPI_finish_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (username_q != username) pfree((void *)username_q);
    if (dataname_q != dataname) pfree((void *)dataname_q);
    pfree(buf.data);
}

static void register_tick_worker(const char *dataname, const char *username, const char *schemaname, const char *tablename, uint32 period) {
    StringInfoData buf;
    uint32 dataname_len = strlen(dataname), username_len = strlen(username), schemaname_len = schemaname ? strlen(schemaname) : 0, tablename_len = strlen(tablename), period_len = sizeof(period);
    BackgroundWorker worker;
    elog(LOG, "%s(%s:%d): dataname = %s, username = %s, schemaname = %s, tablename = %s, period = %u", __func__, __FILE__, __LINE__, dataname, username, schemaname ? schemaname : "(null)", tablename, period);
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
    appendStringInfo(&buf, "pg_task %s%s%s", schemaname ? schemaname : "", schemaname ? "." : "", tablename);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %u", username, dataname, schemaname ? schemaname : "", schemaname ? "." : "", tablename, period);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (dataname_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + period_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, dataname_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + period_len)));
    memcpy(worker.bgw_extra, dataname, dataname_len);
    memcpy(worker.bgw_extra + dataname_len + 1, username, username_len);
    memcpy(worker.bgw_extra + dataname_len + 1 + username_len + 1, schemaname, schemaname_len);
    memcpy(worker.bgw_extra + dataname_len + 1 + username_len + 1 + schemaname_len + 1, tablename, tablename_len);
    *(typeof(period) *)(worker.bgw_extra + dataname_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1) = period;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static void check(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, INT4OID, TEXTOID};
    Datum values[] = {CStringGetTextDatum(pg_task_taskname), UInt32GetDatum(pg_task_period), pg_task_config ? CStringGetTextDatum(pg_task_config) : (Datum)NULL};
    char nulls[] = {' ', ' ', pg_task_config ? ' ' : 'n'};
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS (\n"
        "SELECT      COALESCE(datname, dataname)::TEXT AS dataname,\n"
        "            datname,\n"
        "            COALESCE(COALESCE(usename, username), dataname)::TEXT AS username,\n"
        "            usename,\n"
        "            schemaname,\n"
        "            COALESCE(tablename, $1) AS tablename,\n"
        "            COALESCE(period, $2) AS period\n"
        "FROM        json_populate_recordset(NULL::RECORD, COALESCE($3::JSON, '[{}]'::JSON)) AS s (dataname TEXT, username TEXT, schemaname TEXT, tablename TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON dataname IS NULL OR (datname = dataname AND NOT datistemplate AND datallowconn)\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(username, (SELECT usename FROM pg_user WHERE usesysid = datdba)), dataname)\n"
        ") SELECT s.* FROM s\n"
        "LEFT JOIN   pg_stat_activity AS a ON a.datname = dataname AND a.usename = username AND application_name = concat_ws(' ', 'pg_task', schemaname||'.', tablename, period::TEXT)\n"
        "LEFT JOIN   pg_locks AS l ON l.pid = a.pid AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        "WHERE       a.pid IS NULL";
    elog(LOG, "%s(%s:%d): pg_task_config = %s, pg_task_taskname = %s, pg_task_period = %u", __func__, __FILE__, __LINE__, pg_task_config ? pg_task_config : "(null)", pg_task_taskname, pg_task_period);
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool dataname_isnull, username_isnull, schemaname_isnull, tablename_isnull, period_isnull, datname_isnull, usename_isnull;
        char *dataname = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "dataname", &dataname_isnull);
        char *username = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "username", &username_isnull);
        char *schemaname = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schemaname", &schemaname_isnull);
        char *tablename = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "tablename", &tablename_isnull);
        uint32 period = DatumGetUInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        elog(LOG, "%s(%s:%d): dataname = %s, username = %s, schemaname = %s, tablename = %s, period = %u, datname_isnull = %s, usename_isnull = %s", __func__, __FILE__, __LINE__, dataname, username, schemaname ? schemaname : "(null)", tablename, period, datname_isnull ? "true" : "false", usename_isnull ? "true" : "false");
        if (dataname_isnull) ereport(ERROR, (errmsg("%s(%s:%d): dataname_isnull", __func__, __FILE__, __LINE__)));
        if (username_isnull) ereport(ERROR, (errmsg("%s(%s:%d): username_isnull", __func__, __FILE__, __LINE__)));
        if (tablename_isnull) ereport(ERROR, (errmsg("%s(%s:%d): tablename_isnull", __func__, __FILE__, __LINE__)));
        if (period_isnull) ereport(ERROR, (errmsg("%s(%s:%d): period_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) create_username(username);
        if (datname_isnull) create_dataname(username, dataname);
        register_tick_worker(dataname, username, schemaname, tablename, period);
        pfree(dataname);
        pfree(username);
        if (schemaname) pfree(schemaname);
        pfree(tablename);
    }
    SPI_finish_my(command);
    pfree((void *)values[0]);
    if (pg_task_config) pfree((void *)values[2]);
}

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

void conf_worker(Datum main_arg); void conf_worker(Datum main_arg) {
    elog(LOG, "%s(%s:%d): pg_task_config = %s, pg_task_taskname = %s, pg_task_period = %u", __func__, __FILE__, __LINE__, pg_task_config ? pg_task_config : "(null)", pg_task_taskname, pg_task_period);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    check();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            check();
        }
        if (got_sigterm) break;
    } while (!got_sigterm);
}
