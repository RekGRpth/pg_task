#include "include.h"

PG_MODULE_MAGIC;

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *database;
static char *table;
static int32 period;

void SPI_connect_my(const char *command, const int timeout) {
    int rc;
    pgstat_report_activity(STATE_RUNNING, command);
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_connect_ext = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_start_transaction();
    if (timeout > 0) enable_timeout_after(STATEMENT_TIMEOUT, timeout); else disable_timeout(STATEMENT_TIMEOUT, false);
}

void SPI_finish_my(const char *command) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errmsg("%s(%s:%d): SPI_finish = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    ProcessCompletedNotifies();
    pgstat_report_activity(STATE_IDLE, command);
    pgstat_report_stat(true);
}

static void register_conf_worker(void) {
    StringInfoData buf;
    BackgroundWorker worker;
    initStringInfo(&buf);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "conf_worker");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "postgres postgres pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("%s(%s:%d): !process_shared_preload_libraries_in_progress", __func__, __FILE__, __LINE__)));
    DefineCustomStringVariable("pg_task.database", "pg_task database", NULL, &database, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", NULL, &table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.period", "pg_task period", NULL, &period, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(LOG, "%s(%s:%d): database = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database ? database : "(null)", table, period);
    register_conf_worker();
}

static void register_tick_worker(const char *database, const char *username, const char *schema, const char *table, int32 period) {
    StringInfoData buf;
    uint32 database_len = strlen(database), username_len = strlen(username), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), period_len = sizeof(int32);
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    initStringInfo(&buf);
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, period);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "tick_worker");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s%s%s pg_task tick", schema ? schema : "", schema ? "." : "", table);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s %s%s%s pg_task tick", database, username, schema ? schema : "", schema ? "." : "", table);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1 + period_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1 + period_len)));
    memcpy(worker.bgw_extra, database, database_len);
    memcpy(worker.bgw_extra + database_len + 1, username, username_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1, schema, schema_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1 + schema_len + 1, table, table_len);
    *(uint32 *)(worker.bgw_extra + database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1) = period;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errmsg("%s(%s:%d): !RegisterDynamicBackgroundWorker", __func__, __FILE__, __LINE__)));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_STOPPED", __func__, __FILE__, __LINE__)));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED", __func__, __FILE__, __LINE__)));
        default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
    }
    pfree(handle);
}

static void check(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, INT4OID, TEXTOID};
    Datum values[] = {CStringGetTextDatum(table), Int32GetDatum(period), database ? CStringGetTextDatum(database) : (Datum)NULL};
    char nulls[] = {' ', database ? ' ' : 'n'};
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "SELECT      COALESCE(d.datname, database) AS database,\n"
        "            COALESCE(COALESCE(a.rolname, username), database) AS username,\n"
        "            schema,\n"
        "            COALESCE(\"table\", $1) AS table,\n"
        "            COALESCE(period, $2) AS period\n"
        "FROM        json_populate_recordset(NULL::RECORD, COALESCE($3::JSON, '[{}]'::JSON)) AS s (database TEXT, username TEXT, schema TEXT, \"table\" TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON s.database IS NULL OR (d.datname = s.database AND NOT d.datistemplate AND d.datallowconn)\n"
        "LEFT JOIN   pg_authid AS a ON a.rolname = COALESCE(s.username, (SELECT rolname FROM pg_authid WHERE oid = d.datdba)) AND a.rolcanlogin";
    elog(LOG, "%s(%s:%d): database = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database ? database : "(null)", table, period);
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool database_isnull, usename_isnull, schema_isnull, table_isnull, period_isnull;
        char *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "database"), &database_isnull));
        char *username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "username"), &usename_isnull));
        char *schema = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schema"), &schema_isnull));
        char *table = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "table"), &table_isnull));
        int32 period = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        if (database_isnull) ereport(ERROR, (errmsg("%s(%s:%d): database_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) ereport(ERROR, (errmsg("%s(%s:%d): usename_isnull", __func__, __FILE__, __LINE__)));
        if (table_isnull) ereport(ERROR, (errmsg("%s(%s:%d): table_isnull", __func__, __FILE__, __LINE__)));
        if (period_isnull) ereport(ERROR, (errmsg("%s(%s:%d): period_isnull", __func__, __FILE__, __LINE__)));
        register_tick_worker(database, username, schema, table, period);
        pfree(database);
        pfree(username);
        pfree(schema);
        pfree(table);
    }
    SPI_finish_my(command);
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
    elog(LOG, "%s(%s:%d): database = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database ? database : "(null)", table, period);
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
        if (got_sigterm) proc_exit(0);
    } while (!got_sigterm);
    proc_exit(0);
}
