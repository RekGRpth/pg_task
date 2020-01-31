#include "include.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *database;
static char *table;
static int32 period;

static void register_tick_worker(const char *database, const char *username, const char *schema, const char *table, int32 period) {
    size_t len, database_len = strlen(database), username_len = strlen(username), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table);
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, period);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if ((len = snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task")) != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu != %lu", __func__, __FILE__, __LINE__, len, sizeof("pg_task") - 1)));
    if ((len = snprintf(worker.bgw_function_name, sizeof("tick_worker"), "tick_worker")) != sizeof("tick_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu != %lu", __func__, __FILE__, __LINE__, len, sizeof("tick_worker") - 1)));


    if (snprintf(worker.bgw_type, sizeof("pg_task tick"), "pg_task tick") != sizeof("pg_task tick") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task tick") - 1)));


    len = (sizeof("%s %s pg_task tick") - 1) + (strlen(username) - 1) - 1 + (strlen(database) - 1) - 1;
    if (len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, len + 1)));
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_task tick", username, database) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
    database_len = (sizeof("%s") - 1) + (strlen(database) - 1) - 1;
    username_len = (sizeof("%s") - 1) + (strlen(username) - 1) - 1;
    if (database_len + 1 + username_len + 1 > BGW_EXTRALEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1)));
    if (snprintf(worker.bgw_extra                   , database_len + 1, "%s", database) != database_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, database_len)));
    if (snprintf(worker.bgw_extra + database_len + 1, username_len + 1, "%s", username) != username_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, username_len)));
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not register background process", __func__, __FILE__, __LINE__), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not start background process", __func__, __FILE__, __LINE__), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): cannot start background processes without postmaster", __func__, __FILE__, __LINE__), errhint("Kill all remaining database processes and restart the database.")));
        default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
    }
    pfree(handle);
}

static void check(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, INT4OID, JSONOID};
    Datum values[] = {CStringGetTextDatum(table), Int32GetDatum(period), database ? CStringGetTextDatum(database) : (Datum)NULL};
    char nulls[] = {' ', database ? ' ' : 'n'};
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "SELECT      COALESCE(d.datname, database) AS database,\n"
        "            COALESCE(COALESCE(a.rolname, username), database) AS username,\n"
        "            schema,\n"
        "            COALESCE(\"table\", $1) AS table,\n"
        "            COALESCE(period, $2) AS pertiod\n"
        "FROM        json_populate_recordset(NULL::RECORD, COALESCE($3/*::JSON*/, '[{}]')) AS s (database TEXT, username TEXT, schema TEXT, \"table\" TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON s.database IS NULL OR (d.datname = s.database AND NOT d.datistemplate AND d.datallowconn)\n"
        "LEFT JOIN   pg_authid AS a ON a.rolname = COALESCE(s.username, (SELECT rolname FROM pg_authid WHERE oid = d.datdba)) AND a.rolcanlogin";
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool database_isnull, usename_isnull, schema_isnull, table_isnull, period_isnull;
        char *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &database_isnull));
        char *username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull));
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
//    elog(LOG, "%s(%s:%d): database = %s", __func__, __FILE__, __LINE__, database ? database : "(null)");
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    DefineCustomStringVariable("pg_task.database", "pg_task database", NULL, &database, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", "task", &table, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.period", "pg_task period", NULL, &period, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(LOG, "%s(%s:%d): database = %s, table = %s, period = %d", __func__, __FILE__, __LINE__, database ? database : "(null)", table, period);
    check();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | /*WL_TIMEOUT |*/ WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
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
