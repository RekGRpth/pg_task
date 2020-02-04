#include "include.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

extern char *config;
extern char *default_tablename;
extern uint32 default_period;

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

static void create_database(const char *username, const char *database) {
    StringInfoData buf;
    const char *username_q = quote_identifier(username);
    const char *database_q = quote_identifier(database);
    ParseState *pstate = make_parsestate(NULL);
    List *options = NIL;
    CreatedbStmt *stmt = makeNode(CreatedbStmt);
    elog(LOG, "%s(%s:%d): username = %s, database = %s", __func__, __FILE__, __LINE__, username, database);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE DATABASE %s WITH OWNER = %s", database_q, username_q);
    pstate->p_sourcetext = buf.data;
    options = lappend(options, makeDefElem("owner", (Node *)makeString((char *)username), -1));
    stmt->dbname = (char *)database;
    stmt->options = options;
    SPI_connect_my(buf.data, StatementTimeout);
    createdb(pstate, stmt);
    SPI_commit();
    SPI_finish_my(buf.data);
    free_parsestate(pstate);
    list_free_deep(options);
    pfree(stmt);
    if (username_q != username) pfree((void *)username_q);
    if (database_q != database) pfree((void *)database_q);
    pfree(buf.data);
}

static void register_tick_worker(const char *database, const char *username, const char *schemaname, const char *tablename, uint32 period) {
    StringInfoData buf;
    uint32 database_len = strlen(database), username_len = strlen(username), schemaname_len = schemaname ? strlen(schemaname) : 0, tablename_len = strlen(tablename), period_len = sizeof(uint32);
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s, period = %u", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename, period);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
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
    appendStringInfo(&buf, "%s %s pg_task %s%s%s", username, database, schemaname ? schemaname : "", schemaname ? "." : "", tablename);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + period_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + period_len)));
    memcpy(worker.bgw_extra, database, database_len);
    memcpy(worker.bgw_extra + database_len + 1, username, username_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1, schemaname, schemaname_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1 + schemaname_len + 1, tablename, tablename_len);
    *(uint32 *)(worker.bgw_extra + database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1) = period;
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
    Datum values[] = {CStringGetTextDatum(default_tablename), UInt32GetDatum(default_period), config ? CStringGetTextDatum(config) : (Datum)NULL};
    char nulls[] = {' ', ' ', config ? ' ' : 'n'};
    static const char *command =
        "SELECT      COALESCE(datname, database)::TEXT AS database,\n"
        "            datname,\n"
        "            COALESCE(COALESCE(usename, username), database)::TEXT AS username,\n"
        "            usename,\n"
        "            schemaname,\n"
        "            COALESCE(tablename, $1) AS tablename,\n"
        "            COALESCE(period, $2) AS period\n"
        "FROM        json_populate_recordset(NULL::RECORD, COALESCE($3::JSON, '[{}]'::JSON)) AS s (database TEXT, username TEXT, schemaname TEXT, tablename TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON database IS NULL OR (datname = database AND NOT datistemplate AND datallowconn)\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(username, (SELECT usename FROM pg_user WHERE oid = datdba)), database)";
    elog(LOG, "%s(%s:%d): config = %s, tablename = %s, period = %u", __func__, __FILE__, __LINE__, config ? config : "(null)", default_tablename, default_period);
    SPI_connect_my(command, StatementTimeout);
    if ((rc = SPI_execute_with_args(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool database_isnull, username_isnull, schemaname_isnull, tablename_isnull, period_isnull, datname_isnull, usename_isnull;
        char *database = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "database", &database_isnull);
        char *username = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "username", &username_isnull);
        char *schemaname = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schemaname", &schemaname_isnull);
        char *tablename = TextDatumGetCStringOrNULL(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "tablename", &tablename_isnull);
        uint32 period = DatumGetUInt32(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "period"), &period_isnull));
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &datname_isnull);
        SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull);
        elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s, period = %u, datname_isnull = %s, usename_isnull = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename, period, datname_isnull ? "true" : "false", usename_isnull ? "true" : "false");
        if (database_isnull) ereport(ERROR, (errmsg("%s(%s:%d): database_isnull", __func__, __FILE__, __LINE__)));
        if (username_isnull) ereport(ERROR, (errmsg("%s(%s:%d): username_isnull", __func__, __FILE__, __LINE__)));
        if (tablename_isnull) ereport(ERROR, (errmsg("%s(%s:%d): tablename_isnull", __func__, __FILE__, __LINE__)));
        if (period_isnull) ereport(ERROR, (errmsg("%s(%s:%d): period_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) create_username(username);
        if (datname_isnull) create_database(username, database);
        register_tick_worker(database, username, schemaname, tablename, period);
        pfree(database);
        pfree(username);
        if (schemaname) pfree(schemaname);
        pfree(tablename);
    }
    SPI_finish_my(command);
    pfree((void *)values[0]);
    if (config) pfree((void *)values[2]);
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
    elog(LOG, "%s(%s:%d): config = %s, tablename = %s, period = %u", __func__, __FILE__, __LINE__, config ? config : "(null)", default_tablename, default_period);
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
