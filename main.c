#include "include.h"

PG_MODULE_MAGIC;

static volatile sig_atomic_t got_sighup = false;
volatile sig_atomic_t got_sigterm = false;
static char *databases;

void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

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

static void register_main_worker(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("main_worker"), "main_worker") != sizeof("main_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("main_worker") - 1)));
    if (snprintf(worker.bgw_type, sizeof("pg_task main"), "pg_task main") != sizeof("pg_task main") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task main") - 1)));
    if (snprintf(worker.bgw_name, sizeof("postgres postgres pg_task main"), "postgres postgres pg_task main") != sizeof("postgres postgres pg_task main") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("postgres postgres pg_task main") - 1)));
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("%s(%s:%d): pg_task can only be loaded via shared_preload_libraries", __func__, __FILE__, __LINE__), errhint("Add pg_task to the shared_preload_libraries configuration variable in postgresql.conf.")));
    DefineCustomStringVariable("pg_task.database", "pg_task database", NULL, &databases, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    register_main_worker();
}

static void register_tick_worker(const char *database, const char *username) {
    size_t len, database_len, username_len;
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("tick_worker"), "tick_worker") != sizeof("tick_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("tick_worker") - 1)));
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
    int rc, i = 0;
    List *elemlist;
    StringInfoData buf;
    Oid *argtypes = NULL;
    Datum *values = NULL;
    char *nulls = NULL;
    char **str = NULL;
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    initStringInfo(&buf);
    appendStringInfoString(&buf,
        "WITH s AS (\n"
        "    SELECT      d.oid, d.datname, u.usesysid, u.usename\n"
        "    FROM        pg_database AS d\n"
        "    JOIN        pg_user AS u ON TRUE\n"
        "    INNER JOIN  pg_user AS i ON d.datdba = i.usesysid\n"
        "    WHERE       NOT datistemplate\n"
        "    AND         datallowconn\n");
    if (!databases) appendStringInfoString(&buf, "    AND         i.usesysid = u.usesysid\n"); else {
        char *rawstring = pstrdup(databases);
        if (!SplitGUCList(rawstring, ',', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s(%s:%d): invalid list syntax in parameter `pg_task.database` in postgresql.conf", __func__, __FILE__, __LINE__)));
        argtypes = palloc(sizeof(Oid) * list_length(elemlist) * 2);
        values = palloc(sizeof(Datum) * list_length(elemlist) * 2);
        nulls = palloc(sizeof(char) * list_length(elemlist) * 2);
        str = palloc(sizeof(char *) * list_length(elemlist) * 2);
        appendStringInfoString(&buf, "    AND         (d.datname, u.usename) IN (\n        ");
        for (ListCell *cell = list_head(elemlist); cell; cell = lnext(cell)) {
            const char *database_username = (const char *)lfirst(cell);
            char *rawstring = pstrdup(database_username);
            List *elemlist;
            if (!SplitIdentifierString(rawstring, ':', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s(%s:%d): invalid list syntax in parameter `pg_task.database` in postgresql.conf", __func__, __FILE__, __LINE__))); else {
                ListCell *cell = list_head(elemlist);
                const char *database = (const char *)lfirst(cell);
                const char *username = NULL;
                nulls[2 * i] = ' ';
                nulls[2 * i + 1] = ' ';
                if ((cell = lnext(cell))) username = (const char *)lfirst(cell);
                else nulls[2 * i + 1] = 'n';
                elog(LOG, "%s(%s:%d): database = %s, username = %s", __func__, __FILE__, __LINE__, database, username ? username : "(null)");
                if (i > 0) appendStringInfoString(&buf, ", ");
                appendStringInfo(&buf, "($%i, COALESCE($%i, i.usename))", 2 * i + 1, 2 * i + 1 + 1);
                argtypes[2 * i] = TEXTOID;
                argtypes[2 * i + 1] = TEXTOID;
                str[2 * i] = pstrdup(database);
                str[2 * i + 1] = username ? pstrdup(username) : NULL;
                values[2 * i] = CStringGetTextDatum(str[2 * i]);
                values[2 * i + 1] = username ? CStringGetTextDatum(str[2 * i + 1]) : (Datum)NULL;
            }
            pfree(rawstring);
            list_free(elemlist);
            i++;
        }
        appendStringInfoString(&buf, "\n    )\n");
        pfree(rawstring);
        list_free(elemlist);
    }
    appendStringInfoString(&buf,
        "), l AS (\n"
        "    SELECT * FROM pg_locks WHERE locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        ")\n"
        "SELECT      datname, usename, TRUE AS start\n"
        "FROM        s\n"
        "WHERE       NOT EXISTS (SELECT pid FROM l WHERE classid = oid AND objid = usesysid AND database = oid)\n"
        "UNION\n"
        "SELECT      datname, usename, NOT pg_terminate_backend(pid) AS start\n"
        "FROM        pg_stat_activity\n"
        "INNER JOIN  l USING (pid)\n"
        "WHERE       (datname, usename) NOT IN (SELECT datname, usename FROM s)\n"
        "AND         classid = datid AND objid = usesysid AND database = datid");
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, i * 2, argtypes, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool database_isnull, usename_isnull, start_isnull;
        char *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &database_isnull));
        char *username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull));
        bool start = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "start"), &start_isnull));
        if (database_isnull) ereport(ERROR, (errmsg("%s(%s:%d): database_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) ereport(ERROR, (errmsg("%s(%s:%d): usename_isnull", __func__, __FILE__, __LINE__)));
        if (start_isnull) ereport(ERROR, (errmsg("%s(%s:%d): start_isnull", __func__, __FILE__, __LINE__)));
        if (start) register_tick_worker(database, username);
    }
    SPI_finish_my(buf.data);
    pfree(buf.data);
    if (argtypes) pfree(argtypes);
    if (values) pfree(values);
    if (nulls) pfree(nulls);
    if (str) { for (int j = 0; j < i * 2; j++) if (str[j]) pfree(str[j]); pfree(str); }
}

void main_worker(Datum main_arg); void main_worker(Datum main_arg) {
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
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
