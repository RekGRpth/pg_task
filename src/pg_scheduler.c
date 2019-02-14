#include "postgres.h"
#include "fmgr.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "commands/async.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void launcher(Datum arg);
void ticker(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static char *initial_database = NULL;
static char *initial_username = NULL;
static int stats_period = 30;
static int check_period = 60;
static int retry_period = 30;
static int maint_period = 120;
static int ticker_period = 1;
static unsigned long int time_time = 0;
static unsigned long int next_ticker = 0;
static unsigned long int next_maint = 0;
static unsigned long int next_retry = 0;
static unsigned long int next_stats = 0;
static unsigned long int n_ticks = 0;
static unsigned long int n_maint = 0;
static unsigned long int n_retry = 0;

static inline void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static inline void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    (void)SetLatch(MyLatch);
    errno = save_errno;
}

static inline int min(int a, int b, int c) {
    int m = a;
    if (m > b) m = b;
    if (m > c) return c;
    return m;
}

static inline void connect_my() {
    (void)SetCurrentStatementStartTimestamp();
    (void)StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) elog(FATAL, "SPI_connect != SPI_OK_CONNECT");
    (void)PushActiveSnapshot(GetTransactionSnapshot());
}

static inline int execute_my(char *sql) {
    int ret;
    (void)pgstat_report_activity(STATE_RUNNING, sql);
    ret = SPI_execute(sql, false, 0);
    (void)pgstat_report_activity(STATE_IDLE, NULL);
    (void)pgstat_report_stat(false);
    return ret;
}

static inline void finish_my() {
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH");
    (void)PopActiveSnapshot();
    (void)CommitTransactionCommand();
    (void)ProcessCompletedNotifies();
}

static inline void initialize_ticker() {
    (void)connect_my();
    if (execute_my("SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_namespace.oid::INT) FROM pg_database, pg_namespace WHERE datname = current_catalog AND nspname = 'pgq'") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    else {
        bool isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) elog(FATAL, "isnull");
        if (!lock) elog(FATAL, "already running");
    }
    (void)finish_my();
}

void ticker(Datum arg) {
    StringInfoData buf;
    char *datname = MyBgworkerEntry->bgw_extra;
    char *usename = datname + strlen(datname) + 1;
    elog(LOG, "ticker started datname=%s, usename=%s", datname, usename);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(datname, usename, 0);
    (void)initialize_ticker();
    (void)initStringInfo(&buf);
    while (!got_sigterm) {
        int period = min(retry_period, maint_period, ticker_period);
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period * 1000L, PG_WAIT_EXTENSION);
        (void)ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) (void)proc_exit(1);
        if (got_sigterm) (void)proc_exit(0);
        if (got_sighup) {
            got_sighup = false;
            (void)ProcessConfigFile(PGC_SIGHUP);
        }
        if (rc & WL_TIMEOUT) time_time += period;
        if (time_time >= next_ticker) {
            (void)connect_my();
            if (execute_my("SELECT pgq.ticker()") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
            if (SPI_processed == 1) n_ticks++;
            (void)finish_my();
            next_ticker = time_time + ticker_period;
        }
        if (time_time >= next_maint) {
            (void)connect_my();
            if (execute_my("SELECT func_name, func_arg FROM pgq.maint_operations()") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
            (void)resetStringInfo(&buf);
            for (unsigned int i = 0; i < SPI_processed; i++) {
                char *func_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
                char *func_arg = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
                if (!strncasecmp(func_name, "vacuum", sizeof("vacuum") - 1)) {
                    appendStringInfo(&buf, "%s \"%s\";", func_name, func_arg);
                } else if (func_arg != NULL) {
                    appendStringInfo(&buf, "SELECT %s('%s');", func_name, func_arg);
                } else {
                    appendStringInfo(&buf, "SELECT %s();", func_name);
                }
                if (func_name != NULL) (void)pfree(func_name);
                if (func_arg != NULL) (void)pfree(func_arg);
                n_maint++;
            }
            if (buf.len > 0) {
                elog(LOG, "datname=%s, usename=%s, buf.data=%s", datname, usename, buf.data);
                if (execute_my(buf.data) != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
            }
            (void)finish_my();
            next_maint = time_time + maint_period;
        }
        if (time_time >= next_retry) {
            (void)connect_my();
            for (int retry = 1; retry; ) {
                if (execute_my("SELECT * FROM pgq.maint_retry_events()") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
                if (SPI_processed == 1) {
                    bool isnull;
                    retry = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
                    n_retry += retry;
                } else retry = 0;
            }
            (void)finish_my();
            next_retry = time_time + retry_period;
        }
        if (time_time >= next_stats) {
            elog(LOG, "datname=%s, usename=%s, time_time=%lu, n_ticks=%lu, n_maint=%lu, n_retry=%lu", datname, usename, time_time, n_ticks, n_maint, n_retry);
            next_stats = time_time + stats_period;
            n_ticks = 0;
            n_maint = 0;
            n_retry = 0;
        }
    }
//    elog(LOG, "ticker finished datname=%s, usename=%s", datname, usename);
    (void)proc_exit(1);
}

static inline void initialize_launcher() {
    (void)connect_my();
    if (execute_my("SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'dblink'") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    else {
        bool isnull;
        int ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull) elog(FATAL, "isnull");
        if ((ntup == 0) && (execute_my("CREATE SCHEMA IF NOT EXISTS dblink; CREATE EXTENSION IF NOT EXISTS dblink SCHEMA dblink") != SPI_OK_UTILITY)) elog(FATAL, "execute_my != SPI_OK_UTILITY");
    }
    (void)finish_my();
}

static inline void launch_ticker(char *datname, char *usename) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    pid_t pid;
    int len;
    MemoryContext oldcontext;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_function_name, sizeof("ticker"), "ticker") != sizeof("ticker") - 1) elog(FATAL, "snprintf");
    len = sizeof("%s %s pg_scheduler worker") - 1 + strlen(datname) - 1 + strlen(usename) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler worker", datname, usename) != len) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler worker"), "pg_scheduler worker") != sizeof("pg_scheduler worker") - 1) elog(FATAL, "snprintf");
    len = strlen(usename);
    if (snprintf(worker.bgw_extra + snprintf(worker.bgw_extra, strlen(datname) + 1, "%s", datname) + 1, len + 1, "%s", usename) != len) elog(FATAL, "snprintf");
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    (MemoryContext)MemoryContextSwitchTo(oldcontext);
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED:
            (void)pfree(handle);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
            break;
        case BGWH_POSTMASTER_DIED:
            (void)pfree(handle);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
            break;
        default:
            elog(ERROR, "unexpected bgworker handle status");
            break;
    }
}

void launcher(Datum main_arg) {
    elog(LOG, "launcher started initial_database=%s, initial_username=%s", initial_database, initial_username);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(initial_database, initial_username, 0);
    (void)initialize_launcher();
    while (!got_sigterm) {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, check_period * 1000L, PG_WAIT_EXTENSION);
        (void)ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) (void)proc_exit(1);
        if (got_sigterm) (void)proc_exit(0);
        if (got_sighup) {
            got_sighup = false;
            (void)ProcessConfigFile(PGC_SIGHUP);
        }
        (void)connect_my();
        if (execute_my("WITH subquery AS ( "
            "SELECT datname, "
            "(SELECT usename FROM dblink.dblink('dbname='||datname||' user='||usename, 'SELECT case when pg_try_advisory_lock(pg_database.oid::INT, pg_namespace.oid::INT) then usename else null end as usename FROM pg_database, pg_namespace, pg_user WHERE datname = current_catalog AND nspname = ''pgq'' and usesysid = nspowner') AS (usename name)) AS usename "
            "FROM pg_database "
            "INNER JOIN pg_user ON usesysid = datdba "
            "WHERE NOT datistemplate "
            "AND datallowconn "
        ") SELECT datname, usename FROM subquery WHERE usename IS NOT NULL") != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
        for (unsigned int i = 0; i < SPI_processed; i++) {
            char *datname = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            char *usename = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            (void)launch_ticker(datname, usename);
            if (datname != NULL) (void)pfree(datname);
            if (usename != NULL) (void)pfree(usename);
        }
        (void)finish_my();
    }
//    elog(LOG, "launcher finished initial_database=%s, initial_username=%s", initial_database, initial_username);
    (void)proc_exit(1);
}

void _PG_init(void) {
    BackgroundWorker worker;
    int len;
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errmsg("pg_scheduler can only be loaded via shared_preload_libraries"), errhint("Add pg_scheduler to the shared_preload_libraries configuration variable in postgresql.conf.")));
    (void)DefineCustomStringVariable("pg_scheduler.initial_database", "startup database to query other databases", NULL, &initial_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    (void)DefineCustomStringVariable("pg_scheduler.initial_username", "startup username to query other databases", NULL, &initial_username, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.check_period", "how often to check for new databases", NULL, &check_period, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.retry_period", "how often to flush retry queue", NULL, &retry_period, 30, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.maint_period", "how often to do maintentance", NULL, &maint_period, 120, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.ticker_period", "how often to run ticker", NULL, &ticker_period, 1, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.stats_period", "how often to print statistics", NULL, &stats_period, 30, min(retry_period, maint_period, ticker_period), INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_function_name, sizeof("launcher"), "launcher") != sizeof("launcher") - 1) elog(FATAL, "snprintf");
    len = sizeof("%s %s pg_scheduler launcher") - 1 + strlen(initial_database) - 1 + strlen(initial_username) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler launcher", initial_database, initial_username) != len) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler launcher"), "pg_scheduler launcher") != sizeof("pg_scheduler launcher") - 1) elog(FATAL, "snprintf");
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    (void)RegisterBackgroundWorker(&worker);
}
