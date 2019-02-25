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
#include <catalog/pg_type.h>

PG_MODULE_MAGIC;

void _PG_init(void);

void man(Datum arg);
void task(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *database = NULL;
static char *username = NULL;
static int period = 1;

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

static inline void connect_my() {
    (void)SetCurrentStatementStartTimestamp();
    (void)StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) elog(FATAL, "SPI_connect != SPI_OK_CONNECT");
    (void)PushActiveSnapshot(GetTransactionSnapshot());
}

static inline int execute_my(const char *src) {
    int ret;
    (void)pgstat_report_activity(STATE_RUNNING, src);
    ret = SPI_execute(src, false, 0);
    (void)pgstat_report_activity(STATE_IDLE, src);
    (void)pgstat_report_stat(true);
    return ret;
}

static inline int execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *Values, const char *Nulls) {
    int ret;
    (void)pgstat_report_activity(STATE_RUNNING, src);
    ret = SPI_execute_with_args(src, nargs, argtypes, Values, Nulls, false, 0);
    (void)pgstat_report_activity(STATE_IDLE, src);
    (void)pgstat_report_stat(true);
    return ret;
}

static inline void finish_my() {
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH");
    (void)PopActiveSnapshot();
    (void)CommitTransactionCommand();
    (void)ProcessCompletedNotifies();
}

/*static inline void expirer() {
    (void)connect_my();
    if (execute_my("UPDATE task SET state = 'EXPIRE' WHERE state = 'QUEUE' AND duration IS NOT NULL AND dt + duration < now()") != SPI_OK_UPDATE) elog(FATAL, "execute_my != SPI_OK_UPDATE");
    (void)finish_my();
}*/

static inline void launch_task(Datum id) {
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
    if (snprintf(worker.bgw_function_name, sizeof("task"), "task") != sizeof("task") - 1) elog(FATAL, "snprintf");
    len = sizeof("%s %s pg_scheduler task") - 1 + strlen(database) - 1 + strlen(username) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler task", database, username) != len) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler task"), "pg_scheduler task") != sizeof("pg_scheduler task") - 1) elog(FATAL, "snprintf");
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = id;
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
        default: elog(ERROR, "unexpected bgworker handle status"); break;
    }
//    if (handle != NULL) (void)pfree(handle);
}

/*static inline void assigner() {
    bool isnull;
    (void)connect_my();
    if (execute_my("UPDATE task SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id") != SPI_OK_UPDATE_RETURNING) elog(FATAL, "execute_my != SPI_OK_UPDATE_RETURNING");
    for (unsigned int i = 0; i < SPI_processed; i++) (void)launch_task(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    (void)finish_my();
}*/

void task(Datum main_arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {main_arg};
    elog(LOG, "task started id=%li", DatumGetInt64(main_arg));
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)connect_my();
//    if (execute_with_args_my("SELECT * FROM task WHERE id = $1", 1, argtypes, Values, NULL) != SPI_OK_SELECT) elog(FATAL, "execute_my != SPI_OK_SELECT");
    if (execute_with_args_my("UPDATE task SET state = 'WORK' WHERE id = $1 RETURNING request", 1, argtypes, Values, NULL) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "execute_my != SPI_OK_UPDATE_RETURNING");
//    elog(LOG, "SPI_processed=%li", SPI_processed);
    for (unsigned int i = 0; i < SPI_processed; i++) {
        char *request = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        elog(LOG, "request=%s", request);
        (int)execute_my(request);
        if (request != NULL) (void)pfree(request);
        if (execute_with_args_my("UPDATE task SET state = 'DONE' WHERE id = $1", 1, argtypes, Values, NULL) != SPI_OK_UPDATE) elog(FATAL, "execute_my != SPI_OK_UPDATE");
    }
    (void)finish_my();
}

void man(Datum main_arg) {
    elog(LOG, "man started database=%s, username=%s", database, username);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    while (!got_sigterm) {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period * 1000L, PG_WAIT_EXTENSION);
        (void)ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) (void)proc_exit(1);
        if (got_sigterm) (void)proc_exit(0);
        if (got_sighup) {
            got_sighup = false;
            (void)ProcessConfigFile(PGC_SIGHUP);
        }
        if (rc & WL_TIMEOUT) {
//            expirer();
//            assigner();
            bool isnull;
            (void)connect_my();
            if (execute_my("UPDATE task SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id") != SPI_OK_UPDATE_RETURNING) elog(FATAL, "execute_my != SPI_OK_UPDATE_RETURNING");
            for (unsigned int i = 0; i < SPI_processed; i++) {
                elog(LOG, "i=%i", i);
                (void)launch_task(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
            }
            (void)finish_my();
        }
    }
    (void)proc_exit(1);
}

void _PG_init(void) {
    BackgroundWorker worker;
    int len;
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errmsg("pg_scheduler can only be loaded via shared_preload_libraries"), errhint("Add pg_scheduler to the shared_preload_libraries configuration variable in postgresql.conf.")));
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    (void)DefineCustomStringVariable("pg_scheduler.database", "pg_scheduler database", NULL, &database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    (void)DefineCustomStringVariable("pg_scheduler.username", "pg_scheduler username", NULL, &username, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.period", "how often to run man", NULL, &period, 1, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.restart", "how often to restart man", NULL, &worker.bgw_restart_time, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
//    worker.bgw_restart_time = 10;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_function_name, sizeof("man"), "man") != sizeof("man") - 1) elog(FATAL, "snprintf");
    len = sizeof("%s %s pg_scheduler man") - 1 + strlen(database) - 1 + strlen(username) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler man", database, username) != len) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler man"), "pg_scheduler man") != sizeof("pg_scheduler man") - 1) elog(FATAL, "snprintf");
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    (void)RegisterBackgroundWorker(&worker);
}
