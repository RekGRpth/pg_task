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

void loop(Datum arg);
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
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
        default: elog(ERROR, "unexpected bgworker handle status");
    }
    if (handle != NULL) (void)pfree(handle);
}

static inline void connect_my(const char *cmd_str) {
    (void)pgstat_report_activity(STATE_RUNNING, cmd_str);
    (void)SetCurrentStatementStartTimestamp();
    (void)StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) elog(FATAL, "SPI_connect != SPI_OK_CONNECT");
    (void)PushActiveSnapshot(GetTransactionSnapshot());
}

static inline void finish_my(const char *cmd_str) {
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH");
    (void)PopActiveSnapshot();
    (void)CommitTransactionCommand();
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, cmd_str);
    (void)pgstat_report_stat(true);
}

static inline char *work(Datum main_arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {main_arg};
    char *request = NULL;
    const char *src = "UPDATE task SET state = 'WORK' WHERE id = $1 RETURNING request";
    (void)connect_my(src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE_RETURNING");
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    request = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    (void)finish_my(src);
    return request;
}

//static inline void exception() {
//}

static inline int execute_my(const char *src, bool read_only, long tcount) {
//    int res = 0;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;
    BeginInternalSubTransaction(NULL);
    MemoryContextSwitchTo(oldcontext);
    PG_TRY(); {
        int res = SPI_execute(src, false, 0);
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;
        return res;
    } PG_CATCH(); {
        ErrorData *edata;
        MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;
        elog(LOG, "edata={"
            "\"elevel\":%i,"
            "\"output_to_server\":%s,"
            "\"output_to_client\":%s,"
            "\"show_funcname\":%s,"
            "\"hide_stmt\":%s,"
            "\"hide_ctx\":%s,"
            "\"filename\":\"%s\","
            "\"lineno\":%i,"
            "\"funcname\":\"%s\","
            "\"domain\":\"%s\","
            "\"context_domain\":\"%s\","
            "\"sqlerrcode\":%i,"
            "\"message\":\"%s\","
            "\"detail\":\"%s\","
            "\"detail_log\":\"%s\","
            "\"hint\":\"%s\","
            "\"context\":\"%s\","
            "\"message_id\":\"%s\","
            "\"schema_name\":\"%s\","
            "\"table_name\":\"%s\","
            "\"column_name\":\"%s\","
            "\"datatype_name\":\"%s\","
            "\"constraint_name\":\"%s\","
            "\"cursorpos\":%i,"
            "\"internalpos\":%i,"
            "\"internalquery\":\"%s\","
            "\"saved_errno\":%i"
        "}",
            edata->elevel,
            edata->output_to_server?"true":"false",
            edata->output_to_client?"true":"false",
            edata->show_funcname?"true":"false",
            edata->hide_stmt?"true":"false",
            edata->hide_ctx?"true":"false",
            edata->filename,
            edata->lineno,
            edata->funcname,
            edata->domain,
            edata->context_domain,
            edata->sqlerrcode,
            edata->message,
            edata->detail,
            edata->detail_log,
            edata->hint,
            edata->context,
            edata->message_id,
            edata->schema_name,
            edata->table_name,
            edata->column_name,
            edata->datatype_name,
            edata->constraint_name,
            edata->cursorpos,
            edata->internalpos,
            edata->internalquery,
            edata->saved_errno
        );
        (void)FreeErrorData(edata);
//        PG_RE_THROW();
        return 0;
    } PG_END_TRY();
//    return res;
}

static inline int execute(char *src) {
    int res;
//    MemoryContext oldcontext;
    elog(LOG, "src=%s", src);
    (void)connect_my(src);
//    elog(LOG, "execute_my=%i", execute_my(src, false, 0));
    res = execute_my(src, false, 0);
/*    oldcontext = CurrentMemoryContext;
    PG_TRY(); {
        elog(LOG, "SPI_execute=%i", SPI_execute(src, false, 0));
    } PG_CATCH(); {
        ErrorData *edata;
        MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();
        elog(LOG, "edata={"
            "\"elevel\":%i,"
            "\"output_to_server\":%s,"
            "\"output_to_client\":%s,"
            "\"show_funcname\":%s,"
            "\"hide_stmt\":%s,"
            "\"hide_ctx\":%s,"
            "\"filename\":\"%s\","
            "\"lineno\":%i,"
            "\"funcname\":\"%s\","
            "\"domain\":\"%s\","
            "\"context_domain\":\"%s\","
            "\"sqlerrcode\":%i,"
            "\"message\":\"%s\","
            "\"detail\":\"%s\","
            "\"detail_log\":\"%s\","
            "\"hint\":\"%s\","
            "\"context\":\"%s\","
            "\"message_id\":\"%s\","
            "\"schema_name\":\"%s\","
            "\"table_name\":\"%s\","
            "\"column_name\":\"%s\","
            "\"datatype_name\":\"%s\","
            "\"constraint_name\":\"%s\","
            "\"cursorpos\":%i,"
            "\"internalpos\":%i,"
            "\"internalquery\":\"%s\","
            "\"saved_errno\":%i"
        "}",
            edata->elevel,
            edata->output_to_server?"true":"false",
            edata->output_to_client?"true":"false",
            edata->show_funcname?"true":"false",
            edata->hide_stmt?"true":"false",
            edata->hide_ctx?"true":"false",
            edata->filename,
            edata->lineno,
            edata->funcname,
            edata->domain,
            edata->context_domain,
            edata->sqlerrcode,
            edata->message,
            edata->detail,
            edata->detail_log,
            edata->hint,
            edata->context,
            edata->message_id,
            edata->schema_name,
            edata->table_name,
            edata->column_name,
            edata->datatype_name,
            edata->constraint_name,
            edata->cursorpos,
            edata->internalpos,
            edata->internalquery,
            edata->saved_errno
        );
//        if (edata != NULL) (void)FreeErrorData(edata);
//        PG_RE_THROW();
    } PG_END_TRY();*/
    if (src != NULL) (void)pfree(src);
    (void)finish_my(src);
    return res;
}

static inline void done(Datum main_arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {main_arg};
    const char *src = "UPDATE task SET state = 'DONE' WHERE id = $1";
    (void)connect_my(src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

static inline void fail(Datum main_arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {main_arg};
    const char *src = "UPDATE task SET state = 'FAIL' WHERE id = $1";
    (void)connect_my(src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

void task(Datum main_arg) {
    elog(LOG, "task started id=%li", DatumGetInt64(main_arg));
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    if (execute(work(main_arg)) > 0) (void)done(main_arg);
    else (void)fail(main_arg);
}

static inline void assign() {
    const char *src = "UPDATE task SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id";
    (void)connect_my(src);
    if (SPI_execute(src, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute != SPI_OK_UPDATE_RETURNING");
    else {
        uint64 processed = SPI_processed;
        SPITupleTable *tuptable = SPI_tuptable;
        bool isnull;
        (void)finish_my(src);
        for (uint64 i = 0; i < processed; i++) {
            elog(LOG, "i=%lu", i);
            (void)launch_task(SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, 1, &isnull));
        }
    }
}

void loop(Datum main_arg) {
    elog(LOG, "loop started database=%s, username=%s", database, username);
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
        if (rc & WL_TIMEOUT) (void)assign();
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
    (void)DefineCustomIntVariable("pg_scheduler.period", "how often to run loop", NULL, &period, 1, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_scheduler.restart", "how often to restart loop", NULL, &worker.bgw_restart_time, 10, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_function_name, sizeof("loop"), "loop") != sizeof("loop") - 1) elog(FATAL, "snprintf");
    len = sizeof("%s %s pg_scheduler loop") - 1 + strlen(database) - 1 + strlen(username) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler loop", database, username) != len) elog(FATAL, "snprintf");
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler loop"), "pg_scheduler loop") != sizeof("pg_scheduler loop") - 1) elog(FATAL, "snprintf");
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    (void)RegisterBackgroundWorker(&worker);
}
