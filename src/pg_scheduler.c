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

#include "utils/builtins.h"
#include "utils/lsyscache.h"

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
//    elog(LOG, "connect_my cmd_str=%s", cmd_str);
    (void)pgstat_report_activity(STATE_RUNNING, cmd_str);
    (void)SetCurrentStatementStartTimestamp();
    (void)StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) elog(FATAL, "SPI_connect != SPI_OK_CONNECT");
    (void)PushActiveSnapshot(GetTransactionSnapshot());
}

static inline void finish_my(const char *cmd_str) {
//    elog(LOG, "finish_my cmd_str=%s", cmd_str);
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
    const char *src = "UPDATE task SET state = 'WORK', start = now() WHERE id = $1 RETURNING request";
    char *data;
    (void)connect_my(src);
    elog(LOG, "work src=%s", src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE_RETURNING");
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    data = strdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request")));
    (void)finish_my(src);
    return data;
}

static inline void done(Datum main_arg, const char *data) {
    Oid argtypes[] = {TEXTOID, INT8OID};
    Datum Values[] = {CStringGetTextDatum(data!=NULL?data:"(null)"), main_arg};
    const char *src = "UPDATE task SET state = 'DONE', stop = now(), response=$1 WHERE id = $2";
    (void)connect_my(src);
    elog(LOG, "done src=%s", src);
    if (SPI_execute_with_args(src, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

static inline void fail(Datum main_arg, const char *data) {
    Oid argtypes[] = {TEXTOID, INT8OID};
    Datum Values[] = {CStringGetTextDatum(data!=NULL?data:"(null)"), main_arg};
    const char *src = "UPDATE task SET state = 'FAIL', stop = now(), response=$1 WHERE id = $2";
    (void)connect_my(src);
    elog(LOG, "fail src=%s", src);
    if (SPI_execute_with_args(src, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

static inline char *success() {
    StringInfoData buf;
    (void)initStringInfo(&buf);
    if ((SPI_tuptable != NULL) && (SPI_processed > 0)) {
        for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
            char *name = SPI_fname(SPI_tuptable->tupdesc, col);
            char *type = SPI_gettype(SPI_tuptable->tupdesc, col);
            (void)appendStringInfo(&buf, "%s::%s", name, type);
            if (col > 1) (void)appendStringInfoString(&buf, "\t");
            if (name != NULL) (void)pfree(name);
            if (type != NULL) (void)pfree(type);
        }
        (void)appendStringInfoString(&buf, "\n");
        for (uint64 row = 0; row < SPI_processed; row++) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                (void)appendStringInfo(&buf, "%s", value);
                if (col > 1) (void)appendStringInfoString(&buf, "\t");
                if (value != NULL) (void)pfree(value);
            }
            if (row < SPI_processed - 1) (void)appendStringInfoString(&buf, "\n");
        }
        elog(LOG, "success\n%s", buf.data);
    }
    return buf.data;
}

static inline char *error() {
    ErrorData *edata = CopyErrorData();
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf,
        "elevel::int4\t%i\n"
        "output_to_server::bool\t%s\n"
        "output_to_client::bool\t%s\n"
        "show_funcname::bool\t%s\n"
        "hide_stmt::bool\t%s\n"
        "hide_ctx::bool\t%s\n"
        "filename::text\t%s\n"
        "lineno::int4\t%i\n"
        "funcname::text\t%s\n"
        "domain::text\t%s\n"
        "context_domain::text\t%s\n"
        "sqlerrcode::int4\t%i\n"
        "message::text\t%s\n"
        "detail::text\t%s\n"
        "detail_log::text\t%s\n"
        "hint::text\t%s\n"
        "context::text\t%s\n"
        "message_id::text\t%s\n"
        "schema_name::text\t%s\n"
        "table_name::text\t%s\n"
        "column_name::text\t%s\n"
        "datatype_name::text\t%s\n"
        "constraint_name::text\t%s\n"
        "cursorpos::int4\t%i\n"
        "internalpos::int4\t%i\n"
        "internalquery::text\t%s\n"
        "saved_errno::int4\t%i",
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
    elog(LOG, "error\n%s", buf.data);
    return buf.data;
}

static inline void execute(Datum main_arg) {
    char *src = work(main_arg);
//    elog(LOG, "src=%s", src);
    (void)connect_my(src); {
        MemoryContext oldcontext = CurrentMemoryContext;
//        ResourceOwner oldowner = CurrentResourceOwner;
        elog(LOG, "execute src=%s", src);
        (void)BeginInternalSubTransaction("execute");
        (MemoryContext)MemoryContextSwitchTo(oldcontext);
        PG_TRY(); {
//            elog(LOG, "execute try finish_my 1 src=%s", src);
            if (SPI_execute(src, false, 0) < 0) elog(FATAL, "SPI_execute < 0"); else {
                char *data = success();
//                elog(LOG, "execute try finish_my 2 src=%s", src);
                (void)ReleaseCurrentSubTransaction();
                (MemoryContext)MemoryContextSwitchTo(oldcontext);
//                CurrentResourceOwner = oldowner;
//                elog(LOG, "execute try finish_my 3 src=%s", src);
                (void)finish_my(src);
                (void)done(main_arg, data);
                if (data != NULL) (void)pfree(data);
            }
        } PG_CATCH(); {
            (MemoryContext)MemoryContextSwitchTo(oldcontext); {
                char *data = error();
                (void)FlushErrorState();
                (void)RollbackAndReleaseCurrentSubTransaction();
                (MemoryContext)MemoryContextSwitchTo(oldcontext);
//                CurrentResourceOwner = oldowner;
//                elog(LOG, "execute catch finish_my src=%s", src);
                (void)finish_my(src);
                (void)fail(main_arg, data);
                if (data != NULL) (void)pfree(data);
            }
        } PG_END_TRY();
    }
    if (src != NULL) (void)free(src);
}

void task(Datum main_arg) {
    elog(LOG, "task started id=%li", DatumGetInt64(main_arg));
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)execute(main_arg);
}

static inline void assign() {
    const char *src = "UPDATE task SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id";
    (void)connect_my(src);
//    elog(LOG, "assign src=%s", src);
    if (SPI_execute(src, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute != SPI_OK_UPDATE_RETURNING"); else {
        uint64 processed = SPI_processed;
        SPITupleTable *tuptable = SPI_tuptable;
        bool isnull;
        (void)finish_my(src);
        for (uint64 row = 0; row < processed; row++) {
            elog(LOG, "row=%lu", row);
            (void)launch_task(SPI_getbinval(tuptable->vals[row], tuptable->tupdesc, SPI_fnumber(tuptable->tupdesc, "id"), &isnull));
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
