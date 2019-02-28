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
    char *request = NULL;
    bool isnull;
    const char *src = "UPDATE task SET state = 'WORK' WHERE id = $1 RETURNING request";
    (void)connect_my(src);
    elog(LOG, "work src=%s", src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE_RETURNING");
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
//    request = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    request = strdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"), &isnull)));
    (void)finish_my(src);
    return request;
}

static inline void done(Datum main_arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {main_arg};
    const char *src = "UPDATE task SET state = 'DONE' WHERE id = $1";
    (void)connect_my(src);
    elog(LOG, "done src=%s", src);
    if (SPI_execute_with_args(src, 1, argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

static inline void fail(Datum main_arg, ErrorData *edata) {
    Oid argtypes[] = {
        INT4OID,
        BOOLOID,
        BOOLOID,
        BOOLOID,
        BOOLOID,
        BOOLOID,
        TEXTOID,
        INT4OID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        INT4OID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        TEXTOID,
        INT4OID,
        INT4OID,
        TEXTOID,
        INT4OID,
        INT8OID
    };
    Datum Values[] = {
        Int32GetDatum(edata->elevel),
        BoolGetDatum(edata->output_to_server),
        BoolGetDatum(edata->output_to_client),
        BoolGetDatum(edata->show_funcname),
        BoolGetDatum(edata->hide_stmt),
        BoolGetDatum(edata->hide_ctx),
        CStringGetTextDatum(edata->filename!=NULL?edata->filename:"<NULL>"),
        Int32GetDatum(edata->lineno),
        CStringGetTextDatum(edata->funcname!=NULL?edata->funcname:"<NULL>"),
        CStringGetTextDatum(edata->domain!=NULL?edata->domain:"<NULL>"),
        CStringGetTextDatum(edata->context_domain!=NULL?edata->context_domain:"<NULL>"),
        Int32GetDatum(edata->sqlerrcode),
        CStringGetTextDatum(edata->message!=NULL?edata->message:"<NULL>"),
        CStringGetTextDatum(edata->detail!=NULL?edata->detail:"<NULL>"),
        CStringGetTextDatum(edata->detail_log!=NULL?edata->detail_log:"<NULL>"),
        CStringGetTextDatum(edata->hint!=NULL?edata->hint:"<NULL>"),
        CStringGetTextDatum(edata->context!=NULL?edata->context:"<NULL>"),
        CStringGetTextDatum(edata->message_id!=NULL?edata->message_id:"<NULL>"),
        CStringGetTextDatum(edata->schema_name!=NULL?edata->schema_name:"<NULL>"),
        CStringGetTextDatum(edata->table_name!=NULL?edata->table_name:"<NULL>"),
        CStringGetTextDatum(edata->column_name!=NULL?edata->column_name:"<NULL>"),
        CStringGetTextDatum(edata->datatype_name!=NULL?edata->datatype_name:"<NULL>"),
        CStringGetTextDatum(edata->constraint_name!=NULL?edata->constraint_name:"<NULL>"),
        Int32GetDatum(edata->cursorpos),
        Int32GetDatum(edata->internalpos),
        CStringGetTextDatum(edata->internalquery!=NULL?edata->internalquery:"<NULL>"),
        Int32GetDatum(edata->saved_errno),
        main_arg
    };
    /*const char Nulls[] = {
        ' ',
        ' ',
        ' ',
        ' ',
        ' ',
        ' ',
        edata->filename!=NULL?' ':'n',
        ' ',
        edata->funcname!=NULL?' ':'n',
        edata->domain!=NULL?' ':'n',
        edata->context_domain!=NULL?' ':'n',
        ' ',
        edata->message!=NULL?' ':'n',
        edata->detail!=NULL?' ':'n',
//        edata->detail_log!=NULL?' ':'n',
//        edata->hint!=NULL?' ':'n',
//        edata->context!=NULL?' ':'n',
//        edata->message_id!=NULL?' ':'n',
//        edata->schema_name!=NULL?' ':'n',
//        edata->table_name!=NULL?' ':'n',
//        edata->column_name!=NULL?' ':'n',
//        edata->datatype_name!=NULL?' ':'n',
//        edata->constraint_name!=NULL?' ':'n',
//        ' ',
//        ' ',
//        edata->internalquery!=NULL?' ':'n',
//        ' ',
        ' '
    };*/
    const char *src = "UPDATE task SET state = 'FAIL', response='"
        "elevel\t'||$1::text||'\n"
        "output_to_server\t'||$2::text||'\n"
        "output_to_client\t'||$3::text||'\n"
        "show_funcname\t'||$4::text||'\n"
        "hide_stmt\t'||$5::text||'\n"
        "hide_ctx\t'||$6::text||'\n"
        "filename\t'||$7::text||'\n"
        "lineno\t'||$8::text||'\n"
        "funcname\t'||$9::text||'\n"
        "domain\t'||$10::text||'\n"
        "context_domain\t'||$11::text||'\n"
        "sqlerrcode\t'||$12::text||'\n"
        "message\t'||$13::text||'\n"
        "detail\t'||$14::text||'\n"
        "detail_log\t'||$15::text||'\n"
        "hint\t'||$16::text||'\n"
        "context\t'||$17::text||'\n"
        "message_id\t'||$18::text||'\n"
        "schema_name\t'||$19::text||'\n"
        "table_name\t'||$20::text||'\n"
        "column_name\t'||$21::text||'\n"
        "datatype_name\t'||$22::text||'\n"
        "constraint_name\t'||$23::text||'\n"
        "cursorpos\t'||$24::text||'\n"
        "internalpos\t'||$25::text||'\n"
        "internalquery\t'||$26::text||'\n"
        "saved_errno\t'||$27::text||'"
    "' WHERE id = $28";
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
    (void)connect_my(src);
//    elog(LOG, "fail src=%s", src);
    if (SPI_execute_with_args(src, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE");
    (void)finish_my(src);
}

static inline void execute(Datum main_arg) {
    char *src = work(main_arg);
//    int res;
//    MemoryContext oldcontext;
//    ResourceOwner oldowner;
//    elog(LOG, "src=%s", src);
    (void)connect_my((const char *)src); {
        MemoryContext oldcontext = CurrentMemoryContext;
        ResourceOwner oldowner = CurrentResourceOwner;
        elog(LOG, "execute src=%s", src);
        (void)BeginInternalSubTransaction(NULL);
        (MemoryContext)MemoryContextSwitchTo(oldcontext);
        PG_TRY(); {
//            elog(LOG, "SPI_execute=%i", SPI_execute(src, false, 0));
            uint64 processed;
            SPITupleTable *tuptable;
            bool isnull;
            switch (SPI_execute(src, false, 0)) {
                case SPI_OK_SELECT: elog(LOG, "SPI_OK_SELECT"); break;
                case SPI_OK_SELINTO: elog(LOG, "SPI_OK_SELINTO"); break;
                case SPI_OK_INSERT: elog(LOG, "SPI_OK_INSERT"); break;
                case SPI_OK_DELETE: elog(LOG, "SPI_OK_DELETE"); break;
                case SPI_OK_UPDATE: elog(LOG, "SPI_OK_UPDATE"); break;
                case SPI_OK_INSERT_RETURNING: elog(LOG, "SPI_OK_INSERT_RETURNING"); break;
                case SPI_OK_DELETE_RETURNING: elog(LOG, "SPI_OK_DELETE_RETURNING"); break;
                case SPI_OK_UPDATE_RETURNING: elog(LOG, "SPI_OK_UPDATE_RETURNING"); break;
                case SPI_OK_UTILITY: elog(LOG, "SPI_OK_UTILITY"); break;
                case SPI_OK_REWRITTEN: elog(LOG, "SPI_OK_REWRITTEN"); break;
                case SPI_ERROR_ARGUMENT: elog(FATAL, "SPI_ERROR_ARGUMENT"); break;
                case SPI_ERROR_COPY: elog(FATAL, "SPI_ERROR_COPY"); break;
                case SPI_ERROR_TRANSACTION: elog(FATAL, "SPI_ERROR_TRANSACTION"); break;
                case SPI_ERROR_OPUNKNOWN: elog(FATAL, "SPI_ERROR_OPUNKNOWN"); break;
                case SPI_ERROR_UNCONNECTED: elog(FATAL, "SPI_ERROR_UNCONNECTED"); break;
                default: elog(FATAL, "SPI_execute");
            }
            processed = SPI_processed;
            tuptable = SPI_tuptable;
//            (int)SPI_execute((const char *)src, false, 0);
//            elog(LOG, "execute res=%i", res);
            (void)ReleaseCurrentSubTransaction();
            (MemoryContext)MemoryContextSwitchTo(oldcontext);
            CurrentResourceOwner = oldowner;
            (void)finish_my((const char *)src);
            if (tuptable != NULL) {
                for (uint64 row = 0; row < processed; row++) {
                    for (int col = 1; col <= tuptable->tupdesc->natts; col++) {
                        elog(LOG, "row=%lu, col=%i, p=%lu", row, col, SPI_getbinval(tuptable->vals[row], tuptable->tupdesc, col, &isnull));
                    }
                }
            }
            (void)done(main_arg);
        } PG_CATCH(); {
            ErrorData *edata;
            (MemoryContext)MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            (void)FlushErrorState();
            (void)RollbackAndReleaseCurrentSubTransaction();
            (MemoryContext)MemoryContextSwitchTo(oldcontext);
            CurrentResourceOwner = oldowner;
            (void)finish_my(src);
            (void)fail(main_arg, edata);
            (void)FreeErrorData(edata);
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
//        (void)SPI_freetuptable(tuptable);
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
