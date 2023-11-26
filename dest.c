#include "include.h"
#include <unistd.h>

enum PIPES {READ, WRITE};
extern Task task;
static emit_log_hook_type emit_log_hook_prev = NULL;
static int stdout_fd;
static int stdout_pipe[2];

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    bool isnull;
    bool typisvarlena;
    Datum attr = slot_getattr(slot, fnumber, &isnull);
    Oid foutoid;
    if (isnull) return NULL;
    getTypeOutputInfo(TupleDescAttr(tupdesc, fnumber - 1)->atttypid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, attr);
}

static void headers(TupleDesc tupdesc) {
    if (task.output.len) appendStringInfoString(&task.output, "\n");
    for (int col = 1; col <= tupdesc->natts; col++) {
        if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
        appendBinaryStringInfoEscapeQuote(&task.output, SPI_fname(tupdesc, col), strlen(SPI_fname(tupdesc, col)), false, task.escape, task.quote);
    }
}

static
#if PG_VERSION_NUM >= 90600
bool
#else
void
#endif
receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    if (!task.output.data) initStringInfoMy(&task.output);
    if (task.header && !task.row && tupdesc->natts > 1) headers(tupdesc);
    if (task.output.len) appendStringInfoString(&task.output, "\n");
    for (int col = 1; col <= tupdesc->natts; col++) {
        char *value = SPI_getvalue_my(slot, tupdesc, col);
        if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
        if (!value) appendStringInfoString(&task.output, task.null); else {
            appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !init_oid_is_string(SPI_gettypeid(tupdesc, col)) && task.string, task.escape, task.quote);
            pfree(value);
        }
    }
    task.row++;
#if PG_VERSION_NUM >= 90600
    return true;
#endif
}

static void rStartup(DestReceiver *self, int operation, TupleDesc tupdesc) {
    switch (operation) {
        case CMD_UNKNOWN: elog(DEBUG1, "id = %li, operation = CMD_UNKNOWN", task.shared->id); break;
        case CMD_SELECT: elog(DEBUG1, "id = %li, operation = CMD_SELECT", task.shared->id); break;
        case CMD_UPDATE: elog(DEBUG1, "id = %li, operation = CMD_UPDATE", task.shared->id); break;
        case CMD_INSERT: elog(DEBUG1, "id = %li, operation = CMD_INSERT", task.shared->id); break;
        case CMD_DELETE: elog(DEBUG1, "id = %li, operation = CMD_DELETE", task.shared->id); break;
        case CMD_UTILITY: elog(DEBUG1, "id = %li, operation = CMD_UTILITY", task.shared->id); break;
        case CMD_NOTHING: elog(DEBUG1, "id = %li, operation = CMD_NOTHING", task.shared->id); break;
        default: elog(DEBUG1, "id = %li, operation = %i", task.shared->id, operation); break;
    }
    task.row = 0;
    task.skip = 1;
}

static void rShutdown(DestReceiver *self) {
    elog(DEBUG1, "id = %li", task.shared->id);
}

static void rDestroy(DestReceiver *self) {
    char buffer[PIPE_BUF];
    int nread;
    elog(DEBUG1, "id = %li", task.shared->id);
    if (fflush(stdout)) ereport(ERROR, (errcode_for_socket_access(), errmsg("fflush"), errdetail("%m")));
    if (dup2(stdout_fd, STDOUT_FILENO) < 0) ereport(ERROR, (errcode_for_file_access(), errmsg("dup2 < 0"), errdetail("%m")));
    if (close(stdout_fd) < 0) ereport(ERROR, (errcode_for_file_access(), errmsg("close < 0"), errdetail("%m")));
    while ((nread = read(stdout_pipe[READ], buffer, sizeof(buffer))) > 0) {
        if (!task.output.data) initStringInfoMy(&task.output);
        appendBinaryStringInfo(&task.output, buffer, nread);
        task.skip = 1;
    }
    if (close(stdout_pipe[READ]) < 0) ereport(ERROR, (errcode_for_file_access(), errmsg("close < 0"), errdetail("%m")));
}

static
#if PG_VERSION_NUM >= 120000
const
#endif
DestReceiver myDestReceiver = {
    .receiveSlot = receiveSlot,
    .rStartup = rStartup,
    .rShutdown = rShutdown,
    .rDestroy = rDestroy,
    .mydest = DestDebug,
};

static DestReceiver *CreateDestReceiverMy(CommandDest dest) {
    elog(DEBUG1, "id = %li", task.shared->id);
    if ((stdout_fd = dup(STDOUT_FILENO)) < 0) ereport(ERROR, (errcode_for_socket_access(), errmsg("dup < 0"), errdetail("%m")));
    if (pipe(stdout_pipe) < 0) ereport(ERROR, (errcode_for_socket_access(), errmsg("pipe < 0"), errdetail("%m")));
    if (fflush(stdout)) ereport(ERROR, (errcode_for_socket_access(), errmsg("fflush"), errdetail("%m")));
    if (dup2(stdout_pipe[WRITE], STDOUT_FILENO) < 0) ereport(ERROR, (errcode_for_file_access(), errmsg("dup2 < 0"), errdetail("%m")));
    if (close(stdout_pipe[WRITE]) < 0) ereport(ERROR, (errcode_for_file_access(), errmsg("close < 0"), errdetail("%m")));
#if PG_VERSION_NUM >= 120000
    return unconstify(DestReceiver *, &myDestReceiver);
#else
    return &myDestReceiver;
#endif
}

static void ReadyForQueryMy(CommandDest dest) {
    elog(DEBUG1, "id = %li", task.shared->id);
}

static void NullCommandMy(CommandDest dest) {
    elog(DEBUG1, "id = %li", task.shared->id);
}

#if PG_VERSION_NUM >= 130000
static void BeginCommandMy(CommandTag commandTag, CommandDest dest) {
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, GetCommandTagName(commandTag));
}

static void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output) {
    char completionTag[COMPLETION_TAG_BUFSIZE];
    CommandTag tag = qc->commandTag;
    const char *tagname = GetCommandTagName(tag);
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, qc->nprocessed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = %li, completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        if (!task.output.data) initStringInfoMy(&task.output);
        if (task.output.len) appendStringInfoString(&task.output, "\n");
        appendStringInfoString(&task.output, completionTag);
    }
}
#else
static void BeginCommandMy(const char *commandTag, CommandDest dest) {
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, commandTag);
}

static void EndCommandMy(const char *commandTag, CommandDest dest) {
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, commandTag);
    if (task.skip) task.skip = 0; else {
        if (!task.output.data) initStringInfoMy(&task.output);
        if (task.output.len) appendStringInfoString(&task.output, "\n");
        appendStringInfoString(&task.output, commandTag);
    }
}
#endif

#if PG_VERSION_NUM < 90500
#define PQArgBlock undef
#endif

#include <postgres.c>

void task_execute(void) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(MessageContext);
    MemoryContextResetAndDeleteChildren(MessageContext);
    InvalidateCatalogSnapshotConditionally();
    MemoryContextSwitchTo(oldMemoryContext);
    whereToSendOutput = DestDebug;
    ReadyForQueryMy(whereToSendOutput);
    SetCurrentStatementStartTimestamp();
    exec_simple_query(task.input);
    if (IsTransactionState()) exec_simple_query(SQL(COMMIT));
    if (IsTransactionState()) ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("still active sql transaction")));
}

void task_catch(void) {
    HOLD_INTERRUPTS();
    disable_all_timeouts(false);
    QueryCancelPending = false;
    emit_log_hook_prev = emit_log_hook;
    emit_log_hook = task_error;
    EmitErrorReport();
    debug_query_string = NULL;
    AbortOutOfAnyTransaction();
#if PG_VERSION_NUM >= 110000
    PortalErrorCleanup();
#endif
    if (MyReplicationSlot) ReplicationSlotRelease();
#if PG_VERSION_NUM >= 100000
    ReplicationSlotCleanup();
#endif
#if PG_VERSION_NUM >= 110000
    jit_reset_after_error();
#endif
    FlushErrorState();
    xact_started = false;
    RESUME_INTERRUPTS();
}
