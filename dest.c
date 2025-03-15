#include "include.h"

#include <unistd.h>
#include <utils/lsyscache.h>

#ifndef MemoryContextResetAndDeleteChildren
#define MemoryContextResetAndDeleteChildren(ctx) MemoryContextReset(ctx)
#endif

static Task task = {0};

Task *get_task(void) {
    return &task;
}

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
    elog(DEBUG1, "id = %li", task.shared->id);
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
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 %lu" : "%s %lu", tagname, qc->nprocessed);
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

#include "postgres.c"

static void dest_execute(void) {
    if (!task.shared->spi) {
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
    } else {
        bool count = false;
        bool insert = false;
        char completionTag[COMPLETION_TAG_BUFSIZE];
        int rc = SPI_execute(task.input, false, 0);
        const char *tagname = SPI_result_code_string(rc) + sizeof("SPI_OK_") - 1;
        switch (rc) {
            case SPI_ERROR_ARGUMENT: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_ARGUMENT"))); break;
            case SPI_ERROR_COPY: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_COPY"))); break;
            case SPI_ERROR_OPUNKNOWN: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_OPUNKNOWN"))); break;
            case SPI_ERROR_TRANSACTION: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPI_ERROR_TRANSACTION"))); break;
            case SPI_OK_DELETE: count = true; break;
            case SPI_OK_DELETE_RETURNING: count = true; break;
            case SPI_OK_INSERT: count = true; insert = true; break;
            case SPI_OK_INSERT_RETURNING: count = true; insert = true; break;
            case SPI_OK_SELECT: count = true; task.skip = 1; break;
            case SPI_OK_UPDATE: count = true; break;
            case SPI_OK_UPDATE_RETURNING: count = true; break;
        }
        elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, tagname);
        if (SPI_tuptable) for (uint64 row = 0; row < SPI_processed; row++) {
            task.skip = 1;
            if (!task.output.data) initStringInfoMy(&task.output);
            if (task.header && !row && SPI_tuptable->tupdesc->natts > 1) {
                if (task.output.len) appendStringInfoString(&task.output, "\n");
                for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                    if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
                    appendBinaryStringInfoEscapeQuote(&task.output, SPI_fname(SPI_tuptable->tupdesc, col), strlen(SPI_fname(SPI_tuptable->tupdesc, col)), false, task.escape, task.quote);
                }
            }
            if (task.output.len) appendStringInfoString(&task.output, "\n");
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                if (col > 1) appendStringInfoChar(&task.output, task.delimiter);
                if (!value) appendStringInfoString(&task.output, task.null); else {
                    appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !init_oid_is_string(SPI_gettypeid(SPI_tuptable->tupdesc, col)) && task.string, task.escape, task.quote);
                    pfree(value);
                }
            }
        }
        if (count) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, insert ? "%s 0 %lu" : "%s %lu", tagname, (long)SPI_processed);
        else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
        elog(DEBUG1, "id = %li, completionTag = %s", task.shared->id, completionTag);
        if (task.skip) task.skip = 0; else {
            if (!task.output.data) initStringInfoMy(&task.output);
            if (task.output.len) appendStringInfoString(&task.output, "\n");
            appendStringInfoString(&task.output, completionTag);
        }
    }
}

static void dest_catch(void) {
    if (!task.shared->spi) {
        HOLD_INTERRUPTS();
        disable_all_timeouts(false);
        QueryCancelPending = false;
    }
    EmitErrorReport();
    if (!task.shared->spi) {
        debug_query_string = NULL;
        AbortOutOfAnyTransaction();
#if PG_VERSION_NUM >= 110000
        PortalErrorCleanup();
#endif
        if (MyReplicationSlot) ReplicationSlotRelease();
#if PG_VERSION_NUM >= 170000
        ReplicationSlotCleanup(false);
#elif PG_VERSION_NUM >= 100000
        ReplicationSlotCleanup();
#endif
#if PG_VERSION_NUM >= 110000
        jit_reset_after_error();
#endif
        MemoryContextSwitchTo(TopMemoryContext);
    }
    FlushErrorState();
    if (!task.shared->spi) {
        xact_started = false;
        RESUME_INTERRUPTS();
    }
}

bool dest_timeout(void) {
    int StatementTimeoutMy = StatementTimeout;
    if (task_work(&task)) return true;
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task.shared->id, task.timeout, task.input, task.count);
    set_ps_display_my("timeout");
    StatementTimeout = task.timeout;
    if (task.shared->spi) {
        SPI_connect_my(task.input);
        BeginInternalSubTransaction(NULL);
    }
    PG_TRY();
        dest_execute();
        if (task.shared->spi) ReleaseCurrentSubTransaction();
    PG_CATCH();
        task_error(&task);
        dest_catch();
        if (task.shared->spi) {
            RollbackAndReleaseCurrentSubTransaction();
#if PG_VERSION_NUM < 100000
            SPI_restore_connection();
#endif
        }
    PG_END_TRY();
    if (task.shared->spi) SPI_finish_my();
    StatementTimeout = StatementTimeoutMy;
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    set_ps_display_my("idle");
    return task_done(&task);
}
