#include "include.h"

typedef struct DestReceiverMy {
    DestReceiver pub; // !!! always first !!!
    Task *task;
    uint64 row;
} DestReceiverMy;

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDescData *tupdesc, int fnumber) {
    Oid foutoid, oid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
    bool isnull, typisvarlena;
    Datum val = slot_getattr(slot, fnumber, &isnull);
    if (isnull) return NULL;
    getTypeOutputInfo(oid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, val);
}

static void headers(TupleDescData *typeinfo, Task *task) {
    if (task->output.len) appendStringInfoString(&task->output, "\n");
    for (int col = 1; col <= typeinfo->natts; col++) {
        const char *value = SPI_fname(typeinfo, col);
        if (col > 1) appendStringInfoChar(&task->output, task->delimiter);
        if (task->quote) appendStringInfoChar(&task->output, task->quote);
        if (task->escape) init_escape(&task->output, value, strlen(value), task->escape);
        else appendStringInfoString(&task->output, value);
        if (task->append) {
            const char *type = SPI_gettype(typeinfo, col);
            if (task->escape) init_escape(&task->output, "::", sizeof("::") - 1, task->escape);
            else appendStringInfoString(&task->output, "::");
            if (task->escape) init_escape(&task->output, type, strlen(type), task->escape);
            else appendStringInfoString(&task->output, type);
        }
        if (task->quote) appendStringInfoChar(&task->output, task->quote);
    }
}

static bool receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    TupleDescData *typeinfo = slot->tts_tupleDescriptor;
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (task->header && !my->row && typeinfo->natts > 1 && task->length == 1) headers(typeinfo, task);
    if (task->output.len) appendStringInfoString(&task->output, "\n");
    for (int col = 1; col <= typeinfo->natts; col++) {
        char *value = SPI_getvalue_my(slot, typeinfo, col);
        int len = value ? strlen(value) : 0;
        if (col > 1) appendStringInfoChar(&task->output, task->delimiter);
        if (!value) appendStringInfoString(&task->output, task->null); else {
            if (!init_oid_is_string(SPI_gettypeid(typeinfo, col)) && task->string) {
                if (len) appendStringInfoString(&task->output, value);
            } else {
                if (task->quote) appendStringInfoChar(&task->output, task->quote);
                if (len) {
                    if (task->escape) init_escape(&task->output, value, len, task->escape);
                    else appendStringInfoString(&task->output, value);
                }
                if (task->quote) appendStringInfoChar(&task->output, task->quote);
            }
        }
        if (value) pfree(value);
    }
    my->row++;
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDescData *typeinfo) {
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    if (task->header && task->length > 1) {
        if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
        headers(typeinfo, task);
    }
    my->row = 0;
    task->skip = 1;
}

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) {
    pfree(self);
}

DestReceiver *CreateDestReceiverMy(Task *task) {
    DestReceiverMy *self = (DestReceiverMy *)MemoryContextAllocZero(TopMemoryContext, sizeof(*self));
    self->pub.receiveSlot = receiveSlot;
    self->pub.rStartup = rStartup;
    self->pub.rShutdown = rShutdown;
    self->pub.rDestroy = rDestroy;
    self->pub.mydest = DestDebug;
    self->task = task;
    return (DestReceiver *)self;
}

void ReadyForQueryMy(Task *task) { }

void BeginCommandMy(CommandTag commandTag, Task *task) {
    D1("%li: %s", task->id, GetCommandTagName(commandTag));
}

void NullCommandMy(Task *task) { }

void EndCommandMy(const QueryCompletion *qc, Task *task, bool force_undecorated_output) {
    char completionTag[COMPLETION_TAG_BUFSIZE];
    CommandTag tag = qc->commandTag;
    const char *tagname = GetCommandTagName(tag);
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, qc->nprocessed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    D1("%li: %s", task->id, completionTag);
    if (task->skip) task->skip = 0; else {
        if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
        if (task->output.len) appendStringInfoString(&task->output, "\n");
        appendStringInfoString(&task->output, completionTag);
    }
}
