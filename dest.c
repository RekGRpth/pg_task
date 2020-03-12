#include "include.h"

typedef struct DestReceiverMy {
    DestReceiver pub; // !!! always first !!!
    Task *task;
    uint64 row;
} DestReceiverMy;

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    Oid foutoid, oid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
    bool isnull, typisvarlena;
    Datum val = slot_getattr(slot, fnumber, &isnull);
    if (isnull) return NULL;
    getTypeOutputInfo(oid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, val);
}

static bool receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    if (!task->response.data) initStringInfo(&task->response);
    MemoryContextSwitchTo(oldMemoryContext);
    if (task->header && !my->row && (typeinfo->natts > 1 || task->length > 1)) {
        if (task->response.len) appendStringInfoString(&task->response, "\n");
        for (int col = 1; col <= typeinfo->natts; col++) {
            const char *value = SPI_fname(typeinfo, col);
            if (col > 1) appendStringInfoChar(&task->response, task->delimiter);
            if (task->quote) appendStringInfoChar(&task->response, task->quote);
            if (task->escape) init_escape(&task->response, value, strlen(value), task->escape);
            else appendStringInfoString(&task->response, value);
            if (task->append) {
                const char *type = SPI_gettype(typeinfo, col);
                if (task->escape) init_escape(&task->response, "::", sizeof("::") - 1, task->escape);
                else appendStringInfoString(&task->response, "::");
                if (task->escape) init_escape(&task->response, type, strlen(type), task->escape);
                else appendStringInfoString(&task->response, type);
            }
            if (task->quote) appendStringInfoChar(&task->response, task->quote);
        }
    }
    if (task->response.len) appendStringInfoString(&task->response, "\n");
    for (int col = 1; col <= typeinfo->natts; col++) {
        char *value = SPI_getvalue_my(slot, typeinfo, col);
        int len = value ? strlen(value) : 0;
        if (col > 1) appendStringInfoChar(&task->response, task->delimiter);
        if (!value) appendStringInfoString(&task->response, task->null); else {
            if (!init_oid_is_string(SPI_gettypeid(typeinfo, col)) && task->string) {
                if (len) appendStringInfoString(&task->response, value);
            } else {
                if (task->quote) appendStringInfoChar(&task->response, task->quote);
                if (len) {
                    if (task->escape) init_escape(&task->response, value, len, task->escape);
                    else appendStringInfoString(&task->response, value);
                }
                if (task->quote) appendStringInfoChar(&task->response, task->quote);
            }
        }
        if (value) pfree(value);
    }
    my->row++;
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    my->row = 0;
    /*if (task->header && task->length > 1) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        if (!task->response.data) initStringInfo(&task->response);
        MemoryContextSwitchTo(oldMemoryContext);
        if (task->response.len) appendStringInfoString(&task->response, "\n");
        for (int col = 1; col <= typeinfo->natts; col++) {
            if (col > 1) appendStringInfoChar(&task->response, task->delimiter);
            if (task->quote) appendStringInfoChar(&task->response, task->quote);
            appendStringInfoString(&task->response, SPI_fname(typeinfo, col));
            if (task->append) appendStringInfo(&task->response, "::%s", SPI_gettype(typeinfo, col));
            if (task->quote) appendStringInfoChar(&task->response, task->quote);
        }
    }*/
    task->skip = 1;
}

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) { }

DestReceiver *CreateDestReceiverMy(Task *task) {
    DestReceiverMy *self = (DestReceiverMy *)palloc0(sizeof(*self));
    self->pub.receiveSlot = receiveSlot;
    self->pub.rStartup = rStartup;
    self->pub.rShutdown = rShutdown;
    self->pub.rDestroy = rDestroy;
    self->pub.mydest = DestDebug;
    self->task = task;
    return (DestReceiver *)self;
}

void ReadyForQueryMy(Task *task) { }

void BeginCommandMy(const char *commandTag, Task *task) {
    L(commandTag);
}

void NullCommandMy(Task *task) { }

void EndCommandMy(const char *commandTag, Task *task) {
    L(commandTag);
    if (task->skip) task->skip = 0; else {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        if (!task->response.data) initStringInfo(&task->response);
        MemoryContextSwitchTo(oldMemoryContext);
        if (task->response.len) appendStringInfoString(&task->response, "\n");
        appendStringInfoString(&task->response, commandTag);
    }
}
