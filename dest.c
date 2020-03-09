#include "include.h"

typedef struct DestReceiverMy {
    DestReceiver pub; // !!! always first !!!
    Task *task;
    uint64 row;
} DestReceiverMy;

static Oid SPI_gettypeid_my(TupleDesc tupdesc, int fnumber) {
    if (fnumber > tupdesc->natts || !fnumber || fnumber <= FirstLowInvalidHeapAttributeNumber) E("SPI_ERROR_NOATTRIBUTE");
    return (fnumber > 0 ? TupleDescAttr(tupdesc, fnumber - 1) : SystemAttributeDefinition(fnumber))->atttypid;
}

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    Oid foutoid, oid = SPI_gettypeid_my(tupdesc, fnumber);
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
    if (!my->row && typeinfo->natts > 1 && task->length == 1) {
        if (task->response.len) appendStringInfoString(&task->response, "\n");
        for (int col = 1; col <= typeinfo->natts; col++) {
            if (col > 1) appendStringInfoString(&task->response, "\t");
            appendStringInfoString(&task->response, SPI_fname(typeinfo, col));
            if (task->append) appendStringInfo(&task->response, "::%s", SPI_gettype(typeinfo, col));
        }
    }
    if (task->response.len) appendStringInfoString(&task->response, "\n");
    for (int col = 1; col <= typeinfo->natts; col++) {
        char *value = SPI_getvalue_my(slot, typeinfo, col);
        if (col > 1) appendStringInfoString(&task->response, "\t");
        appendStringInfoString(&task->response, value ? value : "(null)");
        if (value) pfree(value);
    }
    my->row++;
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    my->row = 0;
    if (task->length > 1) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        if (!task->response.data) initStringInfo(&task->response);
        MemoryContextSwitchTo(oldMemoryContext);
        if (task->response.len) appendStringInfoString(&task->response, "\n");
        for (int col = 1; col <= typeinfo->natts; col++) {
            if (col > 1) appendStringInfoString(&task->response, "\t");
            appendStringInfoString(&task->response, SPI_fname(typeinfo, col));
            if (task->append) appendStringInfo(&task->response, "::%s", SPI_gettype(typeinfo, col));
        }
    }
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
