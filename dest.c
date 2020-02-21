#include "include.h"

static Task *task;

static Oid SPI_gettypeid_my(TupleDesc tupdesc, int fnumber) {
    if (fnumber > tupdesc->natts || !fnumber || fnumber <= FirstLowInvalidHeapAttributeNumber) E("SPI_ERROR_NOATTRIBUTE");
    return (fnumber > 0 ? TupleDescAttr(tupdesc, fnumber - 1) : SystemAttributeDefinition(fnumber))->atttypid;
}

static char *SPI_getvalue_my2(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    Oid oid = SPI_gettypeid_my(tupdesc, fnumber);
    bool isnull;
    Oid foutoid;
    bool typisvarlena;
    Datum val = slot_getattr(slot, fnumber, &isnull);
    if (isnull) return NULL;
    getTypeOutputInfo(oid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, val);
}

static bool receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    Work *work = task->work;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(work->context);
    StringInfoData *buf = &task->response;
    task->state = "DONE";
    if (!buf->data) initStringInfo(buf);
    if (!buf->len && slot->tts_tupleDescriptor->natts > 1) {
        for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
            if (col > 1) appendStringInfoString(buf, "\t");
            appendStringInfo(buf, "%s::%s", SPI_fname(slot->tts_tupleDescriptor, col), SPI_gettype(slot->tts_tupleDescriptor, col));
        }
    }
    if (buf->len) appendStringInfoString(buf, "\n");
    for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
        char *value = SPI_getvalue_my2(slot, slot->tts_tupleDescriptor, col);
        if (col > 1) appendStringInfoString(buf, "\t");
        appendStringInfoString(buf, value ? value : "(null)");
        if (value) pfree(value);
    }
    MemoryContextSwitchTo(oldMemoryContext);
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) { }

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) { }

static const DestReceiver DestReceiverMy = {.receiveSlot = receiveSlot, .rStartup = rStartup, .rShutdown = rShutdown, .rDestroy = rDestroy, .mydest = DestDebug};

DestReceiver *CreateDestReceiverMy(CommandDest dest, Task *_task) {
    task = _task;
    return dest == DestDebug ? unconstify(DestReceiver *, &DestReceiverMy) : CreateDestReceiver(dest);
}
