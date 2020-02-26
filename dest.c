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
    if (!task->response.data) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        initStringInfo(&task->response);
        MemoryContextSwitchTo(oldMemoryContext);
    }
    if (!task->response.len && slot->tts_tupleDescriptor->natts > 1) {
        for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
            if (col > 1) appendStringInfoString(&task->response, "\t");
            appendStringInfo(&task->response, "%s::%s", SPI_fname(slot->tts_tupleDescriptor, col), SPI_gettype(slot->tts_tupleDescriptor, col));
        }
    }
    if (task->response.len) appendStringInfoString(&task->response, "\n");
    for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
        char *value = SPI_getvalue_my2(slot, slot->tts_tupleDescriptor, col);
        if (col > 1) appendStringInfoString(&task->response, "\t");
        appendStringInfoString(&task->response, value ? value : "(null)");
        if (value) pfree(value);
    }
    task->success = true;
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
