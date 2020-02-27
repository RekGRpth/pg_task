#include "include.h"

typedef struct DestReceiverMy {
    DestReceiver pub;
    StringInfoData *response;
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
    StringInfoData *response = ((DestReceiverMy *)self)->response;
    if (!response->data) {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        initStringInfo(response);
        MemoryContextSwitchTo(oldMemoryContext);
    }
    if (response->len) appendStringInfoString(response, "\n");
    if (response->len || slot->tts_tupleDescriptor->natts > 1) {
        for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
            if (col > 1) appendStringInfoString(response, "\t");
            appendStringInfo(response, "%s::%s", SPI_fname(slot->tts_tupleDescriptor, col), SPI_gettype(slot->tts_tupleDescriptor, col));
        }
    }
    if (response->len) appendStringInfoString(response, "\n");
    for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
        char *value = SPI_getvalue_my(slot, slot->tts_tupleDescriptor, col);
        if (col > 1) appendStringInfoString(response, "\t");
        appendStringInfoString(response, value ? value : "(null)");
        if (value) pfree(value);
    }
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) { }

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) { }

DestReceiver *CreateDestReceiverMy(StringInfoData *response) {
    DestReceiverMy *self = (DestReceiverMy *)palloc0(sizeof(*self));
    self->pub.receiveSlot = receiveSlot;
    self->pub.rStartup = rStartup;
    self->pub.rShutdown = rShutdown;
    self->pub.rDestroy = rDestroy;
    self->pub.mydest = DestDebug;
    self->response = response;
    return (DestReceiver *)self;
}
