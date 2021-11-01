#include "include.h"

typedef struct DestReceiverMy {
    DestReceiver pub; // !!! always first !!!
    Task *task;
    uint64 row;
} DestReceiverMy;

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDescData *tupdesc, int fnumber) {
    bool isnull, typisvarlena;
    Datum val = slot_getattr(slot, fnumber, &isnull);
    Oid foutoid, oid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
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
        if (task->quote) appendStringInfoChar(&task->output, task->quote);
    }
}

static bool receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    DestReceiverMy *my = (DestReceiverMy *)self;
    Task *task = my->task;
    TupleDescData *typeinfo = slot->tts_tupleDescriptor;
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (task->header && !my->row && typeinfo->natts > 1) headers(typeinfo, task);
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

void NullCommandMy(Task *task) { }

#if (PG_VERSION_NUM >= 140000)
#include <dest.140000.c>
#elif (PG_VERSION_NUM >= 130000)
#include <dest.130000.c>
#elif (PG_VERSION_NUM >= 120000)
#include <dest.120000.c>
#elif (PG_VERSION_NUM >= 110000)
#include <dest.110000.c>
#elif (PG_VERSION_NUM >= 100000)
#include <dest.100000.c>
#elif (PG_VERSION_NUM >= 90000)
#include <dest.90000.c>
#endif
