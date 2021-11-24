#include "include.h"

extern Task *task;

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    bool isnull, typisvarlena;
    Datum val = slot_getattr(slot, fnumber, &isnull);
    Oid foutoid, oid = TupleDescAttr(tupdesc, fnumber - 1)->atttypid;
    if (isnull) return NULL;
    getTypeOutputInfo(oid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, val);
}

static void headers(TupleDesc typeinfo) {
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
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (task->header && !task->row && typeinfo->natts > 1) headers(typeinfo);
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
    task->row++;
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    task->row = 0;
    task->skip = 1;
}

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) {
    pfree(self);
}

DestReceiver *CreateDestReceiverMy(CommandDest dest) {
    DestReceiver *self = MemoryContextAllocZero(TopMemoryContext, sizeof(*self));
    self->receiveSlot = receiveSlot;
    self->rStartup = rStartup;
    self->rShutdown = rShutdown;
    self->rDestroy = rDestroy;
    self->mydest = dest;
    return self;
}

void ReadyForQueryMy(CommandDest dest) { }

void NullCommandMy(CommandDest dest) { }

#if PG_VERSION_NUM >= 140000
#include <dest.14.c>
#elif PG_VERSION_NUM >= 130000
#include <dest.13.c>
#elif PG_VERSION_NUM >= 120000
#include <dest.12.c>
#elif PG_VERSION_NUM >= 110000
#include <dest.11.c>
#elif PG_VERSION_NUM >= 100000
#include <dest.10.c>
#elif PG_VERSION_NUM >= 90000
#include <dest.9.c>
#endif
