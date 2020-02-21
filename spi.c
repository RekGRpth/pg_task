#include "include.h"

extern MemoryContext myMemoryContext;
extern StringInfoData response;

void SPI_start_transaction_my(const char *command) {
    SPI_start_transaction();
    if (StatementTimeout > 0) enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout); else disable_timeout(STATEMENT_TIMEOUT, false);
    pgstat_report_activity(STATE_RUNNING, command);
}

void SPI_connect_my(const char *command) {
    int rc;
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) E("SPI_connect_ext = %s", SPI_result_code_string(rc));
    SPI_start_transaction_my(command);
}

void SPI_commit_my(const char *command) {
    disable_timeout(STATEMENT_TIMEOUT, false);
    SPI_commit();
    ProcessCompletedNotifies();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

void SPI_finish_my(const char *command) {
    int rc;
    if ((rc = SPI_finish()) != SPI_OK_FINISH) E("SPI_finish = %s", SPI_result_code_string(rc));
}

char *SPI_getvalue_my(HeapTuple tuple, TupleDesc tupdesc, int fnumber) {
    bool isnull;
    Datum datum = SPI_getbinval(tuple, tupdesc, fnumber, &isnull);
    if (isnull) return NULL;
    return TextDatumGetCString(datum);
}

SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes) {
    int rc;
    SPIPlanPtr plan;
    if (!(plan = SPI_prepare(src, nargs, argtypes))) E("SPI_prepare = %s", SPI_result_code_string(SPI_result));
    if ((rc = SPI_keepplan(plan))) E("SPI_keepplan = %s", SPI_result_code_string(rc));
    return plan;
}

void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res) {
    int rc;
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != res) E("SPI_execute_plan = %s", SPI_result_code_string(rc));
}

void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res){
    int rc;
    if ((rc = SPI_execute_with_args(src, nargs, argtypes, values, nulls, false, 0)) != res) E("SPI_execute_with_args = %s", SPI_result_code_string(rc));
}

void SPI_rollback_my(const char *command) {
    disable_timeout(STATEMENT_TIMEOUT, false);
    EmitErrorReport();
    AbortCurrentTransaction();
    FlushErrorState();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

static const char *SPI_fname_my(TupleDesc tupdesc, int fnumber) {
    if (fnumber > tupdesc->natts || !fnumber || fnumber <= FirstLowInvalidHeapAttributeNumber) E("SPI_ERROR_NOATTRIBUTE");
    return NameStr((fnumber > 0 ? TupleDescAttr(tupdesc, fnumber - 1) : SystemAttributeDefinition(fnumber))->attname);
}

static char *SPI_getvalue_my2(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    Datum val;
    bool isnull;
    Oid foutoid;
    bool typisvarlena;
    if (fnumber > tupdesc->natts || !fnumber || fnumber <= FirstLowInvalidHeapAttributeNumber) E("SPI_ERROR_NOATTRIBUTE");
    val = slot_getattr(slot, fnumber, &isnull);
    if (isnull) return NULL;
    getTypeOutputInfo(fnumber > 0 ? TupleDescAttr(tupdesc, fnumber - 1)->atttypid : (SystemAttributeDefinition(fnumber))->atttypid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, val);
}

static const char *SPI_gettype_my(TupleDesc tupdesc, int fnumber) {
    HeapTuple typeTuple;
    const char *result;
    if (fnumber > tupdesc->natts || !fnumber || fnumber <= FirstLowInvalidHeapAttributeNumber) E("SPI_ERROR_NOATTRIBUTE");
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fnumber > 0 ? TupleDescAttr(tupdesc, fnumber - 1)->atttypid : (SystemAttributeDefinition(fnumber))->atttypid));
    if (!HeapTupleIsValid(typeTuple)) E("SPI_ERROR_TYPUNKNOWN");
    result = NameStr(((Form_pg_type)GETSTRUCT(typeTuple))->typname);
    ReleaseSysCache(typeTuple);
    return result;
}

static bool receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(myMemoryContext);
    if (!response.data) initStringInfo(&response);
    if (!response.len && slot->tts_tupleDescriptor->natts > 1) {
        for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
            if (col > 1) appendStringInfoString(&response, "\t");
            appendStringInfo(&response, "%s::%s", SPI_fname_my(slot->tts_tupleDescriptor, col), SPI_gettype_my(slot->tts_tupleDescriptor, col));
        }
    }
    if (response.len) appendStringInfoString(&response, "\n");
    for (int col = 1; col <= slot->tts_tupleDescriptor->natts; col++) {
        const char *value = SPI_getvalue_my2(slot, slot->tts_tupleDescriptor, col);
        if (col > 1) appendStringInfoString(&response, "\t");
        appendStringInfoString(&response, value ? value : "(null)");
        if (value) pfree((void *)value);
    }
    MemoryContextSwitchTo(oldMemoryContext);
    return true;
}

static void rStartup(DestReceiver *self, int operation, TupleDesc typeinfo) { }

static void rShutdown(DestReceiver *self) { }

static void rDestroy(DestReceiver *self) { }

static const DestReceiver DestReceiverMy = {.receiveSlot = receiveSlot, .rStartup = rStartup, .rShutdown = rShutdown, .rDestroy = rDestroy, .mydest = DestDebug};

DestReceiver *CreateDestReceiverMy(CommandDest dest) {
    return dest == DestDebug ? unconstify(DestReceiver *, &DestReceiverMy) : CreateDestReceiver(dest);
}
