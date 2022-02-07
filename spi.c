#include "include.h"

Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null) {
    bool isnull;
    Datum datum = SPI_getbinval(tuple, tupdesc, SPI_fnumber(tupdesc, fname), &isnull);
    if (allow_null) return datum;
    if (isnull) ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("column \"%s\" must not be null", fname)));
    return datum;
}

SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes) {
    int rc;
    SPIPlanPtr plan;
    if (!(plan = SPI_prepare(src, nargs, argtypes))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed"), errdetail("%s", SPI_result_code_string(SPI_result)), errcontext("%s", src)));
    if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_keepplan failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    MemoryContextSwitchTo(TopMemoryContext);
    CurrentResourceOwner = AuxProcessResourceOwner;
    return plan;
}

void SPI_connect_my(const char *src) {
    int rc;
    pgstat_report_activity(STATE_RUNNING, src);
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    PushActiveSnapshot(GetTransactionSnapshot());
    StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
    MemoryContextSwitchTo(TopMemoryContext);
    CurrentResourceOwner = AuxProcessResourceOwner;
}

void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res) {
    int rc;
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_plan failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res))));
    MemoryContextSwitchTo(TopMemoryContext);
    CurrentResourceOwner = AuxProcessResourceOwner;
}

void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res) {
    int rc;
    if ((rc = SPI_execute_with_args(src, nargs, argtypes, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_with_args failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res)), errcontext("%s", src)));
    MemoryContextSwitchTo(TopMemoryContext);
    CurrentResourceOwner = AuxProcessResourceOwner;
}

void SPI_finish_my(void) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    PopActiveSnapshot();
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_finish failed"), errdetail("%s", SPI_result_code_string(rc))));
#if PG_VERSION_NUM < 150000
    ProcessCompletedNotifies();
#endif
    CommitTransactionCommand();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    MemoryContextSwitchTo(TopMemoryContext);
    CurrentResourceOwner = AuxProcessResourceOwner;
}

void SPI_tuptable_copy(HeapTuple **vals, TupleDesc *tupdesc) {
    *tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
    *vals = MemoryContextAlloc(TopMemoryContext, SPI_processed * sizeof(**vals));
    for (uint64 row = 0; row < SPI_processed; row++) (*vals)[row] = heap_copytuple(SPI_tuptable->vals[row]);
}

void SPI_tuptable_free(HeapTuple *vals, TupleDesc tupdesc) {
    for (uint64 row = 0; row < SPI_processed; row++) heap_freetuple(vals[row]);
    pfree(vals);
    FreeTupleDesc(tupdesc);
}
