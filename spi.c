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
    return plan;
}

void SPI_commit_my(void) {
#if PG_VERSION_NUM < 110000
    MemoryContext oldcontext = CurrentMemoryContext;
//    ResourceOwner oldowner = CurrentResourceOwner;
#endif
    disable_timeout(STATEMENT_TIMEOUT, false);
    PopActiveSnapshot();
#if PG_VERSION_NUM >= 110000
    SPI_commit();
#else
    ReleaseCurrentSubTransaction();
    MemoryContextSwitchTo(oldcontext);
//    CurrentResourceOwner = oldowner;
#endif
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

void SPI_connect_my(const char *src) {
    int rc;
#if PG_VERSION_NUM < 110000
    MemoryContext oldcontext = CurrentMemoryContext;
#endif
#if PG_VERSION_NUM >= 110000
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect_ext failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
#else
    StartTransactionCommand();
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
//    PushActiveSnapshot(GetTransactionSnapshot());
    MemoryContextSwitchTo(oldcontext);
#endif
    SPI_start_transaction_my(src);
}

void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res, bool commit) {
    int rc;
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_plan failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res))));
    if (commit) SPI_commit_my();
}

void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit) {
    int rc;
    if ((rc = SPI_execute_with_args(src, nargs, argtypes, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_with_args failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res)), errcontext("%s", src)));
    if (commit) SPI_commit_my();
}

void SPI_finish_my(void) {
    int rc;
#if PG_VERSION_NUM < 110000
    MemoryContext oldcontext = CurrentMemoryContext;
#endif
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_finish failed"), errdetail("%s", SPI_result_code_string(rc))));
#if PG_VERSION_NUM >= 110000
    if (!SPI_inside_nonatomic_context()) ProcessCompletedNotifies();
#else
//    PopActiveSnapshot();
    ProcessCompletedNotifies();
    CommitTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
#endif
}

void SPI_start_transaction_my(const char *src) {
#if PG_VERSION_NUM < 110000
    MemoryContext oldcontext = CurrentMemoryContext;
//    ResourceOwner oldowner = CurrentResourceOwner;
#endif
    pgstat_report_activity(STATE_RUNNING, src);
    SetCurrentStatementStartTimestamp();
#if PG_VERSION_NUM >= 110000
    SPI_start_transaction();
#else
    BeginInternalSubTransaction((char *)src);
    MemoryContextSwitchTo(oldcontext);
//    CurrentResourceOwner = oldowner;
#endif
    PushActiveSnapshot(GetTransactionSnapshot());
    StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
}
