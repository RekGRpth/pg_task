#include "include.h"

Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null) {
    bool isnull;
    Datum datum = SPI_getbinval(tuple, tupdesc, SPI_fnumber(tupdesc, fname), &isnull);
    if (allow_null) return datum;
    if (isnull) E("%s isnull", fname);
    return datum;
}

SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes) {
    int rc;
    SPIPlanPtr plan;
    if (!(plan = SPI_prepare(src, nargs, argtypes))) E("SPI_prepare = %s", SPI_result_code_string(SPI_result));
    if ((rc = SPI_keepplan(plan))) E("SPI_keepplan = %s", SPI_result_code_string(rc));
    return plan;
}

void SPI_commit_my(void) {
    disable_timeout(STATEMENT_TIMEOUT, false);
    PopActiveSnapshot();
#if PG_VERSION_NUM >= 110000
    SPI_commit();
#endif
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

void SPI_connect_my(const char *src) {
    int rc;
#if PG_VERSION_NUM >= 110000
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) E("SPI_connect_ext = %s", SPI_result_code_string(rc));
#else
    SetCurrentStatementStartTimestamp();
    if (true) {
        MemoryContext oldcontext = CurrentMemoryContext;
        StartTransactionCommand();
        MemoryContextSwitchTo(oldcontext);
    }
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) E("SPI_connect = %s", SPI_result_code_string(rc));
#endif
    SPI_start_transaction_my(src);
}

void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res, bool commit) {
    int rc;
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != res) E("%s != %s", SPI_result_code_string(res), SPI_result_code_string(rc));
    if (commit) SPI_commit_my();
}

void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit) {
    int rc;
    if ((rc = SPI_execute_with_args(src, nargs, argtypes, values, nulls, false, 0)) != res) E("%s != %s", SPI_result_code_string(res), SPI_result_code_string(rc));
    if (commit) SPI_commit_my();
}

void SPI_finish_my(void) {
    int rc;
    if ((rc = SPI_finish()) != SPI_OK_FINISH) E("SPI_finish = %s", SPI_result_code_string(rc));
#if PG_VERSION_NUM >= 110000
    if (!SPI_inside_nonatomic_context()) ProcessCompletedNotifies();
#else
    ProcessCompletedNotifies();
    if (true) {
        MemoryContext oldcontext = CurrentMemoryContext;
        CommitTransactionCommand();
        MemoryContextSwitchTo(oldcontext);
    }
#endif
}

void SPI_start_transaction_my(const char *src) {
    pgstat_report_activity(STATE_RUNNING, src);
#if PG_VERSION_NUM >= 110000
    SetCurrentStatementStartTimestamp();
    SPI_start_transaction();
#endif
    PushActiveSnapshot(GetTransactionSnapshot());
    StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
}
