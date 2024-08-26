#include "include.h"

#if PG_VERSION_NUM < 150000
#include <access/xact.h>
#include <commands/async.h>
#endif
#include <executor/spi_priv.h>
#include <pgstat.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

typedef enum STMT_TYPE {
    STMT_BIND,
    STMT_EXECUTE,
    STMT_FETCH,
    STMT_PARSE,
    STMT_STATEMENT,
} STMT_TYPE;

static bool was_logged;

static const char *stmt_type(STMT_TYPE stmt) {
    switch (stmt) {
        case STMT_BIND: return "bind";
        case STMT_EXECUTE: return "execute";
        case STMT_FETCH: return "fetch";
        case STMT_PARSE: return "parse";
        case STMT_STATEMENT: default: return "statement";
    }
}

static int errdetail_params_my(int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    if (values && nargs > 0 && !IsAbortedTransactionBlockState()) {
        MemoryContext tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "BuildParamLogString", ALLOCSET_DEFAULT_SIZES);
        MemoryContext oldcontext = MemoryContextSwitchTo(tmpCxt);
        StringInfoData buf;
        initStringInfo(&buf);
        for (int i = 0; i < nargs; i++) {
            appendStringInfo(&buf, "%s$%d = ", i > 0 ? ", " : "", i + 1);
            if ((nulls && nulls[i] == 'n') || !OidIsValid(argtypes[i])) appendStringInfoString(&buf, "NULL"); else {
                bool typisvarlena;
                char *pstring;
                Oid typoutput;
                getTypeOutputInfo(argtypes[i], &typoutput, &typisvarlena);
                pstring = OidOutputFunctionCall(typoutput, values[i]);
                appendStringInfoCharMacro(&buf, '\'');
                for (char *p = pstring; *p; p++)  {
                    if (*p == '\'') appendStringInfoCharMacro(&buf, *p);
                    appendStringInfoCharMacro(&buf, *p);
                }
                appendStringInfoCharMacro(&buf, '\'');
            }
        }
        errdetail("parameters: %s", buf.data);
        MemoryContextSwitchTo(oldcontext);
        MemoryContextDelete(tmpCxt);
    }
    return 0;
}

static void check_log_statement_my(STMT_TYPE stmt, const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool logged) {
    if (!logged) was_logged = false;
    else if (log_statement == LOGSTMT_NONE) was_logged = false;
    else if (log_statement == LOGSTMT_ALL) was_logged = true;
    else was_logged = false;
    debug_query_string = src;
    SetCurrentStatementStartTimestamp();
    if (!logged) ereport(DEBUG2, (errmsg("%s: %s", stmt_type(stmt), src), errhidestmt(true)));
    else if (was_logged) ereport(LOG, (errmsg("%s: %s", stmt_type(stmt), src), errhidestmt(true), errhidestmt(true), errdetail_params_my(nargs, argtypes, values, nulls)));
}

static void check_log_duration_my(STMT_TYPE stmt, const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls) {
    char msec_str[32];
    switch (check_log_duration(msec_str, was_logged)) {
        case 1: ereport(LOG, (errmsg("duration: %s ms", msec_str), errhidestmt(true))); break;
        case 2: ereport(LOG, (errmsg("duration: %s ms  %s: %s", msec_str, stmt_type(stmt), src), errhidestmt(true), errdetail_params_my(nargs, argtypes, values, nulls))); break;
    }
    debug_query_string = NULL;
    was_logged = false;
}

Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid) {
    bool isnull;
    Datum datum;
    int fnumber = SPI_fnumber(tupdesc, fname);
    if (SPI_gettypeid(tupdesc, fnumber) != typeid) ereport(ERROR, (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH), errmsg("type of column \"%s\" must be \"%i\"", fname, typeid)));
    datum = SPI_getbinval(tuple, tupdesc, fnumber, &isnull);
    if (allow_null) return datum;
    if (isnull) ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("column \"%s\" must not be null", fname)));
    return datum;
}

Portal SPI_cursor_open_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, bool read_only) {
    Portal portal;
    SPI_freetuptable(SPI_tuptable);
    check_log_statement_my(STMT_BIND, src, plan->nargs, plan->argtypes, values, nulls, false);
    if (!(portal = SPI_cursor_open(NULL, plan, values, nulls, read_only))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_cursor_open failed"), errdetail("%s", SPI_result_code_string(SPI_result))));
    check_log_duration_my(STMT_BIND, src, plan->nargs, plan->argtypes, values, nulls);
    return portal;
}

Portal SPI_cursor_open_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool read_only) {
    Portal portal;
    SPI_freetuptable(SPI_tuptable);
    check_log_statement_my(STMT_BIND, src, nargs, argtypes, values, nulls, false);
    if (!(portal = SPI_cursor_open_with_args(NULL, src, nargs, argtypes, values, nulls, read_only, 0))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_cursor_open_with_args failed"), errdetail("%s", SPI_result_code_string(SPI_result)), errcontext("%s", src)));
    check_log_duration_my(STMT_BIND, src, nargs, argtypes, values, nulls);
    return portal;
}

SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes) {
    int rc;
    SPIPlanPtr plan;
    check_log_statement_my(STMT_PARSE, src, 0, NULL, NULL, NULL, false);
    if (!(plan = SPI_prepare(src, nargs, argtypes))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed"), errdetail("%s", SPI_result_code_string(SPI_result)), errcontext("%s", src)));
    if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_keepplan failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    check_log_duration_my(STMT_PARSE, src, 0, NULL, NULL, NULL);
    return plan;
}

void SPI_connect_my(const char *src) {
    int rc;
    debug_query_string = src;
    pgstat_report_activity(STATE_RUNNING, src);
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    PushActiveSnapshot(GetTransactionSnapshot());
    StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
}

void SPI_cursor_close_my(Portal portal) {
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
}

void SPI_cursor_fetch_my(const char *src, Portal portal, bool forward, long count) {
    check_log_statement_my(STMT_FETCH, src, 0, NULL, NULL, NULL, true);
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_fetch(portal, forward, count);
    check_log_duration_my(STMT_FETCH, src, 0, NULL, NULL, NULL);
}

void SPI_execute_plan_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, int res) {
    int rc;
    SPI_freetuptable(SPI_tuptable);
    check_log_statement_my(STMT_EXECUTE, src, plan->nargs, plan->argtypes, values, nulls, true);
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_plan failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res))));
    check_log_duration_my(STMT_EXECUTE, src, plan->nargs, plan->argtypes, values, nulls);
}

void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res) {
    int rc;
    SPI_freetuptable(SPI_tuptable);
    check_log_statement_my(STMT_STATEMENT, src, nargs, argtypes, values, nulls, true);
    if ((rc = SPI_execute_with_args(src, nargs, argtypes, values, nulls, false, 0)) != res) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_with_args failed"), errdetail("%s while expecting %s", SPI_result_code_string(rc), SPI_result_code_string(res)), errcontext("%s", src)));
    check_log_duration_my(STMT_STATEMENT, src, nargs, argtypes, values, nulls);
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
    was_logged = false;
    pgstat_report_stat(false);
    debug_query_string = NULL;
    pgstat_report_activity(STATE_IDLE, NULL);
}
