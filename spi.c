#include "include.h"

#include <executor/spi_priv.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#if PG_VERSION_NUM < 150000
#include <access/xact.h>
#include <commands/async.h>
#endif

typedef enum STMT_TYPE {
    STMT_BIND,
    STMT_EXECUTE,
    STMT_FETCH,
    STMT_PARSE,
    STMT_STATEMENT,
} STMT_TYPE;

static bool was_logged;
static bool bookkeeping;
static bool held;
static bool switched;
static int save_sec_context;
static Oid save_userid;

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
#if PG_VERSION_NUM >= 130000
    if (!log_parameter_max_length) return 0; // none logged at all, as the server has it for the parameters of its own, rather than each as ...
#endif
    if (values && nargs > 0 && !IsAbortedTransactionBlockState()) {
        MemoryContext tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "BuildParamLogString", ALLOCSET_DEFAULT_SIZES);
        MemoryContext oldcontext = MemoryContextSwitchTo(tmpCxt);
        StringInfoData buf;
        // all of them within what a line of the log takes along with the rest of it, the output and the error of a task near the most a task may keep among them, see TASK_OUTPUT_MAX, and its quotes doubled: rather than fail the bookkeeping, outside any PG_TRY(), and have the task run again on every reset
        const int budget = MaxAllocSize / 16;
        initStringInfo(&buf);
        for (int i = 0; i < nargs; i++) {
            appendStringInfo(&buf, "%s$%d = ", i > 0 ? ", " : "", i + 1);
            if ((nulls && nulls[i] == 'n') || !OidIsValid(argtypes[i])) appendStringInfoString(&buf, "NULL"); else {
                bool typisvarlena;
                char *pstring;
                int max = INT_MAX;
                Oid typoutput;
                getTypeOutputInfo(argtypes[i], &typoutput, &typisvarlena);
                pstring = OidOutputFunctionCall(typoutput, values[i]);
#if PG_VERSION_NUM >= 130000
                if (log_parameter_max_length >= 0) max = log_parameter_max_length; // as the server has it for the parameters of its own
#endif
                appendStringInfoCharMacro(&buf, '\'');
                for (char *p = pstring; *p; ) {
                    int j, len = pg_mblen(p);
                    if (p - pstring + len > max || buf.len + 2 * len > budget) { appendStringInfoString(&buf, "..."); break; } // at a character
                    for (j = 0; j < len && p[j]; j++) {
                        if (p[j] == '\'') appendStringInfoCharMacro(&buf, p[j]);
                        appendStringInfoCharMacro(&buf, p[j]);
                    }
                    p += j; // up to the end of the string, a character cut short there, rather than past it
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
    else if (was_logged) ereport(LOG, (errmsg("%s: %s", stmt_type(stmt), src), errhidestmt(true), errdetail_params_my(nargs, argtypes, values, nulls)));
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
    if (fnumber == SPI_ERROR_NOATTRIBUTE) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", fname)));
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

SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes) {
    int rc;
    SPIPlanPtr plan;
    check_log_statement_my(STMT_PARSE, src, 0, NULL, NULL, NULL, false);
    if (!(plan = SPI_prepare(src, nargs, argtypes))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed"), errdetail("%s", SPI_result_code_string(SPI_result)), errcontext("%s", src)));
    if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_keepplan failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    check_log_duration_my(STMT_PARSE, src, 0, NULL, NULL, NULL);
    return plan;
}

// a setting of the bookkeeping, for its transaction only, see SPI_connect_my()
static void SPI_set_config_my(const char *name, const char *value) {
    (void)set_config_option(name, value, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0
#if PG_VERSION_NUM >= 90500
        , false
#endif
    );
}

// a valid userid runs the whole transaction as that user, like a security definer function does, and as a security-restricted operation, as PostgreSQL does when running code as a more privileged user within someone else's session: switched only after the transaction started, so that an abort restores it by itself, and restored before the commit, since no transaction may start with a security context set
void SPI_connect_my(const char *src, Oid userid) {
    int rc;
#ifdef HOLD_CANCEL_INTERRUPTS
    // a task worker's bookkeeping, outside any PG_TRY(), which a cancel coming meanwhile (pg_cancel_backend(), a timeout), up to the end of its commit, would fail, taking the worker and the task's result down: hold it off, for no task, as a backend between statements does
    if ((held = OidIsValid(userid))) HOLD_CANCEL_INTERRUPTS();
#endif
    debug_query_string = src;
    pgstat_report_activity(STATE_RUNNING, src);
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    // the bookkeeping of a task, in its author's session, must not go by the transaction characteristics or the timeouts of that session, which the task's input or the author's role may have set (default_transaction_read_only = on would fail the bookkeeping, and the task run again on reset): read write and read committed, before any snapshot, and no timeouts, as for the scheduler's own queries in pg_work
    if ((bookkeeping = OidIsValid(userid))) {
        XactReadOnly = false;
        XactIsoLevel = XACT_READ_COMMITTED;
        XactDeferrable = false;
    }
    if ((switched = OidIsValid(userid))) {
        GetUserIdAndSecContext(&save_userid, &save_sec_context);
        SetUserIdAndSecContext(userid, save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION);
    }
    // and an empty search_path, for no object of the author's schemas, or of pg_temp, to take part in it, no lock_timeout, nor transaction_timeout, which would fail it, the task left to run again on reset, as SET of a security definer function has them: for its transaction only, taken back by its commit, deferred triggers on the table run then included, or its abort, the session's settings, the author's, left as they are, for the input of the next task, as save has it
    if (bookkeeping) {
        (void)NewGUCNestLevel();
        SPI_set_config_my("search_path", "");
        SPI_set_config_my("lock_timeout", "0");
#if PG_VERSION_NUM >= 170000
        SPI_set_config_my("transaction_timeout", "0"); // its timer, armed as the transaction started, disarmed by its assign hook
#endif
    }
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed"), errdetail("%s", SPI_result_code_string(rc)), errcontext("%s", src)));
    PushActiveSnapshot(GetTransactionSnapshot());
    !bookkeeping && StatementTimeout > 0 ? enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout) : disable_timeout(STATEMENT_TIMEOUT, false);
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

// undoes SPI_connect_my() after an error its caller catches to carry on: aborting the transaction also ends SPI, pops the snapshot and takes back a switched userid
void SPI_abort_my(void) {
    disable_timeout(STATEMENT_TIMEOUT, false);
    AbortCurrentTransaction();
    bookkeeping = false;
#ifdef HOLD_CANCEL_INTERRUPTS
    held = false; // nothing to resume: called only once an error was caught, whose errfinish() let cancels through again by itself
#endif
    switched = false;
    was_logged = false;
    debug_query_string = NULL;
    pgstat_report_activity(STATE_IDLE, NULL);
}

void SPI_finish_my(void) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    PopActiveSnapshot();
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_finish failed"), errdetail("%s", SPI_result_code_string(rc))));
    if (switched) SetUserIdAndSecContext(save_userid, save_sec_context); // only when switched: an unswitched SPI task with save = true may legitimately keep its own SET ROLE
    switched = false;
    CommitTransactionCommand();
    bookkeeping = false;
#if PG_VERSION_NUM < 150000
    ProcessCompletedNotifies(); // only now, out of the transaction, as PostgresMain() calls it: before 13 it starts a transaction of its own to signal the listeners of what this one (or a task's input before) notified, which within this one is an error
#endif
#ifdef HOLD_CANCEL_INTERRUPTS
    if (held) RESUME_CANCEL_INTERRUPTS();
    held = false;
#endif
    was_logged = false;
    pgstat_report_stat(false);
    debug_query_string = NULL;
    pgstat_report_activity(STATE_IDLE, NULL);
}
