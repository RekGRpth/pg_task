#include "include.h"

#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_class.h>
#include <mb/pg_wchar.h>
#include <commands/portalcmds.h>
#include <commands/prepare.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <parser/parser.h>
#include <pgstat.h>
#include <replication/slot.h>
#include <storage/ipc.h>
#include <storage/pmsignal.h>
#include <storage/proc.h>
#include <tcop/pquery.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <unistd.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/plancache.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#if PG_VERSION_NUM >= 110000
#include <jit/jit.h>
#endif

#if PG_VERSION_NUM < 100000
#include <parser/scanner.h>
#endif

#define DEST_UNTAKE_TIMEOUT 5000 // milliseconds the next task, whose row someone else holds, may take to be given back on the way out

static Task task = {0};

Task *get_task(void) {
    return &task;
}

// the next task taken for nothing back to PLAN, its row held by someone else, a user's update say, tried again for a while, rather than left in TAKE till the next reset, an hour by default
static void dest_untake(void) {
    TimestampTz end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), DEST_UNTAKE_TIMEOUT);
    while (!task_untake_try(&task)) {
        if (!PostmasterIsAlive() || GetCurrentTimestamp() >= end) {
            ereport(WARNING, (errmsg("id = " INT64_FORMAT ", not put back to PLAN, its row held by someone else, left in TAKE for the next reset", task.shared->id)));
            return;
        }
        pg_usleep(100 * 1000L);
    }
}

static char *SPI_getvalue_my(TupleTableSlot *slot, TupleDesc tupdesc, int fnumber) {
    bool isnull;
    bool typisvarlena;
    Datum attr = slot_getattr(slot, fnumber, &isnull);
    Oid foutoid;
    if (isnull) return NULL;
    getTypeOutputInfo(TupleDescAttr(tupdesc, fnumber - 1)->atttypid, &foutoid, &typisvarlena);
    return OidOutputFunctionCall(foutoid, attr);
}

// the memory of the output functions of the values of a row, a copy of a value read from a table that numeric_out() detoasts, say, and doesn't free, reset after each row, as printtup() has it, rather than kept till the end of the statement, as that of the executor is, gigabytes for a large SELECT whose output keeps within the most a task may keep, see TASK_OUTPUT_MAX
static MemoryContext dest_row_context(void) {
    static MemoryContext context = NULL;
    if (!context) context = AllocSetContextCreate(TopMemoryContext, "dest_row", ALLOCSET_DEFAULT_SIZES);
    return context;
}

static void headers(TupleDesc tupdesc) {
    task_line(&task);
    for (int col = 1; col <= tupdesc->natts; col++) {
        char *fname = SPI_fname(tupdesc, col);
        if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        appendBinaryStringInfoEscapeQuote(&task.output, fname, strlen(fname), false, task.escape, task.quote);
        pfree(fname);
    }
}

// the descriptor of the result, as rStartup() has it, whose names are those of the query, as the server sends them to a client, and remote mode has them, rather than those of the slot, which, its projection skipped, is that of the scan, with the names of the table, or none for VALUES
static TupleDesc result_tupdesc = NULL;

// whether a value of the type goes unquoted, see appendBinaryStringInfoEscapeQuote(), as string = false has it: a domain by its base type, a number say, as the server sends it to a client, and remote mode has it
static bool dest_is_string(Oid type) {
    return init_oid_is_string(type) && init_oid_is_string(getBaseType(type));
}

static
#if PG_VERSION_NUM >= 90600
bool
#else
void
#endif
receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    MemoryContext oldcontext;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    if (!task.shared)
        return
#if PG_VERSION_NUM >= 90600
    true
#endif
    ;
    if (!task.output.data) initStringInfoMy(&task.output);
    oldcontext = MemoryContextSwitchTo(dest_row_context()); // see dest_row_context(), the output itself kept in its own, of TopMemoryContext, see initStringInfoMy()
    if (task.header && !task.row && tupdesc->natts > 1) headers(result_tupdesc && result_tupdesc->natts == tupdesc->natts ? result_tupdesc : tupdesc);
    task_line(&task);
    for (int col = 1; col <= tupdesc->natts; col++) {
        char *value = SPI_getvalue_my(slot, tupdesc, col);
        if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        if (!value) appendStringInfoString(&task.output, task.null); else {
            appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !dest_is_string(SPI_gettypeid(tupdesc, col)) && task.string, task.escape, task.quote);
            pfree(value);
        }
    }
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(dest_row_context());
    task.row++;
    if (task.output.len > (int)TASK_OUTPUT_MAX) ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("task output exceeds %lu bytes", (unsigned long)TASK_OUTPUT_MAX)));
#if PG_VERSION_NUM >= 90600
    return true;
#endif
}

static void rStartup(DestReceiver *self, int operation, TupleDesc tupdesc) {
    if (!task.shared) return;
    switch (operation) {
        case CMD_UNKNOWN: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_UNKNOWN", task.shared->id); break;
        case CMD_SELECT: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_SELECT", task.shared->id); break;
        case CMD_UPDATE: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_UPDATE", task.shared->id); break;
        case CMD_INSERT: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_INSERT", task.shared->id); break;
        case CMD_DELETE: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_DELETE", task.shared->id); break;
        case CMD_UTILITY: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_UTILITY", task.shared->id); break;
        case CMD_NOTHING: elog(DEBUG1, "id = " INT64_FORMAT ", operation = CMD_NOTHING", task.shared->id); break;
        default: elog(DEBUG1, "id = " INT64_FORMAT ", operation = %i", task.shared->id, operation); break;
    }
    task.row = 0;
    task.skip = operation == CMD_SELECT;
    result_tupdesc = tupdesc;
}

static void rShutdown(DestReceiver *self) {
    result_tupdesc = NULL;
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT, task.shared->id);
}

static void rDestroy(DestReceiver *self) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT, task.shared->id);
}

static
#if PG_VERSION_NUM >= 120000
const
#endif
DestReceiver myDestReceiver = {
    .receiveSlot = receiveSlot,
    .rStartup = rStartup,
    .rShutdown = rShutdown,
    .rDestroy = rDestroy,
    .mydest = DestDebug,
};

DestReceiver *CreateDestReceiverMy(CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT, task.shared->id);
#if PG_VERSION_NUM >= 120000
    return unconstify(DestReceiver *, &myDestReceiver);
#else
    return &myDestReceiver;
#endif
}

static void ReadyForQueryMy(CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT, task.shared->id);
}

void NullCommandMy(CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT, task.shared->id);
}

static bool held = false; // interrupts held since the input committed, see dest_xact()

// the next statement of the input after one that committed, COMMIT, say, to run with interrupts, as before
// the locks of the task and of its group back, after a command of its input that let go of them, as pg_advisory_unlock_all() and DISCARD ALL do, not telling them from advisory ones: for a pg_work restarted meanwhile not to take its group for free of it, nor the task for orphaned (see init_task_ids()), and for the bookkeeping to find them
static void dest_relock(void) {
    relock_table_pid_hash(task.shared->oid, task.pid, task.shared->hash);
    if (task.lock) relock_table_id(task.shared->oid, task.shared->id);
}

static void dest_resume(void) {
    if (!held) return;
    held = false;
    RESUME_INTERRUPTS();
}

#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT ", commandTag = %s", task.shared->id, GetCommandTagName(commandTag));
    dest_resume();
}

void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output) {
    char completionTag[COMPLETION_TAG_BUFSIZE];
    CommandTag tag = qc->commandTag;
    const char *tagname = GetCommandTagName(tag);
    if (!task.shared) return;
    dest_relock();
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, qc->nprocessed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = " INT64_FORMAT ", completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, completionTag);
    }
}
#else
void BeginCommandMy(const char *commandTag, CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = " INT64_FORMAT ", commandTag = %s", task.shared->id, commandTag);
    dest_resume();
}

void EndCommandMy(const char *commandTag, CommandDest dest) {
    if (!task.shared) return;
    dest_relock();
    elog(DEBUG1, "id = " INT64_FORMAT ", commandTag = %s", task.shared->id, commandTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, commandTag);
    }
}
#endif

#if PG_VERSION_NUM < 100000
// before 10 a function a query calls that runs queries of its own through SPI, one of PL/pgSQL say, connects to SPI only once the connection of the caller is pushed, as SPI's own calls of the executor have it, failing with SPI_ERROR_CONNECT otherwise: pushed for the calls of the executor here, outside of SPI, as exec_simple_query() runs them in local mode, see dest_execute_prepared(), dest_execute_fetch() and the cursor of dest_execute_spi(), and popped on an error too, see dest_execute_spi_at()
static bool pushed = false;

static void dest_push(void) {
    SPI_push();
    pushed = true;
}

static void dest_pop(void) {
    if (pushed) SPI_pop();
    pushed = false;
}
#else
#define dest_push() ((void)0)
#define dest_pop() ((void)0)
#endif

// stmt is the parse tree of src, whose command tag is the one local mode and remote mode report, rather than SPI's own result code (UTILITY for any utility statement, say)
// SPI tells of EXECUTE neither the command tag nor the row count of the prepared statement it runs, only UTILITY: run that one as exec_simple_query() does, into the receiver and with the completion of local mode, with a snapshot of its own, as SPI_execute() takes for each statement
static void dest_execute_prepared(ExecuteStmt *stmt) {
#if PG_VERSION_NUM >= 130000
    QueryCompletion qc;
    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = task.input; // the whole input, which the locations of stmt count from, read from it as a whole, see dest_execute(), as local mode has it, rather than the text of the statement alone, an error in a parameter at a position past it say
    InitializeQueryCompletion(&qc);
#else
    char completionTag[COMPLETION_TAG_BUFSIZE] = "";
#endif
    CommandCounterIncrement();
    PushActiveSnapshot(GetTransactionSnapshot());
    dest_push();
#if PG_VERSION_NUM >= 130000
    ExecuteQuery(pstate, stmt, NULL, NULL, CreateDestReceiverMy(DestDebug), &qc);
#else
    ExecuteQuery(stmt, NULL, task.input, NULL, CreateDestReceiverMy(DestDebug), completionTag); // see above
#endif
    dest_pop();
    PopActiveSnapshot();
    CommandCounterIncrement();
#if PG_VERSION_NUM >= 130000
    free_parsestate(pstate);
    EndCommandMy(&qc, DestDebug, false);
#else
    EndCommandMy(completionTag, DestDebug);
#endif
}

// SPI tells of MOVE no row count, only UTILITY, and of a FETCH of no rows that it returns rows, whose tag local mode leaves out then, not at all: run it as exec_simple_query() does, into the receiver and with the completion of the fetch, as of EXECUTE above
static void dest_execute_fetch(FetchStmt *stmt) {
#if PG_VERSION_NUM >= 130000
    QueryCompletion qc;
    InitializeQueryCompletion(&qc);
#else
    char completionTag[COMPLETION_TAG_BUFSIZE] = "";
#endif
    CommandCounterIncrement();
#if PG_VERSION_NUM >= 130000
    PerformPortalFetch(stmt, CreateDestReceiverMy(DestDebug), &qc);
    EndCommandMy(&qc, DestDebug, false);
#else
    dest_push();
    PerformPortalFetch(stmt, CreateDestReceiverMy(DestDebug), completionTag);
    dest_pop();
    EndCommandMy(completionTag, DestDebug);
#endif
}

// alone: src is stmt only, rather than the whole input of several statements, which spi mode before 10 runs at once
#if PG_VERSION_NUM >= 180000
#define dest_returning(stmt) ((stmt)->returningClause != NULL)
#else
#define dest_returning(stmt) ((stmt)->returningList != NIL)
#endif

// a statement whose result is rows only, a SELECT, not one INTO a table, or a statement with RETURNING: for a cursor, see dest_execute_spi()
static bool dest_spi_rows(Node *stmt) {
    switch (nodeTag(stmt)) {
        case T_SelectStmt: return !((SelectStmt *)stmt)->intoClause;
        case T_InsertStmt: return dest_returning((InsertStmt *)stmt);
        case T_UpdateStmt: return dest_returning((UpdateStmt *)stmt);
        case T_DeleteStmt: return dest_returning((DeleteStmt *)stmt);
#if PG_VERSION_NUM >= 170000
        case T_MergeStmt: return dest_returning((MergeStmt *)stmt);
#endif
        default: return false;
    }
}

// the statement as the context of an error of its rows fetched, as SPI_execute() has it, see dest_execute_spi()
static void dest_spi_context(void *arg) {
    errcontext("SQL statement \"%s\"", (const char *)arg);
}

// the rows of SPI_tuptable into the output, the first of all, first, after the header, if any, its size checked against the most a task may keep after each
static void dest_spi_output(uint64 first) {
    for (uint64 row = 0; row < SPI_processed; row++) {
        MemoryContext oldcontext;
        task.skip = 1;
        if (!task.output.data) initStringInfoMy(&task.output);
        oldcontext = MemoryContextSwitchTo(dest_row_context()); // as in receiveSlot()
        if (task.header && !first && !row && SPI_tuptable->tupdesc->natts > 1) headers(SPI_tuptable->tupdesc);
        task_line(&task);
        for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
            char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
            if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
            if (!value) appendStringInfoString(&task.output, task.null); else {
                appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !dest_is_string(SPI_gettypeid(SPI_tuptable->tupdesc, col)) && task.string, task.escape, task.quote);
                pfree(value);
            }
        }
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(dest_row_context());
        if (task.output.len > (int)TASK_OUTPUT_MAX) ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("task output exceeds %lu bytes", (unsigned long)TASK_OUTPUT_MAX)));
    }
}

// the settings of the lexer the input was read by at first, by pg_parse_query() in dest_execute(), as local mode and remote mode read all of its statements at once, for SPI to read each statement again by them, its text sliced by that reading, rather than by those the statements before it set: backslash_quote = off failing E'\'' then, or, before 19, standard_conforming_strings = off ending a string with a backslash elsewhere; from right before SPI reads it till its parse analysis, see dest_analyze(), which comes before anything of it runs, a SET of them say, those of the session kept aside meanwhile
static bool lexer_read = false;
static int backslash_quote_input, backslash_quote_session;
#if PG_VERSION_NUM < 190000
static bool standard_conforming_strings_input, standard_conforming_strings_session;
#endif
static post_parse_analyze_hook_type post_parse_analyze_hook_prev = NULL;

static void dest_lexer_input(void) {
    backslash_quote_input = backslash_quote;
#if PG_VERSION_NUM < 190000
    standard_conforming_strings_input = standard_conforming_strings;
#endif
}

static void dest_lexer_read(void) {
    backslash_quote_session = backslash_quote;
    backslash_quote = backslash_quote_input;
#if PG_VERSION_NUM < 190000
    standard_conforming_strings_session = standard_conforming_strings;
    standard_conforming_strings = standard_conforming_strings_input;
#endif
    lexer_read = true;
}

// those of the session back, once read, before anything runs, or on an error, before the abort of the subtransaction, which knows nothing of them
static void dest_lexer_back(void) {
    if (!lexer_read) return;
    backslash_quote = backslash_quote_session;
#if PG_VERSION_NUM < 190000
    standard_conforming_strings = standard_conforming_strings_session;
#endif
    lexer_read = false;
}

#if PG_VERSION_NUM >= 190000
static void dest_analyze(ParseState *pstate, Query *query, const JumbleState *jstate) {
    dest_lexer_back();
    if (post_parse_analyze_hook_prev) post_parse_analyze_hook_prev(pstate, query, jstate);
}
#elif PG_VERSION_NUM >= 140000
static void dest_analyze(ParseState *pstate, Query *query, JumbleState *jstate) {
    dest_lexer_back();
    if (post_parse_analyze_hook_prev) post_parse_analyze_hook_prev(pstate, query, jstate);
}
#else
static void dest_analyze(ParseState *pstate, Query *query) {
    dest_lexer_back();
    if (post_parse_analyze_hook_prev) post_parse_analyze_hook_prev(pstate, query);
}
#endif

// the position of an error in the syntax of the body of a function, of a DO say, function_parse_error_transpose() moves into the text of the active portal, as local mode has it, within the whole input, or from 10 on gives up with none, the position left within the body, the body as the query, while before 10 it takes one for there, asserting it, and with none dereferences, taking the server down: spi mode runs in none, unlike local mode, so one for it, active while a statement runs, see dest_execute_spi_at(), of the text SPI runs, whose error callback takes the position for one within that text, which dest_execute_spi_at() moves into the whole input then
static PortalData dest_portal = {.status = PORTAL_ACTIVE};

// the SELECT of a rule of INSERT, UPDATE or DELETE, whose rows SPI drops for it's not the query that sets the command tag, which local mode and remote mode return: made that one, in the plan about to run, its rows kept then as those of the statement, with no tag, as they have it, true if there is one; a plan is replanned from its own statements, these among them, as long as it stays valid, nothing changing it between here and its run
static bool dest_spi_rule_rows(SPIPlanPtr plan) {
    bool found = false;
    ListCell *source;
    foreach(source, SPI_plan_get_plan_sources(plan)) {
        ListCell *query;
        foreach(query, ((CachedPlanSource *)lfirst(source))->query_list) {
            Query *q = (Query *)lfirst(query);
            if (q->commandType == CMD_SELECT && !q->canSetTag && !q->utilityStmt) { q->canSetTag = true; found = true; }
        }
    }
    return found;
}

static void dest_execute_spi(const char *src, Node *stmt, bool alone) {
    bool count = false;
    bool exists = false;
    char completionTag[COMPLETION_TAG_BUFSIZE];
    int rc;
#if PG_VERSION_NUM >= 130000
    const char *tagname = GetCommandTagName(CreateCommandTag(stmt));
#else
    const char *tagname = CreateCommandTag(stmt);
#endif
    if (StatementTimeout > 0) enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout); else disable_timeout(STATEMENT_TIMEOUT, false); // for each statement of the input anew, as exec_simple_query() has it from 13 on in local mode, rather than once for all of them, see SPI_connect_my(); for an EXECUTE too, and none once the input set statement_timeout to 0, rather than the rest of the one of the statement before
    dest_portal.sourceText = src;
    if (alone && IsA(stmt, ExecuteStmt)) { dest_execute_prepared((ExecuteStmt *)stmt); return; }
    if (alone && IsA(stmt, FetchStmt)) { dest_execute_fetch((FetchStmt *)stmt); return; }
#if PG_VERSION_NUM >= 90500
    // CREATE TABLE AS with IF NOT EXISTS of a table that exists does nothing, which SPI tells only as no rows stored: of a table in the schema it would be created in, as PostgreSQL looks for it, not of one of the same name elsewhere on the search_path, a temporary one say, which keeps none from being created; for a name of no schema, not temporary, by the schema of creation, whose error, of an empty search_path, PostgreSQL has too, as the statement's own, for a temporary one or one of a schema, as RangeVarGetRelid() finds it there, with no error, of a schema that doesn't exist say, which the statement has then
    if (IsA(stmt, CreateTableAsStmt) && ((CreateTableAsStmt *)stmt)->if_not_exists) {
        RangeVar *rel = ((CreateTableAsStmt *)stmt)->into->rel;
        if (!rel->schemaname && rel->relpersistence != RELPERSISTENCE_TEMP) {
            ErrorContextCallback context = {.callback = dest_spi_context, .arg = (void *)src, .previous = error_context_stack};
            error_context_stack = &context;
            exists = OidIsValid(get_relname_relid(rel->relname, RangeVarGetCreationNamespace(rel)));
            error_context_stack = context.previous;
        } else exists = OidIsValid(RangeVarGetRelid(rel, NoLock, true));
    }
#endif
    if (dest_spi_rows(stmt)) {
        SPIPlanPtr plan;
        dest_lexer_read();
        plan = SPI_prepare(src, 0, NULL);
        dest_lexer_back();
        if (!plan) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed"), errdetail("%s", SPI_result_code_string(SPI_result))));
        // fetched a few rows at a time, each checked against the most of output a task may keep as it comes, see dest_spi_output(), rather than all of them first, gigabytes of a SELECT, say, in the worker's memory, with the worker, or the whole server, taken down by running out of it, as local mode and remote mode have it; only the rows, as below
        if (SPI_is_cursor_plan(plan)) {
            ErrorContextCallback context = {.callback = dest_spi_context, .arg = (void *)src, .previous = error_context_stack};
            Portal portal;
            portal = SPI_cursor_open(NULL, plan, NULL, NULL, false); // which has the statement as the context of an error itself, as the fetches don't
            error_context_stack = &context;
            elog(DEBUG1, "id = " INT64_FORMAT ", commandTag = %s, cursor", task.shared->id, tagname);
            // into the receiver of local mode, each row into the output as it comes, rather than by SPI_cursor_fetch() a few at a time, those of a fetch an error, a timeout or a STOP, came within lost with it; in one go, as each run of the portal starts the receiver anew, with the header again
            dest_push();
            (void)PortalRunFetch(portal, FETCH_FORWARD, FETCH_ALL, CreateDestReceiverMy(DestDebug));
            dest_pop();
            error_context_stack = context.previous;
            SPI_cursor_close(portal);
            SPI_freeplan(plan);
            task.skip = 0;
            return;
        }
        SPI_freeplan(plan);
    }
    dest_lexer_read();
    if (IsA(stmt, InsertStmt) || IsA(stmt, UpdateStmt) || IsA(stmt, DeleteStmt)) {
        SPIPlanPtr plan = SPI_prepare(src, 0, NULL);
        if (!plan) { dest_lexer_back(); ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed"), errdetail("%s", SPI_result_code_string(SPI_result)))); }
        if (dest_spi_rule_rows(plan)) rc = SPI_execute_plan(plan, NULL, NULL, false, 0); else rc = SPI_execute(src, false, 0);
        SPI_freeplan(plan);
    } else rc = SPI_execute(src, false, 0);
    dest_lexer_back();
    switch (rc) {
        case SPI_ERROR_ARGUMENT: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid arguments"))); break;
        case SPI_ERROR_COPY: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY is not supported"))); break;
        case SPI_ERROR_OPUNKNOWN: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unrecognized command type"))); break;
        case SPI_ERROR_TRANSACTION: ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("transaction control statement is not supported"))); break;
        // with RETURNING, as for SELECT, only the rows, if any, as local mode and remote mode report
        case SPI_OK_DELETE: count = true; break;
        case SPI_OK_DELETE_RETURNING: task.skip = 1; break;
        case SPI_OK_INSERT: count = true; break;
        case SPI_OK_INSERT_RETURNING: task.skip = 1; break;
#ifdef SPI_OK_MERGE
        case SPI_OK_MERGE: count = true; break;
#endif
#ifdef SPI_OK_MERGE_RETURNING
        case SPI_OK_MERGE_RETURNING: task.skip = 1; break;
#endif
        // a DO INSTEAD rule with no query of the same command as the statement: no rows, which PostgreSQL itself reports with the count of the statement's command at 0
        case SPI_OK_REWRITTEN: count = !strcmp(tagname, "INSERT") || !strcmp(tagname, "UPDATE") || !strcmp(tagname, "DELETE"); break;
        case SPI_OK_SELECT: task.skip = 1; break;
        case SPI_OK_SELINTO: tagname = "SELECT"; count = true; break; // CREATE TABLE AS and SELECT INTO report the rows they stored, as SELECT n
        case SPI_OK_UTILITY: // COPY to or from a file reports its rows too, and so does CREATE TABLE AS with data, as SELECT n
            if (IsA(stmt, CopyStmt)) count = true;
            else if (IsA(stmt, CreateTableAsStmt) && !((CreateTableAsStmt *)stmt)->into->skipData && !exists) { tagname = "SELECT"; count = true; }
            break;
        case SPI_OK_UPDATE: count = true; break;
        case SPI_OK_UPDATE_RETURNING: task.skip = 1; break;
    }
    elog(DEBUG1, "id = " INT64_FORMAT ", commandTag = %s", task.shared->id, tagname);
    if (SPI_tuptable) dest_spi_output(0);
#if PG_VERSION_NUM < 120000
    // the oid of the one row inserted into a table WITH OIDS before 12, as local mode and remote mode report it, 0 for none or several
    if (count && !strcmp(tagname, "INSERT")) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s %u " UINT64_FORMAT, tagname, SPI_processed == 1 ? SPI_lastoid : InvalidOid, (uint64)SPI_processed); else
#endif
    if (count) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, !strcmp(tagname, "INSERT") ? "%s 0 " UINT64_FORMAT : "%s " UINT64_FORMAT, tagname, (uint64)SPI_processed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = " INT64_FORMAT ", completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, completionTag);
    }
}

// the statement of the input at location, in bytes, an error at a position in it at that position in the whole input, as local mode reports it, the input its STATEMENT, rather than within the text of the statement alone, which SPI reports as its QUERY then
static void dest_execute_spi_at(const char *src, Node *stmt, bool alone, int location) {
    MemoryContext oldcontext = CurrentMemoryContext;
    Portal portal = ActivePortal;
    ActivePortal = &dest_portal; // see dest_execute_spi()
    PG_TRY();
        dest_execute_spi(src, stmt, alone);
    PG_CATCH();
        {
            ErrorData *edata;
            dest_pop(); // see dest_push()
            ActivePortal = portal;
            MemoryContextSwitchTo(oldcontext);
            dest_lexer_back(); // an error of SPI reading the statement, see dest_lexer_read()
            edata = CopyErrorData();
            // only that of SPI for the statement itself, not of a query of a function it called, say, which local mode reports so too
            if (edata->cursorpos <= 0 && edata->internalpos > 0 && edata->internalquery && !strcmp(edata->internalquery, src)) {
                FlushErrorState();
                edata->cursorpos = pg_mbstrlen_with_len(task.input, location) + edata->internalpos;
                edata->internalpos = 0;
                pfree(edata->internalquery);
                edata->internalquery = NULL;
                ReThrowError(edata);
            }
            FreeErrorData(edata);
            PG_RE_THROW();
        }
    PG_END_TRY();
    ActivePortal = portal;
}

#if PG_VERSION_NUM < 100000
// no stmt_location before 10 to slice the input by, into the text of each statement, as from 10 on: up to each ; outside parentheses, those of the actions of CREATE RULE say, by the tokens of the server's own scanner, which takes quotes and dollar quotes as the parser did, a statement of no tokens, an empty one, giving no parse tree
static void dest_execute_split(const char *src, List *parsetree_list) {
    bool tokens = false;
    core_yy_extra_type yyextra;
    core_yyscan_t scanner = scanner_init(src, &yyextra, ScanKeywords, NumScanKeywords);
    core_YYSTYPE yylval;
    int depth = 0, start = 0, token;
    List *stmts = NIL; // all of them first, then run, as an error of one would leave the scanner unfinished
    List *locations = NIL; // and where each starts in src
    int i = 0;
    ListCell *stmt, *tree;
    YYLTYPE yylloc;
#if PG_VERSION_NUM >= 90500
    yyextra.escape_string_warning = false; // the parser has warned already, which before 9.5 the scanner does by the setting itself, so twice
#endif
    while ((token = core_yylex(&yylval, &yylloc, scanner))) {
        if (token == ';' && depth <= 0) {
            if (tokens) { stmts = lappend(stmts, pnstrdup(src + start, yylloc + 1 - start)); locations = lappend_int(locations, start); }
            start = yylloc + 1;
            tokens = false;
            continue;
        }
        if (token == '(') depth++; else if (token == ')') depth--;
        tokens = true;
    }
    scanner_finish(scanner);
    if (tokens) { stmts = lappend(stmts, pstrdup(src + start)); locations = lappend_int(locations, start); }
    if (list_length(stmts) != list_length(parsetree_list)) { // not to be, but rather than run a statement by the parse tree of another: as SPI_execute() runs them all, reporting the last statement only
        elog(WARNING, "%i statements of %i parse trees", list_length(stmts), list_length(parsetree_list));
        dest_execute_spi_at(src, (Node *)llast(parsetree_list), false, 0);
        return;
    }
    forboth(stmt, stmts, tree, parsetree_list) dest_execute_spi_at(lfirst(stmt), (Node *)lfirst(tree), true, list_nth_int(locations, i++));
    list_free_deep(stmts);
    list_free(locations);
}
#endif

static void dest_execute(void) {
    if (!task.shared->spi) {
        ListCell *cell;
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(MessageContext);
        MemoryContextResetAndDeleteChildren(MessageContext);
        InvalidateCatalogSnapshotConditionally();
        foreach(cell, pg_parse_query(task.input)) {
#if PG_VERSION_NUM >= 100000
            Node *node = ((RawStmt *)lfirst(cell))->stmt;
#else
            Node *node = (Node *)lfirst(cell);
#endif
            if (IsA(node, CopyStmt) && !((CopyStmt *)node)->filename) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY %s is not supported", ((CopyStmt *)node)->is_from ? "FROM STDIN" : "TO STDOUT")));
        }
        MemoryContextSwitchTo(oldMemoryContext);
        whereToSendOutput = DestNone; // as for any background worker: the receiver and the command hooks above take the output, whatever it is, while DestDebug would have every message of the worker written to stderr too, besides the log_destination, csvlog say, see send_message_to_server_log()
        ReadyForQueryMy(whereToSendOutput);
        SetCurrentStatementStartTimestamp();
        exec_simple_query_my(task.input);
        if (IsTransactionState()) {
            task.skip = 1; // the tag of this END, no statement of the input, left out, as remote mode leaves out that of the COMMIT it sends then, see work_done()
            exec_simple_query_my(SQL(END));
            if (IsTransactionState()) ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("still active sql transaction")));
        }
    } else {
        List *parsetree_list;
        dest_lexer_input();
        parsetree_list = pg_parse_query(task.input);
        if (!parsetree_list) return; // no statement at all: nothing to run and no output, as NullCommand in local mode, rather than SPI_execute()'s SPI_OK_REWRITTEN tag
#if PG_VERSION_NUM >= 100000
        // RawStmt.stmt_location lets us slice task.input into the text of each individual
        // statement and run them through SPI one by one, the same way exec_simple_query and
        // the remote libpq path already report a result per statement instead of just the last.
        if (list_length(parsetree_list) <= 1) dest_execute_spi_at(task.input, linitial_node(RawStmt, parsetree_list)->stmt, true, 0); else {
            ListCell *cell;
            int prev_start = -1;
            Node *prev_stmt = NULL;
            foreach(cell, parsetree_list) {
                int start = lfirst_node(RawStmt, cell)->stmt_location;
                if (prev_start >= 0) {
                    char *stmt = pnstrdup(task.input + prev_start, start - prev_start);
                    dest_execute_spi_at(stmt, prev_stmt, true, prev_start);
                    pfree(stmt);
                }
                prev_start = start;
                prev_stmt = lfirst_node(RawStmt, cell)->stmt;
            }
            dest_execute_spi_at(task.input + prev_start, prev_stmt, true, prev_start);
        }
#else
        if (list_length(parsetree_list) <= 1) dest_execute_spi_at(task.input, (Node *)linitial(parsetree_list), true, 0); else dest_execute_split(task.input, parsetree_list);
#endif
    }
}

static void dest_catch(void) {
    if (!task.shared->spi) {
        HOLD_INTERRUPTS();
        disable_all_timeouts(false);
        stmt_timeout_active_my(false); // as PostgresMain() does, or before 13 the next task's statements would take the statement timeout for still armed and never arm it
        QueryCancelPending = false;
    }
    EmitErrorReport();
    if (!task.shared->spi) {
        debug_query_string = NULL;
        AbortOutOfAnyTransaction();
#if PG_VERSION_NUM >= 110000
        PortalErrorCleanup();
#endif
#if PG_VERSION_NUM >= 110000 && PG_VERSION_NUM < 150000
        // as PostgresMain() does before 11.17, 12.12, 13.8 and 14.5, where a COMMIT of a procedure or a DO block failing, on a deferred constraint say, leaves the stack of SPI to the caller, its connection, gone with its portal, current still, for all tasks of the worker after it, as if within a COMMIT of a procedure, see dest_xact(); a no-op from those on, the server it runs on, rather than the one it was built with, telling
        SPICleanup();
#endif
        if (MyReplicationSlot) ReplicationSlotRelease();
#if PG_VERSION_NUM >= 170000
        ReplicationSlotCleanup(false);
#elif PG_VERSION_NUM >= 100000
        ReplicationSlotCleanup();
#endif
#if PG_VERSION_NUM >= 110000
        jit_reset_after_error();
#endif
        MemoryContextSwitchTo(TopMemoryContext);
    }
    FlushErrorState();
    if (!task.shared->spi) {
        xact_started_my(false);
        RESUME_INTERRUPTS();
    }
}

static void dest_discard(void) {
    Shared *shared = task.shared;
    static const char *src = SQL(SET SESSION AUTHORIZATION DEFAULT; RESET ALL; DEALLOCATE ALL; CLOSE ALL; UNLISTEN *; DISCARD PLANS; DISCARD TEMP; DISCARD SEQUENCES;);
    StringInfoData oid;
    task.shared = NULL; // disable dest receiver and command tags during cleanup
#ifdef HOLD_CANCEL_INTERRUPTS
    HOLD_CANCEL_INTERRUPTS(); // as the bookkeeping does, see SPI_connect_my(): a cancel coming meanwhile is for no task
#endif
    PG_TRY();
        if (shared->spi) {
            SPI_connect_my(src, InvalidOid);
            SPI_execute_with_args_my(src, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
            SPI_finish_my();
        } else exec_simple_query_my(src);
    PG_CATCH();
        task.shared = shared; // restore before any error handling dereferences it, and nothing to resume: the error's errfinish() let cancels through again by itself
        PG_RE_THROW();
    PG_END_TRY();
#ifdef HOLD_CANCEL_INTERRUPTS
    RESUME_CANCEL_INTERRUPTS();
#endif
    task.shared = shared;
    unlock_advisory_all();
    dest_relock(); // the locks of pg_task, let go of with the advisory ones
    SetConfigOption("pg_task.schema", task.shared->schema, PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("pg_task.table", task.shared->table, PGC_USERSET, PGC_S_SESSION);
    initStringInfoMy(&oid);
    appendStringInfo(&oid, "%i", task.shared->oid);
    SetConfigOption("pg_task.oid", oid.data, PGC_USERSET, PGC_S_SESSION);
    pfree(oid.data);
}

#ifdef HAVE_LOG_MIN_MESSAGES_ARRAY
#define log_min_messages_my log_min_messages[MyBackendType]
#else
#define log_min_messages_my log_min_messages
#endif

static volatile sig_atomic_t running = false; // the task's input, rather than its bookkeeping, which a cancel must not fail
static int quiet = 0; // the log_min_messages of the server, above FATAL, which dest_loud() let FATAL through for the input instead of
static bool fatal = false; // the input failed with FATAL, see dest_emit_log()
static bool failed = false; // the worker going down on an error of its own starting the task, a trigger of the table refusing its WORK say, rather than terminated, see dest_shmem_exit() and dest_timeout()
static emit_log_hook_type emit_log_hook_prev = NULL;

// FATAL takes the worker down right away, with no PG_CATCH() to record the error of the input, as exit_on_error or transaction_timeout make one, and the task stays in WORK, to run again on every reset: take it for the task as it's logged, for dest_shmem_exit() to record, but for a termination, as on a shutdown, which leaves the task, not done, to be run again
static void dest_emit_log(ErrorData *edata) {
    if (running && !fatal && task.shared && edata->elevel == FATAL && edata->sqlerrcode != ERRCODE_ADMIN_SHUTDOWN) {
        fatal = true;
        task_error_data(&task, edata);
    }
    if (quiet && edata->elevel == FATAL) edata->output_to_server = false; // not for the log after all, see dest_loud()
    if (emit_log_hook_prev) emit_log_hook_prev(edata);
}

// the hook above sees only what goes to the log, which a FATAL doesn't with log_min_messages = panic: for the input, let one go there, for the hook to fail its task by it, rather than leave it in WORK, to run again on every reset, and to keep it out of the log then
static void dest_loud(void) {
    if (log_min_messages_my <= FATAL) return;
    quiet = log_min_messages_my;
    log_min_messages_my = FATAL;
}

// the level back, unless the input set another one itself
static void dest_quiet(void) {
    if (quiet && log_min_messages_my == FATAL) log_min_messages_my = quiet;
    quiet = 0;
}

// on the way out after such a FATAL, before the connection's own exit callback, as the one removing temporary tables does: abort the input's transaction and fail the task
static void dest_shmem_exit(int code, Datum arg) {
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
    // a task taken and not started, the next one task_live() took, a termination right after the bookkeeping say, or the first one pg_work gave, or within the bookkeeping that would start it, task_work(), its lock taken already, but its WORK not committed: back to PLAN, rather than left in TAKE till the next reset; by SKIP LOCKED, its row held by someone else left so, with no error, which, on the way out, would be FATAL
    // but not one the worker goes down on an error of its own, of its start say, a trigger of the table refusing its WORK, which would come again on every pass: left in TAKE for the reset, as tried again only then
    if (!fatal && !failed && !running && task.shared && task.shared->taken && !task.worked) {
        HOLD_INTERRUPTS();
        AbortOutOfAnyTransaction();
        dest_untake();
        RESUME_INTERRUPTS();
    }
#endif
    if (!code || !fatal) return;
    fatal = false;
    running = false;
    QueryCancelPending = false;
    AbortOutOfAnyTransaction();
    if (task.output.len > (int)TASK_OUTPUT_MAX) task.output.data[task.output.len = pg_mbcliplen(task.output.data, task.output.len, TASK_OUTPUT_MAX)] = '\0';
    (void)task_done(&task, false);
}

// a termination right after the input committed, at the first check for interrupts, as the end of any message logged has, the duration of the statement, say, would leave the task done in WORK, to run again on reset: hold interrupts from its commit on, in local mode, where exec_simple_query() commits it, till the bookkeeping is done, or the next statement of the input starts, see dest_resume()
// not one a utility command makes within itself, as VACUUM does, before it gets to its first table even, or CREATE INDEX CONCURRENTLY, while its portal runs still (exec_simple_query() drops it before its own commit, ActivePortal back to none at the top level): the rest of the command, a lock waited for say, would go on with no timeout, cancel or termination let through, a shutdown waiting for it, and the cost of a VACUUM no longer delayed from 16 on, an interrupt pending
static void dest_xact(XactEvent event, void *arg) {
    if (event != XACT_EVENT_COMMIT || !running || held || (ActivePortal && ActivePortal != &dest_portal)) return; // the portal of spi mode, of no utility command, see dest_execute_spi(), as none
#ifdef HAVE_SPI_INSIDE_NONATOMIC_CONTEXT
    if (SPI_inside_nonatomic_context()) return; // not the commit of a COMMIT a procedure or a DO block runs, the rest of which, no statement of the input starting till its end, would go on with no interrupts, no timeout, cancel or termination let through
#endif
    HOLD_INTERRUPTS();
    held = true;
}

void dest_init(void) {
    RegisterXactCallback(dest_xact, NULL);
    emit_log_hook_prev = emit_log_hook;
    emit_log_hook = dest_emit_log;
    post_parse_analyze_hook_prev = post_parse_analyze_hook;
    post_parse_analyze_hook = dest_analyze;
    before_shmem_exit(dest_shmem_exit, (Datum)0);
}

// work_stop()'s cancel of a task in STOP, sent as SIGUSR2 rather than the SIGINT of pg_cancel_backend(), which stays as it is: it's for the task work_stop() saw this worker hold the lock of, which this worker may be done with by now and running the next one or recording the result, so cancel the query as StatementCancelHandler() does only while running the input of that very task
void dest_cancel(SIGNAL_ARGS) {
    int save_errno = errno;
    if (running && !proc_exit_inprogress && task.shared && task.shared->stopped && task.shared->stop == task.shared->id) {
        InterruptPending = true;
        QueryCancelPending = true;
#ifdef HAVE_SETSID
        (void)kill(-MyProcPid, SIGINT); // and then, as pg_cancel_backend() does, the whole process group of this worker too, for the processes the input started (COPY ... PROGRAM) not to keep it waiting for them to end by themselves: its own SIGINT only cancels the query once more, and one coming too late for the input is dropped, see dest_timeout()
#endif
    }
    SetLatch(MyLatch);
    errno = save_errno;
}

bool dest_timeout(void) {
    bool exit;
    int StatementTimeoutMy = StatementTimeout;
    int StatementTimeoutTask;
    volatile bool released = false, finished = false;
    failed = false; // of this task, see dest_shmem_exit()
#ifdef HOLD_CANCEL_INTERRUPTS
    HOLD_CANCEL_INTERRUPTS(); // a cancel coming from here up to its reset below, after the bookkeeping that starts the task, is for no task, as one coming in between tasks, rather than one failing that start, or, once its WORK is committed, the worker, outside any PG_TRY(), the task left in WORK, to run again on reset, as an elog() of DEBUG1 would let it through, see errfinish(), with log_min_messages that low
#endif
    // an error starting it, rather than a termination, which is FATAL, which no PG_CATCH() sees: told by the error itself, not by the log, which with log_min_messages = panic has none of it
    PG_TRY();
        exit = task_work(&task);
    PG_CATCH();
        failed = true;
        PG_RE_THROW();
    PG_END_TRY();
    if (exit) {
#ifdef HOLD_CANCEL_INTERRUPTS
        RESUME_CANCEL_INTERRUPTS();
#endif
        return true;
    }
    task.skip = 0; // or a task failed before in this worker would hide the command tag of the next one, and with nothing else to output have it deleted
    elog(DEBUG1, "id = " INT64_FORMAT ", timeout = %i, input = %s, count = %i", task.shared->id, task.timeout, task.input, task.count);
    set_ps_display_my("timeout");
    StatementTimeout = task.timeout ? task.timeout : StatementTimeoutMy; // a task without a timeout of its own still runs under the server's statement_timeout, as a remote one does, and as a timeout of its own is capped by it
    StatementTimeoutTask = StatementTimeout;
    QueryCancelPending = false; // a cancel that came in between tasks, held off meanwhile, is for no task, as one coming to an idle backend: before the statement timeout is armed, by SPI_connect_my() in spi mode, not to take its cancel for one, the timeout of the task lost then, its input run with none
    if (task.shared->spi) {
        SPI_connect_my(task.input, InvalidOid);
        BeginInternalSubTransaction(NULL);
    }
#ifdef HOLD_CANCEL_INTERRUPTS
    RESUME_CANCEL_INTERRUPTS(); // see above, and past the start of the transaction and the subtransaction of spi mode, outside any PG_TRY(), which log at DEBUG5, a cancel or a statement timeout coming meanwhile, armed by SPI_connect_my(), to come through only within it, as that of the input
#endif
    PG_TRY();
        dest_loud();
        { const char *name = task_char(&task); if (name) ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("%s is not a single-byte character in encoding \"%s\"", name, GetDatabaseEncodingName()))); }
        running = true;
        if (task.shared->stopped && task.shared->stop == task.shared->id) { InterruptPending = true; QueryCancelPending = true; } // a STOP that came between its start and now, which dest_cancel() dropped, not running yet, and work_stop() sends no more: the input cancelled at once, as the signal would have it
        dest_execute();
        if (task.shared->spi) {
            ReleaseCurrentSubTransaction();
            released = true;
            // the commit of the input, whose deferred triggers and constraints, serialization check or notifications may fail it too, as exec_simple_query() has it in local mode: here, rather than fail the worker, and have the task run again on every reset; and as part of the input still, its deferred triggers to be cancelled, stopped or terminated, with interrupts held off from its very commit on only, see dest_xact(), as in local mode
            SPI_finish_my();
            finished = true;
        }
        running = false;
        dest_quiet();
        if (held) held = false; else HOLD_INTERRUPTS(); // the input done, no termination is to come in between it and its bookkeeping, which would leave the task in WORK, to run again on reset: until the end, see below, if not since its commit already, see dest_xact()
        QueryCancelPending = false; // a cancel that came too late for the input, after its last CHECK_FOR_INTERRUPTS(), isn't meant for the bookkeeping, outside any PG_TRY()
    PG_CATCH();
        dest_lexer_back(); // before the abort of the subtransaction, see dest_lexer_read()
        held = false;
        HOLD_INTERRUPTS(); // as above, the error's errfinish() having let them through again
        running = false;
        dest_quiet();
        QueryCancelPending = false; // a cancel of the input that failed otherwise first, its program killed by the SIGINT dest_cancel() sends with it, say, is for no task any more: rather than fail its bookkeeping, outside any PG_TRY()
        task_error(&task);
        if (task.output.len > (int)TASK_OUTPUT_MAX) task.output.data[task.output.len = pg_mbcliplen(task.output.data, task.output.len, TASK_OUTPUT_MAX)] = '\0'; // past the most it may keep, see TASK_OUTPUT_MAX, as the string buffer would take up to MaxAllocSize
        dest_catch();
        if (task.shared->spi) {
            if (!released) {
                RollbackAndReleaseCurrentSubTransaction();
#if PG_VERSION_NUM < 100000
                SPI_restore_connection();
#endif
            } else if (!finished) {
                SPI_abort_my(); // the commit failed
                finished = true;
            }
        }
    PG_END_TRY();
    if (task.shared->spi && !finished) SPI_finish_my();
    if (task.save && StatementTimeout != StatementTimeoutTask) StatementTimeoutMy = StatementTimeout; // the input set statement_timeout itself, and committed it, a failed one taking it back: the session's for the next tasks now, as save = true keeps the rest of it, and as SHOW tells
    StatementTimeout = StatementTimeoutMy;
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    set_ps_display_my("idle");
    dest_relock(); // the input may have failed, or run in SPI as a whole, after letting go of them
    exit = task_done(&task, true); // the next task taken, if any, told in its slot, see Shared.taken
    if (!exit && !task.save) {
        // the next task, which task_done() took into TAKE already, mustn't stay there, counted against the max of its group, until reset, for a worker that can't reset its session for it and goes: give it back, cleaning up after the error first, as after one of an input
        PG_TRY();
            dest_discard();
        PG_CATCH();
            HOLD_INTERRUPTS(); // as above
            if (task.shared->spi) {
                EmitErrorReport();
                FlushErrorState();
                SPI_abort_my();
            } else dest_catch();
            dest_untake();
            task.shared->taken = false; // back to PLAN already, not to be again on the way out, see dest_shmem_exit()
            exit = true;
        PG_END_TRY();
    }
    if (!exit && ProcDiePending) { dest_untake(); task.shared->taken = false; exit = true; } // to be terminated, now that its interrupts come through: the next task, taken for nothing, back to PLAN
    RESUME_INTERRUPTS();
    return exit;
}
