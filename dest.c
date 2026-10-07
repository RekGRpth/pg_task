#include "include.h"

#include <access/xact.h>
#include <catalog/namespace.h>
#include <mb/pg_wchar.h>
#include <commands/prepare.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <replication/slot.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <unistd.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#if PG_VERSION_NUM >= 110000
#include <jit/jit.h>
#endif

#if PG_VERSION_NUM < 100000
#include <parser/scanner.h>
#endif

static Task task = {0};

Task *get_task(void) {
    return &task;
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

static void headers(TupleDesc tupdesc) {
    task_line(&task);
    for (int col = 1; col <= tupdesc->natts; col++) {
        char *fname = SPI_fname(tupdesc, col);
        if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        appendBinaryStringInfoEscapeQuote(&task.output, fname, strlen(fname), false, task.escape, task.quote);
        pfree(fname);
    }
}

static
#if PG_VERSION_NUM >= 90600
bool
#else
void
#endif
receiveSlot(TupleTableSlot *slot, DestReceiver *self) {
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    if (!task.shared)
        return
#if PG_VERSION_NUM >= 90600
    true
#endif
    ;
    if (!task.output.data) initStringInfoMy(&task.output);
    if (task.header && !task.row && tupdesc->natts > 1) headers(tupdesc);
    task_line(&task);
    for (int col = 1; col <= tupdesc->natts; col++) {
        char *value = SPI_getvalue_my(slot, tupdesc, col);
        if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        if (!value) appendStringInfoString(&task.output, task.null); else {
            appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !init_oid_is_string(SPI_gettypeid(tupdesc, col)) && task.string, task.escape, task.quote);
            pfree(value);
        }
    }
    task.row++;
    if (task.output.len > (int)TASK_OUTPUT_MAX) ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("task output exceeds %lu bytes", (unsigned long)TASK_OUTPUT_MAX)));
#if PG_VERSION_NUM >= 90600
    return true;
#endif
}

static void rStartup(DestReceiver *self, int operation, TupleDesc tupdesc) {
    if (!task.shared) return;
    switch (operation) {
        case CMD_UNKNOWN: elog(DEBUG1, "id = %li, operation = CMD_UNKNOWN", task.shared->id); break;
        case CMD_SELECT: elog(DEBUG1, "id = %li, operation = CMD_SELECT", task.shared->id); break;
        case CMD_UPDATE: elog(DEBUG1, "id = %li, operation = CMD_UPDATE", task.shared->id); break;
        case CMD_INSERT: elog(DEBUG1, "id = %li, operation = CMD_INSERT", task.shared->id); break;
        case CMD_DELETE: elog(DEBUG1, "id = %li, operation = CMD_DELETE", task.shared->id); break;
        case CMD_UTILITY: elog(DEBUG1, "id = %li, operation = CMD_UTILITY", task.shared->id); break;
        case CMD_NOTHING: elog(DEBUG1, "id = %li, operation = CMD_NOTHING", task.shared->id); break;
        default: elog(DEBUG1, "id = %li, operation = %i", task.shared->id, operation); break;
    }
    task.row = 0;
    task.skip = operation == CMD_SELECT;
}

static void rShutdown(DestReceiver *self) {
    if (task.shared) elog(DEBUG1, "id = %li", task.shared->id);
}

static void rDestroy(DestReceiver *self) {
    if (task.shared) elog(DEBUG1, "id = %li", task.shared->id);
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
    if (task.shared) elog(DEBUG1, "id = %li", task.shared->id);
#if PG_VERSION_NUM >= 120000
    return unconstify(DestReceiver *, &myDestReceiver);
#else
    return &myDestReceiver;
#endif
}

static void ReadyForQueryMy(CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = %li", task.shared->id);
}

void NullCommandMy(CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = %li", task.shared->id);
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
    if (task.shared) elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, GetCommandTagName(commandTag));
    dest_resume();
}

void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output) {
    char completionTag[COMPLETION_TAG_BUFSIZE];
    CommandTag tag = qc->commandTag;
    const char *tagname = GetCommandTagName(tag);
    if (!task.shared) return;
    dest_relock();
    if (command_tag_display_rowcount(tag) && !force_undecorated_output) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, tag == CMDTAG_INSERT ? "%s 0 %lu" : "%s %lu", tagname, qc->nprocessed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = %li, completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, completionTag);
    }
}
#else
void BeginCommandMy(const char *commandTag, CommandDest dest) {
    if (task.shared) elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, commandTag);
    dest_resume();
}

void EndCommandMy(const char *commandTag, CommandDest dest) {
    if (!task.shared) return;
    dest_relock();
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, commandTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, commandTag);
    }
}
#endif

// stmt is the parse tree of src, whose command tag is the one local mode and remote mode report, rather than SPI's own result code (UTILITY for any utility statement, say)
// SPI tells of EXECUTE neither the command tag nor the row count of the prepared statement it runs, only UTILITY: run that one as exec_simple_query() does, into the receiver and with the completion of local mode, with a snapshot of its own, as SPI_execute() takes for each statement
static void dest_execute_prepared(const char *src, ExecuteStmt *stmt) {
#if PG_VERSION_NUM >= 130000
    QueryCompletion qc;
    ParseState *pstate = make_parsestate(NULL);
    pstate->p_sourcetext = src;
    InitializeQueryCompletion(&qc);
#else
    char completionTag[COMPLETION_TAG_BUFSIZE] = "";
#endif
    CommandCounterIncrement();
    PushActiveSnapshot(GetTransactionSnapshot());
#if PG_VERSION_NUM >= 130000
    ExecuteQuery(pstate, stmt, NULL, NULL, CreateDestReceiverMy(DestDebug), &qc);
#else
    ExecuteQuery(stmt, NULL, src, NULL, CreateDestReceiverMy(DestDebug), completionTag);
#endif
    PopActiveSnapshot();
    CommandCounterIncrement();
#if PG_VERSION_NUM >= 130000
    free_parsestate(pstate);
    EndCommandMy(&qc, DestDebug, false);
#else
    EndCommandMy(completionTag, DestDebug);
#endif
}

// alone: src is stmt only, rather than the whole input of several statements, which spi mode before 10 runs at once
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
    if (alone && IsA(stmt, ExecuteStmt)) { dest_execute_prepared(src, (ExecuteStmt *)stmt); return; }
#if PG_VERSION_NUM >= 90500
    // CREATE TABLE AS with IF NOT EXISTS of a table that exists does nothing, which SPI tells only as no rows stored
    if (IsA(stmt, CreateTableAsStmt) && ((CreateTableAsStmt *)stmt)->if_not_exists) exists = OidIsValid(RangeVarGetRelid(((CreateTableAsStmt *)stmt)->into->rel, NoLock, true));
#endif
    rc = SPI_execute(src, false, 0);
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
    elog(DEBUG1, "id = %li, commandTag = %s", task.shared->id, tagname);
    if (SPI_tuptable) for (uint64 row = 0; row < SPI_processed; row++) {
        task.skip = 1;
        if (!task.output.data) initStringInfoMy(&task.output);
        if (task.header && !row && SPI_tuptable->tupdesc->natts > 1) headers(SPI_tuptable->tupdesc);
        task_line(&task);
        for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
            char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
            if (col > 1 && task.delimiter) appendStringInfoChar(&task.output, task.delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
            if (!value) appendStringInfoString(&task.output, task.null); else {
                appendBinaryStringInfoEscapeQuote(&task.output, value, strlen(value), !init_oid_is_string(SPI_gettypeid(SPI_tuptable->tupdesc, col)) && task.string, task.escape, task.quote);
                pfree(value);
            }
        }
        if (task.output.len > (int)TASK_OUTPUT_MAX) ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("task output exceeds %lu bytes", (unsigned long)TASK_OUTPUT_MAX)));
    }
    if (count) snprintf(completionTag, COMPLETION_TAG_BUFSIZE, !strcmp(tagname, "INSERT") ? "%s 0 %lu" : "%s %lu", tagname, (unsigned long)SPI_processed);
    else snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s", tagname);
    elog(DEBUG1, "id = %li, completionTag = %s", task.shared->id, completionTag);
    if (task.skip) task.skip = 0; else {
        task_line(&task);
        appendStringInfoString(&task.output, completionTag);
    }
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
    ListCell *stmt, *tree;
    YYLTYPE yylloc;
#if PG_VERSION_NUM >= 90500
    yyextra.escape_string_warning = false; // the parser has warned already, which before 9.5 the scanner does by the setting itself, so twice
#endif
    while ((token = core_yylex(&yylval, &yylloc, scanner))) {
        if (token == ';' && depth <= 0) {
            if (tokens) stmts = lappend(stmts, pnstrdup(src + start, yylloc + 1 - start));
            start = yylloc + 1;
            tokens = false;
            continue;
        }
        if (token == '(') depth++; else if (token == ')') depth--;
        tokens = true;
    }
    scanner_finish(scanner);
    if (tokens) stmts = lappend(stmts, pstrdup(src + start));
    if (list_length(stmts) != list_length(parsetree_list)) { // not to be, but rather than run a statement by the parse tree of another: as SPI_execute() runs them all, reporting the last statement only
        elog(WARNING, "%i statements of %i parse trees", list_length(stmts), list_length(parsetree_list));
        dest_execute_spi(src, (Node *)llast(parsetree_list), false);
        return;
    }
    forboth(stmt, stmts, tree, parsetree_list) dest_execute_spi(lfirst(stmt), (Node *)lfirst(tree), true);
    list_free_deep(stmts);
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
        whereToSendOutput = DestDebug;
        ReadyForQueryMy(whereToSendOutput);
        SetCurrentStatementStartTimestamp();
        exec_simple_query_my(task.input);
        if (IsTransactionState()) {
            exec_simple_query_my(SQL(END));
            if (IsTransactionState()) ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("still active sql transaction")));
        }
    } else {
        List *parsetree_list = pg_parse_query(task.input);
        if (!parsetree_list) return; // no statement at all: nothing to run and no output, as NullCommand in local mode, rather than SPI_execute()'s SPI_OK_REWRITTEN tag
#if PG_VERSION_NUM >= 100000
        // RawStmt.stmt_location lets us slice task.input into the text of each individual
        // statement and run them through SPI one by one, the same way exec_simple_query and
        // the remote libpq path already report a result per statement instead of just the last.
        if (list_length(parsetree_list) <= 1) dest_execute_spi(task.input, linitial_node(RawStmt, parsetree_list)->stmt, true); else {
            ListCell *cell;
            int prev_start = -1;
            Node *prev_stmt = NULL;
            foreach(cell, parsetree_list) {
                int start = lfirst_node(RawStmt, cell)->stmt_location;
                if (prev_start >= 0) {
                    char *stmt = pnstrdup(task.input + prev_start, start - prev_start);
                    dest_execute_spi(stmt, prev_stmt, true);
                    pfree(stmt);
                }
                prev_start = start;
                prev_stmt = lfirst_node(RawStmt, cell)->stmt;
            }
            dest_execute_spi(task.input + prev_start, prev_stmt, true);
        }
#else
        if (list_length(parsetree_list) <= 1) dest_execute_spi(task.input, (Node *)linitial(parsetree_list), true); else dest_execute_split(task.input, parsetree_list);
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
    task_search_path_reset();
    SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
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
    if (!code || !fatal) return;
    fatal = false;
    running = false;
    QueryCancelPending = false;
    AbortOutOfAnyTransaction();
    if (task.output.len > (int)TASK_OUTPUT_MAX) task.output.data[task.output.len = pg_mbcliplen(task.output.data, task.output.len, TASK_OUTPUT_MAX)] = '\0';
    (void)task_done(&task, false);
}

// a termination right after the input committed, at the first check for interrupts, as the end of any message logged has, the duration of the statement, say, would leave the task done in WORK, to run again on reset: hold interrupts from its commit on, in local mode, where exec_simple_query() commits it, till the bookkeeping is done, or the next statement of the input starts, see dest_resume()
static void dest_xact(XactEvent event, void *arg) {
    if (event != XACT_EVENT_COMMIT || !running || held) return;
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
    before_shmem_exit(dest_shmem_exit, (Datum)0);
}

// work_stop()'s cancel of a task in STOP, sent as SIGUSR2 rather than the SIGINT of pg_cancel_backend(), which stays as it is: it's for the task work_stop() saw this worker hold the lock of, which this worker may be done with by now and running the next one or recording the result, so cancel the query as StatementCancelHandler() does only while running the input of that very task
void dest_cancel(SIGNAL_ARGS) {
    int save_errno = errno;
    if (running && !proc_exit_inprogress && task.shared && task.shared->stop && task.shared->stop == task.shared->id) {
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
    if (task_work(&task)) return true;
    task.skip = 0; // or a task failed before in this worker would hide the command tag of the next one, and with nothing else to output have it deleted
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task.shared->id, task.timeout, task.input, task.count);
    set_ps_display_my("timeout");
    StatementTimeout = task.timeout ? task.timeout : StatementTimeoutMy; // a task without a timeout of its own still runs under the server's statement_timeout, as a remote one does, and as a timeout of its own is capped by it
    StatementTimeoutTask = StatementTimeout;
    if (task.shared->spi) {
        SPI_connect_my(task.input, InvalidOid);
        BeginInternalSubTransaction(NULL);
    }
    PG_TRY();
        SetConfigOption("search_path", task_search_path(), PGC_USERSET, PGC_S_SESSION);
        QueryCancelPending = false; // a cancel that came in between tasks, held off meanwhile, is for no task, as one coming to an idle backend
        dest_loud();
        running = true;
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
        if (task.save) task_search_path_save();
        SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
    PG_CATCH();
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
        // only once the failed (sub)transaction is gone, whose abort would take it back to the author's search_path, for the task's bookkeeping to run with: in local mode kept as it's left then, what the input committed itself kept (SET search_path = ...; COMMIT; SELECT 1/0), the rest taken back, as statement_timeout below, and as on a remote connection; in spi mode, where an input commits nothing itself, and where the search_path of the task was set within its subtransaction, which the abort takes back too, to the empty one of the bookkeeping, the one saved before kept
        if (task.save && !task.shared->spi) task_search_path_save();
        SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
    PG_END_TRY();
    if (task.shared->spi && !finished) SPI_finish_my();
    if (task.save && StatementTimeout != StatementTimeoutTask) StatementTimeoutMy = StatementTimeout; // the input set statement_timeout itself, and committed it, a failed one taking it back: the session's for the next tasks now, as save = true keeps the rest of it, and as SHOW tells
    StatementTimeout = StatementTimeoutMy;
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    set_ps_display_my("idle");
    dest_relock(); // the input may have failed, or run in SPI as a whole, after letting go of them
    exit = task_done(&task, true);
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
            task_untake(&task);
            exit = true;
        PG_END_TRY();
    }
    if (!exit && ProcDiePending) { task_untake(&task); exit = true; } // to be terminated, now that its interrupts come through: the next task, taken for nothing, back to PLAN
    RESUME_INTERRUPTS();
    return exit;
}
