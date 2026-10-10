#include "include.h"

#include <signal.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_authid.h>
#include <catalog/pg_collation.h>
#include <libpq/libpq-be.h>
#include <parser/parse_type.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/pmsignal.h>
#include <storage/proc.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/timeout.h>

#ifdef GP_VERSION_NUM
#ifdef HAVE_CREATING_EXTENSION_LOCAL
#include <commands/extension.h>
#endif
#endif

#if PG_VERSION_NUM < 90600
#include "latch_my.h"
#endif

#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#else
#include <access/hash.h>
#endif

#if PG_VERSION_NUM >= 120000
#include <access/relation.h>
#endif

#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif

#if PG_VERSION_NUM < 140000
#include <utils/timestamp.h>
#endif

#if PG_VERSION_NUM < 150000
#include <utils/rel.h>
#endif

#if PG_VERSION_NUM >= 100000
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents) WaitEventSetWait(set, timeout, occurred_events, nevents, PG_WAIT_EXTENSION)
#else
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents) WaitEventSetWait(set, timeout, occurred_events, nevents)
#endif

#if PG_VERSION_NUM >= 170000
#define CreateWaitEventSetMy(nevents) CreateWaitEventSet(NULL, nevents)
#else
#define CreateWaitEventSetMy(nevents) CreateWaitEventSet(TopMemoryContext, nevents)
#endif

#if PG_VERSION_NUM >= 90600
#define PG_DIAG_SEVERITY_MY PG_DIAG_SEVERITY_NONLOCALIZED
#else
#define PG_DIAG_SEVERITY_MY PG_DIAG_SEVERITY
#endif

#if PG_VERSION_NUM >= 190000
#include <storage/fd.h>
#endif

typedef struct Local {
    BackgroundWorkerHandle *handle;
    dlist_node node;
    int hash;
    int64 id;
    int pid;
} Local;

static dlist_head local; // started task workers whose slot we hold too, until they exit: see work_local()
static dlist_head pending; // remote tasks done, their connections closed, whose bookkeeping waits for a row someone else holds, see work_bookkeeping()
static dlist_head remote;

#ifdef LIBPQ_HAS_ASYNC_CANCEL
// a cancel request in flight, sent asynchronously, as PQcancel() would block pg_work and every task it runs until it gets through, for as long as the TCP timeout to a server that doesn't answer
typedef struct Cancel {
    dlist_node node;
    int event;
    int64 id;
    PGcancelConn *conn;
    TimestampTz deadline;
} Cancel;

static dlist_head cancels;
#endif
#define WORK_CANCEL_TIMEOUT 10000 // milliseconds a cancel request, a single packet, may take to get through, or, sent, to take effect, see work_failed()
#define WORK_PENDING_TIMEOUT 5000 // milliseconds the bookkeeping put off may take on the exit of pg_work, see work_shmem_exit()
static volatile uint64 idle_count = 0;
static volatile sig_atomic_t woken = false; // by the wake-up trigger, see work_idle()
static Work work = {0};

Work *get_work(void) {
    return &work;
}

static bool work_bookkeeping(Task *t, bool live, bool *exit);
static void work_defer(Task *t);
static void work_discard(Task *t);
static void work_drain(Task *t);
static void work_fail(Task *t);
static void work_later(Task *t, const char *message, const char *setting);
static bool work_cancel(Task *t);
static bool work_next(Task *t, const char *error);
static void work_query(Task *t);
#ifdef LIBPQ_HAS_ASYNC_CANCEL
static void work_cancel_free(Cancel *c);
#endif
static bool work_reap(const Work *w);
static void work_result(Task *t);
static void work_stop(const Work *w);
static bool work_superuser(const char *user);
static bool work_verify(Task *t);

#define work_error(...) do { \
    bool work_error_exit PG_USED_FOR_ASSERTS_ONLY; \
    bool work_error_remote = t->remote != NULL || t->conn != NULL; /* the next task of a connection, taken by task_live(), its remote freed by task_free() and not read anew yet, see task_work(), its row held, say */ \
    MemoryContext work_error_context = CurrentMemoryContext; \
    PG_TRY(); \
        ereport(ERROR, __VA_ARGS__); \
    PG_CATCH(); \
        MemoryContextSwitchTo(work_error_context); /* out of ErrorContext, the rest to run in, the bookkeeping say, rather than in one that the next error resets */ \
        task_error(t); \
        EmitErrorReport(); \
        FlushErrorState(); \
    PG_END_TRY(); \
    (void)work_verify(t); /* as a task done would have them, see work_encoding(): a remote task's output so far, and the server's messages, its connection broken say */ \
    if (!work_error_remote) { work_error_exit = task_done(t, false); /* with live = false nothing new is taken into t, so it can be dropped */ if (t->held) ereport(WARNING, (errmsg("id = %li, its row held by someone else, its error not recorded, the task left in TAKE for the next reset", t->shared->id))); work_free(t); } \
    else if (work_bookkeeping(t, false, &work_error_exit)) work_finish(t); \
    else work_defer(t); \
} while(0)

// the connection of a remote task broke: on its way from the task done to the next one, which task_done() took into TAKE already, as work_discard() is, or before its start, its row held by someone else, see task_work(), as work_query() is, the task, never run, isn't to fail for it but to go back to PLAN, as work_discard() has it when DISCARD ALL fails, or, its row held still, to be left in TAKE for the next reset, its connection closed and the slot of its group freed either way
#define work_broken(...) do { \
    if (t->socket == work_discard || t->socket == work_query) { \
        ereport(WARNING, __VA_ARGS__); \
        task_untake(t); \
        work_finish(t); \
    } else work_error(__VA_ARGS__); \
} while(0)

static
#if PG_VERSION_NUM >= 120000 && defined(GP_VERSION_NUM)
void
#else
int
#endif
work_errdetail(const char *err) {
    int len;
    if (!err)
#if PG_VERSION_NUM >= 120000 && defined(GP_VERSION_NUM)
        return;
#else
        return 0;
#endif
    len = strlen(err);
    if (!len)
#if PG_VERSION_NUM >= 120000 && defined(GP_VERSION_NUM)
        return;
#else
        return 0;
#endif
    if (err[len - 1] == '\n') len--;
    return errdetail("%.*s", len, err);
}

static
#if PG_VERSION_NUM >= 120000 && defined(GP_VERSION_NUM)
void
#else
int
#endif
work_errhint(const char *hint) {
    if (!hint || !hint[0])
#if PG_VERSION_NUM >= 120000 && defined(GP_VERSION_NUM)
        return;
#else
        return 0;
#endif
    return errhint("%s", hint);
}

static void work_check(const Work *w) {
    bool ok = true;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return;
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(&src);
        init_settings(&src);
        appendStringInfo(&src, SQL(
            SELECT DISTINCT ON ("data", "user", "schema", "table") j.* FROM j WHERE "user" OPERATOR(pg_catalog.=) session_user AND "data" OPERATOR(pg_catalog.=) current_catalog AND pg_catalog.hashtext(pg_catalog.concat_ws('.', pg_catalog.length("schema"), "schema", "table"))::pg_catalog.int4 OPERATOR(pg_catalog.=) %i
            ORDER BY "data", "user", "schema", "table", "i"
        ), w->shared->hash);
    }
    // as pg_conf does, keep running with the settings as they are rather than exit over a pg_task.json that doesn't parse or fit the types of its keys
    PG_TRY();
        SPI_connect_my(src.data,
#if PG_VERSION_NUM >= 90500
            BOOTSTRAP_SUPERUSERID // pg_file_settings is for superusers only, while pg_task.user may be none: as pg_conf, which runs as one, reads it too; the query is pg_task's own, reading the catalogs only, and the one it serves is the session user
#else
            InvalidOid
#endif
        );
        if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
        SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
        if (!SPI_processed) ShutdownRequestPending = true; else {
            HeapTuple val = SPI_tuptable->vals[0];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false, INT8OID));
            { bool isnull; Datum run = SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "run"), &isnull); w->shared->run = Max(isnull ? init_int(val, tupdesc, "run_setting", "pg_task.run") : DatumGetInt32(run), 1); } // the key of pg_task.json, or else the setting, 1 at least, as the setting has it, see conf_check()
            { bool isnull; Datum sleep = SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "sleep"), &isnull); w->shared->sleep = Max(isnull ? init_int(val, tupdesc, "sleep_setting", "pg_task.sleep") : DatumGetInt64(sleep), 1); }
            w->shared->spi = DatumGetBool(SPI_getbinval_my(val, tupdesc, "spi", false, BOOLOID));
            w->shared->limit = init_int(val, tupdesc, "limit", "pg_task.limit");
            elog(DEBUG1, "sleep = %li, reset = %li, schema = %s, table = %s, run = %i, spi = %s, limit = %i, SPI_processed = %lu", w->shared->sleep, w->shared->reset, w->shared->schema, w->shared->table, w->shared->run, w->shared->spi ? "true" : "false", w->shared->limit, (long)SPI_processed);
            SPI_freetuple(val);
        }
        SPI_finish_my();
    PG_CATCH();
        MemoryContextSwitchTo(oldMemoryContext);
        EmitErrorReport();
        FlushErrorState();
        SPI_abort_my();
        ok = false;
    PG_END_TRY();
    if (!ok) ereport(WARNING, (errmsg("pg_task.json not applied, keeping the previous settings")));
    set_ps_display_my("idle");
}

static void work_command(Task *t, PGresult *result) {
    if (t->skip) { t->skip--; return; }
    task_line(t);
    appendStringInfoString(&t->output, PQcmdStatus(result));
}

// returns the position of the first cancel request among the events, after those of the remote tasks
static int work_events(WaitEventSet *set) {
    dlist_mutable_iter iter;
    int pos = 2;
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        AddWaitEventToSet(set, t->event, PQsocket(t->conn), NULL, t);
        pos++;
    }
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_foreach_modify(iter, &cancels) {
        Cancel *c = dlist_container(Cancel, node, iter.cur);
        AddWaitEventToSet(set, c->event, PQcancelSocket(c->conn), NULL, c);
    }
#endif
    return pos;
}

static char *work_severity(const PGresult *result) {
    char *severity = PQresultErrorField(result, PG_DIAG_SEVERITY_MY);
    return severity ? severity : PQresultErrorField(result, PG_DIAG_SEVERITY); // older remote servers don't send the nonlocalized field
}

static void work_fatal(Task *t, const PGresult *result) {
    char *value = NULL;
    char *value2 = NULL;
    char *value3 = NULL;
    if (!t->output.data) initStringInfoMy(&t->output);
    if (!t->error.data) initStringInfoMy(&t->error);
    t->skip++;
    if (t->error.len) appendStringInfoChar(&t->error, '\n');
    if ((value = work_severity(result))) appendStringInfo(&t->error, "%s:  ", _(error_severity(severity_error(value))));
    if (Log_error_verbosity >= PGERROR_VERBOSE && (value = PQresultErrorField(result, PG_DIAG_SQLSTATE))) appendStringInfo(&t->error, "%s: ", value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY))) append_with_tabs(&t->error, value);
    else append_with_tabs(&t->error, _("missing error text"));
    if ((value = PQresultErrorField(result, PG_DIAG_STATEMENT_POSITION))) appendStringInfo(&t->error, _(" at character %s"), value);
    else if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_POSITION))) appendStringInfo(&t->error, _(" at character %s"), value);
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL))) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("DETAIL:  "));
            append_with_tabs(&t->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT))) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("HINT:  "));
            append_with_tabs(&t->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_QUERY))) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("QUERY:  "));
            append_with_tabs(&t->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_CONTEXT))) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("CONTEXT:  "));
            append_with_tabs(&t->error, value);
        }
        if (Log_error_verbosity >= PGERROR_VERBOSE) {
            value2 = PQresultErrorField(result, PG_DIAG_SOURCE_FILE);
            value3 = PQresultErrorField(result, PG_DIAG_SOURCE_LINE);
            if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION)) && value2) { // assume no newlines in funcname or filename...
                if (t->error.len) appendStringInfoChar(&t->error, '\n');
                appendStringInfo(&t->error, _("LOCATION:  %s, %s:%s"), value, value2, value3);
            } else if (value2) {
                if (t->error.len) appendStringInfoChar(&t->error, '\n');
                appendStringInfo(&t->error, _("LOCATION:  %s:%s"), value2, value3);
            }
        }
    }
    if (is_log_level_output(severity_error(work_severity(result)), log_min_error_statement)) { // If the user wants the query that generated this error logged, do it.
        if (t->error.len) appendStringInfoChar(&t->error, '\n');
        appendStringInfoString(&t->error, _("STATEMENT:  "));
        append_with_tabs(&t->error, t->input);
    }
}

static void work_free(Task *t) {
    dlist_delete(&t->node);
    task_free(t);
    pfree(t->shared);
    pfree(t);
}

static void work_unreserve(Task *t) {
    if (t->reserve && !unlock_table_id_hash(t->shared->oid, t->shared->id, t->shared->hash)) ereport(WARNING, (errmsg("!unlock_table_id_hash(%i, %li, %i)", t->shared->oid, t->shared->id, t->shared->hash)));
    t->reserve = false;
}

// the bookkeeping of a remote task, in pg_work, which takes no row someone else holds, for a while, say, see task_done(), as every other remote task, the taking of tasks and their cancels would wait for it, or pg_work fail on a deadlock, with all of them: false, the bookkeeping put off, to be tried again, see work_pending()
static bool work_bookkeeping(Task *t, bool live, bool *exit) {
    ErrorData *edata;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    volatile bool done = true;
    PG_TRY();
        *exit = task_done(t, live);
        if (t->held) { t->held = false; done = false; } // its row held, see task_done()
    PG_CATCH();
        MemoryContextSwitchTo(oldMemoryContext);
        edata = CopyErrorData();
        if (edata->sqlerrcode != ERRCODE_LOCK_NOT_AVAILABLE && edata->sqlerrcode != ERRCODE_T_R_DEADLOCK_DETECTED) ReThrowError(edata);
        FlushErrorState();
        SPI_abort_my();
        // the bookkeeping committed, the taking of the next task failed, without SKIP LOCKED waiting for its row, see task_live(): no row of the task held, nothing to put off, only no next task taken
        if (t->booked) { ereport(WARNING, (errmsg("id = %li, no next task taken", t->shared->id), errdetail("%s", edata->message))); *exit = true; } else done = false;
        FreeErrorData(edata);
    PG_END_TRY();
    return done;
}

static void work_finish(Task *t) {
    if (!proc_exit_inprogress) work_unreserve(t);
    if (t->conn) {
        PQfinish(t->conn);
#if PG_VERSION_NUM >= 130000
        ReleaseExternalFD();
#endif
    }
    if (!proc_exit_inprogress && t->key && !unlock_table_pid_hash(t->shared->oid, t->key, t->shared->hash)) ereport(WARNING, (errmsg("!unlock_table_pid_hash(%i, %i, %i)", t->shared->oid, t->key, t->shared->hash)));
    idle_count = 0; // a slot of its group is free now, for a task of the group that waits for one, which an idle pg_work doesn't wait for: see work_reap()
    work_free(t);
}

// the bookkeeping put off: the connection closed, the task done with it, and the slot of its group freed, but for a negative max, but the task kept, with the lock of its id, for work_reset() to leave it be, till work_pending() records it
static void work_defer(Task *t) {
    ereport(WARNING, (errmsg("id = %li, its row held by someone else, its bookkeeping put off", t->shared->id)));
    if (t->conn) {
        PQfinish(t->conn);
#if PG_VERSION_NUM >= 130000
        ReleaseExternalFD();
#endif
        t->conn = NULL;
    }
    // a pause, of a negative max, scheduled by the bookkeeping only, see task_done(): the slot of the group held till then, by the locks of the connection gone, for no next task of the group to be taken before the pause, see work_pending()
    if (t->shared->max >= 0) {
        work_unreserve(t);
        if (t->key && !unlock_table_pid_hash(t->shared->oid, t->key, t->shared->hash)) ereport(WARNING, (errmsg("!unlock_table_pid_hash(%i, %i, %i)", t->shared->oid, t->key, t->shared->hash)));
        t->key = 0;
        idle_count = 0;
    }
    dlist_delete(&t->node);
    dlist_push_tail(&pending, &t->node);
}

// the remote tasks connected whose rows someone else held as they were to start, see task_work(), tried again
static void work_held(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (!t->held || t->socket != work_query) continue;
        // its connection closed meanwhile by the server, idle_session_timeout say, its FATAL read already maybe, and the end of it now: back to PLAN, see work_broken(), rather than started on it, to fail, never run
        if (PQstatus(t->conn) != CONNECTION_OK || !PQconsumeInput(t->conn)) { work_broken((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
        t->held = false;
        work_query(t);
    }
}

// whether a remote task waits for its row, its bookkeeping put off, or its start, see work_held(), for the loop to wake up for it once a sleep
static bool work_waiting(void) {
    dlist_iter iter;
    if (!dlist_is_empty(&pending)) return true;
    dlist_foreach(iter, &remote) if (dlist_container(Task, node, iter.cur)->held) return true;
    return false;
}

// the bookkeeping put off tried again, with no next task to take, the connection gone
static void work_pending(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &pending) {
        Task *t = dlist_container(Task, node, iter.cur);
        bool exit;
        if (work_bookkeeping(t, false, &exit)) work_finish(t); // the slot of its group freed too, if still held, see work_defer()
    }
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 2;
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_BAD) { work_broken((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
        if (PQsocket(t->conn) == PGINVALID_SOCKET) { work_broken((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsocket == PGINVALID_SOCKET"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
        // a nonblocking connection sends only what the socket takes and keeps the rest of a long query or input, which the server waits for: send more of it whenever the socket is writable again, reading what the server answers meanwhile
        if (PQstatus(t->conn) == CONNECTION_OK) switch (PQflush(t->conn)) {
            case -1: work_broken((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQflush failed"), work_errdetail(PQerrorMessage(t->conn)))); continue;
            case 0: t->event &= ~WL_SOCKET_WRITEABLE; break;
            default: t->event |= WL_SOCKET_WRITEABLE; break;
        }
        nevents++;
    }
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_foreach_modify(iter, &cancels) {
        Cancel *c = dlist_container(Cancel, node, iter.cur);
        if (PQcancelStatus(c->conn) == CONNECTION_BAD || PQcancelSocket(c->conn) == PGINVALID_SOCKET) { ereport(WARNING, (errmsg("id = %li, cancel failed", c->id), work_errdetail(PQcancelErrorMessage(c->conn)))); work_cancel_free(c); continue; }
        nevents++;
    }
#endif
    return nevents;
}

// milliseconds until the soonest deadline of a remote task connecting or of a cancel request, -1 for none
static long work_deadline(void) {
    dlist_iter iter;
    long secs;
    int usecs;
    TimestampTz soonest = 0;
    dlist_foreach(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (t->deadline && (!soonest || t->deadline < soonest)) soonest = t->deadline;
    }
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_foreach(iter, &cancels) {
        Cancel *c = dlist_container(Cancel, node, iter.cur);
        if (!soonest || c->deadline < soonest) soonest = c->deadline;
    }
#endif
    if (!soonest) return -1;
    TimestampDifference(GetCurrentTimestamp(), soonest, &secs, &usecs);
    return secs * 1000 + (usecs + 999) / 1000; // rounded up, so as not to wake up just before
}

// a remote task still connecting past the connect_timeout of its connection string fails, as a synchronous connection would, or goes on to the next host, see work_next(); a cancel request still not through past its time is given up
static void work_expire(void) {
    dlist_mutable_iter iter;
    TimestampTz now = GetCurrentTimestamp();
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        char *error;
        if (!t->deadline || t->deadline > now) continue;
        // the cancel of the rest of its input, past the most of output, see work_failed(), not through, or not taking effect, sent to another server behind a balancer say: the rest left unread, its connection closed, rather than drained for as long as it runs
        if (t->socket == work_drain) { ereport(WARNING, (errmsg("id = %li, the rest of its input not cancelled in time, its connection closed", t->shared->id))); t->deadline = 0; work_fail(t); continue; }
        error = t->hosts ? psprintf("connection to server at \"%s\", port %s failed: timeout expired", PQhost(t->conn), PQport(t->conn)) : NULL;
        if (work_next(t, error)) { pfree(error); continue; }
        if (error) pfree(error);
        if (t->failed) work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("timeout expired"), work_errdetail(t->failed)));
        else work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("timeout expired"), errdetail("Connecting to the remote server took longer than the connect_timeout of its connection string.")));
    }
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_foreach_modify(iter, &cancels) {
        Cancel *c = dlist_container(Cancel, node, iter.cur);
        if (c->deadline <= now) { ereport(WARNING, (errmsg("id = %li, cancel request timed out", c->id))); work_cancel_free(c); }
    }
#endif
}

// a task isn't abandoned only for not being locked by task_work() yet: skip the ones we are still starting, a remote one connecting or between tasks of its connection, a local one whose task worker is starting, or they'd be taken again while we still run them
// in two steps, the ids of the tasks pg_work and the task workers start taken only once the tasks left in TAKE or WORK with no lock are found: a task worker taking the next task of its group, see task_live(), has its id in its slot before it commits it into TAKE, but, its lock taken only after that, see task_work(), one committed into TAKE between the ids taken and the tasks found would be put back to PLAN, the worker exiting on it and the task run by the next one; a task found in TAKE or WORK by the first step was there before the ids are taken
static void work_reset(const Work *w) {
    char *found;
    Datum values[2];
    dlist_iter iter;
    Portal portal;
    StringInfoData ids;
    static Oid argtypes[] = {TEXTOID, TEXTOID};
    static SPIPlanPtr found_plan = NULL;
    static SPIPlanPtr plan = NULL;
    static StringInfoData found_src = {0};
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return; // as work_sleep() does
    set_ps_display_my("reset");
    work_reap(w);
    // the lock of a task is tagged by the high and the low 32 bits of its id, unsigned (see lock_table_id()), the low ones as id & 4294967295, here as in work_timeout() and work_stop(): an arithmetic id << 32 >> 32 would extend their sign, into a negative oid for half the ids, and an error taking pg_work down
    if (!found_src.data) {
        initStringInfoMy(&found_src);
        appendStringInfo(&found_src, SQL(
            SELECT pg_catalog.array_agg("id")::pg_catalog.text AS "found" FROM %1$s AS t LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$u AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.&) 4294967295)
            WHERE "state" OPERATOR(pg_catalog.=) ANY(ARRAY['TAKE', 'WORK']::%3$s[]) AND l.pid IS NULL
        ), w->schema_table, init_table_key(w->shared->oid), w->schema_type);
    }
    SPI_connect_my(found_src.data, InvalidOid);
    if (!found_plan) found_plan = SPI_prepare_my(found_src.data, 0, NULL);
    SPI_execute_plan_my(found_src.data, found_plan, NULL, NULL, SPI_OK_SELECT);
    if (SPI_processed != 1 || !(found = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "found", true, TEXTOID)))) { SPI_finish_my(); set_ps_display_my("idle"); return; } // none, as mostly
    SPI_finish_my();
    initStringInfoMy(&ids);
    appendStringInfoChar(&ids, '{');
    dlist_foreach(iter, &local) appendStringInfo(&ids, "%s%li", ids.len > 1 ? "," : "", dlist_container(Local, node, iter.cur)->id);
    dlist_foreach(iter, &remote) appendStringInfo(&ids, "%s%li", ids.len > 1 ? "," : "", dlist_container(Task, node, iter.cur)->shared->id);
    dlist_foreach(iter, &pending) appendStringInfo(&ids, "%s%li", ids.len > 1 ? "," : "", dlist_container(Task, node, iter.cur)->shared->id); // and those whose bookkeeping is put off, one that failed connecting with no lock of its id yet, see task_work(), say, not to be taken again before its bookkeeping, which would fail the task taken by its error then
    init_task_ids(&ids, w->shared->data, w->shared->oid); // and those of task workers an earlier pg_work started, before it was restarted, which hold the lock of their task no longer, if an input let go of it
    appendStringInfoChar(&ids, '}');
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$u AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.&) 4294967295)
                WHERE "state" OPERATOR(pg_catalog.=) ANY(ARRAY['TAKE', 'WORK']::%3$s[]) AND "id" OPERATOR(pg_catalog.=) ANY(($2)::pg_catalog.int8[]) AND "id" OPERATOR(pg_catalog.<>) ALL(($1)::pg_catalog.int8[]) AND l.pid IS NULL FOR NO KEY UPDATE OF t %4$s
            ) UPDATE %1$s AS t SET "state" = 'PLAN', "start" = NULL, "stop" = NULL, "pid" = NULL FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id RETURNING t.id
        ), w->schema_table, init_table_key(w->shared->oid), w->schema_type,
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
            "SKIP LOCKED"
#else
            ""
#endif
        );
    }
    SPI_connect_my(src.data, InvalidOid);
    values[0] = CStringGetTextDatum(ids.data); // in the memory of SPI, freed with it
    values[1] = CStringGetTextDatum(found);
    pfree(found);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    portal = SPI_cursor_open_my(src.data, plan, values, NULL, false);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, init_work_fetch());
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            ereport(WARNING, (errmsg("row = %lu, reset id = %li", row, DatumGetInt64(SPI_getbinval_my(val, SPI_tuptable->tupdesc, "id", false, INT8OID)))));
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    pfree(ids.data);
    set_ps_display_my("idle");
}

// reset is how long until the next work_reset(), the one to put a task orphaned in TAKE or WORK (its lock held by no one) back to PLAN: waking up for one any sooner only finds it still there, again and again
static long work_timeout(const Work *w, long reset) {
    Datum values[] = {Int64GetDatum(reset)};
    long timeout;
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("timeout");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
           SELECT COALESCE(LEAST((
                SELECT $1 FROM %1$s AS t
                LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$u AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.&) 4294967295)
                WHERE "state" OPERATOR(pg_catalog.=) ANY(ARRAY['TAKE', 'WORK']::%3$s[]) AND l.pid IS NULL LIMIT 1
           ), pg_catalog.ceil(EXTRACT(epoch FROM ((
                SELECT "plan" OPERATOR(pg_catalog.-) %4$s AS "plan" FROM %1$s WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.>=) %4$s AND pg_catalog.isfinite("plan") ORDER BY 1 LIMIT 1
           )))::pg_catalog.float8 OPERATOR(pg_catalog.*) 1000)::pg_catalog.int8), -1)::pg_catalog.int8 as "min"
        ), w->schema_table, init_table_key(w->shared->oid), w->schema_type, init_plan());
    }
    SPI_connect_my(src.data, InvalidOid);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, NULL, SPI_OK_SELECT);
    timeout = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "min", false, INT8OID)) : -1;
    elog(DEBUG1, "timeout = %li", timeout);
    SPI_finish_my();
    set_ps_display_my("idle");
    return timeout;
}

static void work_reload(const Work *w) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    work_check(w);
}

static void work_latch(const Work *w) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) work_reload(w);
    work_stop(w);
}

// a notice of the remote server, which libpq would print to stderr as it is, past log_min_messages and the format of the log, a NOTICE as a WARNING: logged as one of pg_work's own instead, at its level, as a local task's is, but no higher than WARNING, so as not to fail anything
static void work_notice(void *arg, const PGresult *result) {
    const char *message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY) ? PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY) : PQresultErrorMessage(result); // the whole of it with none of its own, checked as well
    const char *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    const Task *t = arg;
    int elevel = severity_error(work_severity(result));
    if (elevel >= ERROR) elevel = WARNING;
    // in the client_encoding of the connection, which an input may have set to another one than that of this database, as work_verify() has it for the output: not to go into the log as it is then
    for (int i = 0; i < 3; i++) {
        const char *field = i == 0 ? message : PQresultErrorField(result, i == 1 ? PG_DIAG_MESSAGE_DETAIL : PG_DIAG_MESSAGE_HINT);
        if (field && !pg_verifymbstr(field, strlen(field), true)) { ereport(elevel, (errmsg("id = %li, a notice of the remote server not in the encoding of this database", t->shared->id))); return; }
    }
    ereport(elevel, (errcode(sqlstate && strlen(sqlstate) == 5 ? MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2], sqlstate[3], sqlstate[4]) : ERRCODE_WARNING), errmsg_internal("id = %li, %s", t->shared->id, message), work_errdetail(PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL)), work_errhint(PQresultErrorField(result, PG_DIAG_MESSAGE_HINT))));
}

static void work_readable(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && !PQconsumeInput(t->conn)) { work_broken((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), work_errdetail(PQerrorMessage(t->conn)))); return; }
    // the notifications of a LISTEN of the task, which libpq would keep for as long as the connection lives, with save say, a task having nowhere to take them: logged for debug and dropped
    for (PGnotify *notify; (notify = PQnotifies(t->conn)); PQfreemem(notify)) elog(DEBUG1, "id = %li, notification \"%s\" from %i: %s", t->shared->id, notify->relname, notify->be_pid, notify->extra);
    if (t->held && t->socket == work_query) return; // waiting for its row to start, see task_work(), which it does once a sleep only, see work_held(), not on what its connection got, the FATAL of a server closing it say, the end of which comes next
    t->socket(t);
}

// PQgetResult waits for a result until all of it has arrived, holding up pg_work and every other task with it (as the first of several statements would, once its result fills the server's send buffer): until then, wait for the socket to read the rest, and come back to socket
static bool work_busy(Task *t, void (*socket) (Task *t)) {
    if (!PQisBusy(t->conn)) return false;
    t->event = WL_SOCKET_READABLE;
    t->socket = socket;
    return true;
}

// the results came in the client_encoding of the connection, which is that of this database (see work_remote()) unless the input set another one, which, as servers from 14 on tell of it only once the input is through, if at all, can't be told by result, and so did the messages of the server, localized: the output and the error are stored only if they are text of this database at least, one that isn't dropped, for the error of it, after the one the task has, if any, rather than stored as it is, invalid for anything that reads it
static bool work_verify(Task *t) {
    MemoryContext context = CurrentMemoryContext;
    StringInfoData bad = {0};
    if (t->output.data && !pg_verifymbstr(t->output.data, t->output.len, true)) { bad = t->output; t->output.data = NULL; t->output.len = 0; }
    if (t->error.data && !pg_verifymbstr(t->error.data, t->error.len, true)) {
        if (bad.data) pfree(t->error.data); else bad = t->error;
        t->error.data = NULL;
        t->error.len = 0;
    }
    if (!bad.data) return true;
    PG_TRY();
        (void)pg_verifymbstr(bad.data, bad.len, false);
    PG_CATCH();
        MemoryContextSwitchTo(context); // out of ErrorContext, as in work_error()
        task_error(t);
        EmitErrorReport();
        FlushErrorState();
    PG_END_TRY();
    pfree(bad.data);
    return false;
}

// a task done with results that aren't text of this database fails, as inserting them would
static bool work_encoding(Task *t) {
    bool exit;
    if (work_verify(t)) return true;
    if (work_bookkeeping(t, false, &exit)) work_finish(t); else work_defer(t); // and the session with the client_encoding it set
    return false;
}

// a cancel of the task still on its way, sent asynchronously, see work_cancel(), which would cancel whatever runs on the connection once it gets there, the next task of the group say
static bool work_cancelling(const Task *t) {
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_iter iter;
    dlist_foreach(iter, &cancels) if (dlist_container(Cancel, node, iter.cur)->id == t->shared->id) return true;
#endif
    return false;
}

static void work_done(Task *t) {
    bool exit;
    bool live;
    if (PQstatus(t->conn) == CONNECTION_OK && PQtransactionStatus(t->conn) != PQTRANS_IDLE) {
        if (!PQsendQuery(t->conn, SQL(COMMIT))) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), work_errdetail(PQerrorMessage(t->conn)))); return; }
        t->event = WL_SOCKET_READABLE;
        t->socket = work_result;
        t->skip++;
        return;
    }
    if (!work_encoding(t)) return;
    live = PQstatus(t->conn) == CONNECTION_OK && !work_cancelling(t); // take the next task of the group only for a connection to run it on, and one no cancel is on its way to
    if (!work_bookkeeping(t, live, &exit)) { work_defer(t); return; }
    if (exit || !live) { work_finish(t); return; }
    if (t->save) { work_query(t); return; }
    if (!PQsendQuery(t->conn, SQL(DISCARD ALL;))) { ereport(WARNING, (errmsg("id = %li, PQsendQuery failed", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); task_untake(t); work_finish(t); return; }
    t->socket = work_discard;
    t->event = WL_SOCKET_READABLE;
}

static void work_discard(Task *t) {
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK; PQclear(result)) {
        if (work_busy(t, work_discard)) return;
        if (!(result = PQgetResult(t->conn))) break;
        switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK: elog(DEBUG1, "id = %li, %s", t->shared->id, PQcmdStatus(result)); break;
            case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", t->shared->id), work_errdetail(PQresultErrorMessage(result)))); PQclear(result); task_untake(t); work_finish(t); return; // closes the connection, whatever else it was to read
            default: elog(DEBUG1, "id = %li, %s", t->shared->id, PQresStatus(PQresultStatus(result))); break;
        }
    }
    if (PQstatus(t->conn) != CONNECTION_OK) { task_untake(t); work_finish(t); } else work_query(t); // the next task, which task_done() took already, has nothing to run on
}

static void work_headers(Task *t, const PGresult *result) {
    task_line(t);
    for (int col = 0; col < PQnfields(result); col++) {
        if (col > 0 && t->delimiter) appendStringInfoChar(&t->output, t->delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        appendBinaryStringInfoEscapeQuote(&t->output, PQfname(result, col), strlen(PQfname(result, col)), false, t->escape, t->quote);
    }
}

static void work_success(Task *t, const PGresult *result, int row, bool first) {
    if (!t->output.data) initStringInfoMy(&t->output);
    if (t->header && first && PQnfields(result) > 1) work_headers(t, result);
    task_line(t);
    for (int col = 0; col < PQnfields(result); col++) {
        if (col > 0 && t->delimiter) appendStringInfoChar(&t->output, t->delimiter); // none for an empty one, as for quote and escape, rather than a NUL ending the output there
        if (PQgetisnull(result, row, col)) appendStringInfoString(&t->output, t->null);
        else appendBinaryStringInfoEscapeQuote(&t->output, PQgetvalue(result, row, col), PQgetlength(result, row, col), !init_oid_is_string(PQftype(result, col)) && t->string, t->escape, t->quote);
    }
}

// past the most of output a task may keep, see TASK_OUTPUT_MAX, as the string buffer would take up to MaxAllocSize
static void work_output(const Task *t) {
    if (t->output.data && t->output.len > (int)TASK_OUTPUT_MAX) ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("task output exceeds %lu bytes", (unsigned long)TASK_OUTPUT_MAX)));
}

// the task failed on its output, too much of it, with the error taken already: keep the most of it it may, and fail it, rather than take pg_work down, with every remote task it runs, and have the task run again on every reset
static void work_fail(Task *t) {
    bool exit;
    if (!work_encoding(t)) return;
    if (work_bookkeeping(t, false, &exit)) work_finish(t); else work_defer(t); // with the rest of the result unread, if any
}

// the rest of the input, cancelled, read and dropped till the server is through with it, the result of the cancel say, and only then the task failed: with the connection closed at once instead, the rest of the input would go on till it sent something, maybe only once committed, and the cancel, sent asynchronously, through the loop of pg_work, would get there no sooner than the bookkeeping of the task was done, a gigabyte of output to store, after that commit
static void work_drain(Task *t) {
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK; PQclear(result)) {
        if (work_busy(t, work_drain)) return;
        if (!(result = PQgetResult(t->conn))) break;
        switch (PQresultStatus(result)) {
            case PGRES_COPY_BOTH: if (PQputCopyEnd(t->conn, "COPY BOTH is not supported") == -1) ereport(WARNING, (errmsg("id = %li, PQputCopyEnd failed", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); break;
            case PGRES_COPY_IN: if (PQputCopyEnd(t->conn, "COPY FROM STDIN is not supported") == -1) ereport(WARNING, (errmsg("id = %li, PQputCopyEnd failed", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); break;
            case PGRES_COPY_OUT: {
                char *buffer;
                int len;
                while ((len = PQgetCopyData(t->conn, &buffer, true)) > 0) PQfreemem(buffer);
                if (!len) { PQclear(result); t->event = WL_SOCKET_READABLE; t->socket = work_drain; return; } // the rest of it once the socket is readable again
            } break;
            default: ereport(DEBUG1, (errmsg("id = %li, dropped %s", t->shared->id, PQresStatus(PQresultStatus(result))), work_errdetail(PQresultErrorMessage(result)))); break;
        }
    }
    work_fail(t);
}

static void work_failed(Task *t) {
    bool drain = PQstatus(t->conn) == CONNECTION_OK && PQtransactionStatus(t->conn) == PQTRANS_ACTIVE && work_cancel(t); // the input still running on the remote server, cancelled before anything else, the cut of a gigabyte of output to the most of it a task may keep say
    if (t->output.data && t->output.len > (int)TASK_OUTPUT_MAX) t->output.data[t->output.len = pg_mbcliplen(t->output.data, t->output.len, TASK_OUTPUT_MAX)] = '\0';
    if (drain) { t->deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), WORK_CANCEL_TIMEOUT); work_drain(t); } else work_fail(t); // the rest drained for no longer than the cancel may take to get through and take effect, see work_expire()
}

static void work_copy(Task *t) {
    static char *buffer = NULL; // not to be clobbered by an error
    int len = 0;
    volatile bool failed = false;
    volatile bool copied = false;
    MemoryContext context = CurrentMemoryContext;
    if (!t->output.data) initStringInfoMy(&t->output);
    PG_TRY();
        while ((len = PQgetCopyData(t->conn, &buffer, true)) > 0) {
            if (!copied) task_line(t); // on a line of its own, after the output before it, if any
            copied = true;
            appendBinaryStringInfo(&t->output, buffer, len);
            PQfreemem(buffer);
            buffer = NULL;
            work_output(t);
        }
    PG_CATCH();
        MemoryContextSwitchTo(context); // out of ErrorContext, as in work_error()
        task_error(t);
        EmitErrorReport();
        FlushErrorState();
        failed = true;
    PG_END_TRY();
    if (failed) { if (buffer) PQfreemem(buffer); buffer = NULL; work_failed(t); return; }
    if (copied) t->line = false; // its rows end with a newline each, the last one included, for the next line to go on from
    switch (len) {
        case 0: t->event = WL_SOCKET_READABLE; t->socket = work_copy; break;
        case -2: work_error((errmsg("id = %li, PQgetCopyData == -2", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); break;
        default: t->skip++; work_result(t); break;
    }
}

static void work_result(Task *t) {
    MemoryContext context = CurrentMemoryContext;
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK; PQclear(result)) {
        volatile bool copy = false, failed = false;
        if (work_busy(t, work_result)) return;
        if (!(result = PQgetResult(t->conn))) break;
        PG_TRY();
            switch (PQresultStatus(result)) {
                case PGRES_COMMAND_OK: work_command(t, result); break;
                case PGRES_COPY_BOTH: if (PQputCopyEnd(t->conn, "COPY BOTH is not supported") == -1) ereport(WARNING, (errmsg("id = %li, PQputCopyEnd failed", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); break;
                case PGRES_COPY_IN: if (PQputCopyEnd(t->conn, "COPY FROM STDIN is not supported") == -1) ereport(WARNING, (errmsg("id = %li, PQputCopyEnd failed", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); break;
                case PGRES_COPY_OUT: copy = true; break;
                case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", t->shared->id), work_errdetail(PQresultErrorMessage(result)))); work_fatal(t, result); break;
                case PGRES_SINGLE_TUPLE: work_success(t, result, 0, !t->rows++); break; // a row at a time, as single-row mode has them, see work_input(), then their result with none
                case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) { work_success(t, result, row, !row && !t->rows); work_output(t); } t->rows = 0; break;
                default: elog(DEBUG1, "id = %li, %s", t->shared->id, PQresStatus(PQresultStatus(result))); break;
            }
            work_output(t);
        PG_CATCH();
            MemoryContextSwitchTo(context); // out of ErrorContext, as in work_error()
            task_error(t);
            EmitErrorReport();
            FlushErrorState();
            failed = true;
        PG_END_TRY();
        if (failed) { PQclear(result); work_failed(t); return; }
        if (copy) { PQclear(result); work_copy(t); return; }
    }
    work_done(t);
}

static void work_input(Task *t) {
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK; PQclear(result)) {
        if (work_busy(t, work_input)) return;
        if (!(result = PQgetResult(t->conn))) break;
        switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK: elog(DEBUG1, "id = %li, %s", t->shared->id, PQcmdStatus(result)); break;
            case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", t->shared->id), work_errdetail(PQresultErrorMessage(result)))); work_fatal(t, result); break;
            default: elog(DEBUG1, "id = %li, %s", t->shared->id, PQresStatus(PQresultStatus(result))); break;
        }
    }
    // a stop come between the preamble and the input, whose cancel the server, idle there, ignored, the input to run to the end otherwise, never cancelled again: not sent, the task failed as the cancel would have it, as a local one is, see dest_timeout()
    if (!t->error.data && task_char(t)) { // its output made here, see work_output(), as a local one's is: failed before its input runs too, see dest_timeout()
        if (!t->output.data) initStringInfoMy(&t->output);
        initStringInfoMy(&t->error);
        appendStringInfo(&t->error, "%s:  ", _(error_severity(ERROR)));
        if (Log_error_verbosity >= PGERROR_VERBOSE) appendStringInfo(&t->error, "%s: ", unpack_sql_state(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE));
        appendStringInfo(&t->error, _("%s is not a single-byte character in encoding \"%s\""), task_char(t), GetDatabaseEncodingName());
    }
    if (!t->error.data && t->shared->stop == t->shared->id) {
        if (!t->output.data) initStringInfoMy(&t->output);
        initStringInfoMy(&t->error);
        appendStringInfo(&t->error, "%s:  ", _(error_severity(ERROR)));
        if (Log_error_verbosity >= PGERROR_VERBOSE) appendStringInfo(&t->error, "%s: ", unpack_sql_state(ERRCODE_QUERY_CANCELED));
        appendStringInfoString(&t->error, _("canceling statement due to user request"));
    }
    if (t->error.data) { work_done(t); return; }
    if (!PQsendQuery(t->conn, t->input)) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), work_errdetail(PQerrorMessage(t->conn)))); return; }
    // the rows one by one, each looked at against the most of output a task may keep, see work_output(), rather than all of a result in libpq's memory first, many more than that, gigabytes of a remote SELECT, with pg_work and every remote task it runs taken down by running out of it
    if (!PQsetSingleRowMode(t->conn)) ereport(WARNING, (errmsg("id = %li, PQsetSingleRowMode failed", t->shared->id)));
    t->rows = 0;
    t->socket = work_result;
    t->event = WL_SOCKET_READABLE;
}

static void work_query(Task *t) {
    StringInfoData preamble;
    const char *quote_group, *quote_schema, *quote_table;
    if (ShutdownRequestPending) return;
    t->socket = work_query;
    if (task_work(t)) { if (t->held) t->event = WL_SOCKET_READABLE; else work_finish(t); return; } // its row held by someone else, tried again once a sleep, see work_held(), on the connection kept

    initStringInfoMy(&preamble);
    t->skip = 0;
    appendStringInfo(&preamble, SQL(SET SESSION "pg_task.id" = %li;), t->shared->id);
    quote_group = quote_literal_cstr(t->group);
    appendStringInfo(&preamble, SQL(SET SESSION "pg_task.group" = %s;), quote_group);
    if (quote_group != t->group) pfree((void *)quote_group);
    // the names as they are, as a local task has them, not as quoted for SQL, as pg_work keeps them besides
    quote_schema = quote_literal_cstr(t->shared->schema);
    appendStringInfo(&preamble, SQL(SET SESSION "pg_task.schema" = %s;), quote_schema);
    pfree((void *)quote_schema);
    quote_table = quote_literal_cstr(t->shared->table);
    appendStringInfo(&preamble, SQL(SET SESSION "pg_task.table" = %s;), quote_table);
    pfree((void *)quote_table);
    if (t->timeout) appendStringInfo(&preamble, SQL(SET SESSION "statement_timeout" = %i;), t->timeout);
    else appendStringInfoString(&preamble, SQL(RESET "statement_timeout";));
    elog(DEBUG1, "id = %li, timeout = %i, preamble = %s, input = %s, count = %i", t->shared->id, t->timeout, preamble.data, t->input, t->count);
    if (!PQsendQuery(t->conn, preamble.data)) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), work_errdetail(PQerrorMessage(t->conn)))); pfree(preamble.data); return; }
    pfree(preamble.data);
    t->socket = work_input;
    t->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *t) {
    bool connected = false;
    int pid;
    static uint32 key = 0;
    switch (PQstatus(t->conn)) {
        case CONNECTION_BAD: if (!work_next(t, PQerrorMessage(t->conn))) work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(t->failed ? t->failed : PQerrorMessage(t->conn)))); return;
        case CONNECTION_OK: elog(DEBUG1, "id = %li, PQstatus == CONNECTION_OK", t->shared->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(t->conn)) {
        case PGRES_POLLING_ACTIVE: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", t->shared->id); break;
        case PGRES_POLLING_FAILED: if (!work_next(t, PQerrorMessage(t->conn))) work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectPoll failed"), work_errdetail(t->failed ? t->failed : PQerrorMessage(t->conn)))); return;
        case PGRES_POLLING_OK: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_OK", t->shared->id); connected = true; break;
        case PGRES_POLLING_READING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_READING", t->shared->id); t->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_WRITING", t->shared->id); t->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        t->deadline = 0;
        // only now does libpq know whether the server actually asked for the password, and it's the task author who must not be able to connect without one
        if (!work_superuser(t->user) && !PQconnectionUsedPassword(t->conn)) { work_error((errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superuser cannot connect if the server does not request a password."), errhint("Target server's authentication method must be changed."))); return; }
        if (!(pid = PQbackendPID(t->conn))) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQbackendPID failed"), work_errdetail(PQerrorMessage(t->conn)))); return; }
        // by a key of its own, in place of the pid of the connection, as a task worker holds it by its pid: the pid of another server, one of the hosts of the connection string say, may be that of another connection of the group, whose lock the same tag would make one, the slots of the group counted one short, rather than the pid of a process here, which no key takes, from 2^31 on
        if (!++key) key = 1;
        if (!lock_table_pid_hash(t->shared->oid, (int)(key | 0x80000000), t->shared->hash)) { work_error((errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("!lock_table_pid_hash(%i, %i, %i)", t->shared->oid, (int)(key | 0x80000000), t->shared->hash))); return; }
        t->key = (int)(key | 0x80000000);
        t->pid = pid;
        work_unreserve(t); // the slot is now held by the lock of the connection
        work_query(t);
    }
}

#ifdef LIBPQ_HAS_ASYNC_CANCEL
static void work_cancel_free(Cancel *c) {
    dlist_delete(&c->node);
    PQcancelFinish(c->conn);
    pfree(c);
}

static void work_cancel_poll(Cancel *c) {
    switch (PQcancelPoll(c->conn)) {
        case PGRES_POLLING_READING: c->event = WL_SOCKET_READABLE; return;
        case PGRES_POLLING_WRITING: c->event = WL_SOCKET_WRITEABLE; return;
        case PGRES_POLLING_FAILED: ereport(WARNING, (errmsg("id = %li, PQcancelPoll failed", c->id), work_errdetail(PQcancelErrorMessage(c->conn)))); break;
        default: elog(DEBUG1, "id = %li, cancel sent", c->id); break;
    }
    work_cancel_free(c);
}

// on exit there's no loop left to get the cancel requests through: wait for them here, for no longer than timeout milliseconds all together
static void work_cancel_drain(long timeout) {
    dlist_mutable_iter iter;
    TimestampTz end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout);
    while (!dlist_is_empty(&cancels)) {
        Cancel *c = dlist_head_element(Cancel, node, &cancels);
        long secs;
        int usecs;
        TimestampDifference(GetCurrentTimestamp(), end, &secs, &usecs);
        if (!secs && !usecs) break;
        if (WaitLatchOrSocketMy(NULL, c->event | WL_TIMEOUT | WL_POSTMASTER_DEATH, PQcancelSocket(c->conn), secs * 1000 + usecs / 1000) & (WL_TIMEOUT | WL_POSTMASTER_DEATH)) break;
        work_cancel_poll(c);
    }
    dlist_foreach_modify(iter, &cancels) work_cancel_free(dlist_container(Cancel, node, iter.cur));
}
#endif

static bool work_cancel(Task *t) {
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    Cancel *c;
    PGcancelConn *conn;
#else
    char errbuf[256];
    PGcancel *cancel;
#endif
    if (PQstatus(t->conn) != CONNECTION_OK) return false;
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    if (!(conn = PQcancelCreate(t->conn))) { ereport(WARNING, (errmsg("PQcancelCreate failed"))); return false; }
    if (!PQcancelStart(conn)) { ereport(WARNING, (errmsg("PQcancelStart failed"), work_errdetail(PQcancelErrorMessage(conn)))); PQcancelFinish(conn); return false; }
    c = MemoryContextAllocZero(TopMemoryContext, sizeof(*c));
    c->conn = conn;
    c->deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), WORK_CANCEL_TIMEOUT);
    c->event = WL_SOCKET_WRITEABLE;
    c->id = t->shared->id;
    dlist_push_tail(&cancels, &c->node);
#else
    if (!(cancel = PQgetCancel(t->conn))) { ereport(WARNING, (errmsg("PQgetCancel failed"), work_errdetail(PQerrorMessage(t->conn)))); return false; }
    if (!PQcancel(cancel, errbuf, sizeof(errbuf))) { ereport(WARNING, (errmsg("PQcancel failed"), errdetail("%s", errbuf))); PQfreeCancel(cancel); return false; }
    PQfreeCancel(cancel);
#endif
    ereport(WARNING, (errmsg("cancel id = %li", t->shared->id)));
    return true;
}

// on the way out, while the session of pg_work and its locks still are, as this runs before the one ending the session, which lets go of them all, see work_main(): the remote tasks running cancelled first, not to wait for what follows, and the bookkeeping put off, of a remote task done whose row someone else holds, tried again, not to be lost, the task left in WORK, for the next pg_work to run it again on its reset, a second time on its remote server, which the lock of its id, and that of the entry of pg_task.json, keeping the next pg_work from starting, see init_work(), still keep from it meanwhile: till done, for a while, as on a shutdown whoever holds the row is terminated too, from 9.5 on, where it fails on such a row with no error, see task_done(), which, on the way out, would be FATAL
static void work_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    elog(DEBUG1, "code = %i", code);
    AbortOutOfAnyTransaction(); // of a query that a termination cut short, say
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        work_cancel(t);
        work_finish(t);
    }
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    work_cancel_drain(1000);
#endif
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
    if (!dlist_is_empty(&pending)) {
        TimestampTz end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), WORK_PENDING_TIMEOUT);
        for (work_pending(); !dlist_is_empty(&pending) && PostmasterIsAlive() && GetCurrentTimestamp() < end; work_pending()) pg_usleep(100 * 1000L);
    }
#endif
    dlist_foreach_modify(iter, &pending) ereport(WARNING, (errmsg("id = %li, its bookkeeping put off is lost, the task to run again on reset", dlist_container(Task, node, iter.cur)->shared->id)));
}

// the slot of pg_work, last, see work_exit()
static void work_shmem_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
    if (!code) init_free(DatumGetInt32(arg));
}

static void work_stop(const Work *w) {
    dlist_mutable_iter iter;
    instr_time now;
    int64 *current;
    static int64 *cancelled = NULL; // ids already cancelled, so that a task gets a single cancel, not another one while it's already done with its query and recording its result
    static instr_time last;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    static uint64 ncancelled = 0;
    uint64 processed;
    INSTR_TIME_SET_CURRENT(now);
    if (!INSTR_TIME_IS_ZERO(last)) {
        instr_time diff = now;
        INSTR_TIME_SUBTRACT(diff, last);
        if (INSTR_TIME_GET_MILLISEC(diff) < w->shared->sleep) return;
    }
    last = now;
    set_ps_display_my("stop");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            SELECT "id", l."pid" FROM %1$s AS t JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$u AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.&) 4294967295)
            WHERE "state" OPERATOR(pg_catalog.=) 'STOP'
        ), w->schema_table, init_table_key(w->shared->oid));
    }
    SPI_connect_my(src.data, InvalidOid);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
    processed = SPI_processed;
    current = processed ? MemoryContextAlloc(TopMemoryContext, processed * sizeof(*current)) : NULL;
    for (uint64 row = 0; row < processed; row++) { // only STOP tasks still running, i.e. whose lock is held: by us for a remote one, by its task worker for a local one
        bool again = false;
        int64 id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false, INT8OID));
        int pid = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "pid", false, INT4OID));
        current[row] = id;
        for (uint64 i = 0; i < ncancelled; i++) if (cancelled[i] == id) { again = true; break; }
        if (again) continue;
        if (pid == MyProcPid) {
            dlist_foreach_modify(iter, &remote) {
                Task *t = dlist_container(Task, node, iter.cur);
                if (t->shared->id == id) { t->shared->stop = id; work_cancel(t); break; } // marked too, for an input not sent yet not to be, see work_input()
            }
        } else { // a task worker held the lock of this task just now, one that an earlier pg_work may have started too, but may be done with it by now: mark the task in its slot, for it to cancel only that one, along with the processes its input started (see dest_cancel()), with no role checks of pg_cancel_backend() to pass either, as the worker runs as the task author, whom pg_task.user may not signal from SQL
            if (!init_stop(w->shared->data, w->shared->oid, id)) elog(DEBUG1, "id = %li, no longer run by a task worker", id);
            else if (kill(pid, SIGUSR2)) ereport(WARNING, (errmsg("id = %li, could not send signal to process %i: %m", id, pid)));
            else ereport(WARNING, (errmsg("cancel id = %li, pid = %i", id, pid)));
        }
    }
    if (cancelled) pfree(cancelled);
    cancelled = current; // forget the tasks no longer running
    ncancelled = processed;
    SPI_finish_my();
    set_ps_display_my("idle");
}

static bool work_superuser(const char *user) {
    Datum values[1];
    static Oid argtypes[] = {TEXTOID};
    bool result;
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfoString(&src, SQL(
        SELECT COALESCE((SELECT "rolsuper" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) $1), false) AS "test"
    ));
    SPI_connect_my(src.data, InvalidOid);
    values[0] = CStringGetTextDatum(user); // in the memory of SPI, freed with it
    SPI_execute_with_args_my(src.data, countof(argtypes), argtypes, values, NULL, SPI_OK_SELECT);
    result = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "test", false, BOOLOID));
    SPI_finish_my();
    pfree(src.data);
    return result;
}

// the number of entries of a list of the connection string, as libpq splits those of host, hostaddr and port, at commas, with no escaping
static int work_count(const char *list) {
    int count = 1;
    if (!list) return 0;
    for (; *list; list++) if (*list == ',') count++;
    return count;
}

// its entry i
static char *work_entry(const char *list, int i) {
    const char *end;
    for (; i > 0; i--) list = strchr(list, ',') + 1;
    return (end = strchr(list, ',')) ? pnstrdup(list, end - list) : pstrdup(list);
}

// the connection started, to the host tried now, if they are tried one at a time (see work_remote()), with the other options of the connection string as they are
static void work_start(Task *t) {
    char *entry[] = {NULL, NULL, NULL};
    char *err;
    char *options = NULL;
    const char **keywords;
    const char **values;
    int arg = 4;
    int connect_timeout = 0;
    PQconninfoOption *opts = PQconninfoParse(t->remote, &err);
    StringInfoData name, value;
    if (!opts) { work_error((errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("PQconninfoParse failed"), work_errdetail(err))); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        if (!strcmp(opt->keyword, "connect_timeout")) connect_timeout = atoi(opt->val);
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "client_encoding")) continue; // the results go into the task's text columns as they come, so in the encoding of this database only: see below
        if (!strcmp(opt->keyword, "options")) { options = opt->val; continue; }
        arg++;
    }
    keywords = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*keywords));
    values = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*values));
    initStringInfoMy(&name);
    appendStringInfo(&name, "pg_task %s %s %s", t->shared->schema, t->shared->table, t->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(&value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, " -c pg_task.oid=%i", t->shared->oid);
    arg++;
    keywords[arg] = "options";
    values[arg] = value.data;
    arg++;
    keywords[arg] = "client_encoding"; // a startup parameter, which a -c client_encoding in options doesn't override, as those come first
    values[arg] = GetDatabaseEncodingName();
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "client_encoding")) continue;
        if (!strcmp(opt->keyword, "options")) continue;
        arg++;
        keywords[arg] = opt->keyword;
        values[arg] = opt->val;
        if (t->hosts) { // of the lists, only the entry of the host tried now, a port for all of them as it is
            if (!strcmp(opt->keyword, "host")) values[arg] = entry[0] = work_entry(opt->val, t->hosts[t->host]);
            if (!strcmp(opt->keyword, "hostaddr")) values[arg] = entry[1] = work_entry(opt->val, t->hosts[t->host]);
            if (!strcmp(opt->keyword, "port") && work_count(opt->val) > 1) values[arg] = entry[2] = work_entry(opt->val, t->hosts[t->host]);
            if (!strcmp(opt->keyword, "target_session_attrs") && t->standby) values[arg] = t->host < t->standby ? "standby" : "any"; // of prefer-standby, in two rounds, see work_remote()
        }
    }
    arg++;
    keywords[arg] = NULL;
    values[arg] = NULL;
    t->event = WL_SOCKET_MASK;
    t->socket = work_connect;
    t->deadline = connect_timeout > 0 ? TimestampTzPlusMilliseconds(GetCurrentTimestamp(), Max(connect_timeout, 2) * 1000L) : 0; // as libpq takes it, 2 seconds at least, for each host
#if PG_VERSION_NUM >= 130000
    if (!AcquireExternalFD()) work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("could not establish connection"), errdetail("There are too many open files on the local server."), errhint("Raise the server's max_files_per_process and/or \"ulimit -n\" limits."))); else
#endif
    if (!(t->conn = PQconnectStartParams(keywords, values, false))) {
#if PG_VERSION_NUM >= 130000
        ReleaseExternalFD();
#endif
        work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectStartParams failed"), work_errdetail(PQerrorMessage(t->conn))));
    }
    else {
        PQsetNoticeReceiver(t->conn, work_notice, t); // the task's, for as long as the connection lives, the same Task taking the next one of the group on it, see work_done()
        if (PQstatus(t->conn) == CONNECTION_BAD) { if (!work_next(t, PQerrorMessage(t->conn))) work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(t->failed ? t->failed : PQerrorMessage(t->conn)))); }
        else if (!PQisnonblocking(t->conn) && PQsetnonblocking(t->conn, true) == -1) work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetnonblocking failed"), work_errdetail(PQerrorMessage(t->conn))));
    }
    for (int i = 0; i < countof(entry); i++) if (entry[i]) pfree(entry[i]);
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
}

// a host of the connection string failed, tried one at a time, see work_remote(): its error kept, for that of the last one to have them all, as libpq has them, and on to the next one, if any (false for none, the error of the task to be raised then); t may be gone on return, its error raised for the next one
static bool work_next(Task *t, const char *error) {
    StringInfoData failed;
    if (!t->hosts) return false;
    initStringInfoMy(&failed);
    if (t->failed) { appendStringInfoString(&failed, t->failed); pfree(t->failed); }
    if (error) appendStringInfoString(&failed, error);
    if (failed.len && failed.data[failed.len - 1] != '\n') appendStringInfoChar(&failed, '\n');
    t->failed = failed.data;
    if (++t->host >= t->nhosts) return false;
    if (t->conn) {
        PQfinish(t->conn);
#if PG_VERSION_NUM >= 130000
        ReleaseExternalFD();
#endif
        t->conn = NULL;
    }
    work_start(t);
    return true;
}

static void work_remote(Task *t) {
    bool password = false;
    bool prefer_standby = false;
    bool shuffle = false;
    char *err;
    const char *host = NULL, *hostaddr = NULL, *port = NULL;
    int connect_timeout = 0;
    int nhosts;
    PQconninfoOption *opts = PQconninfoParse(t->remote, &err);
    elog(DEBUG1, "id = %li, group = %s, remote = %s, max = %i, oid = %i", t->shared->id, t->group, t->remote ? t->remote : init_null(), t->shared->max, t->shared->oid);
#if PG_VERSION_NUM >= 130000
    // the file descriptors a process may hold for others than files, connections say, are a third of the safe ones, max_files_per_process at most, those of the other remote tasks taking them all: back to PLAN, rather than fail it, with nothing held yet; the one taken here is given back, to be taken again, sure to be had then, right before connecting
    if (!AcquireExternalFD()) { if (opts) PQconninfoFree(opts); if (err) PQfreemem(err); work_later(t, "too many open files", "max_files_per_process"); return; }
#if PG_VERSION_NUM < 140000
    // and one more, left over once connected, for the wait event set of a WaitLatch() of pg_work, which on 13 takes one of these for a set of its own every time, waiting for a task worker to start, or for a lock, say, and errors with none left, taking pg_work down with every remote task it runs; from 14 on it waits on a set made once
    if (!AcquireExternalFD()) { ReleaseExternalFD(); if (opts) PQconninfoFree(opts); if (err) PQfreemem(err); work_later(t, "too many open files", "max_files_per_process"); return; }
    ReleaseExternalFD();
#endif
    ReleaseExternalFD();
#endif
    dlist_delete(&t->node);
    dlist_push_tail(&remote, &t->node);
    // hold the slot of the group from now on, not only once connected, or the next work_sleep() doesn't count it and takes another task of the group over its max
    if (!(t->reserve = lock_table_id_hash(t->shared->oid, t->shared->id, t->shared->hash))) ereport(WARNING, (errmsg("!lock_table_id_hash(%i, %li, %i)", t->shared->oid, t->shared->id, t->shared->hash)));
    if (!opts) { work_error((errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("PQconninfoParse failed"), work_errdetail(err))); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        elog(DEBUG1, "%s = %s", opt->keyword, opt->val);
        // Greengage's libpq turns the connection into an internal one with it, which pg_hba.conf lets through unchecked
        if (!strcmp(opt->keyword, "gpconntype")) { work_error((errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("connection option \"%s\" is not allowed", opt->keyword), errdetail("It makes the connection an internal one, which bypasses pg_hba.conf."))); PQconninfoFree(opts); return; }
        if (!strcmp(opt->keyword, "password") && opt->val[0]) password = true; // not an empty one, which libpq takes for none, looking one up in the password file of the server's own OS user instead, as dblink and postgres_fdw have it
        if (!strcmp(opt->keyword, "connect_timeout")) connect_timeout = atoi(opt->val);
        if (!strcmp(opt->keyword, "host")) host = opt->val;
        if (!strcmp(opt->keyword, "hostaddr")) hostaddr = opt->val;
        if (!strcmp(opt->keyword, "port")) port = opt->val;
        if (!strcmp(opt->keyword, "load_balance_hosts") && !strcmp(opt->val, "random")) shuffle = true;
        if (!strcmp(opt->keyword, "target_session_attrs") && !strcmp(opt->val, "prefer-standby")) prefer_standby = true;
    }
    if (!work_superuser(t->user) && !password) { work_error((errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superusers must provide a password in the connection string."))); PQconninfoFree(opts); return; }
    // libpq doesn't enforce the connect_timeout of an asynchronous connection, which pg_work does then, while a synchronous one applies it to each host, going on to the next one once it's up, as pg_work can't make libpq do: try them one at a time, for a connect_timeout of each, with lists libpq would take, from 10 on, which has none before, in the order it would, random or not, leaving the addresses of a host name to libpq still, within the connect_timeout of the host then rather than of each
    nhosts = Max(work_count(host), work_count(hostaddr));
    if (connect_timeout > 0 && nhosts > 1 && PQlibVersion() >= 100000 && (!host || work_count(host) == nhosts) && (!hostaddr || work_count(hostaddr) == nhosts) && (work_count(port) <= 1 || work_count(port) == nhosts)) {
        // with target_session_attrs=prefer-standby in two rounds, as libpq has it, for a standby first, and only then for any, which libpq, given a single host, would take in its second round at once, a primary before a standby further on
        t->hosts = MemoryContextAlloc(TopMemoryContext, (prefer_standby ? 2 : 1) * nhosts * sizeof(*t->hosts));
        for (int i = 0; i < nhosts; i++) t->hosts[i] = i;
        if (shuffle) for (int i = nhosts - 1; i > 0; i--) { int j = random() % (i + 1); int swap = t->hosts[i]; t->hosts[i] = t->hosts[j]; t->hosts[j] = swap; }
        if (prefer_standby) for (int i = 0; i < nhosts; i++) t->hosts[nhosts + i] = t->hosts[i];
        t->standby = prefer_standby ? nhosts : 0;
        t->nhosts = (prefer_standby ? 2 : 1) * nhosts;
        t->host = 0;
    }
    PQconninfoFree(opts);
    t->start = GetCurrentTimestamp();
    work_start(t);
}

// the task worker connects as the task owner: check here that pg_task.user may act as that role at all, the same as SET ROLE to it would require (the user column alone isn't enough, since its trigger doesn't bind the table owner), and fail the task with a proper error instead of letting its connection die with FATAL and the task hang in TAKE until reset
static bool work_owner(Task *t) {
    bool act = false, login = false, connect = false;
    Datum values[1];
    static Oid argtypes[] = {TEXTOID};
    uint64 processed;
    StringInfoData src;
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        SELECT pg_catalog.pg_has_role(current_user, "oid", '%s') AS "act", "rolcanlogin" AS "login", pg_catalog.has_database_privilege("oid", (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) current_catalog), 'CONNECT') AS "connect" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) $1
    ),
#if PG_VERSION_NUM >= 160000
        "SET"
#else
        "MEMBER"
#endif
    );
    SPI_connect_my(src.data, InvalidOid);
    values[0] = CStringGetTextDatum(t->user); // in the memory of SPI, freed with it
    SPI_execute_with_args_my(src.data, countof(argtypes), argtypes, values, NULL, SPI_OK_SELECT);
    if ((processed = SPI_processed) == 1) {
        act = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "act", false, BOOLOID));
        login = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "login", false, BOOLOID));
        connect = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "connect", false, BOOLOID));
    }
    SPI_finish_my();
    pfree(src.data);
#if PG_VERSION_NUM >= 170000
    login = true; // BGWORKER_BYPASS_ROLELOGINCHECK
#endif
    if (processed != 1) { work_error((errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", t->user))); return false; }
    if (!act) { work_error((errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to run task as role \"%s\"", t->user), errdetail("pg_task.user \"%s\" must be a superuser or be able to SET ROLE to it.", t->shared->user))); return false; }
    if (!login) { work_error((errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION), errmsg("role \"%s\" is not permitted to log in", t->user))); return false; }
    if (!connect) { work_error((errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied for database \"%s\"", t->shared->data), errdetail("User \"%s\" does not have CONNECT privilege.", t->user))); return false; }
    return true;
}

// the task worker takes the slot of its group only once connected (task_main), and a work_sleep() before that doesn't count it and takes another task of the group over its max: hold that very slot for it from its start until it exits (work_reap), its pid counted once
static void work_local(const Task *t, BackgroundWorkerHandle *handle) {
    Local *l;
    if (!lock_table_pid_hash(t->shared->oid, t->pid, t->shared->hash)) { ereport(WARNING, (errmsg("!lock_table_pid_hash(%i, %i, %i)", t->shared->oid, t->pid, t->shared->hash))); pfree(handle); return; }
    l = MemoryContextAllocZero(TopMemoryContext, sizeof(*l));
    l->handle = handle;
    l->hash = t->shared->hash;
    l->id = t->shared->id;
    l->pid = t->pid;
    dlist_push_tail(&local, &l->node);
}

// returns whether a task worker exited, freeing a slot of its group
static bool work_reap(const Work *w) {
    bool reaped = false;
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &local) {
        Local *l = dlist_container(Local, node, iter.cur);
        pid_t pid;
        if (GetBackgroundWorkerPid(l->handle, &pid) == BGWH_STARTED) continue;
        if (!unlock_table_pid_hash(w->shared->oid, l->pid, l->hash)) ereport(WARNING, (errmsg("!unlock_table_pid_hash(%i, %i, %i)", w->shared->oid, l->pid, l->hash)));
        dlist_delete(&l->node);
        pfree(l->handle);
        pfree(l);
        reaped = true;
    }
    return reaped;
}

// no worker to be had for the task now, every one in use, or no file descriptor for the connection of a remote one, see work_remote(), which is no error of the task: back to PLAN, for a pass to take it once one is free, as a task worker exiting or a remote task done wakes pg_work, rather than fail it
static void work_later(Task *t, const char *message, const char *setting) {
    ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("id = %li, %s, to be run later", t->shared->id, message), errhint("Consider increasing configuration parameter \"%s\".", setting)));
    task_untake(t);
    work_free(t);
}

static void work_task(Task *t) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    bool registered;
    MemoryContext oldMemoryContext;
    size_t len;
    elog(DEBUG1, "id = %li, group = %s, max = %i, oid = %i", t->shared->id, t->group, t->shared->max, t->shared->oid);
    if (!work_owner(t)) return;
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name)))); return; }
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name)))); return; }
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", t->shared->user, t->shared->data, t->shared->schema, t->shared->table, t->group)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1))); // do not error when group is to long
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type)))); return; }
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    if ((worker.bgw_main_arg = Int32GetDatum(init_arg(t->shared))) == Int32GetDatum(-1)) { work_later(t, "could not find empty slot", "pg_conf.max"); return; }
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext); // the handle outlives this call, see work_local()
    registered = RegisterDynamicBackgroundWorker(&worker, &handle);
    MemoryContextSwitchTo(oldMemoryContext);
    if (!registered) {
        init_free(worker.bgw_main_arg);
        work_later(t, "could not register background worker", "max_worker_processes");
    } else switch (WaitForBackgroundWorkerStartup(handle, &t->pid)) {
        case BGWH_NOT_YET_STARTED: init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: elog(DEBUG1, "started id = %li", t->shared->id); init_task_pid(DatumGetInt32(worker.bgw_main_arg), t->shared->data, t->shared->oid, t->shared->id, t->pid); work_local(t, handle); handle = NULL; work_free(t); break;
        case BGWH_STOPPED: init_free_task(DatumGetInt32(worker.bgw_main_arg), t->shared->data, t->shared->oid, t->shared->id); work_error((errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void work_sleep(Work *w) {
    Datum values[] = {Int32GetDatum(w->shared->run), Int32GetDatum(w->shared->limit), Int32GetDatum(0), (Datum)0, (Datum)0, (Datum)0};
    dlist_head head;
    dlist_mutable_iter iter;
    Portal portal;
    static Oid argtypes[] = {INT4OID, INT4OID, INT4OID, TEXTOID, TEXTOID, TEXTOID};
    StringInfoData pids, hashes, pauses;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return; // its entry gone from pg_task.json, found by the reload in this very turn of its loop, say: take no task it won't run, to be left in TAKE once it's gone
    elog(DEBUG1, "idle_count = %lu", idle_count);
    set_ps_display_my("sleep");
    work_reap(w);
    values[2] = Int32GetDatum(init_free_slots());
    initStringInfoMy(&pids);
    initStringInfoMy(&hashes);
    appendStringInfoChar(&pids, '{');
    appendStringInfoChar(&hashes, '{');
    init_task_pids(w->shared->data, w->shared->oid, &pids, &hashes);
    appendStringInfoChar(&pids, '}');
    appendStringInfoChar(&hashes, '}');
    initStringInfoMy(&pauses);
    appendStringInfoChar(&pauses, '{');
    (void)init_pauses(w->shared->oid, &pauses);
    appendStringInfoChar(&pauses, '}');
    dlist_init(&head);
#ifdef GP_VERSION_NUM
    if (true) {
        static SPIPlanPtr gp_plan = NULL;
        static StringInfoData gp_src = {0};
        if (!gp_src.data) {
            initStringInfoMy(&gp_src);
            appendStringInfo(&gp_src, SQL(
                UPDATE %1$s SET "state" = 'GONE', "start" = %2$s, "stop" = %2$s, "error" = 'ERROR:  task not active' WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.<=) %2$s AND "repeat" OPERATOR(pg_catalog.=) '0 sec' AND "max" OPERATOR(pg_catalog.>=) 0
            ), w->schema_table, init_plan());
        }
        SPI_connect_my(gp_src.data, InvalidOid);
        if (!gp_plan) gp_plan = SPI_prepare_my(gp_src.data, 0, NULL);
        SPI_execute_plan_my(gp_src.data, gp_plan, NULL, NULL, SPI_OK_UPDATE);
        SPI_finish_my();
    }
#endif
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfoString(&src, "WITH ");
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
        // only the rows no one else holds, for a while, say, as the taking of tasks does: or pg_work would wait for them, taking no task, running no remote one meanwhile, or fail on a deadlock, with every remote task it runs; those go GONE on a later pass
        appendStringInfo(&src, SQL(
            n AS (
                UPDATE %1$s AS t SET "state" = 'GONE', "start" = %2$s, "stop" = %2$s, "error" = 'ERROR:  task not active' FROM (
                    SELECT "id" FROM %1$s WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.<=) %2$s AND "repeat" OPERATOR(pg_catalog.=) '0 sec' AND "max" OPERATOR(pg_catalog.>=) 0 FOR NO KEY UPDATE SKIP LOCKED
                ) AS g WHERE t.id OPERATOR(pg_catalog.=) g.id RETURNING t.id
            ),
        ), w->schema_table, init_plan());
#elif !defined(GP_VERSION_NUM)
        appendStringInfo(&src, SQL(
            n AS (
                UPDATE %1$s SET "state" = 'GONE', "start" = %2$s, "stop" = %2$s, "error" = 'ERROR:  task not active' WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.<=) %2$s AND "repeat" OPERATOR(pg_catalog.=) '0 sec' AND "max" OPERATOR(pg_catalog.>=) 0 RETURNING "id"
            ),
        ), w->schema_table, init_plan());
#endif
        // the slots taken in each group: one per pid holding the slot lock (a task worker and we for it hold the same one), or having a slot of a task worker of the group, which its input can't let go of, as it can of its lock, with pg_advisory_unlock_all() say, and which an earlier pg_work, restarted since, held no more, plus one per remote task not yet connected; the tasks that fit in the slots left in their group, cut there before the limit, not after it, or a group with more tasks due than it has slots for would take up the whole limit and keep the others waiting for the next pass, and of them the local ones that fit in the slots of pg_task free now, each needing a task worker in one, as a remote one doesn't, for no more of them to be taken only to go back to PLAN, and only then locked, as a window function can't be
        appendStringInfo(&src, SQL(
            l AS (
                SELECT pg_catalog.count(DISTINCT CASE WHEN "objsubid" OPERATOR(pg_catalog.=) 5 THEN "classid" END) OPERATOR(pg_catalog.+) pg_catalog.count(CASE WHEN "objsubid" OPERATOR(pg_catalog.=) 7 THEN "classid" END) AS "classid", "objid" FROM (
                    SELECT "classid", "objid", "objsubid" FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessShareLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) ANY(ARRAY[5, 7]) AND "database" OPERATOR(pg_catalog.=) %2$u
                    UNION ALL SELECT ((($4)::pg_catalog.int4[])["i"])::pg_catalog.oid, ((($5)::pg_catalog.int4[])["i"])::pg_catalog.oid, 5::pg_catalog.int2 FROM pg_catalog.generate_subscripts(($4)::pg_catalog.int4[], 1) AS w ("i")
                ) AS l GROUP BY "objid"
            ), c AS (
                SELECT "id", "local", "hash", "count" AS "priority", "count" OPERATOR(pg_catalog.-) pg_catalog.row_number() OVER (PARTITION BY "hash" ORDER BY "count" DESC, "id") OPERATOR(pg_catalog.+) 1 AS "count" FROM (
                    SELECT "id", "remote" IS NULL AS "local", pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%6$s')) AS "hash", CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) AS "count" FROM %1$s AS t LEFT JOIN l ON "objid" OPERATOR(pg_catalog.=) pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%6$s'))
                    WHERE "plan" OPERATOR(pg_catalog.<=) %5$s AND "state" OPERATOR(pg_catalog.=) 'PLAN' AND CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) OPERATOR(pg_catalog.>=) 0 AND ("max" OPERATOR(pg_catalog.>=) 0 OR pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%6$s')) OPERATOR(pg_catalog.<>) ALL(($6)::pg_catalog.int4[]))
                    %4$s
                ) AS c
            ), r AS (
                SELECT "id", "priority", CASE WHEN "local" THEN pg_catalog.row_number() OVER (PARTITION BY "local" ORDER BY "priority" DESC, "id") END AS "slot" FROM c WHERE "count" OPERATOR(pg_catalog.>=) 0
            ), s AS (
                SELECT t.id FROM %1$s AS t JOIN r ON t.id OPERATOR(pg_catalog.=) r.id WHERE COALESCE(r.slot OPERATOR(pg_catalog.<=) $3, true) AND t.state OPERATOR(pg_catalog.=) 'PLAN'
                ORDER BY r.priority DESC, t.id LIMIT GREATEST(LEAST($1 OPERATOR(pg_catalog.-) (SELECT COALESCE(pg_catalog.sum("classid"), 0) FROM l), $2), 0) FOR NO KEY UPDATE OF t %3$s
            ) UPDATE %1$s AS t SET "state" = 'TAKE' FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id RETURNING t.id, pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%6$s')) AS "hash", "group", "remote", "max", ("user")::pg_catalog.text AS "user"
        ), w->schema_table, init_table_key(w->shared->oid),
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
        "SKIP LOCKED"
#else
        ""
#endif
        ,
#ifdef GP_VERSION_NUM
        "",
#else
        SQL(AND "id" NOT IN (SELECT "id" FROM n)),
#endif
        init_plan(), "");
    }
    SPI_connect_my(src.data, InvalidOid);
    values[3] = CStringGetTextDatum(pids.data); // in the memory of SPI, freed with it
    values[4] = CStringGetTextDatum(hashes.data);
    values[5] = CStringGetTextDatum(pauses.data);
    pfree(pids.data);
    pfree(hashes.data);
    pfree(pauses.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    portal = SPI_cursor_open_my(src.data, plan, values, NULL, false);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, init_work_fetch());
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            Task *t = MemoryContextAllocZero(TopMemoryContext, sizeof(Task));
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            t->group = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "group", false, TEXTOID));
            t->remote = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "remote", true, TEXTOID));
            t->user = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "user", false, TEXTOID));
            t->shared = MemoryContextAllocZero(TopMemoryContext, sizeof(Shared));
            *t->shared = *w->shared;
            t->shared->pid = 0; // ours, rather than that of the task worker, which sets it itself, see init_task_pids()
            t->shared->reg = 0; // ours, which tells the slot of a pg_work, rather than of a task worker, see init_work_wake()
            t->work = w;
            t->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false, INT4OID));
            t->shared->id = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "id", false, INT8OID));
            t->shared->max = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "max", false, INT4OID));
            strlcpy(t->shared->owner, t->user, sizeof(t->shared->owner));
            elog(DEBUG1, "row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, t->shared->id, t->shared->hash, t->group, t->remote ? t->remote : init_null(), t->shared->max);
            dlist_push_tail(&head, &t->node);
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    if (dlist_is_empty(&head)) {
        // the wake-up trigger signals from within the transaction that inserts or plans the task, before it commits, so the pass right after may well not see it yet: not one to count towards going idle, which waits with no timeout for a task it doesn't see, but for the next one
        if (woken) woken = false; else idle_count++;
    } else {
        idle_count = 0;
        woken = false;
        dlist_foreach_modify(iter, &head) {
            Task *t = dlist_container(Task, node, iter.cur);
            t->remote ? work_remote(t) : work_task(t);
        }
    }
    set_ps_display_my("idle");
}

static void work_writeable(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && t->socket != work_connect) return; // sending the rest of a query, which work_nevents() does
    t->socket(t);
}

// a wake-up, see the README, rather than a query cancel: but PostgreSQL's statement and lock timeouts signal through SIGINT too, which pg_work's own queries, as a scheduler kept running whatever long transactions of others there are, don't go by, except for the lock timeout of the DDL of make_ddl(), which waits for a table busy with its tasks only so long before retrying
static void work_idle(SIGNAL_ARGS) {
    int save_errno = errno;
    if (make_lock_timeout && get_timeout_indicator(LOCK_TIMEOUT, false)) {
        InterruptPending = true;
        QueryCancelPending = true;
    }
    idle_count = 0;
    woken = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void work_main(Datum main_arg) {
    instr_time current_time_reset;
    instr_time current_time_sleep;
    instr_time start_time_reset;
    instr_time start_time_sleep;
    long current_reset = -1;
    long current_sleep = -1;
    StringInfoData application_name, schema_table, schema_type;
    elog(DEBUG1, "main_arg = %i", DatumGetInt32(main_arg));
    work.shared = init_shared(main_arg);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_DISPATCH;
    optimizer = false;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
    if (!work.shared->in_use) { ereport(LOG, (errmsg("shared slot not in use, waiting for pg_conf to reinitialize"))); return; } // before registering work_shmem_exit, so that a slot that isn't ours never gets freed
    before_shmem_exit(work_shmem_exit, main_arg);
    if (init_work_gone(main_arg)) { ereport(LOG, (errmsg("entry no longer in pg_task.json, or taken over by another pg_work"))); return; } // restarted by the postmaster after its entry is gone, or another pg_work was started for it, which pg_conf, with no handle of it, marked in its slot: exit cleanly, for it not to be restarted again, freeing its slot, before connecting, which may well be why it was restarted
    work.shared->pid = MyProcPid; // alive, for pg_conf not to start another one for its entry, see init_work()
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, work_idle);
    pqsignal(SIGTERM, die); // terminate at the next CHECK_FOR_INTERRUPTS(), as a backend does, rather than in the default handler of background workers, whose FATAL right there can come in the middle of a commit
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM < 90600
    InitializeLatchSupportMy();
#endif
    BackgroundWorkerInitializeConnectionMy(work.shared->data, work.shared->user);
    before_shmem_exit(work_exit, (Datum)0); // registered after the one ending the session, so as to run before it
    initStringInfoMy(&application_name);
    appendStringInfo(&application_name, "pg_work %s %s %li", work.shared->schema, work.shared->table, work.shared->sleep);
    SetConfigOption("application_name", application_name.data, PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION); // pg_temp last, which an empty one would search first for tables and types
    // the transaction characteristics of the settings of the database or the role, for the transactions of tasks, not for its own, as for the bookkeeping in a task worker, see SPI_connect_my(): read only would fail every pass, repeatable read or serializable the taking of tasks or the bookkeeping of a remote one on a row changed meanwhile, a stop say, taking pg_work, with every remote task it runs, down, those done already to run again on reset
    SetConfigOption("default_transaction_isolation", "read committed", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("default_transaction_read_only", "off", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("default_transaction_deferrable", "off", PGC_USERSET, PGC_S_SESSION);
#if PG_VERSION_NUM >= 170000
    SetConfigOption("transaction_timeout", "0", PGC_USERSET, PGC_S_SESSION); // that of the settings of the database or the role, for the transactions of tasks, ends any longer one with FATAL, taking pg_work, with every remote task it runs, down: none for its own, which run no code of others, unlike a task's input
#endif
    // make_*() compare what pg_get_expr() deparses with the expressions they make, which these two change, from the server's configuration or the role's and database's settings, and so does quote_identifier() with the names below: once connected, as no GUC can be set before
    SetConfigOption("IntervalStyle", "postgres", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("quote_all_identifiers", "off", PGC_USERSET, PGC_S_SESSION);
    work.data = quote_identifier(work.shared->data);
    work.schema = quote_identifier(work.shared->schema);
    work.table = quote_identifier(work.shared->table);
    work.user = quote_identifier(work.shared->user);
    pgstat_report_appname(application_name.data);
    pfree(application_name.data);
    set_ps_display_my("main");
    process_session_preload_libraries();
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work.schema, work.table);
    work.schema_table = schema_table.data;
    if (!lock_data_user_hash(MyDatabaseId, GetUserId(), work.shared->hash)) { ereport(WARNING, (errmsg("!lock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.shared->hash))); ShutdownRequestPending = true; return; } // exit without error to disable restart, then not start conf
    // restarted after a crash, it may serve what pg_task.json no longer has, and no pg_conf that has been restarted meanwhile knows to cancel that restart: check before taking any task, and exit without error, so as not to be restarted again
    work_check(&work);
    if (ShutdownRequestPending) return;
    dlist_init(&local);
    dlist_init(&pending);
    dlist_init(&remote);
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    dlist_init(&cancels);
#endif
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work.schema);
    work.schema_type = schema_type.data;
    elog(DEBUG1, "sleep = %li, reset = %li, schema_table = %s, schema_type = %s, hash = %i", work.shared->sleep, work.shared->reset, work.schema_table, work.schema_type, work.shared->hash);
#ifdef GP_VERSION_NUM
#ifdef HAVE_CREATING_EXTENSION_LOCAL
    creating_extension_local = true; // force an ENTRY distribution policy without leaving GP_ROLE_DISPATCH, so segments still learn about the relation (see master_only_dispatch_bug)
#else
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
#endif
    if (!lock_data_make(MyDatabaseId)) ereport(WARNING, (errmsg("!lock_data_make(%i)", MyDatabaseId)));
    make_schema(&work);
    make_type(&work);
    make_table(&work);
    if (!unlock_data_make(MyDatabaseId)) ereport(WARNING, (errmsg("!unlock_data_make(%i)", MyDatabaseId)));
#ifdef GP_VERSION_NUM
#ifdef HAVE_CREATING_EXTENSION_LOCAL
    creating_extension_local = false;
#else
    Gp_role = GP_ROLE_DISPATCH;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
#endif
    set_ps_display_my("idle");
    work_reset(&work);
    while (!ShutdownRequestPending) {
        int nevents = work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(WaitEvent));
        WaitEventSet *set = CreateWaitEventSetMy(nevents); // from 13 on it takes a file descriptor of those for others than files, as the connections of remote tasks do, and errors with none left: they are made, by work_sleep(), only while the set before it holds one, which it gives back for this one to take
        long deadline;
        long timeout;
#ifdef LIBPQ_HAS_ASYNC_CANCEL
        int cancel_pos = work_events(set);
#else
        work_events(set);
#endif
        if (current_reset <= 0) {
            INSTR_TIME_SET_CURRENT(start_time_reset);
            current_reset = work.shared->reset;
        }
        if (current_sleep <= 0) {
            INSTR_TIME_SET_CURRENT(start_time_sleep);
            current_sleep = work.shared->sleep;
        }
        if (idle_count < (uint64)init_work_idle()) timeout = Min(current_reset, current_sleep);
        // idle: till the next task is planned, in milliseconds rounded up, not to wake before it, and not before the pass it takes is due either, or once its plan is past, which work_timeout() leaves out, as it does the tasks that wait for a slot of their group, there'd be no pass and no end to the wait
        else {
            // but not for longer than idle passes would take: work_timeout() leaves out tasks due already, some of which an idle pg_work may not have seen, as one committed long after its wake-up, held by someone else on the pass, or waiting for a slot that a task worker of an earlier pg_work frees, whose exit wakes no one
            long most = (long)init_work_idle() * work.shared->sleep;
            TimestampTz until = init_pauses(work.shared->oid, NULL); // the soonest end of a pause of a group, its tasks due already left out by work_timeout() too, see init_pause()
            if ((timeout = work_timeout(&work, current_reset)) < 0 || timeout > most) timeout = most;
            if (until) {
                long secs;
                int usecs;
                TimestampDifference(GetCurrentTimestamp(), until, &secs, &usecs);
                if (secs * 1000 + (usecs + 999) / 1000 < timeout) timeout = secs * 1000 + (usecs + 999) / 1000;
            }
            if (timeout < current_sleep) timeout = current_sleep;
        }
        if ((deadline = work_deadline()) >= 0 && (timeout < 0 || deadline < timeout)) timeout = deadline;
        if (work_waiting() && (timeout < 0 || timeout > work.shared->sleep)) timeout = work.shared->sleep; // to try the bookkeeping put off, or the start of a task whose row was held, again
#if PG_VERSION_NUM < 90600
        // the copy of 9.6's latch.c waits on a self-pipe of its own, which the server's SetLatch(), the one called, from the signal handlers too, never writes to: a signal coming between its check of the latch and its wait wakes it no sooner than its timeout, which, then, is a sleep at most, idle or not
        if (timeout < 0 || timeout > work.shared->sleep) timeout = work.shared->sleep;
#endif
        // the next task planned in more than about 24.8 days (repeat = '1 month', say), or as long a reset, is more than the wait takes: it asserts and passes the int it gets to epoll, which would make it wait forever instead, so wake up in time to compute the timeout again
        if (timeout > INT_MAX) timeout = INT_MAX;
        nevents = WaitEventSetWaitMy(set, timeout, events, nevents);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
#ifdef LIBPQ_HAS_ASYNC_CANCEL
            if (event->pos >= cancel_pos) { if (event->events & WL_SOCKET_MASK) work_cancel_poll(event->user_data); continue; }
#endif
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            else if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        work_expire();
        // an idle pg_work waits only for tasks planned ahead, not for those due already that wait for a slot of their group, which a task done frees: back to passes every sleep, for them to be taken
        if (work_reap(&work)) idle_count = 0;
        work_latch(&work);
        INSTR_TIME_SET_CURRENT(current_time_reset);
        INSTR_TIME_SUBTRACT(current_time_reset, start_time_reset);
        current_reset = work.shared->reset - (long)INSTR_TIME_GET_MILLISEC(current_time_reset);
        if (current_reset <= 0) work_reset(&work);
        INSTR_TIME_SET_CURRENT(current_time_sleep);
        INSTR_TIME_SUBTRACT(current_time_sleep, start_time_sleep);
        current_sleep = work.shared->sleep - (long)INSTR_TIME_GET_MILLISEC(current_time_sleep);
        if (current_sleep <= 0) {
            work_pending(); // once a sleep, as a pass, rather than on every wake-up, of a row of another remote task readable say, each a query
            work_held();
            work_sleep(&work);
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    if (!unlock_data_user_hash(MyDatabaseId, GetUserId(), work.shared->hash)) ereport(WARNING, (errmsg("!unlock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.shared->hash)));
}
