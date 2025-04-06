#include "include.h"

#include <libpq/libpq-be.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/ps_status.h>

#if PG_VERSION_NUM < 90600
#include "latch_my.h"
#endif

#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#else
#include <access/hash.h>
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

static dlist_head remote;
static volatile uint64 idle_count = 0;
static Work work = {0};

Work *get_work(void) {
    return &work;
}

static void work_query(Task *t);

#define work_error(...) do { \
    PG_TRY(); \
        ereport(ERROR, __VA_ARGS__); \
    PG_CATCH(); \
        task_error(t); \
        EmitErrorReport(); \
        FlushErrorState(); \
    PG_END_TRY(); \
    if (task_done(t)) t->remote != NULL ? work_finish(t) : work_free(t); \
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
    len--;
    return errdetail("%.*s", len, err);
}

static void work_check(const Work *w) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return;
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH j AS (
                SELECT  COALESCE(COALESCE("data", "user"), pg_catalog.current_setting('pg_task.data')) AS "data",
                        EXTRACT(epoch FROM COALESCE("reset", pg_catalog.current_setting('pg_task.reset')::pg_catalog.interval))::pg_catalog.int8 OPERATOR(pg_catalog.*) 1000 AS "reset",
                        COALESCE("run", pg_catalog.current_setting('pg_task.run')::pg_catalog.int4) AS "run",
                        COALESCE("schema", pg_catalog.current_setting('pg_task.schema')) AS "schema",
                        COALESCE("table", pg_catalog.current_setting('pg_task.table')) AS "table",
                        COALESCE("sleep", pg_catalog.current_setting('pg_task.sleep')::pg_catalog.int8) AS "sleep",
                        COALESCE("spi", pg_catalog.current_setting('pg_task.spi')::pg_catalog.bool) AS "spi",
                        COALESCE(COALESCE("user", "data"), pg_catalog.current_setting('pg_task.user')) AS "user"
                FROM    pg_catalog.jsonb_to_recordset(pg_catalog.current_setting('pg_task.json')::pg_catalog.jsonb) AS j ("data" text, "reset" interval, "run" int4, "schema" text, "table" text, "sleep" int8, "spi" bool, "user" text)
            ) SELECT    DISTINCT j.* FROM j WHERE "user" OPERATOR(pg_catalog.=) current_user AND "data" OPERATOR(pg_catalog.=) current_catalog AND pg_catalog.hashtext(pg_catalog.concat_ws('.', "schema", "table"))::pg_catalog.int4 OPERATOR(pg_catalog.=) %1$i
        ), w->shared->hash);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
    if (!SPI_processed) ShutdownRequestPending = true; else {
        HeapTuple val = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false, INT8OID));
        w->shared->run = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "run", false, INT4OID));
        w->shared->sleep = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "sleep", false, INT8OID));
        w->shared->spi = DatumGetBool(SPI_getbinval_my(val, tupdesc, "spi", false, BOOLOID));
        elog(DEBUG1, "sleep = %li, reset = %li, schema = %s, table = %s, run = %i, spi = %s, SPI_processed = %lu", w->shared->sleep, w->shared->reset, w->shared->schema, w->shared->table, w->shared->run, w->shared->spi ? "true" : "false", (long)SPI_processed);
        SPI_freetuple(val);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_command(Task *t, PGresult *result) {
    if (t->skip) { t->skip--; return; }
    if (!t->output.data) initStringInfoMy(&t->output);
    appendStringInfo(&t->output, "%s%s", t->output.len ? "\n" : "", PQcmdStatus(result));
}

static void work_events(WaitEventSet *set) {
    dlist_mutable_iter iter;
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        AddWaitEventToSet(set, t->event, PQsocket(t->conn), NULL, t);
    }
}

static void work_fatal(Task *t, const PGresult *result) {
    char *value = NULL;
    char *value2 = NULL;
    char *value3 = NULL;
    if (!t->output.data) initStringInfoMy(&t->output);
    if (!t->error.data) initStringInfoMy(&t->error);
    t->skip++;
    if (t->error.len) appendStringInfoChar(&t->error, '\n');
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY))) appendStringInfo(&t->error, "%s:  ", _(value));
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
    if (is_log_level_output(severity_error(PQresultErrorField(result, PG_DIAG_SEVERITY)), log_min_error_statement)) { // If the user wants the query that generated this error logged, do it.
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

static void work_finish(Task *t) {
    if (t->conn) {
        PQfinish(t->conn);
#if PG_VERSION_NUM >= 130000
        ReleaseExternalFD();
#endif
    }
    if (!proc_exit_inprogress && t->pid && !unlock_table_pid_hash(t->shared->oid, t->pid, t->shared->hash)) ereport(WARNING, (errmsg("!unlock_table_pid_hash(%i, %i, %i)", t->shared->oid, t->pid, t->shared->hash)));
    work_free(t);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 2;
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_BAD) { work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
        if (PQsocket(t->conn) == PGINVALID_SOCKET) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsocket == PGINVALID_SOCKET"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
        nevents++;
    }
    return nevents;
}

static void work_reset(const Work *w) {
    Portal portal;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("reset");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$i AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.<<) 32 OPERATOR(pg_catalog.>>) 32)
                WHERE "state" OPERATOR(pg_catalog.=) ANY(ARRAY['TAKE', 'WORK']::%3$s[]) AND l.pid IS NULL FOR UPDATE OF t %4$s
            ) UPDATE %1$s AS t SET "state" = 'PLAN', "start" = NULL, "stop" = NULL, "pid" = NULL FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id RETURNING t.id::pg_catalog.int8
        ), w->schema_table, w->shared->oid, w->schema_type,
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
            "SKIP LOCKED"
#else
            ""
#endif
        );
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    portal = SPI_cursor_open_my(src.data, plan, NULL, NULL, false);
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
    set_ps_display_my("idle");
}

static long work_timeout(const Work *w) {
    long timeout;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("timeout");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
           SELECT COALESCE(LEAST(EXTRACT(epoch FROM ((
                SELECT GREATEST("plan" OPERATOR(pg_catalog.+) pg_catalog.current_setting('pg_task.reset')::pg_catalog.interval OPERATOR(pg_catalog.-) CURRENT_TIMESTAMP, '0 sec'::pg_catalog.interval) AS "plan" FROM %1$s AS t
                LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 4 AND "database" OPERATOR(pg_catalog.=) %2$i AND "classid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.>>) 32) AND "objid" OPERATOR(pg_catalog.=) ("id" OPERATOR(pg_catalog.<<) 32 OPERATOR(pg_catalog.>>) 32)
                WHERE "state" OPERATOR(pg_catalog.=) ANY(ARRAY['TAKE', 'WORK']::%3$s[]) AND l.pid IS NULL ORDER BY 1 LIMIT 1
           )))::pg_catalog.int8 OPERATOR(pg_catalog.*) 1000, EXTRACT(epoch FROM ((
                SELECT "plan" OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN 0 ELSE "max" END)::pg_catalog.text, 'msec')::pg_catalog.interval OPERATOR(pg_catalog.-) CURRENT_TIMESTAMP AS "plan" FROM %1$s WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN 0 ELSE "max" END)::pg_catalog.text, 'msec')::pg_catalog.interval OPERATOR(pg_catalog.>=) CURRENT_TIMESTAMP ORDER BY 1 LIMIT 1
           )))::pg_catalog.int8 OPERATOR(pg_catalog.*) 1000), -1)::pg_catalog.int8 as "min"
        ), w->schema_table, w->shared->oid, w->schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
    timeout = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "min", false, INT8OID)) : -1;
    elog(DEBUG1, "timeout = %li", timeout);
    SPI_finish_my();
    set_ps_display_my("idle");
    // idle_count = 0;
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
}

static void work_readable(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && !PQconsumeInput(t->conn)) { work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), work_errdetail(PQerrorMessage(t->conn)))); return; }
    t->socket(t);
}

static void work_done(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && PQtransactionStatus(t->conn) != PQTRANS_IDLE) {
        t->socket = work_done;
        if (!PQsendQuery(t->conn, SQL(COMMIT))) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), work_errdetail(PQerrorMessage(t->conn)))); return; }
        t->event = WL_SOCKET_READABLE;
        return;
    }
    task_done(t) || PQstatus(t->conn) != CONNECTION_OK ? work_finish(t) : work_query(t);
}

static void work_headers(Task *t, const PGresult *result) {
    if (t->output.len) appendStringInfoString(&t->output, "\n");
    for (int col = 0; col < PQnfields(result); col++) {
        if (col > 0) appendStringInfoChar(&t->output, t->delimiter);
        appendBinaryStringInfoEscapeQuote(&t->output, PQfname(result, col), strlen(PQfname(result, col)), false, t->escape, t->quote);
    }
}

static void work_success(Task *t, const PGresult *result, int row) {
    if (!t->output.data) initStringInfoMy(&t->output);
    if (t->header && !row && PQnfields(result) > 1) work_headers(t, result);
    if (t->output.len) appendStringInfoString(&t->output, "\n");
    for (int col = 0; col < PQnfields(result); col++) {
        if (col > 0) appendStringInfoChar(&t->output, t->delimiter);
        if (PQgetisnull(result, row, col)) appendStringInfoString(&t->output, t->null);
        else appendBinaryStringInfoEscapeQuote(&t->output, PQgetvalue(result, row, col), PQgetlength(result, row, col), !init_oid_is_string(PQftype(result, col)) && t->string, t->escape, t->quote);
    }
}

static void work_copy(Task *t) {
    char *buffer = NULL;
    int len;
    if (!t->output.data) initStringInfoMy(&t->output);
    switch ((len = PQgetCopyData(t->conn, &buffer, false))) {
        case 0: break;
        case -1: break;
        case -2: work_error((errmsg("id = %li, PQgetCopyData == -2", t->shared->id), work_errdetail(PQerrorMessage(t->conn)))); if (buffer) PQfreemem(buffer); return;
        default: appendBinaryStringInfo(&t->output, buffer, len); break;
    }
    if (buffer) PQfreemem(buffer);
    t->skip++;
}

static void work_result(Task *t) {
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK && (result = PQgetResult(t->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: work_command(t, result); break;
        case PGRES_COPY_OUT: work_copy(t); break;
        case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", t->shared->id), work_errdetail(PQresultErrorMessage(result)))); work_fatal(t, result); break;
        case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) work_success(t, result, row); break;
        default: elog(DEBUG1, "id = %li, %s", t->shared->id, PQresStatus(PQresultStatus(result))); break;
    }
    work_done(t);
}

static void work_query(Task *t) {
    StringInfoData input;
    if (ShutdownRequestPending) return;
    t->socket = work_query;
    if (task_work(t)) { work_finish(t); return; }
    initStringInfoMy(&input);
    t->skip = 0;
    appendStringInfoString(&input, SQL(BEGIN;));
    t->skip++;
    appendStringInfo(&input, SQL(SET SESSION "pg_task.id" = %li;), t->shared->id);
    t->skip++;
    if (t->timeout) {
        appendStringInfo(&input, SQL(SET SESSION "statement_timeout" = %i;), t->timeout);
        t->skip++;
    }
    appendStringInfoString(&input, SQL(COMMIT;));
    t->skip++;
    appendStringInfoString(&input, t->input);
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", t->shared->id, t->timeout, input.data, t->count);
    if (!PQsendQuery(t->conn, input.data)) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), work_errdetail(PQerrorMessage(t->conn)))); pfree(input.data); return; }
    pfree(input.data);
    t->socket = work_result;
    t->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *t) {
    bool connected = false;
    switch (PQstatus(t->conn)) {
        case CONNECTION_BAD: work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(PQerrorMessage(t->conn)))); return;
        case CONNECTION_OK: elog(DEBUG1, "id = %li, PQstatus == CONNECTION_OK", t->shared->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(t->conn)) {
        case PGRES_POLLING_ACTIVE: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", t->shared->id); break;
        case PGRES_POLLING_FAILED: work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectPoll failed"), work_errdetail(PQerrorMessage(t->conn)))); return;
        case PGRES_POLLING_OK: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_OK", t->shared->id); connected = true; break;
        case PGRES_POLLING_READING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_READING", t->shared->id); t->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_WRITING", t->shared->id); t->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if (!(t->pid = PQbackendPID(t->conn))) { work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQbackendPID failed"), work_errdetail(PQerrorMessage(t->conn)))); return; }
        if (!lock_table_pid_hash(t->shared->oid, t->pid, t->shared->hash)) { work_error((errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("!lock_table_pid_hash(%i, %i, %i)", t->shared->oid, t->pid, t->shared->hash))); return; }
        work_query(t);
    }
}

static void work_shmem_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    elog(DEBUG1, "code = %i", code);
    if (!code) init_free(DatumGetInt32(arg));
    dlist_foreach_modify(iter, &remote) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_OK) {
            char errbuf[256];
            PGcancel *cancel = PQgetCancel(t->conn);
            if (!cancel) { ereport(WARNING, (errmsg("PQgetCancel failed"), work_errdetail(PQerrorMessage(t->conn)))); continue; }
            if (!PQcancel(cancel, errbuf, sizeof(errbuf))) { ereport(WARNING, (errmsg("PQcancel failed"), errdetail("%s", errbuf))); PQfreeCancel(cancel); continue; }
            ereport(WARNING, (errmsg("cancel id = %li", t->shared->id)));
            PQfreeCancel(cancel);
        }
        work_finish(t);
    }
}

static void work_remote(Task *t) {
    bool password = false;
    char *err;
    char *options = NULL;
    const char **keywords;
    const char **values;
    int arg = 3;
    PQconninfoOption *opts = PQconninfoParse(t->remote, &err);
    StringInfoData name, value;
    elog(DEBUG1, "id = %li, group = %s, remote = %s, max = %i, oid = %i", t->shared->id, t->group, t->remote ? t->remote : init_null(), t->shared->max, t->shared->oid);
    dlist_delete(&t->node);
    dlist_push_tail(&remote, &t->node);
    if (!opts) { work_error((errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("PQconninfoParse failed"), work_errdetail(err))); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        elog(DEBUG1, "%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "password")) password = true;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "options")) { options = opt->val; continue; }
        arg++;
    }
    if (!superuser() && !password) { work_error((errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superusers must provide a password in the connection string."))); PQconninfoFree(opts); return; }
    keywords = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*keywords));
    values = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*values));
    initStringInfoMy(&name);
    appendStringInfo(&name, "pg_task %s %s %s", t->shared->schema, t->shared->table, t->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(&value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, " -c pg_task.schema=%s", t->shared->schema);
    appendStringInfo(&value, " -c pg_task.table=%s", t->shared->table);
    appendStringInfo(&value, " -c pg_task.oid=%i", t->shared->oid);
    appendStringInfo(&value, " -c pg_task.group=%s", t->group);
    arg++;
    keywords[arg] = "options";
    values[arg] = value.data;
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "options")) continue;
        arg++;
        keywords[arg] = opt->keyword;
        values[arg] = opt->val;
    }
    arg++;
    keywords[arg] = NULL;
    values[arg] = NULL;
    t->event = WL_SOCKET_MASK;
    t->socket = work_connect;
    t->start = GetCurrentTimestamp();
#if PG_VERSION_NUM >= 130000
    if (!AcquireExternalFD()) work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("could not establish connection"), errdetail("There are too many open files on the local server."), errhint("Raise the server's max_files_per_process and/or \"ulimit -n\" limits."))); else
#endif
    if (!(t->conn = PQconnectStartParams(keywords, values, false))) work_error((errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectStartParams failed"), work_errdetail(PQerrorMessage(t->conn))));
    else if (PQstatus(t->conn) == CONNECTION_BAD) work_error((errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), work_errdetail(PQerrorMessage(t->conn))));
    else if (!PQisnonblocking(t->conn) && PQsetnonblocking(t->conn, true) == -1) work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetnonblocking failed"), work_errdetail(PQerrorMessage(t->conn))));
    else if (!superuser() && !PQconnectionUsedPassword(t->conn)) work_error((errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superuser cannot connect if the server does not request a password."), errhint("Target server's authentication method must be changed.")));
    else if (PQclientEncoding(t->conn) != GetDatabaseEncoding() && !PQsetClientEncoding(t->conn, GetDatabaseEncodingName())) work_error((errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetClientEncoding failed"), work_errdetail(PQerrorMessage(t->conn))));
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
    if (t->group) pfree(t->group);
    t->group = NULL;
}

static void work_task(Task *t) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    size_t len;
    elog(DEBUG1, "id = %li, group = %s, max = %i, oid = %i", t->shared->id, t->group, t->shared->max, t->shared->oid);
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name)))); return; }
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name)))); return; }
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", t->shared->user, t->shared->data, t->shared->schema, t->shared->table, t->group)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1))); // do not error when group is to long
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) { work_error((errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type)))); return; }
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    if ((worker.bgw_main_arg = Int32GetDatum(init_arg(t->shared))) == Int32GetDatum(-1)) { work_error((errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not find empty slot"))); return; }
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    } else switch (WaitForBackgroundWorkerStartup(handle, &t->pid)) {
        case BGWH_NOT_YET_STARTED: init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: elog(DEBUG1, "started id = %li", t->shared->id); work_free(t); break;
        case BGWH_STOPPED: init_free(worker.bgw_main_arg); work_error((errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void work_sleep(Work *w) {
    Datum values[] = {Int32GetDatum(w->shared->run)};
    dlist_head head;
    dlist_mutable_iter iter;
    Portal portal;
    static Oid argtypes[] = {INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "idle_count = %lu", idle_count);
    set_ps_display_my("sleep");
    dlist_init(&head);
#ifdef GP_VERSION_NUM
    if (true) {
        static SPIPlanPtr gp_plan = NULL;
        static StringInfoData gp_src = {0};
        initStringInfoMy(&gp_src);
        appendStringInfo(&gp_src, SQL(
            UPDATE %1$s SET "state" = 'GONE', "start" = CURRENT_TIMESTAMP, "stop" = CURRENT_TIMESTAMP, "error" = 'ERROR:  task not active' WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "repeat" OPERATOR(pg_catalog.=) '0 sec' AND "max" OPERATOR(pg_catalog.>=) 0
        ), w->schema_table);
        SPI_connect_my(gp_src.data);
        if (!gp_plan) gp_plan = SPI_prepare_my(gp_src.data, 0, NULL);
        SPI_execute_plan_my(gp_src.data, gp_plan, NULL, NULL, SPI_OK_UPDATE);
        SPI_finish_my();
    }
#endif
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfoString(&src, "WITH ");
#ifndef GP_VERSION_NUM
        appendStringInfo(&src, SQL(
            n AS (
                UPDATE %1$s SET "state" = 'GONE', "start" = CURRENT_TIMESTAMP, "stop" = CURRENT_TIMESTAMP, "error" = 'ERROR:  task not active' WHERE "state" OPERATOR(pg_catalog.=) 'PLAN' AND "plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "repeat" OPERATOR(pg_catalog.=) '0 sec' AND "max" OPERATOR(pg_catalog.>=) 0 RETURNING "id"
            ),
        ), w->schema_table);
#endif
        appendStringInfo(&src, SQL(
            l AS (
                SELECT pg_catalog.count("classid") AS "classid", "objid" FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessShareLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 5 AND "database" OPERATOR(pg_catalog.=) %2$i GROUP BY "objid"
            ), s AS (
                SELECT "id", "hash", CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) AS "count" FROM %1$s AS t LEFT JOIN l ON "objid" OPERATOR(pg_catalog.=) "hash"
                WHERE "plan" OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN 0 ELSE "max" END)::pg_catalog.text, 'msec')::pg_catalog.interval OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "state" OPERATOR(pg_catalog.=) 'PLAN' AND CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) OPERATOR(pg_catalog.>=) 0
                %4$s
                ORDER BY 3 DESC, 1 LIMIT LEAST($1 OPERATOR(pg_catalog.-) (SELECT COALESCE(pg_catalog.sum("classid"), 0) FROM l), pg_catalog.current_setting('pg_task.limit')::pg_catalog.int4) FOR UPDATE OF t %3$s
            ), u AS (
                SELECT "id", "count" OPERATOR(pg_catalog.-) pg_catalog.row_number() OVER (PARTITION BY "hash" ORDER BY "count" DESC, "id") OPERATOR(pg_catalog.+) 1 AS "count" FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS t SET "state" = 'TAKE' FROM u WHERE t.id OPERATOR(pg_catalog.=) u.id AND u.count OPERATOR(pg_catalog.>=) 0 RETURNING t.id::pg_catalog.int8, "hash"::pg_catalog.int4, "group"::pg_catalog.text, "remote"::pg_catalog.text, "max"::pg_catalog.int4
        ), w->schema_table, w->shared->oid,
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
        "SKIP LOCKED"
#else
        ""
#endif
        ,
#ifdef GP_VERSION_NUM
        ""
#else
        SQL(AND "id" NOT IN (SELECT "id" FROM n))
#endif
        );
    }
    SPI_connect_my(src.data);
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
            t->shared = MemoryContextAllocZero(TopMemoryContext, sizeof(Shared));
            *t->shared = *w->shared;
            t->work = w;
            t->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false, INT4OID));
            t->shared->id = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "id", false, INT8OID));
            t->shared->max = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "max", false, INT4OID));
            elog(DEBUG1, "row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, t->shared->id, t->shared->hash, t->group, t->remote ? t->remote : init_null(), t->shared->max);
            dlist_push_tail(&head, &t->node);
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    if (dlist_is_empty(&head)) idle_count++; else {
        idle_count = 0;
        dlist_foreach_modify(iter, &head) {
            Task *t = dlist_container(Task, node, iter.cur);
            t->remote ? work_remote(t) : work_task(t);
        }
    }
    set_ps_display_my("idle");
}

static void work_writeable(Task *t) {
    t->socket(t);
}

static void work_idle(SIGNAL_ARGS) {
    int save_errno = errno;
    idle_count = 0;
    SetLatch(MyLatch);
    errno = save_errno;
}

void work_main(Datum main_arg) {
    const char *application_name;
    instr_time current_time_reset;
    instr_time current_time_sleep;
    instr_time start_time_reset;
    instr_time start_time_sleep;
    long current_reset = -1;
    long current_sleep = -1;
    StringInfoData schema_table, schema_type;
    elog(DEBUG1, "main_arg = %i", DatumGetInt32(main_arg));
    work.shared = init_shared(main_arg);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_DISPATCH;
    optimizer = false;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
    before_shmem_exit(work_shmem_exit, main_arg);
    if (!work.shared->in_use) return;
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, work_idle);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM < 90600
    InitializeLatchSupportMy();
#endif
    work.data = quote_identifier(work.shared->data);
    work.schema = quote_identifier(work.shared->schema);
    work.table = quote_identifier(work.shared->table);
    work.user = quote_identifier(work.shared->user);
    BackgroundWorkerInitializeConnectionMy(work.shared->data, work.shared->user);
    application_name = MyBgworkerEntry->bgw_name + strlen(work.shared->user) + 1 + strlen(work.shared->data) + 1;
    set_config_option_my("application_name", application_name, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname(application_name);
    set_ps_display_my("main");
    process_session_preload_libraries();
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work.schema, work.table);
    work.schema_table = schema_table.data;
    if (!lock_data_user_hash(MyDatabaseId, GetUserId(), work.shared->hash)) { ereport(WARNING, (errmsg("!lock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.shared->hash))); ShutdownRequestPending = true; return; } // exit without error to disable restart, then not start conf
    dlist_init(&remote);
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work.schema);
    work.schema_type = schema_type.data;
    elog(DEBUG1, "sleep = %li, reset = %li, schema_table = %s, schema_type = %s, hash = %i", work.shared->sleep, work.shared->reset, work.schema_table, work.schema_type, work.shared->hash);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
    make_schema(&work);
    make_type(&work);
    make_table(&work);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_DISPATCH;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
    set_ps_display_my("idle");
    work_reset(&work);
    while (!ShutdownRequestPending) {
        int nevents = work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(WaitEvent));
        WaitEventSet *set = CreateWaitEventSetMy(nevents);
        work_events(set);
        if (current_reset <= 0) {
            INSTR_TIME_SET_CURRENT(start_time_reset);
            current_reset = work.shared->reset;
        }
        if (current_sleep <= 0) {
            INSTR_TIME_SET_CURRENT(start_time_sleep);
            current_sleep = work.shared->sleep;
        }
        nevents = WaitEventSetWaitMy(set, idle_count >= (uint64)init_work_idle() ? work_timeout(&work) : Min(current_reset, current_sleep), events, nevents);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_LATCH_SET) work_latch(&work);
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        if (idle_count < (uint64)init_work_idle()) {
            INSTR_TIME_SET_CURRENT(current_time_reset);
            INSTR_TIME_SUBTRACT(current_time_reset, start_time_reset);
            current_reset = work.shared->reset - (long)INSTR_TIME_GET_MILLISEC(current_time_reset);
            if (current_reset <= 0) work_reset(&work);
            INSTR_TIME_SET_CURRENT(current_time_sleep);
            INSTR_TIME_SUBTRACT(current_time_sleep, start_time_sleep);
            current_sleep = work.shared->sleep - (long)INSTR_TIME_GET_MILLISEC(current_time_sleep);
            if (current_sleep <= 0) work_sleep(&work);
        }
        FreeWaitEventSet(set);
        pfree(events);
    }
    if (!unlock_data_user_hash(MyDatabaseId, GetUserId(), work.shared->hash)) ereport(WARNING, (errmsg("!unlock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.shared->hash)));
}
