#include "include.h"

extern char *task_null;
extern int task_idle;
extern int work_close;
extern int work_fetch;
extern TaskShared *taskshared;
extern Task task;
extern WorkShared *workshared;
long current_timeout;
static dlist_head head;
static emit_log_hook_type emit_log_hook_prev = NULL;
static volatile uint64 idle_count = 0;
Work work = {0};

static void work_query(Task *t);

#define work_ereport(finish_or_free, ...) do { \
    Task s = task; \
    bool remote = t->remote != NULL; \
    emit_log_hook_prev = emit_log_hook; \
    emit_log_hook = task_error; \
    task = *t; \
    PG_TRY(); \
        ereport(__VA_ARGS__); \
    PG_CATCH(); \
        EmitErrorReport(); \
        FlushErrorState(); \
    PG_END_TRY(); \
    *t = task; \
    task = s; \
    if (task_done(t) || finish_or_free) remote ? work_finish(t) : work_free(t); \
} while(0)

static char *work_errstr(char *err) {
    int len;
    if (!err) return "";
    len = strlen(err);
    if (!len) return "";
    if (err[len - 1] == '\n') err[len - 1] = '\0';
    return err;
}

static char *PQerrorMessageMy(const PGconn *conn) {
    return work_errstr(PQerrorMessage(conn));
}

static char *PQresultErrorMessageMy(const PGresult *res) {
    return work_errstr(PQresultErrorMessage(res));
}

static void work_check(void) {
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
                        COALESCE("schema", pg_catalog.current_setting('pg_task.schema')) AS "schema",
                        COALESCE("table", pg_catalog.current_setting('pg_task.table')) AS "table",
                        COALESCE("sleep", pg_catalog.current_setting('pg_task.sleep')::pg_catalog.int8) AS "sleep",
                        COALESCE(COALESCE("user", "data"), pg_catalog.current_setting('pg_task.user')) AS "user"
                FROM    pg_catalog.jsonb_to_recordset(pg_catalog.current_setting('pg_task.json')::pg_catalog.jsonb) AS j ("data" text, "reset" interval, "schema" text, "table" text, "sleep" int8, "user" text)
            ) SELECT    DISTINCT j.* FROM j WHERE "user" OPERATOR(pg_catalog.=) current_user AND "data" OPERATOR(pg_catalog.=) current_catalog AND pg_catalog.hashtext(pg_catalog.concat_ws(' ', 'pg_work', "schema", "table", "sleep")) OPERATOR(pg_catalog.=) %1$i
        ), work.hash);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
    if (!SPI_processed) ShutdownRequestPending = true;
    elog(DEBUG1, "sleep = %li, reset = %li, schema = %s, table = %s, SPI_processed = %li", work.shared->sleep, work.shared->reset, work.shared->schema, work.shared->table, (long)SPI_processed);
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
    dlist_foreach_modify(iter, &head) {
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
    appendStringInfo(&t->output, SQL(%sROLLBACK), t->output.len ? "\n" : "");
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
    PQfinish(t->conn);
    if (!proc_exit_inprogress && t->pid && !unlock_table_pid_hash(work.shared->oid, t->pid, t->shared->hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work.shared->oid, t->pid, t->shared->hash);
    work_free(t);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 2;
    dlist_foreach_modify(iter, &head) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_BAD) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn)))); continue; }
        if (PQsocket(t->conn) == PGINVALID_SOCKET) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsocket == PGINVALID_SOCKET"), errdetail("%s", PQerrorMessageMy(t->conn)))); continue; }
        nevents++;
    }
    return nevents;
}

static void work_index(int count, const char *const *indexes) {
    const char *name_quote;
    const RangeVar *rangevar;
    List *names;
    RelationData *relation;
    StringInfoData src, name, idx;
    set_ps_display_my("index");
    initStringInfoMy(&name);
    appendStringInfoString(&name, work.shared->table);
    for (int i = 0; i < count; i++) {
        const char *index = indexes[i];
        appendStringInfoString(&name, "_");
        appendStringInfoString(&name, index);
    }
    appendStringInfoString(&name, "_idx");
    name_quote = quote_identifier(name.data);
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE INDEX %s ON %s USING btree), name_quote, work.schema_table);
    appendStringInfoString(&src, " (");
    for (int i = 0; i < count; i++) {
        const char *index = indexes[i];
        const char *index_quote = quote_identifier(index);
        if (i) appendStringInfoString(&src, ", ");
        appendStringInfoString(&src, index_quote);
        if (index_quote != index) pfree((void *)index_quote);
    }
    appendStringInfoString(&src, ")");
    initStringInfoMy(&idx);
    appendStringInfo(&idx, "%s.%s", work.schema, name_quote);
    names = stringToQualifiedNameListMy(idx.data);
    rangevar = makeRangeVarFromNameList(names);
    elog(DEBUG1, "index = %s, schema_table = %s", idx.data, work.schema_table);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    } else if ((relation = relation_openrv_extended_my(rangevar, AccessShareLock, true))) {
        if (relation->rd_index && relation->rd_index->indrelid != work.shared->oid) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
        relation_close(relation, AccessShareLock);
    }
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    if (name_quote != name.data) pfree((void *)name_quote);
    pfree(idx.data);
    pfree(name.data);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_reset(void) {
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
        ), work.schema_table, work.shared->oid, work.schema_type,
#if PG_VERSION_NUM >= 90500
            "SKIP LOCKED"
#else
            ""
#endif
        );
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    portal = SPI_cursor_open_my(src.data, plan, NULL, NULL);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, work_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) elog(WARNING, "row = %lu, reset id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false, INT8OID)));
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_timeout(void) {
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
        ), work.schema_table, work.shared->oid, work.schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(src.data, plan, NULL, NULL, SPI_OK_SELECT);
    current_timeout = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "min", false, INT8OID)) : -1;
    elog(DEBUG1, "current_timeout = %li", current_timeout);
    SPI_finish_my();
    set_ps_display_my("idle");
    idle_count = 0;
}

static void work_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    work_check();
}

static void work_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) work_reload();
}

static void work_readable(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && !PQconsumeInput(t->conn)) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
    t->socket(t);
}

static void work_done(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && PQtransactionStatus(t->conn) != PQTRANS_IDLE) {
        t->socket = work_done;
        if (!PQsendQuery(t->conn, SQL(COMMIT))) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
        t->event = WL_SOCKET_READABLE;
        return;
    }
    task_done(t) || PQstatus(t->conn) != CONNECTION_OK ? work_finish(t) : work_query(t);
}

static void work_schema(const char *schema_quote) {
    List *names = stringToQualifiedNameListMy(schema_quote);
    StringInfoData src;
    elog(DEBUG1, "schema = %s", schema_quote);
    set_ps_display_my("schema");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE SCHEMA %s), schema_quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_finish_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
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
        case -2: work_ereport(true, ERROR, (errmsg("id = %li, PQgetCopyData == -2", t->shared->id), errdetail("%s", PQerrorMessageMy(t->conn)))); if (buffer) PQfreemem(buffer); return;
        default: appendBinaryStringInfo(&t->output, buffer, len); break;
    }
    if (buffer) PQfreemem(buffer);
    t->skip++;
}

static void work_result(Task *t) {
    for (PGresult *result; PQstatus(t->conn) == CONNECTION_OK && (result = PQgetResult(t->conn)); PQclear(result)) switch (PQresultStatus(result)) {
        case PGRES_COMMAND_OK: work_command(t, result); break;
        case PGRES_COPY_OUT: work_copy(t); break;
        case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", t->shared->id), errdetail("%s", PQresultErrorMessageMy(result)))); work_fatal(t, result); break;
        case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) work_success(t, result, row); break;
        default: elog(DEBUG1, "id = %li, %s", t->shared->id, PQresStatus(PQresultStatus(result))); break;
    }
    work_done(t);
}

static void work_query(Task *t) {
    StringInfoData input;
    for (;;) {
        if (ShutdownRequestPending) return;
        t->socket = work_query;
        if (task_work(t)) { work_finish(t); return; }
        if (t->active) break;
        work_ereport(false, ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task not active")));
        if (!t->shared->id) return;
    }
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
    if (!PQsendQuery(t->conn, input.data)) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); pfree(input.data); return; }
    pfree(input.data);
    t->socket = work_result;
    t->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *t) {
    bool connected = false;
    switch (PQstatus(t->conn)) {
        case CONNECTION_BAD: work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn)))); return;
        case CONNECTION_OK: elog(DEBUG1, "id = %li, PQstatus == CONNECTION_OK", t->shared->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(t->conn)) {
        case PGRES_POLLING_ACTIVE: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", t->shared->id); break;
        case PGRES_POLLING_FAILED: work_ereport(true, ERROR, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectPoll failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return;
        case PGRES_POLLING_OK: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_OK", t->shared->id); connected = true; break;
        case PGRES_POLLING_READING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_READING", t->shared->id); t->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_WRITING", t->shared->id); t->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if (!(t->pid = PQbackendPID(t->conn))) { work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQbackendPID failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
        if (!lock_table_pid_hash(work.shared->oid, t->pid, t->shared->hash)) { work_ereport(true, ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("!lock_table_pid_hash(%i, %i, %i)", work.shared->oid, t->pid, t->shared->hash))); return; }
        work_query(t);
    }
}

static void work_shmem_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    elog(DEBUG1, "code = %i", code);
    if (!code) {
        LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
        MemSet(&workshared[DatumGetInt32(arg)], 0, sizeof(*workshared));
        LWLockRelease(BackgroundWorkerLock);
    }
    dlist_foreach_modify(iter, &head) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_OK) {
            char errbuf[256];
            PGcancel *cancel = PQgetCancel(t->conn);
            if (!cancel) { ereport(WARNING, (errmsg("PQgetCancel failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); continue; }
            if (!PQcancel(cancel, errbuf, sizeof(errbuf))) { ereport(WARNING, (errmsg("PQcancel failed"), errdetail("%s", errbuf))); PQfreeCancel(cancel); continue; }
            elog(WARNING, "cancel id = %li", t->shared->id);
            PQfreeCancel(cancel);
        }
        work_finish(t);
    }
    if (!code) {
        if (!ShutdownRequestPending) init_conf(true);
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
    elog(DEBUG1, "id = %li, group = %s, remote = %s, max = %i, oid = %i", t->shared->id, t->group, t->remote ? t->remote : task_null, t->shared->max, work.shared->oid);
    dlist_delete(&t->node);
    dlist_push_tail(&head, &t->node);
    if (!opts) { work_ereport(true, ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("PQconninfoParse failed"), errdetail("%s", work_errstr(err)))); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        elog(DEBUG1, "%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "password")) password = true;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "options")) { options = opt->val; continue; }
        arg++;
    }
    if (!superuser() && !password) { work_ereport(true, ERROR, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superusers must provide a password in the connection string."))); PQconninfoFree(opts); return; }
    keywords = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*keywords));
    values = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*values));
    initStringInfoMy(&name);
    appendStringInfo(&name, "pg_task %s %s %s", work.shared->schema, work.shared->table, t->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(&value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, " -c pg_task.schema=%s", work.shared->schema);
    appendStringInfo(&value, " -c pg_task.table=%s", work.shared->table);
    appendStringInfo(&value, " -c pg_task.oid=%i", work.shared->oid);
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
    if (!(t->conn = PQconnectStartParams(keywords, values, false))) work_ereport(true, ERROR, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectStartParams failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (PQstatus(t->conn) == CONNECTION_BAD) work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (!PQisnonblocking(t->conn) && PQsetnonblocking(t->conn, true) == -1) work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetnonblocking failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (!superuser() && !PQconnectionUsedPassword(t->conn)) work_ereport(true, ERROR, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superuser cannot connect if the server does not request a password."), errhint("Target server's authentication method must be changed.")));
    else if (PQclientEncoding(t->conn) != GetDatabaseEncoding() && !PQsetClientEncoding(t->conn, GetDatabaseEncodingName())) work_ereport(true, ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetClientEncoding failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
    if (t->group) pfree(t->group);
    t->group = NULL;
}

static int work_bgw_main_arg(TaskShared *ts) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int slot = 0; slot < max_worker_processes; slot++) if (!taskshared[slot].in_use) {
        taskshared[slot] = *ts;
        taskshared[slot].in_use = true;
        LWLockRelease(BackgroundWorkerLock);
        elog(DEBUG1, "slot = %i", slot);
        return slot;
    }
    LWLockRelease(BackgroundWorkerLock);
    return -1;
}

static void work_taskshared_free(int slot) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    MemSet(&taskshared[slot], 0, sizeof(*taskshared));
    LWLockRelease(BackgroundWorkerLock);
}

static void work_task(Task *t) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    size_t len;
    elog(DEBUG1, "id = %li, group = %s, max = %i, oid = %i", t->shared->id, t->group, t->shared->max, work.shared->oid);
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) { work_ereport(true, ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name)))); return; }
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) { work_ereport(true, ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name)))); return; }
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", work.shared->user, work.shared->data, work.shared->schema, work.shared->table, t->group)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) { work_ereport(true, ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type)))); return; }
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    if ((worker.bgw_main_arg = Int32GetDatum(work_bgw_main_arg(t->shared))) == Int32GetDatum(-1)) { work_ereport(true, ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not find empty slot"))); return; }
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    t->shared->slot = DatumGetUInt32(MyBgworkerEntry->bgw_main_arg);
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        work_taskshared_free(worker.bgw_main_arg);
        work_ereport(true, ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    } else switch (WaitForBackgroundWorkerStartup(handle, &t->pid)) {
        case BGWH_NOT_YET_STARTED: work_taskshared_free(worker.bgw_main_arg); work_ereport(true, ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: work_taskshared_free(worker.bgw_main_arg); work_ereport(true, ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: elog(DEBUG1, "started id = %li", t->shared->id); work_free(t); break;
        case BGWH_STOPPED: work_taskshared_free(worker.bgw_main_arg); work_ereport(true, ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void work_sleep(void) {
    Datum values[] = {Int32GetDatum(work.shared->run)};
    dlist_head head;
    dlist_mutable_iter iter;
    Portal portal;
    static Oid argtypes[] = {INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "idle_count = %lu", idle_count);
    set_ps_display_my("sleep");
    dlist_init(&head);
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH l AS (
                SELECT pg_catalog.count("classid") AS "classid", "objid" FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessShareLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 5 AND "database" OPERATOR(pg_catalog.=) %2$i GROUP BY "objid"
            ), s AS (
                SELECT "id", t.hash, CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) AS "count" FROM %1$s AS t LEFT JOIN l ON "objid" OPERATOR(pg_catalog.=) "hash"
                WHERE "plan" OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN 0 ELSE "max" END)::pg_catalog.text, 'msec')::pg_catalog.interval OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "state" OPERATOR(pg_catalog.=) 'PLAN' AND CASE WHEN "max" OPERATOR(pg_catalog.>=) 0 THEN "max" ELSE 0 END OPERATOR(pg_catalog.-) COALESCE("classid", 0) OPERATOR(pg_catalog.>=) 0
                ORDER BY 3 DESC, 1 LIMIT LEAST($1 OPERATOR(pg_catalog.-) (SELECT COALESCE(pg_catalog.sum("classid"), 0) FROM l), pg_catalog.current_setting('pg_task.limit')::pg_catalog.int4) FOR UPDATE OF t %3$s
            ), u AS (
                SELECT "id", "count" OPERATOR(pg_catalog.-) pg_catalog.row_number() OVER (PARTITION BY "hash" ORDER BY "count" DESC, "id") OPERATOR(pg_catalog.+) 1 AS "count" FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS t SET "state" = 'TAKE' FROM u WHERE t.id OPERATOR(pg_catalog.=) u.id AND u.count OPERATOR(pg_catalog.>=) 0 RETURNING t.id::pg_catalog.int8, "hash"::pg_catalog.int4, "group"::pg_catalog.text, "remote"::pg_catalog.text, "max"::pg_catalog.int4
        ), work.schema_table, work.shared->oid,
#if PG_VERSION_NUM >= 90500
        "SKIP LOCKED"
#else
        ""
#endif
        );
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    portal = SPI_cursor_open_my(src.data, plan, values, NULL);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, work_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            Task *t = MemoryContextAllocZero(TopMemoryContext, sizeof(*t));
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            t->group = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "group", false, TEXTOID));
            t->remote = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "remote", true, TEXTOID));
            t->shared = MemoryContextAllocZero(TopMemoryContext, sizeof(*t->shared));
            t->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false, INT4OID));
            t->shared->id = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "id", false, INT8OID));
            t->shared->max = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "max", false, INT4OID));
            elog(DEBUG1, "row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, t->shared->id, t->shared->hash, t->group, t->remote ? t->remote : task_null, t->shared->max);
            dlist_push_tail(&head, &t->node);
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

static void work_table(void) {
    List *names = stringToQualifiedNameListMy(work.schema_table);
    const RangeVar *rangevar = makeRangeVarFromNameList(names);
    StringInfoData src, hash;
    elog(DEBUG1, "schema_table = %s, schema_type = %s", work.schema_table, work.schema_type);
    set_ps_display_my("table");
    initStringInfoMy(&hash);
#if PG_VERSION_NUM >= 120000
    appendStringInfo(&hash, SQL(GENERATED ALWAYS AS (pg_catalog.hashtext("group" OPERATOR(pg_catalog.||) COALESCE("remote", '%1$s'))) STORED), "");
#else
    if (true) {
        const char *function_quote;
        StringInfoData function;
        initStringInfoMy(&function);
        appendStringInfo(&function, "%1$s_hash_generate", work.shared->table);
        function_quote = quote_identifier(function.data);
        appendStringInfo(&hash, SQL(CREATE OR REPLACE FUNCTION %1$s.%2$s() RETURNS TRIGGER SET search_path = pg_catalog, pg_temp AS $function$BEGIN
            IF tg_op OPERATOR(pg_catalog.=) 'INSERT' OR (NEW.group, NEW.remote) IS DISTINCT FROM (OLD.group, OLD.remote) THEN
                NEW.hash = pg_catalog.hashtext(NEW.group OPERATOR(pg_catalog.||) COALESCE(NEW.remote, '%3$s'));
            END IF;
            RETURN NEW;
        END;$function$ LANGUAGE plpgsql;
        CREATE TRIGGER hash_generate BEFORE INSERT OR UPDATE ON %4$s FOR EACH ROW EXECUTE PROCEDURE %1$s.%2$s();), work.schema, function_quote, "", work.schema_table);
        if (function_quote != function.data) pfree((void *)function_quote);
        pfree(function.data);
    }
#endif
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        CREATE TABLE %1$s (
            "id" serial8 NOT NULL PRIMARY KEY,
            "parent" pg_catalog.int8 DEFAULT NULLIF(pg_catalog.current_setting('pg_task.id')::pg_catalog.int8, 0),
            "plan" pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "start" pg_catalog.timestamptz,
            "stop" pg_catalog.timestamptz,
            "active" pg_catalog.interval NOT NULL DEFAULT pg_catalog.current_setting('pg_task.active')::pg_catalog.interval CHECK ("active" > '0 sec'::pg_catalog.interval),
            "live" pg_catalog.interval NOT NULL DEFAULT pg_catalog.current_setting('pg_task.live')::pg_catalog.interval CHECK ("live" >= '0 sec'::pg_catalog.interval),
            "repeat" pg_catalog.interval NOT NULL DEFAULT pg_catalog.current_setting('pg_task.repeat')::pg_catalog.interval CHECK ("repeat" >= '0 sec'::pg_catalog.interval),
            "timeout" pg_catalog.interval NOT NULL DEFAULT pg_catalog.current_setting('pg_task.timeout')::pg_catalog.interval CHECK ("timeout" >= '0 sec'::pg_catalog.interval),
            "count" pg_catalog.int4 NOT NULL DEFAULT pg_catalog.current_setting('pg_task.count')::pg_catalog.int4 CHECK ("count" >= 0),
            "hash" pg_catalog.int4 NOT NULL %3$s,
            "max" pg_catalog.int4 NOT NULL DEFAULT pg_catalog.current_setting('pg_task.max')::pg_catalog.int4,
            "pid" pg_catalog.int4,
            "state" %2$s NOT NULL DEFAULT 'PLAN',
            "delete" pg_catalog.bool NOT NULL DEFAULT pg_catalog.current_setting('pg_task.delete')::pg_catalog.bool,
            "drift" pg_catalog.bool NOT NULL DEFAULT pg_catalog.current_setting('pg_task.drift')::pg_catalog.bool,
            "header" pg_catalog.bool NOT NULL DEFAULT pg_catalog.current_setting('pg_task.header')::pg_catalog.bool,
            "string" pg_catalog.bool NOT NULL DEFAULT pg_catalog.current_setting('pg_task.string')::pg_catalog.bool,
            "delimiter" pg_catalog.char NOT NULL DEFAULT pg_catalog.current_setting('pg_task.delimiter')::pg_catalog.char,
            "escape" pg_catalog.char NOT NULL DEFAULT pg_catalog.current_setting('pg_task.escape')::pg_catalog.char,
            "quote" pg_catalog.char NOT NULL DEFAULT pg_catalog.current_setting('pg_task.quote')::pg_catalog.char,
            "data" pg_catalog.text,
            "error" pg_catalog.text,
            "group" pg_catalog.text NOT NULL DEFAULT pg_catalog.current_setting('pg_task.group'),
            "input" pg_catalog.text NOT NULL,
            "null" pg_catalog.text NOT NULL DEFAULT pg_catalog.current_setting('pg_task.null'),
            "output" pg_catalog.text,
            "remote" pg_catalog.text
        );
        COMMENT ON TABLE %1$s IS 'Tasks';
        COMMENT ON COLUMN %1$s."id" IS 'Primary key';
        COMMENT ON COLUMN %1$s."parent" IS 'Parent task id (if exists, like foreign key to id, but without constraint, for performance)';
        COMMENT ON COLUMN %1$s."plan" IS 'Planned date and time of start';
        COMMENT ON COLUMN %1$s."start" IS 'Actual date and time of start';
        COMMENT ON COLUMN %1$s."stop" IS 'Actual date and time of stop';
        COMMENT ON COLUMN %1$s."active" IS 'Positive period after plan time, when task is active for executing';
        COMMENT ON COLUMN %1$s."live" IS 'Non-negative maximum time of live of current background worker process before exit';
        COMMENT ON COLUMN %1$s."repeat" IS 'Non-negative auto repeat tasks interval';
        COMMENT ON COLUMN %1$s."timeout" IS 'Non-negative allowed time for task run';
        COMMENT ON COLUMN %1$s."count" IS 'Non-negative maximum count of tasks, are executed by current background worker process before exit';
        COMMENT ON COLUMN %1$s."hash" IS 'Hash for identifying tasks group';
        COMMENT ON COLUMN %1$s."max" IS 'Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds';
        COMMENT ON COLUMN %1$s."pid" IS 'Id of process executing task';
        COMMENT ON COLUMN %1$s."state" IS 'Task state';
        COMMENT ON COLUMN %1$s."delete" IS 'Auto delete task when both output and error are nulls';
        COMMENT ON COLUMN %1$s."drift" IS 'Compute next repeat time by stop time instead by plan time';
        COMMENT ON COLUMN %1$s."header" IS 'Show columns headers in output';
        COMMENT ON COLUMN %1$s."string" IS 'Quote only strings';
        COMMENT ON COLUMN %1$s."delimiter" IS 'Results columns delimiter';
        COMMENT ON COLUMN %1$s."escape" IS 'Results columns escape';
        COMMENT ON COLUMN %1$s."quote" IS 'Results columns quote';
        COMMENT ON COLUMN %1$s."data" IS 'Some user data';
        COMMENT ON COLUMN %1$s."error" IS 'Catched error';
        COMMENT ON COLUMN %1$s."group" IS 'Task grouping by name';
        COMMENT ON COLUMN %1$s."input" IS 'Sql command(s) to execute';
        COMMENT ON COLUMN %1$s."null" IS 'Null text value representation';
        COMMENT ON COLUMN %1$s."output" IS 'Received result(s)';
        COMMENT ON COLUMN %1$s."remote" IS 'Connect to remote database (if need)';
    ), work.schema_table, work.schema_type,
#if PG_VERSION_NUM >= 120000
        hash.data
#else
        ""
#endif
    );
#if PG_VERSION_NUM < 120000
    appendStringInfoString(&src, hash.data);
#endif
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    work.shared->oid = RangeVarGetRelid(rangevar, NoLock, false);
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    resetStringInfo(&src);
    pfree(hash.data);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_type(void) {
    int32 typmod;
    Oid type = InvalidOid;
    StringInfoData src;
    set_ps_display_my("type");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'STOP')), work.schema_type);
    SPI_connect_my(src.data);
    parseTypeStringMy(work.schema_type, &type, &typmod);
    if (!OidIsValid(type)) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_update(void) {
    char *schema = quote_literal_cstr(work.shared->schema);
    char *table = quote_literal_cstr(work.shared->table);
    const char *wake_up_quote;
    StringInfoData src;
    StringInfoData wake_up;
    initStringInfoMy(&wake_up);
    appendStringInfo(&wake_up, "%1$s_wake_up", work.shared->table);
    wake_up_quote = quote_identifier(wake_up.data);
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        DO $DO$ BEGIN
            IF NOT EXISTS (SELECT * FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'data') THEN
                ALTER TABLE %1$s ADD COLUMN "data" text;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'active') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_active'::text))::interval$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "active" SET DEFAULT (pg_catalog.current_setting('pg_task.active'))::pg_catalog.interval;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'live') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_live'::text))::interval$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "live" SET DEFAULT (pg_catalog.current_setting('pg_task.live'))::pg_catalog.interval;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'repeat') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_repeat'::text))::interval$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "repeat" SET DEFAULT (pg_catalog.current_setting('pg_task.repeat'))::pg_catalog.interval;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'timeout') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_timeout'::text))::interval$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "timeout" SET DEFAULT (pg_catalog.current_setting('pg_task.timeout'))::pg_catalog.interval;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'count') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_count'::text))::integer$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "count" SET DEFAULT (pg_catalog.current_setting('pg_task.count'))::pg_catalog.int4;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'max') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_max'::text))::integer$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "max" SET DEFAULT (pg_catalog.current_setting('pg_task.max'))::pg_catalog.int4;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'delete') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_delete'::text))::bool$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "delete" SET DEFAULT (pg_catalog.current_setting('pg_task.delete'))::pg_catalog.bool;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'drift') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_drift'::text))::bool$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "drift" SET DEFAULT (pg_catalog.current_setting('pg_task.drift'))::pg_catalog.bool;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'header') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_header'::text))::bool$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "header" SET DEFAULT (pg_catalog.current_setting('pg_task.header'))::pg_catalog.bool;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'string') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_string'::text))::bool$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "string" SET DEFAULT (pg_catalog.current_setting('pg_task.string'))::pg_catalog.bool;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'delimiter') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_delimiter'::text))::"char"$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "delimiter" SET DEFAULT (pg_catalog.current_setting('pg_task.delimiter'))::pg_catalog.char;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'escape') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_escape'::text))::"char"$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "escape" SET DEFAULT (pg_catalog.current_setting('pg_task.escape'))::pg_catalog.char;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'quote') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_quote'::text))::"char"$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "quote" SET DEFAULT (pg_catalog.current_setting('pg_task.quote'))::pg_catalog.char;
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'group') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_group'::text))::text$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "group" SET DEFAULT (pg_catalog.current_setting('pg_task.group'));
            END IF;
            IF (SELECT column_default FROM information_schema.columns WHERE table_schema OPERATOR(pg_catalog.=) %2$s AND table_name OPERATOR(pg_catalog.=) %3$s AND column_name OPERATOR(pg_catalog.=) 'null') IS NOT DISTINCT FROM $$(current_setting('pg_task.default_null'::text))::text$$ THEN
                ALTER TABLE %1$s ALTER COLUMN "null" SET DEFAULT (pg_catalog.current_setting('pg_task.null'));
            END IF;
            CREATE OR REPLACE FUNCTION %4$s.%5$s() RETURNS TRIGGER SET search_path = pg_catalog, pg_temp AS $function$BEGIN
                PERFORM pg_catalog.pg_cancel_backend(pid) FROM "pg_catalog"."pg_locks" WHERE "locktype" OPERATOR(pg_catalog.=) 'userlock' AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock' AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3 AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) current_catalog) AND "classid" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_authid" WHERE "rolname" OPERATOR(pg_catalog.=) current_user) AND "objid" OPERATOR(pg_catalog.=) %6$i;
                RETURN NULL;
            END;$function$ LANGUAGE plpgsql;
            IF NOT EXISTS (SELECT * FROM information_schema.triggers WHERE event_object_table OPERATOR(pg_catalog.=) %3$s AND trigger_name OPERATOR(pg_catalog.=) 'wake_up' AND trigger_schema OPERATOR(pg_catalog.=) %2$s AND event_object_schema OPERATOR(pg_catalog.=) %2$s AND action_orientation OPERATOR(pg_catalog.=) 'STATEMENT' AND action_timing OPERATOR(pg_catalog.=) 'AFTER' AND event_manipulation OPERATOR(pg_catalog.=) 'INSERT') THEN
                CREATE TRIGGER wake_up AFTER INSERT ON %1$s FOR EACH STATEMENT EXECUTE PROCEDURE %4$s.%5$s();
            END IF;
        END; $DO$
    ), work.schema_table, schema, table, work.schema, wake_up_quote, work.hash);
    SPI_connect_my(src.data);
    SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_finish_my();
    if (wake_up_quote != wake_up.data) pfree((void *)wake_up_quote);
    pfree(schema);
    pfree(src.data);
    pfree(table);
    pfree(wake_up.data);
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

void work_main(Datum arg) {
    const char *application_name;
    const char *index_hash[] = {"hash"};
    const char *index_input[] = {"input"};
    const char *index_parent[] = {"parent"};
    const char *index_plan[] = {"plan"};
    const char *index_state[] = {"state"};
    Datum datum;
    instr_time current_time_reset;
    instr_time current_time_sleep;
    instr_time start_time_reset;
    instr_time start_time_sleep;
    long current_reset = -1;
    long current_sleep = -1;
    StringInfoData schema_table, schema_type;
    elog(DEBUG1, "arg = %i", DatumGetInt32(arg));
    work.shared = &workshared[DatumGetInt32(arg)];
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_DISPATCH;
    optimizer = false;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
    on_shmem_exit(work_shmem_exit, arg);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, work_idle);
    BackgroundWorkerUnblockSignals();
    work.data = quote_identifier(work.shared->data);
    work.schema = quote_identifier(work.shared->schema);
    work.table = quote_identifier(work.shared->table);
    work.user = quote_identifier(work.shared->user);
    BackgroundWorkerInitializeConnectionMy(work.shared->data, work.shared->user);
    MemoryContextSwitchTo(TopMemoryContext);
    application_name = MyBgworkerEntry->bgw_name + strlen(work.shared->user) + 1 + strlen(work.shared->data) + 1;
    set_config_option_my("application_name", application_name, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname(application_name);
    set_ps_display_my("main");
    process_session_preload_libraries();
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work.schema, work.table);
    work.schema_table = schema_table.data;
    datum = CStringGetTextDatumMy(application_name);
    work.hash = DatumGetInt32(DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID, datum));
    pfree((void *)datum);
    if (!lock_data_user_hash(MyDatabaseId, GetUserId(), work.hash)) { elog(WARNING, "!lock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.hash); ShutdownRequestPending = true; return; } // exit without error to disable restart, then not start conf
    dlist_init(&head);
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work.schema);
    work.schema_type = schema_type.data;
    elog(DEBUG1, "sleep = %li, reset = %li, schema_table = %s, schema_type = %s, hash = %i", work.shared->sleep, work.shared->reset, work.schema_table, work.schema_type, work.hash);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
    work_schema(work.schema);
    work_type();
    work_table();
    work_update();
    work_index(countof(index_hash), index_hash);
    work_index(countof(index_input), index_input);
    work_index(countof(index_parent), index_parent);
    work_index(countof(index_plan), index_plan);
    work_index(countof(index_state), index_state);
#ifdef GP_VERSION_NUM
    Gp_role = GP_ROLE_DISPATCH;
#if PG_VERSION_NUM < 120000
    Gp_session_role = GP_ROLE_DISPATCH;
#endif
#endif
    set_ps_display_my("idle");
    work_reset();
    while (!ShutdownRequestPending) {
        int nevents = work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
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
        current_timeout = Min(current_reset, current_sleep);
        if (idle_count >= (uint64)task_idle) work_timeout();
        nevents = WaitEventSetWaitMy(set, current_timeout, events, nevents);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_LATCH_SET) work_latch();
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        INSTR_TIME_SET_CURRENT(current_time_reset);
        INSTR_TIME_SUBTRACT(current_time_reset, start_time_reset);
        current_reset = work.shared->reset - (long)INSTR_TIME_GET_MILLISEC(current_time_reset);
        if (current_reset <= 0) work_reset();
        INSTR_TIME_SET_CURRENT(current_time_sleep);
        INSTR_TIME_SUBTRACT(current_time_sleep, start_time_sleep);
        current_sleep = work.shared->sleep - (long)INSTR_TIME_GET_MILLISEC(current_time_sleep);
        if (current_sleep <= 0) work_sleep();
        FreeWaitEventSet(set);
        pfree(events);
    }
    if (!unlock_data_user_hash(MyDatabaseId, GetUserId(), work.hash)) elog(WARNING, "!unlock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.hash);
}
