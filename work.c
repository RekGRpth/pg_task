#include "include.h"

extern char *default_null;
extern int work_fetch;
extern Task task;
static dlist_head head;
static dlist_head local;
static dlist_head remote;
static emit_log_hook_type emit_log_hook_prev = NULL;
Work work;

static void work_query(Task *t);

#define ereport_my(elevel, finish, ...) do { \
    task = *t; \
    emit_log_hook_prev = emit_log_hook; \
    emit_log_hook = task_error; \
    ereport(elevel, __VA_ARGS__); \
    *t = task; \
    if (task_done(t) || finish) work_finish(t); \
    MemSet(&task, 0, sizeof(task)); \
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
        appendStringInfoString(&src, init_check());
        appendStringInfo(&src, SQL(%1$sWHERE "user" = current_user AND "data" = current_catalog AND "schema" = current_setting('pg_task.schema') AND "table" = current_setting('pg_task.table') AND "timeout" = current_setting('pg_task.timeout')::bigint), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT);
    if (!SPI_processed) ShutdownRequestPending = true;
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

static void work_fatal(Task *t, PGresult *result) {
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

static void work_finish(Task *t) {
    dlist_delete(&t->node);
    PQfinish(t->conn);
    if (!proc_exit_inprogress && t->pid && !unlock_table_pid_hash(work.shared->oid, t->pid, t->shared->hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work.shared->oid, t->pid, t->shared->hash);
    task_free(t);
    pfree(t->shared);
    pfree(t);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 2;
    dlist_foreach_modify(iter, &head) {
        Task *t = dlist_container(Task, node, iter.cur);
        if (PQstatus(t->conn) == CONNECTION_BAD) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn)))); continue; }
        if (PQsocket(t->conn) == PGINVALID_SOCKET) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsocket == PGINVALID_SOCKET"), errdetail("%s", PQerrorMessageMy(t->conn)))); continue; }
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
    names = stringToQualifiedNameList(idx.data);
    rangevar = makeRangeVarFromNameList(names);
    elog(DEBUG1, "index = %s, schema_table = %s", idx.data, work.schema_table);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    } else if ((relation = relation_openrv_extended_my(rangevar, AccessShareLock, true, false))) {
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
    Datum values[] = {ObjectIdGetDatum(work.shared->oid)};
    Portal portal;
    static Oid argtypes[] = {OIDOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("reset");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                LEFT JOIN "pg_locks" AS l ON "locktype" = 'userlock' AND "mode" = 'AccessExclusiveLock' AND "granted" AND "objsubid" = 4 AND "database" = $1 AND "classid" = "id">>32 AND "objid" = "id"<<32>>32
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "state" IN ('TAKE'::%2$s, 'WORK'::%2$s) AND l.pid IS NULL
                FOR UPDATE OF t %3$s
            ) UPDATE %1$s AS t SET "state" = 'PLAN'::%2$s, "start" = NULL, "stop" = NULL, "pid" = NULL FROM s
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work.schema_table, work.schema_type,
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
        SPI_cursor_fetch(portal, true, work_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) elog(WARNING, "row = %lu, reset id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    } while (SPI_processed);
    SPI_cursor_close(portal);
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    work_check();
    if (!ShutdownRequestPending) work_reset();
}

static void work_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) work_reload();
}

static void work_readable(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && !PQconsumeInput(t->conn)) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
    t->socket(t);
}

static void work_done(Task *t) {
    if (PQstatus(t->conn) == CONNECTION_OK && PQtransactionStatus(t->conn) != PQTRANS_IDLE) {
        t->socket = work_done;
        if (!PQsendQuery(t->conn, SQL(COMMIT))) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
        t->event = WL_SOCKET_READABLE;
        return;
    }
    task_done(t) || PQstatus(t->conn) != CONNECTION_OK ? work_finish(t) : work_query(t);
}

static void work_schema(const char *schema_quote) {
    List *names = stringToQualifiedNameList(schema_quote);
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

static void work_headers(Task *t, PGresult *result) {
    if (t->output.len) appendStringInfoString(&t->output, "\n");
    for (int col = 0; col < PQnfields(result); col++) {
        if (col > 0) appendStringInfoChar(&t->output, t->delimiter);
        appendBinaryStringInfoEscapeQuote(&t->output, PQfname(result, col), strlen(PQfname(result, col)), false, t->escape, t->quote);
    }
}

static void work_success(Task *t, PGresult *result, int row) {
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
        case -2: ereport_my(WARNING, true, (errmsg("id = %li, PQgetCopyData == -2", t->shared->id), errdetail("%s", PQerrorMessageMy(t->conn)))); if (buffer) PQfreemem(buffer); return;
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
        ereport_my(WARNING, false, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task not active")));
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
    if (!PQsendQuery(t->conn, input.data)) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); pfree(input.data); return; }
    pfree(input.data);
    t->socket = work_result;
    t->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *t) {
    bool connected = false;
    switch (PQstatus(t->conn)) {
        case CONNECTION_BAD: ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn)))); return;
        case CONNECTION_OK: elog(DEBUG1, "id = %li, PQstatus == CONNECTION_OK", t->shared->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(t->conn)) {
        case PGRES_POLLING_ACTIVE: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", t->shared->id); break;
        case PGRES_POLLING_FAILED: ereport_my(WARNING, true, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectPoll failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return;
        case PGRES_POLLING_OK: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_OK", t->shared->id); connected = true; break;
        case PGRES_POLLING_READING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_READING", t->shared->id); t->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_WRITING", t->shared->id); t->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if (!(t->pid = PQbackendPID(t->conn))) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQbackendPID failed"), errdetail("%s", PQerrorMessageMy(t->conn)))); return; }
        if (!lock_table_pid_hash(work.shared->oid, t->pid, t->shared->hash)) { ereport_my(WARNING, true, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("!lock_table_pid_hash(%i, %i, %i)", work.shared->oid, t->pid, t->shared->hash))); return; }
        work_query(t);
    }
}

static void work_proc_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    elog(DEBUG1, "code = %i", code);
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
        if (!ShutdownRequestPending) init_work(true);
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
    elog(DEBUG1, "id = %li, group = %s, remote = %s, max = %i, oid = %i", t->shared->id, t->group, t->remote ? t->remote : default_null, t->shared->max, work.shared->oid);
    dlist_delete(&t->node);
    dlist_push_head(&head, &t->node);
    if (!opts) { ereport_my(WARNING, true, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("PQconninfoParse failed"), errdetail("%s", work_errstr(err)))); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        elog(DEBUG1, "%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "password")) password = true;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "options")) { options = opt->val; continue; }
        arg++;
    }
    if (!superuser() && !password) { ereport_my(WARNING, true, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superusers must provide a password in the connection string."))); PQconninfoFree(opts); return; }
    keywords = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*keywords));
    values = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*values));
    initStringInfoMy(&name);
    appendStringInfo(&name, "pg_task %s %s %s", work.shared->schema, work.shared->table, t->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(&value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, "%s-c pg_task.data=%s", value.len ? " " : "", work.shared->data);
    appendStringInfo(&value, " -c pg_task.user=%s", work.shared->user);
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
    if (!(t->conn = PQconnectStartParams(keywords, values, false))) ereport_my(WARNING, true, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectStartParams failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (PQstatus(t->conn) == CONNECTION_BAD) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (!PQisnonblocking(t->conn) && PQsetnonblocking(t->conn, true) == -1) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetnonblocking failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    else if (!superuser() && !PQconnectionUsedPassword(t->conn)) ereport_my(WARNING, true, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superuser cannot connect if the server does not request a password."), errhint("Target server's authentication method must be changed.")));
    else if (PQclientEncoding(t->conn) != GetDatabaseEncoding() && !PQsetClientEncoding(t->conn, GetDatabaseEncodingName())) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetClientEncoding failed"), errdetail("%s", PQerrorMessageMy(t->conn))));
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
    if (t->group) pfree(t->group);
    t->group = NULL;
}

static void work_table(void) {
    List *names = stringToQualifiedNameList(work.schema_table);
    const RangeVar *rangevar = makeRangeVarFromNameList(names);
    StringInfoData src, hash;
    elog(DEBUG1, "schema_table = %s, schema_type = %s", work.schema_table, work.schema_type);
    set_ps_display_my("table");
    set_config_option_my("pg_task.table", work.shared->table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(&hash);
#if PG_VERSION_NUM >= 120000
    appendStringInfo(&hash, SQL(GENERATED ALWAYS AS (hashtext("group"||COALESCE("remote", '%1$s'))) STORED), "");
#else
    if (true) {
        const char *function_quote;
        StringInfoData function;
        initStringInfoMy(&function);
        appendStringInfo(&function, "%1$s_hash_generate", work.shared->table);
        function_quote = quote_identifier(function.data);
        appendStringInfo(&hash, SQL(;CREATE OR REPLACE FUNCTION %1$s.%2$s() RETURNS TRIGGER AS $$BEGIN
            IF tg_op = 'INSERT' OR (new.group, new.remote) IS DISTINCT FROM (old.group, old.remote) THEN
                new.hash = hashtext(new.group||COALESCE(new.remote, '%3$s'));
            END IF;
            return new;
        end;$$ LANGUAGE plpgsql;
        CREATE TRIGGER hash_generate BEFORE INSERT OR UPDATE ON %4$s FOR EACH ROW EXECUTE PROCEDURE %1$s.%2$s()), work.schema, function_quote, "", work.schema_table);
        if (function_quote != function.data) pfree((void *)function_quote);
        pfree(function.data);
    }
#endif
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        CREATE TABLE %1$s (
            "id" bigserial NOT NULL PRIMARY KEY,
            "parent" bigint DEFAULT NULLIF(current_setting('pg_task.id')::bigint, 0),
            "plan" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "start" timestamp with time zone,
            "stop" timestamp with time zone,
            "active" interval NOT NULL DEFAULT current_setting('pg_task.default_active')::interval CHECK ("active" > '0 sec'::interval),
            "live" interval NOT NULL DEFAULT current_setting('pg_task.default_live')::interval CHECK ("live" >= '0 sec'::interval),
            "repeat" interval NOT NULL DEFAULT current_setting('pg_task.default_repeat')::interval CHECK ("repeat" >= '0 sec'::interval),
            "timeout" interval NOT NULL DEFAULT current_setting('pg_task.default_timeout')::interval CHECK ("timeout" >= '0 sec'::interval),
            "count" integer NOT NULL DEFAULT current_setting('pg_task.count')::integer CHECK ("count" >= 0),
            "hash" integer NOT NULL %3$s,
            "max" integer NOT NULL DEFAULT current_setting('pg_task.default_max')::integer,
            "pid" integer,
            "state" %2$s NOT NULL DEFAULT 'PLAN'::%2$s,
            "delete" boolean NOT NULL DEFAULT current_setting('pg_task.delete')::boolean,
            "drift" boolean NOT NULL DEFAULT current_setting('pg_task.drift')::boolean,
            "header" boolean NOT NULL DEFAULT current_setting('pg_task.header')::boolean,
            "string" boolean NOT NULL DEFAULT current_setting('pg_task.string')::boolean,
            "delimiter" "char" NOT NULL DEFAULT current_setting('pg_task.default_delimiter')::"char",
            "escape" "char" NOT NULL DEFAULT current_setting('pg_task.default_escape')::"char",
            "quote" "char" NOT NULL DEFAULT current_setting('pg_task.default_quote')::"char",
            "error" text,
            "group" text NOT NULL DEFAULT current_setting('pg_task.default_group'),
            "input" text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.default_null'),
            "output" text,
            "remote" text
        )
    ), work.schema_table, work.schema_type,
#if PG_VERSION_NUM >= 120000
        hash.data
#else
        ""
#endif
    );
#if PG_VERSION_NUM >= 120000
#else
    appendStringInfoString(&src, hash.data);
#endif
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    work.shared->oid = RangeVarGetRelid(rangevar, NoLock, false);
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    set_config_option_my("pg_task.table", work.shared->table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    resetStringInfo(&src);
    appendStringInfo(&src, "%i", work.shared->oid);
    set_config_option_my("pg_task.oid", src.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(hash.data);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_task(Task *t) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len;
    elog(DEBUG1, "id = %li, group = %s, max = %i, oid = %i", t->shared->id, t->group, t->shared->max, work.shared->oid);
    dlist_delete(&t->node);
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", work.shared->user, work.shared->data, work.shared->schema, work.shared->table, t->group)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(t->seg));
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    pfree(handle);
    dsm_detach(t->seg);
    task_free(t);
    pfree(t);
}

static void work_type(void) {
    int32 typmod;
    Oid type = InvalidOid;
    StringInfoData src;
    set_ps_display_my("type");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'STOP')), work.schema_type);
    SPI_connect_my(src.data);
    parseTypeString(work.schema_type, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_timeout(void) {
    Datum values[] = {ObjectIdGetDatum(work.shared->oid)};
    dlist_mutable_iter iter;
    Portal portal;
    static Oid argtypes[] = {OIDOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("timeout");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH l AS (
                SELECT count("classid") AS "classid", "objid" FROM "pg_locks" WHERE "locktype" = 'userlock' AND "mode" = 'AccessShareLock' AND "granted" AND "objsubid" = 5 AND "database" = $1 GROUP BY "objid"
            ), s AS (
                SELECT "id", t.hash, CASE WHEN "max" >= 0 THEN "max" ELSE 0 END - COALESCE("classid", 0) AS "count" FROM %1$s AS t LEFT JOIN l ON "objid" = "hash"
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "state" = 'PLAN'::%2$s AND CASE WHEN "max" >= 0 THEN "max" ELSE 0 END - COALESCE("classid", 0) >= 0
                ORDER BY 3 DESC, 1 LIMIT current_setting('pg_task.limit')::integer FOR UPDATE OF t %3$s
            ), u AS (
                SELECT "id", "count" - row_number() OVER (PARTITION BY "hash" ORDER BY "count" DESC, "id") + 1 AS "count" FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS t SET "state" = 'TAKE'::%2$s FROM u
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = u.id AND u.count >= 0 RETURNING t.id, "hash", "group", "remote", "max"
        ), work.schema_table, work.schema_type,
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
        SPI_cursor_fetch(portal, true, work_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            Task *t = MemoryContextAllocZero(TopMemoryContext, sizeof(*t));
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            t->group = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "group", false));
            t->remote = TextDatumGetCStringMy(SPI_getbinval_my(val, tupdesc, "remote", true));
            t->shared = t->remote ? MemoryContextAllocZero(TopMemoryContext, sizeof(*t->shared)) : shm_toc_allocate_my(PG_TASK_MAGIC, &t->seg, sizeof(*t->shared));
            t->shared->handle = DatumGetUInt32(MyBgworkerEntry->bgw_main_arg);
            t->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false));
            t->shared->id = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "id", false));
            t->shared->max = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "max", false));
            elog(DEBUG1, "row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, t->shared->id, t->shared->hash, t->group, t->remote ? t->remote : default_null, t->shared->max);
            dlist_push_head(t->remote ? &remote : &local, &t->node);
        }
    } while (SPI_processed);
    SPI_cursor_close(portal);
    SPI_finish_my();
    dlist_foreach_modify(iter, &local) work_task(dlist_container(Task, node, iter.cur));
    dlist_foreach_modify(iter, &remote) work_remote(dlist_container(Task, node, iter.cur));
    set_ps_display_my("idle");
}

static void work_writeable(Task *t) {
    t->socket(t);
}

void work_main(Datum arg) {
    const char *index_hash[] = {"hash"};
    const char *index_input[] = {"input"};
    const char *index_parent[] = {"parent"};
    const char *index_plan[] = {"plan"};
    const char *index_state[] = {"state"};
    Datum datum;
    dsm_segment *seg;
    instr_time current_reset_time;
    instr_time current_timeout_time;
    instr_time start_time;
    long current_reset = -1;
    long current_timeout = -1;
    shm_toc *toc;
    StringInfoData schema_table, schema_type, timeout;
    on_proc_exit(work_proc_exit, (Datum)NULL);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    CreateAuxProcessResourceOwner();
#ifdef GP_VERSION_NUM
    optimizer = false;
    Gp_role = GP_ROLE_UTILITY;
#if PG_VERSION_NUM >= 120000
#else
    Gp_session_role = GP_ROLE_UTILITY;
#endif
#endif
    if (!(seg = dsm_attach(DatumGetUInt32(arg)))) { ereport(WARNING, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment"))); return; }
#if PG_VERSION_NUM >= 100000
    dsm_unpin_segment(dsm_segment_handle(seg));
#endif
    if (!(toc = shm_toc_attach(PG_WORK_MAGIC, dsm_segment_address(seg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("bad magic number in dynamic shared memory segment")));
    work.shared = shm_toc_lookup_my(toc, 0, false);
    work.data = quote_identifier(work.shared->data);
    work.schema = quote_identifier(work.shared->schema);
    work.table = quote_identifier(work.shared->table);
    work.user = quote_identifier(work.shared->user);
    BackgroundWorkerInitializeConnectionMy(work.shared->data, work.shared->user, 0);
    set_config_option_my("application_name", MyBgworkerEntry->bgw_name + strlen(work.shared->user) + 1 + strlen(work.shared->data) + 1, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pgstat_report_appname(MyBgworkerEntry->bgw_name + strlen(work.shared->user) + 1 + strlen(work.shared->data) + 1);
    set_ps_display_my("main");
    process_session_preload_libraries();
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work.schema, work.table);
    work.schema_table = schema_table.data;
    datum = CStringGetTextDatumMy(work.schema_table);
    work.hash = DatumGetInt32(DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID, datum));
    pfree((void *)datum);
    if (!lock_data_user_hash(MyDatabaseId, GetUserId(), work.hash)) { elog(WARNING, "!lock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.hash); ShutdownRequestPending = true; return; }
    dlist_init(&head);
    dlist_init(&local);
    dlist_init(&remote);
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work.schema);
    work.schema_type = schema_type.data;
    elog(DEBUG1, "timeout = %li, reset = %li, schema_table = %s, schema_type = %s, hash = %i", work.shared->timeout, work.shared->reset, work.schema_table, work.schema_type, work.hash);
    work_schema(work.schema);
    set_config_option_my("pg_task.schema", work.shared->schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    work_type();
    work_table();
    work_index(countof(index_hash), index_hash);
    work_index(countof(index_input), index_input);
    work_index(countof(index_parent), index_parent);
    work_index(countof(index_plan), index_plan);
    work_index(countof(index_state), index_state);
    set_config_option_my("pg_task.data", work.shared->data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.user", work.shared->user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(&timeout);
    appendStringInfo(&timeout, "%li", work.shared->timeout);
    set_config_option_my("pg_task.timeout", timeout.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(timeout.data);
    set_ps_display_my("idle");
    work_reset();
    while (!ShutdownRequestPending) {
        int nevents = work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        work_events(set);
        if (current_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            current_timeout = work.shared->timeout;
        }
        if (current_reset <= 0) current_reset = work.shared->reset;
        nevents = WaitEventSetWaitMy(set, current_timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) work_latch();
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        INSTR_TIME_SET_CURRENT(current_timeout_time);
        INSTR_TIME_SUBTRACT(current_timeout_time, start_time);
        current_timeout = work.shared->timeout - (long)INSTR_TIME_GET_MILLISEC(current_timeout_time);
        if (work.shared->reset >= 0) {
            INSTR_TIME_SET_CURRENT(current_reset_time);
            INSTR_TIME_SUBTRACT(current_reset_time, start_time);
            current_reset = work.shared->reset - (long)INSTR_TIME_GET_MILLISEC(current_reset_time);
            if (current_reset <= 0) work_reset();
        }
        if (current_timeout <= 0) work_timeout();
        FreeWaitEventSet(set);
        pfree(events);
    }
    if (!unlock_data_user_hash(MyDatabaseId, GetUserId(), work.hash)) elog(WARNING, "!unlock_data_user_hash(%i, %i, %i)", MyDatabaseId, GetUserId(), work.hash);
}
