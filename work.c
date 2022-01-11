#include "include.h"

extern char *default_null;
extern Task *task;
static emit_log_hook_type emit_log_hook_prev = NULL;
static Task **taskp = &task;
static void work_query(Task *task);
Work *work;

#define ereport_my(elevel, finish, ...) do { \
    *taskp = task; \
    emit_log_hook_prev = emit_log_hook; \
    emit_log_hook = task_error; \
    ereport(elevel, __VA_ARGS__); \
    if (task_done(task) || finish) work_finish(task); \
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
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, init_check(), "");
        appendStringInfo(&src, SQL(%1$sWHERE "user" = current_user AND data = current_catalog AND schema = current_setting('pg_task.schema') AND "table" = current_setting('pg_task.table') AND timeout = current_setting('pg_task.timeout')::bigint), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    if (!SPI_processed) ShutdownRequestPending = true;
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_command(Task *task, PGresult *result) {
    if (task->skip) { task->skip--; return; }
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    appendStringInfo(&task->output, "%s%s", task->output.len ? "\n" : "", PQcmdStatus(result));
}

static void work_event(WaitEventSet *set) {
    dlist_mutable_iter iter;
#if PG_VERSION_NUM >= 90500
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
#else
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, &MyProc->procLatch, NULL);
#endif
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &work->head) {
        Task *task = dlist_container(Task, node, iter.cur);
        AddWaitEventToSet(set, task->event, PQsocket(task->conn), NULL, task);
    }
}

static void work_fatal(Task *task, PGresult *result) {
    char *value = NULL;
    char *value2 = NULL;
    char *value3 = NULL;
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
    appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
    task->skip++;
    if (task->error.len) appendStringInfoChar(&task->error, '\n');
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY))) appendStringInfo(&task->error, "%s:  ", _(value));
    if (Log_error_verbosity >= PGERROR_VERBOSE && (value = PQresultErrorField(result, PG_DIAG_SQLSTATE))) appendStringInfo(&task->error, "%s: ", value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY))) append_with_tabs(&task->error, value);
    else append_with_tabs(&task->error, _("missing error text"));
    if ((value = PQresultErrorField(result, PG_DIAG_STATEMENT_POSITION))) appendStringInfo(&task->error, _(" at character %s"), value);
    else if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_POSITION))) appendStringInfo(&task->error, _(" at character %s"), value);
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL))) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("DETAIL:  "));
            append_with_tabs(&task->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT))) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("HINT:  "));
            append_with_tabs(&task->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_QUERY))) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("QUERY:  "));
            append_with_tabs(&task->error, value);
        }
        if ((value = PQresultErrorField(result, PG_DIAG_CONTEXT))) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("CONTEXT:  "));
            append_with_tabs(&task->error, value);
        }
        if (Log_error_verbosity >= PGERROR_VERBOSE) {
            value2 = PQresultErrorField(result, PG_DIAG_SOURCE_FILE);
            value3 = PQresultErrorField(result, PG_DIAG_SOURCE_LINE);
            if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION)) && value2) { // assume no newlines in funcname or filename...
                if (task->error.len) appendStringInfoChar(&task->error, '\n');
                appendStringInfo(&task->error, _("LOCATION:  %s, %s:%s"), value, value2, value3);
            } else if (value2) {
                if (task->error.len) appendStringInfoChar(&task->error, '\n');
                appendStringInfo(&task->error, _("LOCATION:  %s:%s"), value2, value3);
            }
        }
    }
    if (is_log_level_output(severity_error(PQresultErrorField(result, PG_DIAG_SEVERITY)), log_min_error_statement)) { // If the user wants the query that generated this error logged, do it.
        if (task->error.len) appendStringInfoChar(&task->error, '\n');
        appendStringInfoString(&task->error, _("STATEMENT:  "));
        append_with_tabs(&task->error, task->input);
    }
}

static void work_free(Task *task) {
    task_free(task);
    pfree(task);
}

static void work_finish(Task *task) {
    dlist_delete(&task->node);
    PQfinish(task->conn);
    if (!proc_exit_inprogress && task->pid && !unlock_table_pid_hash(work->oid.table, task->pid, task->hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work->oid.table, task->pid, task->hash);
    work_free(task);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 0;
    dlist_foreach_modify(iter, &work->head) {
        Task *task = dlist_container(Task, node, iter.cur);
        if (PQstatus(task->conn) == CONNECTION_BAD) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(task->conn)))); continue; }
        if (PQsocket(task->conn) == PGINVALID_SOCKET) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsocket == PGINVALID_SOCKET"), errdetail("%s", PQerrorMessageMy(task->conn)))); continue; }
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
    initStringInfoMy(TopMemoryContext, &name);
    appendStringInfoString(&name, work->str.table);
    for (int i = 0; i < count; i++) {
        const char *index = indexes[i];
        appendStringInfoString(&name, "_");
        appendStringInfoString(&name, index);
    }
    appendStringInfoString(&name, "_idx");
    name_quote = quote_identifier(name.data);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE INDEX %s ON %s USING btree ), name_quote, work->schema_table);
    appendStringInfoString(&src, "(");
    for (int i = 0; i < count; i++) {
        const char *index = indexes[i];
        const char *index_quote = quote_identifier(index);
        if (i) appendStringInfoString(&src, ", ");
        appendStringInfoString(&src, index_quote);
        if (index_quote != index) pfree((void *)index_quote);
    }
    appendStringInfoString(&src, ")");
    initStringInfoMy(TopMemoryContext, &idx);
    appendStringInfo(&idx, "%s.%s", work->quote.schema, name_quote);
    names = stringToQualifiedNameList(idx.data);
    rangevar = makeRangeVarFromNameList(names);
    elog(DEBUG1, "index = %s, schema_table = %s", idx.data, work->schema_table);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    } else if ((relation = relation_openrv_extended(rangevar, AccessShareLock, true))) {
        if (relation->rd_index && relation->rd_index->indrelid != work->oid.table) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
        relation_close(relation, AccessShareLock);
    }
    SPI_commit_my();
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
    Datum values[] = {ObjectIdGetDatum(work->oid.table)};
    static Oid argtypes[] = {OIDOID};
    StringInfoData src;
    set_ps_display_my("reset");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(
        WITH s AS (
            SELECT id FROM %1$s AS t
            LEFT JOIN pg_locks AS l ON l.locktype = 'userlock' AND l.mode = 'AccessExclusiveLock' AND l.granted AND l.objsubid = 4 AND l.database = $1 AND l.classid = t.id>>32 AND l.objid = t.id<<32>>32
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND state IN ('TAKE'::%2$s, 'WORK'::%2$s) AND l.pid IS NULL
            FOR UPDATE OF t %3$s
        ) UPDATE %1$s AS t SET state = 'PLAN'::%2$s, start = NULL, stop = NULL, pid = NULL FROM s
        WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
    ), work->schema_table, work->schema_type,
#if PG_VERSION_NUM >= 90500
        "SKIP LOCKED"
#else
        ""
#endif
    );
    SPI_connect_my(src.data);
    SPI_execute_with_args_my(src.data, countof(argtypes), argtypes, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_processed; row++) elog(WARNING, "row = %lu, reset id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    work_check();
    if (!ShutdownRequestPending) work_reset();
}

static void work_latch(void) {
#if PG_VERSION_NUM >= 90500
    ResetLatch(MyLatch);
#else
    ResetLatch(&MyProc->procLatch);
#endif
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) work_reload();
}

static void work_readable(Task *task) {
    if (PQstatus(task->conn) == CONNECTION_OK && !PQconsumeInput(task->conn)) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("!PQconsumeInput"), errdetail("%s", PQerrorMessageMy(task->conn)))); return; }
    task->socket(task);
}

static void work_done(Task *task) {
    if (PQstatus(task->conn) == CONNECTION_OK && PQtransactionStatus(task->conn) != PQTRANS_IDLE) {
        task->socket = work_done;
        if (!PQsendQuery(task->conn, SQL(COMMIT))) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(task->conn)))); return; }
        task->event = WL_SOCKET_READABLE;
        return;
    }
    task_done(task) || PQstatus(task->conn) != CONNECTION_OK ? work_finish(task) : work_query(task);
}

static Oid work_schema(const char *schema_quote) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "schema = %s", schema_quote);
    set_ps_display_my("schema");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE SCHEMA %s), schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    oid = get_namespace_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

static void work_headers(Task *task, PGresult *result) {
    if (task->output.len) appendStringInfoString(&task->output, "\n");
    for (int col = 0; col < PQnfields(result); col++) {
        const char *value = PQfname(result, col);
        if (col > 0) appendStringInfoChar(&task->output, task->delimiter);
        if (task->quote) appendStringInfoChar(&task->output, task->quote);
        if (task->escape) init_escape(&task->output, value, strlen(value), task->escape);
        else appendStringInfoString(&task->output, value);
        if (task->quote) appendStringInfoChar(&task->output, task->quote);
    }
}

static void work_success(Task *task, PGresult *result, int row) {
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (task->header && !row && PQnfields(result) > 1) work_headers(task, result);
    if (task->output.len) appendStringInfoString(&task->output, "\n");
    for (int col = 0; col < PQnfields(result); col++) {
        const char *value = PQgetvalue(result, row, col);
        int len = PQgetlength(result, row, col);
        if (col > 0) appendStringInfoChar(&task->output, task->delimiter);
        if (PQgetisnull(result, row, col)) appendStringInfoString(&task->output, task->null); else {
            if (!init_oid_is_string(PQftype(result, col)) && task->string) {
                if (len) appendStringInfoString(&task->output, value);
            } else {
                if (task->quote) appendStringInfoChar(&task->output, task->quote);
                if (len) {
                    if (task->escape) init_escape(&task->output, value, len, task->escape);
                    else appendStringInfoString(&task->output, value);
                }
                if (task->quote) appendStringInfoChar(&task->output, task->quote);
            }
        }
    }
}

static void work_result(Task *task) {
    for (PGresult *result; PQstatus(task->conn) == CONNECTION_OK && (result = PQgetResult(task->conn)); ) {
        switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK: work_command(task, result); break;
            case PGRES_FATAL_ERROR: ereport(WARNING, (errmsg("id = %li, PQresultStatus == PGRES_FATAL_ERROR", task->id), errdetail("%s", PQresultErrorMessageMy(result)))); work_fatal(task, result); break;
            case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) work_success(task, result, row); break;
            default: elog(DEBUG1, "id = %li, %s", task->id, PQresStatus(PQresultStatus(result))); break;
        }
        PQclear(result);
    }
    work_done(task);
}

static void work_query(Task *task) {
    StringInfoData input;
    for (;;) {
        if (ShutdownRequestPending) return;
        task->socket = work_query;
        if (task_work(task)) { work_finish(task); return; }
        if (task->active) break;
        ereport_my(WARNING, false, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task not active")));
        if (!task->id) return;
    }
    initStringInfoMy(TopMemoryContext, &input);
    task->skip = 0;
    appendStringInfoString(&input, SQL(BEGIN;));
    task->skip++;
    appendStringInfo(&input, SQL(SET SESSION "pg_task.id" = %li;), task->id);
    task->skip++;
    if (task->timeout) {
        appendStringInfo(&input, SQL(SET SESSION "statement_timeout" = %i;), task->timeout);
        task->skip++;
    }
    appendStringInfoString(&input, SQL(COMMIT;));
    task->skip++;
    appendStringInfoString(&input, task->input);
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task->id, task->timeout, input.data, task->count);
    if (!PQsendQuery(task->conn, input.data)) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsendQuery failed"), errdetail("%s", PQerrorMessageMy(task->conn)))); pfree(input.data); return; }
    pfree(input.data);
    task->socket = work_result;
    task->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *task) {
    bool connected = false;
    switch (PQstatus(task->conn)) {
        case CONNECTION_BAD: ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(task->conn)))); return;
        case CONNECTION_OK: elog(DEBUG1, "id = %li, PQstatus == CONNECTION_OK", task->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(task->conn)) {
        case PGRES_POLLING_ACTIVE: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", task->id); break;
        case PGRES_POLLING_FAILED: ereport_my(WARNING, true, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectPoll failed"), errdetail("%s", PQerrorMessageMy(task->conn)))); return;
        case PGRES_POLLING_OK: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_OK", task->id); connected = true; break;
        case PGRES_POLLING_READING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_READING", task->id); task->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: elog(DEBUG1, "id = %li, PQconnectPoll == PGRES_POLLING_WRITING", task->id); task->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if (!(task->pid = PQbackendPID(task->conn))) { ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQbackendPID failed"), errdetail("%s", PQerrorMessageMy(task->conn)))); return; }
        if (!lock_table_pid_hash(work->oid.table, task->pid, task->hash)) { ereport_my(WARNING, true, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("!lock_table_pid_hash(%i, %i, %i)", work->oid.table, task->pid, task->hash))); return; }
        work_query(task);
    }
}

static void work_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    elog(DEBUG1, "code = %i", code);
    dlist_foreach_modify(iter, &work->head) {
        char errbuf[256];
        Task *task = dlist_container(Task, node, iter.cur);
        PGcancel *cancel = PQgetCancel(task->conn);
        if (!cancel) { ereport(WARNING, (errmsg("PQgetCancel failed"), errdetail("%s", PQerrorMessageMy(task->conn)))); continue; }
        if (!PQcancel(cancel, errbuf, sizeof(errbuf))) { ereport(WARNING, (errmsg("PQcancel failed"), errdetail("%s", errbuf))); PQfreeCancel(cancel); continue; }
        elog(WARNING, "cancel id = %li", task->id);
        PQfreeCancel(cancel);
        work_finish(task);
    }
}

#if PG_VERSION_NUM >= 120000
static void work_extension(const char *schema_quote, const char *extension) {
    const char *extension_quote = quote_identifier(extension);
    List *names;
    StringInfoData src;
    elog(DEBUG1, "extension = %s", extension);
    set_ps_display_my("extension");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE EXTENSION %s SCHEMA %s), extension_quote, schema_quote);
    names = stringToQualifiedNameList(extension_quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_extension_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (extension_quote != extension) pfree((void *)extension_quote);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_partman(void) {
    const char *pkey_quote;
    const char *template_quote;
    const RangeVar *rangevar;
    List *names;
    StringInfoData src, pkey, template, template_table;
    set_ps_display_my("partman");
    work->oid.partman = work_schema(work->quote.partman);
    work_extension(work->quote.partman, "pg_partman");
    initStringInfoMy(TopMemoryContext, &pkey);
    appendStringInfo(&pkey, "%s_pkey", work->str.table);
    initStringInfoMy(TopMemoryContext, &template);
    appendStringInfo(&template, "template_%s_%s", work->str.schema, work->str.table);
    pkey_quote = quote_identifier(pkey.data);
    template_quote = quote_identifier(template.data);
    initStringInfoMy(TopMemoryContext, &template_table);
    appendStringInfo(&template_table, "%s.%s", work->quote.partman, template_quote);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE TABLE %1$s (LIKE %2$s INCLUDING ALL, CONSTRAINT %3$s PRIMARY KEY (id))), template_table.data, work->schema_table, pkey_quote);
    names = stringToQualifiedNameList(template_table.data);
    rangevar = makeRangeVarFromNameList(names);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        Datum values[] = {CStringGetTextDatumMy(TopMemoryContext, work->schema_table), CStringGetTextDatumMy(TopMemoryContext, template_table.data)};
        static Oid argtypes[] = {TEXTOID, TEXTOID};
        StringInfoData create_parent;
        initStringInfoMy(TopMemoryContext, &create_parent);
        appendStringInfo(&create_parent, SQL(SELECT %1$s.create_parent(p_parent_table := $1, p_control := 'plan', p_type := 'native', p_interval := 'monthly', p_template_table := $2)), work->quote.partman);
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
        SPI_commit_my();
        SPI_start_transaction_my(create_parent.data);
        SPI_execute_with_args_my(create_parent.data, countof(argtypes), argtypes, values, NULL, SPI_OK_SELECT, false);
        if (SPI_processed != 1) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_processed %lu != 1", SPI_processed)));
        if (!DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "create_parent", false))) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("could not create parent")));
        if (values[0]) pfree((void *)values[0]);
        if (values[1]) pfree((void *)values[1]);
    }
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    if (pkey_quote != pkey.data) pfree((void *)pkey_quote);
    if (template_quote != template.data) pfree((void *)template_quote);
    pfree(pkey.data);
    pfree(src.data);
    pfree(template.data);
    pfree(template_table.data);
    set_ps_display_my("idle");
}
#endif

static void work_remote(Task *task) {
    bool password = false;
    char *err;
    char *options = NULL;
    const char **keywords;
    const char **values;
    int arg = 3;
    PQconninfoOption *opts = PQconninfoParse(task->remote, &err);
    StringInfoData name, value;
    elog(DEBUG1, "id = %li, group = %s, remote = %s, max = %i, oid = %i", task->id, task->group, task->remote ? task->remote : default_null, task->max, work->oid.table);
    dlist_push_head(&work->head, &task->node);
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
    initStringInfoMy(TopMemoryContext, &name);
    appendStringInfo(&name, "pg_task %s %s %s", work->str.schema, work->str.table, task->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(TopMemoryContext, &value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, "%s-c pg_task.data=%s", value.len ? " " : "", work->str.data);
    appendStringInfo(&value, " -c pg_task.user=%s", work->str.user);
    appendStringInfo(&value, " -c pg_task.schema=%s", work->str.schema);
    appendStringInfo(&value, " -c pg_task.table=%s", work->str.table);
    appendStringInfo(&value, " -c pg_task.oid=%i", work->oid.table);
    appendStringInfo(&value, " -c pg_task.group=%s", task->group);
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
    task->event = WL_SOCKET_MASK;
    task->socket = work_connect;
    task->start = GetCurrentTimestamp();
    if (!(task->conn = PQconnectStartParams(keywords, values, false))) ereport_my(WARNING, true, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION), errmsg("PQconnectStartParams failed"), errdetail("%s", PQerrorMessageMy(task->conn))));
    else if (PQstatus(task->conn) == CONNECTION_BAD) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("PQstatus == CONNECTION_BAD"), errdetail("%s", PQerrorMessageMy(task->conn))));
    else if (!PQisnonblocking(task->conn) && PQsetnonblocking(task->conn, true) == -1) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetnonblocking failed"), errdetail("%s", PQerrorMessageMy(task->conn))));
    else if (!superuser() && !PQconnectionUsedPassword(task->conn)) ereport_my(WARNING, true, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"), errdetail("Non-superuser cannot connect if the server does not request a password."), errhint("Target server's authentication method must be changed.")));
    else if (PQclientEncoding(task->conn) != GetDatabaseEncoding() && !PQsetClientEncoding(task->conn, GetDatabaseEncodingName())) ereport_my(WARNING, true, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("PQsetClientEncoding failed"), errdetail("%s", PQerrorMessageMy(task->conn))));
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
    if (task->group) pfree(task->group);
    task->group = NULL;
}

static void work_table(void) {
    const RangeVar *rangevar;
    List *names;
    StringInfoData src, hash;
    elog(DEBUG1, "schema_table = %s, schema_type = %s", work->schema_table, work->schema_type);
    set_ps_display_my("table");
    set_config_option_my("pg_task.table", work->str.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(TopMemoryContext, &hash);
#if PG_VERSION_NUM >= 120000
    appendStringInfo(&hash, SQL(GENERATED ALWAYS AS (hashtext("group"||COALESCE(remote, '%1$s'))) STORED), "");
#else
    if (true) {
        const char *function_quote;
        StringInfoData function;
        initStringInfoMy(TopMemoryContext, &function);
        appendStringInfo(&function, "%1$s_hash_generate", work->str.table);
        function_quote = quote_identifier(function.data);
        appendStringInfo(&hash, SQL(;CREATE OR REPLACE FUNCTION %1$s.%2$s() RETURNS TRIGGER AS $$BEGIN
            IF tg_op = 'INSERT' OR (new.group, new.remote) IS DISTINCT FROM (old.group, old.remote) THEN
                new.hash = hashtext(new.group||COALESCE(new.remote, '%3$s'));
            END IF;
            return new;
        end;$$ LANGUAGE plpgsql;
        CREATE TRIGGER hash_generate BEFORE INSERT OR UPDATE ON %4$s FOR EACH ROW EXECUTE PROCEDURE %1$s.%2$s()), work->quote.schema, function_quote, "", work->schema_table);
        if (function_quote != function.data) pfree((void *)function_quote);
        pfree(function.data);
    }
#endif
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(
        CREATE TABLE %1$s (
            id bigserial NOT NULL%4$s,
            parent bigint DEFAULT NULLIF(current_setting('pg_task.id')::bigint, 0),
            plan timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
            start timestamp with time zone,
            stop timestamp with time zone,
            active interval NOT NULL DEFAULT current_setting('pg_task.default_active')::interval CHECK (active > '0 sec'::interval),
            live interval NOT NULL DEFAULT current_setting('pg_task.default_live')::interval CHECK (live >= '0 sec'::interval),
            repeat interval NOT NULL DEFAULT current_setting('pg_task.default_repeat')::interval CHECK (repeat >= '0 sec'::interval),
            timeout interval NOT NULL DEFAULT current_setting('pg_task.default_timeout')::interval CHECK (timeout >= '0 sec'::interval),
            count integer NOT NULL DEFAULT current_setting('pg_task.default_count')::integer CHECK (count >= 0),
            hash integer NOT NULL %3$s,
            max integer NOT NULL DEFAULT current_setting('pg_task.default_max')::integer,
            pid integer,
            state %2$s NOT NULL DEFAULT 'PLAN'::%2$s,
            delete boolean NOT NULL DEFAULT current_setting('pg_task.default_delete')::boolean,
            drift boolean NOT NULL DEFAULT current_setting('pg_task.default_drift')::boolean,
            header boolean NOT NULL DEFAULT current_setting('pg_task.default_header')::boolean,
            string boolean NOT NULL DEFAULT current_setting('pg_task.default_string')::boolean,
            delimiter "char" NOT NULL DEFAULT current_setting('pg_task.default_delimiter')::"char",
            escape "char",
            quote "char",
            error text,
            "group" text NOT NULL DEFAULT current_setting('pg_task.default_group'),
            input text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.default_null'),
            output text,
            remote text
        )
    ), work->schema_table, work->schema_type,
#if PG_VERSION_NUM >= 120000
        hash.data,
#else
        "",
#endif
        work->str.partman ? "" : " PRIMARY KEY");
    if (work->str.partman) appendStringInfoString(&src, " PARTITION BY RANGE (plan)");
#if PG_VERSION_NUM < 120000
    appendStringInfoString(&src, hash.data);
#endif
    names = stringToQualifiedNameList(work->schema_table);
    rangevar = makeRangeVarFromNameList(names);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    work->oid.table = RangeVarGetRelid(rangevar, NoLock, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    set_config_option_my("pg_task.table", work->str.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    resetStringInfo(&src);
    appendStringInfo(&src, "%i", work->oid.table);
    set_config_option_my("pg_task.oid", src.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(hash.data);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_task(Task *task) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    dsm_segment *seg;
    pid_t pid;
    shm_toc_estimator e;
    shm_toc *toc;
    Size segsize;
    size_t len = 0;
    elog(DEBUG1, "id = %li, group = %s, max = %i, oid = %i", task->id, task->group, task->max, work->oid.table);
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", work->str.user, work->str.data, work->str.schema, work->str.table, task->group)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
#if PG_VERSION_NUM < 100000
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_task");
#endif
    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, sizeof(task->hash));
    shm_toc_estimate_chunk(&e, sizeof(task->id));
    shm_toc_estimate_chunk(&e, sizeof(task->max));
    shm_toc_estimate_chunk(&e, sizeof(work->oid.data));
    shm_toc_estimate_chunk(&e, sizeof(work->oid.schema));
    shm_toc_estimate_chunk(&e, sizeof(work->oid.table));
    shm_toc_estimate_chunk(&e, sizeof(work->oid.user));
    shm_toc_estimate_chunk(&e, strlen(task->group) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->str.data) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->str.schema) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->str.table) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->str.user) + 1);
    shm_toc_estimate_keys(&e, PG_TASK_NKEYS);
    segsize = shm_toc_estimate(&e);
    seg = dsm_create_my(segsize, 0);
    toc = shm_toc_create(PG_TASK_MAGIC, dsm_segment_address(seg), segsize);
    { typeof(task->group) group = shm_toc_allocate(toc, strlen(task->group) + 1); strcpy(group, task->group); shm_toc_insert(toc, PG_TASK_KEY_GROUP, group); }
    { typeof(task->hash) *hash = shm_toc_allocate(toc, sizeof(task->hash)); *hash = task->hash; shm_toc_insert(toc, PG_TASK_KEY_HASH, hash); }
    { typeof(task->id) *id = shm_toc_allocate(toc, sizeof(task->id)); *id = task->id; shm_toc_insert(toc, PG_TASK_KEY_ID, id); }
    { typeof(task->max) *max = shm_toc_allocate(toc, sizeof(task->max)); *max = task->max; shm_toc_insert(toc, PG_TASK_KEY_MAX, max); }
    { typeof(work->oid.data) *oid_data = shm_toc_allocate(toc, sizeof(work->oid.data)); *oid_data = work->oid.data; shm_toc_insert(toc, PG_TASK_KEY_OID_DATA, oid_data); }
    { typeof(work->oid.schema) *oid_schema = shm_toc_allocate(toc, sizeof(work->oid.schema)); *oid_schema = work->oid.schema; shm_toc_insert(toc, PG_TASK_KEY_OID_SCHEMA, oid_schema); }
    { typeof(work->oid.table) *oid_table = shm_toc_allocate(toc, sizeof(work->oid.table)); *oid_table = work->oid.table; shm_toc_insert(toc, PG_TASK_KEY_OID_TABLE, oid_table); }
    { typeof(work->oid.user) *oid_user = shm_toc_allocate(toc, sizeof(work->oid.user)); *oid_user = work->oid.user; shm_toc_insert(toc, PG_TASK_KEY_OID_USER, oid_user); }
    { typeof(work->str.data) str_data = shm_toc_allocate(toc, strlen(work->str.data) + 1); strcpy(str_data, work->str.data); shm_toc_insert(toc, PG_TASK_KEY_STR_DATA, str_data); }
    { typeof(work->str.schema) str_schema = shm_toc_allocate(toc, strlen(work->str.schema) + 1); strcpy(str_schema, work->str.schema); shm_toc_insert(toc, PG_TASK_KEY_STR_SCHEMA, str_schema); }
    { typeof(work->str.table) str_table = shm_toc_allocate(toc, strlen(work->str.table) + 1); strcpy(str_table, work->str.table); shm_toc_insert(toc, PG_TASK_KEY_STR_TABLE, str_table); }
    { typeof(work->str.user) str_user = shm_toc_allocate(toc, strlen(work->str.user) + 1); strcpy(str_user, work->str.user); shm_toc_insert(toc, PG_TASK_KEY_STR_USER, str_user); }
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
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
    dsm_pin_segment(seg);
    dsm_detach(seg);
    work_free(task);
}

static void work_type(void) {
    int32 typmod;
    Oid type = InvalidOid;
    StringInfoData src;
    set_ps_display_my("type");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'STOP')), work->schema_type);
    SPI_connect_my(src.data);
    parseTypeString(work->schema_type, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_conf(void) {
    const char *index_input[] = {"input"};
    const char *index_parent[] = {"parent"};
    const char *index_plan[] = {"plan"};
    const char *index_state[] = {"state"};
    StringInfoData schema_table, schema_type, timeout;
    initStringInfoMy(TopMemoryContext, &schema_table);
    appendStringInfo(&schema_table, "%s.%s", work->quote.schema, work->quote.table);
    work->schema_table = schema_table.data;
    initStringInfoMy(TopMemoryContext, &schema_type);
    appendStringInfo(&schema_type, "%s.state", work->quote.schema);
    work->schema_type = schema_type.data;
    elog(DEBUG1, "timeout = %li, reset = %li, schema_table = %s, schema_type = %s, partman = %s", work->timeout, work->reset, work->schema_table, work->schema_type, work->str.partman ? work->str.partman : default_null);
    set_ps_display_my("conf");
    work->oid.schema = work_schema(work->quote.schema);
    set_config_option_my("pg_task.schema", work->str.schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    work_type();
    work_table();
    work_index(countof(index_input), index_input);
    work_index(countof(index_parent), index_parent);
    work_index(countof(index_plan), index_plan);
    work_index(countof(index_state), index_state);
#if PG_VERSION_NUM >= 120000
    if (work->str.partman) work_partman();
#endif
    set_config_option_my("pg_task.data", work->str.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.user", work->str.user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(TopMemoryContext, &timeout);
    appendStringInfo(&timeout, "%li", work->timeout);
    set_config_option_my("pg_task.timeout", timeout.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(timeout.data);
    dlist_init(&work->head);
    set_ps_display_my("idle");
}

static void work_init(Datum main_arg) {
    dsm_segment *seg;
    shm_toc *toc;
    work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
    on_proc_exit(work_exit, (Datum)work);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM < 100000
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_task");
#endif
    if (!(seg = dsm_attach(DatumGetUInt32(main_arg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment")));
    if (!(toc = shm_toc_attach(PG_WORK_MAGIC, dsm_segment_address(seg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("bad magic number in dynamic shared memory segment")));
    work->oid.data = *(typeof(work->oid.data) *)shm_toc_lookup_my(toc, PG_WORK_KEY_OID_DATA, false);
    work->oid.user = *(typeof(work->oid.user) *)shm_toc_lookup_my(toc, PG_WORK_KEY_OID_USER, false);
    work->reset = *(typeof(work->reset) *)shm_toc_lookup_my(toc, PG_WORK_KEY_RESET, false);
    work->str.data = MemoryContextStrdup(TopMemoryContext, shm_toc_lookup_my(toc, PG_WORK_KEY_STR_DATA, false));
    work->str.partman = MemoryContextStrdup(TopMemoryContext, shm_toc_lookup_my(toc, PG_WORK_KEY_STR_PARTMAN, false));
    work->str.schema = MemoryContextStrdup(TopMemoryContext, shm_toc_lookup_my(toc, PG_WORK_KEY_STR_SCHEMA, false));
    work->str.table = MemoryContextStrdup(TopMemoryContext, shm_toc_lookup_my(toc, PG_WORK_KEY_STR_TABLE, false));
    work->str.user = MemoryContextStrdup(TopMemoryContext, shm_toc_lookup_my(toc, PG_WORK_KEY_STR_USER, false));
    work->timeout = *(typeof(work->timeout) *)shm_toc_lookup_my(toc, PG_WORK_KEY_TIMEOUT, false);
    dsm_detach(seg);
    if (!strlen(work->str.partman)) work->str.partman = NULL;
    BackgroundWorkerInitializeConnectionMy(work->str.data, work->str.user, 0);
    set_ps_display_my("init");
    process_session_preload_libraries();
    work->quote.data = (char *)quote_identifier(work->str.data);
    if (work->str.partman) work->quote.partman = (char *)quote_identifier(work->str.partman);
    work->quote.schema = (char *)quote_identifier(work->str.schema);
    work->quote.table = (char *)quote_identifier(work->str.table);
    work->quote.user = (char *)quote_identifier(work->str.user);
    pgstat_report_appname(MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1);
    set_config_option_my("application_name", MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    elog(DEBUG1, "timeout = %li, reset = %li, partman = %s", work->timeout, work->reset, work->str.partman ? work->str.partman : default_null);
    work_conf();
    work_reset();
}

static void work_timeout(void) {
    Datum values[] = {ObjectIdGetDatum(work->oid.table)};
    static Oid argtypes[] = {OIDOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("timeout");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH l AS (
                SELECT count(classid) AS pid, objid AS hash FROM pg_locks WHERE locktype = 'userlock' AND mode = 'AccessShareLock' AND granted AND objsubid = 5 AND database = $1 GROUP BY objid
            ), s AS (
                SELECT t.id, t.hash, CASE WHEN t.max >= 0 THEN t.max ELSE 0 END - COALESCE(l.pid, 0) AS count FROM %1$s AS t LEFT JOIN l ON l.hash = t.hash
                WHERE t.plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.state = 'PLAN'::%2$s AND CASE WHEN t.max >= 0 THEN t.max ELSE 0 END - COALESCE(l.pid, 0) >= 0 FOR UPDATE OF t %3$s
            ), u AS (
                SELECT id, hash, count - row_number() OVER (PARTITION BY hash ORDER BY count DESC, id) + 1 AS count FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS t SET state = 'TAKE'::%2$s FROM u
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = u.id AND u.count >= 0 RETURNING t.id, t.hash, t.group, t.remote, t.max, t.delimiter
        ), work->schema_table, work->schema_type,
#if PG_VERSION_NUM >= 90500
        "SKIP LOCKED"
#else
        ""
#endif
        );
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Task *task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task));
        task->delimiter = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delimiter", false));
        task->group = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "group", false));
        task->hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "hash", false));
        task->id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false));
        task->max = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "max", false));
        task->remote = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "remote", true));
        elog(DEBUG1, "row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, task->id, task->hash, task->group, task->remote ? task->remote : default_null, task->max);
        task->remote ? work_remote(task) : work_task(task);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_writeable(Task *task) {
    task->socket(task);
}

void work_main(Datum main_arg) {
    instr_time current_reset_time;
    instr_time current_timeout_time;
    instr_time start_time;
    long current_reset = -1;
    long current_timeout = -1;
    work_init(main_arg);
    if (!lock_data_user_table(MyDatabaseId, GetUserId(), work->oid.table)) { elog(WARNING, "!lock_data_user_table(%i, %i, %i)", MyDatabaseId, GetUserId(), work->oid.table); return; }
    while (!ShutdownRequestPending) {
        int nevents = 2 + work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        work_event(set);
        if (current_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            current_timeout = work->timeout;
        }
        if (current_reset <= 0) current_reset = work->reset;
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
        current_timeout = work->timeout - (long)INSTR_TIME_GET_MILLISEC(current_timeout_time);
        if (work->reset >= 0) {
            INSTR_TIME_SET_CURRENT(current_reset_time);
            INSTR_TIME_SUBTRACT(current_reset_time, start_time);
            current_reset = work->reset - (long)INSTR_TIME_GET_MILLISEC(current_reset_time);
            if (current_reset <= 0) work_reset();
        }
        if (current_timeout <= 0) work_timeout();
        FreeWaitEventSet(set);
        pfree(events);
    }
    if (!unlock_data_user_table(MyDatabaseId, GetUserId(), work->oid.table)) elog(WARNING, "!unlock_data_user_table(%i, %i, %i)", MyDatabaseId, GetUserId(), work->oid.table);
}
