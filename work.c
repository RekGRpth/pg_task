#include "include.h"

extern char *default_null;
static void work_query(Task *task);
Work *work;

static char *PQerrorMessageMy(const PGconn *conn) {
    char *err = PQerrorMessage(conn);
    int len;
    if (!err) return err;
    len = strlen(err);
    if (!len) return err;
    if (err[len - 1] == '\n') err[len - 1] = '\0';
    return err;
}

static char *PQresultErrorMessageMy(const PGresult *res) {
    char *err = PQresultErrorMessage(res);
    int len;
    if (!err) return err;
    len = strlen(err);
    if (!len) return err;
    if (err[len - 1] == '\n') err[len - 1] = '\0';
    return err;
}

static void work_check(void) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return;
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, init_check(), "");
        appendStringInfo(&src, SQL(%1$sWHERE "user" = current_user AND data = current_catalog AND schema = current_setting('pg_task.schema', false) AND "table" = current_setting('pg_task.table', false) AND timeout = current_setting('pg_task.timeout', false)::integer), " ");
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
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &work->head) {
        Task *task = dlist_container(Task, node, iter.cur);
        AddWaitEventToSet(set, task->event, PQsocket(task->conn), NULL, task);
    }
}

static void work_fatal(Task *task, PGresult *result) {
    char *value = NULL;
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY))) appendStringInfo(&task->error, "%sseverity%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY_NONLOCALIZED))) appendStringInfo(&task->error, "%sseverity_nonlocalized%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SQLSTATE))) appendStringInfo(&task->error, "%ssqlstate%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY))) appendStringInfo(&task->error, "%smessage_primary%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL))) appendStringInfo(&task->error, "%smessage_detail%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT))) appendStringInfo(&task->error, "%smessage_hint%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_STATEMENT_POSITION))) appendStringInfo(&task->error, "%sstatement_position%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_POSITION))) appendStringInfo(&task->error, "%sinternal_position%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_QUERY))) appendStringInfo(&task->error, "%sinternal_query%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_CONTEXT))) appendStringInfo(&task->error, "%scontext%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SCHEMA_NAME))) appendStringInfo(&task->error, "%sschema_name%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_TABLE_NAME))) appendStringInfo(&task->error, "%stable_name%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_COLUMN_NAME))) appendStringInfo(&task->error, "%scolumn_name%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_DATATYPE_NAME))) appendStringInfo(&task->error, "%sdatatype_name%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_CONSTRAINT_NAME))) appendStringInfo(&task->error, "%sconstraint_name%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FILE))) appendStringInfo(&task->error, "%ssource_file%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_LINE))) appendStringInfo(&task->error, "%ssource_line%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION))) appendStringInfo(&task->error, "%ssource_function%c%s", task->error.len ? "\n" : "", task->delimiter, value);
    if (value) appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
    task->skip++;
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

static void work_error(Task *task, const char *msg, const char *err, bool finish) {
    if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    appendStringInfo(&task->error, "%s%s", task->error.len ? "\n" : "", msg);
    if (err && strlen(err)) appendStringInfo(&task->error, " and %s", err);
    elog(WARNING, "id = %li, error = %s", task->id, task->error.data);
    appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
    task->skip++;
    task_done(task) || finish ? work_finish(task) : work_free(task);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 0;
    dlist_foreach_modify(iter, &work->head) {
        Task *task = dlist_container(Task, node, iter.cur);
        if (PQstatus(task->conn) == CONNECTION_BAD) { work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessageMy(task->conn), true); continue; }
        if (PQsocket(task->conn) == PGINVALID_SOCKET) { work_error(task, "PQsocket == PGINVALID_SOCKET", PQerrorMessageMy(task->conn), true); continue; }
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
    D1("index = %s, schema_table = %s", idx.data, work->schema_table);
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
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND state IN ('TAKE'::%2$s, 'WORK'::%2$s) AND l.pid IS NULL
            FOR UPDATE OF t SKIP LOCKED
        ) UPDATE %1$s AS t SET state = 'PLAN'::%2$s, start = NULL, stop = NULL, pid = NULL FROM s
        WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
    ), work->schema_table, work->schema_type);
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
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) work_reload();
}

static bool work_busy(Task *task, int event) {
    if (PQisBusy(task->conn)) { elog(WARNING, "id = %li, PQisBusy", task->id); task->event = event; return false; }
    return true;
}

static bool work_consume(Task *task) {
    if (!PQconsumeInput(task->conn)) { work_error(task, "!PQconsumeInput", PQerrorMessageMy(task->conn), true); return false; }
    return true;
}

static bool work_flush(Task *task) {
    switch (PQflush(task->conn)) {
        case 0: break;
        case 1: D1("id = %li, PQflush == 1", task->id); task->event = WL_SOCKET_MASK; return false;
        case -1: work_error(task, "PQflush == -1", PQerrorMessageMy(task->conn), true); return false;
    }
    return true;
}

static bool work_consume_flush_busy(Task *task) {
    if (!work_consume(task)) return false;
    if (!work_flush(task)) return false;
    if (!work_busy(task, WL_SOCKET_READABLE)) return false;
    return true;
}

static void work_readable(Task *task) {
    if (PQstatus(task->conn) == CONNECTION_OK) if (!work_consume_flush_busy(task)) return;
    task->socket(task);
}

static void work_done(Task *task) {
    if (PQstatus(task->conn) == CONNECTION_OK && PQtransactionStatus(task->conn) != PQTRANS_IDLE) {
        task->socket = work_done;
        if (!work_busy(task, WL_SOCKET_WRITEABLE)) return;
        if (!PQsendQuery(task->conn, SQL(COMMIT))) { work_error(task, "!PQsendQuery", PQerrorMessageMy(task->conn), false); return; }
        if (!work_flush(task)) return;
        task->event = WL_SOCKET_READABLE;
        return;
    }
    task_done(task) || PQstatus(task->conn) != CONNECTION_OK ? work_finish(task) : work_query(task);
}

static Oid work_schema(const char *schema_quote) {
    List *names;
    Oid oid;
    StringInfoData src;
    D1("schema = %s", schema_quote);
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
            case PGRES_FATAL_ERROR: elog(WARNING, "id = %li, PQresultStatus == PGRES_FATAL_ERROR and %s", task->id, PQresultErrorMessageMy(result)); work_fatal(task, result); break;
            case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) work_success(task, result, row); break;
            default: D1("id = %li, %s", task->id, PQresStatus(PQresultStatus(result))); break;
        }
        PQclear(result);
        if (!work_consume_flush_busy(task)) return;
    }
    work_done(task);
}

static bool work_input(Task *task) {
    StringInfoData input;
    if (ShutdownRequestPending) return true;
    if (task_work(task)) return true;
    D1("id = %li, timeout = %i, input = %s, count = %i", task->id, task->timeout, task->input, task->count);
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
    pfree(task->input);
    task->input = input.data;
    return false;
}

static void work_query(Task *task) {
    if (ShutdownRequestPending) return;
    task->socket = work_query;
    if (!work_busy(task, WL_SOCKET_WRITEABLE)) return;
    if (work_input(task)) { work_finish(task); return; }
    if (!task->active) {
        if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
        if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
        appendStringInfo(&task->error, "%sseverity%cERROR", task->error.len ? "\n" : "", task->delimiter);
        appendStringInfo(&task->error, "%sseverity_nonlocalized%cERROR", task->error.len ? "\n" : "", task->delimiter);
        appendStringInfo(&task->error, "%ssqlstate%c%s", task->error.len ? "\n" : "", task->delimiter, unpack_sql_state(ERRCODE_INTERNAL_ERROR));
        appendStringInfo(&task->error, "%smessage_primary%ctask %li not active", task->error.len ? "\n" : "", task->delimiter, task->id);
        appendStringInfo(&task->error, "%ssource_file%c%s", task->error.len ? "\n" : "", task->delimiter, __FILE__);
        appendStringInfo(&task->error, "%ssource_line%c%i", task->error.len ? "\n" : "", task->delimiter, __LINE__);
        appendStringInfo(&task->error, "%ssource_function%c%s", task->error.len ? "\n" : "", task->delimiter, __func__);
        appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
        task->skip++;
        work_done(task);
        return;
    }
    D1("input = %s", task->input);
    if (!PQsendQuery(task->conn, task->input)) { work_error(task, "!PQsendQuery", PQerrorMessageMy(task->conn), false); return; }
    task->socket = work_result;
    if (!work_flush(task)) return;
    task->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *task) {
    bool connected = false;
    switch (PQstatus(task->conn)) {
        case CONNECTION_BAD: D1("id = %li, PQstatus == CONNECTION_BAD", task->id); work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessageMy(task->conn), true); return;
        case CONNECTION_OK: D1("id = %li, PQstatus == CONNECTION_OK", task->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(task->conn)) {
        case PGRES_POLLING_ACTIVE: D1("id = %li, PQconnectPoll == PGRES_POLLING_ACTIVE", task->id); break;
        case PGRES_POLLING_FAILED: D1("id = %li, PQconnectPoll == PGRES_POLLING_FAILED", task->id); work_error(task, "PQconnectPoll == PGRES_POLLING_FAILED", PQerrorMessageMy(task->conn), true); return;
        case PGRES_POLLING_OK: D1("id = %li, PQconnectPoll == PGRES_POLLING_OK", task->id); connected = true; break;
        case PGRES_POLLING_READING: D1("id = %li, PQconnectPoll == PGRES_POLLING_READING", task->id); task->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("id = %li, PQconnectPoll == PGRES_POLLING_WRITING", task->id); task->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if(!(task->pid = PQbackendPID(task->conn))) { work_error(task, "!PQbackendPID", PQerrorMessageMy(task->conn), true); return; }
        if (!lock_table_pid_hash(work->oid.table, task->pid, task->hash)) { elog(WARNING, "!lock_table_pid_hash(%i, %i, %i)", work->oid.table, task->pid, task->hash); work_error(task, "!lock_table_pid_hash", NULL, true); return; }
        work_query(task);
    }
}

static void work_exit(int code, Datum arg) {
    dlist_mutable_iter iter;
    D1("code = %i", code);
    dlist_foreach_modify(iter, &work->head) {
        char errbuf[256];
        Task *task = dlist_container(Task, node, iter.cur);
        PGcancel *cancel = PQgetCancel(task->conn);
        if (!cancel) { elog(WARNING, "!PQgetCancel and %s", PQerrorMessageMy(task->conn)); continue; }
        if (!PQcancel(cancel, errbuf, sizeof(errbuf))) { elog(WARNING, "!PQcancel and %s", errbuf); PQfreeCancel(cancel); continue; }
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
    D1("extension = %s", extension);
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
        if (SPI_processed != 1) E("%lu != 1", SPI_processed);
        if (!DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "create_parent", false))) E("!create_parent");
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
    D1("id = %li, group = %s, remote = %s, max = %i, oid = %i", task->id, task->group, task->remote ? task->remote : default_null, task->max, work->oid.table);
    if (!opts) { work_error(task, "!PQconninfoParse", err, false); if (err) PQfreemem(err); return; }
    for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
        if (!opt->val) continue;
        D1("%s = %s", opt->keyword, opt->val);
        if (!strcmp(opt->keyword, "password")) password = true;
        if (!strcmp(opt->keyword, "fallback_application_name")) continue;
        if (!strcmp(opt->keyword, "application_name")) continue;
        if (!strcmp(opt->keyword, "options")) { options = opt->val; continue; }
        arg++;
    }
    if (!superuser() && !password) { work_error(task, "!superuser && !password", NULL, false); PQconninfoFree(opts); return; }
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
    dlist_push_head(&work->head, &task->node);
    if (!(task->conn = PQconnectStartParams(keywords, values, false))) work_error(task, "!PQconnectStartParams", PQerrorMessageMy(task->conn), true);
    else if (PQstatus(task->conn) == CONNECTION_BAD) work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessageMy(task->conn), true);
    else if (!PQisnonblocking(task->conn) && PQsetnonblocking(task->conn, true) == -1) work_error(task, "PQsetnonblocking == -1", PQerrorMessageMy(task->conn), true);
    else if (!superuser() && !PQconnectionUsedPassword(task->conn)) work_error(task, "!superuser && !PQconnectionUsedPassword", PQerrorMessageMy(task->conn), true);
    else if (PQclientEncoding(task->conn) != GetDatabaseEncoding()) PQsetClientEncoding(task->conn, GetDatabaseEncodingName());
    pfree(name.data);
    pfree(value.data);
    pfree(keywords);
    pfree(values);
    PQconninfoFree(opts);
    pfree(task->group);
    task->group = NULL;
}

static void work_table(void) {
    const RangeVar *rangevar;
    List *names;
    StringInfoData src, hash;
    D1("schema_table = %s, schema_type = %s", work->schema_table, work->schema_type);
    set_ps_display_my("table");
    set_config_option("pg_task.table", work->str.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
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
            parent bigint DEFAULT current_setting('pg_task.id', true)::bigint,
            plan timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
            start timestamp with time zone,
            stop timestamp with time zone,
            active interval NOT NULL DEFAULT current_setting('pg_task.default_active', false)::interval CHECK (active > '0 sec'::interval),
            live interval NOT NULL DEFAULT current_setting('pg_task.default_live', false)::interval CHECK (live >= '0 sec'::interval),
            repeat interval NOT NULL DEFAULT current_setting('pg_task.default_repeat', false)::interval CHECK (repeat >= '0 sec'::interval),
            timeout interval NOT NULL DEFAULT current_setting('pg_task.default_timeout', false)::interval CHECK (timeout >= '0 sec'::interval),
            count integer NOT NULL DEFAULT current_setting('pg_task.default_count', false)::integer CHECK (count >= 0),
            hash integer NOT NULL %3$s,
            max integer NOT NULL DEFAULT current_setting('pg_task.default_max', false)::integer CHECK (max > 0),
            pid integer,
            state %2$s NOT NULL DEFAULT 'PLAN'::%2$s,
            delete boolean NOT NULL DEFAULT current_setting('pg_task.default_delete', false)::boolean,
            drift boolean NOT NULL DEFAULT current_setting('pg_task.default_drift', false)::boolean,
            header boolean NOT NULL DEFAULT current_setting('pg_task.default_header', false)::boolean,
            string boolean NOT NULL DEFAULT current_setting('pg_task.default_string', false)::boolean,
            delimiter "char" NOT NULL DEFAULT current_setting('pg_task.default_delimiter', false)::"char",
            escape "char",
            quote "char",
            error text,
            "group" text NOT NULL DEFAULT current_setting('pg_task.default_group', false),
            input text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.default_null', false),
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
#if PG_VERSION_NUM >= 120000
#else
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
    set_config_option("pg_task.table", work->str.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    resetStringInfo(&src);
    appendStringInfo(&src, "%i", work->oid.table);
    set_config_option("pg_task.oid", src.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(hash.data);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void work_task(Task *task) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len = 0;
    D1("id = %li, group = %s, max = %i, oid = %i", task->id, task->group, task->max, work->oid.table);
    if ((len = strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_task %s %s %s", work->str.user, work->str.data, work->str.schema, work->str.table, task->group)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    len = 0;
#define X(name, serialize, deserialize) serialize(name);
    TASK
#undef X
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = Int64GetDatum(task->id);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: elog(ERROR, "BGWH_NOT_YET_STARTED is never returned!"); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    pfree(handle);
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
    D1("timeout = %i, count = %i, live = %li, schema_table = %s, schema_type = %s, partman = %s", work->timeout, work->count, work->live, work->schema_table, work->schema_type, work->str.partman ? work->str.partman : default_null);
    set_ps_display_my("conf");
    work->oid.schema = work_schema(work->quote.schema);
    set_config_option("pg_task.schema", work->str.schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    work_type();
    work_table();
    work_index(countof(index_input), index_input);
    work_index(countof(index_parent), index_parent);
    work_index(countof(index_plan), index_plan);
    work_index(countof(index_state), index_state);
#if PG_VERSION_NUM >= 120000
    if (work->str.partman) work_partman();
#endif
    set_config_option("pg_task.data", work->str.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.user", work->str.user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(TopMemoryContext, &timeout);
    appendStringInfo(&timeout, "%i", work->timeout);
    set_config_option("pg_task.timeout", timeout.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(timeout.data);
    dlist_init(&work->head);
    set_ps_display_my("idle");
}

static void work_init(void) {
    char *p = MyBgworkerEntry->bgw_extra;
    MemoryContext oldcontext = CurrentMemoryContext;
    work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
    on_proc_exit(work_exit, (Datum)work);
#define X(name, serialize, deserialize) deserialize(name);
    WORK
#undef X
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
#if PG_VERSION_NUM >= 110000
    BackgroundWorkerInitializeConnectionByOid(work->oid.data, work->oid.user, 0);
#else
    BackgroundWorkerInitializeConnectionByOid(work->oid.data, work->oid.user);
#endif
    set_ps_display_my("init");
    process_session_preload_libraries();
    StartTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
    if (!(work->str.data = get_database_name(work->oid.data))) E("!get_database_name");
    if (!(work->str.user = GetUserNameFromId(work->oid.user, true))) E("!GetUserNameFromId");
    CommitTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
    work->quote.data = (char *)quote_identifier(work->str.data);
    if (work->str.partman) work->quote.partman = (char *)quote_identifier(work->str.partman);
    work->quote.schema = (char *)quote_identifier(work->str.schema);
    work->quote.table = (char *)quote_identifier(work->str.table);
    work->quote.user = (char *)quote_identifier(work->str.user);
    pgstat_report_appname(MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1);
    set_config_option("application_name", MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    D1("timeout = %i, count = %i, live = %li, partman = %s", work->timeout, work->count, work->live, work->str.partman ? work->str.partman : default_null);
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
            WITH s AS ( WITH l AS (
                SELECT count(classid) AS classid, objid FROM pg_locks WHERE locktype = 'userlock' AND mode = 'AccessShareLock' AND granted AND objsubid = 5 AND database = $1 GROUP BY objid
            ), s AS (
                SELECT t.id, t.hash, t.max - COALESCE(classid, 0) AS count FROM %1$s AS t LEFT JOIN l ON objid = t.hash
                WHERE t.plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.state = 'PLAN'::%2$s AND t.max > COALESCE(classid, 0) FOR UPDATE OF t SKIP LOCKED
            ) SELECT id, hash, count - row_number() OVER (PARTITION BY hash ORDER BY count DESC, id) + 1 AS count FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS t SET state = 'TAKE'::%2$s FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.id = s.id AND s.count > 0 RETURNING t.id, t.hash, t.group, t.remote, t.max
        ), work->schema_table, work->schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Task *task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task));
        task->group = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "group", false));
        task->hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "hash", false));
        task->id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false));
        task->max = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "max", false));
        task->remote = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "remote", true));
        D1("row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, task->id, task->hash, task->group, task->remote ? task->remote : default_null, task->max);
        task->remote ? work_remote(task) : work_task(task);
    }
    if (work->count) work->processed += SPI_processed;
    SPI_finish_my();
    set_ps_display_my("idle");
}

static void work_writeable(Task *task) {
    if (PQstatus(task->conn) == CONNECTION_OK) if (!work_flush(task)) return;
    task->socket(task);
}

void work_main(Datum main_arg) {
    instr_time cur_time;
    instr_time start_time;
    long cur_timeout = -1;
    work_init();
#if PG_VERSION_NUM >= 120000
#else
    MyStartTimestamp = GetCurrentTimestamp();
#endif
    if (!lock_data_user_table(MyDatabaseId, GetUserId(), work->oid.table)) { elog(WARNING, "!lock_data_user_table(%i, %i, %i)", MyDatabaseId, GetUserId(), work->oid.table); return; }
    while (!ShutdownRequestPending) {
        int nevents = 2 + work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        work_event(set);
        if (cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = work->timeout;
        }
#if PG_VERSION_NUM >= 100000
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents, PG_WAIT_EXTENSION);
#else
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents);
#endif
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) work_latch();
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        if (work->timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = work->timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout <= 0) work_timeout();
        }
        FreeWaitEventSet(set);
        pfree(events);
        if (work->count && work->processed >= work->count) break;
        if (work->live && TimestampDifferenceExceeds(MyStartTimestamp, GetCurrentTimestamp(), work->live * 1000)) break;
    }
    if (!unlock_data_user_table(MyDatabaseId, GetUserId(), work->oid.table)) elog(WARNING, "!unlock_data_user_table(%i, %i, %i)", MyDatabaseId, GetUserId(), work->oid.table);
    MyBgworkerEntry->bgw_notify_pid = MyProcPid;
    if (!ShutdownRequestPending) conf_work(MyBgworkerEntry);
}
