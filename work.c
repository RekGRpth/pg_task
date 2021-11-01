#include "include.h"

extern char *default_null;
static void work_query(Task *task);
Work work;

static bool work_is_log_level_output(int elevel, int log_min_level) {
    if (elevel == LOG || elevel == LOG_SERVER_ONLY) {
        if (log_min_level == LOG || log_min_level <= ERROR) return true;
    } else if (log_min_level == LOG) { /* elevel != LOG */
        if (elevel >= FATAL) return true;
    } /* Neither is LOG */ else if (elevel >= log_min_level) return true;
    return false;
}

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

static const char *work_status(Task *task) {
    switch (PQstatus(task->conn)) {
        case CONNECTION_AUTH_OK: return "CONNECTION_AUTH_OK";
        case CONNECTION_AWAITING_RESPONSE: return "CONNECTION_AWAITING_RESPONSE";
        case CONNECTION_BAD: return "CONNECTION_BAD";
#if (PG_VERSION_NUM >= 140000)
        case CONNECTION_CHECK_STANDBY: return "CONNECTION_CHECK_STANDBY";
#endif
#if (PG_VERSION_NUM >= 130000)
        case CONNECTION_CHECK_TARGET: return "CONNECTION_CHECK_TARGET";
#endif
        case CONNECTION_CHECK_WRITABLE: return "CONNECTION_CHECK_WRITABLE";
        case CONNECTION_CONSUME: return "CONNECTION_CONSUME";
        case CONNECTION_GSS_STARTUP: return "CONNECTION_GSS_STARTUP";
        case CONNECTION_MADE: return "CONNECTION_MADE";
        case CONNECTION_NEEDED: return "CONNECTION_NEEDED";
        case CONNECTION_OK: return "CONNECTION_OK";
        case CONNECTION_SETENV: return "CONNECTION_SETENV";
        case CONNECTION_SSL_STARTUP: return "CONNECTION_SSL_STARTUP";
        case CONNECTION_STARTED: return "CONNECTION_STARTED";
    }
    return "";
}

static void work_check(void) {
    static SPI_plan *plan = NULL;
    static const char *command = SQL(
        WITH j AS (
            SELECT  COALESCE(COALESCE(j.user, data), current_setting('pg_task.default_user', false)) AS user,
                    COALESCE(COALESCE(data, j.user), current_setting('pg_task.default_data', false)) AS data,
                    COALESCE(schema, current_setting('pg_task.default_schema', false)) AS schema,
                    COALESCE(j.table, current_setting('pg_task.default_table', false)) AS table,
                    COALESCE(timeout, current_setting('pg_task.default_timeout', false)::int4) AS timeout,
                    COALESCE(count, current_setting('pg_task.default_count', false)::int4) AS count,
                    EXTRACT(epoch FROM COALESCE(live, current_setting('pg_task.default_live', false)::interval))::int8 AS live,
                    COALESCE(partman, current_setting('pg_task.default_partman', false)) AS partman
            FROM    json_populate_recordset(NULL::record, current_setting('pg_task.json', false)::json) AS j ("user" text, data text, schema text, "table" text, timeout int4, count int4, live interval, partman text)
        ) SELECT    DISTINCT j.* FROM j
        INNER JOIN  pg_user AS u ON usename = j.user
        INNER JOIN  pg_database AS d ON datname = data AND NOT datistemplate AND datallowconn AND usesysid = datdba
        WHERE       j.user = current_user AND data = current_catalog AND schema = current_setting('pg_task.schema', false) AND j.table = current_setting('pg_task.table', false) AND timeout = current_setting('pg_task.timeout', false)::int4
    );
    if (ShutdownRequestPending) return;
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    if (!SPI_tuptable->numvals) ShutdownRequestPending = true;
    SPI_finish_my();
}

static void work_command(Task *task, PGresult *result) {
    if (task->skip) { task->skip--; return; }
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    appendStringInfo(&task->output, "%s%s", task->output.len ? "\n" : "", PQcmdStatus(result));
}

static void work_edata(Task *task, const char *filename, int lineno, const char *funcname, const char *message) {
    ErrorData edata;
    MemSet(&edata, 0, sizeof(edata));
    edata.elevel = FATAL;
    edata.output_to_server = work_is_log_level_output(edata.elevel, log_min_messages);
    edata.filename = filename;
    edata.lineno = lineno;
    edata.funcname = funcname;
    edata.domain = TEXTDOMAIN ? TEXTDOMAIN : PG_TEXTDOMAIN("postgres");
    edata.context_domain = edata.domain;
    edata.sqlerrcode = ERRCODE_ADMIN_SHUTDOWN;
    edata.message = (char *)message;
    edata.message_id = edata.message;
    task_error(task, &edata);
    task_done(task);
}

static void work_event(WaitEventSet *set) {
    dlist_mutable_iter iter;
    AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
    AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);
    dlist_foreach_modify(iter, &work.head) {
        Task *task = dlist_container(Task, node, iter.cur);
        AddWaitEventToSet(set, task->event, PQsocket(task->conn), NULL, task);
    }
}

static void work_fail(Task *task, PGresult *result) {
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
    task->fail = true;
}

static void work_free(Task *task) {
    if (task->error.data) pfree(task->error.data);
    if (task->group) pfree(task->group);
    if (task->input) pfree(task->input);
    if (task->null) pfree(task->null);
    if (task->output.data) pfree(task->output.data);
    if (task->remote) pfree(task->remote);
    pfree(task);
}

static void work_finish(Task *task) {
    dlist_delete(&task->node);
    PQfinish(task->conn);
    if (!init_table_pid_hash_unlock(work.table, task->pid, task->hash)) W("!init_table_pid_hash_unlock(%i, %i, %i)", work.table, task->pid, task->hash);
    work_free(task);
}

static void work_error(Task *task, const char *msg, const char *err, bool finish) {
    if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
    appendStringInfo(&task->error, "%s%s", task->error.len ? "\n" : "", msg);
    if (err && strlen(err)) appendStringInfo(&task->error, " and %s", err);
    W("%li: %s", task->id, task->error.data);
    appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
    task->fail = true;
    task->skip++;
    task_done(task);
    finish ? work_finish(task) : work_free(task);
}

static int work_nevents(void) {
    dlist_mutable_iter iter;
    int nevents = 0;
    dlist_foreach_modify(iter, &work.head) {
        Task *task = dlist_container(Task, node, iter.cur);
        if (PQstatus(task->conn) == CONNECTION_BAD) { work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessageMy(task->conn), true); continue; }
        if (PQsocket(task->conn) == PGINVALID_SOCKET) { work_error(task, "PQsocket == PGINVALID_SOCKET", PQerrorMessageMy(task->conn), true); continue; }
        nevents++;
    }
    return nevents;
}

static void work_fini(void) {
    dlist_mutable_iter iter;
    StringInfoData error;
    D1("user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li", work.user, work.data, work.conf.schema, work.conf.table, work.conf.timeout, work.conf.count, work.conf.live);
    initStringInfoMy(TopMemoryContext, &error);
    appendStringInfo(&error, "terminating background worker \"%s\" due to administrator command", MyBgworkerEntry->bgw_type);
    dlist_foreach_modify(iter, &work.head) {
        Task *task = dlist_container(Task, node, iter.cur);
        if (!PQrequestCancel(task->conn)) work_error(task, error.data, PQerrorMessageMy(task->conn), true); else {
            work_edata(task, __FILE__, __LINE__, __func__, error.data);
            work_finish(task);
        }
    }
    pfree(error.data);
    if (ShutdownRequestPending) return;
    conf_work(&work.conf, work.data, work.user);
}

static void work_index(int count, const char *const *indexes) {
    const char *name_quote;
    const char *schema_quote = quote_identifier(work.conf.schema);
    const RangeVar *rangevar;
    List *names;
    RelationData *relation;
    StringInfoData src, name, idx;
    initStringInfoMy(TopMemoryContext, &name);
    appendStringInfoString(&name, work.conf.table);
    for (int i = 0; i < count; i++) {
        const char *index = indexes[i];
        appendStringInfoString(&name, "_");
        appendStringInfoString(&name, index);
    }
    appendStringInfoString(&name, "_idx");
    name_quote = quote_identifier(name.data);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE INDEX %s ON %s USING btree ), name_quote, work.schema_table);
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
    appendStringInfo(&idx, "%s.%s", schema_quote, name_quote);
    names = stringToQualifiedNameList(idx.data);
    rangevar = makeRangeVarFromNameList(names);
    D1("user = %s, data = %s, schema = %s, table = %s, index = %s, schema_table = %s", work.user, work.data, work.conf.schema, work.conf.table, idx.data, work.schema_table);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    } else if ((relation = relation_openrv_extended(rangevar, AccessShareLock, true))) {
        if (relation->rd_index && relation->rd_index->indrelid != work.table) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
        relation_close(relation, AccessShareLock);
    }
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    if (name_quote != name.data) pfree((void *)name_quote);
    if (schema_quote != work.conf.schema) pfree((void *)schema_quote);
    pfree(idx.data);
    pfree(name.data);
    pfree(src.data);
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

static bool work_busy(Task *task, int event) {
    if (PQisBusy(task->conn)) { W("%li: PQisBusy", task->id); task->event = event; return false; }
    return true;
}

static bool work_consume(Task *task) {
    if (!PQconsumeInput(task->conn)) { work_error(task, "!PQconsumeInput", PQerrorMessageMy(task->conn), true); return false; }
    return true;
}

static bool work_flush(Task *task) {
    switch (PQflush(task->conn)) {
        case 0: break;
        case 1: D1("%li: PQflush == 1", task->id); task->event = WL_SOCKET_MASK; return false;
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
    if (task_done(task)) { work_finish(task); return; }
    D1("id = %li, repeat = %s, delete = %s, live = %s", task->id, task->repeat ? "true" : "false", task->delete ? "true" : "false", task->live ? "true" : "false");
    if (task->repeat) task_repeat(task);
    if (task->delete && !task->output.data) task_delete(task);
    if (task->output.data) pfree(task->output.data);
    task->output.data = NULL;
    if (task->error.data) pfree(task->error.data);
    task->error.data = NULL;
    if (ShutdownRequestPending) task->live = false;
    (PQstatus(task->conn) != CONNECTION_OK || !task->live || task_live(task)) ? work_finish(task) : work_query(task);
}

static void work_schema(const char *schema, bool assign) {
    const char *schema_quote = quote_identifier(schema);
    List *names;
    StringInfoData src;
    D1("user = %s, data = %s, schema = %s", work.user, work.data, schema);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE SCHEMA %s), schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    if (assign) work.schema = get_namespace_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (schema_quote != schema) pfree((void *)schema_quote);
    pfree(src.data);
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
            case PGRES_FATAL_ERROR: W("%li: PQresultStatus == PGRES_FATAL_ERROR and %s", task->id, PQresultErrorMessageMy(result)); work_fail(task, result); break;
            case PGRES_TUPLES_OK: for (int row = 0; row < PQntuples(result); row++) work_success(task, result, row); break;
            default: D1("%li: %s", task->id, PQresStatus(PQresultStatus(result))); break;
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
    appendStringInfo(&input, SQL(SET "pg_task.id" = %li;), task->id);
    task->skip++;
    if (task->timeout) {
        appendStringInfo(&input, SQL(SET "statement_timeout" = %i;), task->timeout);
        task->skip++;
    }
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
    if (!PQsendQuery(task->conn, task->input)) { work_error(task, "!PQsendQuery", PQerrorMessageMy(task->conn), false); return; }
    task->socket = work_result;
    if (!work_flush(task)) return;
    task->event = WL_SOCKET_READABLE;
}

static void work_connect(Task *task) {
    bool connected = false;
    switch (PQstatus(task->conn)) {
        case CONNECTION_BAD: D1("%li: PQstatus == CONNECTION_BAD", task->id); work_error(task, "PQstatus == CONNECTION_BAD", PQerrorMessageMy(task->conn), true); return;
        case CONNECTION_OK: D1("%li: PQstatus == CONNECTION_OK", task->id); connected = true; break;
        default: break;
    }
    if (!connected) switch (PQconnectPoll(task->conn)) {
        case PGRES_POLLING_ACTIVE: D1("%li: PQconnectPoll == PGRES_POLLING_ACTIVE and %s", task->id, work_status(task)); break;
        case PGRES_POLLING_FAILED: D1("%li: PQconnectPoll == PGRES_POLLING_FAILED and %s", task->id, work_status(task)); work_error(task, "PQconnectPoll == PGRES_POLLING_FAILED", PQerrorMessageMy(task->conn), true); return;
        case PGRES_POLLING_OK: D1("%li: PQconnectPoll == PGRES_POLLING_OK and %s", task->id, work_status(task)); connected = true; break;
        case PGRES_POLLING_READING: D1("%li: PQconnectPoll == PGRES_POLLING_READING and %s", task->id, work_status(task)); task->event = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: D1("%li: PQconnectPoll == PGRES_POLLING_WRITING and %s", task->id, work_status(task)); task->event = WL_SOCKET_WRITEABLE; break;
    }
    if (connected) {
        if(!(task->pid = PQbackendPID(task->conn))) { work_error(task, "!PQbackendPID", PQerrorMessageMy(task->conn), true); return; }
        if (!init_table_pid_hash_lock(work.table, task->pid, task->hash)) { W("!init_table_pid_hash_lock(%i, %i, %i)", work.table, task->pid, task->hash); work_error(task, "!init_table_pid_hash_lock", NULL, true); return; }
        work_query(task);
    }
}

static void work_extension(const char *schema, const char *extension) {
    const char *extension_quote = quote_identifier(extension);
    const char *schema_quote = quote_identifier(schema);
    List *names;
    StringInfoData src;
    D1("user = %s, data = %s, schema = %s, extension = %s", work.user, work.data, schema, extension);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE EXTENSION %s SCHEMA %s), extension_quote, schema_quote);
    names = stringToQualifiedNameList(extension_quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_extension_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    list_free_deep(names);
    if (extension_quote != extension) pfree((void *)extension_quote);
    if (schema_quote != schema) pfree((void *)schema_quote);
    pfree(src.data);
}

static void work_partman(void) {
    const char *partman_quote = quote_identifier(work.conf.partman);
    const char *pkey_quote;
    const char *template_quote;
    const RangeVar *rangevar;
    List *names;
    StringInfoData create_template, pkey, template, partman_template;
    work_schema(work.conf.partman, false);
    work_extension(work.conf.partman, "pg_partman");
    initStringInfoMy(TopMemoryContext, &pkey);
    appendStringInfo(&pkey, "%s_pkey", work.conf.table);
    initStringInfoMy(TopMemoryContext, &template);
    appendStringInfo(&template, "template_%s_%s", work.conf.schema, work.conf.table);
    pkey_quote = quote_identifier(pkey.data);
    template_quote = quote_identifier(template.data);
    initStringInfoMy(TopMemoryContext, &partman_template);
    appendStringInfo(&partman_template, "%s.%s", partman_quote, template_quote);
    initStringInfoMy(TopMemoryContext, &create_template);
    appendStringInfo(&create_template, SQL(CREATE TABLE %1$s (LIKE %2$s INCLUDING ALL, CONSTRAINT %3$s PRIMARY KEY (id))), partman_template.data, work.schema_table, pkey_quote);
    names = stringToQualifiedNameList(partman_template.data);
    rangevar = makeRangeVarFromNameList(names);
    SPI_connect_my(create_template.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) {
        Datum values[] = {CStringGetTextDatumMy(TopMemoryContext, work.schema_table), CStringGetTextDatumMy(TopMemoryContext, "plan"), CStringGetTextDatumMy(TopMemoryContext, "native"), CStringGetTextDatumMy(TopMemoryContext, "monthly"), CStringGetTextDatumMy(TopMemoryContext, partman_template.data)};
        static Oid argtypes[] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID};
        StringInfoData create_parent;
        initStringInfoMy(TopMemoryContext, &create_parent);
        appendStringInfo(&create_parent, SQL(SELECT %1$s.create_parent(p_parent_table := $1, p_control := $2, p_type := $3, p_interval := $4, p_template_table := $5)), partman_quote);
        SPI_execute_with_args_my(create_template.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
        SPI_commit_my();
        SPI_start_transaction_my(create_parent.data);
        SPI_execute_with_args_my(create_parent.data, countof(argtypes), argtypes, values, NULL, SPI_OK_SELECT, false);
        if (SPI_tuptable->numvals != 1) E("SPI_tuptable->numvals != 1");
        if (!DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "create_parent", false))) E("!create_parent");
        if (values[0]) pfree((void *)values[0]);
        if (values[1]) pfree((void *)values[1]);
        if (values[2]) pfree((void *)values[2]);
        if (values[3]) pfree((void *)values[3]);
        if (values[4]) pfree((void *)values[4]);
    }
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    if (partman_quote != work.conf.partman) pfree((void *)partman_quote);
    if (pkey_quote != pkey.data) pfree((void *)pkey_quote);
    if (template_quote != template.data) pfree((void *)template_quote);
    pfree(create_template.data);
    pfree(partman_template.data);
    pfree(pkey.data);
    pfree(template.data);
}

static void work_remote(Task *task_) {
    bool password = false;
    char *err;
    char *options = NULL;
    const char **keywords;
    const char **values;
    int arg = 3;
    PQconninfoOption *opts = PQconninfoParse(task_->remote, &err);
    StringInfoData name, value;
    Task *task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task));
    *task = *task_;
    D1("id = %li, group = %s, remote = %s, max = %i, oid = %i", task->id, task->group, task->remote ? task->remote : default_null, task->max, work.table);
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
    appendStringInfo(&name, "pg_task %s %s %s", work.conf.schema, work.conf.table, task->group);
    arg = 0;
    keywords[arg] = "application_name";
    values[arg] = name.data;
    initStringInfoMy(TopMemoryContext, &value);
    if (options) appendStringInfoString(&value, options);
    appendStringInfo(&value, "%s-c pg_task.data=%s", value.len ? " " : "", work.data);
    appendStringInfo(&value, " -c pg_task.user=%s", work.user);
    appendStringInfo(&value, " -c pg_task.schema=%s", work.conf.schema);
    appendStringInfo(&value, " -c pg_task.table=%s", work.conf.table);
    appendStringInfo(&value, " -c pg_task.oid=%i", work.table);
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
    dlist_push_head(&work.head, &task->node);
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
}

static void work_table(void) {
    const RangeVar *rangevar;
    List *names;
    StringInfoData src;
    D1("user = %s, data = %s, schema = %s, table = %s, schema_table = %s, schema_type = %s", work.user, work.data, work.conf.schema, work.conf.table, work.schema_table, work.schema_type);
    set_config_option("pg_task.table", work.conf.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(
        CREATE TABLE %1$s (
            id bigserial NOT NULL%4$s,
            parent int8 DEFAULT current_setting('pg_task.id', true)::int8,
            plan timestamptz NOT NULL DEFAULT current_timestamp,
            start timestamptz,
            stop timestamptz,
            live interval NOT NULL DEFAULT '0 sec',
            timeout interval NOT NULL DEFAULT '0 sec',
            repeat interval NOT NULL DEFAULT '0 sec',
            hash int4 NOT NULL GENERATED ALWAYS AS (hashtext("group"||COALESCE(remote, '%3$s'))) STORED,
            count int4 NOT NULL DEFAULT 0,
            max int4 NOT NULL DEFAULT ~(1<<31),
            pid int4,
            state %2$s NOT NULL DEFAULT 'PLAN'::%2$s,
            delete boolean NOT NULL DEFAULT false,
            drift boolean NOT NULL DEFAULT true,
            header boolean NOT NULL DEFAULT true,
            string boolean NOT NULL DEFAULT true,
            delimiter "char" NOT NULL DEFAULT '%5$s',
            escape "char",
            quote "char",
            error text,
            "group" text NOT NULL DEFAULT 'group',
            input text NOT NULL,
            "null" text NOT NULL DEFAULT current_setting('pg_task.default_null', false),
            output text,
            remote text
        )
    ), work.schema_table, work.schema_type, "", work.conf.partman ? "" : " PRIMARY KEY", "\t");
    if (work.conf.partman) appendStringInfoString(&src, " PARTITION BY RANGE (plan)");
    names = stringToQualifiedNameList(work.schema_table);
    rangevar = makeRangeVarFromNameList(names);
    SPI_connect_my(src.data);
    if (!OidIsValid(RangeVarGetRelid(rangevar, NoLock, true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    work.table = RangeVarGetRelid(rangevar, NoLock, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree((void *)rangevar);
    list_free_deep(names);
    set_config_option("pg_task.table", work.conf.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    resetStringInfo(&src);
    appendStringInfo(&src, "%i", work.table);
    set_config_option("pg_task.oid", src.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(src.data);
}

static void work_task(Task *task) {
    BackgroundWorkerHandle *handle = NULL;
    BackgroundWorker worker;
    pid_t pid;
    size_t len = 0;
    D1("user = %s, data = %s, schema = %s, table = %s, id = %li, group = %s, max = %i, oid = %i", work.user, work.data, work.conf.schema, work.conf.table, task->id, task->group, task->max, work.table);
    MemSet(&worker, 0, sizeof(worker));
    if (strlcpy(worker.bgw_function_name, "task_main", sizeof(worker.bgw_function_name)) >= sizeof(worker.bgw_function_name)) { work_error(task, "strlcpy", NULL, false); return; }
    if (strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name)) >= sizeof(worker.bgw_library_name)) { work_error(task, "strlcpy", NULL, false); return; }
    if (snprintf(worker.bgw_type, sizeof(worker.bgw_type) - 1, "pg_task %s %s %s", work.conf.schema, work.conf.table, task->group) >= sizeof(worker.bgw_type) - 1) { work_error(task, "snprintf", NULL, false); return; }
    if (snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s %s", work.user, work.data, worker.bgw_type) >= sizeof(worker.bgw_name) - 1) { work_error(task, "snprintf", NULL, false); return; }
#define X(name, serialize, deserialize) serialize(task->name);
    TASK
#undef X
#define X(name, serialize, deserialize) serialize(work.name);
    WORK
#undef X
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = Int64GetDatum(task->id);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (init_check_ascii_all(&worker)) { work_error(task, "init_check_ascii_all", NULL, false); return; }
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) { work_error(task, "RegisterDynamicBackgroundWorker", NULL, false); if (handle) pfree(handle); return; }
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: work_error(task, "WaitForBackgroundWorkerStartup == BGWH_NOT_YET_STARTED", NULL, false); pfree(handle); return;
        case BGWH_POSTMASTER_DIED: work_error(task, "WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED", NULL, false); pfree(handle); return;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: work_error(task, "WaitForBackgroundWorkerStartup == BGWH_STOPPED", NULL, false); pfree(handle); return;
    }
    pfree(handle);
    pfree(task->group);
}

static void work_type(void) {
    int32 typmod;
    Oid type = InvalidOid;
    StringInfoData src;
    D1("user = %s, data = %s, schema = %s, table = %s", work.user, work.data, work.conf.schema, work.conf.table);
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP')), work.schema_type);
    SPI_connect_my(src.data);
    parseTypeString(work.schema_type, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my();
    pfree(src.data);
}

static void work_conf(void) {
    const char *index_input[] = {"input"};
    const char *index_parent[] = {"parent"};
    const char *index_plan[] = {"plan"};
    const char *index_state[] = {"state"};
    const char *schema_quote = quote_identifier(work.conf.schema);
    const char *table_quote = quote_identifier(work.conf.table);
    StringInfoData buf;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%s.%s", schema_quote, table_quote);
    if (work.schema_table) pfree(work.schema_table);
    work.schema_table = buf.data;
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%s.state", schema_quote);
    if (work.schema_type) pfree(work.schema_type);
    work.schema_type = buf.data;
    if (work.conf.schema != schema_quote) pfree((void *)schema_quote);
    if (work.conf.table != table_quote) pfree((void *)table_quote);
    D1("user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, schema_table = %s, schema_type = %s, partman = %s", work.user, work.data, work.conf.schema, work.conf.table, work.conf.timeout, work.conf.count, work.conf.live, work.schema_table, work.schema_type, work.conf.partman ? work.conf.partman : default_null);
    work_schema(work.conf.schema, true);
    set_config_option("pg_task.schema", work.conf.schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    work_type();
    work_table();
    work_index(countof(index_input), index_input);
    work_index(countof(index_parent), index_parent);
    work_index(countof(index_plan), index_plan);
    work_index(countof(index_state), index_state);
    if (work.conf.partman) work_partman();
    set_config_option("pg_task.data", work.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.user", work.user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    initStringInfoMy(TopMemoryContext, &buf);
    appendStringInfo(&buf, "%i", work.conf.timeout);
    set_config_option("pg_task.timeout", buf.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(buf.data);
    dlist_init(&work.head);
}

static void work_update(void) {
    Datum values[] = {ObjectIdGetDatum(work.table)};
    static Oid argtypes[] = {OIDOID};
    StringInfoData src;
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(
        WITH s AS (
            SELECT id FROM %1$s AS t
            LEFT JOIN pg_locks AS l ON l.locktype = 'userlock' AND l.mode = 'AccessExclusiveLock' AND l.granted AND l.objsubid = 4 AND l.database = $1 AND l.classid = t.id>>32 AND l.objid = t.id<<32>>32
            WHERE plan < current_timestamp AND state IN ('TAKE'::%2$s, 'WORK'::%2$s) AND l.pid IS NULL
            FOR UPDATE OF t SKIP LOCKED
        ) UPDATE %1$s AS u SET state = 'PLAN'::%2$s, start = NULL, stop = NULL, pid = NULL FROM s WHERE u.id = s.id RETURNING u.id
    ), work.schema_table, work.schema_type);
    SPI_connect_my(src.data);
    SPI_execute_with_args_my(src.data, countof(argtypes), argtypes, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_tuptable->numvals; row++) {
        int64 id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false));
        W("row = %lu, id = %li", row, id);
    }
    SPI_finish_my();
    pfree(src.data);
}

static void work_init(void) {
    char *p = MyBgworkerEntry->bgw_extra;
    MemoryContextData *oldcontext = CurrentMemoryContext;
    MemSet(&work, 0, sizeof(work));
#define X(type, name, get, serialize, deserialize) deserialize(work.conf.name);
    CONF
#undef X
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionByOid(work.conf.data, work.conf.user, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    StartTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
    work.data = get_database_name(work.conf.data);
    work.user = GetUserNameFromId(work.conf.user, false);
    CommitTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    if (!MyProcPort->user_name) MyProcPort->user_name = work.user;
    if (!MyProcPort->database_name) MyProcPort->database_name = work.data;
    set_config_option("application_name", MyBgworkerEntry->bgw_type, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    D1("user_oid = %i, data_oid = %i, user = %s, data = %s, schema = %s, table = %s, timeout = %i, count = %i, live = %li, partman = %s", work.conf.user, work.conf.data, work.user, work.data, work.conf.schema, work.conf.table, work.conf.timeout, work.conf.count, work.conf.live, work.conf.partman ? work.conf.partman : default_null);
    work_conf();
    work_update();
}

static void work_timeout(void) {
    Datum values[] = {ObjectIdGetDatum(work.table)};
    static char *command = NULL;
    static Oid argtypes[] = {OIDOID};
    static SPI_plan *plan = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfoMy(TopMemoryContext, &buf);
        appendStringInfo(&buf, SQL(
            WITH s AS ( WITH l AS (
                SELECT count(classid) AS classid, objid FROM pg_locks WHERE locktype = 'userlock' AND mode = 'AccessShareLock' AND granted AND objsubid = 5 AND database = $1 GROUP BY objid
            ), s AS (
                SELECT t.id, t.hash, CASE WHEN t.max > 0 THEN t.max ELSE 1 END - COALESCE(classid, 0) AS count FROM %1$s AS t LEFT JOIN l ON objid = t.hash
                WHERE t.state = 'PLAN'::%2$s AND t.plan + concat_ws(' ', (CASE WHEN t.max < 0 THEN -t.max ELSE 0 END)::text, 'msec')::interval <= current_timestamp AND t.start IS NULL AND t.stop IS NULL AND t.pid IS NULL
                AND CASE WHEN t.max > 0 THEN t.max ELSE 1 END > COALESCE(classid, 0) FOR UPDATE OF t SKIP LOCKED
            ) SELECT id, hash, count - row_number() OVER (PARTITION BY hash ORDER BY count DESC, id) + 1 AS count FROM s ORDER BY s.count DESC, id
            ) UPDATE %1$s AS u SET state = 'TAKE'::%2$s FROM s WHERE u.id = s.id AND s.count > 0 RETURNING u.id, u.hash, u.group, u.remote, u.max
        ), work.schema_table, work.schema_type);
        command = buf.data;
    }
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_tuptable->numvals; row++) {
        Task task;
        MemSet(&task, 0, sizeof(task));
        task.group = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "group", false));
        task.hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "hash", false));
        task.id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false));
        task.max = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "max", false));
        task.remote = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "remote", true));
        D1("row = %lu, id = %li, hash = %i, group = %s, remote = %s, max = %i", row, task.id, task.hash, task.group, task.remote ? task.remote : default_null, task.max);
        task.remote ? work_remote(&task) : work_task(&task);
    }
    if (work.conf.count) work.count += SPI_tuptable->numvals;
    SPI_finish_my();
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
    if (!init_data_user_table_lock(MyDatabaseId, GetUserId(), work.table)) W("!init_data_user_table_lock(%i, %i, %i)", MyDatabaseId, GetUserId(), work.table); else while (!ShutdownRequestPending) {
        int nevents = 2 + work_nevents();
        WaitEvent *events = MemoryContextAllocZero(TopMemoryContext, nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        work_event(set);
        if (cur_timeout <= 0) {
            INSTR_TIME_SET_CURRENT(start_time);
            cur_timeout = work.conf.timeout;
        }
        nevents = WaitEventSetWait(set, cur_timeout, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) work_latch();
            if (event->events & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
            if (event->events & WL_SOCKET_READABLE) work_readable(event->user_data);
            if (event->events & WL_SOCKET_WRITEABLE) work_writeable(event->user_data);
        }
        if (work.conf.timeout >= 0) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = work.conf.timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout <= 0) work_timeout();
        }
        FreeWaitEventSet(set);
        pfree(events);
        if (work.conf.count && work.count >= work.conf.count) break;
        if (work.conf.live && TimestampDifferenceExceeds(MyStartTimestamp, GetCurrentTimestamp(), work.conf.live * 1000)) break;
    }
    if (!init_data_user_table_unlock(MyDatabaseId, GetUserId(), work.table)) W("!init_data_user_table_unlock(%i, %i, %i)", MyDatabaseId, GetUserId(), work.table);
    work_fini();
}
