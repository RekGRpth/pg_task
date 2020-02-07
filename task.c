#include "include.h"

static volatile sig_atomic_t got_sigterm = false;

static char *request;
static char *response;
static char *state;

static const char *data;
static const char *data_quote;
static const char *point;
static const char *schema;
static const char *schema_quote;
static const char *table;
static const char *table_quote;
static const char *user;
static const char *user_quote;

static const char *queue;

static Datum data_datum;
static Datum user_datum;
static Datum schema_datum;
static Datum table_datum;
static Datum queue_datum;

static Datum id;
static MemoryContext executeMemoryContext;
static TimestampTz start;
static uint32 count;
static uint32 max;
static uint64 timeout;

static void update_ps_display(void) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_name, DatumGetUInt64(id));
    init_ps_display(buf.data, "", "", "");
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_type, DatumGetUInt64(id));
    pgstat_report_appname(application_name = pstrdup(buf.data));
    pfree(buf.data);
}

static void work(void) {
    int rc;
    static Oid argtypes[] = {INT8OID, INT8OID};
    Datum values[] = {id, MyProcPid};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    update_ps_display();
    timeout = 0;
    count++;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE)\n"
            "UPDATE  %s%s%s AS u\n"
            "SET     state = 'WORK',\n"
            "        start = current_timestamp,\n"
            "        pid = $2\n"
            "FROM s WHERE u.id = s.id RETURNING request,\n"
            "        COALESCE(EXTRACT(epoch FROM timeout), 0)::INT * 1000 AS timeout", schema_quote, point, table_quote, schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        MemoryContext workMemoryContext = MemoryContextSwitchTo(executeMemoryContext);
        bool timeout_isnull;
        request = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"));
        timeout = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "timeout"), &timeout_isnull));
        if (timeout_isnull) ereport(ERROR, (errmsg("%s(%s:%d): timeout_isnull", __func__, __FILE__, __LINE__)));
        MemoryContextSwitchTo(workMemoryContext);
    }
    SPI_commit();
    SPI_finish_my(command);
    if (0 < StatementTimeout && StatementTimeout < timeout) timeout = StatementTimeout;
}

static void repeat_task(void) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id AS parent, CASE\n"
            "    WHEN drift THEN current_timestamp + repeat\n"
            "    ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1)\n"
            "END AS dt, queue, max, request, 'PLAN' AS state, timeout, delete, repeat, drift, count, live\n"
            "FROM %s%s%s WHERE id = $1 AND state IN ('DONE', 'FAIL') LIMIT 1\n"
            ") INSERT INTO %s%s%s (parent, dt, queue, max, request, state, timeout, delete, repeat, drift, count, live) SELECT * FROM s", schema_quote, point, table_quote, schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_INSERT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(command);
}

static void delete_task(void) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DELETE FROM %s%s%s WHERE id = $1", schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_DELETE) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(command);
}

static void more(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TIMESTAMPTZOID, INT4OID, INT4OID};
    Datum values[] = {queue_datum, TimestampTzGetDatum(start), UInt32GetDatum(max), UInt32GetDatum(count)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (\n"
            "SELECT  id\n"
            "FROM    %s%s%s\n"
            "WHERE   state = 'PLAN'\n"
            "AND     dt <= current_timestamp\n"
            "AND     queue = $1\n"
            "AND     $2 + COALESCE(live, '0 sec'::INTERVAL) >= current_timestamp\n"
            "AND     COALESCE(max, ~(1<<31)) >= $3\n"
            "AND     COALESCE(count, 0) > $4\n"
            "ORDER BY COALESCE(max, ~(1<<31)) DESC LIMIT 1 FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id, set_config('pg_task.id', u.id::TEXT, false)", schema_quote, point, table_quote, schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    if (!SPI_processed) got_sigterm = true; else {
        bool id_isnull;
        id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
    }
    SPI_commit();
    SPI_finish_my(command);
}

static void done(void) {
    int rc;
    bool delete, repeat;
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    Datum values[] = {id, CStringGetTextDatum(state), response ? CStringGetTextDatum(response) : (Datum)NULL};
    char nulls[] = {' ', ' ', response ? ' ' : 'n'};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(nulls)/sizeof(nulls[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    elog(LOG, "%s(%s:%d): id = %lu, response = %s, state = %s", __func__, __FILE__, __LINE__, DatumGetUInt64(id), response ? response : "(null)", state);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE\n)\n"
            "UPDATE %s%s%s AS u SET state = $2::STATE, stop = current_timestamp, response = $3 FROM s WHERE u.id = s.id\n"
            "RETURNING delete, queue,\n"
            "repeat IS NOT NULL AND state IN ('DONE', 'FAIL') AS repeat", schema_quote, point, table_quote, schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool delete_isnull, repeat_isnull;
        delete = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "delete"), &delete_isnull));
        repeat = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "repeat"), &repeat_isnull));
        if (delete_isnull) ereport(ERROR, (errmsg("%s(%s:%d): delete_isnull", __func__, __FILE__, __LINE__)));
        if (repeat_isnull) ereport(ERROR, (errmsg("%s(%s:%d): repeat_isnull", __func__, __FILE__, __LINE__)));
    }
    SPI_commit();
    SPI_finish_my(command);
    pfree((void *)values[1]);
    if (response) pfree((void *)values[2]);
    if (repeat) repeat_task();
    if (delete && !response) delete_task();
    if (response) pfree(response);
}

static void success(void) {
    response = NULL;
    if ((SPI_tuptable) && (SPI_processed > 0)) {
        MemoryContext successMemoryContext = MemoryContextSwitchTo(executeMemoryContext);
        StringInfoData buf;
        initStringInfo(&buf);
        if (SPI_tuptable->tupdesc->natts > 1) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *name = SPI_fname(SPI_tuptable->tupdesc, col);
                char *type = SPI_gettype(SPI_tuptable->tupdesc, col);
                appendStringInfo(&buf, "%s::%s", name, type);
                if (col > 1) appendStringInfoString(&buf, "\t");
                pfree(name);
                pfree(type);
            }
            appendStringInfoString(&buf, "\n");
        }
        for (uint64 row = 0; row < SPI_processed; row++) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                appendStringInfo(&buf, "%s", value);
                if (col > 1) appendStringInfoString(&buf, "\t");
                pfree(value);
            }
            if (row < SPI_processed - 1) appendStringInfoString(&buf, "\n");
        }
        response = buf.data;
        MemoryContextSwitchTo(successMemoryContext);
    }
    state = "DONE";
}

static void error(void) {
    MemoryContext errorMemoryContext = MemoryContextSwitchTo(executeMemoryContext);
    ErrorData *edata = CopyErrorData();
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "elevel::int4\t%i", edata->elevel);
    appendStringInfo(&buf, "\noutput_to_server::bool\t%s", edata->output_to_server ? "true" : "false");
    appendStringInfo(&buf, "\noutput_to_client::bool\t%s", edata->output_to_client ? "true" : "false");
    appendStringInfo(&buf, "\nshow_funcname::bool\t%s", edata->show_funcname ? "true" : "false");
    appendStringInfo(&buf, "\nhide_stmt::bool\t%s", edata->hide_stmt ? "true" : "false");
    appendStringInfo(&buf, "\nhide_ctx::bool\t%s", edata->hide_ctx ? "true" : "false");
    if (edata->filename) appendStringInfo(&buf, "\nfilename::text\t%s", edata->filename);
    if (edata->lineno) appendStringInfo(&buf, "\nlineno::int4\t%i", edata->lineno);
    if (edata->funcname) appendStringInfo(&buf, "\nfuncname::text\t%s", edata->funcname);
    if (edata->domain) appendStringInfo(&buf, "\ndomain::text\t%s", edata->domain);
    if (edata->context_domain) appendStringInfo(&buf, "\ncontext_domain::text\t%s", edata->context_domain);
    if (edata->sqlerrcode) appendStringInfo(&buf, "\nsqlerrcode::int4\t%i", edata->sqlerrcode);
    if (edata->message) appendStringInfo(&buf, "\nmessage::text\t%s", edata->message);
    if (edata->detail) appendStringInfo(&buf, "\ndetail::text\t%s", edata->detail);
    if (edata->detail_log) appendStringInfo(&buf, "\ndetail_log::text\t%s", edata->detail_log);
    if (edata->hint) appendStringInfo(&buf, "\nhint::text\t%s", edata->hint);
    if (edata->context) appendStringInfo(&buf, "\ncontext::text\t%s", edata->context);
    if (edata->message_id) appendStringInfo(&buf, "\nmessage_id::text\t%s", edata->message_id);
    if (edata->schema_name) appendStringInfo(&buf, "\nschema_name::text\t%s", edata->schema_name);
    if (edata->table_name) appendStringInfo(&buf, "\ntable_name::text\t%s", edata->table_name);
    if (edata->column_name) appendStringInfo(&buf, "\ncolumn_name::text\t%s", edata->column_name);
    if (edata->datatype_name) appendStringInfo(&buf, "\ndatatype_name::text\t%s", edata->datatype_name);
    if (edata->constraint_name) appendStringInfo(&buf, "\nconstraint_name::text\t%s", edata->constraint_name);
    if (edata->cursorpos) appendStringInfo(&buf, "\ncursorpos::int4\t%i", edata->cursorpos);
    if (edata->internalpos) appendStringInfo(&buf, "\ninternalpos::int4\t%i", edata->internalpos);
    if (edata->internalquery) appendStringInfo(&buf, "\ninternalquery::text\t%s", edata->internalquery);
    if (edata->saved_errno) appendStringInfo(&buf, "\nsaved_errno::int4\t%i", edata->saved_errno);
    FreeErrorData(edata);
    response = buf.data;
    state = "FAIL";
    MemoryContextSwitchTo(errorMemoryContext);
}

static void execute(void) {
    work();
    elog(LOG, "%s(%s:%d): id = %lu, timeout = %lu, request = %s, count = %u", __func__, __FILE__, __LINE__, DatumGetUInt64(id), timeout, request, count);
    SPI_connect_my(request, timeout);
    PG_TRY(); {
        int rc;
        if ((rc = SPI_execute(request, false, 0)) < 0) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
        success();
        SPI_commit();
    } PG_CATCH(); {
        error();
        SPI_rollback();
    } PG_END_TRY();
    SPI_finish_my(request);
    pfree(request);
    done();
    more();
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void task_worker(Datum main_arg); void task_worker(Datum main_arg) {
    StringInfoData buf;
    executeMemoryContext = CurrentMemoryContext;
    id = main_arg;
    start = GetCurrentTimestamp();
    count = 0;
    data = MyBgworkerEntry->bgw_extra;
    user = data + strlen(data) + 1;
    schema = user + strlen(user) + 1;
    table = schema + strlen(schema) + 1;
    queue = table + strlen(table) + 1;
    max = *(typeof(max) *)(queue + strlen(queue) + 1);
    if (table == schema + 1) schema = NULL;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max);
    data_quote = quote_identifier(data);
    data_datum = CStringGetTextDatum(data);
    user_quote = quote_identifier(user);
    user_datum = CStringGetTextDatum(user);
    schema_quote = schema ? quote_identifier(schema) : "";
    schema_datum = schema ? CStringGetTextDatum(schema) : (Datum)NULL;
    point = schema ? "." : "";
    table_quote = quote_identifier(table);
    table_datum = CStringGetTextDatum(table);
    queue_datum = CStringGetTextDatum(queue);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(data, user, 0);
    set_config_option("pg_task.data", data, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    set_config_option("pg_task.user", user, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    if (schema) set_config_option("pg_task.schema", schema, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    set_config_option("pg_task.table", table, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    set_config_option("pg_task.queue", queue, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%lu", DatumGetUInt64(id));
    set_config_option("pg_task.id", buf.data, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    pfree(buf.data);
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) break;
        if (!BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid)) break;
        if (rc & WL_LATCH_SET) { ResetLatch(MyLatch); CHECK_FOR_INTERRUPTS(); }
        if (got_sigterm) break;
        if (rc & WL_TIMEOUT) execute();
    } while (!got_sigterm);
}
