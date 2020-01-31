#include "include.h"

static char *database = NULL;
static char *username = NULL;
static char *schema = NULL;
static char *table = NULL;
static char *queue = NULL;
static uint64 max;
static uint64 count;
static const char *database_q;
static const char *username_q;
static const char *schema_q;
static const char *point;
static const char *table_q;
static TimestampTz start;

static void work(const MemoryContext oldMemoryContext, const Datum id, char **request, uint64 *timeout) {
    int rc;
    static Oid argtypes[] = {INT8OID, INT8OID};
    Datum values[] = {id, MyProcPid};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%lu", DatumGetUInt64(id));
    if (set_config_option("pg_task.task_id", buf.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false) <= 0) ereport(ERROR, (errmsg("%s(%s:%d): set_config_option <= 0", __func__, __FILE__, __LINE__)));
    pfree(buf.data);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE)\n"
            "UPDATE  %s%s%s AS u\n"
            "SET     state = 'WORK',\n"
            "        start = current_timestamp,\n"
            "        pid = $2\n"
            "FROM s WHERE u.id = s.id RETURNING request, COALESCE(EXTRACT(epoch FROM timeout), 0)::INT * 1000 AS timeout", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool request_isnull, timeout_isnull;
        char *value = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"), &request_isnull));
        *timeout = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "timeout"), &timeout_isnull));
        if (request_isnull) ereport(ERROR, (errmsg("%s(%s:%d): request_isnull", __func__, __FILE__, __LINE__)));
        if (timeout_isnull) ereport(ERROR, (errmsg("%s(%s:%d): timeout_isnull", __func__, __FILE__, __LINE__)));
        *request = MemoryContextStrdup(oldMemoryContext, value);
        pfree(value);
    }
    SPI_finish_my(command);
}

static void repeat_task(const Datum id) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id AS parent, CASE\n"
            "    WHEN drift THEN current_timestamp + repeat\n"
            "    ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1)\n"
            "END AS dt, queue, max, request, 'PLAN' AS state, timeout, delete, repeat, drift, count, live\n"
            "FROM %s%s%s WHERE id = '1' AND state IN ('DONE', 'FAIL') LIMIT 1\n"
            ") INSERT INTO %s%s%s (parent, dt, queue, max, request, state, timeout, delete, repeat, drift, count, live) SELECT * FROM s", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
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

static void delete_task(const Datum id) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DELETE FROM %s%s%s WHERE id = $1", schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
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

static void done(const Datum id, const char *data, const char *state) {
    int rc;
    bool delete, repeat;
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    Datum values[] = {id, CStringGetTextDatum(state), data ? CStringGetTextDatum(data) : (Datum)NULL};
    char nulls[] = {' ', ' ', data ? ' ' : 'n', ' ', ' '};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    elog(LOG, "%s(%s:%d): id = %lu, data = %s, state = %s", __func__, __FILE__, __LINE__, DatumGetUInt64(id), data ? data : "(null)", state);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE\n)\n"
            "UPDATE %s%s%s AS u SET state = $2, stop = current_timestamp, response = $3 FROM s WHERE u.id = s.id\n"
            "RETURNING delete, queue,\n"
            "repeat IS NOT NULL AND state IN ('DONE', 'FAIL') AS repeat", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool delete_isnull, repeat_isnull;
        delete = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "delete"), &delete_isnull));
        repeat = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "repeat"), &repeat_isnull));
        if (delete_isnull) ereport(ERROR, (errmsg("%s(%s:%d): delete_isnull", __func__, __FILE__, __LINE__)));
        if (repeat_isnull) ereport(ERROR, (errmsg("%s(%s:%d): repeat_isnull", __func__, __FILE__, __LINE__)));
    }
    SPI_finish_my(command);
    if (repeat) repeat_task(id);
    if (delete && !data) delete_task(id);
}

static void success(const MemoryContext oldMemoryContext, char **data, char **state) {
    if ((SPI_tuptable) && (SPI_processed > 0)) {
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
        *data = MemoryContextStrdup(oldMemoryContext, buf.data);
        pfree(buf.data);
    }
    *state = "DONE";
}

static void error(const MemoryContext oldMemoryContext, char **data, char **state) {
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
    *data = MemoryContextStrdup(oldMemoryContext, buf.data);
    pfree(buf.data);
    *state = "FAIL";
}

static void update_ps_display(const Datum id) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_name, DatumGetUInt64(id));
    init_ps_display(buf.data, "", "", "");
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_type, DatumGetUInt64(id));
    pgstat_report_appname(buf.data);
    pfree(buf.data);
}

static void execute(const Datum id);
static void more(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TIMESTAMPTZOID, INT8OID, INT8OID};
    Datum values[] = {CStringGetTextDatum(queue), TimestampTzGetDatum(start), UInt64GetDatum(max), UInt64GetDatum(count)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
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
            "AND     COALESCE(count, 0) >= $4\n"
            "ORDER BY COALESCE(max, ~(1<<31)) DESC LIMIT 1 FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) SPI_finish_my(command); else {
        bool id_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
        SPI_finish_my(command);
        execute(id);
    }
}

static void execute(const Datum id) {
    int rc;
    uint64 timeout = 0;
    char *request, *data = NULL, *state;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    count++;
    update_ps_display(id);
    work(oldMemoryContext, id, &request, &timeout);
    if (0 < StatementTimeout && StatementTimeout < timeout) timeout = StatementTimeout;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, id = %lu, timeout = %lu, request = %s, count = %lu", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, DatumGetUInt64(id), timeout, request, count);
    SPI_connect_my(request, timeout);
    PG_TRY(); {
        if ((rc = SPI_execute(request, false, 0)) < 0) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
        success(oldMemoryContext, &data, &state);
        SPI_commit();
    } PG_CATCH(); {
        error(oldMemoryContext, &data, &state);
        SPI_rollback();
    } PG_END_TRY();
    SPI_finish_my(request);
    pfree(request);
    done(id, data, state);
    if (data) pfree(data);
    more();
}

void task_worker(Datum main_arg); void task_worker(Datum main_arg) {
    Datum id = main_arg;
    start = GetCurrentTimestamp();
    count = 0;
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    schema = username + strlen(username) + 1;
    table = schema + strlen(schema) + 1;
    queue = table + strlen(table) + 1;
    max = *(uint64 *)(queue + strlen(queue) + 1);
    if (table == schema + 1) schema = NULL;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %lu", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max);
    database_q = quote_identifier(database);
    username_q = quote_identifier(username);
    schema_q = schema ? quote_identifier(schema) : "";
    point = schema ? "." : "";
    table_q = quote_identifier(table);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(database, username, 0);
    execute(id);
}
