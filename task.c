#include "include.h"

extern bool xact_started;
extern char *default_null;
extern int task_default_fetch;
extern Work work;
static emit_log_hook_type emit_log_hook_prev = NULL;
Task task;

static bool task_live(Task *t) {
    Datum values[] = {Int32GetDatum(t->shared->hash), Int32GetDatum(t->shared->max), Int32GetDatum(t->count), TimestampTzGetDatum(t->start)};
    static Oid argtypes[] = {INT4OID, INT4OID, INT4OID, TIMESTAMPTZOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, hash = %i, max = %i, count = %i, start = %s", t->shared->id, t->shared->hash, t->shared->max, t->count, timestamptz_to_str(t->start));
    set_ps_display_my("live");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "state" = 'PLAN'::%2$s AND "hash" = $1 AND "max" >= $2 AND CASE
                    WHEN "count" > 0 AND "live" > '0 sec' THEN "count" > $3 AND $4 + "live" > CURRENT_TIMESTAMP ELSE "count" > $3 OR $4 + "live" > CURRENT_TIMESTAMP
                END ORDER BY "max" DESC, "id" LIMIT 1 FOR UPDATE OF t %3$s
            ) UPDATE %1$s AS t SET "state" = 'TAKE'::%2$s FROM s
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work.schema_table, work.schema_type,
#if PG_VERSION_NUM >= 90500
        "SKIP LOCKED"
#else
        ""
#endif
        );
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    t->shared->id = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false)) : 0;
    elog(DEBUG1, "id = %li", t->shared->id);
    set_ps_display_my("idle");
    return ShutdownRequestPending || !t->shared->id;
}

static void task_delete(Task *t) {
    Datum values[] = {Int64GetDatum(t->shared->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", t->shared->id);
    set_ps_display_my("delete");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "id" = $1 FOR UPDATE OF t
            ) DELETE FROM %1$s AS t
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "id" = $1 RETURNING t.id
        ), work.schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_DELETE_RETURNING);
    if (SPI_processed != 1) elog(WARNING, "delete id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed);
    else elog(DEBUG1, "delete id = %li", DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

static void task_insert(Task *t) {
    Datum values[] = {Int64GetDatum(t->shared->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", t->shared->id);
    set_ps_display_my("insert");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT * FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "id" = $1 FOR UPDATE OF t
            ) INSERT INTO %1$s AS t ("parent", "plan", "active", "live", "repeat", "timeout", "count", "max", "delete", "drift", "header", "string", "delimiter", "escape", "quote", "group", "input", "null", "remote")
            SELECT "id", CASE
                WHEN "drift" THEN CURRENT_TIMESTAMP + "repeat"
                ELSE (WITH RECURSIVE r AS (SELECT "plan" AS p UNION SELECT p + "repeat" FROM r WHERE p <= CURRENT_TIMESTAMP) SELECT * FROM r ORDER BY 1 DESC LIMIT 1)
            END AS "plan", "active", "live", "repeat", "timeout", "count", "max", "delete", "drift", "header", "string", "delimiter", "escape", "quote", "group", "input", "null", "remote" FROM s WHERE "repeat" > '0 sec' LIMIT 1 RETURNING t.id
        ), work.schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_INSERT_RETURNING);
    if (SPI_processed != 1) elog(WARNING, "insert id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed);
    else elog(DEBUG1, "insert id = %li", DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

static void task_update(Task *t) {
    Datum values[] = {Int32GetDatum(t->shared->hash)};
    Portal portal;
    static Oid argtypes[] = {INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "hash = %i", t->shared->hash);
    set_ps_display_my("update");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "state" = 'PLAN'::%2$s AND "hash" = $1 AND "max" < 0 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET "plan" = CASE WHEN "drift" THEN CURRENT_TIMESTAMP ELSE "plan" END + concat_ws(' ', (-"max")::text, 'msec')::interval FROM s
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work.schema_table, work.schema_type);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    portal = SPI_cursor_open_my(src.data, plan, values, NULL);
    do {
        SPI_cursor_fetch(portal, true, task_default_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) elog(DEBUG1, "row = %lu, update id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    } while (SPI_processed);
    SPI_cursor_close(portal);
    set_ps_display_my("idle");
}

bool task_done(Task *t) {
    bool delete = false, exit = true, insert = false, update = false;
    char nulls[] = {' ', t->output.data ? ' ' : 'n', t->error.data ? ' ' : 'n'};
    Datum values[] = {Int64GetDatum(t->shared->id), CStringGetTextDatumMy(t->output.data), CStringGetTextDatumMy(t->error.data)};
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, output = %s, error = %s", t->shared->id, t->output.data ? t->output.data : default_null, t->error.data ? t->error.data : default_null);
    set_ps_display_my("done");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "id" = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET "state" = 'DONE'::%2$s, "stop" = CURRENT_TIMESTAMP, "output" = $2, "error" = $3 FROM s
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING "delete" AND "output" IS NULL AS "delete", "repeat" > '0 sec' AS "insert", "max" >= 0 AND ("count" > 0 OR "live" > '0 sec') AS "live", "max" < 0 AS "update"
        ), work.schema_table, work.schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) elog(WARNING, "id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed); else {
        delete = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delete", false));
        exit = !DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "live", false));
        insert = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "insert", false));
        update = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "update", false));
        elog(DEBUG1, "delete = %s, exit = %s, insert = %s, update = %s", delete ? "true" : "false", exit ? "true" : "false", insert ? "true" : "false", update ? "true" : "false");
    }
    if (values[1]) pfree((void *)values[1]);
    if (values[2]) pfree((void *)values[2]);
    if (insert) task_insert(t);
    if (delete) task_delete(t);
    if (update) task_update(t);
    if (t->lock && !unlock_table_id(work.shared->oid, t->shared->id)) { elog(WARNING, "!unlock_table_id(%i, %li)", work.shared->oid, t->shared->id); exit = true; }
    t->lock = false;
    exit = exit || task_live(t);
    SPI_finish_my();
    task_free(t);
    set_ps_display_my("idle");
    return ShutdownRequestPending || exit;
}

bool task_work(Task *t) {
    bool exit = false;
    Datum values[] = {Int64GetDatum(t->shared->id), Int32GetDatum(t->pid)};
    static Oid argtypes[] = {INT8OID, INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return true;
    if (!lock_table_id(work.shared->oid, t->shared->id)) { elog(WARNING, "!lock_table_id(%i, %li)", work.shared->oid, t->shared->id); return true; }
    t->lock = true;
    t->count++;
    elog(DEBUG1, "id = %li, max = %i, oid = %i, count = %i, pid = %i", t->shared->id, t->shared->max, work.shared->oid, t->count, t->pid);
    set_ps_display_my("work");
    if (!t->conn) {
        StringInfoData id;
        initStringInfoMy(&id);
        appendStringInfo(&id, "%li", t->shared->id);
        set_config_option_my("pg_task.id", id.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
        pfree(id.data);
    }
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT "id" FROM %1$s AS t
                WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND "id" = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET "state" = 'WORK'::%2$s, "start" = CURRENT_TIMESTAMP, "pid" = $2 FROM s
            WHERE "plan" BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING "group", "hash", "input", EXTRACT(epoch FROM "timeout")::integer * 1000 AS "timeout", "header", "string", "null", "delimiter", "quote", "escape", "plan" + "active" > CURRENT_TIMESTAMP AS "active", "remote"
        ), work.schema_table, work.schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) {
        elog(WARNING, "id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed);
        exit = true;
    } else {
        t->active = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "active", false));
        t->delimiter = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delimiter", false));
        t->escape = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "escape", false));
        t->group = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "group", false));
        t->shared->hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "hash", false));
        t->header = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "header", false));
        t->input = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "input", false));
        t->null = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "null", false));
        t->quote = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "quote", false));
        t->remote = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "remote", true));
        t->string = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "string", false));
        t->timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "timeout", false));
        if (0 < StatementTimeout && StatementTimeout < t->timeout) t->timeout = StatementTimeout;
        elog(DEBUG1, "group = %s, remote = %s, hash = %i, input = %s, timeout = %i, header = %s, string = %s, null = %s, delimiter = %c, quote = %c, escape = %c, active = %s", t->group, t->remote ? t->remote : default_null, t->shared->hash, t->input, t->timeout, t->header ? "true" : "false", t->string ? "true" : "false", t->null, t->delimiter, t->quote ? t->quote : 30, t->escape ? t->escape : 30, t->active ? "true" : "false");
        if (!t->remote) set_config_option_my("pg_task.group", t->group, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
    return exit;
}

void task_error(ErrorData *edata) {
    if ((emit_log_hook = emit_log_hook_prev)) (*emit_log_hook)(edata);
    if (!task.error.data) initStringInfoMy(&task.error);
    if (!task.output.data) initStringInfoMy(&task.output);
    if (task.remote && edata->elevel == WARNING) edata->elevel = ERROR;
    appendStringInfo(&task.output, SQL(%sROLLBACK), task.output.len ? "\n" : "");
    task.skip++;
    if (task.error.len) appendStringInfoChar(&task.error, '\n');
    appendStringInfo(&task.error, "%s:  ", _(error_severity(edata->elevel)));
    if (Log_error_verbosity >= PGERROR_VERBOSE) appendStringInfo(&task.error, "%s: ", unpack_sql_state(edata->sqlerrcode));
    if (edata->message) append_with_tabs(&task.error, edata->message);
    else append_with_tabs(&task.error, _("missing error text"));
    if (edata->cursorpos > 0) appendStringInfo(&task.error, _(" at character %d"), edata->cursorpos);
    else if (edata->internalpos > 0) appendStringInfo(&task.error, _(" at character %d"), edata->internalpos);
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        if (edata->detail_log) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("DETAIL:  "));
            append_with_tabs(&task.error, edata->detail_log);
        } else if (edata->detail) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("DETAIL:  "));
            append_with_tabs(&task.error, edata->detail);
        }
        if (edata->hint) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("HINT:  "));
            append_with_tabs(&task.error, edata->hint);
        }
        if (edata->internalquery) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("QUERY:  "));
            append_with_tabs(&task.error, edata->internalquery);
        }
        if (edata->context
#if PG_VERSION_NUM >= 90500
            && !edata->hide_ctx
#endif
        ) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("CONTEXT:  "));
            append_with_tabs(&task.error, edata->context);
        }
        if (Log_error_verbosity >= PGERROR_VERBOSE) {
            if (edata->funcname && edata->filename) { // assume no newlines in funcname or filename...
                if (task.error.len) appendStringInfoChar(&task.error, '\n');
                appendStringInfo(&task.error, _("LOCATION:  %s, %s:%d"), edata->funcname, edata->filename, edata->lineno);
            } else if (edata->filename) {
                if (task.error.len) appendStringInfoChar(&task.error, '\n');
                appendStringInfo(&task.error, _("LOCATION:  %s:%d"), edata->filename, edata->lineno);
            }
        }
#if PG_VERSION_NUM >= 130000
        if (edata->backtrace) {
            if (task.error.len) appendStringInfoChar(&task.error, '\n');
            appendStringInfoString(&task.error, _("BACKTRACE:  "));
            append_with_tabs(&task.error, edata->backtrace);
        }
#endif
    }
    if (task.input && is_log_level_output(edata->elevel, log_min_error_statement) && !edata->hide_stmt) { // If the user wants the query that generated this error logged, do it.
        if (task.error.len) appendStringInfoChar(&task.error, '\n');
        appendStringInfoString(&task.error, _("STATEMENT:  "));
        append_with_tabs(&task.error, task.input);
    }
}


static void task_execute(void) {
    int StatementTimeoutMy = StatementTimeout;
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(MessageContext);
    MemoryContextResetAndDeleteChildren(MessageContext);
    InvalidateCatalogSnapshotConditionally();
    MemoryContextSwitchTo(oldMemoryContext);
    whereToSendOutput = DestDebug;
    ReadyForQueryMy(whereToSendOutput);
    SetCurrentStatementStartTimestamp();
    StatementTimeout = task.timeout;
    exec_simple_query_my(task.input);
    if (IsTransactionState()) exec_simple_query_my(SQL(COMMIT));
    if (IsTransactionState()) ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("still active sql transaction")));
    StatementTimeout = StatementTimeoutMy;
}

static void task_proc_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
    if (!code) return;
#ifdef HAVE_SETSID
    if (kill(-MyBgworkerEntry->bgw_notify_pid, SIGHUP))
#else
    if (kill(MyBgworkerEntry->bgw_notify_pid, SIGHUP))
#endif
        ereport(WARNING, (errmsg("could not send signal to process %d: %m", MyBgworkerEntry->bgw_notify_pid)));
}

static void task_catch(void) {
    HOLD_INTERRUPTS();
    disable_all_timeouts(false);
    QueryCancelPending = false;
    emit_log_hook_prev = emit_log_hook;
    emit_log_hook = task_error;
    EmitErrorReport();
    debug_query_string = NULL;
    AbortOutOfAnyTransaction();
#if PG_VERSION_NUM >= 110000
    PortalErrorCleanup();
    SPICleanup();
#endif
    if (MyReplicationSlot) ReplicationSlotRelease();
#if PG_VERSION_NUM >= 100000
    ReplicationSlotCleanup();
#endif
#if PG_VERSION_NUM >= 110000
    jit_reset_after_error();
#endif
    MemoryContextSwitchTo(TopMemoryContext);
    FlushErrorState();
    xact_started = false;
    RESUME_INTERRUPTS();
}

static void task_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static bool task_timeout(void) {
    if (task_work(&task)) return true;
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task.shared->id, task.timeout, task.input, task.count);
    set_ps_display_my("timeout");
    PG_TRY();
        if (!task.active) ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task not active")));
        task_execute();
    PG_CATCH();
        task_catch();
    PG_END_TRY();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    set_ps_display_my("idle");
    return task_done(&task);
}

void task_free(Task *t) {
    if (t->error.data) { pfree(t->error.data); t->error.data = NULL; }
    if (t->group) { pfree(t->group); t->group = NULL; }
    if (t->input) { pfree(t->input); t->input = NULL; }
    if (t->null) { pfree(t->null); t->null = NULL; }
    if (t->output.data) { pfree(t->output.data); t->output.data = NULL; }
    if (t->remote) { pfree(t->remote); t->remote = NULL; }
}

void task_main(Datum arg) {
    dsm_segment *seg;
    shm_toc *toc;
    StringInfoData oid, schema_table, schema_type;
    on_proc_exit(task_proc_exit, (Datum)NULL);
    BackgroundWorkerUnblockSignals();
    if (!(seg = dsm_attach(DatumGetUInt32(arg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment")));
    if (!(toc = shm_toc_attach(PG_TASK_MAGIC, dsm_segment_address(seg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("bad magic number in dynamic shared memory segment")));
    task.shared = shm_toc_lookup_my(toc, 0, false);
    if (!(seg = dsm_attach(task.shared->handle))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment")));
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
    elog(DEBUG1, "oid = %i, id = %li, hash = %i, max = %i", work.shared->oid, task.shared->id, task.shared->hash, task.shared->max);
    set_config_option_my("pg_task.data", work.shared->data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.schema", work.shared->schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.table", work.shared->table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.user", work.shared->user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work.schema, work.table);
    work.schema_table = schema_table.data;
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work.schema);
    work.schema_type = schema_type.data;
    initStringInfoMy(&oid);
    appendStringInfo(&oid, "%i", work.shared->oid);
    set_config_option_my("pg_task.oid", oid.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(oid.data);
    task.pid = MyProcPid;
    task.start = GetCurrentTimestamp();
    set_ps_display_my("idle");
    if (!lock_table_pid_hash(work.shared->oid, task.pid, task.shared->hash)) { elog(WARNING, "!lock_table_pid_hash(%i, %i, %i)", work.shared->oid, task.pid, task.shared->hash); return; }
    while (!ShutdownRequestPending) {
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
        if (rc & WL_TIMEOUT) if (task_timeout()) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) task_latch();
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
    }
    if (!unlock_table_pid_hash(work.shared->oid, task.pid, task.shared->hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work.shared->oid, task.pid, task.shared->hash);
}
