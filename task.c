#include "include.h"

extern bool xact_started;
extern char *default_null;
extern Work *work;
static emit_log_hook_type emit_log_hook_prev = NULL;
Task *task;

static bool task_live(Task *task) {
    Datum values[] = {Int32GetDatum(task->shared.hash), Int32GetDatum(task->shared.max), Int32GetDatum(task->count), TimestampTzGetDatum(task->start)};
    static Oid argtypes[] = {INT4OID, INT4OID, INT4OID, TIMESTAMPTZOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, hash = %i, max = %i, count = %i, start = %s", task->shared.id, task->shared.hash, task->shared.max, task->count, timestamptz_to_str(task->start));
    set_ps_display_my("live");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND state = 'PLAN'::%2$s AND hash = $1 AND max >= $2 AND CASE
                    WHEN count > 0 AND live > '0 sec' THEN count > $3 AND $4 + live > CURRENT_TIMESTAMP ELSE count > $3 OR $4 + live > CURRENT_TIMESTAMP
                END ORDER BY max DESC, id LIMIT 1 FOR UPDATE OF t %3$s
            ) UPDATE %1$s AS t SET state = 'TAKE'::%2$s FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work->schema_table, work->schema_type,
#if PG_VERSION_NUM >= 90500
        "SKIP LOCKED"
#else
        ""
#endif
        );
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    task->shared.id = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false)) : 0;
    elog(DEBUG1, "id = %li", task->shared.id);
    set_ps_display_my("idle");
    return ShutdownRequestPending || !task->shared.id;
}

static void task_delete(Task *task) {
    Datum values[] = {Int64GetDatum(task->shared.id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", task->shared.id);
    set_ps_display_my("delete");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) DELETE FROM %1$s AS t WHERE
            plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND id = $1 RETURNING t.id
        ), work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_DELETE_RETURNING);
    for (uint64 row = 0; row < SPI_processed; row++) elog(DEBUG1, "row = %lu, delete id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

static void task_insert(Task *task) {
    Datum values[] = {Int64GetDatum(task->shared.id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", task->shared.id);
    set_ps_display_my("insert");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT * FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) INSERT INTO %1$s AS t (parent, plan, "group", max, input, timeout, delete, repeat, drift, count, live, remote)
            SELECT id, CASE
                WHEN drift THEN CURRENT_TIMESTAMP + repeat
                ELSE (WITH RECURSIVE r AS (SELECT plan AS p UNION SELECT p + repeat FROM r WHERE p <= CURRENT_TIMESTAMP) SELECT * FROM r ORDER BY 1 DESC LIMIT 1)
            END AS plan, "group", max, input, timeout, delete, repeat, drift, count, live, remote FROM s WHERE repeat > '0 sec' LIMIT 1 RETURNING t.id
        ), work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_INSERT_RETURNING);
    for (uint64 row = 0; row < SPI_processed; row++) elog(DEBUG1, "row = %lu, insert id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

static void task_update(Task *task) {
    Datum values[] = {Int32GetDatum(task->shared.hash)};
    static Oid argtypes[] = {INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "hash = %i", task->shared.hash);
    set_ps_display_my("update");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND state = 'PLAN'::%2$s AND hash = $1 AND max < 0 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET plan = CASE WHEN drift THEN CURRENT_TIMESTAMP ELSE plan END + concat_ws(' ', (-max)::text, 'msec')::interval FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work->schema_table, work->schema_type);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    for (uint64 row = 0; row < SPI_processed; row++) elog(DEBUG1, "row = %lu, update id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

bool task_done(Task *task) {
    bool delete = false, exit = true, insert = false, update = false;
    char nulls[] = {' ', task->output.data ? ' ' : 'n', task->error.data ? ' ' : 'n'};
    Datum values[] = {Int64GetDatum(task->shared.id), CStringGetTextDatumMy(task->output.data), CStringGetTextDatumMy(task->error.data)};
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, output = %s, error = %s", task->shared.id, task->output.data ? task->output.data : default_null, task->error.data ? task->error.data : default_null);
    set_ps_display_my("done");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET state = 'DONE'::%2$s, stop = CURRENT_TIMESTAMP, output = $2, error = $3 FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING delete AND output IS NULL AS delete, repeat > '0 sec' AS insert, max >= 0 AND (count > 0 OR live > '0 sec') AS live, max < 0 AS update
        ), work->schema_table, work->schema_type);
    }
    SPI_connect_my(src.data);
    SPI_start_transaction_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) elog(WARNING, "id = %li, SPI_processed %lu != 1", task->shared.id, (long)SPI_processed); else {
        delete = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delete", false));
        exit = !DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "live", false));
        insert = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "insert", false));
        update = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "update", false));
        elog(DEBUG1, "delete = %s, exit = %s, insert = %s, update = %s", delete ? "true" : "false", exit ? "true" : "false", insert ? "true" : "false", update ? "true" : "false");
    }
    if (values[1]) pfree((void *)values[1]);
    if (values[2]) pfree((void *)values[2]);
    if (insert) task_insert(task);
    if (delete) task_delete(task);
    if (update) task_update(task);
    if (task->lock && !unlock_table_id(work->shared->oid, task->shared.id)) { elog(WARNING, "!unlock_table_id(%i, %li)", work->shared->oid, task->shared.id); exit = true; }
    task->lock = false;
    exit = exit || task_live(task);
    SPI_commit_my();
    SPI_finish_my();
    task_free(task);
    set_ps_display_my("idle");
    return ShutdownRequestPending || exit;
}

bool task_work(Task *task) {
    bool exit = false;
    Datum values[] = {Int64GetDatum(task->shared.id), Int32GetDatum(task->pid)};
    static Oid argtypes[] = {INT8OID, INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return true;
    if (!lock_table_id(work->shared->oid, task->shared.id)) { elog(WARNING, "!lock_table_id(%i, %li)", work->shared->oid, task->shared.id); return true; }
    task->lock = true;
    task->count++;
    elog(DEBUG1, "id = %li, max = %i, oid = %i, count = %i, pid = %i", task->shared.id, task->shared.max, work->shared->oid, task->count, task->pid);
    set_ps_display_my("work");
    if (!task->conn) {
        StringInfoData id;
        initStringInfoMy(&id);
        appendStringInfo(&id, "%li", task->shared.id);
        set_config_option_my("pg_task.id", id.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
        pfree(id.data);
    }
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET state = 'WORK'::%2$s, start = CURRENT_TIMESTAMP, pid = $2 FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active')::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING "group", hash, input, EXTRACT(epoch FROM timeout)::integer * 1000 AS timeout, header, string, "null", delimiter, quote, escape, plan + active > CURRENT_TIMESTAMP AS active, remote
        ), work->schema_table, work->schema_type);
    }
    SPI_connect_my(src.data);
    SPI_start_transaction_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) {
        elog(WARNING, "id = %li, SPI_processed %lu != 1", task->shared.id, (long)SPI_processed);
        exit = true;
    } else {
        task->active = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "active", false));
        task->delimiter = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delimiter", false));
        task->escape = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "escape", true));
        task->group = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "group", false));
        task->shared.hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "hash", false));
        task->header = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "header", false));
        task->input = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "input", false));
        task->null = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "null", false));
        task->quote = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "quote", true));
        task->remote = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "remote", true));
        task->string = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "string", false));
        task->timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "timeout", false));
        if (0 < StatementTimeout && StatementTimeout < task->timeout) task->timeout = StatementTimeout;
        elog(DEBUG1, "group = %s, remote = %s, hash = %i, input = %s, timeout = %i, header = %s, string = %s, null = %s, delimiter = %c, quote = %c, escape = %c, active = %s", task->group, task->remote ? task->remote : default_null, task->shared.hash, task->input, task->timeout, task->header ? "true" : "false", task->string ? "true" : "false", task->null, task->delimiter, task->quote ? task->quote : 30, task->escape ? task->escape : 30, task->active ? "true" : "false");
        if (!task->remote) set_config_option_my("pg_task.group", task->group, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    }
    SPI_commit_my();
    SPI_finish_my();
    set_ps_display_my("idle");
    return exit;
}

void task_error(ErrorData *edata) {
    if ((emit_log_hook = emit_log_hook_prev)) (*emit_log_hook)(edata);
    if (!task->error.data) initStringInfoMy(&task->error);
    if (!task->output.data) initStringInfoMy(&task->output);
    if (task->remote && edata->elevel == WARNING) edata->elevel = ERROR;
    appendStringInfo(&task->output, SQL(%sROLLBACK), task->output.len ? "\n" : "");
    task->skip++;
    if (task->error.len) appendStringInfoChar(&task->error, '\n');
    appendStringInfo(&task->error, "%s:  ", _(error_severity(edata->elevel)));
    if (Log_error_verbosity >= PGERROR_VERBOSE) appendStringInfo(&task->error, "%s: ", unpack_sql_state(edata->sqlerrcode));
    if (edata->message) append_with_tabs(&task->error, edata->message);
    else append_with_tabs(&task->error, _("missing error text"));
    if (edata->cursorpos > 0) appendStringInfo(&task->error, _(" at character %d"), edata->cursorpos);
    else if (edata->internalpos > 0) appendStringInfo(&task->error, _(" at character %d"), edata->internalpos);
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        if (edata->detail_log) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("DETAIL:  "));
            append_with_tabs(&task->error, edata->detail_log);
        } else if (edata->detail) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("DETAIL:  "));
            append_with_tabs(&task->error, edata->detail);
        }
        if (edata->hint) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("HINT:  "));
            append_with_tabs(&task->error, edata->hint);
        }
        if (edata->internalquery) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("QUERY:  "));
            append_with_tabs(&task->error, edata->internalquery);
        }
        if (edata->context
#if PG_VERSION_NUM >= 90500
            && !edata->hide_ctx
#endif
        ) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("CONTEXT:  "));
            append_with_tabs(&task->error, edata->context);
        }
        if (Log_error_verbosity >= PGERROR_VERBOSE) {
            if (edata->funcname && edata->filename) { // assume no newlines in funcname or filename...
                if (task->error.len) appendStringInfoChar(&task->error, '\n');
                appendStringInfo(&task->error, _("LOCATION:  %s, %s:%d"), edata->funcname, edata->filename, edata->lineno);
            } else if (edata->filename) {
                if (task->error.len) appendStringInfoChar(&task->error, '\n');
                appendStringInfo(&task->error, _("LOCATION:  %s:%d"), edata->filename, edata->lineno);
            }
        }
#if PG_VERSION_NUM >= 130000
        if (edata->backtrace) {
            if (task->error.len) appendStringInfoChar(&task->error, '\n');
            appendStringInfoString(&task->error, _("BACKTRACE:  "));
            append_with_tabs(&task->error, edata->backtrace);
        }
#endif
    }
    if (is_log_level_output(edata->elevel, log_min_error_statement) && !edata->hide_stmt) { // If the user wants the query that generated this error logged, do it.
        if (task->error.len) appendStringInfoChar(&task->error, '\n');
        appendStringInfoString(&task->error, _("STATEMENT:  "));
        append_with_tabs(&task->error, task->input);
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
    StatementTimeout = task->timeout;
    exec_simple_query_my(task->input);
    if (IsTransactionState()) exec_simple_query_my(SQL(COMMIT));
    if (IsTransactionState()) ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("still active sql transaction")));
    StatementTimeout = StatementTimeoutMy;
}

static void task_proc_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i, id = %li", code, task->shared.id);
    if ((dsm_segment *)arg) dsm_detach((dsm_segment *)arg);
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
    if (task_work(task)) return true;
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task->shared.id, task->timeout, task->input, task->count);
    set_ps_display_my("timeout");
    PG_TRY();
        if (!task->active) ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task not active")));
        task_execute();
    PG_CATCH();
        task_catch();
    PG_END_TRY();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    set_ps_display_my("idle");
    return task_done(task);
}

void task_free(Task *task) {
    if (task->error.data) { pfree(task->error.data); task->error.data = NULL; }
    if (task->group) { pfree(task->group); task->group = NULL; }
    if (task->input) { pfree(task->input); task->input = NULL; }
    if (task->null) { pfree(task->null); task->null = NULL; }
    if (task->output.data) { pfree(task->output.data); task->output.data = NULL; }
    if (task->remote) { pfree(task->remote); task->remote = NULL; }
}

void task_main(Datum arg) {
    dsm_segment *seg = NULL;
    shm_toc *toc;
    StringInfoData oid, schema_table, schema_type;
    BackgroundWorkerUnblockSignals();
    CreateAuxProcessResourceOwner();
    task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task));
    on_proc_exit(task_proc_exit, (Datum)seg);
    work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
    if (!(seg = dsm_attach(DatumGetUInt32(arg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment")));
    if (!(toc = shm_toc_attach(PG_TASK_MAGIC, dsm_segment_address(seg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("bad magic number in dynamic shared memory segment")));
    task->shared = *(typeof(task->shared) *)shm_toc_lookup_my(toc, 0, false);
    if (!(seg = dsm_attach(task->shared.handle))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unable to map dynamic shared memory segment")));
    if (!(toc = shm_toc_attach(PG_WORK_MAGIC, dsm_segment_address(seg)))) ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("bad magic number in dynamic shared memory segment")));
    work->shared = shm_toc_lookup_my(toc, 0, false);
    BackgroundWorkerInitializeConnectionMy(work->shared->data.str, work->shared->user.str, 0);
    set_config_option_my("application_name", MyBgworkerEntry->bgw_name + strlen(work->shared->user.str) + 1 + strlen(work->shared->data.str) + 1, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pgstat_report_appname(MyBgworkerEntry->bgw_name + strlen(work->shared->user.str) + 1 + strlen(work->shared->data.str) + 1);
    set_ps_display_my("main");
    process_session_preload_libraries();
    elog(DEBUG1, "oid = %i, id = %li, hash = %i, max = %i", work->shared->oid, task->shared.id, task->shared.hash, task->shared.max);
    set_config_option_my("pg_task.data", work->shared->data.str, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.schema", work->shared->schema.str, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.table", work->shared->table.str, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option_my("pg_task.user", work->shared->user.str, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", work->shared->schema.quote, work->shared->table.quote);
    work->schema_table = schema_table.data;
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state", work->shared->schema.quote);
    work->schema_type = schema_type.data;
    initStringInfoMy(&oid);
    appendStringInfo(&oid, "%i", work->shared->oid);
    set_config_option_my("pg_task.oid", oid.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(oid.data);
    task->pid = MyProcPid;
    task->start = GetCurrentTimestamp();
    set_ps_display_my("idle");
    if (!lock_table_pid_hash(work->shared->oid, task->pid, task->shared.hash)) { elog(WARNING, "!lock_table_pid_hash(%i, %i, %i)", work->shared->oid, task->pid, task->shared.hash); return; }
    while (!ShutdownRequestPending) {
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
        if (rc & WL_TIMEOUT) if (task_timeout()) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) task_latch();
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
    }
    if (!unlock_table_pid_hash(work->shared->oid, task->pid ? task->pid : MyProcPid, task->shared.hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work->shared->oid, task->pid ? task->pid : MyProcPid, task->shared.hash);
}
