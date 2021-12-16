#include "include.h"

extern bool xact_started;
extern char *default_null;
extern Work *work;
static emit_log_hook_type emit_log_hook_prev = NULL;
Task *task;

static bool task_live(Task *task) {
    Datum values[] = {Int32GetDatum(task->hash), Int32GetDatum(task->max), Int32GetDatum(task->count), TimestampTzGetDatum(task->start)};
    static Oid argtypes[] = {INT4OID, INT4OID, INT4OID, TIMESTAMPTZOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, hash = %i, max = %i, count = %i, start = %s", task->id, task->hash, task->max, task->count, timestamptz_to_str(task->start));
    set_ps_display_my("live");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND state = 'PLAN'::%2$s AND hash = $1 AND max >= $2 AND CASE
                    WHEN count > 0 AND live > '0 sec' THEN count > $3 AND $4 + live > CURRENT_TIMESTAMP ELSE count > $3 OR $4 + live > CURRENT_TIMESTAMP
                END ORDER BY max DESC, id LIMIT 1 FOR UPDATE OF t SKIP LOCKED
            ) UPDATE %1$s AS t SET state = 'TAKE'::%2$s FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.id = s.id RETURNING t.id
        ), work->schema_table, work->schema_type);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, false);
    task->id = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false)) : 0;
    elog(DEBUG1, "id = %li", task->id);
    set_ps_display_my("idle");
    return ShutdownRequestPending || !task->id;
}

static void task_delete(Task *task) {
    Datum values[] = {Int64GetDatum(task->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", task->id);
    set_ps_display_my("delete");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) DELETE FROM %1$s AS t WHERE
            plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND id = $1 RETURNING t.id
        ), work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_DELETE_RETURNING, false);
    for (uint64 row = 0; row < SPI_processed; row++) elog(WARNING, "row = %lu, delete id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

static void task_insert(Task *task) {
    Datum values[] = {Int64GetDatum(task->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", task->id);
    set_ps_display_my("insert");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT * FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) INSERT INTO %1$s AS t (parent, plan, "group", max, input, timeout, delete, repeat, drift, count, live, remote)
            SELECT id, CASE
                WHEN drift THEN CURRENT_TIMESTAMP + repeat
                ELSE (WITH RECURSIVE r AS (SELECT plan AS p UNION SELECT p + repeat FROM r WHERE p <= CURRENT_TIMESTAMP) SELECT * FROM r ORDER BY 1 DESC LIMIT 1)
            END AS plan, "group", max, input, timeout, delete, repeat, drift, count, live, remote FROM s WHERE repeat > '0 sec' LIMIT 1 RETURNING t.id
        ), work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_INSERT_RETURNING, false);
    for (uint64 row = 0; row < SPI_processed; row++) elog(WARNING, "row = %lu, insert id = %li", row, DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "id", false)));
    set_ps_display_my("idle");
}

bool task_done(Task *task) {
    bool delete = false, exit = true, insert = false;
    char nulls[] = {' ', task->output.data ? ' ' : 'n', task->error.data ? ' ' : 'n'};
    Datum values[] = {Int64GetDatum(task->id), CStringGetTextDatumMy(TopMemoryContext, task->output.data), CStringGetTextDatumMy(TopMemoryContext, task->error.data)};
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, output = %s, error = %s", task->id, task->output.data ? task->output.data : default_null, task->error.data ? task->error.data : default_null);
    set_ps_display_my("done");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET state = 'DONE'::%2$s, stop = CURRENT_TIMESTAMP, output = $2, error = $3 FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING delete AND output IS NULL AS delete, repeat > '0 sec' AS insert, count > 0 OR live > '0 sec' AS live
        ), work->schema_table, work->schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING, false);
    if (SPI_processed != 1) elog(WARNING, "id = %li, SPI_processed %lu != 1", SPI_processed, task->id); else {
        delete = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delete", false));
        exit = !DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "live", false));
        insert = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "insert", false));
        elog(DEBUG1, "delete = %s, exit = %s, insert = %s", delete ? "true" : "false", exit ? "true" : "false", insert ? "true" : "false");
    }
    if (values[1]) pfree((void *)values[1]);
    if (values[2]) pfree((void *)values[2]);
    if (insert) task_insert(task);
    if (delete) task_delete(task);
    if (task->lock && !unlock_table_id(work->oid.table, task->id)) { elog(WARNING, "!unlock_table_id(%i, %li)", work->oid.table, task->id); exit = true; }
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
    Datum values[] = {Int64GetDatum(task->id), Int32GetDatum(task->pid)};
    static Oid argtypes[] = {INT8OID, INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return true;
    if (!lock_table_id(work->oid.table, task->id)) { elog(WARNING, "!lock_table_id(%i, %li)", work->oid.table, task->id); return true; }
    task->lock = true;
    task->count++;
    elog(DEBUG1, "id = %li, max = %i, oid = %i, count = %i, pid = %i", task->id, task->max, work->oid.table, task->count, task->pid);
    set_ps_display_my("work");
    if (!task->conn) {
        StringInfoData id;
        initStringInfoMy(TopMemoryContext, &id);
        appendStringInfo(&id, "%li", task->id);
        set_config_option("pg_task.id", id.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
        pfree(id.data);
    }
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, SQL(
            WITH s AS (
                SELECT id FROM %1$s AS t
                WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND id = $1 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET state = 'WORK'::%2$s, start = CURRENT_TIMESTAMP, pid = $2 FROM s
            WHERE plan BETWEEN CURRENT_TIMESTAMP - current_setting('pg_work.default_active', false)::interval AND CURRENT_TIMESTAMP AND t.id = s.id
            RETURNING "group", hash, input, EXTRACT(epoch FROM timeout)::integer * 1000 AS timeout, header, string, "null", delimiter, quote, escape, plan + active > CURRENT_TIMESTAMP AS active, remote
        ), work->schema_table, work->schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    if (SPI_processed != 1) {
        elog(WARNING, "id = %li, SPI_processed %lu != 1", SPI_processed, task->id);
        exit = true;
    } else {
        task->active = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "active", false));
        task->delimiter = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delimiter", false));
        task->escape = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "escape", true));
        task->group = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "group", false));
        task->hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "hash", false));
        task->header = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "header", false));
        task->input = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "input", false));
        task->null = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "null", false));
        task->quote = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "quote", true));
        task->remote = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "remote", true));
        task->string = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "string", false));
        task->timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "timeout", false));
        if (0 < StatementTimeout && StatementTimeout < task->timeout) task->timeout = StatementTimeout;
        elog(DEBUG1, "group = %s, hash = %i, input = %s, timeout = %i, header = %s, string = %s, null = %s, delimiter = %c, quote = %c, escape = %c, active = %s", task->group, task->hash, task->input, task->timeout, task->header ? "true" : "false", task->string ? "true" : "false", task->null, task->delimiter, task->quote ? task->quote : 30, task->escape ? task->escape : 30, task->active ? "true" : "false");
    }
    SPI_finish_my();
    set_ps_display_my("idle");
    return exit;
}

void task_error(ErrorData *edata) {
    if ((emit_log_hook = emit_log_hook_prev)) (*emit_log_hook)(edata);
    if (!task->error.data) initStringInfoMy(TopMemoryContext, &task->error);
    if (!task->output.data) initStringInfoMy(TopMemoryContext, &task->output);
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
        if (edata->context && !edata->hide_ctx) {
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

static void task_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i, id = %li", code, task->id);
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

static void task_init(void) {
    char *p = MyBgworkerEntry->bgw_extra;
    MemoryContext oldcontext = CurrentMemoryContext;
    StringInfoData oid, schema_table, schema_type;
    emit_log_hook_prev = emit_log_hook;
    task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task));
    on_proc_exit(task_exit, (Datum)task);
    work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
#define X(name, serialize, deserialize) deserialize(name);
    TASK
#undef X
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
    if (!(work->str.data = get_database_name(work->oid.data))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("database %u does not exist", work->oid.data)));
    if (!(work->str.schema = get_namespace_name(work->oid.schema))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("schema %u does not exist", work->oid.schema)));
    if (!(work->str.table = get_rel_name(work->oid.table))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("table %u does not exist", work->oid.table)));
    if (!(work->str.user = GetUserNameFromId(work->oid.user, true))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("user %u does not exist", work->oid.user)));
    CommitTransactionCommand();
    MemoryContextSwitchTo(oldcontext);
    work->quote.data = (char *)quote_identifier(work->str.data);
    work->quote.schema = (char *)quote_identifier(work->str.schema);
    work->quote.table = (char *)quote_identifier(work->str.table);
    work->quote.user = (char *)quote_identifier(work->str.user);
    pgstat_report_appname(MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1);
    task->id = DatumGetInt64(MyBgworkerEntry->bgw_main_arg);
    set_config_option("application_name", MyBgworkerEntry->bgw_name + strlen(work->str.user) + 1 + strlen(work->str.data) + 1, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.data", work->str.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.group", task->group, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.schema", work->str.schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.table", work->str.table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    set_config_option("pg_task.user", work->str.user, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    elog(DEBUG1, "oid = %i, id = %li, hash = %i, group = %s, max = %i", work->oid.table, task->id, task->hash, task->group, task->max);
    initStringInfoMy(TopMemoryContext, &schema_table);
    appendStringInfo(&schema_table, "%s.%s", work->quote.schema, work->quote.table);
    work->schema_table = schema_table.data;
    initStringInfoMy(TopMemoryContext, &schema_type);
    appendStringInfo(&schema_type, "%s.state", work->quote.schema);
    work->schema_type = schema_type.data;
    initStringInfoMy(TopMemoryContext, &oid);
    appendStringInfo(&oid, "%i", work->oid.table);
    set_config_option("pg_task.oid", oid.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pfree(oid.data);
    task->pid = MyProcPid;
    task->start = GetCurrentTimestamp();
    set_ps_display_my("idle");
}

static void task_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static bool task_timeout(void) {
    if (task_work(task)) return true;
    elog(DEBUG1, "id = %li, timeout = %i, input = %s, count = %i", task->id, task->timeout, task->input, task->count);
    set_ps_display_my("timeout");
    PG_TRY();
        if (!task->active) ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("task %li not active", task->id)));
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

void task_main(Datum main_arg) {
    task_init();
    if (!lock_table_pid_hash(work->oid.table, task->pid, task->hash)) { elog(WARNING, "!lock_table_pid_hash(%i, %i, %i)", work->oid.table, task->pid, task->hash); return; }
    while (!ShutdownRequestPending) {
#if PG_VERSION_NUM >= 100000
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
#else
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0);
#endif
        if (rc & WL_TIMEOUT) if (task_timeout()) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) task_latch();
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
    }
    if (!unlock_table_pid_hash(work->oid.table, task->pid ? task->pid : MyProcPid, task->hash)) elog(WARNING, "!unlock_table_pid_hash(%i, %i, %i)", work->oid.table, task->pid ? task->pid : MyProcPid, task->hash);
}
