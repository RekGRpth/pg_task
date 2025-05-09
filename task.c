#include "include.h"

#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>

#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif

#if PG_VERSION_NUM < 140000
#include <utils/timestamp.h>
#endif

static bool task_live(const Task *t) {
    Datum values[] = {Int32GetDatum(t->shared->hash), Int32GetDatum(t->shared->max), Int32GetDatum(t->count), TimestampTzGetDatum(t->start)};
    static Oid argtypes[] = {INT4OID, INT4OID, INT4OID, TIMESTAMPTZOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, hash = %i, max = %i, count = %i, start = %s", t->shared->id, t->shared->hash, t->shared->max, t->count, timestamptz_to_str(t->start));
    set_ps_display_my("live");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (SELECT "id" FROM %1$s AS t WHERE "plan" OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "state" OPERATOR(pg_catalog.=) 'PLAN' AND "hash" OPERATOR(pg_catalog.=) $1 AND "max" OPERATOR(pg_catalog.>=) $2 AND ("plan" OPERATOR(pg_catalog.+) "active" OPERATOR(pg_catalog.>) CURRENT_TIMESTAMP OR "repeat" OPERATOR(pg_catalog.>) '0 sec' OR "max" OPERATOR(pg_catalog.<) 0) AND CASE
                WHEN "count" OPERATOR(pg_catalog.>) 0 AND "live" OPERATOR(pg_catalog.>) '0 sec' THEN "count" OPERATOR(pg_catalog.>) $3 AND $4 OPERATOR(pg_catalog.+) "live" OPERATOR(pg_catalog.>) CURRENT_TIMESTAMP ELSE "count" OPERATOR(pg_catalog.>) $3 OR $4 OPERATOR(pg_catalog.+) "live" OPERATOR(pg_catalog.>) CURRENT_TIMESTAMP
            END ORDER BY "max" DESC, "id" LIMIT 1 FOR UPDATE OF t %2$s) UPDATE %1$s AS t SET "state" = 'TAKE' FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id RETURNING t.id::pg_catalog.int8
        ), t->work->schema_table,
#if PG_VERSION_NUM >= 90500 && !defined(GP_VERSION_NUM)
        "SKIP LOCKED"
#else
        ""
#endif
        );
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    t->shared->id = SPI_processed == 1 ? DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false, INT8OID)) : 0;
    elog(DEBUG1, "id = %li", t->shared->id);
    SPI_finish_my();
    set_ps_display_my("idle");
    return ShutdownRequestPending || !t->shared->id;
}

static const char *task_columns(const Task *t) {
    const char *columns = NULL;
    Datum values[] = {ObjectIdGetDatum(t->shared->oid)};
    static Oid argtypes[] = {OIDOID};
    static const char *src = SQL(
        SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname), ', ')::pg_catalog.text AS columns FROM pg_catalog.pg_attribute WHERE attrelid OPERATOR(pg_catalog.=) $1 AND attnum OPERATOR(pg_catalog.>) 0 AND NOT attisdropped AND attname OPERATOR(pg_catalog.<>) ALL(ARRAY['id', 'plan', 'parent', 'start', 'stop', 'hash', 'pid', 'state', 'error', 'output'])
    );
    SPI_execute_with_args_my(src, countof(argtypes), argtypes, values, NULL, SPI_OK_SELECT);
    if (SPI_processed != 1) ereport(WARNING, (errmsg("columns id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed))); else {
        columns = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "columns", false, TEXTOID));
        elog(DEBUG1, "columns id = %li, %s", t->shared->id, columns);
    }
    return columns;
}

static void task_delete(const Task *t) {
    Datum values[] = {Int64GetDatum(t->shared->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", t->shared->id);
    set_ps_display_my("delete");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (SELECT "id" FROM %1$s AS t WHERE "id" OPERATOR(pg_catalog.=) $1 FOR UPDATE OF t) DELETE FROM %1$s AS t WHERE "id" OPERATOR(pg_catalog.=) $1 RETURNING t.id::pg_catalog.int8
        ), t->work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, NULL, SPI_OK_DELETE_RETURNING);
    if (SPI_processed != 1) ereport(WARNING, (errmsg("delete id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed)));
    else elog(DEBUG1, "delete id = %li", DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false, INT8OID)));
    set_ps_display_my("idle");
}

static void task_insert(const Task *t) {
    Datum values[] = {Int64GetDatum(t->shared->id)};
    static Oid argtypes[] = {INT8OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li", t->shared->id);
    set_ps_display_my("insert");
    if (!src.data) {
        const char *columns = task_columns(t);
        if (!columns) return;
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (SELECT * FROM %1$s AS t WHERE "id" OPERATOR(pg_catalog.=) $1 FOR UPDATE OF t) INSERT INTO %1$s AS t ("parent", "plan", %2$s) SELECT "id", CASE
                WHEN "drift" THEN CURRENT_TIMESTAMP OPERATOR(pg_catalog.+) "repeat" ELSE (WITH RECURSIVE r AS (SELECT "plan" AS p UNION SELECT p OPERATOR(pg_catalog.+) "repeat" FROM r WHERE p OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP) SELECT * FROM r ORDER BY 1 DESC LIMIT 1)
            END AS "plan", %2$s FROM s WHERE "repeat" OPERATOR(pg_catalog.>) '0 sec' LIMIT 1 RETURNING t.id::pg_catalog.int8
        ), t->work->schema_table, columns);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, NULL, SPI_OK_INSERT_RETURNING);
    if (SPI_processed != 1) ereport(WARNING, (errmsg("insert id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed)));
    else elog(DEBUG1, "insert id = %li", DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false, INT8OID)));
    set_ps_display_my("idle");
}

static void task_update(const Task *t) {
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
                WHERE "plan" OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) "max")::pg_catalog.text, 'msec')::pg_catalog.interval OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP AND "state" OPERATOR(pg_catalog.=) 'PLAN' AND "hash" OPERATOR(pg_catalog.=) $1 AND "max" OPERATOR(pg_catalog.<) 0 FOR UPDATE OF t
            ) UPDATE %1$s AS t SET "plan" = CASE WHEN "drift" THEN CURRENT_TIMESTAMP ELSE (WITH RECURSIVE r AS (SELECT "plan" AS p UNION SELECT p OPERATOR(pg_catalog.+) pg_catalog.concat_ws(' ', (OPERATOR(pg_catalog.-) "max")::pg_catalog.text, 'msec')::pg_catalog.interval FROM r WHERE p OPERATOR(pg_catalog.<=) CURRENT_TIMESTAMP) SELECT * FROM r ORDER BY 1 DESC LIMIT 1) END FROM s
            WHERE t.id OPERATOR(pg_catalog.=) s.id RETURNING t.id::pg_catalog.int8
        ), t->work->schema_table);
    }
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    portal = SPI_cursor_open_my(src.data, plan, values, NULL, false);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, init_task_fetch());
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            elog(DEBUG1, "row = %lu, update id = %li", row, DatumGetInt64(SPI_getbinval_my(val, SPI_tuptable->tupdesc, "id", false, INT8OID)));
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    set_ps_display_my("idle");
}

bool task_done(Task *t) {
    bool delete = false, exit = true, insert = false, update = false;
    char nulls[] = {' ', t->output.data ? ' ' : 'n', t->error.data ? ' ' : 'n'};
    Datum values[] = {Int64GetDatum(t->shared->id), CStringGetTextDatumMy(t->output.data), CStringGetTextDatumMy(t->error.data)};
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    elog(DEBUG1, "id = %li, output = %s, error = %s", t->shared->id, t->output.data ? t->output.data : init_null(), t->error.data ? t->error.data : init_null());
    set_ps_display_my("done");
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (SELECT "id" FROM %1$s AS t WHERE "id" OPERATOR(pg_catalog.=) $1 FOR UPDATE OF t)
            UPDATE %1$s AS t SET "state" = CASE WHEN $3 IS NULL THEN 'DONE' ELSE 'FAIL' END::%2$s, "stop" = CURRENT_TIMESTAMP, "output" = $2, "error" = $3 FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id
            RETURNING ("delete" AND "output" IS NULL AND "error" IS NULL)::pg_catalog.bool AS "delete", ("repeat" OPERATOR(pg_catalog.>) '0 sec')::pg_catalog.bool AS "insert", ("max" OPERATOR(pg_catalog.>=) 0 AND ("count" OPERATOR(pg_catalog.>) 0 OR "live" OPERATOR(pg_catalog.>) '0 sec'))::pg_catalog.bool AS "live", ("max" OPERATOR(pg_catalog.<) 0)::pg_catalog.bool AS "update"
        ), t->work->schema_table, t->work->schema_type);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, nulls, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) ereport(WARNING, (errmsg("id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed))); else {
        delete = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delete", false, BOOLOID));
        exit = !DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "live", false, BOOLOID));
        insert = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "insert", false, BOOLOID));
        update = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "update", false, BOOLOID));
        elog(DEBUG1, "delete = %s, exit = %s, insert = %s, update = %s", delete ? "true" : "false", exit ? "true" : "false", insert ? "true" : "false", update ? "true" : "false");
    }
    if (values[1]) pfree((void *)values[1]);
    if (values[2]) pfree((void *)values[2]);
    if (insert) task_insert(t);
    if (delete) task_delete(t);
    if (update) task_update(t);
    if (t->lock && !unlock_table_id(t->shared->oid, t->shared->id)) { ereport(WARNING, (errmsg("!unlock_table_id(%i, %li)", t->shared->oid, t->shared->id))); exit = true; }
    t->lock = false;
    SPI_finish_my();
    task_free(t);
    set_ps_display_my("idle");
    return ShutdownRequestPending || exit || task_live(t);
}

bool task_work(Task *t) {
    bool exit = false;
    Datum values[] = {Int64GetDatum(t->shared->id), Int32GetDatum(t->pid)};
    static Oid argtypes[] = {INT8OID, INT4OID};
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    if (ShutdownRequestPending) return true;
    if (!lock_table_id(t->shared->oid, t->shared->id)) { ereport(WARNING, (errmsg("!lock_table_id(%i, %li)", t->shared->oid, t->shared->id))); return true; }
    t->lock = true;
    t->count++;
    elog(DEBUG1, "id = %li, max = %i, oid = %i, count = %i, pid = %i", t->shared->id, t->shared->max, t->shared->oid, t->count, t->pid);
    set_ps_display_my("work");
    if (!t->conn) {
        StringInfoData id;
        initStringInfoMy(&id);
        appendStringInfo(&id, "%li", t->shared->id);
        set_config_option_my("pg_task.id", id.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
        pfree(id.data);
    }
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH s AS (SELECT "id" FROM %1$s AS t WHERE "id" OPERATOR(pg_catalog.=) $1 FOR UPDATE OF t)
            UPDATE %1$s AS t SET "state" = 'WORK', "start" = CURRENT_TIMESTAMP, "pid" = $2 FROM s WHERE t.id OPERATOR(pg_catalog.=) s.id
            RETURNING "group"::pg_catalog.text, "hash"::pg_catalog.int4, "input"::pg_catalog.text, (EXTRACT(epoch FROM "timeout")::pg_catalog.int4 OPERATOR(pg_catalog.*) 1000)::pg_catalog.int4 AS "timeout", "header"::pg_catalog.bool, "string"::pg_catalog.bool, "null"::pg_catalog.text, "delimiter"::pg_catalog.char, "quote"::pg_catalog.char, "escape"::pg_catalog.char, "remote"::pg_catalog.text
        ), t->work->schema_table);
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, countof(argtypes), argtypes);
    SPI_execute_plan_my(src.data, plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) {
        ereport(WARNING, (errmsg("id = %li, SPI_processed %lu != 1", t->shared->id, (long)SPI_processed)));
        exit = true;
    } else {
        t->delimiter = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delimiter", false, CHAROID));
        t->escape = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "escape", false, CHAROID));
        t->group = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "group", false, TEXTOID));
        t->shared->hash = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "hash", false, INT4OID));
        t->header = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "header", false, BOOLOID));
        t->input = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "input", false, TEXTOID));
        t->null = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "null", false, TEXTOID));
        t->quote = DatumGetChar(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "quote", false, CHAROID));
        t->remote = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "remote", true, TEXTOID));
        t->string = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "string", false, BOOLOID));
        t->timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "timeout", false, INT4OID));
        if (0 < StatementTimeout && StatementTimeout < t->timeout) t->timeout = StatementTimeout;
        elog(DEBUG1, "group = %s, remote = %s, hash = %i, input = %s, timeout = %i, header = %s, string = %s, null = %s, delimiter = %c, quote = %c, escape = %c", t->group, t->remote ? t->remote : init_null(), t->shared->hash, t->input, t->timeout, t->header ? "true" : "false", t->string ? "true" : "false", t->null, t->delimiter, t->quote ? t->quote : 30, t->escape ? t->escape : 30);
        if (!t->remote) set_config_option_my("pg_task.group", t->group, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
    return exit;
}

void task_error(Task *t) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    ErrorData *edata = CopyErrorData();
    MemoryContextSwitchTo(oldMemoryContext);
    if (!t->error.data) initStringInfoMy(&t->error);
    if (!t->output.data) initStringInfoMy(&t->output);
    t->skip++;
    if (t->error.len) appendStringInfoChar(&t->error, '\n');
    appendStringInfo(&t->error, "%s:  ", _(error_severity(edata->elevel)));
    if (Log_error_verbosity >= PGERROR_VERBOSE) appendStringInfo(&t->error, "%s: ", unpack_sql_state(edata->sqlerrcode));
    if (edata->message) append_with_tabs(&t->error, edata->message);
    else append_with_tabs(&t->error, _("missing error text"));
    if (edata->cursorpos > 0) appendStringInfo(&t->error, _(" at character %d"), edata->cursorpos);
    else if (edata->internalpos > 0) appendStringInfo(&t->error, _(" at character %d"), edata->internalpos);
    if (Log_error_verbosity >= PGERROR_DEFAULT) {
        if (edata->detail_log) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("DETAIL:  "));
            append_with_tabs(&t->error, edata->detail_log);
        } else if (edata->detail) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("DETAIL:  "));
            append_with_tabs(&t->error, edata->detail);
        }
        if (edata->hint) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("HINT:  "));
            append_with_tabs(&t->error, edata->hint);
        }
        if (edata->internalquery) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("QUERY:  "));
            append_with_tabs(&t->error, edata->internalquery);
        }
        if (edata->context
#if PG_VERSION_NUM >= 90500
            && !edata->hide_ctx
#endif
        ) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("CONTEXT:  "));
            append_with_tabs(&t->error, edata->context);
        }
        if (Log_error_verbosity >= PGERROR_VERBOSE) {
            if (edata->funcname && edata->filename) { // assume no newlines in funcname or filename...
                if (t->error.len) appendStringInfoChar(&t->error, '\n');
                appendStringInfo(&t->error, _("LOCATION:  %s, %s:%d"), edata->funcname, edata->filename, edata->lineno);
            } else if (edata->filename) {
                if (t->error.len) appendStringInfoChar(&t->error, '\n');
                appendStringInfo(&t->error, _("LOCATION:  %s:%d"), edata->filename, edata->lineno);
            }
        }
#if PG_VERSION_NUM >= 130000
        if (edata->backtrace) {
            if (t->error.len) appendStringInfoChar(&t->error, '\n');
            appendStringInfoString(&t->error, _("BACKTRACE:  "));
            append_with_tabs(&t->error, edata->backtrace);
        }
#endif
    }
    if (t->input && is_log_level_output(edata->elevel, log_min_error_statement) && !edata->hide_stmt) { // If the user wants the query that generated this error logged, do it.
        if (t->error.len) appendStringInfoChar(&t->error, '\n');
        appendStringInfoString(&t->error, _("STATEMENT:  "));
        append_with_tabs(&t->error, t->input);
    }
    FreeErrorData(edata);
}

static void task_shmem_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
    init_free(DatumGetInt32(arg));
}

static void task_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
}

static void task_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) task_reload();
}

void task_free(Task *t) {
    if (t->error.data) { pfree(t->error.data); t->error.data = NULL; t->error.len = 0; }
    if (t->group) { pfree(t->group); t->group = NULL; }
    if (t->input) { pfree(t->input); t->input = NULL; }
    if (t->null) { pfree(t->null); t->null = NULL; }
    if (t->output.data) { pfree(t->output.data); t->output.data = NULL; t->output.len = 0; }
    if (t->remote) { pfree(t->remote); t->remote = NULL; }
}

void task_main(Datum main_arg) {
    const char *application_name;
    StringInfoData oid, schema_table, schema_type;
    Task *task = get_task();
    elog(DEBUG1, "main_arg = %i", DatumGetInt32(main_arg));
    task->work = get_work();
    task->shared = init_shared(main_arg);
    before_shmem_exit(task_shmem_exit, main_arg);
    if (!task->shared->in_use) return;
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    task->work->data = quote_identifier(task->shared->data);
    task->work->schema = quote_identifier(task->shared->schema);
    task->work->table = quote_identifier(task->shared->table);
    task->work->user = quote_identifier(task->shared->user);
    BackgroundWorkerInitializeConnectionMy(task->shared->data, task->shared->user);
    application_name = MyBgworkerEntry->bgw_name + strlen(task->shared->user) + 1 + strlen(task->shared->data) + 1;
    set_config_option_my("application_name", application_name, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname(application_name);
    set_ps_display_my("main");
    process_session_preload_libraries();
    elog(DEBUG1, "oid = %i, id = %li, hash = %i, max = %i", task->shared->oid, task->shared->id, task->shared->hash, task->shared->max);
    set_config_option_my("pg_task.schema", task->shared->schema, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    set_config_option_my("pg_task.table", task->shared->table, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    initStringInfoMy(&schema_table);
    appendStringInfo(&schema_table, "%s.%s", task->work->schema, task->work->table);
    task->work->schema_table = schema_table.data;
    initStringInfoMy(&schema_type);
    appendStringInfo(&schema_type, "%s.state",task->work->schema);
    task->work->schema_type = schema_type.data;
    initStringInfoMy(&oid);
    appendStringInfo(&oid, "%i", task->shared->oid);
    set_config_option_my("pg_task.oid", oid.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pfree(oid.data);
    task->pid = MyProcPid;
    task->start = GetCurrentTimestamp();
    set_ps_display_my("idle");
    if (!lock_table_pid_hash(task->shared->oid, task->pid, task->shared->hash)) { ereport(WARNING, (errmsg("!lock_table_pid_hash(%i, %i, %i)", task->shared->oid, task->pid, task->shared->hash))); return; }
    while (!ShutdownRequestPending) {
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 0);
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
        if (rc & WL_LATCH_SET) task_latch();
        if (rc & WL_TIMEOUT) if (dest_timeout()) ShutdownRequestPending = true;
    }
    if (!unlock_table_pid_hash(task->shared->oid, task->pid, task->shared->hash)) ereport(WARNING, (errmsg("!unlock_table_pid_hash(%i, %i, %i)", task->shared->oid, task->pid, task->shared->hash)));
}
