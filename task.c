#include "include.h"

extern bool stmt_timeout_active;
extern bool xact_started;
extern volatile sig_atomic_t sigterm;

bool task_work(Task *task) {
    #define ID 1
    #define SID S(ID)
    #define PID 2
    #define SPID S(PID)
    bool exit = false;
    Work *work = task->work;
    static Oid argtypes[] = {[ID - 1] = INT8OID, [PID - 1] = INT4OID};
    Datum values[] = {[ID - 1] = Int64GetDatum(task->id), [PID - 1] = Int32GetDatum(task->pid)};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    task->count++;
    L("id = %li, group = %s, max = %i, oid = %i, count = %i, pid = %i", task->id, task->group, task->max, work->oid, task->count, task->pid);
    if (!pg_try_advisory_lock_int4_my(work->oid, task->id)) {
        W("!pg_try_advisory_lock_int4_my(%i, %li)", work->oid, task->id);
        return true;
    }
    if (!task->conn) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "%li", task->id);
        SetConfigOptionMy("pg_task.id", buf.data);
        pfree(buf.data);
    }
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %1$s WHERE id = $" SID " AND state = 'TAKE'::state FOR UPDATE)\n"
            "UPDATE  %1$s AS u\n"
            "SET     state = 'WORK'::state,\n"
            "        start = current_timestamp,\n"
            "        pid = $" SPID "\n"
            "FROM s WHERE u.id = s.id RETURNING request, COALESCE(EXTRACT(epoch FROM timeout), 0)::int4 * 1000 AS timeout, append", work->schema_table);
        command = buf.data;
    }
    #undef ID
    #undef SID
    #undef PID
    #undef SPID
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING, true);
    if (SPI_processed != 1) {
        W("SPI_processed != 1");
        exit = true;
    } else {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        task->request = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "request", false));
        MemoryContextSwitchTo(oldMemoryContext);
        task->timeout = DatumGetInt32(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "timeout", false));
        task->append = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "append", false));
        if (0 < StatementTimeout && StatementTimeout < task->timeout) task->timeout = StatementTimeout;
        L("request = %s, timeout = %i", task->request, task->timeout);
    }
    SPI_finish_my();
    return exit;
}

void task_repeat(Task *task) {
    #define ID 1
    #define SID S(ID)
    static Oid argtypes[] = {[ID - 1] = INT8OID};
    Datum values[] = {[ID - 1] = Int64GetDatum(task->id)};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        Work *work = task->work;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "INSERT INTO %1$s (parent, dt, \"group\", max, request, timeout, delete, repeat, drift, count, live)\n"
            "SELECT $" SID ", CASE WHEN drift THEN current_timestamp + repeat\n"
            "ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1)\n"
            "END AS dt, \"group\", max, request, timeout, delete, repeat, drift, count, live\n"
            "FROM %1$s WHERE id = $" SID " AND state IN ('DONE'::state, 'FAIL'::state) LIMIT 1", work->schema_table);
        command = buf.data;
    }
    #undef ID
    #undef SID
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_INSERT, true);
    SPI_finish_my();
}

void task_delete(Task *task) {
    #define ID 1
    #define SID S(ID)
    static Oid argtypes[] = {[ID - 1] = INT8OID};
    Datum values[] = {[ID - 1] = Int64GetDatum(task->id)};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        Work *work = task->work;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DELETE FROM %s WHERE id = $" SID " AND state IN ('DONE'::state, 'FAIL'::state)", work->schema_table);
        command = buf.data;
    }
    #undef ID
    #undef SID
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_DELETE, true);
    SPI_finish_my();
}

bool task_live(Task *task) {
    #define GROUP 1
    #define SGROUP S(GROUP)
    #define REMOTE 2
    #define SREMOTE S(REMOTE)
    #define MAX 3
    #define SMAX S(MAX)
    #define COUNT 4
    #define SCOUNT S(COUNT)
    #define START 5
    #define SSTART S(START)
    bool exit = false;
    static Oid argtypes[] = {[GROUP - 1] = TEXTOID, [REMOTE - 1] = TEXTOID, [MAX - 1] = INT4OID, [COUNT - 1] = INT4OID, [START - 1] = TIMESTAMPTZOID};
    Datum values[] = {[GROUP - 1] = CStringGetTextDatum(task->group), [REMOTE - 1] = task->remote ? CStringGetTextDatum(task->remote) : (Datum)NULL, [MAX - 1] = Int32GetDatum(task->max), [COUNT - 1] = Int32GetDatum(task->count), [START - 1] = TimestampTzGetDatum(task->start)};
    char nulls[] = {[GROUP - 1] = ' ', [REMOTE - 1] = task->remote ? ' ' : 'n', [MAX - 1] = ' ', [COUNT - 1] = ' ', [START - 1] = ' '};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(nulls)/sizeof(nulls[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        Work *work = task->work;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (\n"
            "SELECT  id\n"
            "FROM    %1$s\n"
            "WHERE   state = 'PLAN'::state\n"
            "AND     dt <= current_timestamp\n"
            "AND     \"group\" = $" SGROUP "\n"
            "AND     remote IS NOT DISTINCT FROM $" SREMOTE "\n"
            "AND     COALESCE(max, ~(1<<31)) >= $" SMAX "\n"
            "AND     CASE WHEN count IS NOT NULL AND live IS NOT NULL THEN count > $" SCOUNT " AND $" SSTART " + live > current_timestamp ELSE COALESCE(count, 0) > $" SCOUNT " OR $" SSTART " + COALESCE(live, '0 sec'::interval) > current_timestamp END\n"
            "ORDER BY COALESCE(max, ~(1<<31)) DESC LIMIT 1 FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %1$s AS u SET state = 'TAKE'::state FROM s WHERE u.id = s.id RETURNING u.id", work->schema_table);
        command = buf.data;
    }
    #undef MAX
    #undef SMAX
    #undef COUNT
    #undef SCOUNT
    #undef START
    #undef SSTART
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING, true);
    if (!SPI_processed) exit = true; else task->id = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "id", false));
    SPI_finish_my();
    pfree((void *)values[GROUP - 1]);
    #undef GROUP
    #undef SGROUP
    if (task->remote) pfree((void *)values[REMOTE - 1]);
    #undef REMOTE
    #undef SREMOTE
    return exit;
}

bool task_done(Task *task) {
    #define ID 1
    #define SID S(ID)
    #define FAIL 2
    #define SFAIL S(FAIL)
    #define RESPONSE 3
    #define SRESPONSE S(RESPONSE)
    bool exit = false;
    Work *work = task->work;
    static Oid argtypes[] = {[ID - 1] = INT8OID, [FAIL - 1] = BOOLOID, [RESPONSE - 1] = TEXTOID};
    Datum values[] = {[ID - 1] = Int64GetDatum(task->id), [FAIL - 1] = BoolGetDatum(task->fail = task->response.data ? task->fail : false), [RESPONSE - 1] = task->response.data ? CStringGetTextDatum(task->response.data) : (Datum)NULL};
    char nulls[] = {[ID - 1] = ' ', [FAIL - 1] = ' ', [RESPONSE - 1] = task->response.data ? ' ' : 'n'};
    static SPI_plan *plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(nulls)/sizeof(nulls[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    L("id = %li, response = %s, fail = %s", task->id, task->response.data ? task->response.data : "(null)", task->fail ? "true" : "false");
    if (!command) {
        Work *work = task->work;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %1$s WHERE id = $" SID " AND state IN ('WORK'::state, 'TAKE'::state) FOR UPDATE\n)\n"
            "UPDATE %1$s AS u SET state = CASE WHEN $" SFAIL " THEN 'FAIL'::state ELSE 'DONE'::state END, stop = current_timestamp, response = $" SRESPONSE " FROM s WHERE u.id = s.id\n"
            "RETURNING delete, repeat IS NOT NULL AND state IN ('DONE'::state, 'FAIL'::state) AS repeat, count IS NOT NULL OR live IS NOT NULL AS live", work->schema_table);
        command = buf.data;
    }
    #undef ID
    #undef SID
    #undef FAIL
    #undef SFAIL
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING, true);
    if (SPI_processed != 1) {
        W("SPI_processed != 1");
        exit = true;
    } else {
        task->delete = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "delete", false));
        task->repeat = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "repeat", false));
        task->live = DatumGetBool(SPI_getbinval_my(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, "live", false));
    }
    SPI_finish_my();
    if (task->response.data) pfree((void *)values[RESPONSE - 1]);
    #undef RESPONSE
    #undef SRESPONSE
    pg_advisory_unlock_int4_my(work->oid, task->id);
    return exit;
}

static void task_success(Task *task) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(MessageContext);
    MemoryContextResetAndDeleteChildren(MessageContext);
    InvalidateCatalogSnapshotConditionally();
    MemoryContextSwitchTo(oldMemoryContext);
    ReadyForQueryMy(task);
    SetCurrentStatementStartTimestamp();
    exec_simple_query_my(task);
    pfree(task->request);
    task->request = "COMMIT";
    if (IsTransactionState()) exec_simple_query_my(task);
    if (IsTransactionState()) E("IsTransactionState");
}

static void task_fail(Task *task) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    ErrorData *edata = CopyErrorData();
    if (!task->response.data) initStringInfo(&task->response);
    MemoryContextSwitchTo(oldMemoryContext);
    if (edata->elevel) appendStringInfo(&task->response, "%selevel%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->elevel);
    if (edata->output_to_server) appendStringInfo(&task->response, "%soutput_to_server%s\ttrue", task->response.len ? "\n" : "", task->append ? "::bool" : "");
    if (edata->output_to_client) appendStringInfo(&task->response, "%soutput_to_client%s\ttrue", task->response.len ? "\n" : "", task->append ? "::bool" : "");
    if (edata->show_funcname) appendStringInfo(&task->response, "%sshow_funcname%s\ttrue", task->response.len ? "\n" : "", task->append ? "::bool" : "");
    if (edata->hide_stmt) appendStringInfo(&task->response, "%shide_stmt%s\ttrue", task->response.len ? "\n" : "", task->append ? "::bool" : "");
    if (edata->hide_ctx) appendStringInfo(&task->response, "%shide_ctx%s\ttrue", task->response.len ? "\n" : "", task->append ? "::bool" : "");
    if (edata->filename) appendStringInfo(&task->response, "%sfilename%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->filename);
    if (edata->lineno) appendStringInfo(&task->response, "%slineno%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->lineno);
    if (edata->funcname) appendStringInfo(&task->response, "%sfuncname%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->funcname);
    if (edata->domain) appendStringInfo(&task->response, "%sdomain%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->domain);
    if (edata->context_domain) appendStringInfo(&task->response, "%scontext_domain%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->context_domain);
    if (edata->sqlerrcode) appendStringInfo(&task->response, "%ssqlerrcode%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->sqlerrcode);
    if (edata->message) appendStringInfo(&task->response, "%smessage%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->message);
    if (edata->detail) appendStringInfo(&task->response, "%sdetail%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->detail);
    if (edata->detail_log) appendStringInfo(&task->response, "%sdetail_log%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->detail_log);
    if (edata->hint) appendStringInfo(&task->response, "%shint%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->hint);
    if (edata->context) appendStringInfo(&task->response, "%scontext%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->context);
    if (edata->message_id) appendStringInfo(&task->response, "%smessage_id%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->message_id);
    if (edata->schema_name) appendStringInfo(&task->response, "%sschema_name%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->schema_name);
    if (edata->table_name) appendStringInfo(&task->response, "%stable_name%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->table_name);
    if (edata->column_name) appendStringInfo(&task->response, "%scolumn_name%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->column_name);
    if (edata->datatype_name) appendStringInfo(&task->response, "%sdatatype_name%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->datatype_name);
    if (edata->constraint_name) appendStringInfo(&task->response, "%sconstraint_name%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->constraint_name);
    if (edata->cursorpos) appendStringInfo(&task->response, "%scursorpos%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->cursorpos);
    if (edata->internalpos) appendStringInfo(&task->response, "%sinternalpos%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->internalpos);
    if (edata->internalquery) appendStringInfo(&task->response, "%sinternalquery%s\t%s", task->response.len ? "\n" : "", task->append ? "::text" : "", edata->internalquery);
    if (edata->saved_errno) appendStringInfo(&task->response, "%ssaved_errno%s\t%i", task->response.len ? "\n" : "", task->append ? "::int4" : "", edata->saved_errno);
    appendStringInfo(&task->response, "%sROLLBACK", task->response.len ? "\n" : "");
    FreeErrorData(edata);
    HOLD_INTERRUPTS();
    disable_all_timeouts(false);
    QueryCancelPending = false;
    stmt_timeout_active = false;
    EmitErrorReport();
    debug_query_string = NULL;
    AbortOutOfAnyTransaction();
    PortalErrorCleanup();
    SPICleanup();
    if (MyReplicationSlot) ReplicationSlotRelease();
    ReplicationSlotCleanup();
    jit_reset_after_error();
    MemoryContextSwitchTo(TopMemoryContext);
    FlushErrorState();
    xact_started = false;
    RESUME_INTERRUPTS();
    task->fail = true;
}

static bool task_timeout(Task *task) {
    if (task_work(task)) return true;
    L("id = %li, timeout = %i, request = %s, count = %i", task->id, task->timeout, task->request, task->count);
    PG_TRY();
        task_success(task);
    PG_CATCH();
        task_fail(task);
    PG_END_TRY();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
    if (task_done(task)) return true;
    L("repeat = %s, delete = %s, live = %s", task->repeat ? "true" : "false", task->delete ? "true" : "false", task->live ? "true" : "false");
    if (task->repeat) task_repeat(task);
    if (task->delete && !task->response.data) task_delete(task);
    if (task->response.data) pfree(task->response.data);
    task->response.data = NULL;
    return !task->live || task_live(task);
}

static void task_init(Work *work, Task *task) {
    StringInfoData buf;
    const char *schema_quote;
    const char *table_quote;
    char *p = MyBgworkerEntry->bgw_extra;
    task->work = work;
    work->user = p;
    p += strlen(work->user) + 1;
    work->data = p;
    p += strlen(work->data) + 1;
    work->schema = p;
    p += strlen(work->schema) + 1;
    work->table = p;
    p += strlen(work->table) + 1;
    if (work->table == work->schema + 1) work->schema = NULL;
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    if (!MyProcPort->user_name) MyProcPort->user_name = work->user;
    if (!MyProcPort->database_name) MyProcPort->database_name = work->data;
    SetConfigOptionMy("application_name", MyBgworkerEntry->bgw_type);
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    L("user = %s, data = %s, schema = %s, table = %s", work->user, work->data, work->schema ? work->schema : "(null)", work->table);
    SetConfigOptionMy("pg_task.data", work->data);
    SetConfigOptionMy("pg_task.user", work->user);
    if (work->schema) SetConfigOptionMy("pg_task.schema", work->schema);
    SetConfigOptionMy("pg_task.table", work->table);
    schema_quote = work->schema ? quote_identifier(work->schema) : NULL;
    table_quote = quote_identifier(work->table);
    initStringInfo(&buf);
    if (work->schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    work->schema_table = buf.data;
    work->oid = *(typeof(work->oid) *)p;
    p += sizeof(work->oid);
    L("oid = %i", work->oid);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%i", work->oid);
    SetConfigOptionMy("pg_task.oid", buf.data);
    pfree(buf.data);
    if (work->schema && schema_quote && work->schema != schema_quote) pfree((void *)schema_quote);
    if (work->table != table_quote) pfree((void *)table_quote);
    task->pid = MyProcPid;
    task->id = MyBgworkerEntry->bgw_main_arg;
    task->start = GetCurrentTimestamp();
    task->count = 0;
    task->group = p;
    p += strlen(task->group) + 1;
    task->max = *(typeof(task->max) *)p;
    L("id = %li, group = %s, max = %i", task->id, task->group, task->max);
    pqsignal(SIGTERM, init_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(work->data, work->user, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    process_session_preload_libraries();
    SetConfigOptionMy("pg_task.group", task->group);
}

static void task_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

void task_worker(Datum main_arg); void task_worker(Datum main_arg) {
    Work work;
    Task task;
    MemSet(&work, 0, sizeof(work));
    MemSet(&task, 0, sizeof(task));
    task_init(&work, &task);
    while (!sigterm) {
        int nevents = 2;
        WaitEvent *events = palloc0(nevents * sizeof(*events));
        WaitEventSet *set = CreateWaitEventSet(TopMemoryContext, nevents);
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
        nevents = WaitEventSetWait(set, 0, events, nevents, PG_WAIT_EXTENSION);
        for (int i = 0; i < nevents; i++) {
            WaitEvent *event = &events[i];
            if (event->events & WL_LATCH_SET) L("WL_LATCH_SET");
            if (event->events & WL_SOCKET_READABLE) L("WL_SOCKET_READABLE");
            if (event->events & WL_SOCKET_WRITEABLE) L("WL_SOCKET_WRITEABLE");
            if (event->events & WL_POSTMASTER_DEATH) L("WL_POSTMASTER_DEATH");
            if (event->events & WL_EXIT_ON_PM_DEATH) L("WL_EXIT_ON_PM_DEATH");
            if (event->events & WL_LATCH_SET) task_latch();
        }
        if (!nevents) sigterm = sigterm || task_timeout(&task);
        FreeWaitEventSet(set);
        pfree(events);
    }
}
