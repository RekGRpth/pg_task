#include "include.h"

bool response_isnull;
int timeout;
static char *request;
static char *state;
static const char *data;
static const char *data_quote;
static const char *point;
static const char *queue;
static const char *schema;
static const char *schema_quote;
extern const char *schema_quote_point_table_quote;
static const char *table;
static const char *table_quote;
static const char *user;
static const char *user_quote;
static Datum done;
static Datum fail;
static Datum id;
static Datum queue_datum;
static Datum state_datum;
static int count;
static int max;
static Oid oid = 0;
static TimestampTz start;
static volatile sig_atomic_t sigterm = false;
StringInfoData response;

static void update_ps_display(void) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_name, DatumGetUInt64(id));
    init_ps_display(buf.data, "", "", "");
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_type, DatumGetUInt64(id));
    SetConfigOptionMy("application_name", buf.data);
    pgstat_report_appname(buf.data);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%lu", DatumGetUInt64(id));
    set_config_option_my("pg_task.id", buf.data);
    pfree(buf.data);
}

void task_work(Datum id, char **request, int *timeout) {
    #define ID 1
    #define SID S(ID)
    static Oid argtypes[] = {[ID - 1] = INT8OID};
    Datum values[] = {[ID - 1] = id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    update_ps_display();
    count++;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %1$s WHERE id = $" SID " FOR UPDATE)\n"
            "UPDATE  %1$s AS u\n"
            "SET     state = 'WORK'::state,\n"
            "        start = current_timestamp,\n"
            "        pid = pg_backend_pid()\n"
            "FROM s WHERE u.id = s.id RETURNING request, COALESCE(EXTRACT(epoch FROM timeout), 0)::int4 * 1000 AS timeout", schema_quote_point_table_quote);
        command = buf.data;
    }
    #undef ID
    #undef SID
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) E("SPI_processed != 1"); else {
        MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
        bool timeout_isnull;
        *request = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"));
        *timeout = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "timeout"), &timeout_isnull));
        L("request = %s, timeout = %i", *request, *timeout);
        if (timeout_isnull) E("timeout_isnull");
        MemoryContextSwitchTo(oldMemoryContext);
    }
    SPI_commit_my(command);
    state = "DONE";
    state_datum = done;
    response_isnull = true;
    initStringInfo(&response);
    if (0 < StatementTimeout && StatementTimeout < *timeout) *timeout = StatementTimeout;
}

static void task_repeat(void) {
    #define ID 1
    #define SID S(ID)
    static Oid argtypes[] = {[ID - 1] = INT8OID};
    Datum values[] = {[ID - 1] = id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "INSERT INTO %1$s (dt, queue, max, request, timeout, delete, repeat, drift, count, live)\n"
            "SELECT CASE WHEN drift THEN current_timestamp + repeat\n"
            "ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1)\n"
            "END AS dt, queue, max, request, timeout, delete, repeat, drift, count, live\n"
            "FROM %1$s WHERE id = $" SID " AND state IN ('DONE'::state, 'FAIL'::state) LIMIT 1", schema_quote_point_table_quote);
        command = buf.data;
    }
    #undef ID
    #undef SID
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_INSERT);
    SPI_commit_my(command);
}

static void task_delete(void) {
    #define ID 1
    #define SID S(ID)
    static Oid argtypes[] = {[ID - 1] = INT8OID};
    Datum values[] = {[ID - 1] = id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DELETE FROM %s WHERE id = $" SID, schema_quote_point_table_quote);
        command = buf.data;
    }
    #undef ID
    #undef SID
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_DELETE);
    SPI_commit_my(command);
}

static void task_live(void) {
    #define QUEUE 1
    #define SQUEUE S(QUEUE)
    #define MAX 2
    #define SMAX S(MAX)
    #define COUNT 3
    #define SCOUNT S(COUNT)
    #define START 4
    #define SSTART S(START)
    static Oid argtypes[] = {[QUEUE - 1] = TEXTOID, [MAX - 1] = INT4OID, [COUNT - 1] = INT4OID, [START - 1] = TIMESTAMPTZOID};
    Datum values[] = {[QUEUE - 1] = queue_datum, [MAX - 1] = Int32GetDatum(max), [COUNT - 1] = Int32GetDatum(count), [START - 1] = TimestampTzGetDatum(start)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    pg_advisory_unlock_int4_my(oid, DatumGetUInt64(id));
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (\n"
            "SELECT  id\n"
            "FROM    %1$s\n"
            "WHERE   state = 'PLAN'::state\n"
            "AND     dt <= current_timestamp\n"
            "AND     queue = $" SQUEUE "\n"
            "AND     COALESCE(max, ~(1<<31)) >= $" SMAX "\n"
            "AND     CASE WHEN count IS NOT NULL AND live IS NOT NULL THEN count > $" SCOUNT " AND $" SSTART " + live > current_timestamp ELSE COALESCE(count, 0) > $" SCOUNT " OR $" SSTART " + COALESCE(live, '0 sec'::interval) > current_timestamp END\n"
            "ORDER BY COALESCE(max, ~(1<<31)) DESC LIMIT 1 FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %1$s AS u SET state = 'TAKE'::state FROM s WHERE u.id = s.id RETURNING u.id", schema_quote_point_table_quote);
        command = buf.data;
    }
    #undef QUEUE
    #undef SQUEUE
    #undef MAX
    #undef SMAX
    #undef COUNT
    #undef SCOUNT
    #undef START
    #undef SSTART
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, NULL, SPI_OK_UPDATE_RETURNING);
    if (!SPI_processed) sigterm = true; else {
        bool id_isnull;
        id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        if (id_isnull) E("id_isnull");
    }
    SPI_commit_my(command);
}

static void task_done(void) {
    bool delete, repeat, live;
    #define ID 1
    #define SID S(ID)
    #define STATE 2
    #define SSTATE S(STATE)
    #define RESPONSE 3
    #define SRESPONSE S(RESPONSE)
    static Oid argtypes[] = {[ID - 1] = INT8OID, [STATE - 1] = TEXTOID, [RESPONSE - 1] = TEXTOID};
    Datum values[] = {[ID - 1] = id, [STATE - 1] = state_datum, [RESPONSE - 1] = !response_isnull ? CStringGetTextDatum(response.data) : (Datum)NULL};
    char nulls[] = {[ID - 1] = ' ', [STATE - 1] = ' ', [RESPONSE - 1] = !response_isnull ? ' ' : 'n'};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    StaticAssertStmt(sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(nulls)/sizeof(nulls[0]), "sizeof(argtypes)/sizeof(argtypes[0]) == sizeof(values)/sizeof(values[0])");
    L("id = %lu, response = %s, state = %s", DatumGetUInt64(id), !response_isnull ? response.data : "(null)", state);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %1$s WHERE id = $" SID " FOR UPDATE\n)\n"
            "UPDATE %1$s AS u SET state = $" SSTATE "::state, stop = current_timestamp, response = $" SRESPONSE " FROM s WHERE u.id = s.id\n"
            "RETURNING delete, repeat IS NOT NULL AND state IN ('DONE'::state, 'FAIL'::state) AS repeat, count IS NOT NULL OR live IS NOT NULL AS live", schema_quote_point_table_quote);
        command = buf.data;
    }
    #undef ID
    #undef SID
    #undef STATE
    #undef SSTATE
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes);
    SPI_execute_plan_my(plan, values, nulls, SPI_OK_UPDATE_RETURNING);
    if (SPI_processed != 1) E("SPI_processed != 1"); else {
        bool delete_isnull, repeat_isnull, live_isnull;
        delete = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "delete"), &delete_isnull));
        repeat = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "repeat"), &repeat_isnull));
        live = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "live"), &live_isnull));
        if (delete_isnull) E("delete_isnull");
        if (repeat_isnull) E("repeat_isnull");
        if (live_isnull) E("live_isnull");
    }
    SPI_commit_my(command);
    if (!response_isnull) pfree((void *)values[RESPONSE - 1]);
    #undef RESPONSE
    #undef SRESPONSE
    if (repeat) task_repeat();
    if (delete && response_isnull) task_delete();
    pfree(response.data);
    if (live) task_live(); else sigterm = true;
}

static void task_success(void) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(MessageContext);
    MemoryContextResetAndDeleteChildren(MessageContext);
    InvalidateCatalogSnapshotConditionally();
    MemoryContextSwitchTo(oldMemoryContext);
    SetCurrentStatementStartTimestamp();
    whereToSendOutput = DestDebug;
    exec_simple_query(request);
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

static void task_error(void) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    ErrorData *edata = CopyErrorData();
    appendStringInfo(&response, "elevel::int4\t%i", edata->elevel);
    appendStringInfo(&response, "\noutput_to_server::bool\t%s", edata->output_to_server ? "true" : "false");
    appendStringInfo(&response, "\noutput_to_client::bool\t%s", edata->output_to_client ? "true" : "false");
    appendStringInfo(&response, "\nshow_funcname::bool\t%s", edata->show_funcname ? "true" : "false");
    appendStringInfo(&response, "\nhide_stmt::bool\t%s", edata->hide_stmt ? "true" : "false");
    appendStringInfo(&response, "\nhide_ctx::bool\t%s", edata->hide_ctx ? "true" : "false");
    if (edata->filename) appendStringInfo(&response, "\nfilename::text\t%s", edata->filename);
    if (edata->lineno) appendStringInfo(&response, "\nlineno::int4\t%i", edata->lineno);
    if (edata->funcname) appendStringInfo(&response, "\nfuncname::text\t%s", edata->funcname);
    if (edata->domain) appendStringInfo(&response, "\ndomain::text\t%s", edata->domain);
    if (edata->context_domain) appendStringInfo(&response, "\ncontext_domain::text\t%s", edata->context_domain);
    if (edata->sqlerrcode) appendStringInfo(&response, "\nsqlerrcode::int4\t%i", edata->sqlerrcode);
    if (edata->message) appendStringInfo(&response, "\nmessage::text\t%s", edata->message);
    if (edata->detail) appendStringInfo(&response, "\ndetail::text\t%s", edata->detail);
    if (edata->detail_log) appendStringInfo(&response, "\ndetail_log::text\t%s", edata->detail_log);
    if (edata->hint) appendStringInfo(&response, "\nhint::text\t%s", edata->hint);
    if (edata->context) appendStringInfo(&response, "\ncontext::text\t%s", edata->context);
    if (edata->message_id) appendStringInfo(&response, "\nmessage_id::text\t%s", edata->message_id);
    if (edata->schema_name) appendStringInfo(&response, "\nschema_name::text\t%s", edata->schema_name);
    if (edata->table_name) appendStringInfo(&response, "\ntable_name::text\t%s", edata->table_name);
    if (edata->column_name) appendStringInfo(&response, "\ncolumn_name::text\t%s", edata->column_name);
    if (edata->datatype_name) appendStringInfo(&response, "\ndatatype_name::text\t%s", edata->datatype_name);
    if (edata->constraint_name) appendStringInfo(&response, "\nconstraint_name::text\t%s", edata->constraint_name);
    if (edata->cursorpos) appendStringInfo(&response, "\ncursorpos::int4\t%i", edata->cursorpos);
    if (edata->internalpos) appendStringInfo(&response, "\ninternalpos::int4\t%i", edata->internalpos);
    if (edata->internalquery) appendStringInfo(&response, "\ninternalquery::text\t%s", edata->internalquery);
    if (edata->saved_errno) appendStringInfo(&response, "\nsaved_errno::int4\t%i", edata->saved_errno);
    FreeErrorData(edata);
    state = "FAIL";
    state_datum = fail;
    response_isnull = false;
    MemoryContextSwitchTo(oldMemoryContext);
    SPI_rollback_my(request);
}

static void task_loop(void) {
    if (!pg_try_advisory_lock_int4_my(oid, DatumGetUInt64(id))) E("lock id = %lu, oid = %d", DatumGetUInt64(id), oid);
    task_work(id, &request, &timeout);
    L("id = %lu, timeout = %d, request = %s, count = %u", DatumGetUInt64(id), timeout, request, count);
    PG_TRY();
        task_success();
    PG_CATCH();
        task_error();
    PG_END_TRY();
    pfree(request);
    task_done();
}

static void task_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void task_init(void) {
    StringInfoData buf;
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    id = MyBgworkerEntry->bgw_main_arg;
    start = GetCurrentTimestamp();
    count = 0;
    user = MyBgworkerEntry->bgw_extra;
    if (!MyProcPort->user_name) MyProcPort->user_name = (char *)user;
    data = user + strlen(user) + 1;
    if (!MyProcPort->database_name) MyProcPort->database_name = (char *)data;
    schema = data + strlen(data) + 1;
    table = schema + strlen(schema) + 1;
    queue = table + strlen(table) + 1;
    max = *(typeof(max) *)(queue + strlen(queue) + 1);
    oid = *(typeof(oid) *)(queue + strlen(queue) + 1 + sizeof(max));
    if (table == schema + 1) schema = NULL;
    data_quote = quote_identifier(data);
    user_quote = quote_identifier(user);
    schema_quote = schema ? quote_identifier(schema) : NULL;
    point = schema ? "." : "";
    table_quote = quote_identifier(table);
    initStringInfo(&buf);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    schema_quote_point_table_quote = buf.data;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_type, DatumGetUInt64(id));
    SetConfigOptionMy("application_name", buf.data);
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u, oid = %d", user, data, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max, oid);
    pqsignal(SIGTERM, task_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(data, user, 0);
    pgstat_report_appname(buf.data);
    set_config_option_my("pg_task.data", data);
    set_config_option_my("pg_task.user", user);
    if (schema) set_config_option_my("pg_task.schema", schema);
    set_config_option_my("pg_task.table", table);
    set_config_option_my("pg_task.queue", queue);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%d", oid);
    set_config_option_my("pg_task.oid", buf.data);
    pfree(buf.data);
    MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    done = CStringGetTextDatum("DONE");
    fail = CStringGetTextDatum("FAIL");
    queue_datum = CStringGetTextDatum(queue);
}

static void task_reset(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

void task_worker(Datum main_arg); void task_worker(Datum main_arg) {
    task_init();
    while (!sigterm) {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
        if (!BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid)) break;
        if (rc & WL_LATCH_SET) task_reset();
        if (rc & WL_TIMEOUT) task_loop();
    }
}
