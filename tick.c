#include "include.h"

static volatile sig_atomic_t sighup = false;
static volatile sig_atomic_t sigterm = false;

static void tick_schema(Work *work) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = quote_identifier(work->schema);
    L("user = %s, data = %s, schema = %s, table = %s", work->user, work->data, work->schema, work->table);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my(true);
    list_free_deep(names);
    if (schema_quote != work->schema) pfree((void *)schema_quote);
    pfree(buf.data);
    SetConfigOptionMy("pg_task.schema", work->schema);
}

static void tick_type(Work *work) {
    StringInfoData buf, name;
    Oid type = InvalidOid;
    int32 typmod;
    const char *schema_quote = work->schema ? quote_identifier(work->schema) : NULL;
    L("user = %s, data = %s, schema = %s, table = %s", work->user, work->data, work->schema ? work->schema : "(null)", work->table);
    initStringInfo(&name);
    if (schema_quote) appendStringInfo(&name, "%s.", schema_quote);
    appendStringInfoString(&name, "state");
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP')", name.data);
    SPI_connect_my(buf.data);
    parseTypeString(name.data, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my(true);
    if (work->schema && schema_quote && work->schema != schema_quote) pfree((void *)schema_quote);
    pfree(name.data);
    pfree(buf.data);
}

static void tick_table(Work *work) {
    StringInfoData buf, name;
    List *names;
    const RangeVar *relation;
    const char *name_quote;
    L("user = %s, data = %s, schema = %s, table = %s", work->user, work->data, work->schema ? work->schema : "(null)", work->table);
    if (work->oid) pg_advisory_unlock_int8_my(work->oid);
    SetConfigOptionMy("pg_task.table", work->table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", work->table);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE %1$s (\n"
        "    id bigserial NOT NULL PRIMARY KEY,\n"
        "    parent int8 DEFAULT current_setting('pg_task.id', true)::int8,\n"
        "    dt timestamp NOT NULL DEFAULT current_timestamp,\n"
        "    start timestamp,\n"
        "    stop timestamp,\n"
        "    \"group\" text NOT NULL DEFAULT 'group',\n"
        "    max int4,\n"
        "    pid int4,\n"
        "    request text NOT NULL,\n"
        "    response text,\n"
        "    state state NOT NULL DEFAULT 'PLAN'::state,\n"
        "    timeout interval,\n"
        "    delete boolean NOT NULL DEFAULT false,\n"
        "    repeat interval,\n"
        "    drift boolean NOT NULL DEFAULT true,\n"
        "    count int4,\n"
        "    live interval,\n"
        "    CONSTRAINT %2$s FOREIGN KEY (parent) REFERENCES %1$s (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL\n"
        ")", work->schema_table, name_quote);
    names = stringToQualifiedNameList(work->schema_table);
    relation = makeRangeVarFromNameList(names);
    SPI_connect_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    work->oid = RangeVarGetRelid(relation, NoLock, false);
    SPI_commit_my();
    SPI_finish_my(true);
    pfree((void *)relation);
    list_free_deep(names);
    if (name_quote != name.data) pfree((void *)name_quote);
    pfree(name.data);
    SetConfigOptionMy("pg_task.table", work->table);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%d", work->oid);
    SetConfigOptionMy("pg_task.oid", buf.data);
    pfree(buf.data);
}

static void tick_index(Work *work, const char *index) {
    StringInfoData buf, name;
    List *names;
    const RangeVar *relation;
    const char *name_quote;
    const char *index_quote = quote_identifier(index);
    L("user = %s, data = %s, schema = %s, table = %s, index = %s", work->user, work->data, work->schema ? work->schema : "(null)", work->table, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", work->table, index);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX %s ON %s USING btree (%s)", name_quote, work->schema_table, index_quote);
    names = stringToQualifiedNameList(name_quote);
    relation = makeRangeVarFromNameList(names);
    SPI_connect_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    SPI_commit_my();
    SPI_finish_my(true);
    pfree((void *)relation);
    list_free_deep(names);
    pfree(buf.data);
    pfree(name.data);
    if (name_quote != name.data) pfree((void *)name_quote);
    if (index_quote != index) pfree((void *)index_quote);
}

static void tick_fix(Work *work) {
    StringInfoData buf;
    L("user = %s, data = %s, schema = %s, table = %s", work->user, work->data, work->schema ? work->schema : "(null)", work->table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "WITH s AS (SELECT id FROM %1$s AS t WHERE state IN ('TAKE'::state, 'WORK'::state) AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), \"group\", id)\n"
        ") FOR UPDATE SKIP LOCKED) UPDATE %1$s AS u SET state = 'PLAN'::state FROM s WHERE u.id = s.id", work->schema_table);
    SPI_connect_my(buf.data);
    SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UPDATE, true);
    SPI_finish_my(true);
    pfree(buf.data);
}

static void task_free(Task *task) {
    pfree(task->group);
    pfree(task);
}

static void tick_finish(Task *task) {
    queue_remove(&task->queue);
    PQfinish(task->conn);
    W(PQerrorMessage(task->conn));
    initStringInfo(&task->response);
    appendStringInfoString(&task->response, PQerrorMessage(task->conn));
    task_done(task);
    if (task->request) pfree(task->request);
    if (task->response.data) pfree(task->response.data);
    task_free(task);
}

static void task_remote(Work *work, int64 id, const char *group, int max, const char **keywords, const char **values) {
    Task *task;
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, group = %s, max = %u, oid = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, id, group, max, work->oid);
    if (!(task = MemoryContextAllocZero(TopMemoryContext, sizeof(*task)))) E("!MemoryContextAllocZero");
    task->work = work;
    task->id = id;
    task->group = MemoryContextStrdup(TopMemoryContext, group);
    task->max = max;
    task_work(task, false);
    L("id = %lu, timeout = %d, request = %s, count = %u", task->id, task->timeout, task->request, task->count);
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, group = %s, max = %u, oid = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, task->id, task->group, task->max, work->oid);
    task->conn = PQconnectStartParams(keywords, values, false);
    if (PQstatus(task->conn) == CONNECTION_BAD || (!PQisnonblocking(task->conn) && PQsetnonblocking(task->conn, true) == -1) || (task->fd = PQsocket(task->conn)) < 0) {
        tick_finish(task);
    } else {
        task->events = WL_SOCKET_WRITEABLE;
        task->state = CONNECT;
        queue_insert_tail(&work->queue, &task->queue);
    }
}

static void task_worker(Work *work, int64 id, const char *group, int max) {
    StringInfoData buf;
    int user_len = strlen(work->user), data_len = strlen(work->data), schema_len = work->schema ? strlen(work->schema) : 0, table_len = strlen(work->table), group_len = strlen(group), max_len = sizeof(max), oid_len = sizeof(work->oid);
    BackgroundWorker worker;
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, group = %s, max = %u, oid = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, id, group, max, work->oid);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = id;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "task_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s %s", work->schema ? work->schema : "", work->schema ? " " : "", work->table, group);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %s", work->user, work->data, work->schema ? work->schema : "", work->schema ? " " : "", work->table, group);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + group_len + 1 + max_len + oid_len > BGW_EXTRALEN) E("%u > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + group_len + 1 + max_len + oid_len);
    work->p = worker.bgw_extra;
    memcpy(work->p, work->user, user_len);
    work->p += user_len + 1;
    memcpy(work->p, work->data, data_len);
    work->p += data_len + 1;
    memcpy(work->p, work->schema, schema_len);
    work->p += schema_len + 1;
    memcpy(work->p, work->table, table_len);
    work->p += table_len + 1;
    *(typeof(work->oid) *)work->p = work->oid;
    work->p += oid_len;
    memcpy(work->p, group, group_len);
    work->p += group_len + 1;
    *(typeof(max) *)work->p = max;
    work->p += max_len;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static void tick_work(Work *work, int64 id, const char *group, int max) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    PQconninfoOption *opts = PQconninfoParse(group, NULL);
    MemoryContextSwitchTo(oldMemoryContext);
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, group = %s, max = %u, oid = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, id, group, max, work->oid);
    if (!opts) task_worker(work, id, group, max); else {
        const char **keywords;
        const char **values;
        StringInfoData buf;
        const char *application_name = "group";
        int arg = 2;
        for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
            if (!opt->val) continue;
            L("%s = %s", opt->keyword, opt->val);
            if (!pg_strncasecmp(opt->keyword, "fallback_application_name", sizeof("fallback_application_name") - 1)) continue;
            if (!pg_strncasecmp(opt->keyword, "application_name", sizeof("application_name") - 1)) { application_name = opt->val; continue; }
            arg++;
        }
        if (!(keywords = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*keywords)))) E("!MemoryContextAlloc");
        if (!(values = MemoryContextAlloc(TopMemoryContext, arg * sizeof(*values)))) E("!MemoryContextAlloc");
        initStringInfo(&buf);
        appendStringInfo(&buf, "pg_task %s%s%s %s", work->schema ? work->schema : "", work->schema ? " " : "", work->table, application_name);
        arg = 0;
        keywords[arg] = "application_name";
        values[arg] = buf.data;
        for (PQconninfoOption *opt = opts; opt->keyword; opt++) {
            if (!opt->val) continue;
            if (!pg_strncasecmp(opt->keyword, "fallback_application_name", sizeof("fallback_application_name") - 1)) continue;
            if (!pg_strncasecmp(opt->keyword, "application_name", sizeof("application_name") - 1)) continue;
            arg++;
            keywords[arg] = opt->keyword;
            values[arg] = opt->val;
        }
        arg++;
        keywords[arg] = NULL;
        values[arg] = NULL;
        task_remote(work, id, group, max, keywords, values);
        pfree(buf.data);
        pfree(keywords);
        pfree(values);
        PQconninfoFree(opts);
    }
}

void tick_loop(Work *work) {
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (WITH s AS (WITH s AS (WITH s AS (WITH s AS (\n"
            "SELECT      id, \"group\", COALESCE(max, ~(1<<31)) AS max, a.pid\n"
            "FROM        %1$s AS t\n"
            "LEFT JOIN   pg_stat_activity AS a\n"
            "ON          datname = current_catalog\n"
            "AND         usename = current_user\n"
            "AND         application_name = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), \"group\")\n"
            "WHERE       t.state = 'PLAN'::state\n"
            "AND         dt <= current_timestamp\n"
            ") SELECT id, \"group\", max - count(pid) AS count FROM s GROUP BY id, \"group\", max\n"
            ") SELECT array_agg(id ORDER BY id) AS id, \"group\", count FROM s WHERE count > 0 GROUP BY \"group\", count\n"
            ") SELECT unnest(id[:count]) AS id, \"group\", count FROM s ORDER BY count DESC\n"
            ") SELECT s.* FROM s INNER JOIN %1$s USING (id) FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %1$s AS u SET state = 'TAKE'::state FROM s WHERE u.id = s.id RETURNING u.id, u.group, COALESCE(u.max, ~(1<<31)) AS max", work->schema_table);
        command = buf.data;
    }
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_UPDATE_RETURNING, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, max_isnull;
        int64 id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull));
        char *group = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "group"));
        int max = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) E("id_isnull");
        if (max_isnull) E("max_isnull");
        tick_work(work, id, group, max);
        pfree(group);
    }
    SPI_finish_my(true);
}

static void tick_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void tick_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void tick_check(void) {
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS ("
        "SELECT      COALESCE(COALESCE(usename, \"user\"), data)::text AS user,\n"
        "            COALESCE(datname, data)::text AS data,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::int4) AS period\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.config', false)::json) AS s (\"user\" text, data text, schema text, \"table\" text, period int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT DISTINCT * FROM s WHERE \"user\" = current_user AND data = current_catalog AND schema IS NOT DISTINCT FROM NULLIF(current_setting('pg_task.schema', true), '') AND \"table\" = current_setting('pg_task.table', false) AND period = current_setting('pg_task.period', false)::int4";
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    if (!SPI_processed) sigterm = true;
    SPI_finish_my(true);
}

static void tick_init_conf(Work *work) {
    work->p = MyBgworkerEntry->bgw_extra;
    work->user = work->p;
    work->p += strlen(work->user) + 1;
    work->data = work->p;
    work->p += strlen(work->data) + 1;
    work->schema = work->p;
    work->p += strlen(work->schema) + 1;
    work->table = work->p;
    work->p += strlen(work->table) + 1;
    work->period = *(typeof(work->period) *)work->p;
    work->p += sizeof(work->period);
    if (work->table == work->schema + 1) work->schema = NULL;
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    if (!MyProcPort->user_name) MyProcPort->user_name = work->user;
    if (!MyProcPort->database_name) MyProcPort->database_name = work->data;
    SetConfigOptionMy("application_name", MyBgworkerEntry->bgw_type);
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, work->period);
    pqsignal(SIGHUP, tick_sighup);
    pqsignal(SIGTERM, tick_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(work->data, work->user, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
}

bool tick_init_work(Work *work) {
    const char *schema_quote = work->schema ? quote_identifier(work->schema) : NULL;
    const char *table_quote = quote_identifier(work->table);
    StringInfoData buf;
    initStringInfo(&buf);
    if (work->schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    if (work->schema_table) pfree(work->schema_table);
    work->schema_table = buf.data;
    if (work->schema && schema_quote && work->schema != schema_quote) pfree((void *)schema_quote);
    if (work->table != table_quote) pfree((void *)table_quote);
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", work->user, work->data, work->schema ? work->schema : "(null)", work->table, work->period);
    if (work->schema) tick_schema(work);
    tick_type(work);
    tick_table(work);
    tick_index(work, "dt");
    tick_index(work, "state");
    SetConfigOptionMy("pg_task.data", work->data);
    SetConfigOptionMy("pg_task.user", work->user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%d", work->period);
    SetConfigOptionMy("pg_task.period", buf.data);
    pfree(buf.data);
    if (!pg_try_advisory_lock_int8_my(work->oid)) { W("lock oid = %d", work->oid); return true; }
    tick_fix(work);
    queue_init(&work->queue);
    return false;
}

static void tick_reset(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
}

static void tick_reload(void) {
    sighup = false;
    ProcessConfigFile(PGC_SIGHUP);
    tick_check();
}

static void tick_sucess(Task *task, PGresult *result) {
    if (!task->response.data) initStringInfo(&task->response);
    if (task->response.len) appendStringInfoString(&task->response, "\n");
    if (task->response.len || PQnfields(result) > 1) {
        for (int col = 0; col < PQnfields(result); col++) {
            Oid oid = PQftype(result, col);
            const char *type = PQftypeMy(oid);
            if (col > 0) appendStringInfoString(&task->response, "\t");
            if (type) appendStringInfo(&task->response, "%s::%s", PQfname(result, col), type);
            else appendStringInfo(&task->response, "%s::%d", PQfname(result, col), oid);
        }
    }
    if (task->response.len) appendStringInfoString(&task->response, "\n");
    for (int row = 0; row < PQntuples(result); row++) {
        for (int col = 0; col < PQnfields(result); col++) {
            if (col > 1) appendStringInfoString(&task->response, "\t");
            appendStringInfoString(&task->response, PQgetisnull(result, row, col) ? "(null)" : PQgetvalue(result, row, col));
        }
    }
    task->success = true;
}

static void tick_error(Task *task, PGresult *result) {
    char *value;
    initStringInfo(&task->response);
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY))) appendStringInfo(&task->response, "severity::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SEVERITY_NONLOCALIZED))) appendStringInfo(&task->response, "\nseverity_nonlocalized::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SQLSTATE))) appendStringInfo(&task->response, "\nsqlstate::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY))) appendStringInfo(&task->response, "\nmessage_primary::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL))) appendStringInfo(&task->response, "\nmessage_detail::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT))) appendStringInfo(&task->response, "\nmessage_hint::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_STATEMENT_POSITION))) appendStringInfo(&task->response, "\nstatement_position::int4\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_POSITION))) appendStringInfo(&task->response, "\ninternal_position::int4\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_INTERNAL_QUERY))) appendStringInfo(&task->response, "\ninternal_query::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_CONTEXT))) appendStringInfo(&task->response, "\ncontext::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SCHEMA_NAME))) appendStringInfo(&task->response, "\nschema_name::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_TABLE_NAME))) appendStringInfo(&task->response, "\ntable_name::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_COLUMN_NAME))) appendStringInfo(&task->response, "\ncolumn_name::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_DATATYPE_NAME))) appendStringInfo(&task->response, "\ndatatype_name::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_CONSTRAINT_NAME))) appendStringInfo(&task->response, "\nconstraint_name::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FILE))) appendStringInfo(&task->response, "\nsource_file::text\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_LINE))) appendStringInfo(&task->response, "\nsource_line::int4\t%s", value);
    if ((value = PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION))) appendStringInfo(&task->response, "\nsource_function::text\t%s", value);
}

static void tick_idle(Task *task) {
}

static void tick_query(Task *task) {
    if (!PQconsumeInput(task->conn)) { tick_finish(task); return; }
    if (PQisBusy(task->conn)) { W("PQisBusy"); return; }
    if (task->timeout) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "SET statement_timeout = %d;\n%s", task->timeout, task->request);
        pfree(task->request);
        task->request = buf.data;
    }
    if (!PQsendQuery(task->conn, task->request)) { tick_finish(task); return; }
    pfree(task->request);
    task->request = NULL;
    task->events = WL_SOCKET_WRITEABLE;
    task->state = RESULT;
}

static void tick_result(Task *task) {
    if (!PQconsumeInput(task->conn)) { tick_finish(task); return; }
    for (PGresult *result; (result = PQgetResult(task->conn)); PQclear(result)) {
        if (PQresultStatus(result) == PGRES_FATAL_ERROR) tick_error(task, result); else {
            L(PQcmdStatus(result));
            L(PQcmdTuples(result));
            L(PQresStatus(PQresultStatus(result)));
            if (!strlen(PQcmdStatus(result))) continue;
            if (!strlen(PQcmdTuples(result))) continue;
            if (!pg_strncasecmp(PQcmdTuples(result), "0", sizeof("0") - 1)) continue;
            switch (PQresultStatus(result)) {
                case PGRES_BAD_RESPONSE: break;
                case PGRES_COMMAND_OK: break;
                case PGRES_COPY_BOTH: break;
                case PGRES_COPY_IN: break;
                case PGRES_COPY_OUT: break;
                case PGRES_EMPTY_QUERY: break;
                case PGRES_FATAL_ERROR: break;
                case PGRES_NONFATAL_ERROR: break;
                case PGRES_SINGLE_TUPLE: break;
                case PGRES_TUPLES_OK: tick_sucess(task, result); break;
            }
        }
    }
    task->state = IDLE;
    task_done(task);
    L("repeat = %s, delete = %s, live = %s", task->repeat ? "true" : "false", task->delete ? "true" : "false", task->delete ? "true" : "false");
    if (task->repeat) task_repeat(task);
    if (task->delete && !task->response.data) task_delete(task);
    if (task->response.data) pfree(task->response.data);
    task->response.data = NULL;
    if (task->live && task_live(task)) tick_query(task); else {
        queue_remove(&task->queue);
        PQfinish(task->conn);
        task_free(task);
    }
}

static void tick_connect(Task *task) {
    switch (PQstatus(task->conn)) {
        case CONNECTION_AUTH_OK: L("PQstatus == CONNECTION_AUTH_OK"); break;
        case CONNECTION_AWAITING_RESPONSE: L("PQstatus == CONNECTION_AWAITING_RESPONSE"); break;
        case CONNECTION_BAD: E("PQstatus == CONNECTION_BAD"); tick_finish(task); return;
        case CONNECTION_CHECK_WRITABLE: L("PQstatus == CONNECTION_CHECK_WRITABLE"); break;
        case CONNECTION_CONSUME: L("PQstatus == CONNECTION_CONSUME"); break;
        case CONNECTION_GSS_STARTUP: L("PQstatus == CONNECTION_GSS_STARTUP"); break;
        case CONNECTION_MADE: L("PQstatus == CONNECTION_MADE"); break;
        case CONNECTION_NEEDED: L("PQstatus == CONNECTION_NEEDED"); break;
        case CONNECTION_OK: L("PQstatus == CONNECTION_OK"); task->state = QUERY; task->pid = PQbackendPID(task->conn); tick_query(task); return;
        case CONNECTION_SETENV: L("PQstatus == CONNECTION_SETENV"); break;
        case CONNECTION_SSL_STARTUP: L("PQstatus == CONNECTION_SSL_STARTUP"); break;
        case CONNECTION_STARTED: L("PQstatus == CONNECTION_STARTED"); break;
    }
    switch (PQconnectPoll(task->conn)) {
        case PGRES_POLLING_ACTIVE: L("PQconnectPoll == PGRES_POLLING_ACTIVE"); break;
        case PGRES_POLLING_FAILED: E("PQconnectPoll == PGRES_POLLING_FAILED"); tick_finish(task); return;
        case PGRES_POLLING_OK: L("PQconnectPoll == PGRES_POLLING_OK"); task->state = QUERY; task->pid = PQbackendPID(task->conn); tick_query(task); return;
        case PGRES_POLLING_READING: L("PQconnectPoll == PGRES_POLLING_READING"); task->events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: L("PQconnectPoll == PGRES_POLLING_WRITING"); task->events = WL_SOCKET_WRITEABLE; break;
    }
    if ((task->fd = PQsocket(task->conn)) < 0) tick_finish(task);
}

static void tick_socket(Task *task) {
    switch (task->state) {
        case CONNECT: tick_connect(task); break;
        case QUERY: tick_query(task); break;
        case RESULT: tick_result(task); break;
        case IDLE: tick_idle(task); break;
    }
}

void tick_worker(Datum main_arg); void tick_worker(Datum main_arg) {
    Work work;
    MemSet(&work, 0, sizeof(work));
    tick_init_conf(&work);
    sigterm = tick_init_work(&work);
    while (!sigterm) {
        WaitEvent event = {.events = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH};
        int rc = WaitLatchOrSocketMy(MyLatch, &event, &work.queue, work.period, PG_WAIT_EXTENSION);
        if (!BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid)) break;
        if (rc & WL_LATCH_SET) tick_reset();
        if (sighup) tick_reload();
        if (rc & WL_TIMEOUT) tick_loop(&work);
        if (rc & WL_SOCKET_MASK) tick_socket(event.user_data);
    }
}
