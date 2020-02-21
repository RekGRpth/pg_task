#include "include.h"

Work work;
static volatile sig_atomic_t sighup = false;
static volatile sig_atomic_t sigterm = false;

static void tick_schema(Conf *conf) {
    StringInfoData buf;
    List *names;
    const char *schema_quote = quote_identifier(conf->schema);
    L("user = %s, data = %s, schema = %s, table = %s", conf->user, conf->data, conf->schema, conf->table);
    SetConfigOptionMy("pg_task.schema", conf->schema);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_connect_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    SPI_finish_my(buf.data);
    list_free_deep(names);
    if (schema_quote != conf->schema) pfree((void *)schema_quote);
    pfree(buf.data);
    SetConfigOptionMy("pg_task.schema", conf->schema);
}

static void tick_type(Conf *conf) {
    StringInfoData buf, name;
    Oid type = InvalidOid;
    int32 typmod;
    const char *schema_quote = conf->schema ? quote_identifier(conf->schema) : NULL;
    L("user = %s, data = %s, schema = %s, table = %s", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table);
    initStringInfo(&name);
    if (schema_quote) appendStringInfo(&name, "%s.", schema_quote);
    appendStringInfoString(&name, "state");
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP')", name.data);
    SPI_connect_my(buf.data);
    parseTypeString(name.data, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    SPI_finish_my(buf.data);
    if (conf->schema && schema_quote && conf->schema != schema_quote) pfree((void *)schema_quote);
    pfree(name.data);
    pfree(buf.data);
}

static void tick_table(Work *work) {
    Conf *conf = &work->conf;
    StringInfoData buf, name;
    List *names;
    const RangeVar *relation;
    const char *name_quote;
    L("user = %s, data = %s, schema = %s, table = %s", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table);
    if (work->oid) pg_advisory_unlock_int8_my(work->oid);
    SetConfigOptionMy("pg_task.table", conf->table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", conf->table);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE %1$s (\n"
        "    id bigserial NOT NULL PRIMARY KEY,\n"
        "    parent int8 DEFAULT current_setting('pg_task.id', true)::int8,\n"
        "    dt timestamp NOT NULL DEFAULT current_timestamp,\n"
        "    start timestamp,\n"
        "    stop timestamp,\n"
        "    queue text NOT NULL DEFAULT 'queue',\n"
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
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    work->oid = RangeVarGetRelid(relation, NoLock, false);
    SPI_commit_my(buf.data);
    SPI_finish_my(buf.data);
    pfree((void *)relation);
    list_free_deep(names);
    if (name_quote != name.data) pfree((void *)name_quote);
    pfree(name.data);
    SetConfigOptionMy("pg_task.table", conf->table);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%d", work->oid);
    SetConfigOptionMy("pg_task.oid", buf.data);
    pfree(buf.data);
}

static void tick_index(Work *work, const char *index) {
    Conf *conf = &work->conf;
    StringInfoData buf, name;
    List *names;
    const RangeVar *relation;
    const char *name_quote;
    const char *index_quote = quote_identifier(index);
    L("user = %s, data = %s, schema = %s, table = %s, index = %s", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", conf->table, index);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX %s ON %s USING btree (%s)", name_quote, work->schema_table, index_quote);
    names = stringToQualifiedNameList(name_quote);
    relation = makeRangeVarFromNameList(names);
    SPI_connect_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(relation, NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    SPI_finish_my(buf.data);
    pfree((void *)relation);
    list_free_deep(names);
    pfree(buf.data);
    pfree(name.data);
    if (name_quote != name.data) pfree((void *)name_quote);
    if (index_quote != index) pfree((void *)index_quote);
}

static void tick_fix(Work *work) {
    Conf *conf = &work->conf;
    StringInfoData buf;
    L("user = %s, data = %s, schema = %s, table = %s", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "WITH s AS (SELECT id FROM %1$s AS t WHERE state IN ('TAKE'::state, 'WORK'::state) AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), queue, id)\n"
        ") FOR UPDATE SKIP LOCKED) UPDATE %1$s AS u SET state = 'PLAN'::state FROM s WHERE u.id = s.id", work->schema_table);
    SPI_connect_my(buf.data);
    SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UPDATE);
    SPI_commit_my(buf.data);
    SPI_finish_my(buf.data);
    pfree(buf.data);
}

static void task_remote(Task *task) {
    Work *work = &task->work;
    Conf *conf = &work->conf;
    Remote *remote;
    MemoryContext oldMemoryContext;
    task_work(task);
    L("id = %lu, timeout = %d, request = %s, count = %u", task->id, task->timeout, task->request, task->count);
    oldMemoryContext = MemoryContextSwitchTo(work->context);
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u, oid = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, task->id, task->queue, task->max, work->oid);
    if (!(remote = palloc0(sizeof(remote)))) E("!palloc");
    remote->task = *task;
    remote->event.events = WL_SOCKET_WRITEABLE;
    if (!(remote->conn = PQconnectStart(task->queue))) E("!PQconnectStart");
    if (PQstatus(remote->conn) == CONNECTION_BAD) E("PQstatus == CONNECTION_BAD, %s", PQerrorMessage(remote->conn));
    if (!PQisnonblocking(remote->conn) && PQsetnonblocking(remote->conn, true) == -1) E(PQerrorMessage(remote->conn));
    if ((remote->event.fd = PQsocket(remote->conn)) < 0) E("PQsocket < 0, %s", PQerrorMessage(remote->conn));
    queue_insert_tail(work->queue, &remote->queue);
    MemoryContextSwitchTo(oldMemoryContext);
}

static void task_worker(Task *task) {
    Work *work = &task->work;
    Conf *conf = &work->conf;
    StringInfoData buf;
    int user_len = strlen(conf->user), data_len = strlen(conf->data), schema_len = conf->schema ? strlen(conf->schema) : 0, table_len = strlen(conf->table), queue_len = strlen(task->queue), max_len = sizeof(task->max), oid_len = sizeof(work->oid);
    BackgroundWorker worker;
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u, oid = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, task->id, task->queue, task->max, work->oid);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = task->id;
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
    appendStringInfo(&buf, "pg_task %s%s%s %s", conf->schema ? conf->schema : "", conf->schema ? " " : "", conf->table, task->queue);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %s", conf->user, conf->data, conf->schema ? conf->schema : "", conf->schema ? " " : "", conf->table, task->queue);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len + oid_len > BGW_EXTRALEN) E("%u > BGW_EXTRALEN", user_len + 1 + data_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len + oid_len);
    conf->p = worker.bgw_extra;
    memcpy(conf->p, conf->user, user_len);
    conf->p += user_len + 1;
    memcpy(conf->p, conf->data, data_len);
    conf->p += data_len + 1;
    memcpy(conf->p, conf->schema, schema_len);
    conf->p += schema_len + 1;
    memcpy(conf->p, conf->table, table_len);
    conf->p += table_len + 1;
    *(typeof(work->oid) *)conf->p = work->oid;
    conf->p += oid_len;
    memcpy(conf->p, task->queue, queue_len);
    conf->p += queue_len + 1;
    *(typeof(task->max) *)conf->p = task->max;
    conf->p += max_len;
    RegisterDynamicBackgroundWorker_my(&worker);
}

static void tick_work(Task *task) {
    Work *work = &task->work;
    Conf *conf = &work->conf;
    PQconninfoOption *opts;
    L("user = %s, data = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u, oid = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, task->id, task->queue, task->max, work->oid);
    if (!(opts = PQconninfoParse(task->queue, NULL))) task_worker(task); else {
        task_remote(task);
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
            "SELECT      id, queue, COALESCE(max, ~(1<<31)) AS max, a.pid\n"
            "FROM        %1$s AS t\n"
            "LEFT JOIN   pg_stat_activity AS a\n"
            "ON          datname = current_catalog\n"
            "AND         usename = current_user\n"
            "AND         backend_type = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), queue)\n"
            "WHERE       t.state = 'PLAN'::state\n"
            "AND         dt <= current_timestamp\n"
            ") SELECT id, queue, max - count(pid) AS count FROM s GROUP BY id, queue, max\n"
            ") SELECT array_agg(id ORDER BY id) AS id, queue, count FROM s WHERE count > 0 GROUP BY queue, count\n"
            ") SELECT unnest(id[:count]) AS id, queue, count FROM s ORDER BY count DESC\n"
            ") SELECT s.* FROM s INNER JOIN %1$s USING (id) FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %1$s AS u SET state = 'TAKE'::state FROM s WHERE u.id = s.id RETURNING u.id, u.queue, COALESCE(u.max, ~(1<<31)) AS max", work->schema_table);
        command = buf.data;
    }
    SPI_connect_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_UPDATE_RETURNING);
    SPI_commit_my(command);
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, max_isnull;
        Task task;
        MemSet(&task, 0, sizeof(task));
        task.work = *work;
        task.id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull));
        task.queue = SPI_getvalue_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"));
        task.max = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) E("id_isnull");
        if (max_isnull) E("max_isnull");
        tick_work(&task);
        pfree(task.queue);
    }
    SPI_finish_my(command);
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
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT);
    if (!SPI_processed) sigterm = true;
    SPI_commit_my(command);
    SPI_finish_my(command);
}

static void tick_init_conf(Conf *conf) {
    conf->p = MyBgworkerEntry->bgw_extra;
    conf->user = conf->p;
    conf->p += strlen(conf->user) + 1;
    conf->data = conf->p;
    conf->p += strlen(conf->data) + 1;
    conf->schema = conf->p;
    conf->p += strlen(conf->schema) + 1;
    conf->table = conf->p;
    conf->p += strlen(conf->table) + 1;
    conf->period = *(typeof(conf->period) *)conf->p;
    conf->p += sizeof(conf->period);
    if (conf->table == conf->schema + 1) conf->schema = NULL;
    if (!MessageContext) MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext", ALLOCSET_DEFAULT_SIZES);
    if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
    if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
    if (!MyProcPort->user_name) MyProcPort->user_name = conf->user;
    if (!MyProcPort->database_name) MyProcPort->database_name = conf->data;
    SetConfigOptionMy("application_name", MyBgworkerEntry->bgw_type);
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, conf->period);
    pqsignal(SIGHUP, tick_sighup);
    pqsignal(SIGTERM, tick_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(conf->data, conf->user, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
}

bool tick_init_work(const bool is_conf, Work *work) {
    Conf *conf = &work->conf;
    const char *schema_quote = conf->schema ? quote_identifier(conf->schema) : NULL;
    const char *table_quote = quote_identifier(conf->table);
    StringInfoData buf;
    initStringInfo(&buf);
    if (conf->schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    if (work->schema_table) pfree(work->schema_table);
    work->schema_table = buf.data;
    if (conf->schema && schema_quote && conf->schema != schema_quote) pfree((void *)schema_quote);
    if (conf->table != table_quote) pfree((void *)table_quote);
    L("user = %s, data = %s, schema = %s, table = %s, period = %d", conf->user, conf->data, conf->schema ? conf->schema : "(null)", conf->table, conf->period);
    if (conf->schema) tick_schema(conf);
    tick_type(conf);
    tick_table(work);
    tick_index(work, "dt");
    tick_index(work, "state");
    SetConfigOptionMy("pg_task.data", conf->data);
    SetConfigOptionMy("pg_task.user", conf->user);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%d", conf->period);
    SetConfigOptionMy("pg_task.period", buf.data);
    pfree(buf.data);
    if (!pg_try_advisory_lock_int8_my(work->oid)) { W("lock oid = %d", work->oid); return true; }
    tick_fix(work);
    if (!work->context) work->context = AllocSetContextCreate(TopMemoryContext, "myMemoryContext", ALLOCSET_DEFAULT_SIZES);
    if (!work->queue && !(work->queue = palloc0(sizeof(work->queue)))) E("!palloc0");
    queue_init(work->queue);
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
//    Work *work = &task->work;
//    MemoryContext oldMemoryContext = MemoryContextSwitchTo(work->context);
    StringInfoData *buf = &task->response;
    initStringInfo(buf);
    if (PQnfields(result) > 1) {
        for (int col = 0; col < PQnfields(result); col++) {
            if (col > 0) appendStringInfoString(buf, "\t");
            appendStringInfo(buf, "%s::%s", PQfname(result, col), PQftypeMy(result, col));
        }
    }
    if (buf->len) appendStringInfoString(buf, "\n");
    for (int row = 0; row < PQntuples(result); row++) {
        for (int col = 0; col < PQnfields(result); col++) {
            if (col > 1) appendStringInfoString(buf, "\t");
            appendStringInfoString(buf, PQgetisnull(result, row, col) ? "(null)" : PQgetvalue(result, row, col));
        }
    }
    L("response = %s", buf->data);
//    MemoryContextSwitchTo(oldMemoryContext);
}

static void tick_socket(Remote *remote) {
    Task *task = &remote->task;
//    Work *work = &task->work;
//    MemoryContext oldMemoryContext = MemoryContextSwitchTo(work->context);
    switch (PQstatus(remote->conn)) {
        case CONNECTION_AUTH_OK: L("PQstatus == CONNECTION_AUTH_OK"); break;
        case CONNECTION_AWAITING_RESPONSE: L("PQstatus == CONNECTION_AWAITING_RESPONSE"); break;
        case CONNECTION_BAD: E("PQstatus == CONNECTION_BAD");
        case CONNECTION_CHECK_WRITABLE: L("PQstatus == CONNECTION_CHECK_WRITABLE"); break;
        case CONNECTION_CONSUME: L("PQstatus == CONNECTION_CONSUME"); break;
        case CONNECTION_GSS_STARTUP: L("PQstatus == CONNECTION_GSS_STARTUP"); break;
        case CONNECTION_MADE: L("PQstatus == CONNECTION_MADE"); break;
        case CONNECTION_NEEDED: L("PQstatus == CONNECTION_NEEDED"); break;
        case CONNECTION_OK: L("PQstatus == CONNECTION_OK"); goto ok;
        case CONNECTION_SETENV: L("PQstatus == CONNECTION_SETENV"); break;
        case CONNECTION_SSL_STARTUP: L("PQstatus == CONNECTION_SSL_STARTUP"); break;
        case CONNECTION_STARTED: L("PQstatus == CONNECTION_STARTED"); break;
    }
    switch (PQconnectPoll(remote->conn)) {
        case PGRES_POLLING_ACTIVE: L("PQconnectPoll == PGRES_POLLING_ACTIVE"); goto done;
        case PGRES_POLLING_FAILED: E("PQconnectPoll == PGRES_POLLING_FAILED");
        case PGRES_POLLING_OK: L("PQconnectPoll == PGRES_POLLING_OK"); goto ok;
        case PGRES_POLLING_READING: L("PQconnectPoll == PGRES_POLLING_READING"); remote->event.events = WL_SOCKET_READABLE; break;
        case PGRES_POLLING_WRITING: L("PQconnectPoll == PGRES_POLLING_WRITING"); remote->event.events = WL_SOCKET_WRITEABLE; break;
    }
    if ((remote->event.fd = PQsocket(remote->conn)) < 0) E("PQsocket < 0");
    goto done;
ok:
    if (remote->event.events & WL_SOCKET_READABLE) {
        if (!remote->send) {
            L("id = %lu, timeout = %d, request = %s", task->id, task->timeout, task->request);
            if (!PQsendQuery(remote->conn, task->request)) E("!PQsendQuery, %s", PQerrorMessage(remote->conn));
            remote->event.events = WL_SOCKET_WRITEABLE;
            remote->send = true;
        } else {
            if (!PQconsumeInput(remote->conn)) E("!PQconsumeInput, %s", PQerrorMessage(remote->conn));
            if (!PQisBusy(remote->conn)) queue_remove(&remote->queue);
            for (PGresult *result; (result = PQgetResult(remote->conn)); PQclear(result)) switch (PQresultStatus(result)) {
                case PGRES_BAD_RESPONSE: L("PGRES_BAD_RESPONSE"); break;
                case PGRES_COMMAND_OK: L("PGRES_COMMAND_OK"); break;
                case PGRES_COPY_BOTH: L("PGRES_COPY_BOTH"); break;
                case PGRES_COPY_IN: L("PGRES_COPY_IN"); break;
                case PGRES_COPY_OUT: L("PGRES_COPY_OUT"); break;
                case PGRES_EMPTY_QUERY: L("PGRES_EMPTY_QUERY"); break;
                case PGRES_FATAL_ERROR: L("PGRES_FATAL_ERROR"); break;
                case PGRES_NONFATAL_ERROR: L("PGRES_NONFATAL_ERROR"); break;
                case PGRES_SINGLE_TUPLE: L("PGRES_SINGLE_TUPLE"); break;
                case PGRES_TUPLES_OK: L("PGRES_TUPLES_OK"); {
                    tick_sucess(task, result);
                    pfree(task->request);
//                    task_done(task);
//                    L("repeat = %s, delete = %s, live = %s", task->repeat ? "true" : "false", task->delete ? "true" : "false", task->delete ? "true" : "false");
                } goto done;
            }
        }
    }
    if (remote->event.events & WL_SOCKET_WRITEABLE) {
        switch (PQflush(remote->conn)) {
            case 0: L("PQflush = 0"); remote->event.events = WL_SOCKET_READABLE; break;
            case 1: L("PQflush = 1"); break;
            default: E("PQflush");
        }
    }
done:;
//    MemoryContextSwitchTo(oldMemoryContext);
}

void tick_worker(Datum main_arg); void tick_worker(Datum main_arg) {
    Work work;
    MemSet(&work, 0, sizeof(work));
    tick_init_conf(&work.conf);
    sigterm = tick_init_work(false, &work);
    while (!sigterm) {
        WaitEvent event;
        int rc = WaitLatchOrSocketMy(MyLatch, &event, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, work.queue, work.conf.period, PG_WAIT_EXTENSION);
        if (!BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid)) break;
        if (rc & WL_LATCH_SET) tick_reset();
        if (sighup) tick_reload();
        if (rc & WL_TIMEOUT) tick_loop(&work);
        if (rc & WL_SOCKET_MASK) tick_socket(event.user_data);
    }
}
