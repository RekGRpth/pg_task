#include "include.h"

static const char *data;
static const char *data_quote = NULL;
static const char *point;
static const char *schema;
static const char *schema_quote = NULL;
static const char *schema_quote_point_table_quote = NULL;
static const char *table;
static const char *table_quote = NULL;
static const char *user;
static const char *user_quote = NULL;
static int period;
static Oid oid = 0;
static volatile sig_atomic_t sighup = false;
static volatile sig_atomic_t sigterm = false;

static void tick_schema(void) {
    StringInfoData buf;
    List *names;
    L("data = %s, user = %s, schema = %s, table = %s", data, user, schema ? schema : "(null)", table);
    set_config_option("pg_task.schema", schema, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA %s", schema_quote);
    names = stringToQualifiedNameList(schema_quote);
    SPI_begin_my(buf.data);
    if (!OidIsValid(get_namespace_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    list_free_deep(names);
    pfree(buf.data);
}

static void tick_type(void) {
    StringInfoData buf, name;
    Oid type = InvalidOid;
    int32 typmod;
    L("data = %s, user = %s, schema = %s, table = %s", data, user, schema ? schema : "(null)", table);
    initStringInfo(&name);
    if (schema) appendStringInfo(&name, "%s.", schema_quote);
    appendStringInfoString(&name, "state");
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE TYPE %s AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP')", name.data);
    SPI_begin_my(buf.data);
    parseTypeString(name.data, &type, &typmod, true);
    if (!OidIsValid(type)) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    pfree(name.data);
    pfree(buf.data);
}

static void tick_table(void) {
    StringInfoData buf, name;
    List *names;
    const char *name_quote;
    L("data = %s, user = %s, schema = %s, table = %s", data, user, schema ? schema : "(null)", table);
    if (oid) pg_advisory_unlock_int8_my(oid);
    set_config_option("pg_task.table", table, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", table);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE %1$s (\n"
        "    id bigserial NOT NULL PRIMARY KEY,\n"
        "    parent int8 DEFAULT current_setting('pg_task.id', true)::int8,\n"
        "    dt timestamp NOT NULL DEFAULT current_timestamp,\n"
        "    start timestamp,\n"
        "    stop timestamp,\n"
        "    queue text NOT NULL DEFAULT 'default',\n"
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
        ")", schema_quote_point_table_quote, name_quote);
    names = stringToQualifiedNameList(schema_quote_point_table_quote);
    SPI_begin_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    oid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);
    SPI_commit_my(buf.data);
    list_free_deep(names);
    if (name_quote != name.data) pfree((void *)name_quote);
    pfree(name.data);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%d", oid);
    set_config_option("pg_task.oid", buf.data, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    pfree(buf.data);
}

static void tick_index(const char *index) {
    StringInfoData buf, name;
    List *names;
    const char *name_quote;
    const char *index_quote = quote_identifier(index);
    L("data = %s, user = %s, schema = %s, table = %s, index = %s", data, user, schema ? schema : "(null)", table, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", table, index);
    name_quote = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX %s ON %s USING btree (%s)", name_quote, schema_quote_point_table_quote, index_quote);
    names = stringToQualifiedNameList(name_quote);
    SPI_begin_my(buf.data);
    if (!OidIsValid(RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true))) SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_commit_my(buf.data);
    list_free_deep(names);
    pfree(buf.data);
    pfree(name.data);
    if (name_quote != name.data) pfree((void *)name_quote);
    if (index_quote != index) pfree((void *)index_quote);
}

static void tick_fix(void) {
    StringInfoData buf;
    L("data = %s, user = %s, schema = %s, table = %s", data, user, schema ? schema : "(null)", table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "WITH s AS (SELECT id FROM %1$s AS t WHERE state IN ('TAKE'::state, 'WORK'::state) AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), queue, id)\n"
        ") FOR UPDATE SKIP LOCKED) UPDATE %1$s AS u SET state = 'PLAN'::state FROM s WHERE u.id = s.id", schema_quote_point_table_quote);
    SPI_begin_my(buf.data);
    SPI_execute_with_args_my(buf.data, 0, NULL, NULL, NULL, SPI_OK_UPDATE);
    SPI_commit_my(buf.data);
    pfree(buf.data);
}

static void task_worker(const Datum id, const char *queue, const int max) {
    StringInfoData buf;
    int data_len = strlen(data), user_len = strlen(user), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), queue_len = strlen(queue), max_len = sizeof(max), oid_len = sizeof(oid);
    BackgroundWorker worker;
    L("data = %s, user = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u, oid = %d", data, user, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max, oid);
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
    appendStringInfo(&buf, "pg_task %s%s%s %s", schema ? schema : "", schema ? " " : "", table, queue);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %s", user, data, schema ? schema : "", schema ? " " : "", table, queue);
    if (buf.len + 1 > BGW_MAXLEN) E("%u > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len + oid_len > BGW_EXTRALEN) E("%u > BGW_EXTRALEN", data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len + oid_len);
    memcpy(worker.bgw_extra, data, data_len);
    memcpy(worker.bgw_extra + data_len + 1, user, user_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1, schema, schema_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1, table, table_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1, queue, queue_len);
    *(typeof(max + 0) *)(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1) = max;
    *(typeof(oid) *)(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len) = oid;
    RegisterDynamicBackgroundWorker_my(&worker);
}

void tick_loop(void) {
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
            ") UPDATE %1$s AS u SET state = 'TAKE'::state FROM s WHERE u.id = s.id RETURNING u.id, u.queue, COALESCE(u.max, ~(1<<31)) AS max", schema_quote_point_table_quote);
        command = buf.data;
    }
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_UPDATE_RETURNING);
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, max_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        const char *queue = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"));
        const int max = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) E("id_isnull");
        if (max_isnull) E("max_isnull");
        task_worker(id, queue, max);
        pfree((void *)queue);
    }
    SPI_commit_my(command);
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
        "SELECT      COALESCE(datname, data)::text AS data,\n"
        "            COALESCE(COALESCE(usename, \"user\"), data)::text AS user,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::int4) AS period\n"
        "FROM        json_populate_recordset(NULL::record, current_setting('pg_task.config', false)::json) AS s (data text, \"user\" text, schema text, \"table\" text, period int4)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT * FROM s WHERE data = current_catalog AND \"user\" = current_user AND schema IS NOT DISTINCT FROM NULLIF(current_setting('pg_task.schema', true), '') AND \"table\" = current_setting('pg_task.table', false) AND period = current_setting('pg_task.period', false)::int4";
    L("data = %s, user = %s, schema = %s, table = %s, period = %d", data, user, schema ? schema : "(null)", table, period);
    SPI_begin_my(command);
    if (!plan) plan = SPI_prepare_my(command, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT);
    if (!SPI_processed) sigterm = true;
    SPI_commit_my(command);
}

void tick_init(const bool conf, const char *_data, const char *_user, const char *_schema, const char *_table, int _period) {
    StringInfoData buf;
    data = _data; user = _user; schema = _schema; table = _table; period = _period;
    if (!conf) {
        if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) E("!calloc");
        if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
        if (!MyProcPort->database_name) MyProcPort->database_name = (char *)data;
        if (!MyProcPort->user_name) MyProcPort->user_name = (char *)user;
    }
    if (data_quote && data_quote != data) pfree((void *)data_quote);
    data_quote = quote_identifier(data);
    if (user_quote && user_quote != user) pfree((void *)user_quote);
    user_quote = quote_identifier(user);
    if (schema_quote && schema_quote != schema) pfree((void *)schema_quote);
    schema_quote = schema ? quote_identifier(schema) : NULL;
    point = schema ? "." : "";
    if (table_quote && table_quote != table) pfree((void *)table_quote);
    table_quote = quote_identifier(table);
    if (schema_quote_point_table_quote) pfree((void *)schema_quote_point_table_quote);
    initStringInfo(&buf);
    if (schema) appendStringInfo(&buf, "%s.", schema_quote);
    appendStringInfoString(&buf, table_quote);
    schema_quote_point_table_quote = buf.data;
    if (!conf) {
        initStringInfo(&buf);
        appendStringInfo(&buf, "%s %d", MyBgworkerEntry->bgw_type, period);
        SetConfigOption("application_name", buf.data, PGC_USERSET, PGC_S_OVERRIDE);
        pqsignal(SIGHUP, tick_sighup);
        pqsignal(SIGTERM, tick_sigterm);
        BackgroundWorkerUnblockSignals();
        BackgroundWorkerInitializeConnection(data, user, 0);
        pgstat_report_appname(buf.data);
        pfree(buf.data);
    }
    L("data = %s, user = %s, schema = %s, table = %s, period = %d", data, user, schema ? schema : "(null)", table, period);
    initStringInfo(&buf);
    appendStringInfo(&buf, "%d", period);
    set_config_option("pg_task.period", buf.data, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    pfree(buf.data);
    if (schema) tick_schema();
    tick_type();
    tick_table();
    tick_index("dt");
    tick_index("state");
    if (!pg_try_advisory_lock_int8_my(oid)) { sigterm = true; W("lock oid = %d", oid); return; }
    tick_fix();
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

void tick_worker(Datum main_arg); void tick_worker(Datum main_arg) {
    const char *data = MyBgworkerEntry->bgw_extra;
    const char *user = data + strlen(data) + 1;
    const char *schema = user + strlen(user) + 1;
    const char *table = schema + strlen(schema) + 1;
    const int period = *(typeof(period) *)(table + strlen(table) + 1);
    if (table == schema + 1) schema = NULL;
    tick_init(false, data, user, schema, table, period);
    while (!sigterm) {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) break;
        if (!BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid)) break;
        if (rc & WL_LATCH_SET) tick_reset();
        if (sighup) tick_reload();
        if (rc & WL_TIMEOUT) tick_loop();
    }
}
