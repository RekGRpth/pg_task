#include "include.h"

static volatile sig_atomic_t sighup = false;
static volatile sig_atomic_t sigterm = false;

static const char *data;
static const char *data_quote = NULL;
static const char *point;
static const char *schema;
static const char *schema_quote = NULL;
static const char *table;
static const char *table_quote = NULL;
static const char *user;
static const char *user_quote = NULL;

static long period;

static void tick_schema(void) {
    int rc;
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", schema_quote);
    SPI_start_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(buf.data);
    pfree(buf.data);
}

static void tick_type(void) {
    int rc;
    static const char *command =
        "DO $$ BEGIN\n"
        "    IF NOT EXISTS (SELECT 1 FROM pg_type AS t INNER JOIN pg_namespace AS n ON n.oid = typnamespace WHERE nspname = COALESCE(NULLIF(current_setting('pg_task.schema', true), ''), current_schema) AND typname = 'state') THEN\n"
        "        CREATE TYPE STATE AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');\n"
        "    END IF;\n"
        "END; $$";
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table);
    SPI_start_my(command, StatementTimeout);
    if ((rc = SPI_execute(command, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(command);
}

static void tick_table(void) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", table);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE IF NOT EXISTS %s%s%s (\n"
        "    id BIGSERIAL NOT NULL PRIMARY KEY,\n"
        "    parent BIGINT DEFAULT current_setting('pg_task.id', true)::BIGINT,\n"
        "    dt TIMESTAMP NOT NULL DEFAULT current_timestamp,\n"
        "    start TIMESTAMP,\n"
        "    stop TIMESTAMP,\n"
        "    queue TEXT NOT NULL DEFAULT 'default',\n"
        "    max INT,\n"
        "    pid INT,\n"
        "    request TEXT NOT NULL,\n"
        "    response TEXT,\n"
        "    state STATE NOT NULL DEFAULT 'PLAN'::STATE,\n"
        "    timeout INTERVAL,\n"
        "    delete BOOLEAN NOT NULL DEFAULT false,\n"
        "    repeat INTERVAL,\n"
        "    drift BOOLEAN NOT NULL DEFAULT true,\n"
        "    count INT,\n"
        "    live INTERVAL,\n"
        "    CONSTRAINT %s FOREIGN KEY (parent) REFERENCES %s%s%s (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL\n"
        ")", schema_quote, point, table_quote, name_q, schema_quote, point, table_quote);
    SPI_start_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(buf.data);
    if (name_q != name.data) pfree((void *)name_q);
    pfree(name.data);
    pfree(buf.data);
}

static void tick_index(const char *index) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    const char *index_q = quote_identifier(index);
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, index = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", table, index);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON %s%s%s USING btree (%s)", name_q, schema_quote, point, table_quote, index_q);
    SPI_start_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(buf.data);
    pfree(buf.data);
    pfree(name.data);
    if (name_q != name.data) pfree((void *)name_q);
    if (index_q != index) pfree((void *)index_q);
}

static void tick_lock(void) {
    int rc;
    static const char *command =
        "SELECT      pg_try_advisory_lock(c.oid::BIGINT) AS lock, set_config('pg_task.lock', c.oid::TEXT, false)\n"
        "FROM        pg_class AS c\n"
        "INNER JOIN  pg_namespace AS n ON n.oid = relnamespace\n"
        "INNER JOIN  pg_tables AS t ON tablename = relname AND nspname = schemaname\n"
        "WHERE       schemaname = COALESCE(NULLIF(current_setting('pg_task.schema', true), ''), current_schema)\n"
        "AND         tablename = current_setting('pg_task.table', false)";
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table);
    SPI_start_my(command, StatementTimeout);
    if ((rc = SPI_execute(command, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool lock_isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "lock"), &lock_isnull));
        if (lock_isnull) ereport(ERROR, (errmsg("%s(%s:%d): lock_isnull", __func__, __FILE__, __LINE__)));
        if (!lock) {
            ereport(WARNING, (errmsg("%s(%s:%d): Already running data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table)));
            sigterm = true;
        }
    }
    SPI_commit_my(command);
}

static void tick_fix(void) {
    int rc;
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "WITH s AS (SELECT id FROM %s%s%s AS t WHERE state IN ('TAKE'::STATE, 'WORK'::STATE) AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), queue, id)\n"
        ") FOR UPDATE SKIP LOCKED) UPDATE %s%s%s AS u SET state = 'PLAN'::STATE FROM s WHERE u.id = s.id", schema_quote, point, table_quote, schema_quote, point, table_quote);
    SPI_start_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UPDATE) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit_my(buf.data);
    pfree(buf.data);
}

static void task_worker(const Datum id, const char *queue, const uint32 max) {
    StringInfoData buf;
    uint32 data_len = strlen(data), user_len = strlen(user), schema_len = schema ? strlen(schema) : 0, table_len = strlen(table), queue_len = strlen(queue), max_len = sizeof(max);
    BackgroundWorker worker;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %u", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max);
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = id;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "task_worker");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task %s%s%s %s", schema ? schema : "", schema ? " " : "", table, queue);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %s", user, data, schema ? schema : "", schema ? " " : "", table, queue);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + max_len)));
    memcpy(worker.bgw_extra, data, data_len);
    memcpy(worker.bgw_extra + data_len + 1, user, user_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1, schema, schema_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1, table, table_len);
    memcpy(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1, queue, queue_len);
    *(typeof(max + 0) *)(worker.bgw_extra + data_len + 1 + user_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1) = max;
    RegisterDynamicBackgroundWorker_my(&worker);
}

void tick_loop(void) {
    int rc;
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (WITH s AS (WITH s AS (WITH s AS (WITH s AS (\n"
            "SELECT      id, queue, COALESCE(max, ~(1<<31)) AS max, a.pid\n"
            "FROM        %s%s%s AS t\n"
            "LEFT JOIN   pg_stat_activity AS a\n"
            "ON          datname = current_catalog\n"
            "AND         usename = current_user\n"
            "AND         backend_type = concat_ws(' ', 'pg_task', NULLIF(current_setting('pg_task.schema', true), ''), current_setting('pg_task.table', false), queue)\n"
            "WHERE       t.state = 'PLAN'::STATE\n"
            "AND         dt <= current_timestamp\n"
            ") SELECT id, queue, max - count(pid) AS count FROM s GROUP BY id, queue, max\n"
            ") SELECT array_agg(id ORDER BY id) AS id, queue, count FROM s WHERE count > 0 GROUP BY queue, count\n"
            ") SELECT unnest(id[:count]) AS id, queue, count FROM s ORDER BY count DESC\n"
            ") SELECT s.* FROM s INNER JOIN %s%s%s USING (id) FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE'::STATE FROM s WHERE u.id = s.id RETURNING u.id, u.queue, COALESCE(u.max, ~(1<<31)) AS max", schema_quote, point, table_quote, schema_quote, point, table_quote, schema_quote, point, table_quote);
        command = buf.data;
    }
    SPI_start_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, 0, NULL))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, NULL, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, max_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        const char *queue = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"));
        const uint32 max = DatumGetUInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
        if (max_isnull) ereport(ERROR, (errmsg("%s(%s:%d): max_isnull", __func__, __FILE__, __LINE__)));
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
    int rc;
    static SPIPlanPtr plan = NULL;
    static const char *command =
        "WITH s AS ("
        "SELECT      COALESCE(datname, data)::TEXT AS data,\n"
        "            COALESCE(COALESCE(usename, \"user\"), data)::TEXT AS user,\n"
        "            schema,\n"
        "            COALESCE(\"table\", current_setting('pg_task.task', false)) AS table,\n"
        "            COALESCE(period, current_setting('pg_task.tick', false)::BIGINT) AS period\n"
        "FROM        json_populate_recordset(NULL::RECORD, current_setting('pg_task.config', false)::JSON) AS s (data TEXT, \"user\" TEXT, schema TEXT, \"table\" TEXT, period BIGINT)\n"
        "LEFT JOIN   pg_database AS d ON (data IS NULL OR datname = data) AND NOT datistemplate AND datallowconn\n"
        "LEFT JOIN   pg_user AS u ON usename = COALESCE(COALESCE(\"user\", (SELECT usename FROM pg_user WHERE usesysid = datdba)), data)\n"
        ") SELECT * FROM s WHERE data = current_catalog AND \"user\" = current_user AND schema IS NOT DISTINCT FROM NULLIF(current_setting('pg_task.schema', true), '') AND \"table\" = current_setting('pg_task.table', false) AND period = current_setting('pg_task.period', false)::BIGINT";
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %ld", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period);
    SPI_start_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, 0, NULL))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, NULL, NULL, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    if (!SPI_processed) sigterm = true;
    SPI_commit_my(command);
}

void tick_init(const bool conf, const char *_data, const char *_user, const char *_schema, const char *_table, long _period) {
    StringInfoData buf;
    data = _data; user = _user; schema = _schema; table = _table; period = _period;
    elog(LOG, "%s(%s:%d): data = %s, user = %s, schema = %s, table = %s, period = %ld", __func__, __FILE__, __LINE__, data, user, schema ? schema : "(null)", table, period);
    if (!conf) {
        if (!MyProcPort && !(MyProcPort = (Port *) calloc(1, sizeof(Port)))) ereport(ERROR, (errmsg("%s(%s:%d): !calloc", __func__, __FILE__, __LINE__)));
        if (!MyProcPort->remote_host) MyProcPort->remote_host = "[local]";
        if (!MyProcPort->database_name) MyProcPort->database_name = (char *)data;
        if (!MyProcPort->user_name) MyProcPort->user_name = (char *)user;
    }
    if (data_quote && data_quote != data) { pfree((void *)data_quote); data_quote = NULL; }
    data_quote = quote_identifier(data);
    if (user_quote && user_quote != user) { pfree((void *)user_quote); user_quote = NULL; }
    user_quote = quote_identifier(user);
    if (schema && schema_quote && schema_quote != schema) { pfree((void *)schema_quote); schema_quote = NULL; }
    schema_quote = schema ? quote_identifier(schema) : "";
    point = schema ? "." : "";
    if (table_quote && table_quote != table) { pfree((void *)table_quote); table_quote = NULL; }
    table_quote = quote_identifier(table);
    initStringInfo(&buf);
    if (!conf) {
        appendStringInfo(&buf, "%s %ld", MyBgworkerEntry->bgw_type, period);
        SetConfigOption("application_name", buf.data, PGC_USERSET, PGC_S_OVERRIDE);
        pqsignal(SIGHUP, tick_sighup);
        pqsignal(SIGTERM, tick_sigterm);
        BackgroundWorkerUnblockSignals();
        BackgroundWorkerInitializeConnection(data, user, 0);
        pgstat_report_appname(buf.data);
    }
    if (schema) set_config_option("pg_task.schema", schema, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    set_config_option("pg_task.table", table, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", period);
    set_config_option("pg_task.period", buf.data, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, false ? GUC_ACTION_LOCAL : GUC_ACTION_SET, true, 0, false);
    pfree(buf.data);
    if (schema) tick_schema();
    tick_type();
    tick_table();
    tick_index("dt");
    tick_index("state");
    tick_lock();
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
    const long period = *(typeof(period) *)(table + strlen(table) + 1);
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
