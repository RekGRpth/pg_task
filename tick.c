#include "include.h"

extern volatile sig_atomic_t got_sigterm;
static int period;
static char *database = NULL;
static char *username = NULL;
static char *schema = NULL;
static char *table = NULL;
static const char *database_q;
static const char *username_q;
static const char *schema_q;
static const char *point;
static const char *table_q;

static void init_schema(void) {
    int rc;
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", schema_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
}

static void init_type(void) {
    int rc;
    const char *schema_q = schema ? quote_literal_cstr(schema) : "current_schema";
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "DO $$ BEGIN\n"
        "    IF NOT EXISTS (SELECT 1 FROM pg_type AS t INNER JOIN pg_namespace AS n ON n.oid = typnamespace WHERE nspname = %s AND typname = 'state') THEN\n"
        "        CREATE TYPE STATE AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');\n"
        "    END IF;\n"
        "END; $$", schema_q);
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
    if (schema && schema_q != schema) pfree((void *)schema_q);
}

static void init_table(void) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", table);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE IF NOT EXISTS %s%s%s (\n"
        "    id BIGSERIAL NOT NULL PRIMARY KEY,\n"
        "    parent BIGINT DEFAULT NULLIF((current_setting('pg_scheduler.task_id'::TEXT, true))::BIGINT, 0),\n"
        "    dt TIMESTAMP NOT NULL DEFAULT current_timestamp,\n"
        "    start TIMESTAMP,\n"
        "    stop TIMESTAMP,\n"
        "    queue TEXT NOT NULL DEFAULT 'default',\n"
        "    max INT,\n"
        "    pid INT,\n"
        "    request TEXT NOT NULL,\n"
        "    response TEXT,\n"
        "    state STATE NOT NULL DEFAULT 'PLAN',\n"
        "    timeout INTERVAL,\n"
        "    delete BOOLEAN NOT NULL DEFAULT false,\n"
        "    repeat INTERVAL,\n"
        "    drift BOOLEAN NOT NULL DEFAULT true,\n"
        "    count INT,\n"
        "    live INTERVAL,\n"
        "    CONSTRAINT %s FOREIGN KEY (parent) REFERENCES %s%s%s (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL\n"
        ")", schema_q, point, table_q, name_q, schema_q, point, table_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    if (name_q != name.data) pfree((void *)name_q);
    pfree(name.data);
    pfree(buf.data);
}

static void init_index(const char *index) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    const char *index_q = quote_identifier(index);
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, index = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", table, index);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON %s%s%s USING btree (%s)", name_q, schema_q, point, table_q, index_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
    pfree(name.data);
    if (name_q != name.data) pfree((void *)name_q);
    if (index_q != index) pfree((void *)index_q);
}

static void init_lock(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TEXTOID};
    Datum values[] = {schema ? CStringGetTextDatum(schema) : (Datum)NULL, CStringGetTextDatum(table)};
    char nulls[] = {schema ? ' ' : 'n', ' '};
    static const char *command =
        "SELECT      pg_try_advisory_lock(c.oid::BIGINT) AS lock\n"
        "FROM        pg_class AS c\n"
        "INNER JOIN  pg_namespace AS n ON n.oid = relnamespace\n"
        "INNER JOIN  pg_tables AS t ON tablename = relname AND nspname = schemaname\n"
        "WHERE       schemaname = COALESCE($1, current_schema)\n"
        "AND         tablename = $2";
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    SPI_connect_my(command, StatementTimeout);
    if ((rc = SPI_execute_with_args(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool lock_isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "lock"), &lock_isnull));
        if (lock_isnull) ereport(ERROR, (errmsg("%s(%s:%d): lock_isnull", __func__, __FILE__, __LINE__)));
        if (!lock) {
            ereport(WARNING, (errmsg("%s(%s:%d): Already running database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table)));
            SPI_finish_my(command);
            proc_exit(0);
            return;
        }
    }
    SPI_finish_my(command);
}

static void init_fix(void) {
    int rc;
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "with s as (select id from %s%s%s as t WHERE state IN ('TAKE', 'WORK') AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task task', queue, id)\n"
        ") for update skip locked) update %s%s%s as u set state = 'PLAN' from s where u.id = s.id", schema_q, point, table_q, schema_q, point, table_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UPDATE) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
}

static void register_task_worker(const Datum id, const char *queue, const uint64 max) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    pid_t pid;
    size_t len, database_len, username_len, table_len, schema_len = 0, queue_len;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %lu", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = id;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("task_worker"), "task_worker") != sizeof("task_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("task_worker") - 1)));
    len = (sizeof("pg_task task %s") - 1) + (strlen(queue) - 1) - 1;
    if (len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, len + 1)));
    if (snprintf(worker.bgw_type, len + 1, "pg_task task %s", queue) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
    len = (sizeof("%s %s pg_task task %s") - 1) + (strlen(username) - 1) + (strlen(database) - 1) + (strlen(queue) - 1) - 1 - 1 - 1;
    if (len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, len + 1)));
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_task task %s", username, database, queue) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
    database_len = (sizeof("%s") - 1) + (strlen(database) - 1) - 1;
    username_len = (sizeof("%s") - 1) + (strlen(username) - 1) - 1;
    if (schema) schema_len = (sizeof("%s") - 1) + (strlen(schema) - 1) - 1;
    table_len = (sizeof("%s") - 1) + (strlen(table) - 1) - 1;
    queue_len = (sizeof("%s") - 1) + (strlen(queue) - 1) - 1;
    if (database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + sizeof(uint64)> BGW_EXTRALEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1 + sizeof(uint64))));
    if (snprintf(worker.bgw_extra, database_len + 1, "%s", database) != database_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, database_len)));
    if (snprintf(worker.bgw_extra + database_len + 1, username_len + 1, "%s", username) != username_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, username_len)));
    if (schema && snprintf(worker.bgw_extra + database_len + 1 + username_len + 1, schema_len + 1, "%s", schema) != schema_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, schema_len)));
    if (snprintf(worker.bgw_extra + database_len + 1 + username_len + 1 + schema_len + 1, table_len + 1, "%s", table) != table_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, table_len)));
    if (snprintf(worker.bgw_extra + database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1, queue_len + 1, "%s", queue) != queue_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, queue_len)));
    *(uint64 *)(worker.bgw_extra + database_len + 1 + username_len + 1 + schema_len + 1 + table_len + 1 + queue_len + 1) = max;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not register background process", __func__, __FILE__, __LINE__), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not start background process", __func__, __FILE__, __LINE__), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): cannot start background processes without postmaster", __func__, __FILE__, __LINE__), errhint("Kill all remaining database processes and restart the database.")));
        default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
    }
    pfree(handle);
}

static void tick(void) {
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
            "AND         backend_type = concat('pg_task task ', queue)\n"
            "WHERE       t.state = 'PLAN'\n"
            "AND         dt <= current_timestamp\n"
            ") SELECT id, queue, max - count(pid) AS count FROM s GROUP BY id, queue, max\n"
            ") SELECT array_agg(id ORDER BY id) AS id, queue, count FROM s WHERE count > 0 GROUP BY queue, count\n"
            ") SELECT unnest(id[:count]) AS id, queue, count FROM s ORDER BY count DESC\n"
            ") SELECT s.* FROM s INNER JOIN %s%s%s USING (id) FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id, u.queue, COALESCE(u.max, ~(1<<31)) AS max", schema_q, point, table_q, schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, 0, NULL))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, NULL, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, queue_isnull, max_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        char *queue = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"), &queue_isnull));
        uint64 max = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
        if (queue_isnull) ereport(ERROR, (errmsg("%s(%s:%d): queue_isnull", __func__, __FILE__, __LINE__)));
        if (max_isnull) ereport(ERROR, (errmsg("%s(%s:%d): max_isnull", __func__, __FILE__, __LINE__)));
        register_task_worker(id, queue, max);
        pfree(queue);
    }
    SPI_finish_my(command);
}

static void init(void) {
    if (schema) init_schema();
    init_type();
    init_table();
    init_index("dt");
    init_index("state");
    init_lock();
    init_fix();
}

void tick_worker(Datum main_arg); void tick_worker(Datum main_arg) {
    StringInfoData buf;
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    initStringInfo(&buf);
    appendStringInfo(&buf, "pg_task_period.%s", database);
    DefineCustomIntVariable(buf.data, "how often to run tick", NULL, &period, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task_schema.%s", database);
    DefineCustomStringVariable(buf.data, "pg_task schema", NULL, &schema, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "pg_task_table.%s", database);
    DefineCustomStringVariable(buf.data, "pg_task table", NULL, &table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    pfree(buf.data);
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, period = %i", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, period);
    database_q = quote_identifier(database);
    username_q = quote_identifier(username);
    schema_q = schema ? quote_identifier(schema) : "";
    point = schema ? "." : "";
    table_q = quote_identifier(table);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(database, username, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    init();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period, PG_WAIT_EXTENSION);
//        if (rc & WL_LATCH_SET) elog(LOG, "tick WL_LATCH_SET");
//        if (rc & WL_TIMEOUT) elog(LOG, "tick WL_TIMEOUT");
//        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "tick WL_POSTMASTER_DEATH");
//        if (got_sigterm) elog(LOG, "tick got_sigterm");
//        if (got_sighup) elog(LOG, "tick got_sighup");
//        if (ProcDiePending) elog(LOG, "loop ProcDiePending");
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
//        if (got_sighup) {
//            got_sighup = false;
//            ProcessConfigFile(PGC_SIGHUP);
//            init();
//        }
        if (got_sigterm) proc_exit(0);
        if (rc & WL_TIMEOUT) tick();
    } while (!got_sigterm);
    proc_exit(0);
}
