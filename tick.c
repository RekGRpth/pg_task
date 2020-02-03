#include "include.h"

static volatile sig_atomic_t got_sigterm = false;

static const char *database;
static const char *database_q;
static const char *point;
static const char *schemaname;
static const char *schemaname_q;
static const char *tablename;
static const char *tablename_q;
static const char *username;
static const char *username_q;
static uint32 period;

static void init_schema(void) {
    int rc;
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", schemaname_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
}

static void init_type(void) {
    int rc;
    const char *schemaname_q = schemaname ? quote_literal_cstr(schemaname) : "current_schema";
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "DO $$ BEGIN\n"
        "    IF NOT EXISTS (SELECT 1 FROM pg_type AS t INNER JOIN pg_namespace AS n ON n.oid = typnamespace WHERE nspname = %s AND typname = 'state') THEN\n"
        "        CREATE TYPE STATE AS ENUM ('PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP');\n"
        "    END IF;\n"
        "END; $$", schemaname_q);
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
    if (schemaname && schemaname_q != schemaname) pfree((void *)schemaname_q);
}

static void init_table(void) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", tablename);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "CREATE TABLE IF NOT EXISTS %s%s%s (\n"
        "    id BIGSERIAL NOT NULL PRIMARY KEY,\n"
        "    parent BIGINT DEFAULT current_setting('pg_scheduler.task_id', true)::BIGINT,\n"
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
        ")", schemaname_q, point, tablename_q, name_q, schemaname_q, point, tablename_q);
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
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s, index = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename, index);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_%s_idx", tablename, index);
    name_q = quote_identifier(name.data);
    initStringInfo(&buf);
    appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON %s%s%s USING btree (%s)", name_q, schemaname_q, point, tablename_q, index_q);
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
    Datum values[] = {schemaname ? CStringGetTextDatum(schemaname) : (Datum)NULL, CStringGetTextDatum(tablename)};
    char nulls[] = {schemaname ? ' ' : 'n', ' '};
    static const char *command =
        "SELECT      pg_try_advisory_lock(c.oid::BIGINT) AS lock\n"
        "FROM        pg_class AS c\n"
        "INNER JOIN  pg_namespace AS n ON n.oid = relnamespace\n"
        "INNER JOIN  pg_tables AS t ON tablename = relname AND nspname = schemaname\n"
        "WHERE       schemaname = COALESCE($1, current_schema)\n"
        "AND         tablename = $2";
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename);
    SPI_connect_my(command, StatementTimeout);
    if ((rc = SPI_execute_with_args(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, values, nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool lock_isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "lock"), &lock_isnull));
        if (lock_isnull) ereport(ERROR, (errmsg("%s(%s:%d): lock_isnull", __func__, __FILE__, __LINE__)));
        if (!lock) {
            ereport(WARNING, (errmsg("%s(%s:%d): Already running database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename)));
            SPI_finish_my(command);
            proc_exit(0);
            return;
        }
    }
    SPI_finish_my(command);
}

static void init_fix(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TEXTOID};
    Datum values[] = {schemaname ? CStringGetTextDatum(schemaname) : (Datum)NULL, CStringGetTextDatum(tablename)};
    char nulls[] = {schemaname ? ' ' : 'n', ' '};
    StringInfoData buf;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename);
    initStringInfo(&buf);
    appendStringInfo(&buf,
        "with s as (select id from %s%s%s as t WHERE state IN ('TAKE', 'WORK') AND pid NOT IN (\n"
        "    SELECT  pid\n"
        "    FROM    pg_stat_activity\n"
        "    WHERE   datname = current_catalog\n"
        "    AND     usename = current_user\n"
        "    AND     application_name = concat_ws(' ', 'pg_task', $1||'.', $2, queue, id)\n"
        ") for update skip locked) update %s%s%s as u set state = 'PLAN' from s where u.id = s.id", schemaname_q, point, tablename_q, schemaname_q, point, tablename_q);
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, values, nulls, false, 0)) != SPI_OK_UPDATE) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(buf.data);
    pfree(buf.data);
}

static void register_task_worker(const Datum id, const char *queue, const uint32 max) {
    StringInfoData buf;
    uint32 database_len = strlen(database), username_len = strlen(username), schemaname_len = schemaname ? strlen(schemaname) : 0, tablename_len = strlen(tablename), queue_len = strlen(queue), max_len = sizeof(uint32);
    pid_t pid;
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s, id = %lu, queue = %s, max = %u", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename, DatumGetUInt64(id), queue, max);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
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
    appendStringInfo(&buf, "pg_task %s%s%s %s", schemaname ? schemaname : "", schemaname ? "." : "", tablename, queue);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %s pg_task %s%s%s %s", username, database, schemaname ? schemaname : "", schemaname ? "." : "", tablename, queue);
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    if (database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + queue_len + 1 + max_len > BGW_EXTRALEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + queue_len + 1 + max_len)));
    memcpy(worker.bgw_extra, database, database_len);
    memcpy(worker.bgw_extra + database_len + 1, username, username_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1, schemaname, schemaname_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1 + schemaname_len + 1, tablename, tablename_len);
    memcpy(worker.bgw_extra + database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1, queue, queue_len);
    *(uint32 *)(worker.bgw_extra + database_len + 1 + username_len + 1 + schemaname_len + 1 + tablename_len + 1 + queue_len + 1) = max;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errmsg("%s(%s:%d): !RegisterDynamicBackgroundWorker", __func__, __FILE__, __LINE__)));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_STOPPED", __func__, __FILE__, __LINE__)));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED", __func__, __FILE__, __LINE__)));
        default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
    }
    pfree(handle);
}

static void tick(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TEXTOID};
    Datum values[] = {schemaname ? CStringGetTextDatum(schemaname) : (Datum)NULL, CStringGetTextDatum(tablename)};
    char nulls[] = {schemaname ? ' ' : 'n', ' '};
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
            "AND         backend_type = concat_ws(' ', 'pg_task', $1||'.', $2, queue)\n"
            "WHERE       t.state = 'PLAN'\n"
            "AND         dt <= current_timestamp\n"
            ") SELECT id, queue, max - count(pid) AS count FROM s GROUP BY id, queue, max\n"
            ") SELECT array_agg(id ORDER BY id) AS id, queue, count FROM s WHERE count > 0 GROUP BY queue, count\n"
            ") SELECT unnest(id[:count]) AS id, queue, count FROM s ORDER BY count DESC\n"
            ") SELECT s.* FROM s INNER JOIN %s%s%s USING (id) FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id, u.queue, COALESCE(u.max, ~(1<<31)) AS max", schemaname_q, point, tablename_q, schemaname_q, point, tablename_q, schemaname_q, point, tablename_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, values, nulls, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool id_isnull, queue_isnull, max_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        char *queue = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"), &queue_isnull));
        uint32 max = DatumGetUInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "max"), &max_isnull));
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
        if (queue_isnull) ereport(ERROR, (errmsg("%s(%s:%d): queue_isnull", __func__, __FILE__, __LINE__)));
        if (max_isnull) ereport(ERROR, (errmsg("%s(%s:%d): max_isnull", __func__, __FILE__, __LINE__)));
        register_task_worker(id, queue, max);
        pfree(queue);
    }
    SPI_finish_my(command);
}

static void init(void) {
    if (schemaname) init_schema();
    init_type();
    init_table();
    init_index("dt");
    init_index("state");
    init_lock();
    init_fix();
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void tick_worker(Datum main_arg); void tick_worker(Datum main_arg) {
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    schemaname = username + strlen(username) + 1;
    tablename = schemaname + strlen(schemaname) + 1;
    period = *(uint32 *)(tablename + strlen(tablename) + 1);
    if (tablename == schemaname + 1) schemaname = NULL;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schemaname = %s, tablename = %s, period = %i", __func__, __FILE__, __LINE__, database, username, schemaname ? schemaname : "(null)", tablename, period);
    database_q = quote_identifier(database);
    username_q = quote_identifier(username);
    schemaname_q = schemaname ? quote_identifier(schemaname) : "";
    point = schemaname ? "." : "";
    tablename_q = quote_identifier(tablename);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(database, username, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    init();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period, PG_WAIT_EXTENSION);
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        if (got_sigterm) proc_exit(0);
        if (rc & WL_TIMEOUT) tick();
    } while (!got_sigterm);
    proc_exit(0);
}
