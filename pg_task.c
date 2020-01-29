#include <postgres.h>

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>
#include <utils/varlena.h>

typedef void (*Callback) (const char *src, va_list args);

PG_MODULE_MAGIC;

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *databases;

int period;
int task_id;
char *database = NULL;
char *username = NULL;
char *schema = NULL;
char *table = NULL;
char *queue = NULL;
uint64 max;
uint64 count;
const char *schema_q;
const char *point;
const char *table_q;
TimestampTz start;

/*void _PG_fini(void); void _PG_fini(void) {
    if (schema && schema_q != schema) pfree((void *)schema_q);
    if (table && table_q != table) pfree((void *)table_q);
}*/

static void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void register_main_worker(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("main_worker"), "main_worker") != sizeof("main_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("main_worker") - 1)));
    if (snprintf(worker.bgw_type, sizeof("pg_task main"), "pg_task main") != sizeof("pg_task main") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task main") - 1)));
    if (snprintf(worker.bgw_name, sizeof("postgres postgres pg_task main"), "postgres postgres pg_task main") != sizeof("postgres postgres pg_task main") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("postgres postgres pg_task main") - 1)));
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("%s(%s:%d): pg_task can only be loaded via shared_preload_libraries", __func__, __FILE__, __LINE__), errhint("Add pg_task to the shared_preload_libraries configuration variable in postgresql.conf.")));
    DefineCustomStringVariable("pg_task.database", "pg_task database", NULL, &databases, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.task_id", "pg_task task_id", NULL, &task_id, 0, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    register_main_worker();
}

static void register_tick_worker(const char *database, const char *username) {
    size_t len, database_len, username_len;
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("tick_worker"), "tick_worker") != sizeof("tick_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("tick_worker") - 1)));
    if (snprintf(worker.bgw_type, sizeof("pg_task tick"), "pg_task tick") != sizeof("pg_task tick") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task tick") - 1)));
    len = (sizeof("%s %s pg_task tick") - 1) + (strlen(username) - 1) - 1 + (strlen(database) - 1) - 1;
    if (len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, len + 1)));
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_task tick", username, database) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
    database_len = (sizeof("%s") - 1) + (strlen(database) - 1) - 1;
    username_len = (sizeof("%s") - 1) + (strlen(username) - 1) - 1;
    if (database_len + 1 + username_len + 1 > BGW_EXTRALEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_EXTRALEN", __func__, __FILE__, __LINE__, database_len + 1 + username_len + 1)));
    if (snprintf(worker.bgw_extra                   , database_len + 1, "%s", database) != database_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, database_len)));
    if (snprintf(worker.bgw_extra + database_len + 1, username_len + 1, "%s", username) != username_len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, username_len)));
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not register background process", __func__, __FILE__, __LINE__), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): could not start background process", __func__, __FILE__, __LINE__), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): cannot start background processes without postmaster", __func__, __FILE__, __LINE__), errhint("Kill all remaining database processes and restart the database.")));
        default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
    }
    pfree(handle);
}

static void SPI_connect_my(const char *command, const int timeout) {
    int rc;
//    elog(LOG, "%s(%s:%d): command = %s, MyBgworkerEntry->bgw_type = %s, MyBgworkerEntry->bgw_name = %s", __func__, __FILE__, __LINE__, command, MyBgworkerEntry->bgw_type, MyBgworkerEntry->bgw_name);
    pgstat_report_activity(STATE_RUNNING, command);
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_connect_ext = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
//    pgstat_report_appname(MyBgworkerEntry->bgw_name);
//    set_ps_display(MyBgworkerEntry->bgw_name, true);
    SPI_start_transaction();
    if (timeout > 0) enable_timeout_after(STATEMENT_TIMEOUT, timeout); else disable_timeout(STATEMENT_TIMEOUT, false);
}

static void SPI_finish_my(const char *command) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errmsg("%s(%s:%d): SPI_finish = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    ProcessCompletedNotifies();
    pgstat_report_activity(STATE_IDLE, command);
    pgstat_report_stat(true);
}

static void check(void) {
    int rc, i = 0;
    List *elemlist;
    StringInfoData buf;
    Oid *argtypes = NULL;
    Datum *Values = NULL;
    char *Nulls = NULL;
    char **str = NULL;
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    initStringInfo(&buf);
    appendStringInfoString(&buf,
        "WITH s AS (\n"
        "    SELECT      d.oid, d.datname, u.usesysid, u.usename\n"
        "    FROM        pg_database AS d\n"
        "    JOIN        pg_user AS u ON TRUE\n"
        "    INNER JOIN  pg_user AS i ON d.datdba = i.usesysid\n"
        "    WHERE       NOT datistemplate\n"
        "    AND         datallowconn\n");
    if (!databases) appendStringInfoString(&buf, "    AND         i.usesysid = u.usesysid\n"); else {
        char *rawstring = pstrdup(databases);
        if (!SplitGUCList(rawstring, ',', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s(%s:%d): invalid list syntax in parameter `pg_task.database` in postgresql.conf", __func__, __FILE__, __LINE__)));
        argtypes = palloc(sizeof(Oid) * list_length(elemlist) * 2);
        Values = palloc(sizeof(Datum) * list_length(elemlist) * 2);
        Nulls = palloc(sizeof(char) * list_length(elemlist) * 2);
        str = palloc(sizeof(char *) * list_length(elemlist) * 2);
        appendStringInfoString(&buf, "    AND         (d.datname, u.usename) IN (\n        ");
        for (ListCell *cell = list_head(elemlist); cell; cell = lnext(cell)) {
            const char *database_username = (const char *)lfirst(cell);
            char *rawstring = pstrdup(database_username);
            List *elemlist;
            if (!SplitIdentifierString(rawstring, ':', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s(%s:%d): invalid list syntax in parameter `pg_task.database` in postgresql.conf", __func__, __FILE__, __LINE__))); else {
                ListCell *cell = list_head(elemlist);
                const char *database = (const char *)lfirst(cell);
                const char *username = NULL;
                Nulls[2 * i] = ' ';
                Nulls[2 * i + 1] = ' ';
                if ((cell = lnext(cell))) username = (const char *)lfirst(cell);
                else Nulls[2 * i + 1] = 'n';
                elog(LOG, "%s(%s:%d): database = %s, username = %s", __func__, __FILE__, __LINE__, database, username ? username : "(null)");
                if (i > 0) appendStringInfoString(&buf, ", ");
                appendStringInfo(&buf, "($%i, COALESCE($%i, i.usename))", 2 * i + 1, 2 * i + 1 + 1);
                argtypes[2 * i] = TEXTOID;
                argtypes[2 * i + 1] = TEXTOID;
                str[2 * i] = pstrdup(database);
                str[2 * i + 1] = username ? pstrdup(username) : NULL;
                Values[2 * i] = CStringGetTextDatum(str[2 * i]);
                Values[2 * i + 1] = username ? CStringGetTextDatum(str[2 * i + 1]) : (Datum)NULL;
            }
            pfree(rawstring);
            list_free(elemlist);
            i++;
        }
        appendStringInfoString(&buf, "\n    )\n");
        pfree(rawstring);
        list_free(elemlist);
    }
    appendStringInfoString(&buf,
        "), l AS (\n"
        "    SELECT * FROM pg_locks WHERE locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted\n"
        ")\n"
        "SELECT      datname, usename, TRUE AS start\n"
        "FROM        s\n"
        "WHERE       NOT EXISTS (SELECT pid FROM l WHERE classid = oid AND objid = usesysid AND database = oid)\n"
        "UNION\n"
        "SELECT      datname, usename, NOT pg_terminate_backend(pid) AS start\n"
        "FROM        pg_stat_activity\n"
        "INNER JOIN  l USING (pid)\n"
        "WHERE       (datname, usename) NOT IN (SELECT datname, usename FROM s)\n"
        "AND         classid = datid AND objid = usesysid AND database = datid");
    SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, i * 2, argtypes, Values, Nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_with_args = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool database_isnull, usename_isnull, start_isnull;
        char *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &database_isnull));
        char *username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &usename_isnull));
        bool start = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "start"), &start_isnull));
        if (database_isnull) ereport(ERROR, (errmsg("%s(%s:%d): database_isnull", __func__, __FILE__, __LINE__)));
        if (usename_isnull) ereport(ERROR, (errmsg("%s(%s:%d): usename_isnull", __func__, __FILE__, __LINE__)));
        if (start_isnull) ereport(ERROR, (errmsg("%s(%s:%d): start_isnull", __func__, __FILE__, __LINE__)));
        if (start) register_tick_worker(database, username);
//        pfree(database);
//        pfree(username);
    }
    SPI_finish_my(buf.data);
    pfree(buf.data);
    if (argtypes) pfree(argtypes);
    if (Values) pfree(Values);
    if (Nulls) pfree(Nulls);
    if (str) { for (int j = 0; j < i * 2; j++) if (str[j]) pfree(str[j]); pfree(str); }
}

void main_worker(Datum _); void main_worker(Datum _) {
    elog(LOG, "%s(%s:%d): databases = %s", __func__, __FILE__, __LINE__, databases ? databases : "(null)");
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    check();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | /*WL_TIMEOUT |*/ WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
//        if (rc & WL_LATCH_SET) elog(LOG, "loop WL_LATCH_SET");
//        if (rc & WL_TIMEOUT) elog(LOG, "loop WL_TIMEOUT");
//        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "loop WL_POSTMASTER_DEATH");
//        if (got_sigterm) elog(LOG, "loop got_sigterm");
//        if (got_sighup) elog(LOG, "loop got_sighup");
//        if (ProcDiePending) elog(LOG, "loop ProcDiePending");
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            check();
        }
        if (got_sigterm) proc_exit(0);
    } while (!got_sigterm);
    proc_exit(0);
}

static void lock(void) {
    int rc;
    const char *command = "SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_user.usesysid::INT) FROM pg_database, pg_user WHERE datname = current_catalog AND usename = current_user";
    SPI_connect_my(command, StatementTimeout);
    if ((rc = SPI_execute(command, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool lock_isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "pg_try_advisory_lock"), &lock_isnull));
        if (lock_isnull) ereport(ERROR, (errmsg("%s(%s:%d): lock_isnull", __func__, __FILE__, __LINE__)));
        if (!lock) ereport(ERROR, (errmsg("%s(%s:%d): Already running database = %s, username = %s", __func__, __FILE__, __LINE__, database, username)));
        MyBgworkerEntry->bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    }
    SPI_finish_my(command);
}

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

static void init_table(void) {
    int rc;
    StringInfoData buf, name;
    const char *name_q;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table);
    initStringInfo(&name);
    appendStringInfo(&name, "%s_parent_fkey", table);
    name_q = quote_identifier(name.data);
//    elog(LOG, "%s(%s:%d): name_q = %s, name_q != name.data = %s", __func__, __FILE__, __LINE__, name_q, name_q != name.data ? "true" : "false");
    initStringInfo(&buf);
//    elog(LOG, "%s(%s:%d): %s%s%s %s %s%s%s", __func__, __FILE__, __LINE__, schema_q, point, table_q, name_q, schema_q, point, table_q);
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
        "    state TEXT NOT NULL DEFAULT 'PLAN',\n"
        "    timeout INTERVAL,\n"
        "    delete BOOLEAN NOT NULL DEFAULT false,\n"
        "    repeat INTERVAL,\n"
        "    drift BOOLEAN NOT NULL DEFAULT true,\n"
        "    count INT,\n"
        "    live INTERVAL,\n"
        "    CONSTRAINT %s FOREIGN KEY (parent) REFERENCES %s%s%s (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL\n"
        ")", schema_q, point, table_q, name_q, schema_q, point, table_q);
//    elog(LOG, "%s(%s:%d): buf.data = %s", __func__, __FILE__, __LINE__, buf.data);
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
    init_table();
    init_index("dt");
    init_index("state");
    init_fix();
}

void tick_worker(Datum _); void tick_worker(Datum _) {
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
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(database, username, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    schema_q = schema ? quote_identifier(schema) : "";
    point = schema ? "." : "";
    table_q = quote_identifier(table);
    lock();
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
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            init();
        }
        if (got_sigterm) proc_exit(0);
        if (rc & WL_TIMEOUT) tick();
    } while (!got_sigterm);
    proc_exit(0);
}

static void work(const MemoryContext oldMemoryContext, const Datum id, char **request, uint64 *timeout) {
    int rc;
    static Oid argtypes[] = {INT8OID, INT8OID};
    Datum Values[] = {id, MyProcPid};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%lu", DatumGetUInt64(id));
    if (set_config_option("pg_task.task_id", buf.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false) <= 0) ereport(ERROR, (errmsg("%s(%s:%d): set_config_option <= 0", __func__, __FILE__, __LINE__)));
    pfree(buf.data);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE)\n"
            "UPDATE  %s%s%s AS u\n"
            "SET     state = 'WORK',\n"
            "        start = current_timestamp,\n"
            "        pid = $2\n"
            "FROM s WHERE u.id = s.id RETURNING request, COALESCE(EXTRACT(epoch FROM timeout), 0)::INT * 1000 AS timeout", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool request_isnull, timeout_isnull;
        char *value = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"), &request_isnull));
        *timeout = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "timeout"), &timeout_isnull));
        if (request_isnull) ereport(ERROR, (errmsg("%s(%s:%d): request_isnull", __func__, __FILE__, __LINE__)));
        if (timeout_isnull) ereport(ERROR, (errmsg("%s(%s:%d): timeout_isnull", __func__, __FILE__, __LINE__)));
        *request = MemoryContextStrdup(oldMemoryContext, value);
        pfree(value);
    }
    SPI_finish_my(command);
}

static void repeat_task(const Datum id) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum Values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id AS parent, CASE\n"
            "    WHEN drift THEN current_timestamp + repeat\n"
            "    ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1)\n"
            "END AS dt, queue, max, request, 'PLAN' AS state, timeout, delete, repeat, drift, count, live\n"
            "FROM %s%s%s WHERE id = '1' AND state IN ('DONE', 'FAIL') LIMIT 1\n"
            ") INSERT INTO %s%s%s (parent, dt, queue, max, request, state, timeout, delete, repeat, drift, count, live) SELECT * FROM s", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_INSERT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(command);
}

static void delete_task(const Datum id) {
    int rc;
    static Oid argtypes[] = {INT8OID};
    Datum Values[] = {id};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DELETE FROM %s%s%s WHERE id = $1", schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_DELETE) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    SPI_finish_my(command);
}

/*static void execute(const Datum id);
static void more_task(const char *queue) {
    int rc;
    Oid argtypes[] = {TEXTOID};
    Datum Values[] = {CStringGetTextDatum(queue)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    elog(LOG, "%s(%s:%d): queue = %s", __func__, __FILE__, __LINE__, queue);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfoString(&buf, "WITH s AS (\nSELECT id, COALESCE(max, ~(1<<31)) AS max FROM ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s WHERE id IN (\n", quote_identifier(table));
        appendStringInfoString(&buf,
            "WITH s AS (\n"
            "    SELECT      id, COALESCE(max, ~(1<<31)) AS max\n"
            "    FROM        ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s AS t\n"
            "    WHERE       t.state = 'PLAN'\n"
            "    AND         dt <= current_timestamp\n"
            "    AND         queue = $1\n"
            "    GROUP BY    1, 2\n"
            "    ORDER BY    2 DESC, 1\n"
            ") SELECT unnest((array_agg(id ORDER BY id))[:GREATEST(max(max), 0)]) AS id FROM s\n", quote_identifier(table));
        appendStringInfoString(&buf, ") ORDER BY 2 DESC, 1 LIMIT 1 FOR UPDATE SKIP LOCKED\n) UPDATE ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id", quote_identifier(table));
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) SPI_finish_my(command); else {
        bool isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &isnull);
        if (isnull) ereport(ERROR, (errmsg("%s(%s:%d): isnull", __func__, __FILE__, __LINE__)));
        SPI_finish_my(command);
        execute(id);
    }
}*/

static void done(const Datum id, const char *data, const char *state) {
    int rc;
//    static uint64 count = 0;
//    static TimestampTz start;
    bool delete, repeat;//, more;
//    char *queue = NULL;
    static Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID/*, INT8OID, TIMESTAMPTZOID*/};
    Datum Values[] = {id, CStringGetTextDatum(state), data ? CStringGetTextDatum(data) : (Datum)NULL/*, UInt64GetDatum(count), TimestampTzGetDatum(count ? start : (start = GetCurrentTimestamp()))*/};
    char Nulls[] = {' ', ' ', data ? ' ' : 'n', ' ', ' '};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    elog(LOG, "%s(%s:%d): id = %lu, data = %s, state = %s", __func__, __FILE__, __LINE__, DatumGetUInt64(id), data ? data : "(null)", state);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (SELECT id FROM %s%s%s WHERE id = $1 FOR UPDATE\n)\n"
            "UPDATE %s%s%s AS u SET state = $2, stop = current_timestamp, response = $3 FROM s WHERE u.id = s.id\n"
            "RETURNING delete, queue,\n"
//            "COALESCE(count, 0) > $4 AND $5 + COALESCE(live, '0 sec'::INTERVAL) > current_timestamp AS more,\n"
            "repeat IS NOT NULL AND state IN ('DONE', 'FAIL') AS repeat", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, Nulls, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("%s(%s:%d): SPI_processed != 1", __func__, __FILE__, __LINE__))); else {
        bool delete_isnull, repeat_isnull;//, more_isnull;//, queue_isnull;
        delete = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "delete"), &delete_isnull));
        repeat = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "repeat"), &repeat_isnull));
//        more = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "more"), &more_isnull));
        if (delete_isnull) ereport(ERROR, (errmsg("%s(%s:%d): delete_isnull", __func__, __FILE__, __LINE__)));
        if (repeat_isnull) ereport(ERROR, (errmsg("%s(%s:%d): repeat_isnull", __func__, __FILE__, __LINE__)));
//        if (more_isnull) ereport(ERROR, (errmsg("%s(%s:%d): more_isnull", __func__, __FILE__, __LINE__)));
//        if (more) {
//            queue = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"), &queue_isnull));
//            if (queue_isnull) ereport(ERROR, (errmsg("%s(%s:%d): queue_isnull", __func__, __FILE__, __LINE__)));
//        }
    }
    SPI_finish_my(command);
    if (repeat) repeat_task(id);
    if (delete && !data) delete_task(id);
//    if (queue) { more_task(queue); pfree(queue); }
//    count++;
}

static void success(const MemoryContext oldMemoryContext, char **data, char **state) {
    if ((SPI_tuptable) && (SPI_processed > 0)) {
        StringInfoData buf;
        initStringInfo(&buf);
        if (SPI_tuptable->tupdesc->natts > 1) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *name = SPI_fname(SPI_tuptable->tupdesc, col);
                char *type = SPI_gettype(SPI_tuptable->tupdesc, col);
                appendStringInfo(&buf, "%s::%s", name, type);
                if (col > 1) appendStringInfoString(&buf, "\t");
                pfree(name);
                pfree(type);
            }
            appendStringInfoString(&buf, "\n");
        }
        for (uint64 row = 0; row < SPI_processed; row++) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                appendStringInfo(&buf, "%s", value);
                if (col > 1) appendStringInfoString(&buf, "\t");
                pfree(value);
            }
            if (row < SPI_processed - 1) appendStringInfoString(&buf, "\n");
        }
        *data = MemoryContextStrdup(oldMemoryContext, buf.data);
        pfree(buf.data);
//        elog(LOG, "%s(%s:%d): data = %s", __func__, __FILE__, __LINE__, *data);
    }
    *state = "DONE";
}

static void error(const MemoryContext oldMemoryContext, char **data, char **state) {
    ErrorData *edata = CopyErrorData();
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "elevel::int4\t%i", edata->elevel);
    appendStringInfo(&buf, "\noutput_to_server::bool\t%s", edata->output_to_server ? "true" : "false");
    appendStringInfo(&buf, "\noutput_to_client::bool\t%s", edata->output_to_client ? "true" : "false");
    appendStringInfo(&buf, "\nshow_funcname::bool\t%s", edata->show_funcname ? "true" : "false");
    appendStringInfo(&buf, "\nhide_stmt::bool\t%s", edata->hide_stmt ? "true" : "false");
    appendStringInfo(&buf, "\nhide_ctx::bool\t%s", edata->hide_ctx ? "true" : "false");
    if (edata->filename) appendStringInfo(&buf, "\nfilename::text\t%s", edata->filename);
    if (edata->lineno) appendStringInfo(&buf, "\nlineno::int4\t%i", edata->lineno);
    if (edata->funcname) appendStringInfo(&buf, "\nfuncname::text\t%s", edata->funcname);
    if (edata->domain) appendStringInfo(&buf, "\ndomain::text\t%s", edata->domain);
    if (edata->context_domain) appendStringInfo(&buf, "\ncontext_domain::text\t%s", edata->context_domain);
    if (edata->sqlerrcode) appendStringInfo(&buf, "\nsqlerrcode::int4\t%i", edata->sqlerrcode);
    if (edata->message) appendStringInfo(&buf, "\nmessage::text\t%s", edata->message);
    if (edata->detail) appendStringInfo(&buf, "\ndetail::text\t%s", edata->detail);
    if (edata->detail_log) appendStringInfo(&buf, "\ndetail_log::text\t%s", edata->detail_log);
    if (edata->hint) appendStringInfo(&buf, "\nhint::text\t%s", edata->hint);
    if (edata->context) appendStringInfo(&buf, "\ncontext::text\t%s", edata->context);
    if (edata->message_id) appendStringInfo(&buf, "\nmessage_id::text\t%s", edata->message_id);
    if (edata->schema_name) appendStringInfo(&buf, "\nschema_name::text\t%s", edata->schema_name);
    if (edata->table_name) appendStringInfo(&buf, "\ntable_name::text\t%s", edata->table_name);
    if (edata->column_name) appendStringInfo(&buf, "\ncolumn_name::text\t%s", edata->column_name);
    if (edata->datatype_name) appendStringInfo(&buf, "\ndatatype_name::text\t%s", edata->datatype_name);
    if (edata->constraint_name) appendStringInfo(&buf, "\nconstraint_name::text\t%s", edata->constraint_name);
    if (edata->cursorpos) appendStringInfo(&buf, "\ncursorpos::int4\t%i", edata->cursorpos);
    if (edata->internalpos) appendStringInfo(&buf, "\ninternalpos::int4\t%i", edata->internalpos);
    if (edata->internalquery) appendStringInfo(&buf, "\ninternalquery::text\t%s", edata->internalquery);
    if (edata->saved_errno) appendStringInfo(&buf, "\nsaved_errno::int4\t%i", edata->saved_errno);
    FreeErrorData(edata);
    *data = MemoryContextStrdup(oldMemoryContext, buf.data);
    pfree(buf.data);
//    elog(LOG, "%s(%s:%d): data = %s", __func__, __FILE__, __LINE__, *data);
    *state = "FAIL";
}

/*static void update_bgw_type(const Datum arg) {
    uint64 id = DatumGetUInt64(arg);
    static size_t bgw_type_len = 0;
    size_t len = (sizeof(" %lu") - 1) - 2;
    for (int number = id; number /= 10; len++);
    if (!bgw_type_len) bgw_type_len = strlen(MyBgworkerEntry->bgw_type);
    if (bgw_type_len + len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, bgw_type_len + len + 1)));
    if (snprintf(MyBgworkerEntry->bgw_type + bgw_type_len, len + 1, " %lu", id) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
    MyBgworkerEntry->bgw_type[bgw_type_len + len + 1] = '\0';
    elog(LOG, "%s(%s:%d): MyBgworkerEntry->bgw_type = %s, MyBgworkerEntry->bgw_name = %s", __func__, __FILE__, __LINE__, MyBgworkerEntry->bgw_type, MyBgworkerEntry->bgw_name);
}*/

static void update_ps_display(const Datum id) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_name, DatumGetUInt64(id));
//    static size_t bgw_name_len = 0;
//    size_t len = (sizeof(" %lu") - 1) - 2;
//    for (int number = DatumGetUInt64(id); number /= 10; len++);
//    if (!bgw_name_len) bgw_name_len = strlen(MyBgworkerEntry->bgw_name);
//    if (bgw_name_len + len + 1 > BGW_MAXLEN) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): %lu > BGW_MAXLEN", __func__, __FILE__, __LINE__, bgw_name_len + len + 1)));
//    if (snprintf(MyBgworkerEntry->bgw_name + bgw_name_len, len + 1, " %lu", DatumGetUInt64(id)) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, len)));
//    MyBgworkerEntry->bgw_name[bgw_name_len + len + 1] = '\0';
//    elog(LOG, "%s(%s:%d): id = %lu, MyBgworkerEntry->bgw_type = %s, MyBgworkerEntry->bgw_name = %s", __func__, __FILE__, __LINE__, DatumGetUInt64(id), MyBgworkerEntry->bgw_type, MyBgworkerEntry->bgw_name);
//    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    init_ps_display(buf.data, "", "", "");
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%s %lu", MyBgworkerEntry->bgw_type, DatumGetUInt64(id));
    pgstat_report_appname(buf.data);
    pfree(buf.data);
}

static void execute(const Datum id);
static void more(void) {
    int rc;
    static Oid argtypes[] = {TEXTOID, TIMESTAMPTZOID, INT8OID, INT8OID};
    Datum Values[] = {CStringGetTextDatum(queue), TimestampTzGetDatum(start), UInt64GetDatum(max), UInt64GetDatum(count)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf,
            "WITH s AS (\n"
            "SELECT  id\n"
            "FROM    %s%s%s\n"
            "WHERE   state = 'PLAN'\n"
            "AND     dt <= current_timestamp\n"
            "AND     queue = $1\n"
            "AND     $2 + COALESCE(live, '0 sec'::INTERVAL) >= current_timestamp\n"
            "AND     COALESCE(max, ~(1<<31)) >= $3\n"
            "AND     COALESCE(count, 0) >= $4\n"
            "ORDER BY COALESCE(max, ~(1<<31)) DESC LIMIT 1 FOR UPDATE SKIP LOCKED\n"
            ") UPDATE %s%s%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id", schema_q, point, table_q, schema_q, point, table_q);
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) SPI_finish_my(command); else {
        bool id_isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &id_isnull);
        if (id_isnull) ereport(ERROR, (errmsg("%s(%s:%d): id_isnull", __func__, __FILE__, __LINE__)));
        SPI_finish_my(command);
        execute(id);
    }
}

static void execute(const Datum id) {
    int rc;
    uint64 timeout = 0;
    char *request, *data = NULL, *state;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    count++;
    update_ps_display(id);
    work(oldMemoryContext, id, &request, &timeout);
    if (0 < StatementTimeout && StatementTimeout < timeout) timeout = StatementTimeout;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, id = %lu, timeout = %lu, request = %s, count = %lu", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, DatumGetUInt64(id), timeout, request, count);
    SPI_connect_my(request, timeout);
    PG_TRY(); {
        if ((rc = SPI_execute(request, false, 0)) < 0) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
        success(oldMemoryContext, &data, &state);
        SPI_commit();
    } PG_CATCH(); {
        error(oldMemoryContext, &data, &state);
        SPI_rollback();
    } PG_END_TRY();
    SPI_finish_my(request);
    pfree(request);
    done(id, data, state);
    if (data) pfree(data);
    more();
}

/*static void take(void) {
    int rc;
    Oid argtypes[] = {TEXTOID};
    Datum Values[] = {CStringGetTextDatum(queue)};
    static SPIPlanPtr plan = NULL;
    static char *command = NULL;
    elog(LOG, "%s(%s:%d): queue = %s", __func__, __FILE__, __LINE__, queue);
    if (!command) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfoString(&buf, "WITH s AS (\nSELECT id, COALESCE(max, ~(1<<31)) AS max FROM ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s WHERE id IN (\n", quote_identifier(table));
        appendStringInfoString(&buf,
            "WITH s AS (\n"
            "    SELECT      id, COALESCE(max, ~(1<<31)) AS max\n"
            "    FROM        ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s AS t\n"
            "    WHERE       t.state = 'PLAN'\n"
            "    AND         dt <= current_timestamp\n"
            "    AND         queue = $1\n"
            "    GROUP BY    1, 2\n"
            "    ORDER BY    2 DESC, 1\n"
            ") SELECT unnest((array_agg(id ORDER BY id))[:GREATEST(max(max), 0)]) AS id FROM s\n", quote_identifier(table));
        appendStringInfoString(&buf, ") ORDER BY 2 DESC, 1 LIMIT 1 FOR UPDATE SKIP LOCKED\n) UPDATE ");
        if (schema) appendStringInfo(&buf, "%s.", quote_identifier(schema));
        appendStringInfo(&buf, "%s AS u SET state = 'TAKE' FROM s WHERE u.id = s.id RETURNING u.id", quote_identifier(table));
        command = pstrdup(buf.data);
        pfree(buf.data);
    }
    SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if (!(plan = SPI_prepare(command, sizeof(argtypes)/sizeof(argtypes[0]), argtypes))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_prepare = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("%s(%s:%d): SPI_keepplan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, Values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("%s(%s:%d): SPI_execute_plan = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_commit();
    if (SPI_processed != 1) SPI_finish_my(command); else {
        bool isnull;
        Datum id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &isnull);
        if (isnull) ereport(ERROR, (errmsg("%s(%s:%d): isnull", __func__, __FILE__, __LINE__)));
        SPI_finish_my(command);
        execute(id);
    }
}*/

void task_worker(Datum id); void task_worker(Datum id) {
    start = GetCurrentTimestamp();
    count = 0;
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    schema = username + strlen(username) + 1;
    table = schema + strlen(schema) + 1;
    queue = table + strlen(table) + 1;
    max = *(uint64 *)(queue + strlen(queue) + 1);
    if (table == schema + 1) schema = NULL;
    elog(LOG, "%s(%s:%d): database = %s, username = %s, schema = %s, table = %s, id = %lu, queue = %s, max = %lu", __func__, __FILE__, __LINE__, database, username, schema ? schema : "(null)", table, DatumGetUInt64(id), queue, max);
    schema_q = schema ? quote_identifier(schema) : "";
    point = schema ? "." : "";
    table_q = quote_identifier(table);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(database, username, 0);
    pgstat_report_appname(MyBgworkerEntry->bgw_type);
    execute(id);
}
