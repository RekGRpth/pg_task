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
char *database;
char *username;
char *schema;
char *table;

SPIPlanPtr plan = NULL;
char *command = NULL;

static void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    (void)SetLatch(MyLatch);
    errno = save_errno;
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    (void)SetLatch(MyLatch);
    errno = save_errno;
}

static void launch_loop(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_function_name, sizeof("loop"), "loop") != sizeof("loop") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_type, sizeof("pg_task loop"), "pg_task loop") != sizeof("pg_task loop") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_name, sizeof("postgres postgres pg_task loop"), "postgres postgres pg_task loop") != sizeof("postgres postgres pg_task loop") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    (void)RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("pg_task can only be loaded via shared_preload_libraries"), errhint("Add pg_task to the shared_preload_libraries configuration variable in postgresql.conf.")));
    (void)DefineCustomStringVariable("pg_task.database", "pg_task database", NULL, &databases, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)DefineCustomIntVariable("pg_task.task_id", "pg_task task_id", NULL, &task_id, 0, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    (void)launch_loop();
}

static void launch_tick(const char *database, const char *username) {
    int len, len2;
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_function_name, sizeof("tick"), "tick") != sizeof("tick") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_type, sizeof("pg_task tick"), "pg_task tick") != sizeof("pg_task tick") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len = (sizeof("%s %s pg_task tick") - 1) + (strlen(username) - 1) + (strlen(database) - 1) - 1 - 1;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_task tick", username, database) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len = (sizeof("%s") - 1) + (strlen(database) - 1) - 1;
    if (snprintf(worker.bgw_extra, len + 1, "%s", database) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len2 = (sizeof("%s") - 1) + (strlen(username) - 1) - 1;
    if (snprintf(worker.bgw_extra + len + 1, len2 + 1, "%s", username) != len2) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
        default: ereport(ERROR, (errmsg("Unexpected bgworker handle status")));
    }
    (void)pfree(handle);
}

static void SPI_connect_my(const char *command, int timeout) {
    int rc;
    (void)pgstat_report_activity(STATE_RUNNING, command);
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) ereport(ERROR, (errmsg("SPI_connect_ext = %s", SPI_result_code_string(rc))));
    (void)pgstat_report_appname(MyBgworkerEntry->bgw_type);
    (void)SPI_start_transaction();
    if (timeout > 0) (void)enable_timeout_after(STATEMENT_TIMEOUT, timeout); else (void)disable_timeout(STATEMENT_TIMEOUT, false);
}

static void SPI_finish_my(const char *command) {
    int rc;
    (void)disable_timeout(STATEMENT_TIMEOUT, false);
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errmsg("SPI_finish = %s", SPI_result_code_string(rc))));
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, command);
    (void)pgstat_report_stat(true);
}

static void check(void) {
    int rc, i = 0;
    List *elemlist;
    StringInfoData buf;
    Oid *argtypes = NULL;
    Datum *Values = NULL;
    char *Nulls = NULL;
    char **str = NULL;
//    elog(LOG, "check database = %s", databases);
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf,
        "WITH s AS (\n"
        "    SELECT      d.oid, d.datname, u.usesysid, u.usename\n"
        "    FROM        pg_database AS d\n"
        "    JOIN        pg_user AS u ON TRUE\n"
        "    INNER JOIN  pg_user AS i ON d.datdba = i.usesysid\n"
        "    WHERE       NOT datistemplate\n"
        "    AND         datallowconn\n");
    if (!databases) (void)appendStringInfoString(&buf, "    AND         i.usesysid = u.usesysid\n"); else {
        char *rawstring = pstrdup(databases);
        if (!SplitGUCList(rawstring, ',', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid list syntax in parameter \"pg_task.database\" in postgresql.conf")));
        argtypes = palloc(sizeof(Oid) * list_length(elemlist) * 2);
        Values = palloc(sizeof(Datum) * list_length(elemlist) * 2);
        Nulls = palloc(sizeof(char) * list_length(elemlist) * 2);
        str = palloc(sizeof(char *) * list_length(elemlist) * 2);
        (void)appendStringInfoString(&buf, "    AND         (d.datname, u.usename) IN (\n        ");
        for (ListCell *cell = list_head(elemlist); cell; cell = lnext(cell)) {
            const char *database_username = (const char *)lfirst(cell);
            char *rawstring = pstrdup(database_username);
            List *elemlist;
            if (!SplitIdentifierString(rawstring, ':', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid list syntax in parameter \"pg_task.database\" in postgresql.conf"))); else {
                ListCell *cell = list_head(elemlist);
                const char *database = (const char *)lfirst(cell);
                const char *username = NULL;
                Nulls[2 * i] = ' ';
                Nulls[2 * i + 1] = ' ';
                if ((cell = lnext(cell))) username = (const char *)lfirst(cell);
                else Nulls[2 * i + 1] = 'n';
//                elog(LOG, "check database = %s, username = %s", database, username);
                if (i > 0) (void)appendStringInfoString(&buf, ", ");
                (void)appendStringInfo(&buf, "($%i, COALESCE($%i, i.usename))", 2 * i + 1, 2 * i + 1 + 1);
                argtypes[2 * i] = TEXTOID;
                argtypes[2 * i + 1] = TEXTOID;
                str[2 * i] = pstrdup(database);
                str[2 * i + 1] = username ? pstrdup(username) : NULL;
                Values[2 * i] = CStringGetTextDatum(str[2 * i]);
                Values[2 * i + 1] = username ? CStringGetTextDatum(str[2 * i + 1]) : (Datum)NULL;
            }
            (void)pfree(rawstring);
            (void)list_free(elemlist);
            i++;
        }
        (void)appendStringInfoString(&buf, "\n    )\n");
        (void)pfree(rawstring);
        (void)list_free(elemlist);
    }
    (void)appendStringInfoString(&buf,
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
//    elog(LOG, "check buf.data = %s", buf.data);
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, i * 2, argtypes, Values, Nulls, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("SPI_execute_with_args = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool isnull, start;
        char *username, *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        start = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "start"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        if (start) (void)launch_tick(database, username);
    }
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
    if (argtypes) (void)pfree(argtypes);
    if (Values) (void)pfree(Values);
    if (Nulls) (void)pfree(Nulls);
    if (str) { for (int j = 0; j < i * 2; j++) if (str[j]) (void)pfree(str[j]); (void)pfree(str); }
}

void loop(Datum arg); void loop(Datum arg) {
//    elog(LOG, "loop database = %s", databases);
    (pqsigfunc)pqsignal(SIGHUP, sighup);
    (pqsigfunc)pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    (void)check();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | /*WL_TIMEOUT |*/ WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
//        if (rc & WL_LATCH_SET) elog(LOG, "loop WL_LATCH_SET");
//        if (rc & WL_TIMEOUT) elog(LOG, "loop WL_TIMEOUT");
//        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "loop WL_POSTMASTER_DEATH");
//        if (got_sigterm) elog(LOG, "loop got_sigterm");
//        if (got_sighup) elog(LOG, "loop got_sighup");
//        if (ProcDiePending) elog(LOG, "loop ProcDiePending");
        if (rc & WL_POSTMASTER_DEATH) (void)proc_exit(1);
        if (rc & WL_LATCH_SET) {
            (void)ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        if (got_sighup) {
            got_sighup = false;
            (void)ProcessConfigFile(PGC_SIGHUP);
            (void)check();
        }
        if (got_sigterm) (void)proc_exit(0);
    } while (!got_sigterm);
    (void)proc_exit(0);
}

static void lock(void) {
    int rc;
    const char *command = "SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_user.usesysid::INT) FROM pg_database, pg_user WHERE datname = current_catalog AND usename = current_user";
    (void)SPI_connect_my(command, StatementTimeout);
    if ((rc = SPI_execute(command, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed != 1"))); else {
        bool isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "pg_try_advisory_lock"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        if (!lock) ereport(ERROR, (errmsg("Already running database = %s, username = %s", database, username)));
        MyBgworkerEntry->bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    }
    (void)SPI_finish_my(command);
}

static void init_schema(void) {
    int rc;
    StringInfoData buf;
//    elog(LOG, "init_schema database = %s, username = %s, period = %i, schema = %s, table = %s", database, username, period, schema, table);
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", quote_identifier(schema));
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void init_table(void) {
    int rc;
    StringInfoData buf, name;
//    elog(LOG, "init_table database = %s, username = %s, period = %i, schema = %s, table = %s", database, username, period, schema, table);
    (void)initStringInfo(&buf);
    (void)initStringInfo(&name);
    (void)appendStringInfo(&name, "%s_parent_fkey", table);
    (void)appendStringInfoString(&buf, "CREATE TABLE IF NOT EXISTS ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s (\n", quote_identifier(table));
    (void)appendStringInfo(&buf,
        "    id BIGSERIAL NOT NULL PRIMARY KEY,\n"
        "    parent BIGINT,\n"
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
        "    CONSTRAINT %s FOREIGN KEY (parent) REFERENCES ", quote_identifier(name.data));
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL\n)", quote_identifier(table));
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void init_index(const char *index) {
    int rc;
    StringInfoData buf, name;
//    elog(LOG, "init_index database = %s, username = %s, period = %i, schema = %s, table = %s, index = %s", database, username, period, schema, table, index);
    (void)initStringInfo(&buf);
    (void)initStringInfo(&name);
    (void)appendStringInfo(&name, "%s_%s_idx", table, index);
    (void)appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON ", quote_identifier(name.data));
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s USING btree (%s)", quote_identifier(table), quote_identifier(index));
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UTILITY) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
    (void)pfree(name.data);
}

static void init_fix(void) {
    int rc;
    StringInfoData buf;
//    elog(LOG, "init_fix database = %s, username = %s, period = %i, schema = %s, table = %s", database, username, period, schema, table);
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf, "UPDATE ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf,
        "%s\n"
        "    SET state = 'PLAN'\n"
        "    WHERE state IN ('TAKE', 'WORK')\n"
        "    AND pid NOT IN (\n"
        "        SELECT pid\n"
        "        FROM pg_stat_activity\n"
        "        WHERE datname = current_catalog\n"
        "        AND usename = current_user\n"
        "        AND application_name = concat_ws(' ', 'pg_task task', queue, id)\n"
        "    )",
        quote_identifier(table));
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute(buf.data, false, 0)) != SPI_OK_UPDATE) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void launch_task(Datum arg, const char *queue) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    pid_t pid;
    int len, len2, len3, len4;
    uint64 id = DatumGetInt64(arg);
//    elog(LOG, "launch_task database = %s, username = %s, schema = %s, table = %s, id = %lu, queue = %s", database, username, schema, table, id, queue);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = arg;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (snprintf(worker.bgw_function_name, sizeof("task"), "task") != sizeof("task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len = (sizeof("pg_task task %s") - 1) + (strlen(queue) - 1) - 1;
    if (snprintf(worker.bgw_type, len + 1, "pg_task task %s", queue) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len = (sizeof("%s %s pg_task task %s %lu") - 1) + (strlen(username) - 1) + (strlen(database) - 1) + (strlen(queue) - 1) - 1 - 1 - 1 - 2;
    for (int number = id; number /= 10; len++);
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_task task %s %lu", username, database, queue, id) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len = (sizeof("%s") - 1) + (strlen(database) - 1) - 1;
    if (snprintf(worker.bgw_extra, len + 1, "%s", database) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len2 = (sizeof("%s") - 1) + (strlen(username) - 1) - 1;
    if (snprintf(worker.bgw_extra + len + 1, len2 + 1, "%s", username) != len2) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    len3 = (sizeof("%s") - 1) + (strlen(table) - 1) - 1;
    if (snprintf(worker.bgw_extra + len + 1 + len2 + 1, len3 + 1, "%s", table) != len3) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    if (schema) {
        len4 = (sizeof("%s") - 1) + (strlen(schema) - 1) - 1;
        if (snprintf(worker.bgw_extra + len + 1 + len2 + 1 + len3 + 1, len4 + 1, "%s", schema) != len4) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
    }
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
        default: ereport(ERROR, (errmsg("Unexpected bgworker handle status")));
    }
    (void)pfree(handle);
}

static void take(void) {
    int rc;
    if (!command) {
        StringInfoData buf;
        (void)initStringInfo(&buf);
        (void)appendStringInfoString(&buf,
            "WITH s AS (\n"
            "    SELECT      id, queue, COALESCE(max, ~(1<<31)) AS max, count(a.pid)\n"
            "    FROM        ");
        if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
        (void)appendStringInfo(&buf, "%s AS t\n"
            "    LEFT JOIN   pg_stat_activity AS a ON datname = current_catalog AND usename = current_user AND backend_type = concat('pg_task task ', queue)\n"
            "    WHERE       t.state = 'PLAN'\n"
            "    AND         dt <= current_timestamp\n"
            "    GROUP BY    1, 2, 3\n"
            "    ORDER BY    3 DESC, 1\n"
            ") SELECT unnest((array_agg(id))[:GREATEST(max(max) - count, 0)]) AS id, queue FROM s GROUP BY queue, count", quote_identifier(table));
        command = pstrdup(buf.data);
        (void)pfree(buf.data);
    }
    (void)SPI_connect_my(command, StatementTimeout);
    if (!plan) {
        if(!(plan = SPI_prepare(command, 0, NULL))) ereport(ERROR, (errmsg("SPI_prepare = %s", SPI_result_code_string(SPI_result))));
        if ((rc = SPI_keepplan(plan))) ereport(ERROR, (errmsg("SPI_keepplan = %s", SPI_result_code_string(rc))));
    }
    if ((rc = SPI_execute_plan(plan, NULL, NULL, false, 0)) != SPI_OK_SELECT) ereport(ERROR, (errmsg("SPI_execute_plan = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool isnull;
        char *queue;
        Datum id = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &isnull);
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        queue = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "queue"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
//        elog(LOG, "take_callback row = %lu, id = %lu, queue = %s", row, DatumGetInt64(id), queue);
        (void)launch_task(id, queue);
        (void)pfree(queue);
    }
    (void)SPI_finish_my(command);
}

static void init(void) {
    if (schema) (void)init_schema();
    (void)init_table();
    (void)init_index("dt");
    (void)init_index("state");
    (void)init_fix();
}

void tick(Datum arg); void tick(Datum arg) {
    StringInfoData buf;
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_task_period.%s", database);
    (void)DefineCustomIntVariable(buf.data, "how often to run tick", NULL, &period, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)resetStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_task_schema.%s", database);
    (void)DefineCustomStringVariable(buf.data, "pg_task schema", NULL, &schema, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)resetStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_task_table.%s", database);
    (void)DefineCustomStringVariable(buf.data, "pg_task table", NULL, &table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)pfree(buf.data);
//    elog(LOG, "tick database = %s, username = %s, period = %i, schema = %s, table = %s", database, username, period, schema, table);
    (pqsigfunc)pqsignal(SIGHUP, sighup);
    (pqsigfunc)pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)lock();
    (void)init();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period, PG_WAIT_EXTENSION);
//        if (rc & WL_LATCH_SET) elog(LOG, "tick WL_LATCH_SET");
//        if (rc & WL_TIMEOUT) elog(LOG, "tick WL_TIMEOUT");
//        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "tick WL_POSTMASTER_DEATH");
//        if (got_sigterm) elog(LOG, "tick got_sigterm");
//        if (got_sighup) elog(LOG, "tick got_sighup");
//        if (ProcDiePending) elog(LOG, "loop ProcDiePending");
        if (rc & WL_POSTMASTER_DEATH) (void)proc_exit(1);
        if (rc & WL_LATCH_SET) {
            (void)ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        if (got_sighup) {
            got_sighup = false;
            (void)ProcessConfigFile(PGC_SIGHUP);
            (void)init();
        }
        if (got_sigterm) (void)proc_exit(0);
        if (rc & WL_TIMEOUT) (void)take();
    } while (!got_sigterm);
    (void)proc_exit(0);
}

static void work(Datum arg, char **src, int *timeout) {
    int rc;
    Oid argtypes[] = {INT8OID, INT8OID};
    Datum Values[] = {arg, MyProcPid};
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    StringInfoData buf;
//    elog(LOG, "work database = %s, username = %s, schema = %s, table = %s, id = %lu", database, username, schema, table, DatumGetInt64(arg));
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "%lu", DatumGetInt64(arg));
    if (set_config_option("pg_task.task_id", buf.data, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false) <= 0) ereport(ERROR, (errmsg("set_config_option <= 0")));
    (void)resetStringInfo(&buf);
    (void)appendStringInfoString(&buf, "WITH s AS (\n    SELECT id FROM ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s\n"
        "    WHERE id = $1\n"
//        "    AND state = 'TAKE'\n"
        "    FOR UPDATE SKIP LOCKED\n)\n", quote_identifier(table));
    (void)appendStringInfoString(&buf, "UPDATE ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s AS u\n"
        "SET state = 'WORK',\n"
        "start = current_timestamp,\n"
        "pid = $2\n"
        "FROM s\n"
        "WHERE u.id = s.id\n"
        "RETURNING request,\n"
        "COALESCE(EXTRACT(epoch FROM timeout), 0)::INT * 1000 AS timeout", quote_identifier(table));
//    elog(LOG, "work buf.data = %s", buf.data);
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("SPI_execute_with_args = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed != 1"))); else {
        bool isnull;
        char *value = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        *timeout = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "timeout"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        *src = MemoryContextStrdup(oldMemoryContext, value);
//        elog(LOG, "work timeout = %i, src = %s", *timeout, *src);
        (void)pfree(value);
    }
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void repeat_task(Datum arg) {
    int rc;
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {arg};
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf, "INSERT INTO ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s (parent, dt, queue, max, request, state, timeout, delete, repeat, drift) (SELECT ", quote_identifier(table));
    (void)appendStringInfoString(&buf, "id AS parent, CASE WHEN drift THEN current_timestamp + repeat ELSE (WITH RECURSIVE s AS (SELECT dt AS t UNION SELECT t + repeat FROM s WHERE t <= current_timestamp) SELECT * FROM s ORDER BY 1 DESC LIMIT 1) END AS dt, queue, max, request, 'PLAN' as state, timeout, delete, repeat, drift FROM ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s WHERE id = $1 AND state IN ('DONE', 'FAIL') LIMIT 1)", quote_identifier(table));
//    elog(LOG, "repeat_task buf.data = %s", buf.data);
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0)) != SPI_OK_INSERT) ereport(ERROR, (errmsg("SPI_execute_with_args = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void delete_task(Datum arg) {
    int rc;
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {arg};
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf, "DELETE FROM ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s WHERE id = $1", quote_identifier(table));
//    elog(LOG, "delete_task buf.data = %s", buf.data);
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0)) != SPI_OK_DELETE) ereport(ERROR, (errmsg("SPI_execute_with_args = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
}

static void done(Datum arg, const char *data, const char *state) {
    int rc;
    bool delete, repeat;
    Oid argtypes[] = {INT8OID, TEXTOID, TEXTOID};
    Datum Values[] = {arg, CStringGetTextDatum(state), data ? CStringGetTextDatum(data) : (Datum)NULL};
    char Nulls[] = {' ', ' ', data ? ' ' : 'n'};
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf, "WITH s AS (\n    SELECT id FROM ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s\n"
        "    WHERE id = $1\n"
//        "    AND state = 'TAKE'\n"
        "    FOR UPDATE SKIP LOCKED\n)\n", quote_identifier(table));
    (void)appendStringInfoString(&buf, "UPDATE ");
    if (schema) (void)appendStringInfo(&buf, "%s.", quote_identifier(schema));
    (void)appendStringInfo(&buf, "%s AS u\n"
        "SET state = $2,\n"
        "stop = current_timestamp,\n"
        "response = $3\n"
        "FROM s\n"
        "WHERE u.id = s.id\n"
        "RETURNING delete,\n"
        "repeat IS NOT NULL AND state IN ('DONE', 'FAIL') AS repeat", quote_identifier(table));
//    elog(LOG, "done buf.data = %s", buf.data);
    (void)SPI_connect_my(buf.data, StatementTimeout);
    if ((rc = SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, Nulls, false, 0)) != SPI_OK_UPDATE_RETURNING) ereport(ERROR, (errmsg("SPI_execute_with_args = %s", SPI_result_code_string(rc))));
    (void)SPI_commit();
    if (SPI_processed != 1) ereport(ERROR, (errmsg("SPI_processed != 1"))); else {
        bool isnull;
        delete = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "delete"), &isnull)) && (Nulls[2] == 'n');
        if (isnull) ereport(ERROR, (errmsg("isnull")));
        repeat = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "repeat"), &isnull));
        if (isnull) ereport(ERROR, (errmsg("isnull")));
    }
    (void)SPI_finish_my(buf.data);
    (void)pfree(buf.data);
    if (repeat) (void)repeat_task(arg);
    if (delete) (void)delete_task(arg);
}

static char *success(MemoryContext oldMemoryContext) {
    char *data = NULL;
    StringInfoData buf;
    (void)initStringInfo(&buf);
    if ((SPI_tuptable) && (SPI_processed > 0)) {
        if (SPI_tuptable->tupdesc->natts > 1) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *name = SPI_fname(SPI_tuptable->tupdesc, col);
                char *type = SPI_gettype(SPI_tuptable->tupdesc, col);
                (void)appendStringInfo(&buf, "%s::%s", name, type);
                if (col > 1) (void)appendStringInfoString(&buf, "\t");
                (void)pfree(name);
                (void)pfree(type);
            }
            (void)appendStringInfoString(&buf, "\n");
        }
        for (uint64 row = 0; row < SPI_processed; row++) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                (void)appendStringInfo(&buf, "%s", value);
                if (col > 1) (void)appendStringInfoString(&buf, "\t");
                (void)pfree(value);
            }
            if (row < SPI_processed - 1) (void)appendStringInfoString(&buf, "\n");
        }
//        elog(LOG, "success\n%s", buf.data);
        data = MemoryContextStrdup(oldMemoryContext, buf.data);
    }
    (void)pfree(buf.data);
    return data;
}

static char *error(MemoryContext oldMemoryContext) {
    char *data = NULL;
    ErrorData *edata = CopyErrorData();
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "elevel::int4\t%i", edata->elevel);
    (void)appendStringInfo(&buf, "\noutput_to_server::bool\t%s", edata->output_to_server ? "true" : "false");
    (void)appendStringInfo(&buf, "\noutput_to_client::bool\t%s", edata->output_to_client ? "true" : "false");
    (void)appendStringInfo(&buf, "\nshow_funcname::bool\t%s", edata->show_funcname ? "true" : "false");
    (void)appendStringInfo(&buf, "\nhide_stmt::bool\t%s", edata->hide_stmt ? "true" : "false");
    (void)appendStringInfo(&buf, "\nhide_ctx::bool\t%s", edata->hide_ctx ? "true" : "false");
    if (edata->filename) (void)appendStringInfo(&buf, "\nfilename::text\t%s", edata->filename);
    if (edata->lineno) (void)appendStringInfo(&buf, "\nlineno::int4\t%i", edata->lineno);
    if (edata->funcname) (void)appendStringInfo(&buf, "\nfuncname::text\t%s", edata->funcname);
    if (edata->domain) (void)appendStringInfo(&buf, "\ndomain::text\t%s", edata->domain);
    if (edata->context_domain) (void)appendStringInfo(&buf, "\ncontext_domain::text\t%s", edata->context_domain);
    if (edata->sqlerrcode) (void)appendStringInfo(&buf, "\nsqlerrcode::int4\t%i", edata->sqlerrcode);
    if (edata->message) (void)appendStringInfo(&buf, "\nmessage::text\t%s", edata->message);
    if (edata->detail) (void)appendStringInfo(&buf, "\ndetail::text\t%s", edata->detail);
    if (edata->detail_log) (void)appendStringInfo(&buf, "\ndetail_log::text\t%s", edata->detail_log);
    if (edata->hint) (void)appendStringInfo(&buf, "\nhint::text\t%s", edata->hint);
    if (edata->context) (void)appendStringInfo(&buf, "\ncontext::text\t%s", edata->context);
    if (edata->message_id) (void)appendStringInfo(&buf, "\nmessage_id::text\t%s", edata->message_id);
    if (edata->schema_name) (void)appendStringInfo(&buf, "\nschema_name::text\t%s", edata->schema_name);
    if (edata->table_name) (void)appendStringInfo(&buf, "\ntable_name::text\t%s", edata->table_name);
    if (edata->column_name) (void)appendStringInfo(&buf, "\ncolumn_name::text\t%s", edata->column_name);
    if (edata->datatype_name) (void)appendStringInfo(&buf, "\ndatatype_name::text\t%s", edata->datatype_name);
    if (edata->constraint_name) (void)appendStringInfo(&buf, "\nconstraint_name::text\t%s", edata->constraint_name);
    if (edata->cursorpos) (void)appendStringInfo(&buf, "\ncursorpos::int4\t%i", edata->cursorpos);
    if (edata->internalpos) (void)appendStringInfo(&buf, "\ninternalpos::int4\t%i", edata->internalpos);
    if (edata->internalquery) (void)appendStringInfo(&buf, "\ninternalquery::text\t%s", edata->internalquery);
    if (edata->saved_errno) (void)appendStringInfo(&buf, "\nsaved_errno::int4\t%i", edata->saved_errno);
    (void)FreeErrorData(edata);
//    elog(LOG, "error\n%s", buf.data);
    data = MemoryContextStrdup(oldMemoryContext, buf.data);
    (void)pfree(buf.data);
    return data;
}

static void execute(Datum arg) {
    int rc;
    char *src, *data = NULL, *state;
    int timeout = 0;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    (void)work(arg, &src, &timeout);
    if ((StatementTimeout > 0) && (StatementTimeout < timeout)) timeout = StatementTimeout;
//    elog(LOG, "execute database = %s, username = %s, schema = %s, table = %s, timeout = %i, src = %s", database, username, schema, table, timeout, src);
    (void)SPI_connect_my(src, timeout);
    PG_TRY(); {
        if ((rc = SPI_execute(src, false, 0)) < 0) ereport(ERROR, (errmsg("SPI_execute = %s", SPI_result_code_string(rc))));
        state = "DONE";
        data = success(oldMemoryContext);
        (void)SPI_commit();
    } PG_CATCH(); {
        state = "FAIL";
        data = error(oldMemoryContext);
        (void)SPI_rollback();
    } PG_END_TRY();
    (void)SPI_finish_my(src);
    (void)done(arg, data, state);
    (void)pfree(src);
    if (data) (void)pfree(data);
}

static void update_bgw_type(Datum arg) {
    uint64 id = DatumGetInt64(arg);
    int len = (sizeof(" %lu") - 1) - 2;
    for (int number = id; number /= 10; len++);
    if (snprintf(MyBgworkerEntry->bgw_type + strlen(MyBgworkerEntry->bgw_type), len + 1, " %lu", id) != len) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("snprintf")));
}

void task(Datum arg); void task(Datum arg) {
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    table = username + strlen(username) + 1;
    schema = table + strlen(table) + 1;
    if (!strlen(schema)) schema = NULL;
//    elog(LOG, "task database = %s, username = %s, schema = %s, table = %s, id = %lu", database, username, schema, table, DatumGetInt64(arg));
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)update_bgw_type(arg);
    (void)execute(arg);
}
