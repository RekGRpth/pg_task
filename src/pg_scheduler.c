#include <postgres.h>
#include <fmgr.h>

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <executor/spi.h>
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
#include "utils/varlena.h"

PG_MODULE_MAGIC;

void _PG_init(void);

void loop(Datum arg);
void tick(Datum arg);
void task(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static char *databases;

int period;
char *database;
char *username;
char *schema;
char *table;

static inline void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    (void)SetLatch(MyLatch);
    errno = save_errno;
}

static inline void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    (void)SetLatch(MyLatch);
    errno = save_errno;
}

static inline void launch_loop() {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_function_name, sizeof("loop"), "loop") != sizeof("loop") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler loop"), "pg_scheduler loop") != sizeof("pg_scheduler loop") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_name, sizeof("postgres postgres pg_scheduler loop"), "postgres postgres pg_scheduler loop") != sizeof("postgres postgres pg_scheduler loop") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    (void)RegisterBackgroundWorker(&worker);
}

void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errmsg("pg_scheduler can only be loaded via shared_preload_libraries"), errhint("Add pg_scheduler to the shared_preload_libraries configuration variable in postgresql.conf.")));
    (void)DefineCustomStringVariable("pg_scheduler.database", "pg_scheduler database", NULL, &databases, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)launch_loop();
}

static inline void launch_tick(const char *database, const char *username) {
    int len, len2;
    pid_t pid;
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_function_name, sizeof("tick"), "tick") != sizeof("tick") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler tick"), "pg_scheduler tick") != sizeof("pg_scheduler tick") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len = sizeof("%s %s pg_scheduler tick") - 1 + strlen(database) - 1 + strlen(username) - 1 - 1 - 1;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler tick", database, username) != len) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len = sizeof("%s") - 1 + strlen(database) - 1 - 1;
    if (snprintf(worker.bgw_extra, len + 1, "%s", database) != len) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len2 = sizeof("%s") - 1 + strlen(username) - 1 - 1;
    if (snprintf(worker.bgw_extra + len + 1, len2 + 1, "%s", username) != len2) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
        default: elog(ERROR, "unexpected bgworker handle status");
    }
    if (handle != NULL) (void)pfree(handle);
}

static inline void check() {
    int i = 0;
    List *elemlist;
    StringInfoData buf;
    Oid *argtypes = NULL;
    Datum *Values = NULL;
    char *nulls = NULL;
    char **str = NULL;
    elog(LOG, "check database=%s", databases);
    (void)initStringInfo(&buf);
    (void)appendStringInfoString(&buf,
        "WITH s AS (\n"
        "    SELECT      d.oid, d.datname, u.usesysid, u.usename\n"
        "    FROM        pg_database AS d\n"
        "    JOIN        pg_user AS u ON TRUE\n"
        "    INNER JOIN  pg_user AS i ON d.datdba = i.usesysid\n"
        "    WHERE       NOT datistemplate\n"
        "    AND         datallowconn\n");
    if (databases == NULL) {
        (void)appendStringInfoString(&buf,
            "    AND         i.usesysid = u.usesysid\n");
    } else{
        char *rawstring = pstrdup(databases);
        if (!SplitGUCList(rawstring, ',', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid list syntax in parameter \"pg_scheduler.database\" in postgresql.conf")));
        if ((argtypes = palloc(sizeof(Oid) * list_length(elemlist) * 2)) == NULL) elog(FATAL, "argtypes == NULL %s %i", __FILE__, __LINE__);
        if ((Values = palloc(sizeof(Datum) * list_length(elemlist) * 2)) == NULL) elog(FATAL, "Values == NULL %s %i", __FILE__, __LINE__);
        if ((nulls = palloc(sizeof(char) * list_length(elemlist) * 2)) == NULL) elog(FATAL, "nulls == NULL %s %i", __FILE__, __LINE__);
        if ((str = palloc(sizeof(char *) * list_length(elemlist) * 2)) == NULL) elog(FATAL, "str == NULL %s %i", __FILE__, __LINE__);
        (void)appendStringInfoString(&buf,
            "    AND         (d.datname, u.usename) IN (\n        ");
        for (ListCell *cell = list_head(elemlist); cell != NULL; cell = lnext(cell)) {
            const char *database_username = (const char *)lfirst(cell);
            char *rawstring = pstrdup(database_username);
            List *elemlist;
            if (!SplitIdentifierString(rawstring, ':', &elemlist)) ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid list syntax in parameter \"pg_scheduler.database\" in postgresql.conf"))); else {
                ListCell *cell = list_head(elemlist);
                const char *database = (const char *)lfirst(cell);
                const char *username = database;
                nulls[2 * i] = ' ';
                nulls[2 * i + 1] = ' ';
                if ((cell = lnext(cell)) != NULL) username = (const char *)lfirst(cell);
                else nulls[2 * i + 1] = 'n';
                elog(LOG, "check database=%s, username=%s", database, username);
                if (i > 0) (void)appendStringInfoString(&buf, ", ");
                (void)appendStringInfo(&buf, "($%i, COALESCE($%i, i.usename))", 2 * i + 1, 2 * i + 1 + 1);
                argtypes[2 * i] = TEXTOID;
                argtypes[2 * i + 1] = TEXTOID;
                str[2 * i] = pstrdup(database);
                str[2 * i + 1] = pstrdup(username);
                Values[2 * i] = CStringGetTextDatum(str[2 * i]);
                Values[2 * i + 1] = CStringGetTextDatum(str[2 * i + 1]);
            }
            if (rawstring != NULL) (void)pfree(rawstring);
            if (elemlist != NULL) (void)list_free(elemlist);
            i++;
        }
        (void)appendStringInfoString(&buf,
        "\n    )\n");
        if (rawstring != NULL) (void)pfree(rawstring);
        if (elemlist != NULL) (void)list_free(elemlist);
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
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
    elog(LOG, "check buf.data=\n%s", buf.data);
    if (SPI_execute_with_args(buf.data, i * 2, argtypes, Values, nulls, false, 0) != SPI_OK_SELECT) elog(FATAL, "SPI_execute_with_args != SPI_OK_SELECT %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool isnull;
        char *database = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "datname"), &isnull));
        char *username = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "usename"), &isnull));
        bool start = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "start"), &isnull));
        elog(LOG, "check row=%lu, database=%s, username=%s, start=%s", row, database, username, start?"true":"false");
        if (start) (void)launch_tick(database, username);
    }
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
    if (argtypes != NULL) (void)pfree(argtypes);
    if (Values != NULL) (void)pfree(Values);
    if (nulls != NULL) (void)pfree(nulls);
    for (int j = 0; j < i * 2; j++) if (str[j] != NULL) (void)pfree(str[j]);
    if (str != NULL) (void)pfree(str);
}

void loop(Datum arg) {
    elog(LOG, "loop database=%s", databases);
    (pqsigfunc)pqsignal(SIGHUP, sighup);
    (pqsigfunc)pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection("postgres", "postgres", 0);
    (void)check();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | /*WL_TIMEOUT |*/ WL_POSTMASTER_DEATH, LONG_MAX, PG_WAIT_EXTENSION);
        if (rc & WL_LATCH_SET) elog(LOG, "loop WL_LATCH_SET");
        //if (rc & WL_TIMEOUT) elog(LOG, "loop WL_TIMEOUT");
        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "loop WL_POSTMASTER_DEATH");
        if (got_sigterm) elog(LOG, "loop got_sigterm");
        if (got_sighup) elog(LOG, "loop got_sighup");
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

static inline void lock() {
    const char *src = "SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_user.usesysid::INT) FROM pg_database, pg_user WHERE datname = current_catalog AND usename = current_user";
    (void)pgstat_report_activity(STATE_RUNNING, src);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
    elog(LOG, "lock src=%s", src);
    if (SPI_execute(src, false, 0) != SPI_OK_SELECT) elog(FATAL, "SPI_execute != SPI_OK_SELECT %s %i", __FILE__, __LINE__);
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1 %s %i", __FILE__, __LINE__); else {
        bool isnull;
        bool lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "pg_try_advisory_lock"), &isnull));
        if (isnull) elog(FATAL, "isnull %s %i", __FILE__, __LINE__);
        if (!lock) elog(FATAL, "already running database=%s, username=%s %s %i", database, username, __FILE__, __LINE__);
    }
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, src);
    (void)pgstat_report_stat(true);
}

static inline void init_schema() {
    StringInfoData buf;
    elog(LOG, "init database=%s, username=%s, period=%i, schema=%s, table=%s", database, username, period, schema, table);
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS %s", quote_identifier(schema));
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
    elog(LOG, "init_schema buf.data=%s", buf.data);
    if (SPI_execute(buf.data, false, 0) != SPI_OK_UTILITY) elog(FATAL, "SPI_execute != SPI_OK_UTILITY %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
}

static inline void init_table() {
    StringInfoData buf;
    elog(LOG, "init database=%s, username=%s, period=%i, schema=%s, table=%s", database, username, period, schema, table);
    (void)initStringInfo(&buf);
    if (schema != NULL) (void)appendStringInfo(&buf, "CREATE TABLE IF NOT EXISTS %s.%s (\n", quote_identifier(schema), quote_identifier(table));
    else (void)appendStringInfo(&buf, "CREATE TABLE IF NOT EXISTS %s (\n", quote_identifier(table));
    (void)appendStringInfo(&buf,
    "    id BIGSERIAL NOT NULL PRIMARY KEY,\n"
    "    dt TIMESTAMP NOT NULL DEFAULT NOW(),\n"
    "    start TIMESTAMP,\n"
    "    stop TIMESTAMP,\n"
    "    request TEXT NOT NULL,\n"
    "    response TEXT,\n"
    "    state TEXT NOT NULL DEFAULT 'QUEUE',\n"
    "    timeout INTERVAL"
    ")");
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
    elog(LOG, "init_table buf.data=%s", buf.data);
    if (SPI_execute(buf.data, false, 0) != SPI_OK_UTILITY) elog(FATAL, "SPI_execute != SPI_OK_UTILITY %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
}

static inline void init_index(const char *index) {
    StringInfoData buf, name;
    elog(LOG, "init database=%s, username=%s, period=%i, schema=%s, table=%s, index=%s", database, username, period, schema, table, index);
    (void)initStringInfo(&buf);
    (void)initStringInfo(&name);
    (void)appendStringInfo(&name, "%s_%s_idx", table, index);
    if (schema != NULL) (void)appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON %s.%s USING btree (%s)", quote_identifier(name.data), quote_identifier(schema), quote_identifier(table), quote_identifier(index));
    else (void)appendStringInfo(&buf, "CREATE INDEX IF NOT EXISTS %s ON %s USING btree (%s)", quote_identifier(name.data), quote_identifier(table), quote_identifier(index));
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
    elog(LOG, "init_schema buf.data=%s", buf.data);
    if (SPI_execute(buf.data, false, 0) != SPI_OK_UTILITY) elog(FATAL, "SPI_execute != SPI_OK_UTILITY %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
    if (name.data != NULL) (void)pfree(name.data);
}

static inline void launch_task(Datum arg) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    pid_t pid;
    int len, len2, len3, len4;
    elog(LOG, "launch_task database=%s, username=%s, schema=%s, table=%s, id=%li", database, username, schema, table, DatumGetInt64(arg));
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    if (snprintf(worker.bgw_library_name, sizeof("pg_scheduler"), "pg_scheduler") != sizeof("pg_scheduler") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_function_name, sizeof("task"), "task") != sizeof("task") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (snprintf(worker.bgw_type, sizeof("pg_scheduler task"), "pg_scheduler task") != sizeof("pg_scheduler task") - 1) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len = sizeof("%s %s pg_scheduler task") - 1 + strlen(database) - 1 + strlen(username) - 1 - 2;
    if (snprintf(worker.bgw_name, len + 1, "%s %s pg_scheduler task", database, username) != len) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len = sizeof("%s") - 1 + strlen(database) - 1 - 1;
    if (snprintf(worker.bgw_extra, len + 1, "%s", database) != len) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len2 = sizeof("%s") - 1 + strlen(username) - 1 - 1;
    if (snprintf(worker.bgw_extra + len + 1, len2 + 1, "%s", username) != len2) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    len3 = sizeof("%s") - 1 + strlen(table) - 1 - 1;
    if (snprintf(worker.bgw_extra + len + 1 + len2 + 1, len3 + 1, "%s", table) != len3) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    if (schema != NULL) {
        len4 = sizeof("%s") - 1 + strlen(schema) - 1 - 1;
        if (snprintf(worker.bgw_extra + len + 1 + len2 + 1 + len3 + 1, len4 + 1, "%s", schema) != len4) elog(FATAL, "snprintf %s %i", __FILE__, __LINE__);
    }
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = arg;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
        default: elog(ERROR, "unexpected bgworker handle status");
    }
    if (handle != NULL) (void)pfree(handle);
}

static inline void assign() {
    StringInfoData buf;
    (void)initStringInfo(&buf);
    if (schema != NULL) (void)appendStringInfo(&buf, "UPDATE %s.%s SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id", quote_identifier(schema), quote_identifier(table));
    else (void)appendStringInfo(&buf, "UPDATE %s SET state = 'ASSIGN' WHERE state = 'QUEUE' AND dt <= now() RETURNING id", quote_identifier(table));
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
//    elog(LOG, "assign buf.data=%s", buf.data);
    if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute != SPI_OK_UPDATE_RETURNING %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    for (uint64 row = 0; row < SPI_processed; row++) {
        bool isnull;
        Datum value = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "id"), &isnull);
        if (isnull) elog(FATAL, "isnull %s %i", __FILE__, __LINE__);
        elog(LOG, "row=%lu", row);
        (void)launch_task(value);
    }
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
}

static inline void init() {
    if (schema != NULL) (void)init_schema();
    (void)init_table();
    (void)init_index("dt");
    (void)init_index("state");
}

void tick(Datum arg) {
    StringInfoData buf;
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_scheduler_period.%s", database);
    (void)DefineCustomIntVariable(buf.data, "how often to run tick", NULL, &period, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)resetStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_scheduler_schema.%s", database);
    (void)DefineCustomStringVariable(buf.data, "pg_scheduler schema", NULL, &schema, NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);
    (void)resetStringInfo(&buf);
    (void)appendStringInfo(&buf, "pg_scheduler_table.%s", database);
    (void)DefineCustomStringVariable(buf.data, "pg_scheduler table", NULL, &table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    if (buf.data != NULL) (void)pfree(buf.data);
    elog(LOG, "tick database=%s, username=%s, period=%i, schema=%s, table=%s", database, username, period, schema, table);
    (pqsigfunc)pqsignal(SIGHUP, sighup);
    (pqsigfunc)pqsignal(SIGTERM, sigterm);
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)lock();
    (void)init();
    do {
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period, PG_WAIT_EXTENSION);
        if (rc & WL_LATCH_SET) elog(LOG, "tick WL_LATCH_SET");
        //if (rc & WL_TIMEOUT) elog(LOG, "tick WL_TIMEOUT");
        if (rc & WL_POSTMASTER_DEATH) elog(LOG, "tick WL_POSTMASTER_DEATH");
        if (got_sigterm) elog(LOG, "tick got_sigterm");
        if (got_sighup) elog(LOG, "tick got_sighup");
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
        if (rc & WL_TIMEOUT) (void)assign();
    } while (!got_sigterm);
    (void)proc_exit(0);
}

static inline char *work(Datum arg) {
    Oid argtypes[] = {INT8OID};
    Datum Values[] = {arg};
    char *data;
    StringInfoData buf;
    elog(LOG, "work database=%s, username=%s, schema=%s, table=%s, id=%li", database, username, schema, table, DatumGetInt64(arg));
    (void)initStringInfo(&buf);
    if (schema != NULL) (void)appendStringInfo(&buf, "UPDATE %s.%s SET state = 'WORK', start = now() WHERE id = $1 RETURNING request", quote_identifier(schema), quote_identifier(table));
    else (void)appendStringInfo(&buf, "UPDATE %s SET state = 'WORK', start = now() WHERE id = $1 RETURNING request", quote_identifier(table));
    elog(LOG, "work buf.data=%s", buf.data);
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
//    elog(LOG, "work buf.data=%s", buf.data);
    if (SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE_RETURNING) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE_RETURNING %s %i", __FILE__, __LINE__);
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1 %s %i", __FILE__, __LINE__); else {
        char *value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "request"));
        data = strdup(value);
        if (value != NULL) (void)pfree(value);
    }
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
    return data;
}

static inline void done(Datum arg, const char *data, const char *status) {
    Oid argtypes[] = {TEXTOID, TEXTOID, INT8OID};
    Datum Values[] = {CStringGetTextDatum(status), CStringGetTextDatum(data!=NULL?data:"(null)"), arg};
    StringInfoData buf;
    (void)initStringInfo(&buf);
    if (schema != NULL) (void)appendStringInfo(&buf, "UPDATE %s.%s SET state = $1, stop = now(), response=$2 WHERE id = $3", quote_identifier(schema), quote_identifier(table));
    else (void)appendStringInfo(&buf, "UPDATE %s SET state = $1, stop = now(), response=$2 WHERE id = $3", quote_identifier(table));
    elog(LOG, "done buf.data=%s", buf.data);
    (void)pgstat_report_activity(STATE_RUNNING, buf.data);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
//    elog(LOG, "done buf.data=%s", buf.data);
    if (SPI_execute_with_args(buf.data, sizeof(argtypes)/sizeof(argtypes[0]), argtypes, Values, NULL, false, 0) != SPI_OK_UPDATE) elog(FATAL, "SPI_execute_with_args != SPI_OK_UPDATE %s %i", __FILE__, __LINE__);
    (void)SPI_commit();
    if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
    (void)ProcessCompletedNotifies();
    (void)pgstat_report_activity(STATE_IDLE, buf.data);
    (void)pgstat_report_stat(true);
    if (buf.data != NULL) (void)pfree(buf.data);
}

static inline char *success() {
    StringInfoData buf;
    (void)initStringInfo(&buf);
    if ((SPI_tuptable != NULL) && (SPI_processed > 0)) {
        for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
            char *name = SPI_fname(SPI_tuptable->tupdesc, col);
            char *type = SPI_gettype(SPI_tuptable->tupdesc, col);
            (void)appendStringInfo(&buf, "%s::%s", name, type);
            if (col > 1) (void)appendStringInfoString(&buf, "\t");
            if (name != NULL) (void)pfree(name);
            if (type != NULL) (void)pfree(type);
        }
        (void)appendStringInfoString(&buf, "\n");
        for (uint64 row = 0; row < SPI_processed; row++) {
            for (int col = 1; col <= SPI_tuptable->tupdesc->natts; col++) {
                char *value = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, col);
                (void)appendStringInfo(&buf, "%s", value);
                if (col > 1) (void)appendStringInfoString(&buf, "\t");
                if (value != NULL) (void)pfree(value);
            }
            if (row < SPI_processed - 1) (void)appendStringInfoString(&buf, "\n");
        }
        elog(LOG, "success\n%s", buf.data);
    }
    return buf.data;
}

static inline char *error() {
    ErrorData *edata = CopyErrorData();
    StringInfoData buf;
    (void)initStringInfo(&buf);
    (void)appendStringInfo(&buf,
        "elevel::int4\t%i\n"
        "output_to_server::bool\t%s\n"
        "output_to_client::bool\t%s\n"
        "show_funcname::bool\t%s\n"
        "hide_stmt::bool\t%s\n"
        "hide_ctx::bool\t%s\n"
        "filename::text\t%s\n"
        "lineno::int4\t%i\n"
        "funcname::text\t%s\n"
        "domain::text\t%s\n"
        "context_domain::text\t%s\n"
        "sqlerrcode::int4\t%i\n"
        "message::text\t%s\n"
        "detail::text\t%s\n"
        "detail_log::text\t%s\n"
        "hint::text\t%s\n"
        "context::text\t%s\n"
        "message_id::text\t%s\n"
        "schema_name::text\t%s\n"
        "table_name::text\t%s\n"
        "column_name::text\t%s\n"
        "datatype_name::text\t%s\n"
        "constraint_name::text\t%s\n"
        "cursorpos::int4\t%i\n"
        "internalpos::int4\t%i\n"
        "internalquery::text\t%s\n"
        "saved_errno::int4\t%i",
        edata->elevel,
        edata->output_to_server?"true":"false",
        edata->output_to_client?"true":"false",
        edata->show_funcname?"true":"false",
        edata->hide_stmt?"true":"false",
        edata->hide_ctx?"true":"false",
        edata->filename,
        edata->lineno,
        edata->funcname,
        edata->domain,
        edata->context_domain,
        edata->sqlerrcode,
        edata->message,
        edata->detail,
        edata->detail_log,
        edata->hint,
        edata->context,
        edata->message_id,
        edata->schema_name,
        edata->table_name,
        edata->column_name,
        edata->datatype_name,
        edata->constraint_name,
        edata->cursorpos,
        edata->internalpos,
        edata->internalquery,
        edata->saved_errno
    );
    (void)FreeErrorData(edata);
    elog(LOG, "error\n%s", buf.data);
    return buf.data;
}

static inline void execute(Datum arg) {
    char *src = work(arg);
//    elog(LOG, "execute src=%s", src);
    elog(LOG, "execute database=%s, username=%s, schema=%s, table=%s, src=\n%s", database, username, schema, table, src);
//    elog(LOG, "src=%s", src);
    (void)pgstat_report_activity(STATE_RUNNING, src);
    if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT) elog(FATAL, "SPI_connect_ext != SPI_OK_CONNECT %s %i", __FILE__, __LINE__);
    (void)SPI_start_transaction();
//    elog(LOG, "execute src=%s", src);
    if (StatementTimeout > 0) (void)enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout); else (void)disable_timeout(STATEMENT_TIMEOUT, false);
    PG_TRY(); {
//        elog(LOG, "execute try SPI_commit_or_rollback_and_finish 1 src=%s", src);
        if (SPI_execute(src, false, 0) < 0) elog(FATAL, "SPI_execute < 0 %s %i", __FILE__, __LINE__); else {
            char *data = success();
            (void)disable_timeout(STATEMENT_TIMEOUT, false);
//            elog(LOG, "execute try SPI_commit_or_rollback_and_finish 2 src=%s", src);
            (void)SPI_commit();
            if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
            (void)ProcessCompletedNotifies();
            (void)pgstat_report_activity(STATE_IDLE, src);
            (void)pgstat_report_stat(true);
            (void)done(arg, data, "DONE");
            if (data != NULL) (void)pfree(data);
        }
    } PG_CATCH(); {
        char *data = error();
        (void)disable_timeout(STATEMENT_TIMEOUT, false);
//        elog(LOG, "execute catch SPI_commit_or_rollback_and_finish src=%s", src);
        (void)SPI_rollback();
        if (SPI_finish() != SPI_OK_FINISH) elog(FATAL, "SPI_finish != SPI_OK_FINISH %s %i", __FILE__, __LINE__);
        (void)ProcessCompletedNotifies();
        (void)pgstat_report_activity(STATE_IDLE, src);
        (void)pgstat_report_stat(true);
        (void)done(arg, data, "FAIL");
        if (data != NULL) (void)pfree(data);
    } PG_END_TRY();
    if (src != NULL) (void)free(src);
}

void task(Datum arg) {
    database = MyBgworkerEntry->bgw_extra;
    username = database + strlen(database) + 1;
    table = username + strlen(username) + 1;
    schema = table + strlen(table) + 1;
    if (strlen(schema) == 0) schema = NULL;
    elog(LOG, "task database=%s, username=%s, schema=%s, table=%s, id=%li", database, username, schema, table, DatumGetInt64(arg));
    (void)BackgroundWorkerUnblockSignals();
    (void)BackgroundWorkerInitializeConnection(database, username, 0);
    (void)execute(arg);
}
