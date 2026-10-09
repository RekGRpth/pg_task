#include "include.h"

#ifndef WIN32
#include <dlfcn.h>
#endif
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/timestamp.h>

#if PG_VERSION_NUM < 90500
#include <storage/barrier.h>
#endif

#if PG_VERSION_NUM < 130000
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif

PG_MODULE_MAGIC;

static struct {
    char *null;
    char *plan;
    struct {
        int fetch;
        int max;
        int restart;
    } conf;
    struct {
        bool delete;
        bool drift;
        bool header;
        bool save;
        bool spi;
        bool string;
        char *active;
        char *data;
        char *delimiter;
        char *escape;
        char *group;
        char *id;
        char *json;
        char *live;
        char *quote;
        char *repeat;
        char *reset;
        char *schema;
        char *table;
        char *timeout;
        char *user;
        int count;
        int fetch;
        int limit;
        int max;
        int run;
        int sleep;
    } task;
    struct {
        int fetch;
        int idle;
        int restart;
    } work;
} init = {0};
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static Shared *shared = NULL;
// the pauses of groups, which a task of a negative max schedules as it's done (see task_done()), till their ends: not only for the tasks of the group planned then, whose plans task_update() puts off, but for those inserted or planned later too, which work_sleep() takes no sooner; in shared memory, as the task worker that's done with a local task is another process than the pg_work taking the next one, and kept no longer than the server runs, a pause across its restart not kept
typedef struct Pause {
    int hash;
    Oid database;
    Oid oid;
    TimestampTz until;
} Pause;
static Pause *pauses = NULL;
#if PG_VERSION_NUM < 130000
volatile sig_atomic_t ShutdownRequestPending = false;
#endif

const char *init_null(void) { return init.null; }
const char *init_plan(void) { return init.plan; }
int init_conf_fetch(void) { return init.conf.fetch; }
int init_task_fetch(void) { return init.task.fetch; }
int init_work_fetch(void) { return init.work.fetch; }
int init_work_idle(void) { return init.work.idle; }

bool init_oid_is_string(Oid oid) {
    switch (oid) {
        case BITOID:
        case BOOLOID:
        case CIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case NUMERICOID:
        case OIDOID:
        case TIDOID:
        case XIDOID:
            return false;
        default: return true;
    }
}

// the table in the tags of the locks of its tasks and groups, with the database it's in: one made from another as its template has a task table of the same oid, and the same ids, both in pg_task.json say, whose locks would be one and the same; as pg_locks shows it as the database of the lock
uint32 init_table_key(Oid table) {
    return (uint32)table ^ ((uint32)MyDatabaseId * 2654435761U);
}

bool lock_data_user_hash(Oid data, Oid user, int hash) {
    LOCKTAG tag = {data, user, (uint32)hash, 3, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i, user = %i, hash = %i", data, user, hash);
    return LockAcquire(&tag, AccessExclusiveLock, true, true) == LOCKACQUIRE_OK;
}

bool lock_data_user(Oid data, Oid user) {
    LOCKTAG tag = {data, data, user, 6, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i, user = %i", data, user);
    return LockAcquire(&tag, AccessExclusiveLock, true, true) == LOCKACQUIRE_OK;
}

// the self-provisioning of pg_work in a database, waited for: two of them, for tables of the same schema, would both find it or its enum of states missing, and both create it, the second one failing on the first one's
bool lock_data_make(Oid data) {
    LOCKTAG tag = {data, 0, 0, 8, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i", data);
    return LockAcquire(&tag, AccessExclusiveLock, true, false) == LOCKACQUIRE_OK;
}

bool unlock_data_make(Oid data) {
    LOCKTAG tag = {data, 0, 0, 8, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i", data);
    return LockRelease(&tag, AccessExclusiveLock, true);
}

bool lock_table_id(Oid table, int64 id) {
    LOCKTAG tag = {init_table_key(table), (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li", table, id);
    return LockAcquire(&tag, AccessExclusiveLock, true, true) == LOCKACQUIRE_OK;
}

// the slot of the group hash a task runs in: taken by its task worker and, while that runs, by pg_work too (work_local), or by pg_work for its remote connection; so already held is fine, it's counted by distinct pid
bool lock_table_pid_hash(Oid table, int pid, int hash) {
    LOCKTAG tag = {init_table_key(table), (uint32)pid, (uint32)hash, 5, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, pid = %i, hash = %i", table, pid, hash);
    return LockAcquire(&tag, AccessShareLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

// the slot of the group hash a remote task takes before its connection has a pid to lock with lock_table_pid_hash(); only the low half of the id, so already held is fine
bool lock_table_id_hash(Oid table, int64 id, int hash) {
    LOCKTAG tag = {init_table_key(table), (uint32)id, (uint32)hash, 7, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li, hash = %i", table, id, hash);
    return LockAcquire(&tag, AccessShareLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

bool unlock_data_user_hash(Oid data, Oid user, int hash) {
    LOCKTAG tag = {data, user, (uint32)hash, 3, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i, user = %i, hash = %i", data, user, hash);
    return LockRelease(&tag, AccessExclusiveLock, true);
}

bool unlock_data_user(Oid data, Oid user) {
    LOCKTAG tag = {data, data, user, 6, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "data = %i, user = %i", data, user);
    return LockRelease(&tag, AccessExclusiveLock, true);
}

bool unlock_table_id(Oid table, int64 id) {
    LOCKTAG tag = {init_table_key(table), (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li", table, id);
    return LockRelease(&tag, AccessExclusiveLock, true);
}

bool unlock_table_id_hash(Oid table, int64 id, int hash) {
    LOCKTAG tag = {init_table_key(table), (uint32)id, (uint32)hash, 7, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li, hash = %i", table, id, hash);
    return LockRelease(&tag, AccessShareLock, true);
}

bool unlock_table_pid_hash(Oid table, int pid, int hash) {
    LOCKTAG tag = {init_table_key(table), (uint32)pid, (uint32)hash, 5, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, pid = %i, hash = %i", table, pid, hash);
    return LockRelease(&tag, AccessShareLock, true);
}

// the advisory locks of the session, which pg_advisory_lock() and the like take, let go of as DISCARD ALL does, but not those of pg_task, as pg_advisory_unlock_all() would too: one hold at a time, the lock manager keeping the count of them to itself
void unlock_advisory_all(void) {
    for (bool released = true; released; ) {
        LockData *data = GetLockStatusData();
        released = false;
        for (int i = 0; i < data->nelements && !released; i++) {
            LockInstanceData *instance = &data->locks[i];
            if (instance->pid != MyProcPid || instance->locktag.locktag_type != LOCKTAG_ADVISORY || instance->locktag.locktag_lockmethodid != USER_LOCKMETHOD) continue;
            for (LOCKMODE mode = 1; mode < MAX_LOCKMODES; mode++) if (instance->holdMask & LOCKBIT_ON(mode) && LockRelease(&instance->locktag, mode, true)) released = true;
        }
        pfree(data->locks);
        pfree(data);
    }
}

// a lock of pg_task back, which an input may have let go of, as pg_advisory_unlock_all() and DISCARD ALL let go of user locks with advisory ones: held still, the hold just taken goes again, for it to stay held once
static void relock(const LOCKTAG *tag, LOCKMODE mode) {
    switch (LockAcquire(tag, mode, true, true)) {
        case LOCKACQUIRE_OK: elog(DEBUG1, "taken back"); break;
        case LOCKACQUIRE_NOT_AVAIL: ereport(WARNING, (errmsg("could not take back lock %u, %u, %u, %u", tag->locktag_field1, tag->locktag_field2, tag->locktag_field3, tag->locktag_field4))); break;
        default: LockRelease(tag, mode, true); break;
    }
}

void relock_table_id(Oid table, int64 id) {
    LOCKTAG tag = {init_table_key(table), (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li", table, id);
    relock(&tag, AccessExclusiveLock);
}

void relock_table_pid_hash(Oid table, int pid, int hash) {
    LOCKTAG tag = {init_table_key(table), (uint32)pid, (uint32)hash, 5, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, pid = %i, hash = %i", table, pid, hash);
    relock(&tag, AccessShareLock);
}

static char *text_to_cstring_my(const text *t) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    char *result = text_to_cstring(t);
    MemoryContextSwitchTo(oldMemoryContext);
    return result;
}

char *TextDatumGetCStringMy(Datum datum) {
    return datum ? text_to_cstring_my((text *)DatumGetPointer(datum)) : NULL;
}


void appendBinaryStringInfoEscapeQuote(StringInfo buf, const char *data, int len, bool string, char escape, char quote) {
    if (!string && quote) appendStringInfoChar(buf, quote);
    if (len) {
        if (!string && escape && quote) for (int i = 0; len-- > 0; i++) {
            if (quote == data[i] || escape == data[i]) appendStringInfoChar(buf, escape);
            appendStringInfoChar(buf, data[i]);
        } else appendBinaryStringInfo(buf, data, len);
    }
    if (!string && quote) appendStringInfoChar(buf, quote);
}

static size_t init_shared_memsize(void) {
    return add_size(mul_size(init.conf.max, sizeof(Shared)), mul_size(init.conf.max, sizeof(Pause))); // as many pauses as slots, those of the more groups at once putting off the ones of the soonest ends, see init_pause()
}

#if PG_VERSION_NUM >= 150000
static void init_shmem_request_hook(void) {
    if (prev_shmem_request_hook) prev_shmem_request_hook();
    RequestAddinShmemSpace(init_shared_memsize());
}
#endif

static void init_shmem_startup_hook(void) {
    bool found;
    if (prev_shmem_startup_hook) prev_shmem_startup_hook();
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    shared = ShmemInitStruct("pg_shared", init_shared_memsize(), &found);
    if (!found) MemSet(shared, 0, init_shared_memsize());
    pauses = (Pause *)(shared + init.conf.max);
    elog(DEBUG1, "pg_shared %s found", found ? "" : "not");
    LWLockRelease(AddinShmemInitLock);
}

void initStringInfoMy(StringInfo buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}

static void init_libpq(void) {
#ifndef WIN32
    Dl_info backend, libpq;
    // Greengage's postgres executable carries its own backend build of libpq, which speaks the internal protocol that pg_hba.conf lets through unchecked, so the remote tasks must never end up there
    if (dladdr((void *)palloc, &backend) && dladdr((void *)PQconnectStartParams, &libpq) && backend.dli_fbase == libpq.dli_fbase) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("libpq is resolved into the postgres executable"), errdetail("Remote tasks would connect with the backend build of libpq, which bypasses pg_hba.conf."), errhint("Rebuild pg_task so that it links the frontend libpq privately.")));
#endif
}

// pg_task.reset is a positive interval, which pg_conf and pg_work cast it to in one query for all the entries, of every database: one that isn't, set for a database or a role by its owner, which a setting of the user's own allows, would fail it for all of them, the other databases' pg_work left unstarted; refused as set, then, as a setting of a type of its own is
static bool init_check_interval(char **newval, void **extra, GucSource source) {
    bool valid = true;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    Interval *volatile interval = NULL;
    PG_TRY();
        interval = DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum(*newval), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
    PG_CATCH();
        MemoryContextSwitchTo(oldMemoryContext);
        FlushErrorState();
        GUC_check_errdetail("\"%s\" is not an interval.", *newval);
        valid = false;
    PG_END_TRY();
#ifdef INTERVAL_NOT_FINITE
    // a finite one, from 17 on, whose milliseconds would be out of the range of int8
    if (valid && INTERVAL_NOT_FINITE(interval)) {
        GUC_check_errdetail("\"%s\" is not a finite interval.", *newval);
        valid = false;
    }
#endif
    // and a positive one, or pg_work would reset ever after, or never: as EXTRACT(epoch) has it, which the queries take it by, its whole years of 365.25 days, the months left of 30
    if (valid && (double)(interval->month / MONTHS_PER_YEAR) * DAYS_PER_YEAR * USECS_PER_DAY + (double)(interval->month % MONTHS_PER_YEAR) * DAYS_PER_MONTH * USECS_PER_DAY + (double)interval->day * USECS_PER_DAY + (double)interval->time <= 0) {
        GUC_check_errdetail("\"%s\" is not a positive interval.", *newval);
        valid = false;
    }
    if (interval) pfree(interval);
    return valid;
}

void _PG_init(void) {
    BackgroundWorker worker = {0};
    size_t len;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    init_libpq();
    DefineCustomBoolVariable("pg_task.delete", "pg_task delete", "Auto delete task when both output and error are nulls", &init.task.delete, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.drift", "pg_task drift", "Compute next repeat time by stop time instead by plan time", &init.task.drift, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.header", "pg_task header", "Show columns headers in output", &init.task.header, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.save", "pg_task save", "Save session state between tasks", &init.task.save, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.spi", "pg_task spi", "SPI (or local) execution?", &init.task.spi, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.string", "pg_task string", "Quote only strings", &init.task.string, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.fetch", "pg_conf fetch", "Fetch conf rows at once", &init.conf.fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.max", "pg_conf work", "Maximum task and work workers", &init.conf.max, max_worker_processes, 1, max_worker_processes, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.restart", "pg_conf restart", "Restart conf interval, seconds", &init.conf.restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.count", "pg_task count", "Non-negative maximum count of tasks, are executed by current background worker process before exit", &init.task.count, 0, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.fetch", "pg_task fetch", "Fetch task rows at once", &init.task.fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.limit", "pg_task limit", "Limit task rows at once", &init.task.limit, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.max", "pg_task max", "Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds", &init.task.max, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.run", "pg_task run", "Maximum count of concurrently executing tasks in work", &init.task.run, INT_MAX, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.sleep", "pg_task sleep", "Check tasks every sleep milliseconds", &init.task.sleep, 1000, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.fetch", "pg_work fetch", "Fetch work rows at once", &init.work.fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.idle", "pg_work idle", "Idle work count", &init.work.idle, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.restart", "pg_work restart", "Restart work interval, seconds", &init.work.restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.active", "pg_task active", "Positive period after plan time, when task is active for executing", &init.task.active, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.data", "pg_task data", "Database name for tasks table", &init.task.data, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.delimiter", "pg_task delimiter", "Results columns delimiter", &init.task.delimiter, "\t", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.escape", "pg_task escape", "Results columns escape", &init.task.escape, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.group", "pg_task group", "Task grouping by name", &init.task.group, "group", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.id", "pg_task id", "Current task id", &init.task.id, "0", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", "Json configuration, available keys: data, reset, run, schema, sleep, spi, table and user", &init.task.json, SQL([{"data":"postgres"}]), PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.live", "pg_task live", "Non-negative maximum time of live of current background worker process before exit", &init.task.live, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.null", "pg_task null", "Null text value representation", &init.null, "\\N", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.plan", "pg_task plan", "Default value for plan timestamp", &init.plan, "statement_timestamp()", PGC_SUSET, 0, NULL, NULL, NULL); // an SQL expression, which the bookkeeping, as pg_task.user, and pg_work run as is: for superusers only to set, not for task authors in their own sessions
    DefineCustomStringVariable("pg_task.quote", "pg_task quote", "Results columns quote", &init.task.quote, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.repeat", "pg_task repeat", "Non-negative auto repeat tasks interval", &init.task.repeat, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.reset", "pg_task reset", "Interval of reset tasks", &init.task.reset, "1 hour", PGC_USERSET, 0, init_check_interval, NULL, NULL);
    DefineCustomStringVariable("pg_task.schema", "pg_task schema", "Schema name for tasks table", &init.task.schema, "public", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", "Table name for tasks table", &init.task.table, "task", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.timeout", "pg_task timeout", "Non-negative allowed time for task run", &init.task.timeout, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.user", "pg_task user", "User name for tasks table", &init.task.user, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(DEBUG1, "json = %s, user = %s, data = %s, schema = %s, table = %s, null = %s, sleep = %i, reset = %s, active = %s", init.task.json, init.task.user, init.task.data, init.task.schema, init.task.table, init.null, init.task.sleep, init.task.reset, init.task.active);
#ifdef GP_VERSION_NUM
    if (!IS_QUERY_DISPATCHER()) return;
#endif
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = init_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = init_shmem_request_hook;
#else
    RequestAddinShmemSpace(init_shared_memsize());
#endif
    if ((len = strlcpy(worker.bgw_function_name, "conf_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = strlcpy(worker.bgw_name, "postgres pg_conf", sizeof(worker.bgw_name))) >= sizeof(worker.bgw_name)) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_name))));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = init.conf.restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    RegisterBackgroundWorker(&worker);
}

// into the error of a task, up to the most it may keep, see TASK_OUTPUT_MAX, rather than fail on a message or a statement that won't fit in a string buffer with the rest of it: task_done() cuts it to that, at a character
void append_with_tabs(StringInfo buf, const char *str) {
    char ch;
    while ((ch = *str++) != '\0' && buf->len < (int)TASK_OUTPUT_MAX) {
        appendStringInfoCharMacro(buf, ch);
        if (ch == '\n') appendStringInfoCharMacro(buf, '\t');
    }
}

const char *error_severity(int elevel) {
    const char *prefix;
    switch (elevel) {
        case DEBUG1: case DEBUG2: case DEBUG3: case DEBUG4: case DEBUG5: prefix = gettext_noop("DEBUG"); break;
        case LOG:
#if PG_VERSION_NUM >= 90600
        case LOG_SERVER_ONLY:
#endif
            prefix = gettext_noop("LOG"); break;
        case INFO: prefix = gettext_noop("INFO"); break;
        case NOTICE: prefix = gettext_noop("NOTICE"); break;
        case WARNING:
#if PG_VERSION_NUM >= 140000
        case WARNING_CLIENT_ONLY:
#endif
            prefix = gettext_noop("WARNING"); break;
        case ERROR: prefix = gettext_noop("ERROR"); break;
        case FATAL: prefix = gettext_noop("FATAL"); break;
        case PANIC: prefix = gettext_noop("PANIC"); break;
        default: prefix = "???"; break;
    }
    return prefix;
}

int severity_error(const char *error) {
    if (!error) return ERROR;
    if (!pg_strcasecmp("DEBUG", error)) return DEBUG1;
    if (!pg_strcasecmp("ERROR", error)) return ERROR;
    if (!pg_strcasecmp("FATAL", error)) return FATAL;
    if (!pg_strcasecmp("INFO", error)) return INFO;
    if (!pg_strcasecmp("LOG", error)) return LOG;
    if (!pg_strcasecmp("NOTICE", error)) return NOTICE;
    if (!pg_strcasecmp("PANIC", error)) return PANIC;
    if (!pg_strcasecmp("WARNING", error)) return WARNING;
    return ERROR;
}

bool is_log_level_output(int elevel, int log_min_level) {
    if (elevel == LOG
#if PG_VERSION_NUM >= 90600
        || elevel == LOG_SERVER_ONLY
#endif
    ) {
        if (log_min_level == LOG || log_min_level <= ERROR) return true;
#if PG_VERSION_NUM >= 140000
    } else if (elevel == WARNING_CLIENT_ONLY) {
        return false; // never sent to log, regardless of log_min_level
#endif
    } else if (log_min_level == LOG) {
        if (elevel >= FATAL) return true; // elevel != LOG
    } else if (elevel >= log_min_level) return true; // Neither is LOG
    return false;
}

int init_arg(const Shared *s) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int slot = 0; slot < init.conf.max; slot++) if (!shared[slot].in_use) {
        shared[slot] = *s;
        pg_write_barrier();
        shared[slot].in_use = true;
        LWLockRelease(BackgroundWorkerLock);
        elog(DEBUG1, "slot = %i", slot);
        return slot;
    }
    LWLockRelease(BackgroundWorkerLock);
    return -1;
}

// the entries of pg_task.json with their settings, as WITH j AS (...) of a query, which pg_conf takes them all by, and pg_work its own, see conf_check() and work_check(): a setting of an entry by its key in pg_task.json, or else as set for its role in its database, for its role, for its database, or for the server, the one of the configuration files rather than of the session asking, as that is of a database and a role of its own; the sleep, run and reset, past the bounds of the settings, 1 at least, as the settings have them, reset in milliseconds rounded up, not to seconds first, and below 1e15, infinity say; the int settings of a database or a role as the text they are kept in, parsed as the settings parse them, see init_int(); and the place of the entry in pg_task.json, i, for the first of the entries of the same database, user, schema and table to be taken
void init_settings(StringInfo src) {
    appendStringInfo(src, SQL(
            WITH j AS (
                WITH s AS (
                    WITH s AS (
                        SELECT "setdatabase", "setrole", ARRAY[pg_catalog.split_part("kv", '=', 1), pg_catalog.substr("kv", pg_catalog.length(pg_catalog.split_part("kv", '=', 1)) OPERATOR(pg_catalog.+) 2)] AS "setconfig" FROM "pg_catalog"."pg_db_role_setting", pg_catalog.unnest("setconfig") AS "kv"
                    ) SELECT "setdatabase", "setrole", pg_catalog.%s(pg_catalog.array_agg("setconfig"[1]), pg_catalog.array_agg("setconfig"[2])) AS "setconfig" FROM s GROUP BY 1, 2
                ), g AS (
                    %s
                ) SELECT    COALESCE("data", "user", pg_catalog.current_setting('pg_task.data')::pg_catalog.name)::pg_catalog.text AS "data",
                            pg_catalog.ceil(GREATEST(LEAST(EXTRACT(epoch FROM COALESCE("reset", (r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, (g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval))::pg_catalog.float8 OPERATOR(pg_catalog.*) 1000, 1e15), 1))::pg_catalog.int8 AS "reset",
                            "run", COALESCE(r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run', g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run') AS "run_setting",
                            COALESCE("schema", r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', (g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema'))::pg_catalog.text AS "schema",
                            COALESCE("table", r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', (g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table'))::pg_catalog.text AS "table",
                            "sleep", COALESCE(r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep', g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep') AS "sleep_setting",
                            COALESCE("spi", (r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool, (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool, (g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool)::pg_catalog.bool AS "spi",
                            COALESCE(r."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit', g."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit') AS "limit",
                            COALESCE(r."setconfig" OPERATOR(pg_catalog.->>) 'pg_work.restart', u."setconfig" OPERATOR(pg_catalog.->>) 'pg_work.restart', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_work.restart', g."setconfig" OPERATOR(pg_catalog.->>) 'pg_work.restart') AS "restart",
                            COALESCE("user", "data", pg_catalog.current_setting('pg_task.user')::pg_catalog.name)::pg_catalog.text AS "user", "i"
                FROM        ROWS FROM (pg_catalog.jsonb_to_recordset(pg_catalog.current_setting('pg_task.json')::pg_catalog.jsonb) AS ("data" pg_catalog.name, "reset" interval, "run" int4, "schema" text, "table" text, "sleep" int8, "spi" bool, "user" pg_catalog.name)) WITH ORDINALITY AS j ("data", "reset", "run", "schema", "table", "sleep", "spi", "user", "i")
                CROSS JOIN  g
                LEFT JOIN   s AS d on d."setdatabase" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) COALESCE("data", "user", pg_catalog.current_setting('pg_task.data')::pg_catalog.name)) AND d."setrole" OPERATOR(pg_catalog.=) 0::pg_catalog.oid
                LEFT JOIN   s AS u on u."setrole" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) COALESCE("user", "data", pg_catalog.current_setting('pg_task.user')::pg_catalog.name)) AND u."setdatabase" OPERATOR(pg_catalog.=) 0::pg_catalog.oid
                LEFT JOIN   s AS r on r."setdatabase" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) COALESCE("data", "user", pg_catalog.current_setting('pg_task.data')::pg_catalog.name)) AND r."setrole" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) COALESCE("user", "data", pg_catalog.current_setting('pg_task.user')::pg_catalog.name))
            )
    ),
#if PG_VERSION_NUM >= 90500
        "jsonb_object",
        // the session of pg_conf or pg_work itself got the settings of its own database and role on connecting, which aren't those of other databases and roles: for a setting from there, fall back to the one of the server's configuration files instead, or else to the default
        SQL(
            SELECT pg_catalog.jsonb_object(pg_catalog.array_agg("name"), pg_catalog.array_agg("setting")) AS "setconfig" FROM (
                SELECT "name", CASE WHEN "source" OPERATOR(pg_catalog.=) ANY(ARRAY['database', 'user', 'database user']) THEN COALESCE((SELECT f."setting" FROM "pg_catalog"."pg_file_settings" AS f WHERE f."name" OPERATOR(pg_catalog.=) p."name" AND f."error" IS NULL ORDER BY f."seqno" DESC LIMIT 1), "boot_val") ELSE "setting" END AS "setting" FROM "pg_catalog"."pg_settings" AS p WHERE "name" OPERATOR(pg_catalog.~~) 'pg_task.%' OR "name" OPERATOR(pg_catalog.=) 'pg_work.restart'
            ) AS p
        )
#else
        "json_object",
        // no pg_file_settings yet to tell the server's configuration files apart from the settings of the database and role of pg_conf or pg_work itself
        SQL(
            SELECT pg_catalog.json_object(pg_catalog.array_agg("name"), pg_catalog.array_agg("setting")) AS "setconfig" FROM "pg_catalog"."pg_settings" WHERE "name" OPERATOR(pg_catalog.~~) 'pg_task.%' OR "name" OPERATOR(pg_catalog.=) 'pg_work.restart'
        )
#endif
    );
}

// an int setting of a database or a role, as the text it's kept in there, of a column of a query of pg_conf or pg_work, parsed as the setting parses it, rather than cast, which takes neither 1.5 nor 1e3, nor 0x10 before 16, failing that query for every entry of pg_task.json, and takes 010 for 10, the setting for 8: one that doesn't parse, as none should, the setting checked as it was set, goes by the setting of this session
int init_int(HeapTuple val, TupleDesc tupdesc, const char *column, const char *name) {
    char *value = TextDatumGetCString(SPI_getbinval_my(val, tupdesc, column, false, TEXTOID));
    int result;
    if (!parse_int(value, &result, 0, NULL)) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value for parameter \"%s\": \"%s\"", name, value)));
        if (!parse_int(GetConfigOption(name, false, false), &result, 0, NULL)) result = 0;
    }
    pfree(value);
    return result;
}

// the slots free for task workers now, one each, to take no more local tasks than that: only a guess, others taking some meanwhile
int init_free_slots(void) {
    int free = 0;
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    for (int slot = 0; slot < init.conf.max; slot++) if (!shared[slot].in_use) free++;
    LWLockRelease(BackgroundWorkerLock);
    return free;
}

void init_free(int slot) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    MemSet(&shared[slot], 0, sizeof(Shared));
    LWLockRelease(BackgroundWorkerLock);
}

// free the slot of a stopped pg_work only if it still holds that pg_work: a clean exit has already freed it itself, and by now it may belong to someone else (a task worker has a non-zero id)
bool init_free_work(int slot, int64 reg) {
    bool freed;
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    // by its registration rather than its entry: a later pg_work of the same entry may have taken the slot by now
    if ((freed = shared[slot].in_use && !shared[slot].id && shared[slot].reg == reg)) MemSet(&shared[slot], 0, sizeof(Shared));
    LWLockRelease(BackgroundWorkerLock);
    return freed;
}

// the slots of pg_work are what pg_conf knows of the ones it started across its own restarts, which lose its handles of them: one that crashed, or can't connect, its database not allowing connections, say, is restarted by the postmaster after a while with its slot kept, and over and over in the latter case, so a pg_conf restarted meanwhile mustn't add another one for its entry each time, nor leave it restarting once its entry is gone; in one go, for a pg_work restarted meanwhile not to see a state half way: in_use tells of each entry wanted whether a pg_work of it is alive, started and not exited yet, or starting, with no pid yet, for no other one to be started; one that isn't, waiting to be restarted, is gone, taken over by the one pg_conf starts now, as is one whose entry is gone, for either to exit cleanly once restarted, freeing its slot
void init_work(int n, const char **data, const char **user, const int *hash, bool *in_use) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int i = 0; i < n; i++) in_use[i] = false;
    for (int slot = 0; slot < init.conf.max; slot++) if (shared[slot].in_use && !shared[slot].id && !shared[slot].gone) {
        int wanted = -1;
        for (int i = 0; i < n; i++) if (shared[slot].hash == hash[i] && !strcmp(shared[slot].data, data[i]) && !strcmp(shared[slot].user, user[i])) { wanted = i; break; }
        if (wanted >= 0 && (!shared[slot].pid || !kill(shared[slot].pid, 0) || errno != ESRCH)) in_use[wanted] = true;
        else shared[slot].gone = true;
    }
    LWLockRelease(BackgroundWorkerLock);
}

// a task worker on its way out frees a slot of its group, for the next task of the group, which its pg_work learns of from the postmaster, but not one restarted since an earlier one started the task worker: wake the pg_work of its table, as the wake-up trigger does, found by its slot
void init_work_wake(const Shared *task) {
    int pid = 0;
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    for (int slot = 0; slot < init.conf.max; slot++) if (shared[slot].in_use && !shared[slot].id && shared[slot].reg && !shared[slot].gone && shared[slot].pid && !strcmp(shared[slot].data, task->data) && !strcmp(shared[slot].user, task->user) && !strcmp(shared[slot].schema, task->schema) && !strcmp(shared[slot].table, task->table)) { pid = shared[slot].pid; break; }
    LWLockRelease(BackgroundWorkerLock);
    if (pid && kill(pid, SIGINT)) elog(DEBUG1, "could not wake pg_work %i: %m", pid);
}

// the tasks of a table that task workers run, by their slots, which outlive a restart of the pg_work that started them, and which no input can let go of, as it can of the lock of its task, with pg_advisory_unlock_all() say: for work_reset() not to take them for orphaned
void init_task_ids(StringInfo ids, const char *data, Oid oid) {
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    for (int slot = 0; slot < init.conf.max; slot++) if (shared[slot].in_use && shared[slot].id && shared[slot].oid == oid && !strcmp(shared[slot].data, data)) appendStringInfo(ids, "%s%li", ids->len > 1 ? "," : "", shared[slot].id);
    LWLockRelease(BackgroundWorkerLock);
}

// the task workers of a table, by their slots, each with its pid, or a placeholder unlike any for one yet to start, and the hash of its group: for a pass to count the slots of a group taken with them too, which their inputs can't let go of, as they can of their locks, a worker counted once, its lock held still or not (see work_sleep())
void init_task_pids(const char *data, Oid oid, StringInfo pids, StringInfo hashes) {
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    for (int slot = 0; slot < init.conf.max; slot++) if (shared[slot].in_use && shared[slot].id && shared[slot].oid == oid && !strcmp(shared[slot].data, data)) {
        appendStringInfo(pids, "%s%i", pids->len > 1 ? "," : "", shared[slot].pid ? shared[slot].pid : -1 - slot);
        appendStringInfo(hashes, "%s%i", hashes->len > 1 ? "," : "", shared[slot].hash);
    }
    LWLockRelease(BackgroundWorkerLock);
}

// the pause of a group till until, the later of it and one there already, in place of one ended, or, none such, of the one of the soonest end
void init_pause(Oid oid, int hash, TimestampTz until) {
    int slot = -1;
    TimestampTz now = GetCurrentTimestamp();
    if (until <= now) return; // over already, not to put off one still on in its place
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int i = 0; i < init.conf.max; i++) if (pauses[i].until > now && pauses[i].database == MyDatabaseId && pauses[i].oid == oid && pauses[i].hash == hash) { slot = i; break; }
    if (slot >= 0) { if (pauses[slot].until < until) pauses[slot].until = until; } else {
        for (int i = 0; i < init.conf.max; i++) if (slot < 0 || pauses[i].until < pauses[slot].until) slot = i; // an ended one, never used say, sooner than any other
        pauses[slot].database = MyDatabaseId;
        pauses[slot].oid = oid;
        pauses[slot].hash = hash;
        pauses[slot].until = until;
    }
    LWLockRelease(BackgroundWorkerLock);
}

// the groups of a table on a pause now, by their hashes, if hashes, and the soonest end of their pauses, 0 for none
TimestampTz init_pauses(Oid oid, StringInfo hashes) {
    TimestampTz now = GetCurrentTimestamp();
    TimestampTz soonest = 0;
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    for (int i = 0; i < init.conf.max; i++) if (pauses[i].until > now && pauses[i].database == MyDatabaseId && pauses[i].oid == oid) {
        if (hashes) appendStringInfo(hashes, "%s%i", hashes->len > 1 ? "," : "", pauses[i].hash);
        if (!soonest || pauses[i].until < soonest) soonest = pauses[i].until;
    }
    LWLockRelease(BackgroundWorkerLock);
    return soonest;
}

// the pid of a task worker in its slot, as soon as pg_work knows it started, before it takes the lock of the group's slot by it: till the worker writes it itself, its slot would count apart from that lock, see init_task_pids(), the group a slot short; only while the slot still has the task, the worker gone already and its slot another's maybe
void init_task_pid(int slot, const char *data, Oid oid, int64 id, int pid) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    if (shared[slot].in_use && shared[slot].id == id && shared[slot].oid == oid && !strcmp(shared[slot].data, data) && !shared[slot].pid) shared[slot].pid = pid;
    LWLockRelease(BackgroundWorkerLock);
}

bool init_work_gone(Datum main_arg) {
    bool gone;
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    gone = shared[DatumGetInt32(main_arg)].gone;
    LWLockRelease(BackgroundWorkerLock);
    return gone;
}

// the same for the slot of a task worker that stopped before pg_work saw it start: one that ran and exited meanwhile has freed it itself, and by now it may belong to someone else
bool init_free_task(int slot, const char *data, Oid oid, int64 id) {
    bool freed;
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    if ((freed = shared[slot].in_use && shared[slot].id == id && shared[slot].oid == oid && !strcmp(shared[slot].data, data))) MemSet(&shared[slot], 0, sizeof(Shared));
    LWLockRelease(BackgroundWorkerLock);
    return freed;
}

// marks the task work_stop() cancels in the slot of the task worker running it, for that worker to cancel only that one (see dest_cancel()): found by the task itself, as it may be a worker that an earlier pg_work started, before it was restarted
bool init_stop(const char *data, Oid oid, int64 id) {
    bool marked = false;
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int slot = 0; slot < init.conf.max; slot++) if (shared[slot].in_use && shared[slot].id == id && shared[slot].oid == oid && !strcmp(shared[slot].data, data)) {
        shared[slot].stop = id;
        marked = true;
        break;
    }
    LWLockRelease(BackgroundWorkerLock);
    return marked;
}

Shared *init_shared(Datum main_arg) {
    int slot = DatumGetInt32(main_arg);
    LWLockAcquire(BackgroundWorkerLock, LW_SHARED);
    LWLockRelease(BackgroundWorkerLock);
    return &shared[slot];
}

#if PG_VERSION_NUM < 130000
void
SignalHandlerForConfigReload(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ConfigReloadPending = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
#endif
