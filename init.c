#include "include.h"

#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/memutils.h>

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
    struct {
        int close;
        int fetch;
        int max;
        int restart;
    } conf;
    struct {
        bool delete;
        bool drift;
        bool header;
        bool spi;
        bool string;
        char *active;
        char *data;
        char *delimiter;
        char *escape;
        char *group;
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
        int id;
        int limit;
        int max;
        int run;
        int sleep;
    } task;
    struct {
        char *active;
        int close;
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
#if PG_VERSION_NUM < 130000
volatile sig_atomic_t ShutdownRequestPending = false;
#endif

const char *init_null(void) { return init.null; }
int init_conf_fetch(void) { return init.conf.fetch; }
int init_task_fetch(void) { return init.task.fetch; }
int init_work_fetch(void) { return init.work.fetch; }
int init_work_idle(void) { return init.work.idle; }
int init_work_restart(void) { return init.work.restart; }

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

bool lock_table_id(Oid table, int64 id) {
    LOCKTAG tag = {table, (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li", table, id);
    return LockAcquire(&tag, AccessExclusiveLock, true, true) == LOCKACQUIRE_OK;
}

bool lock_table_pid_hash(Oid table, int pid, int hash) {
    LOCKTAG tag = {table, (uint32)pid, (uint32)hash, 5, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, pid = %i, hash = %i", table, pid, hash);
    return LockAcquire(&tag, AccessShareLock, true, true) == LOCKACQUIRE_OK;
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
    LOCKTAG tag = {table, (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, id = %li", table, id);
    return LockRelease(&tag, AccessExclusiveLock, true);
}

bool unlock_table_pid_hash(Oid table, int pid, int hash) {
    LOCKTAG tag = {table, (uint32)pid, (uint32)hash, 5, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    elog(DEBUG1, "table = %i, pid = %i, hash = %i", table, pid, hash);
    return LockRelease(&tag, AccessShareLock, true);
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

static text *cstring_to_text_my(const char *s) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    text *result = cstring_to_text(s);
    MemoryContextSwitchTo(oldMemoryContext);
    return result;
}

Datum CStringGetTextDatumMy(const char *s) {
    return s ? PointerGetDatum(cstring_to_text_my(s)) : (Datum)NULL;
}

void appendBinaryStringInfoEscapeQuote(StringInfo buf, const char *data, int len, bool string, char escape, char quote) {
    if (!string && quote) appendStringInfoChar(buf, quote);
    if (len) {
        if (!string && escape && quote) for (int i = 0; len-- > 0; i++) {
            if (quote == data[i]) appendStringInfoChar(buf, escape);
            appendStringInfoChar(buf, data[i]);
        } else appendBinaryStringInfo(buf, data, len);
    }
    if (!string && quote) appendStringInfoChar(buf, quote);
}

static size_t init_shared_memsize(void) {
    return mul_size(init.conf.max, sizeof(Shared));
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
    elog(DEBUG1, "pg_shared %s found", found ? "" : "not");
    LWLockRelease(AddinShmemInitLock);
}

void initStringInfoMy(StringInfo buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}

void _PG_init(void) {
    BackgroundWorker worker = {0};
    size_t len;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomBoolVariable("pg_task.delete", "pg_task delete", "Auto delete task when both output and error are nulls", &init.task.delete, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.drift", "pg_task drift", "Compute next repeat time by stop time instead by plan time", &init.task.drift, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.header", "pg_task header", "Show columns headers in output", &init.task.header, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.spi", "pg_task spi", "SPI (or local) execution?", &init.task.spi, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.string", "pg_task string", "Quote only strings", &init.task.string, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.close", "pg_conf close", "Close conf, milliseconds", &init.conf.close, BGW_DEFAULT_RESTART_INTERVAL * 1000, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.fetch", "pg_conf fetch", "Fetch conf rows at once", &init.conf.fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.max", "pg_conf work", "Maximum task and work workers", &init.conf.max, max_worker_processes, 1, max_worker_processes, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.restart", "pg_conf restart", "Restart conf interval, seconds", &init.conf.restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.count", "pg_task count", "Non-negative maximum count of tasks, are executed by current background worker process before exit", &init.task.count, 0, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.fetch", "pg_task fetch", "Fetch task rows at once", &init.task.fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.id", "pg_task id", "Current task id", &init.task.id, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.limit", "pg_task limit", "Limit task rows at once", &init.task.limit, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.max", "pg_task max", "Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds", &init.task.max, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.run", "pg_task run", "Maximum count of concurrently executing tasks in work", &init.task.run, INT_MAX, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.sleep", "pg_task sleep", "Check tasks every sleep milliseconds", &init.task.sleep, 1000, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.close", "pg_work close", "Close work, milliseconds", &init.work.close, BGW_DEFAULT_RESTART_INTERVAL * 1000, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.fetch", "pg_work fetch", "Fetch work rows at once", &init.work.fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.idle", "pg_work idle", "Idle work count", &init.work.idle, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.restart", "pg_work restart", "Restart work interval, seconds", &init.work.restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.active", "pg_task active", "Positive period after plan time, when task is active for executing", &init.task.active, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.data", "pg_task data", "Database name for tasks table", &init.task.data, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.delimiter", "pg_task delimiter", "Results columns delimiter", &init.task.delimiter, "\t", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.escape", "pg_task escape", "Results columns escape", &init.task.escape, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.group", "pg_task group", "Task grouping by name", &init.task.group, "group", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", "Json configuration, available keys: data, reset, schema, table, sleep and user", &init.task.json, SQL([{"data":"postgres"}]), PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.live", "pg_task live", "Non-negative maximum time of live of current background worker process before exit", &init.task.live, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.null", "pg_task null", "Null text value representation", &init.null, "\\N", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.quote", "pg_task quote", "Results columns quote", &init.task.quote, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.repeat", "pg_task repeat", "Non-negative auto repeat tasks interval", &init.task.repeat, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.reset", "pg_task reset", "Interval of reset tasks", &init.task.reset, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.schema", "pg_task schema", "Schema name for tasks table", &init.task.schema, "public", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", "Table name for tasks table", &init.task.table, "task", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.timeout", "pg_task timeout", "Non-negative allowed time for task run", &init.task.timeout, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.user", "pg_task user", "User name for tasks table", &init.task.user, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(DEBUG1, "json = %s, user = %s, data = %s, schema = %s, table = %s, null = %s, sleep = %i, reset = %s, active = %s", init.task.json, init.task.user, init.task.data, init.task.schema, init.task.table, init.null, init.task.sleep, init.task.reset, init.work.active);
#ifdef GP_VERSION_NUM
    if (!IS_QUERY_DISPATCHER()) return;
#endif
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = init_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = init_shmem_request_hook;
#elif PG_VERSION_NUM >= 90600
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

void append_with_tabs(StringInfo buf, const char *str) {
    char ch;
    while ((ch = *str++) != '\0') {
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

int init_arg(Shared *s) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int slot = 0; slot < init.conf.max; slot++) if (!shared[slot].in_use) {
        pg_write_barrier();
        shared[slot] = *s;
        shared[slot].in_use = true;
        LWLockRelease(BackgroundWorkerLock);
        elog(DEBUG1, "slot = %i", slot);
        return slot;
    }
    LWLockRelease(BackgroundWorkerLock);
    return -1;
}

void init_free(int slot) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    pg_read_barrier();
    MemSet(&shared[slot], 0, sizeof(Shared));
    LWLockRelease(BackgroundWorkerLock);
}

Shared *init_shared(Datum main_arg) {
    return &shared[DatumGetInt32(main_arg)];
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
