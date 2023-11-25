#include "include.h"

PG_MODULE_MAGIC;

char *task_null;
int conf_close;
int conf_fetch;
int task_fetch;
int task_idle;
int work_close;
int work_fetch;
int work_restart;
static bool task_delete;
static bool task_drift;
static bool task_header;
static bool task_string;
static char *task_active;
static char *task_data;
static char *task_delimiter;
static char *task_escape;
static char *task_group;
static char *task_json;
static char *task_live;
static char *task_quote;
static char *task_repeat;
static char *task_reset;
static char *task_schema;
static char *task_table;
static char *task_timeout;
static char *task_user;
static char *work_active;
static int conf_restart;
static int task_count;
static int task_id;
static int task_limit;
static int task_max;
static int task_run;
static int task_sleep;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
TaskShared *taskshared = NULL;
WorkShared *workshared = NULL;

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

void appendBinaryStringInfoEscapeQuote(StringInfoData *buf, const char *data, int len, bool string, char escape, char quote) {
    if (!string && quote) appendStringInfoChar(buf, quote);
    if (len) {
        if (!string && escape && quote) for (int i = 0; len-- > 0; i++) {
            if (quote == data[i]) appendStringInfoChar(buf, escape);
            appendStringInfoChar(buf, data[i]);
        } else appendBinaryStringInfo(buf, data, len);
    }
    if (!string && quote) appendStringInfoChar(buf, quote);
}

void init_conf(bool dynamic) {
    BackgroundWorker worker = {0};
    size_t len;
    if ((len = strlcpy(worker.bgw_function_name, "conf_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = strlcpy(worker.bgw_name, "postgres pg_conf", sizeof(worker.bgw_name))) >= sizeof(worker.bgw_name)) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_name))));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = conf_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (dynamic) {
        worker.bgw_notify_pid = MyProcPid;
        IsUnderPostmaster = true;
        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
        IsUnderPostmaster = false;
    } else RegisterBackgroundWorker(&worker);
}

static void init_assign_int(const char *var, int newval, void *extra) {
    int oldval;
    if (PostmasterPid != MyProcPid) return;
    if (process_shared_preload_libraries_in_progress) return;
    oldval = atoi(GetConfigOption(var, true, true));
    if (oldval == newval) return;
    elog(DEBUG1, "oldval = %i, newval = %i", oldval, newval);
    init_conf(true);
}

static void init_assign_string(const char *var, const char *newval, void *extra) {
    bool new_isnull;
    bool old_isnull;
    const char *oldval;
    if (PostmasterPid != MyProcPid) return;
    if (process_shared_preload_libraries_in_progress) return;
    oldval = GetConfigOption(var, true, true);
    old_isnull = !oldval || oldval[0] == '\0';
    new_isnull = !newval || newval[0] == '\0';
    if (old_isnull && new_isnull) return;
    if (!old_isnull && !new_isnull && !strcmp(oldval, newval)) return;
    elog(DEBUG1, "oldval = %s, newval = %s", !old_isnull ? oldval : task_null, !new_isnull ? newval : task_null);
    init_conf(true);
}

static void init_assign_data(const char *newval, void *extra) { init_assign_string("pg_task.data", newval, extra); }
static void init_assign_json(const char *newval, void *extra) { init_assign_string("pg_task.json", newval, extra); }
static void init_assign_reset(const char *newval, void *extra) { init_assign_string("pg_task.reset", newval, extra); }
static void init_assign_schema(const char *newval, void *extra) { init_assign_string("pg_task.schema", newval, extra); }
static void init_assign_sleep(int newval, void *extra) { init_assign_int("pg_task.sleep", newval, extra); }
static void init_assign_table(const char *newval, void *extra) { init_assign_string("pg_task.table", newval, extra); }
static void init_assign_user(const char *newval, void *extra) { init_assign_string("pg_task.user", newval, extra); }


#if PG_VERSION_NUM >= 150000
static void init_shmem_request_hook(void) {
    if (prev_shmem_request_hook) prev_shmem_request_hook();
    RequestAddinShmemSpace(sizeof(*taskshared));
    RequestAddinShmemSpace(sizeof(*workshared));
}
#endif

static void init_shmem_startup_hook(void) {
    bool found;
    if (prev_shmem_startup_hook) prev_shmem_startup_hook();
    taskshared = ShmemInitStruct("pg_taskshared", sizeof(*taskshared), &found);
    if (!found) MemSet(taskshared, 0, sizeof(*taskshared));
    workshared = ShmemInitStruct("pg_workshared", sizeof(*workshared), &found);
    if (!found) MemSet(workshared, 0, sizeof(*workshared));
}

void initStringInfoMy(StringInfoData *buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomBoolVariable("pg_task.delete", "pg_task delete", "Auto delete task when both output and error are nulls", &task_delete, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.drift", "pg_task drift", "Compute next repeat time by stop time instead by plan time", &task_drift, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.header", "pg_task header", "Show columns headers in output", &task_header, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.string", "pg_task string", "Quote only strings", &task_string, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.close", "pg_conf close", "Close conf, milliseconds", &conf_close, BGW_DEFAULT_RESTART_INTERVAL * 1000, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.fetch", "pg_conf fetch", "Fetch conf rows at once", &conf_fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.restart", "pg_conf restart", "Restart conf interval, seconds", &conf_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.count", "pg_task count", "Non-negative maximum count of tasks, are executed by current background worker process before exit", &task_count, 0, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.fetch", "pg_task fetch", "Fetch task rows at once", &task_fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.idle", "pg_task idle", "Idle task count", &task_idle, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.id", "pg_task id", "Current task id", &task_id, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.limit", "pg_task limit", "Limit task rows at once", &task_limit, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.max", "pg_task max", "Maximum count of concurrently executing tasks in group, negative value means pause between tasks in milliseconds", &task_max, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.run", "pg_task run", "Maximum count of concurrently executing tasks in work", &task_run, INT_MAX, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.sleep", "pg_task sleep", "Check tasks every sleep milliseconds", &task_sleep, 1000, 1, INT_MAX, PGC_USERSET, 0, NULL, init_assign_sleep, NULL);
    DefineCustomIntVariable("pg_work.close", "pg_work close", "Close work, milliseconds", &work_close, BGW_DEFAULT_RESTART_INTERVAL * 1000, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.fetch", "pg_work fetch", "Fetch work rows at once", &work_fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.restart", "pg_work restart", "Restart work interval, seconds", &work_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.active", "pg_task active", "Positive period after plan time, when task is active for executing", &task_active, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.data", "pg_task data", "Database name for tasks table", &task_data, "postgres", PGC_SIGHUP, 0, NULL, init_assign_data, NULL);
    DefineCustomStringVariable("pg_task.delimiter", "pg_task delimiter", "Results columns delimiter", &task_delimiter, "\t", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.escape", "pg_task escape", "Results columns escape", &task_escape, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.group", "pg_task group", "Task grouping by name", &task_group, "group", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", "Json configuration, available keys: data, reset, schema, table, sleep and user", &task_json, SQL([{"data":"postgres"}]), PGC_SIGHUP, 0, NULL, init_assign_json, NULL);
    DefineCustomStringVariable("pg_task.live", "pg_task live", "Non-negative maximum time of live of current background worker process before exit", &task_live, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.null", "pg_task null", "Null text value representation", &task_null, "\\N", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.quote", "pg_task quote", "Results columns quote", &task_quote, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.repeat", "pg_task repeat", "Non-negative auto repeat tasks interval", &task_repeat, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.reset", "pg_task reset", "Interval of reset tasks", &task_reset, "1 hour", PGC_USERSET, 0, NULL, init_assign_reset, NULL);
    DefineCustomStringVariable("pg_task.schema", "pg_task schema", "Schema name for tasks table", &task_schema, "public", PGC_USERSET, 0, NULL, init_assign_schema, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", "Table name for tasks table", &task_table, "task", PGC_USERSET, 0, NULL, init_assign_table, NULL);
    DefineCustomStringVariable("pg_task.timeout", "pg_task timeout", "Non-negative allowed time for task run", &task_timeout, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.user", "pg_task user", "User name for tasks table", &task_user, "postgres", PGC_SIGHUP, 0, NULL, init_assign_user, NULL);
    elog(DEBUG1, "json = %s, user = %s, data = %s, schema = %s, table = %s, null = %s, sleep = %i, reset = %s, active = %s", task_json, task_user, task_data, task_schema, task_table, task_null, task_sleep, task_reset, work_active);
#ifdef GP_VERSION_NUM
    if (!IS_QUERY_DISPATCHER()) return;
#endif
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = init_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = init_shmem_request_hook;
#elif PG_VERSION_NUM >= 90600
    RequestAddinShmemSpace(sizeof(*taskshared));
    RequestAddinShmemSpace(sizeof(*workshared));
#endif
    init_conf(false);
}

#if PG_VERSION_NUM < 130000
volatile sig_atomic_t ShutdownRequestPending;

void
SignalHandlerForConfigReload(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ConfigReloadPending = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

void
SignalHandlerForShutdownRequest(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ShutdownRequestPending = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
#endif

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
