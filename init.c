#include "include.h"

PG_MODULE_MAGIC;

char *default_null;
int conf_default_fetch;
int task_default_fetch;
int work_default_fetch;
int work_default_restart;
static bool task_default_delete;
static bool task_default_drift;
static bool task_default_header;
static bool task_default_string;
static char *default_json;
static char *task_default_active;
static char *task_default_delimiter;
static char *task_default_escape;
static char *task_default_group;
static char *task_default_live;
static char *task_default_quote;
static char *task_default_repeat;
static char *task_default_timeout;
static char *work_default_active;
static char *work_default_data;
static char *work_default_partman;
static char *work_default_reset;
static char *work_default_schema;
static char *work_default_table;
static char *work_default_user;
static int conf_default_restart;
static int task_default_count;
static int task_default_limit;
static int task_default_max;
static int task_id;
static int work_default_timeout;

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

const char *init_check(void) {
    return SQL(
        WITH j AS (
            SELECT  COALESCE(COALESCE("data", "user"), current_setting('pg_work.default_data')) AS "data",
                    COALESCE("partman", current_setting('pg_work.default_partman')) AS "partman",
                    EXTRACT(epoch FROM COALESCE("reset", current_setting('pg_work.default_reset')::interval))::bigint AS "reset",
                    COALESCE("schema", current_setting('pg_work.default_schema')) AS "schema",
                    COALESCE("table", current_setting('pg_work.default_table')) AS "table",
                    COALESCE("timeout", current_setting('pg_work.default_timeout')::bigint) AS "timeout",
                    COALESCE(COALESCE("user", "data"), current_setting('pg_work.default_user')) AS "user"
            FROM    json_to_recordset(current_setting('pg_task.json')::json) AS j ("data" text, "partman" text, "reset" interval, "schema" text, "table" text, "timeout" bigint, "user" text)
        ) SELECT    DISTINCT j.* FROM j
    );
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

void *shm_toc_allocate_my(uint64 magic, dsm_segment **seg, Size nbytes) {
    shm_toc_estimator e;
    shm_toc *toc;
    Size segsize;
    void *ptr;
    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, nbytes);
    shm_toc_estimate_keys(&e, 1);
    segsize = shm_toc_estimate(&e);
    *seg = dsm_create_my(segsize, 0);
    dsm_pin_mapping(*seg);
    toc = shm_toc_create(magic, dsm_segment_address(*seg), segsize);
    ptr = shm_toc_allocate(toc, nbytes);
    MemSet(ptr, 0, nbytes);
    shm_toc_insert(toc, 0, ptr);
    return ptr;
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

void init_work(bool dynamic) {
    BackgroundWorker worker = {0};
    size_t len;
    if ((len = strlcpy(worker.bgw_function_name, "conf_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = strlcpy(worker.bgw_name, "postgres postgres pg_conf", sizeof(worker.bgw_name))) >= sizeof(worker.bgw_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_name))));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = conf_default_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (dynamic) {
        worker.bgw_notify_pid = MyProcPid;
        IsUnderPostmaster = true;
        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
        IsUnderPostmaster = false;
    } else RegisterBackgroundWorker(&worker);
}

static void init_assign(const char *newval, void *extra) {
    bool new_isnull;
    bool old_isnull;
    const char *oldval;
    if (PostmasterPid != MyProcPid) return;
    if (process_shared_preload_libraries_in_progress) return;
    oldval = GetConfigOption("pg_task.json", true, true);
    old_isnull = !oldval || oldval[0] == '\0';
    new_isnull = !newval || newval[0] == '\0';
    if (old_isnull && new_isnull) return;
    if (!old_isnull && !new_isnull && !strcmp(oldval, newval)) return;
    elog(DEBUG1, "oldval = %s, newval = %s", !old_isnull ? oldval : default_null, !new_isnull ? newval : default_null);
    init_work(true);
}

#if PG_VERSION_NUM >= 130000
#elif PG_VERSION_NUM >= 120000
static bool
is_extension_control_filename(const char *filename)
{
	const char *extension = strrchr(filename, '.');

	return (extension != NULL) && (strcmp(extension, ".control") == 0);
}

static char *
get_extension_control_directory(void)
{
	char		sharepath[MAXPGPATH];
	char	   *result;

	get_share_path(my_exec_path, sharepath);
	result = (char *) palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/extension", sharepath);

	return result;
}

static bool
extension_file_exists(const char *extensionName)
{
	bool		result = false;
	char	   *location;
	DIR		   *dir;
	struct dirent *de;

	location = get_extension_control_directory();
	dir = AllocateDir(location);

	/*
	 * If the control directory doesn't exist, we want to silently return
	 * false.  Any other error will be reported by ReadDir.
	 */
	if (dir == NULL && errno == ENOENT)
	{
		/* do nothing */
	}
	else
	{
		while ((de = ReadDir(dir, location)) != NULL)
		{
			char	   *extname;

			if (!is_extension_control_filename(de->d_name))
				continue;

			/* extract extension name from 'name.control' filename */
			extname = pstrdup(de->d_name);
			*strrchr(extname, '.') = '\0';

			/* ignore it if it's an auxiliary control file */
			if (strstr(extname, "--"))
				continue;

			/* done if it matches request */
			if (strcmp(extname, extensionName) == 0)
			{
				result = true;
				break;
			}
		}

		FreeDir(dir);
	}

	return result;
}
#endif

void initStringInfoMy(StringInfoData *buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomBoolVariable("pg_task.default_delete", "pg_task default delete", "delete task if output is null", &task_default_delete, true, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.default_drift", "pg_task default drift", "compute next repeat time by plan instead current", &task_default_drift, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.default_header", "pg_task default header", "show headers", &task_default_header, true, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.default_string", "pg_task default string", "quote string only", &task_default_string, true, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.default_fetch", "pg_conf default fetch", "fetch at once", &conf_default_fetch, 10, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.default_restart", "pg_conf default restart", "conf default restart interval", &conf_default_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_count", "pg_task default count", "do count tasks before exit", &task_default_count, 0, 0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_fetch", "pg_task default fetch", "fetch tasks at once", &task_default_fetch, 100, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_limit", "pg_task default limit", "limit tasks at once", &task_default_limit, 1000, 0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_max", "pg_task default max", "maximum parallel tasks", &task_default_max, 0, INT_MIN, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.id", "pg_task id", "task id", &task_id, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.default_fetch", "pg_work default fetch", "fetch at once", &work_default_fetch, 100, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.default_restart", "pg_work default restart", "work default restart interval", &work_default_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.default_timeout", "pg_work default timeout", "check tasks every timeout milliseconds", &work_default_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_active", "pg_task default active", "task active after plan time", &task_default_active, "1 hour", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_delimiter", "pg_task default delimiter", "results colums delimiter", &task_default_delimiter, "\t", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_escape", "pg_task default escape", "results colums escape", &task_default_escape, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_group", "pg_task default group", "group tasks name", &task_default_group, "group", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_live", "pg_task default live", "exit until timeout", &task_default_live, "0 sec", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_null", "pg_task default null", "text null representation", &default_null, "\\N", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_quote", "pg_task default quote", "results colums quote", &task_default_quote, "", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_repeat", "pg_task default repeat", "repeat task", &task_default_repeat, "0 sec", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_timeout", "pg_task default timeout", "task timeout", &task_default_timeout, "0 sec", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", "json configuration: available keys are: user, data, schema, table, timeout, count, live and partman", &default_json, SQL([{"data":"postgres"}]), PGC_SIGHUP, 0, NULL, init_assign, NULL);
    DefineCustomStringVariable("pg_work.default_active", "pg_work default active", "task active before now", &work_default_active, "1 week", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_data", "pg_work default data", "default database name", &work_default_data, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_partman", "pg_work default partman", "partman schema name, if empty then do not use partman", &work_default_partman,
#if PG_VERSION_NUM >= 120000
        extension_file_exists("pg_partman") ? "partman" : "",
#else
        "",
#endif
        PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_reset", "pg_work default reset", "reset tasks every interval", &work_default_reset, "1 hour", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_schema", "pg_work default schema", "schema name for tasks table", &work_default_schema, "public", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_table", "pg_work default table", "table name for tasks table", &work_default_table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.default_user", "pg_work default user", "default username", &work_default_user, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(DEBUG1, "json = %s, user = %s, data = %s, schema = %s, table = %s, null = %s, timeout = %i, reset = %s, active = %s, partman = %s", default_json, work_default_user, work_default_data, work_default_schema, work_default_table, default_null, work_default_timeout, work_default_reset, work_default_active, work_default_partman && work_default_partman[0] ? work_default_partman : default_null);
    init_work(false);
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
