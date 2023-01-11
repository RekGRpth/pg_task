#include "include.h"

PG_MODULE_MAGIC;

char *task_null;
int conf_fetch;
int task_fetch;
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
static char *task_live;
static char *task_quote;
static char *task_repeat;
static char *task_reset;
static char *task_schema;
static char *task_table;
static char *task_timeout;
static char *task_user;
static char *work_active;
static char *work_user;
static int conf_restart;
static int task_count;
static int task_id;
static int task_limit;
static int task_max;
static int task_sleep;
static object_access_hook_type next_object_access_hook = NULL;

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
    dsm_pin_segment(*seg);
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

static void init_conf(void) {
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
    RegisterBackgroundWorker(&worker);
}

void initStringInfoMy(StringInfoData *buf) {
    MemoryContext oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(buf);
    MemoryContextSwitchTo(oldMemoryContext);
}

static void init_reset(const char *data, const char *name) {
    ResourceOwner currentOwner = CurrentResourceOwner;
    AlterDatabaseSetStmt *stmt = makeNode(AlterDatabaseSetStmt);
    stmt->dbname = (char *)data;
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->kind = VAR_RESET;
    stmt->setstmt->name = (char *)name;
    BeginInternalSubTransaction(NULL);
    CurrentResourceOwner = currentOwner;
    (void)AlterDatabaseSet(stmt);
    ReleaseCurrentSubTransaction();
    CurrentResourceOwner = currentOwner;
    pfree(stmt->setstmt);
    pfree(stmt);
}

static A_Const *makeAConst(const char *str) {
    A_Const *v = makeNode(A_Const);
    String *s = makeString((char *)str);
    v->val.sval = *s;
    pfree(s);
    return v;
}

static void init_set(const char *data, const char *name, const char *value) {
    ResourceOwner currentOwner = CurrentResourceOwner;
    AlterDatabaseSetStmt *stmt = makeNode(AlterDatabaseSetStmt);
    stmt->dbname = (char *)data;
    stmt->setstmt = makeNode(VariableSetStmt);
    stmt->setstmt->args = list_make1(makeAConst((char *)value));
    stmt->setstmt->kind = VAR_SET_VALUE;
    stmt->setstmt->name = (char *)name;
    BeginInternalSubTransaction(NULL);
    CurrentResourceOwner = currentOwner;
    (void)AlterDatabaseSet(stmt);
    ReleaseCurrentSubTransaction();
    CurrentResourceOwner = currentOwner;
    list_free_deep(stmt->setstmt->args);
    pfree(stmt->setstmt);
    pfree(stmt);
}

static Oid
get_extension_schema(Oid ext_oid)
{
	Oid			result;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	rel = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	table_close(rel, AccessShareLock);

	return result;
}

static void init_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg) {
    if (next_object_access_hook) next_object_access_hook(access, classId, objectId, subId, arg);
    if (classId != ExtensionRelationId) return;
    CommandCounterIncrement();
    if (get_extension_oid("pg_task", true) != objectId) return;
    switch (access) {
        case OAT_DROP: {
            char *data;
            if (!(data = get_database_name(MyDatabaseId))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("database %u does not exist", MyDatabaseId)));
            init_reset(data, "pg_task.data");
            init_reset(data, "pg_task.reset");
            init_reset(data, "pg_task.schema");
            init_reset(data, "pg_task.sleep");
            init_reset(data, "pg_task.table");
            init_reset(data, "pg_task.user");
            pfree(data);
        } break;
        case OAT_POST_CREATE: {
            char *data;
            char *schema;
            const char *table = GetConfigOption("pg_task.table", true, true);
            const char *reset = GetConfigOption("pg_task.reset", true, true);
            const char *sleep = GetConfigOption("pg_task.sleep", true, true);
            char *user;
            Oid ext_oid;
            size_t len;
            Work *w = palloc0(sizeof(*w));
            if ((ext_oid = get_extension_schema(objectId)) == InvalidOid) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("schema for extension %u does not exist", objectId)));
            if (!(data = get_database_name(MyDatabaseId))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("database %u does not exist", MyDatabaseId)));
            if (!(schema = get_namespace_name(ext_oid))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("schema %u does not exist", ext_oid)));
            if (!(user = GetUserNameFromIdMy(GetUserId()))) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("user %u does not exist", GetUserId())));
            init_set(data, "pg_task.data", data);
            init_set(data, "pg_task.reset", reset);
            init_set(data, "pg_task.schema", schema);
            init_set(data, "pg_task.sleep", sleep);
            init_set(data, "pg_task.table", table);
            init_set(data, "pg_task.user", user);
            w->shared = shm_toc_allocate_my(PG_WORK_MAGIC, &w->seg, sizeof(*w->shared));
            w->shared->reset = strtol(reset, NULL, 10);
            w->shared->sleep = strtol(sleep, NULL, 10);
            if ((len = strlcpy(w->shared->data, data, sizeof(w->shared->data))) >= sizeof(w->shared->data)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(w->shared->data))));
            if ((len = strlcpy(w->shared->schema, schema, sizeof(w->shared->schema))) >= sizeof(w->shared->schema)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(w->shared->schema))));
            if ((len = strlcpy(w->shared->table, table, sizeof(w->shared->table))) >= sizeof(w->shared->table)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(w->shared->table))));
            if ((len = strlcpy(w->shared->user, user, sizeof(w->shared->user))) >= sizeof(w->shared->user)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(w->shared->user))));
            conf_work(w);
            pfree(data);
            pfree(schema);
            pfree(user);
            pfree(w);
        } break;
        default: break;
    }
}

void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("This module can only be loaded via shared_preload_libraries")));
    DefineCustomBoolVariable("pg_task.delete", "pg_task delete", "delete task if output is null", &task_delete, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.drift", "pg_task drift", "compute next repeat time by plan instead current", &task_drift, false, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.header", "pg_task header", "show headers", &task_header, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_task.string", "pg_task string", "quote string only", &task_string, true, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.fetch", "pg_conf fetch", "fetch at once", &conf_fetch, 10, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_conf.restart", "pg_conf restart", "conf restart interval", &conf_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.count", "pg_task count", "do count tasks before exit", &task_count, 0, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.fetch", "pg_task fetch", "fetch tasks at once", &task_fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.id", "pg_task id", "task id", &task_id, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.limit", "pg_task limit", "limit tasks at once", &task_limit, 1000, 0, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.max", "pg_task max", "maximum parallel tasks", &task_max, 0, INT_MIN, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.sleep", "pg_task sleep", "check tasks every sleep milliseconds", &task_sleep, 1000, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.fetch", "pg_work fetch", "fetch at once", &work_fetch, 100, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_work.restart", "pg_work restart", "work restart interval", &work_restart, BGW_DEFAULT_RESTART_INTERVAL, 1, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.active", "pg_task active", "task active after plan time", &task_active, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.data", "pg_task data", "database name for tasks table", &task_data, "postgres", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.delimiter", "pg_task delimiter", "results colums delimiter", &task_delimiter, "\t", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.escape", "pg_task escape", "results colums escape", &task_escape, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.group", "pg_task group", "group tasks name", &task_group, "group", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.live", "pg_task live", "exit until timeout", &task_live, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.null", "pg_task null", "text null representation", &task_null, "\\N", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.quote", "pg_task quote", "results colums quote", &task_quote, "", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.repeat", "pg_task repeat", "repeat task", &task_repeat, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.reset", "pg_task reset", "reset tasks every interval", &task_reset, "1 hour", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.schema", "pg_task schema", "schema name for tasks table", &task_schema, "public", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.table", "pg_task table", "table name for tasks table", &task_table, "task", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.timeout", "pg_task timeout", "task timeout", &task_timeout, "0 sec", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.user", "pg_task user", "user name for tasks table", &task_user, "postgres", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.active", "pg_work active", "task active before now", &work_active, "1 week", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_work.user", "pg_work user", "username", &work_user, "postgres", PGC_USERSET, 0, NULL, NULL, NULL);
    elog(DEBUG1, "user = %s, null = %s, sleep = %i, reset = %s, active = %s", work_user, task_null, task_sleep, task_reset, work_active);
#ifdef GP_VERSION_NUM
    if (!IS_QUERY_DISPATCHER()) return;
#endif
    next_object_access_hook = object_access_hook;
    object_access_hook = init_object_access;
    init_conf();
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

#if PG_VERSION_NUM < 120000
ResourceOwner AuxProcessResourceOwner = NULL;

static void ReleaseAuxProcessResourcesCallback(int code, Datum arg);

/*
 * Establish an AuxProcessResourceOwner for the current process.
 */
void
CreateAuxProcessResourceOwner(void)
{
	Assert(AuxProcessResourceOwner == NULL);
	Assert(CurrentResourceOwner == NULL);
	AuxProcessResourceOwner = ResourceOwnerCreate(NULL, "AuxiliaryProcess");
	CurrentResourceOwner = AuxProcessResourceOwner;

	/*
	 * Register a shmem-exit callback for cleanup of aux-process resource
	 * owner.  (This needs to run after, e.g., ShutdownXLOG.)
	 */
	on_shmem_exit(ReleaseAuxProcessResourcesCallback, 0);

}

/*
 * Convenience routine to release all resources tracked in
 * AuxProcessResourceOwner (but that resowner is not destroyed here).
 * Warn about leaked resources if isCommit is true.
 */
void
ReleaseAuxProcessResources(bool isCommit)
{
	/*
	 * At this writing, the only thing that could actually get released is
	 * buffer pins; but we may as well do the full release protocol.
	 */
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 isCommit, true);
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 isCommit, true);
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 isCommit, true);
}

/*
 * Shmem-exit callback for the same.
 * Warn about leaked resources if process exit code is zero (ie normal).
 */
static void
ReleaseAuxProcessResourcesCallback(int code, Datum arg)
{
	bool		isCommit = (code == 0);

	ReleaseAuxProcessResources(isCommit);
}
#endif
