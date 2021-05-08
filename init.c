#include "include.h"

PG_MODULE_MAGIC;

char *default_null;
static char *default_data;
static char *default_json;
static char *default_live;
static char *default_table;
static char *default_user;
static int default_count;
static int default_reset;
static int default_timeout;

static bool init_check_ascii(char *data) {
    for (char *ch = data; *ch; ch++) {
        if (32 <= *ch && *ch <= 127) continue;
        if (*ch == '\n' || *ch == '\r' || *ch == '\t') continue;
        return true;
    }
    return false;
}

bool init_check_ascii_all(BackgroundWorker *worker) {
    if (init_check_ascii(worker->bgw_type)) return true;
    if (init_check_ascii(worker->bgw_name)) return true;
    return false;
}

bool init_data_user_table_lock(Oid data, Oid user, Oid table) {
    LOCKTAG tag = {data, user, table, 3, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    return LockAcquire(&tag, AccessExclusiveLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

bool init_data_user_table_unlock(Oid data, Oid user, Oid table) {
    LOCKTAG tag = {data, user, table, 3, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    return LockRelease(&tag, AccessExclusiveLock, true);
}

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

bool init_table_id_lock(Oid table, int64 id) {
    LOCKTAG tag = {table, (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    return LockAcquire(&tag, AccessExclusiveLock, true, true) != LOCKACQUIRE_NOT_AVAIL;
}

bool init_table_id_unlock(Oid table, int64 id) {
    LOCKTAG tag = {table, (uint32)(id >> 32), (uint32)id, 4, LOCKTAG_USERLOCK, USER_LOCKMETHOD};
    return LockRelease(&tag, AccessExclusiveLock, true);
}

void init_escape(StringInfoData *buf, const char *data, int len, char escape) {
    for (int i = 0; len-- > 0; i++) {
        if (escape == data[i]) appendStringInfoChar(buf, escape);
        appendStringInfoChar(buf, data[i]);
    }
}

static void init_work(bool dynamic) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    if (snprintf(worker.bgw_function_name, sizeof(worker.bgw_function_name) - 1, "conf") >= sizeof(worker.bgw_function_name) - 1) E("snprintf");
    if (snprintf(worker.bgw_library_name, sizeof(worker.bgw_library_name) - 1, "pg_task") >= sizeof(worker.bgw_library_name) - 1) E("snprintf");
    if (snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "postgres postgres pg_task conf") >= sizeof(worker.bgw_name) - 1) E("snprintf");
    if (snprintf(worker.bgw_type, sizeof(worker.bgw_type) - 1, "pg_task conf") >= sizeof(worker.bgw_type) - 1) E("snprintf");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (init_check_ascii_all(&worker)) E("init_check_ascii_all");
    if (dynamic) {
        IsUnderPostmaster = true;
        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) E("!RegisterDynamicBackgroundWorker");
        IsUnderPostmaster = false;
    } else RegisterBackgroundWorker(&worker);
}

static void init_assign(const char *newval, void *extra) {
    const char *oldval = GetConfigOption("pg_task.json", false, true);
    if (PostmasterPid != MyProcPid) return;
    if (process_shared_preload_libraries_in_progress) return;
    if (!strcmp(oldval, newval)) return;
    D1("oldval = %s, newval = %s", oldval, newval);
    init_work(true);
}

static void init_conf(void) {
    DefineCustomIntVariable("pg_task.default_count", "pg_task default count", NULL, &default_count, 1000, 0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_reset", "pg_task default reset", NULL, &default_reset, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_timeout", "pg_task default timeout", NULL, &default_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_data", "pg_task default data", NULL, &default_data, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_live", "pg_task default live", NULL, &default_live, "1 hour", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_null", "pg_task default null", NULL, &default_null, "\\N", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_table", "pg_task default table", NULL, &default_table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_user", "pg_task default user", NULL, &default_user, "postgres", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", NULL, &default_json, "[{\"data\":\"postgres\"}]", PGC_SIGHUP, 0, NULL, init_assign, NULL);
    D1("json = %s, table = %s, null = %s, reset = %i, timeout = %i, count = %i, live = %s", default_json, default_table, default_null, default_reset, default_timeout, default_count, default_live);
}

void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    init_conf();
    init_work(false);
}
