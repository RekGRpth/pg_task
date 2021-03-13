#include "include.h"

PG_MODULE_MAGIC;

char *default_null;
static char *default_json;
static char *default_table;
static int default_reset;
static int default_timeout;

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

void init_escape(StringInfoData *buf, const char *data, int len, char escape) {
    for (int i = 0; len-- > 0; i++) {
        if (escape == data[i]) appendStringInfoChar(buf, escape);
        appendStringInfoChar(buf, data[i]);
    }
}

static void conf_work(void) {
    StringInfoData buf;
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "conf_worker");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "postgres postgres pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) E("%i > BGW_MAXLEN", buf.len + 1);
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) { W("IsBinaryUpgrade"); return; }
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    DefineCustomIntVariable("pg_task.default_reset", "pg_task default reset", NULL, &default_reset, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.default_timeout", "pg_task default timeout", NULL, &default_timeout, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.json", "pg_task json", NULL, &default_json, "[{\"data\":\"postgres\"}]", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_null", "pg_task default null", NULL, &default_null, "\\N", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.default_table", "pg_task default table", NULL, &default_table, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    D1("json = %s, table = %s, null = %s, reset = %i, timeout = %i", default_json, default_table, default_null, default_reset, default_timeout);
    conf_work();
}
