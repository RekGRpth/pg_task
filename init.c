#include "include.h"

PG_MODULE_MAGIC;

static char *pg_task_task;
static char *pg_task_config;
static int pg_task_tick;

void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker) {
    BackgroundWorkerHandle *handle;
    if (!RegisterDynamicBackgroundWorker(worker, &handle)) E("!RegisterDynamicBackgroundWorker"); else {
        pid_t pid;
        switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
            case BGWH_STARTED: break;
            case BGWH_STOPPED: E("WaitForBackgroundWorkerStartup == BGWH_STOPPED");
            case BGWH_POSTMASTER_DIED: E("WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED");
            default: E("Unexpected bgworker handle status");
        }
    }
    pfree(handle);
}

static void conf_worker(void) {
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
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) F("!process_shared_preload_libraries_in_progress");
    DefineCustomStringVariable("pg_task.config", "pg_task config", NULL, &pg_task_config, "[{\"data\":\"postgres\"}]", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.task", "pg_task task", NULL, &pg_task_task, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.tick", "pg_task tick", NULL, &pg_task_tick, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    L("pg_task_config = %s, pg_task_task = %s, pg_task_tick = %i", pg_task_config, pg_task_task, pg_task_tick);
    conf_worker();
}
