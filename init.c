#include "include.h"

PG_MODULE_MAGIC;

static char *pg_task_config;
static char *pg_task_task;
static uint32 pg_task_tick;

void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker) {
    BackgroundWorkerHandle *handle;
    if (!RegisterDynamicBackgroundWorker(worker, &handle)) ereport(ERROR, (errmsg("%s(%s:%d): !RegisterDynamicBackgroundWorker", __func__, __FILE__, __LINE__))); else {
        pid_t pid;
        switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
            case BGWH_STARTED: break;
            case BGWH_STOPPED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_STOPPED", __func__, __FILE__, __LINE__)));
            case BGWH_POSTMASTER_DIED: ereport(ERROR, (errmsg("%s(%s:%d): WaitForBackgroundWorkerStartup == BGWH_POSTMASTER_DIED", __func__, __FILE__, __LINE__)));
            default: ereport(ERROR, (errmsg("%s(%s:%d): Unexpected bgworker handle status", __func__, __FILE__, __LINE__)));
        }
    }
    pfree(handle);
}

void SPI_connect_my(const char *command, const int timeout) {
    int rc;
    pgstat_report_activity(STATE_RUNNING, command);
    if ((rc = SPI_connect_ext(SPI_OPT_NONATOMIC)) != SPI_OK_CONNECT) ereport(ERROR, (errmsg("%s(%s:%d): SPI_connect_ext = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    SPI_start_transaction();
    if (timeout > 0) enable_timeout_after(STATEMENT_TIMEOUT, timeout); else disable_timeout(STATEMENT_TIMEOUT, false);
}

void SPI_finish_my(const char *command) {
    int rc;
    disable_timeout(STATEMENT_TIMEOUT, false);
    if ((rc = SPI_finish()) != SPI_OK_FINISH) ereport(ERROR, (errmsg("%s(%s:%d): SPI_finish = %s", __func__, __FILE__, __LINE__, SPI_result_code_string(rc))));
    ProcessCompletedNotifies();
    pgstat_report_activity(STATE_IDLE, command);
    pgstat_report_stat(true);
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
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_library_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "conf_worker");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_function_name, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_type, buf.data, buf.len);
    resetStringInfo(&buf);
    appendStringInfoString(&buf, "postgres postgres pg_task conf");
    if (buf.len + 1 > BGW_MAXLEN) ereport(ERROR, (errmsg("%s(%s:%d): %u > BGW_MAXLEN", __func__, __FILE__, __LINE__, buf.len + 1)));
    memcpy(worker.bgw_name, buf.data, buf.len);
    pfree(buf.data);
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("%s(%s:%d): !process_shared_preload_libraries_in_progress", __func__, __FILE__, __LINE__)));
    DefineCustomStringVariable("pg_task.config", "pg_task config", NULL, &pg_task_config, "[{\"data\":\"postgres\"}]", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pg_task.task", "pg_task task", NULL, &pg_task_task, "task", PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_task.tick", "pg_task tick", NULL, (int *)&pg_task_tick, 1000, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    elog(LOG, "%s(%s:%d): pg_task_config = %s, pg_task_task = %s, pg_task_tick = %u", __func__, __FILE__, __LINE__, pg_task_config, pg_task_task, pg_task_tick);
    conf_worker();
}
