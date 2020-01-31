#include "include.h"

PG_MODULE_MAGIC;

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

static void register_conf_worker(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    if (snprintf(worker.bgw_library_name, sizeof("pg_task"), "pg_task") != sizeof("pg_task") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task") - 1)));
    if (snprintf(worker.bgw_function_name, sizeof("conf_worker"), "conf_worker") != sizeof("conf_worker") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("conf_worker") - 1)));
    if (snprintf(worker.bgw_type, sizeof("pg_task conf"), "pg_task conf") != sizeof("pg_task conf") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("pg_task conf") - 1)));
    if (snprintf(worker.bgw_name, sizeof("postgres postgres pg_task conf"), "postgres postgres pg_task conf") != sizeof("postgres postgres pg_task conf") - 1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s(%s:%d): snprintf != %lu", __func__, __FILE__, __LINE__, sizeof("postgres postgres pg_task conf") - 1)));
    RegisterBackgroundWorker(&worker);
}

void _PG_init(void); void _PG_init(void) {
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(FATAL, (errmsg("%s(%s:%d): pg_task can only be loaded via shared_preload_libraries", __func__, __FILE__, __LINE__), errhint("Add pg_task to the shared_preload_libraries configuration variable in postgresql.conf.")));
    register_conf_worker();
}
