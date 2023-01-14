#include "include.h"

extern int conf_fetch;
extern int work_restart;

static void conf_work(const Work *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len;
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(w->seg));
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = work_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    pfree(handle);
    dsm_detach(w->seg);
}

void conf_main(Datum arg) {
    Portal portal;
    static const char *src = SQL(
        WITH _ AS (
            WITH _ AS (
                SELECT "oid", "datname", "usesysid", "usename", regexp_split_to_array(UNNEST("setconfig"), '=') AS "setconfig" FROM pg_db_role_setting INNER JOIN pg_database ON "setdatabase" = "oid" INNER JOIN pg_user ON "usesysid" = "datdba"
            ) SELECT "oid", "datname", "usesysid", "usename", jsonb_object(array_agg("setconfig"[1]), array_agg("setconfig"[2])) AS "setconfig" FROM _ GROUP BY 1, 2, 3, 4
        ) SELECT    "datname" AS "data",
                    "usename" AS "user",
                    EXTRACT(epoch FROM ("setconfig"->>'pg_task.reset')::interval)::bigint AS "reset",
                    "setconfig"->>'pg_task.schema' AS "schema",
                    ("setconfig"->>'pg_task.sleep')::bigint AS "sleep",
                    "setconfig"->>'pg_task.table' AS "table"
        FROM _ LEFT JOIN "pg_locks" AS l ON "locktype" = 'userlock' AND "mode" = 'AccessExclusiveLock' AND "granted" AND "objsubid" = 3 AND "database" = "oid" AND "classid" = "usesysid" AND "objid" = hashtext(quote_ident("setconfig"->>'pg_task.schema')||'.'||quote_ident("setconfig"->>'pg_task.table'))::oid
        WHERE "pid" IS NULL
    );
    BackgroundWorkerUnblockSignals();
    CreateAuxProcessResourceOwner();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL, 0);
    SetConfigOption("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!lock_data_user(MyDatabaseId, GetUserId())) { elog(WARNING, "!lock_data_user(%i, %i)", MyDatabaseId, GetUserId()); return; }
    SPI_connect_my(src);
    portal = SPI_cursor_open_with_args_my(src, src, 0, NULL, NULL, NULL);
    do {
        SPI_cursor_fetch(portal, true, conf_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            Work *w = MemoryContextAllocZero(TopMemoryContext, sizeof(*w));;
            set_ps_display_my("row");
            w->shared = shm_toc_allocate_my(PG_WORK_MAGIC, &w->seg, sizeof(*w->shared));
            w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false));
            w->shared->sleep = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "sleep", false));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "data", false)), w->shared->data, sizeof(w->shared->data));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "schema", false)), w->shared->schema, sizeof(w->shared->schema));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "table", false)), w->shared->table, sizeof(w->shared->table));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "user", false)), w->shared->user, sizeof(w->shared->user));
            elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, sleep = %li, reset = %li", row, w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep, w->shared->reset);
            conf_work(w);
            pfree(w);
        }
    } while (SPI_processed);
    SPI_cursor_close(portal);
    SPI_finish_my();
    if (!unlock_data_user(MyDatabaseId, GetUserId())) elog(WARNING, "!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId());
}
