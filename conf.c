#include "include.h"

extern char *default_null;
extern int work_default_restart;

static void conf_data(Work *work) {
    List *names = stringToQualifiedNameList(work->data);
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", work->shared->user, work->shared->data);
    set_ps_display_my("data");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), work->data, work->user);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)work->shared->data;
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString((char *)work->shared->user), -1));
        pstate->p_sourcetext = src.data;
        createdb_my(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_finish_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void conf_user(Work *work) {
    List *names = stringToQualifiedNameList(work->user);
    StringInfoData src;
    elog(DEBUG1, "user = %s", work->shared->user);
    set_ps_display_my("user");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE USER %s), work->user);
#if PG_VERSION_NUM >= 120000
    if (work->shared->partman[0]) appendStringInfoString(&src, " SUPERUSER");
#endif
    SPI_connect_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY);
    SPI_finish_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
}

void conf_main(Datum arg) {
    SPITupleTableMy SPI_tuptable_my;
    StringInfoData src;
    BackgroundWorkerUnblockSignals();
    CreateAuxProcessResourceOwner();
    BackgroundWorkerInitializeConnectionMy("postgres", "postgres", 0);
    set_config_option_my("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!lock_data_user(MyDatabaseId, GetUserId())) { elog(WARNING, "!lock_data_user(%i, %i)", MyDatabaseId, GetUserId()); return; }
    initStringInfoMy(&src);
    appendStringInfoString(&src, init_check());
    appendStringInfo(&src, SQL(%1$s
        LEFT JOIN "pg_locks" AS l ON "locktype" = 'userlock' AND "mode" = 'AccessExclusiveLock' AND "granted" AND "objsubid" = 3
        AND "database" = (SELECT "oid" FROM "pg_database" WHERE "datname" = "data")
        AND "classid" = (SELECT "oid" FROM "pg_authid" WHERE "rolname" = "user")
        AND "objid" = hashtext(quote_ident("schema")||'.'||quote_ident("table"))::oid
        WHERE "pid" IS NULL)
    , " ");
    SPI_connect_my(src.data);
    SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_SELECT);
    SPI_tuptable_copy(&SPI_tuptable_my);
    SPI_finish_my();
    for (uint64 row = 0; row < SPI_tuptable_my.numvals; row++) {
        BackgroundWorkerHandle *handle;
        BackgroundWorker worker = {0};
        char *str;
        dsm_segment *seg;
        pid_t pid;
        shm_toc_estimator e;
        shm_toc *toc;
        Size segsize;
        size_t len;
        Work *work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
        set_ps_display_my("row");
        shm_toc_initialize_estimator(&e);
        shm_toc_estimate_chunk(&e, sizeof(*work->shared));
        shm_toc_estimate_keys(&e, 1);
        segsize = shm_toc_estimate(&e);
        seg = dsm_create_my(segsize, 0);
        toc = shm_toc_create(PG_WORK_MAGIC, dsm_segment_address(seg), segsize);
        work->shared = shm_toc_allocate(toc, sizeof(*work->shared));
        memset(work->shared, 0, sizeof(*work->shared));
        work->shared->reset = DatumGetInt64(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "reset", false));
        work->shared->timeout = DatumGetInt64(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "timeout", false));
        if ((len = strlcpy(work->shared->data, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "data", false)), sizeof(work->shared->data))) >= sizeof(work->shared->data)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(work->shared->data))));
        pfree(str);
#if PG_VERSION_NUM >= 120000
        if ((len = strlcpy(work->shared->partman, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "partman", false)), sizeof(work->shared->partman))) >= sizeof(work->shared->partman)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(work->shared->partman))));
        pfree(str);
#endif
        if ((len = strlcpy(work->shared->schema, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "schema", false)), sizeof(work->shared->schema))) >= sizeof(work->shared->schema)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(work->shared->schema))));
        pfree(str);
        if ((len = strlcpy(work->shared->table, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "table", false)), sizeof(work->shared->table))) >= sizeof(work->shared->table)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(work->shared->table))));
        pfree(str);
        if ((len = strlcpy(work->shared->user, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "user", false)), sizeof(work->shared->user))) >= sizeof(work->shared->user)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(work->shared->user))));
        pfree(str);
        elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li, partman = %s", row, work->shared->user, work->shared->data, work->shared->schema, work->shared->table, work->shared->timeout, work->shared->reset,
#if PG_VERSION_NUM >= 120000
        work->shared->partman[0] ? work->shared->partman : default_null
#else
        default_null
#endif
        );
        shm_toc_insert(toc, 0, work->shared);
        work->data = quote_identifier(work->shared->data);
        work->user = quote_identifier(work->shared->user);
        conf_user(work);
        conf_data(work);
        if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
        if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
        if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", work->shared->user, work->shared->data, work->shared->schema, work->shared->table, work->shared->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
        if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
        worker.bgw_notify_pid = MyProcPid;
        worker.bgw_restart_time = work_default_restart;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        set_ps_display_my("work");
        if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
        switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
            case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
            case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
            case BGWH_STARTED: break;
            case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
        }
//        if (work->data != work->shared->data) pfree((void *)work->data);
//        if (work->user != work->shared->data) pfree((void *)work->user);
        pfree(handle);
        dsm_pin_segment(seg);
        dsm_detach(seg);
    }
    SPI_tuptable_free(&SPI_tuptable_my);
    set_ps_display_my("idle");
    pfree(src.data);
    if (!unlock_data_user(MyDatabaseId, GetUserId())) elog(WARNING, "!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId());
}
