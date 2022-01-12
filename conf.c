#include "include.h"

extern char *default_null;

static Oid conf_data(Work *work) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", work->user.str, work->data.str);
    set_ps_display_my("data");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), work->data.quote, work->user.quote);
    names = stringToQualifiedNameList(work->data.quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)work->data.str;
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString((char *)work->user.str), -1));
        pstate->p_sourcetext = src.data;
        oid = createdb_my(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_commit_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

static Oid conf_user(Work *work) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "user = %s", work->user.str);
    set_ps_display_my("user");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE USER %s), work->user.quote);
    if (work->partman.str) appendStringInfoString(&src, " SUPERUSER");
    names = stringToQualifiedNameList(work->user.quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    oid = get_role_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

static void conf_work(Work *work) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    dsm_segment *seg;
    pid_t pid;
    shm_toc_estimator e;
    shm_toc *toc;
    Size segsize;
    size_t len = 0;
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", work->user.str, work->data.str, work->schema.str, work->table.str, work->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
#if PG_VERSION_NUM < 100000
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_task");
#endif
    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, sizeof(work->data.oid));
    shm_toc_estimate_chunk(&e, sizeof(work->user.oid));
    shm_toc_estimate_chunk(&e, sizeof(work->reset));
    shm_toc_estimate_chunk(&e, sizeof(work->timeout));
    shm_toc_estimate_chunk(&e, strlen(work->data.str) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->partman.str ? work->partman.str : "") + 1);
    shm_toc_estimate_chunk(&e, strlen(work->schema.str) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->table.str) + 1);
    shm_toc_estimate_chunk(&e, strlen(work->user.str) + 1);
    shm_toc_estimate_keys(&e, PG_WORK_NKEYS);
    segsize = shm_toc_estimate(&e);
    seg = dsm_create_my(segsize, 0);
    toc = shm_toc_create(PG_WORK_MAGIC, dsm_segment_address(seg), segsize);
    { typeof(work->data.oid) *oid_data = shm_toc_allocate(toc, sizeof(work->data.oid)); *oid_data = work->data.oid; shm_toc_insert(toc, PG_WORK_KEY_OID_DATA, oid_data); }
    { typeof(work->user.oid) *oid_user = shm_toc_allocate(toc, sizeof(work->user.oid)); *oid_user = work->user.oid; shm_toc_insert(toc, PG_WORK_KEY_OID_USER, oid_user); }
    { typeof(work->reset) *reset = shm_toc_allocate(toc, sizeof(work->reset)); *reset = work->reset; shm_toc_insert(toc, PG_WORK_KEY_RESET, reset); }
    { typeof(work->data.str) str_data = shm_toc_allocate(toc, strlen(work->data.str) + 1); strcpy(str_data, work->data.str); shm_toc_insert(toc, PG_WORK_KEY_STR_DATA, str_data); }
    { typeof(work->partman.str) str_partman = shm_toc_allocate(toc, strlen(work->partman.str ? work->partman.str : "") + 1); strcpy(str_partman, work->partman.str ? work->partman.str : ""); shm_toc_insert(toc, PG_WORK_KEY_STR_PARTMAN, str_partman); }
    { typeof(work->schema.str) str_schema = shm_toc_allocate(toc, strlen(work->schema.str) + 1); strcpy(str_schema, work->schema.str); shm_toc_insert(toc, PG_WORK_KEY_STR_SCHEMA, str_schema); }
    { typeof(work->table.str) str_table = shm_toc_allocate(toc, strlen(work->table.str) + 1); strcpy(str_table, work->table.str); shm_toc_insert(toc, PG_WORK_KEY_STR_TABLE, str_table); }
    { typeof(work->user.str) str_user = shm_toc_allocate(toc, strlen(work->user.str) + 1); strcpy(str_user, work->user.str); shm_toc_insert(toc, PG_WORK_KEY_STR_USER, str_user); }
    { typeof(work->timeout) *timeout = shm_toc_allocate(toc, sizeof(work->timeout)); *timeout = work->timeout; shm_toc_insert(toc, PG_WORK_KEY_TIMEOUT, timeout); }
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    set_ps_display_my("work");
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_NOT_YET_STARTED: ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: break;
        case BGWH_STOPPED: ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    pfree(handle);
    dsm_pin_segment(seg);
//    dsm_detach(seg);
}

static void conf_check(void) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("check");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfo(&src, init_check(), "");
        appendStringInfo(&src, SQL(%1$sLEFT JOIN pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text) WHERE pid IS NULL), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        Work *work = MemoryContextAllocZero(TopMemoryContext, sizeof(*work));
        work->reset =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "reset", false));
        work->data.str = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false));
        work->partman.str = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "partman", true));
        work->schema.str = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", false));
        work->table.str = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false));
        work->user.str = TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false));
        work->timeout =  DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li, partman = %s", row, work->user.str, work->data.str, work->schema.str, work->table.str, work->timeout, work->reset, work->partman.str ? work->partman.str : default_null);
        set_ps_display_my("row");
        work->data.quote = (char *)quote_identifier(work->data.str);
        if (work->partman.str) work->partman.quote = (char *)quote_identifier(work->partman.str);
        work->schema.quote = (char *)quote_identifier(work->schema.str);
        work->table.quote = (char *)quote_identifier(work->table.str);
        work->user.quote = (char *)quote_identifier(work->user.str);
        work->user.oid = conf_user(work);
        work->data.oid = conf_data(work);
        conf_work(work);
        if (work->data.quote != work->data.str) pfree(work->data.quote);
        if (work->partman.str && work->partman.quote != work->partman.str) pfree(work->partman.quote);
        if (work->schema.quote != work->schema.str) pfree(work->schema.quote);
        if (work->table.quote != work->table.str) pfree(work->table.quote);
        if (work->user.quote != work->user.str) pfree(work->user.quote);
        pfree(work->data.str);
        if (work->partman.str) pfree(work->partman.str);
        pfree(work->schema.str);
        pfree(work->table.str);
        pfree(work->user.str);
        pfree(work);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
}

void conf_main(Datum main_arg) {
    set_config_option_my("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", "postgres", 0);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    conf_check();
}
