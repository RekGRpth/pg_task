#include "include.h"

extern char *default_null;
extern int work_default_restart;
static ResourceOwner TopResourceOwner;

static Oid conf_data(WorkShared *workshared) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", workshared->user.str, workshared->data.str);
    set_ps_display_my("data");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), workshared->data.quote, workshared->user.quote);
    names = stringToQualifiedNameList(workshared->data.quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(oid = get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)workshared->data.str;
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString((char *)workshared->user.str), -1));
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

static Oid conf_user(WorkShared *workshared) {
    List *names;
    Oid oid;
    StringInfoData src;
    elog(DEBUG1, "user = %s", workshared->user.str);
    set_ps_display_my("user");
    initStringInfoMy(TopMemoryContext, &src);
    appendStringInfo(&src, SQL(CREATE USER %s), workshared->user.quote);
    if (workshared->partman.str[0]) appendStringInfoString(&src, " SUPERUSER");
    names = stringToQualifiedNameList(workshared->user.quote);
    SPI_start_transaction_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) SPI_execute_with_args_my(src.data, 0, NULL, NULL, NULL, SPI_OK_UTILITY, false);
    oid = get_role_oid(strVal(linitial(names)), false);
    SPI_commit_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
    return oid;
}

void conf_main(Datum main_arg) {
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_config_option_my("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", "postgres", 0);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    TopResourceOwner = ResourceOwnerCreate(NULL, "pg_task");
    if (!src.data) {
        initStringInfoMy(TopMemoryContext, &src);
        appendStringInfoString(&src, init_check());
        appendStringInfo(&src, SQL(%1$sLEFT JOIN pg_stat_activity AS a ON a.usename = j.user AND a.datname = data AND application_name = concat_ws(' ', 'pg_work', schema, j.table, timeout::text) WHERE pid IS NULL), " ");
    }
    SPI_connect_my(src.data);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    SPI_execute_plan_my(plan, NULL, NULL, SPI_OK_SELECT, true);
    for (uint64 row = 0; row < SPI_processed; row++) {
        BackgroundWorkerHandle *handle;
        BackgroundWorker worker = {0};
        dsm_segment *seg;
        pid_t pid;
        ResourceOwner oldowner = CurrentResourceOwner;
        shm_toc_estimator e;
        shm_toc *toc;
        Size segsize;
        size_t len;
        WorkShared *workshared;
        set_ps_display_my("row");
        CurrentResourceOwner = TopResourceOwner;
        shm_toc_initialize_estimator(&e);
        shm_toc_estimate_chunk(&e, sizeof(*workshared));
        shm_toc_estimate_keys(&e, 1);
        segsize = shm_toc_estimate(&e);
        seg = dsm_create_my(segsize, 0);
        toc = shm_toc_create(PG_WORK_MAGIC, dsm_segment_address(seg), segsize);
        workshared = shm_toc_allocate(toc, sizeof(*workshared));
        memset(workshared, 0, sizeof(*workshared));
        workshared->reset = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "reset", false));
        workshared->timeout = DatumGetInt64(SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "timeout", false));
        if ((len = strlcpy(workshared->data.str, TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "data", false)), sizeof(workshared->data.str))) >= sizeof(workshared->data.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->data.str))));
        if ((len = strlcpy(workshared->partman.str, TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "partman", false)), sizeof(workshared->partman.str))) >= sizeof(workshared->partman.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->partman.str))));
        if ((len = strlcpy(workshared->schema.str, TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "schema", false)), sizeof(workshared->schema.str))) >= sizeof(workshared->schema.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->schema.str))));
        if ((len = strlcpy(workshared->table.str, TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "table", false)), sizeof(workshared->table.str))) >= sizeof(workshared->table.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->table.str))));
        if ((len = strlcpy(workshared->user.str, TextDatumGetCStringMy(TopMemoryContext, SPI_getbinval_my(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, "user", false)), sizeof(workshared->user.str))) >= sizeof(workshared->user.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->user.str))));
        elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li, partman = %s", row, workshared->user.str, workshared->data.str, workshared->schema.str, workshared->table.str, workshared->timeout, workshared->reset, workshared->partman.str[0] ? workshared->partman.str : default_null);
        shm_toc_insert(toc, 0, workshared);
        CurrentResourceOwner = oldowner;
        if ((len = strlcpy(workshared->data.quote, quote_identifier(workshared->data.str), sizeof(workshared->data.quote))) >= sizeof(workshared->data.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->data.quote))));
        if (workshared->partman.str[0] && (len = strlcpy(workshared->partman.quote, quote_identifier(workshared->partman.str), sizeof(workshared->partman.quote))) >= sizeof(workshared->partman.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->partman.quote))));
        if ((len = strlcpy(workshared->schema.quote, quote_identifier(workshared->schema.str), sizeof(workshared->schema.quote))) >= sizeof(workshared->schema.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->schema.quote))));
        if ((len = strlcpy(workshared->table.quote, quote_identifier(workshared->table.str), sizeof(workshared->table.quote))) >= sizeof(workshared->table.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->table.quote))));
        if ((len = strlcpy(workshared->user.quote, quote_identifier(workshared->user.str), sizeof(workshared->user.quote))) >= sizeof(workshared->user.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(workshared->user.quote))));
        workshared->user.oid = conf_user(workshared);
        workshared->data.oid = conf_data(workshared);
        if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
        if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
        if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", workshared->user.str, workshared->data.str, workshared->schema.str, workshared->table.str, workshared->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
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
        pfree(handle);
        dsm_pin_segment(seg);
        dsm_detach(seg);
    }
    SPI_finish_my();
    set_ps_display_my("idle");
}
