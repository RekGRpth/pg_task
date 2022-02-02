#include "include.h"

extern char *default_null;
extern int work_default_restart;

static void conf_data(WorkShared *ws) {
    List *names = stringToQualifiedNameList(ws->data.quote);
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", ws->user.str, ws->data.str);
    set_ps_display_my("data");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), ws->data.quote, ws->user.quote);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = (char *)ws->data.str;
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString((char *)ws->user.str), -1));
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

static void conf_user(WorkShared *ws) {
    List *names = stringToQualifiedNameList(ws->user.quote);
    StringInfoData src;
    elog(DEBUG1, "user = %s", ws->user.str);
    set_ps_display_my("user");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE USER %s), ws->user.quote);
    if (ws->partman.str[0]) appendStringInfoString(&src, " SUPERUSER");
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
        const char *quote;
        dsm_segment *seg;
        pid_t pid;
        shm_toc_estimator e;
        shm_toc *toc;
        Size segsize;
        size_t len;
        WorkShared *ws;
        set_ps_display_my("row");
        shm_toc_initialize_estimator(&e);
        shm_toc_estimate_chunk(&e, sizeof(*ws));
        shm_toc_estimate_keys(&e, 1);
        segsize = shm_toc_estimate(&e);
        seg = dsm_create_my(segsize, 0);
        toc = shm_toc_create(PG_WORK_MAGIC, dsm_segment_address(seg), segsize);
        ws = shm_toc_allocate(toc, sizeof(*ws));
        memset(ws, 0, sizeof(*ws));
        ws->reset = DatumGetInt64(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "reset", false));
        ws->timeout = DatumGetInt64(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "timeout", false));
        if ((len = strlcpy(ws->data.str, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "data", false)), sizeof(ws->data.str))) >= sizeof(ws->data.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->data.str))));
        pfree(str);
        if ((len = strlcpy(ws->partman.str, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "partman", false)), sizeof(ws->partman.str))) >= sizeof(ws->partman.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->partman.str))));
        pfree(str);
        if ((len = strlcpy(ws->schema.str, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "schema", false)), sizeof(ws->schema.str))) >= sizeof(ws->schema.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->schema.str))));
        pfree(str);
        if ((len = strlcpy(ws->table.str, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "table", false)), sizeof(ws->table.str))) >= sizeof(ws->table.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->table.str))));
        pfree(str);
        if ((len = strlcpy(ws->user.str, str = TextDatumGetCStringMy(SPI_getbinval_my(SPI_tuptable_my.vals[row], SPI_tuptable_my.tupdesc, "user", false)), sizeof(ws->user.str))) >= sizeof(ws->user.str)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->user.str))));
        pfree(str);
        elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li, partman = %s", row, ws->user.str, ws->data.str, ws->schema.str, ws->table.str, ws->timeout, ws->reset, ws->partman.str[0] ? ws->partman.str : default_null);
        shm_toc_insert(toc, 0, ws);
        if ((len = strlcpy(ws->data.quote, quote = quote_identifier(ws->data.str), sizeof(ws->data.quote))) >= sizeof(ws->data.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->data.quote))));
        if (quote != ws->data.str) pfree((void *)quote);
        if (ws->partman.str[0] && (len = strlcpy(ws->partman.quote, quote = quote_identifier(ws->partman.str), sizeof(ws->partman.quote))) >= sizeof(ws->partman.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->partman.quote))));
        if (ws->partman.str[0] && quote != ws->partman.str) pfree((void *)quote);
        if ((len = strlcpy(ws->schema.quote, quote = quote_identifier(ws->schema.str), sizeof(ws->schema.quote))) >= sizeof(ws->schema.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->schema.quote))));
        if (quote != ws->schema.str) pfree((void *)quote);
        if ((len = strlcpy(ws->table.quote, quote = quote_identifier(ws->table.str), sizeof(ws->table.quote))) >= sizeof(ws->table.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->table.quote))));
        if (quote != ws->table.str) pfree((void *)quote);
        if ((len = strlcpy(ws->user.quote, quote = quote_identifier(ws->user.str), sizeof(ws->user.quote))) >= sizeof(ws->user.quote)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(ws->user.quote))));
        if (quote != ws->user.str) pfree((void *)quote);
        conf_user(ws);
        conf_data(ws);
        if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
        if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
        if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", ws->user.str, ws->data.str, ws->schema.str, ws->table.str, ws->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
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
    SPI_tuptable_free(&SPI_tuptable_my);
    set_ps_display_my("idle");
    pfree(src.data);
    if (!unlock_data_user(MyDatabaseId, GetUserId())) elog(WARNING, "!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId());
}
