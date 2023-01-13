#include "include.h"

extern char *task_null;
extern int conf_fetch;
extern int work_restart;
static dlist_head head;

static void conf_data(Work *w) {
    List *names = stringToQualifiedNameList(w->data);
    StringInfoData src;
    elog(DEBUG1, "user = %s, data = %s", w->shared->user, w->shared->data);
    set_ps_display_my("data");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE DATABASE %s WITH OWNER = %s), w->data, w->user);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_database_oid(strVal(linitial(names)), true))) {
        CreatedbStmt *stmt = makeNode(CreatedbStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->dbname = w->shared->data;
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString(w->shared->user), -1));
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

static void conf_user(Work *w) {
    List *names = stringToQualifiedNameList(w->user);
    StringInfoData src;
    elog(DEBUG1, "user = %s", w->shared->user);
    set_ps_display_my("user");
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(CREATE ROLE %s WITH LOGIN), w->user);
    SPI_connect_my(src.data);
    if (!OidIsValid(get_role_oid(strVal(linitial(names)), true))) {
        CreateRoleStmt *stmt = makeNode(CreateRoleStmt);
        ParseState *pstate = make_parsestate(NULL);
        stmt->role = w->shared->user;
        stmt->options = list_make1(makeDefElemMy("canlogin", (Node *)makeInteger(1), -1));
        pstate->p_sourcetext = src.data;
        CreateRoleMy(pstate, stmt);
        list_free_deep(stmt->options);
        free_parsestate(pstate);
        pfree(stmt);
    }
    SPI_finish_my();
    list_free_deep(names);
    pfree(src.data);
    set_ps_display_my("idle");
}

static void conf_work(Work *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    pid_t pid;
    size_t len;
    set_ps_display_my("work");
    dlist_delete(&w->node);
    w->data = quote_identifier(w->shared->data);
    w->user = quote_identifier(w->shared->user);
    conf_user(w);
    conf_data(w);
    if (w->data != w->shared->data) pfree((void *)w->data);
    if (w->user != w->shared->user) pfree((void *)w->user);
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->timeout)) >= sizeof(worker.bgw_name) - 1) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
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
    pfree(w);
}

void conf_main(Datum arg) {
    dlist_mutable_iter iter;
    Portal portal;
    StringInfoData src;
    BackgroundWorkerUnblockSignals();
    CreateAuxProcessResourceOwner();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL, 0);
    set_config_option_my("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR, false);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!lock_data_user(MyDatabaseId, GetUserId())) { elog(WARNING, "!lock_data_user(%i, %i)", MyDatabaseId, GetUserId()); return; }
    dlist_init(&head);
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
    portal = SPI_cursor_open_with_args_my(src.data, src.data, 0, NULL, NULL, NULL);
    do {
        SPI_cursor_fetch(portal, true, conf_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            Work *w = MemoryContextAllocZero(TopMemoryContext, sizeof(*w));;
            set_ps_display_my("row");
            w->shared = shm_toc_allocate_my(PG_WORK_MAGIC, &w->seg, sizeof(*w->shared));
            w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false));
            w->shared->timeout = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "timeout", false));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "data", false)), w->shared->data, sizeof(w->shared->data));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "schema", false)), w->shared->schema, sizeof(w->shared->schema));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "table", false)), w->shared->table, sizeof(w->shared->table));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "user", false)), w->shared->user, sizeof(w->shared->user));
            elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, timeout = %li, reset = %li", row, w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->timeout, w->shared->reset);
            dlist_push_head(&head, &w->node);
        }
    } while (SPI_processed);
    SPI_cursor_close(portal);
    SPI_finish_my();
    set_ps_display_my("idle");
    pfree(src.data);
    dlist_foreach_modify(iter, &head) conf_work(dlist_container(Work, node, iter.cur));
    if (!unlock_data_user(MyDatabaseId, GetUserId())) elog(WARNING, "!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId());
}
