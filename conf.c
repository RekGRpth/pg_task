#include "include.h"

#include <commands/dbcommands.h>
#include <commands/user.h>
#if PG_VERSION_NUM < 130000
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif
#include <nodes/makefuncs.h>
#if PG_VERSION_NUM < 100000
#include <parser/parse_node.h>
#endif
#include <pgstat.h>
#include <postmaster/bgworker.h>
#if PG_VERSION_NUM < 90500
#include <storage/barrier.h>
#endif
#include <storage/ipc.h>
#include <storage/proc.h>
#include <utils/acl.h>
#if PG_VERSION_NUM < 150000
#include <utils/guc.h>
#endif
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#endif

extern char *task_null;
extern int conf_close;
extern int conf_fetch;
extern int work_restart;
extern Shared *shared;
static volatile dlist_head head;

static void conf_data(const Work *w) {
    List *names = stringToQualifiedNameListMy(w->data);
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
        stmt->options = list_make1(makeDefElemMy("owner", (Node *)makeString(w->shared->user)));
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

static void conf_free(Work *w) {
    dlist_delete(&w->node);
    pfree(w->shared);
    pfree(w);
}

static void conf_shmem_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
}

static void conf_user(const Work *w) {
    List *names = stringToQualifiedNameListMy(w->user);
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
        stmt->options = list_make1(makeDefElemMy("canlogin", (Node *)makeInteger(1)));
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

static int conf_bgw_main_arg(Shared *ws) {
    LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
    for (int slot = 0; slot < max_worker_processes; slot++) if (!shared[slot].in_use) {
        pg_write_barrier();
        shared[slot] = *ws;
        shared[slot].in_use = true;
        LWLockRelease(BackgroundWorkerLock);
        elog(DEBUG1, "slot = %i", slot);
        return slot;
    }
    LWLockRelease(BackgroundWorkerLock);
    return -1;
}

static void conf_work(Work *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    size_t len;
    set_ps_display_my("work");
    w->data = quote_identifier(w->shared->data);
    w->user = quote_identifier(w->shared->user);
    conf_user(w);
    conf_data(w);
    if (w->data != w->shared->data) pfree((void *)w->data);
    if (w->user != w->shared->user) pfree((void *)w->user);
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    if ((worker.bgw_main_arg = Int32GetDatum(conf_bgw_main_arg(w->shared))) == Int32GetDatum(-1)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not find empty slot")));
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = work_restart;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        shared_free(worker.bgw_main_arg);
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    }
    switch (WaitForBackgroundWorkerStartup(handle, &w->pid)) {
        case BGWH_NOT_YET_STARTED: shared_free(worker.bgw_main_arg); ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: shared_free(worker.bgw_main_arg); ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: elog(DEBUG1, "started"); conf_free(w); break;
        case BGWH_STOPPED: shared_free(worker.bgw_main_arg); ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

void conf_main(Datum main_arg) {
    dlist_mutable_iter iter;
    Portal portal;
    StringInfoData src;
    before_shmem_exit(conf_shmem_exit, main_arg);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    set_config_option_my("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!lock_data_user(MyDatabaseId, GetUserId())) { elog(WARNING, "!lock_data_user(%i, %i)", MyDatabaseId, GetUserId()); return; }
    dlist_init((dlist_head *)&head);
    initStringInfoMy(&src);
    appendStringInfo(&src, SQL(
        WITH j AS (
            WITH s AS (
                WITH s AS (
                    SELECT "setdatabase", "setrole", pg_catalog.regexp_split_to_array(pg_catalog.unnest("setconfig"), '=') AS "setconfig" FROM "pg_catalog"."pg_db_role_setting"
                ) SELECT "setdatabase", "setrole", pg_catalog.%1$s(pg_catalog.array_agg("setconfig"[1]), pg_catalog.array_agg("setconfig"[2])) AS "setconfig" FROM s GROUP BY 1, 2
            ) SELECT    COALESCE("data", "user", pg_catalog.current_setting('pg_task.data'))::pg_catalog.text AS "data",
                        (EXTRACT(epoch FROM COALESCE("reset", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, pg_catalog.current_setting('pg_task.reset')::pg_catalog.interval))::pg_catalog.int8 OPERATOR(pg_catalog.*) 1000)::pg_catalog.int8 AS "reset",
                        COALESCE("run", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run')::pg_catalog.int4, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run')::pg_catalog.int4, pg_catalog.current_setting('pg_task.run')::pg_catalog.int4)::pg_catalog.int4 AS "run",
                        COALESCE("schema", u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', pg_catalog.current_setting('pg_task.schema'))::pg_catalog.text AS "schema",
                        COALESCE("table", u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', pg_catalog.current_setting('pg_task.table'))::pg_catalog.text AS "table",
                        COALESCE("sleep", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep')::pg_catalog.int8, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep')::pg_catalog.int8, pg_catalog.current_setting('pg_task.sleep')::pg_catalog.int8)::pg_catalog.int8 AS "sleep",
                        COALESCE("user", "data", pg_catalog.current_setting('pg_task.user'))::pg_catalog.text AS "user"
            FROM        pg_catalog.jsonb_to_recordset(pg_catalog.current_setting('pg_task.json')::pg_catalog.jsonb) AS j ("data" text, "reset" interval, "run" int4, "schema" text, "table" text, "sleep" int8, "user" text)
            LEFT JOIN   s AS d on d."setdatabase" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) COALESCE("data", "user", pg_catalog.current_setting('pg_task.data')))
            LEFT JOIN   s AS u on u."setrole" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) COALESCE("user", "data", pg_catalog.current_setting('pg_task.user')))
        ) SELECT    DISTINCT j.*, pg_catalog.hashtext(pg_catalog.concat_ws('.', "schema", "table"))::pg_catalog.int4 AS "hash" FROM j
        LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock'
        AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock'
        AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3
        AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) "data")
        AND "classid" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) "user")
        AND "objid" OPERATOR(pg_catalog.=) pg_catalog.hashtext(pg_catalog.concat_ws('.', "schema", "table"))::pg_catalog.oid
        WHERE "pid" IS NULL
    ),
#if PG_VERSION_NUM >= 90500
        "jsonb_object"
#else
        "json_object"
#endif
    );
    SPI_connect_my(src.data);
    portal = SPI_cursor_open_with_args_my(src.data, 0, NULL, NULL, NULL, true);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, conf_fetch);
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            Work *w = MemoryContextAllocZero(TopMemoryContext, sizeof(Work));
            set_ps_display_my("row");
            w->shared = MemoryContextAllocZero(TopMemoryContext, sizeof(Shared));
            w->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false, INT4OID));
            w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false, INT8OID));
            w->shared->run = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "run", false, INT4OID));
            w->shared->sleep = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "sleep", false, INT8OID));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "data", false, TEXTOID)), w->shared->data, sizeof(w->shared->data));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "schema", false, TEXTOID)), w->shared->schema, sizeof(w->shared->schema));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "table", false, TEXTOID)), w->shared->table, sizeof(w->shared->table));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "user", false, TEXTOID)), w->shared->user, sizeof(w->shared->user));
            elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, sleep = %li, reset = %li, run = %i, hash = %i", row, w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep, w->shared->reset, w->shared->run, w->shared->hash);
            dlist_push_tail((dlist_head *)&head, &w->node);
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    pfree(src.data);
    set_ps_display_my("idle");
    dlist_foreach_modify(iter, (dlist_head *)&head) conf_work(dlist_container(Work, node, iter.cur));
    if (!unlock_data_user(MyDatabaseId, GetUserId())) elog(WARNING, "!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId());
}
