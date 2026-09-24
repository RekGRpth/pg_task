#include "include.h"

#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>

#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#else
#include <parser/parse_node.h>
#endif

#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
#include <catalog/pg_type.h>
#include <miscadmin.h>
#endif

#if PG_VERSION_NUM < 150000
#include <utils/guc.h>
#endif

static dlist_head head;
static dlist_head reg_head;

typedef struct Registered {
    BackgroundWorkerHandle *handle;
    char data[NAMEDATALEN];
    char user[NAMEDATALEN];
    dlist_node node;
    int hash;
    int slot;
} Registered;

static void conf_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
}

static void conf_reconcile(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &reg_head) {
        Registered *r = dlist_container(Registered, node, iter.cur);
        dlist_iter want;
        bool running = false;
        pid_t pid;
        dlist_foreach(want, &head) {
            const Work *w = dlist_container(Work, node, want.cur);
            if (!w->spawn && w->shared->hash == r->hash && !strcmp(w->shared->data, r->data) && !strcmp(w->shared->user, r->user)) { running = true; break; }
        }
        if (running) continue; // wanted and holding its lock
        // still running: let it notice the same reload via its own work_check() and self-terminate cleanly; only reap it here once it's confirmed stopped, so we never signal a worker that might be mid-SPI-call
        if (GetBackgroundWorkerPid(r->handle, &pid) != BGWH_STOPPED) continue;
        // stopped, and either no longer wanted or about to be spawned anew by conf_work(): drop it either way, so no stale entry is left to match a later worker with the same data/user/hash
        elog(DEBUG1, "reaping stopped worker, data = %s, user = %s, hash = %i, slot = %i", r->data, r->user, r->hash, r->slot);
        TerminateBackgroundWorker(r->handle); // cancel a pending crash restart
        init_free_work(r->slot, r->data, r->user, r->hash);
        pfree(r->handle);
        dlist_delete(&r->node);
        pfree(r);
    }
}

static void conf_free(Work *w) {
    dlist_delete(&w->node);
    pfree(w->shared);
    pfree(w);
}

static void conf_work(Work *w) {
    BackgroundWorkerHandle *handle;
    BackgroundWorker worker = {0};
    int slot;
    size_t len;
    set_ps_display_my("work");
    w->data = quote_identifier(w->shared->data);
    w->user = quote_identifier(w->shared->user);
    make_user(w);
    make_data(w);
    if (w->data != w->shared->data) pfree((void *)w->data);
    if (w->user != w->shared->user) pfree((void *)w->user);
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    if ((slot = init_arg(w->shared)) == -1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not find empty slot")));
    worker.bgw_main_arg = Int32GetDatum(slot);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = init_work_restart();
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        init_free(slot);
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("could not register background worker"), errhint("Consider increasing configuration parameter \"max_worker_processes\".")));
    }
    switch (WaitForBackgroundWorkerStartup(handle, &w->pid)) {
        case BGWH_NOT_YET_STARTED: init_free(slot); ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("BGWH_NOT_YET_STARTED is never returned!"))); break;
        case BGWH_POSTMASTER_DIED: init_free(slot); ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background worker without postmaster"), errhint("Kill all remaining database processes and restart the database."))); break;
        case BGWH_STARTED: {
            Registered *r = MemoryContextAllocZero(TopMemoryContext, sizeof(Registered));
            r->handle = handle;
            r->hash = w->shared->hash;
            r->slot = slot;
            strlcpy(r->data, w->shared->data, sizeof(r->data));
            strlcpy(r->user, w->shared->user, sizeof(r->user));
            dlist_push_tail(&reg_head, &r->node);
            handle = NULL;
            elog(DEBUG1, "started, slot = %i", slot);
            conf_free(w);
            break;
        }
        case BGWH_STOPPED: init_free(slot); ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void conf_check(void) {
    dlist_mutable_iter iter;
    Portal portal;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("check");
    dlist_init(&head);
    if (!src.data) {
        initStringInfoMy(&src);
        appendStringInfo(&src, SQL(
            WITH j AS (
                WITH s AS (
                    WITH s AS (
                        SELECT "setdatabase", "setrole", ARRAY[pg_catalog.split_part("kv", '=', 1), pg_catalog.substr("kv", pg_catalog.length(pg_catalog.split_part("kv", '=', 1)) OPERATOR(pg_catalog.+) 2)] AS "setconfig" FROM "pg_catalog"."pg_db_role_setting", pg_catalog.unnest("setconfig") AS "kv"
                    ) SELECT "setdatabase", "setrole", pg_catalog.%s(pg_catalog.array_agg("setconfig"[1]), pg_catalog.array_agg("setconfig"[2])) AS "setconfig" FROM s GROUP BY 1, 2
                ) SELECT    COALESCE("data", "user", pg_catalog.current_setting('pg_task.data'))::pg_catalog.text AS "data",
                            (EXTRACT(epoch FROM COALESCE("reset", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.reset')::pg_catalog.interval, pg_catalog.current_setting('pg_task.reset')::pg_catalog.interval))::pg_catalog.int8 OPERATOR(pg_catalog.*) 1000)::pg_catalog.int8 AS "reset",
                            COALESCE("run", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run')::pg_catalog.int4, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.run')::pg_catalog.int4, pg_catalog.current_setting('pg_task.run')::pg_catalog.int4)::pg_catalog.int4 AS "run",
                            COALESCE("schema", u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.schema', pg_catalog.current_setting('pg_task.schema'))::pg_catalog.text AS "schema",
                            COALESCE("table", u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.table', pg_catalog.current_setting('pg_task.table'))::pg_catalog.text AS "table",
                            COALESCE("sleep", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep')::pg_catalog.int8, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.sleep')::pg_catalog.int8, pg_catalog.current_setting('pg_task.sleep')::pg_catalog.int8)::pg_catalog.int8 AS "sleep",
                            COALESCE("spi", (u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.spi')::pg_catalog.bool, pg_catalog.current_setting('pg_task.spi')::pg_catalog.bool)::pg_catalog.bool AS "spi",
                            COALESCE((u."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit')::pg_catalog.int4, (d."setconfig" OPERATOR(pg_catalog.->>) 'pg_task.limit')::pg_catalog.int4, pg_catalog.current_setting('pg_task.limit')::pg_catalog.int4)::pg_catalog.int4 AS "limit",
                            COALESCE("user", "data", pg_catalog.current_setting('pg_task.user'))::pg_catalog.text AS "user"
                FROM        pg_catalog.jsonb_to_recordset(pg_catalog.current_setting('pg_task.json')::pg_catalog.jsonb) AS j ("data" text, "reset" interval, "run" int4, "schema" text, "table" text, "sleep" int8, "spi" bool, "user" text)
                LEFT JOIN   s AS d on d."setdatabase" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) COALESCE("data", "user", pg_catalog.current_setting('pg_task.data')))
                LEFT JOIN   s AS u on u."setrole" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) COALESCE("user", "data", pg_catalog.current_setting('pg_task.user')))
            ) SELECT    DISTINCT j.*, pg_catalog.hashtext(pg_catalog.concat_ws('.', "schema", "table"))::pg_catalog.int4 AS "hash", "pid" IS NULL AS "new" FROM j
            LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock'
            AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock'
            AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3
            AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) "data")
            AND "classid" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) "user")
            AND "objid" OPERATOR(pg_catalog.=) pg_catalog.hashtext(pg_catalog.concat_ws('.', "schema", "table"))::pg_catalog.oid
        ),
#if PG_VERSION_NUM >= 90500
        "jsonb_object"
#else
        "json_object"
#endif
        );
    }
    SPI_connect_my(src.data, InvalidOid);
    if (!plan) plan = SPI_prepare_my(src.data, 0, NULL);
    portal = SPI_cursor_open_my(src.data, plan, NULL, NULL, false);
    do {
        SPI_cursor_fetch_my(src.data, portal, true, init_conf_fetch());
        for (uint64 row = 0; row < SPI_processed; row++) {
            HeapTuple val = SPI_tuptable->vals[row];
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            Work *w = MemoryContextAllocZero(TopMemoryContext, sizeof(Work));
            set_ps_display_my("row");
            w->shared = MemoryContextAllocZero(TopMemoryContext, sizeof(Shared));
            w->shared->hash = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "hash", false, INT4OID));
            w->spawn = DatumGetBool(SPI_getbinval_my(val, tupdesc, "new", false, BOOLOID));
            w->shared->reset = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "reset", false, INT8OID));
            w->shared->run = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "run", false, INT4OID));
            w->shared->sleep = DatumGetInt64(SPI_getbinval_my(val, tupdesc, "sleep", false, INT8OID));
            w->shared->spi = DatumGetBool(SPI_getbinval_my(val, tupdesc, "spi", false, BOOLOID));
            w->shared->limit = DatumGetInt32(SPI_getbinval_my(val, tupdesc, "limit", false, INT4OID));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "data", false, TEXTOID)), w->shared->data, sizeof(w->shared->data));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "schema", false, TEXTOID)), w->shared->schema, sizeof(w->shared->schema));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "table", false, TEXTOID)), w->shared->table, sizeof(w->shared->table));
            text_to_cstring_buffer((text *)DatumGetPointer(SPI_getbinval_my(val, tupdesc, "user", false, TEXTOID)), w->shared->user, sizeof(w->shared->user));
            elog(DEBUG1, "row = %lu, user = %s, data = %s, schema = %s, table = %s, sleep = %li, reset = %li, run = %i, hash = %i, spi = %s, limit = %i, spawn = %s", row, w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep, w->shared->reset, w->shared->run, w->shared->hash, w->shared->spi ? "true" : "false", w->shared->limit, w->spawn ? "true" : "false");
            dlist_push_tail(&head, &w->node);
            SPI_freetuple(val);
        }
    } while (SPI_processed);
    SPI_cursor_close_my(portal);
    SPI_finish_my();
    set_ps_display_my("idle");
    conf_reconcile();
    dlist_foreach_modify(iter, &head) {
        Work *w = dlist_container(Work, node, iter.cur);
        if (w->spawn) conf_work(w); else conf_free(w);
    }
}

static void conf_reload(void) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
    conf_check();
}

static void conf_latch(void) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) conf_reload();
}

void conf_main(Datum main_arg) {
    dlist_init(&reg_head);
    before_shmem_exit(conf_exit, main_arg);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    SetConfigOption("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
    pgstat_report_appname("pg_conf");
    set_ps_display_my("main");
    process_session_preload_libraries();
    if (!lock_data_user(MyDatabaseId, GetUserId())) { ereport(WARNING, (errmsg("!lock_data_user(%i, %i)", MyDatabaseId, GetUserId()))); return; }
    conf_check();
    while (!ShutdownRequestPending) {
        int rc = WaitLatchMy(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
        if (rc & WL_POSTMASTER_DEATH) ShutdownRequestPending = true;
        conf_latch();
    }
    if (!unlock_data_user(MyDatabaseId, GetUserId())) ereport(WARNING, (errmsg("!unlock_data_user(%i, %i)", MyDatabaseId, GetUserId())));
}
