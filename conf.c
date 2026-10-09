#include "include.h"

#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <tcop/tcopprot.h>
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
    int64 reg;
} Registered;

// tells a registration of pg_work from any other, across restarts of pg_conf too, for its slot to be freed only while still its, see init_free_work()
static int64 conf_reg(void) {
    static uint32 n = 0;
    return ((int64)MyProcPid << 32) | ++n;
}

static void conf_exit(int code, Datum arg) {
    elog(DEBUG1, "code = %i", code);
}

static void conf_reconcile(void) {
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &reg_head) {
        Registered *r = dlist_container(Registered, node, iter.cur);
        pid_t pid;
        // still running: let it notice the same reload via its own work_check() and self-terminate cleanly; only reap it here once it's confirmed stopped, so we never signal a worker that might be mid-SPI-call
        if (GetBackgroundWorkerPid(r->handle, &pid) != BGWH_STOPPED) continue;
        // stopped, and either no longer wanted, taken over by another pg_work of its entry, or about to be spawned anew by conf_work(): drop it either way, so no stale registration is left to be restarted, or to free a slot that a later pg_work has taken
        elog(DEBUG1, "reaping stopped worker, data = %s, user = %s, hash = %i, slot = %i", r->data, r->user, r->hash, r->slot);
        TerminateBackgroundWorker(r->handle); // cancel a pending crash restart
        init_free_work(r->slot, r->reg);
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

// in_use: a pg_work of the entry has a slot already, one started by an earlier pg_conf, say, which the postmaster restarts while it can't connect: make its role and database, for it to connect at last, but start no other one
static void conf_work(Work *w, bool in_use) {
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
    if (in_use) { elog(DEBUG1, "data = %s, user = %s, hash = %i, in use", w->shared->data, w->shared->user, w->shared->hash); conf_free(w); return; }
    if ((len = strlcpy(worker.bgw_function_name, "work_main", sizeof(worker.bgw_function_name))) >= sizeof(worker.bgw_function_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_function_name))));
    if ((len = strlcpy(worker.bgw_library_name, "pg_task", sizeof(worker.bgw_library_name))) >= sizeof(worker.bgw_library_name)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_library_name))));
    if ((len = snprintf(worker.bgw_name, sizeof(worker.bgw_name) - 1, "%s %s pg_work %s %s %li", w->shared->user, w->shared->data, w->shared->schema, w->shared->table, w->shared->sleep)) >= sizeof(worker.bgw_name) - 1) ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("snprintf %li >= %li", len, sizeof(worker.bgw_name) - 1)));
#if PG_VERSION_NUM >= 110000
    if ((len = strlcpy(worker.bgw_type, worker.bgw_name, sizeof(worker.bgw_type))) >= sizeof(worker.bgw_type)) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof(worker.bgw_type))));
#endif
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    w->shared->reg = conf_reg();
    if ((slot = init_arg(w->shared)) == -1) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not find empty slot")));
    worker.bgw_main_arg = Int32GetDatum(slot);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_restart_time = w->restart; // that of the role and database of the entry, rather than of pg_conf's own session
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
            r->reg = w->shared->reg;
            strlcpy(r->data, w->shared->data, sizeof(r->data));
            strlcpy(r->user, w->shared->user, sizeof(r->user));
            dlist_push_tail(&reg_head, &r->node);
            handle = NULL;
            elog(DEBUG1, "started, slot = %i", slot);
            conf_free(w);
            break;
        }
        case BGWH_STOPPED: // gone before it was seen to start, with its exit code 1 the postmaster would restart it after a while still, with the slot freed here, by then maybe someone else's: cancel that first, as conf_reconcile() does
            TerminateBackgroundWorker(handle);
            pfree(handle);
            init_free_work(slot, w->shared->reg);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background worker"), errhint("More details may be available in the server log."))); break;
    }
    if (handle) pfree(handle);
}

static void conf_check(void) {
    bool ok = true;
    bool *in_use;
    int n;
    dlist_mutable_iter iter;
    MemoryContext oldMemoryContext = CurrentMemoryContext;
    Portal portal;
    static SPIPlanPtr plan = NULL;
    static StringInfoData src = {0};
    set_ps_display_my("check");
    dlist_init(&head);
    if (!src.data) {
        initStringInfoMy(&src);
        // an entry is known by the hash of its schema and table, the length of the schema before them, or a dot in a name would make two entries one, schema a.b with table c and schema a with table b.c, the second of which then never started; work_check() of pg_work goes by the same one
        init_settings(&src);
        appendStringInfoString(&src, SQL(
            SELECT    DISTINCT ON ("data", "user", "schema", "table") j.*, pg_catalog.hashtext(pg_catalog.concat_ws('.', pg_catalog.length("schema"), "schema", "table"))::pg_catalog.int4 AS "hash", "pid" IS NULL AS "new" FROM j
            LEFT JOIN "pg_catalog"."pg_locks" AS l ON "locktype" OPERATOR(pg_catalog.=) 'userlock'
            AND "mode" OPERATOR(pg_catalog.=) 'AccessExclusiveLock'
            AND "granted" AND "objsubid" OPERATOR(pg_catalog.=) 3
            AND "database" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_database" WHERE "datname" OPERATOR(pg_catalog.=) "data")
            AND "classid" OPERATOR(pg_catalog.=) (SELECT "oid" FROM "pg_catalog"."pg_roles" WHERE "rolname" OPERATOR(pg_catalog.=) "user")
            AND "objid" OPERATOR(pg_catalog.=) pg_catalog.hashtext(pg_catalog.concat_ws('.', pg_catalog.length("schema"), "schema", "table"))::pg_catalog.oid
            ORDER BY "data", "user", "schema", "table", "i"
        ));
    }
    // a pg_task.json that doesn't parse or fit the types of its keys must not take pg_conf down, to be restarted into the same error over and over: keep the workers as they are until it's fixed
    PG_TRY();
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
                { bool isnull; Datum run = SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "run"), &isnull); w->shared->run = Max(isnull ? init_int(val, tupdesc, "run_setting", "pg_task.run") : DatumGetInt32(run), 1); } // the key of pg_task.json, or else the setting, 1 at least, as the setting has it
                { bool isnull; Datum sleep = SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "sleep"), &isnull); w->shared->sleep = Max(isnull ? init_int(val, tupdesc, "sleep_setting", "pg_task.sleep") : DatumGetInt64(sleep), 1); }
                w->shared->spi = DatumGetBool(SPI_getbinval_my(val, tupdesc, "spi", false, BOOLOID));
                w->shared->limit = init_int(val, tupdesc, "limit", "pg_task.limit");
                w->restart = Max(init_int(val, tupdesc, "restart", "pg_work.restart"), 1); // 1 at least, as the setting has it: one set before pg_task was loaded, not checked then, of 0 or below, had the postmaster restart pg_work at once, over and over, or never
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
    PG_CATCH();
        MemoryContextSwitchTo(oldMemoryContext);
        EmitErrorReport();
        FlushErrorState();
        SPI_abort_my();
        ok = false;
    PG_END_TRY();
    set_ps_display_my("idle");
    if (!ok) {
        ereport(WARNING, (errmsg("pg_task.json not applied, keeping the previous configuration")));
        dlist_foreach_modify(iter, &head) conf_free(dlist_container(Work, node, iter.cur));
        return;
    }
    conf_reconcile();
    n = 0;
    dlist_foreach_modify(iter, &head) n++;
    in_use = palloc0(Max(n, 1) * sizeof(*in_use));
    {
        const char **data, **user;
        int *hash;
        data = palloc0(Max(n, 1) * sizeof(*data));
        user = palloc0(Max(n, 1) * sizeof(*user));
        hash = palloc0(Max(n, 1) * sizeof(*hash));
        n = 0;
        dlist_foreach_modify(iter, &head) {
            Work *w = dlist_container(Work, node, iter.cur);
            data[n] = w->shared->data;
            user[n] = w->shared->user;
            hash[n] = w->shared->hash;
            n++;
        }
        init_work(n, data, user, hash, in_use);
        pfree(data);
        pfree(user);
        pfree(hash);
    }
    n = 0;
    dlist_foreach_modify(iter, &head) {
        Work *w = dlist_container(Work, node, iter.cur);
        bool used = in_use[n++];
        if (!w->spawn) { conf_free(w); continue; }
        // an entry that can't be started, its role not made (a reserved name), its database neither (template1 in use), or no worker to be had, mustn't take pg_conf down, to be restarted into the same error over and over, keeping the entries after it from starting: report it and go on, for it to be tried again on the next reload
        PG_TRY();
            conf_work(w, used);
        PG_CATCH();
            MemoryContextSwitchTo(oldMemoryContext);
            EmitErrorReport();
            FlushErrorState();
            SPI_abort_my();
            // and the state of exec_simple_query() too, which CREATE DATABASE of make_data() runs through, as PostgresMain() resets it after an error, or the next one would take a transaction for started already, and run with none
            xact_started_my(false);
            stmt_timeout_active_my(false);
            conf_free(w);
        PG_END_TRY();
    }
    pfree(in_use);
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
    pqsignal(SIGTERM, die); // terminate at the next CHECK_FOR_INTERRUPTS(), as a backend does, rather than in the default handler of background workers, whose FATAL right there can come in the middle of a commit
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnectionMy("postgres", NULL);
    SetConfigOption("application_name", "pg_conf", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
    // the transaction characteristics of the settings of the database or the role, for the transactions of tasks, not for its own: read only would fail every one of them, repeatable read or serializable some on a row changed meanwhile
    SetConfigOption("default_transaction_isolation", "read committed", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("default_transaction_read_only", "off", PGC_USERSET, PGC_S_SESSION);
    SetConfigOption("default_transaction_deferrable", "off", PGC_USERSET, PGC_S_SESSION);
#if PG_VERSION_NUM >= 170000
    SetConfigOption("transaction_timeout", "0", PGC_USERSET, PGC_S_SESSION); // that of the settings of the database or the role, for the transactions of tasks, ends any longer one with FATAL, taking pg_conf down: none for its own, which run no code of others, unlike a task's input
#endif
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
