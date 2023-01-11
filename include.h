#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define countof(array) (sizeof(array)/sizeof(array[0]))
#define SQL(...) #__VA_ARGS__

#include <postgres.h>

#include <access/htup_details.h>
#include <access/printtup.h>
#include <access/hash.h>
#if PG_VERSION_NUM >= 120000
#include <access/relation.h>
#endif
#include <access/table.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/namespace.h>
#include <catalog/objectaccess.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_extension.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <commands/dbcommands.h>
#include <commands/extension.h>
#include <commands/prepare.h>
#include <commands/user.h>
#include <executor/spi.h>
#if PG_VERSION_NUM >= 110000
#include <jit/jit.h>
#endif
#if PG_VERSION_NUM < 90600
#include "latch.h"
#endif
#include <libpq-fe.h>
#include <libpq/libpq-be.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/analyze.h>
#include <parser/parse_type.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
extern PGDLLIMPORT volatile sig_atomic_t ShutdownRequestPending;
extern void SignalHandlerForConfigReload(SIGNAL_ARGS);
extern void SignalHandlerForShutdownRequest(SIGNAL_ARGS);
#endif
#include <replication/slot.h>
#include <storage/dsm.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/procarray.h>
#include <storage/proc.h>
#include <storage/shm_toc.h>
#include <tcop/pquery.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#endif
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>
#include <utils/timestamp.h>

#include "dest.h"

#ifdef GP_VERSION_NUM
#include "cdb/cdbvars.h"
#endif

#if PG_VERSION_NUM >= 90500
#define dsm_create_my(size, flags) dsm_create(size, flags)
#define GetUserNameFromIdMy(roleid) GetUserNameFromId(roleid, false)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel, is_reload) set_config_option(name, value, context, source, action, changeVal, elevel, is_reload)
#else
#define dsm_create_my(size, flags) dsm_create(size)
#define GetUserNameFromIdMy(roleid) GetUserNameFromId(roleid)
#define MyLatch (&MyProc->procLatch)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel, is_reload) set_config_option(name, value, context, source, action, changeVal, elevel)
#endif

#if PG_VERSION_NUM >= 100000
#define createdb_my(pstate, stmt) createdb(pstate, stmt)
#define CreateRoleMy(pstate, stmt) CreateRole(pstate, stmt)
#define makeDefElemMy(name, arg, location) makeDefElem(name, arg, location)
#define shm_toc_lookup_my(toc, key, noError) shm_toc_lookup(toc, key, noError)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents, wait_event_info) WaitEventSetWait(set, timeout, occurred_events, nevents, wait_event_info)
#define WaitLatchMy(latch, wakeEvents, timeout, wait_event_info) WaitLatch(latch, wakeEvents, timeout, wait_event_info)
#else
#define createdb_my(pstate, stmt) createdb(stmt)
#define CreateRoleMy(pstate, stmt) CreateRole(stmt)
#define makeDefElemMy(name, arg, location) makeDefElem(name, arg)
#ifdef GP_VERSION_NUM
#define shm_toc_lookup_my(toc, key, noError) shm_toc_lookup(toc, key, noError)
#else
#define shm_toc_lookup_my(toc, key, noError) shm_toc_lookup(toc, key)
#endif
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents, wait_event_info) WaitEventSetWait(set, timeout, occurred_events, nevents)
#define WaitLatchMy(latch, wakeEvents, timeout, wait_event_info) WaitLatch(latch, wakeEvents, timeout)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username, flags) BackgroundWorkerInitializeConnection(dbname, username, flags)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username, flags) BackgroundWorkerInitializeConnection(dbname, username)
#endif

#if PG_VERSION_NUM >= 120000
#define relation_openrv_extended_my(relation, lockmode, missing_ok, noWait) relation_openrv_extended(relation, lockmode, missing_ok)
#else
#ifdef GP_VERSION_NUM
#define relation_openrv_extended_my(relation, lockmode, missing_ok, noWait) relation_openrv_extended(relation, lockmode, missing_ok, noWait)
#else
#define relation_openrv_extended_my(relation, lockmode, missing_ok, noWait) relation_openrv_extended(relation, lockmode, missing_ok)
#endif
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

#define PG_WORK_MAGIC 0x776f726b

typedef struct WorkShared {
    char data[NAMEDATALEN];
    char schema[NAMEDATALEN];
    char table[NAMEDATALEN];
    char user[NAMEDATALEN];
    int64 reset;
    int64 sleep;
    Oid oid;
} WorkShared;

typedef struct Work {
    char *schema_table;
    char *schema_type;
    const char *schema;
    const char *table;
    dsm_segment *seg;
    int hash;
    WorkShared *shared;
} Work;

#define PG_TASK_MAGIC 0x7461736b

typedef struct TaskShared {
    dsm_handle handle;
    int64 id;
    int hash;
    int max;
} TaskShared;

typedef struct Task {
    bool active;
    bool header;
    bool lock;
    bool string;
    char delimiter;
    char escape;
    char *group;
    char *input;
    char *null;
    char quote;
    char *remote;
    dlist_node node;
    dsm_segment *seg;
    int count;
    int event;
    int pid;
    int skip;
    int timeout;
    PGconn *conn;
    StringInfoData error;
    StringInfoData output;
    TaskShared *shared;
    TimestampTz start;
    uint64 row;
    void (*socket) (struct Task *t);
} Task;

bool init_oid_is_string(Oid oid);
bool is_log_level_output(int elevel, int log_min_level);
bool lock_data_user_hash(Oid data, Oid user, int hash);
bool lock_data_user(Oid data, Oid user);
bool lock_table_id(Oid table, int64 id);
bool lock_table_pid_hash(Oid table, int pid, int hash);
bool task_done(Task *t);
bool task_work(Task *t);
bool unlock_data_user_hash(Oid data, Oid user, int hash);
bool unlock_data_user(Oid data, Oid user);
bool unlock_table_id(Oid table, int64 id);
bool unlock_table_pid_hash(Oid table, int pid, int hash);
char *TextDatumGetCStringMy(Datum datum);
const char *error_severity(int elevel);
Datum CStringGetTextDatumMy(const char *s);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
int severity_error(const char *error);
Portal SPI_cursor_open_my(const char *name, SPIPlanPtr plan, Datum *values, const char *nulls);
Portal SPI_cursor_open_with_args_my(const char *name, const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void appendBinaryStringInfoEscapeQuote(StringInfoData *buf, const char *data, int len, bool string, char escape, char quote);
void append_with_tabs(StringInfo buf, const char *str);
void conf_main(Datum main_arg);
void conf_work(const Work *w);
#if PG_VERSION_NUM < 120000
extern PGDLLIMPORT ResourceOwner AuxProcessResourceOwner;
void CreateAuxProcessResourceOwner(void);
void ReleaseAuxProcessResources(bool isCommit);
#endif
void initStringInfoMy(StringInfoData *buf);
void _PG_init(void);
void *shm_toc_allocate_my(uint64 magic, dsm_segment **seg, Size nbytes);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(void);
void task_error(ErrorData *edata);
void task_free(Task *t);
void task_main(Datum main_arg);
void work_main(Datum main_arg);

#endif // _INCLUDE_H_
