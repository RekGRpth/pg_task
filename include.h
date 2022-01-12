#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define countof(array) (sizeof(array)/sizeof(array[0]))
#define SQL(...) #__VA_ARGS__

#include <postgres.h>

#include <access/printtup.h>
#if PG_VERSION_NUM >= 120000
#include <access/relation.h>
#endif
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/namespace.h>
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
#include <storage/proc.h>
#include <storage/shm_toc.h>
#include <tcop/pquery.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
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

#define PG_WORK_MAGIC 0x776f726b

enum {
    PG_WORK_KEY_OID_DATA,
    PG_WORK_KEY_OID_USER,
    PG_WORK_KEY_RESET,
    PG_WORK_KEY_STR_DATA,
    PG_WORK_KEY_STR_PARTMAN,
    PG_WORK_KEY_STR_SCHEMA,
    PG_WORK_KEY_STR_TABLE,
    PG_WORK_KEY_STR_USER,
    PG_WORK_KEY_TIMEOUT,
    PG_WORK_NKEYS,
};

#define PG_TASK_MAGIC 0x7461736b

enum {
    PG_TASK_KEY_GROUP,
    PG_TASK_KEY_HASH,
    PG_TASK_KEY_ID,
    PG_TASK_KEY_MAX,
    PG_TASK_KEY_OID_DATA,
    PG_TASK_KEY_OID_SCHEMA,
    PG_TASK_KEY_OID_TABLE,
    PG_TASK_KEY_OID_USER,
    PG_TASK_KEY_STR_DATA,
    PG_TASK_KEY_STR_SCHEMA,
    PG_TASK_KEY_STR_TABLE,
    PG_TASK_KEY_STR_USER,
    PG_TASK_NKEYS,
};

#if PG_VERSION_NUM >= 90500
#define dsm_create_my(size, flags) dsm_create(size, flags)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel, is_reload) set_config_option(name, value, context, source, action, changeVal, elevel, is_reload)
#else
#define dsm_create_my(size, flags) dsm_create(size)
#define MyLatch (&MyProc->procLatch)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel, is_reload) set_config_option(name, value, context, source, action, changeVal, elevel)
#endif

#if PG_VERSION_NUM >= 100000
#define createdb_my(pstate, stmt) createdb(pstate, stmt)
#define makeDefElemMy(name, arg, location) makeDefElem(name, arg, location)
#define shm_toc_lookup_my(toc, key, noError) shm_toc_lookup(toc, key, noError)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents, wait_event_info) WaitEventSetWait(set, timeout, occurred_events, nevents, wait_event_info)
#define WaitLatchMy(latch, wakeEvents, timeout, wait_event_info) WaitLatch(latch, wakeEvents, timeout, wait_event_info)
#else
#define createdb_my(pstate, stmt) createdb(stmt)
#define makeDefElemMy(name, arg, location) makeDefElem(name, arg)
#define shm_toc_lookup_my(toc, key, noError) shm_toc_lookup(toc, key)
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents, wait_event_info) WaitEventSetWait(set, timeout, occurred_events, nevents)
#define WaitLatchMy(latch, wakeEvents, timeout, wait_event_info) WaitLatch(latch, wakeEvents, timeout)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username, flags) BackgroundWorkerInitializeConnection(dbname, username, flags)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username, flags) BackgroundWorkerInitializeConnection(dbname, username)
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

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
    int64 id;
    int count;
    int event;
    int hash;
    int max;
    int pid;
    int skip;
    int timeout;
    PGconn *conn;
    StringInfoData error;
    StringInfoData output;
    TimestampTz start;
    uint64 row;
    void (*socket) (struct Task *task);
} Task;

typedef struct Work {
    char *schema_table;
    char *schema_type;
    dlist_head head;
    int64 reset;
    int64 timeout;
    struct {
        Oid data;
        Oid partman;
        Oid schema;
        Oid table;
        Oid user;
    } oid;
    struct {
        char *data;
        char *partman;
        char *schema;
        char *table;
        char *user;
    } quote;
    struct {
        char *data;
        char *partman;
        char *schema;
        char *table;
        char *user;
    } str;
} Work;

bool init_oid_is_string(Oid oid);
bool is_log_level_output(int elevel, int log_min_level);
bool lock_data_user_table(Oid data, Oid user, Oid table);
bool lock_table_id(Oid table, int64 id);
bool lock_table_pid_hash(Oid table, int pid, int hash);
bool task_done(Task *task);
bool task_work(Task *task);
bool unlock_data_user_table(Oid data, Oid user, Oid table);
bool unlock_table_id(Oid table, int64 id);
bool unlock_table_pid_hash(Oid table, int pid, int hash);
char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum);
const char *error_severity(int elevel);
const char *init_check(void);
Datum CStringGetTextDatumMy(MemoryContext memoryContext, const char *s);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
DestReceiver *CreateDestReceiverMy(CommandDest dest);
int severity_error(const char *error);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest);
#else
void BeginCommandMy(const char *commandTag, CommandDest dest);
#endif
void conf_main(Datum main_arg);
#if PG_VERSION_NUM >= 130000
void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output);
#else
void EndCommandMy(const char *commandTag, CommandDest dest);
#endif
void append_with_tabs(StringInfo buf, const char *str);
void exec_simple_query_my(const char *query_string);
void init_escape(StringInfoData *buf, const char *data, int len, char escape);
void initStringInfoMy(MemoryContext memoryContext, StringInfoData *buf);
void NullCommandMy(CommandDest dest);
void _PG_init(void);
void ReadyForQueryMy(CommandDest dest);
void SPI_commit_my(void);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void task_error(ErrorData *edata);
void task_free(Task *task);
void task_main(Datum main_arg);
void work_main(Datum main_arg);

#endif // _INCLUDE_H_
