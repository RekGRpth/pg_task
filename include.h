#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define D1(...) ereport(DEBUG1, (errmsg(__VA_ARGS__)))
#define D2(...) ereport(DEBUG2, (errmsg(__VA_ARGS__)))
#define D3(...) ereport(DEBUG3, (errmsg(__VA_ARGS__)))
#define D4(...) ereport(DEBUG4, (errmsg(__VA_ARGS__)))
#define D5(...) ereport(DEBUG5, (errmsg(__VA_ARGS__)))
#define E(...) ereport(ERROR, (errmsg(__VA_ARGS__)))
#define F(...) ereport(FATAL, (errmsg(__VA_ARGS__)))
#define I(...) ereport(INFO, (errmsg(__VA_ARGS__)))
#define L(...) ereport(LOG, (errmsg(__VA_ARGS__)))
#define N(...) ereport(NOTICE, (errmsg(__VA_ARGS__)))
#define W(...) ereport(WARNING, (errmsg(__VA_ARGS__)))

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
#if PG_VERSION_NUM >= 120000
#else
extern PGDLLIMPORT TimestampTz MyStartTimestamp;
#endif
#include <replication/slot.h>
#include <storage/ipc.h>
#if PG_VERSION_NUM >= 140000
#include <storage/proc.h>
#endif
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

#define serialize_bool(src) if ((len += sizeof(src)) >= sizeof(worker.bgw_extra)) E("sizeof"); else memcpy(worker.bgw_extra + len - sizeof(src), &(src), sizeof(src));
#define serialize_char_null(src) serialize_char((src) ? (src) : "")
#define serialize_char(src) if ((len += strlcpy(worker.bgw_extra + len, (src), sizeof(worker.bgw_extra)) + 1) >= sizeof(worker.bgw_extra)) E("strlcpy")
#define serialize_int(src) if ((len += sizeof(src)) >= sizeof(worker.bgw_extra)) E("sizeof"); else memcpy(worker.bgw_extra + len - sizeof(src), &(src), sizeof(src));

#define deserialize_bool(dst) (dst) = *(typeof(dst) *)p; p += sizeof(dst);
#define deserialize_char(dst) (dst) = p; p += strlen(dst) + 1;
#define deserialize_char_null(dst) deserialize_char(dst); if (p == (dst) + 1) (dst) = NULL;
#define deserialize_int(dst) (dst) = *(typeof(dst) *)p; p += sizeof(dst);

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

#if PG_VERSION_NUM >= 100000
#else
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#endif

#define TASK \
    X(task->group, serialize_char, deserialize_char) \
    X(task->hash, serialize_int, deserialize_int) \
    X(task->max, serialize_int, deserialize_int) \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.schema, serialize_int, deserialize_int) \
    X(work->oid.table, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int)

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

#define WORK \
    X(work->count,  serialize_int, deserialize_int) \
    X(work->live, serialize_int, deserialize_int) \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int) \
    X(work->str.partman, serialize_char_null, deserialize_char_null) \
    X(work->str.schema, serialize_char, deserialize_char) \
    X(work->str.table, serialize_char, deserialize_char) \
    X(work->timeout,  serialize_int, deserialize_int)

typedef struct Work {
    char *schema_table;
    char *schema_type;
    dlist_head head;
    int32 count;
    int32 processed;
    int32 timeout;
    int64 live;
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

bool init_check_ascii_all(BackgroundWorker *worker);
bool init_oid_is_string(Oid oid);
bool lock_data_user_table(Oid data, Oid user, Oid table);
bool lock_table_id(Oid table, int64 id);
bool lock_table_pid_hash(Oid table, int pid, int hash);
bool task_done(Task *task);
bool task_work(Task *task);
bool unlock_data_user_table(Oid data, Oid user, Oid table);
bool unlock_table_id(Oid table, int64 id);
bool unlock_table_pid_hash(Oid table, int pid, int hash);
char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum);
Datum CStringGetTextDatumMy(MemoryContext memoryContext, const char *s);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
DestReceiver *CreateDestReceiverMy(CommandDest dest);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest);
#else
void BeginCommandMy(const char *commandTag, CommandDest dest);
#endif
void conf_main(Datum main_arg);
void conf_work(BackgroundWorker *worker);
#if PG_VERSION_NUM >= 130000
void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output);
#else
void EndCommandMy(const char *commandTag, CommandDest dest);
#endif
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
void task_free(Task *task);
void task_main(Datum main_arg);
void work_main(Datum main_arg);

#endif // _INCLUDE_H_
