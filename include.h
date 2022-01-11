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
#include <storage/shm_toc.h>
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
#include <utils/timestamp.h>

#if PG_VERSION_NUM >= 90600
#else
#include <storage/latch.h>
#include <storage/proc.h>
#include "latch.h"
#endif

#define PG_WORK_MAGIC 0x776f726b

enum {
    PG_WORK_KEY_OID_DATA,
    PG_WORK_KEY_OID_USER,
    PG_WORK_KEY_RESET,
    PG_WORK_KEY_TIMEOUT,
    PG_WORK_KEY_STR_PARTMAN,
    PG_WORK_KEY_STR_SCHEMA,
    PG_WORK_KEY_STR_TABLE,
#if PG_VERSION_NUM >= 90500
#else
    PG_WORK_KEY_STR_DATA,
    PG_WORK_KEY_STR_USER,
#endif
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
#if PG_VERSION_NUM >= 90500
#else
    PG_TASK_KEY_STR_DATA,
    PG_TASK_KEY_STR_USER,
#endif
    PG_TASK_NKEYS,
};

#define serialize_bool(src) if ((len += sizeof(src)) >= sizeof_worker_bgw_extra) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("sizeof %li >= %li", len, sizeof_worker_bgw_extra))); else memcpy(worker_bgw_extra + len - sizeof(src), &(src), sizeof(src));
#define serialize_char_null(src) serialize_char((src) ? (src) : "")
#define serialize_char(src) if ((len += strlcpy(worker_bgw_extra + len, (src), sizeof_worker_bgw_extra - len) + 1) >= sizeof_worker_bgw_extra) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("strlcpy %li >= %li", len, sizeof_worker_bgw_extra)));
#define serialize_int(src) if ((len += sizeof(src)) >= sizeof_worker_bgw_extra) ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("sizeof %li >= %li", len, sizeof_worker_bgw_extra))); else memcpy(worker_bgw_extra + len - sizeof(src), &(src), sizeof(src));

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

#if PG_VERSION_NUM >= 90500
#define MyBgworkerEntry_bgw_extra (MyBgworkerEntry->bgw_extra)
#define sizeof_worker_bgw_extra (sizeof(worker.bgw_extra))
#define worker_bgw_extra (worker.bgw_extra)
#define TASK \
    X(task->group, serialize_char, deserialize_char) \
    X(task->hash, serialize_int, deserialize_int) \
    X(task->max, serialize_int, deserialize_int) \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.schema, serialize_int, deserialize_int) \
    X(work->oid.table, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int)
#define WORK \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int) \
    X(work->reset, serialize_int, deserialize_int) \
    X(work->str.partman, serialize_char_null, deserialize_char_null) \
    X(work->str.schema, serialize_char, deserialize_char) \
    X(work->str.table, serialize_char, deserialize_char) \
    X(work->timeout,  serialize_int, deserialize_int)
#else
#define MyBgworkerEntry_bgw_extra (MyBgworkerEntry->bgw_library_name + sizeof("pg_task"))
#define sizeof_worker_bgw_extra (sizeof(worker.bgw_library_name) - sizeof("pg_task"))
#define worker_bgw_extra (worker.bgw_library_name + sizeof("pg_task"))
#define TASK \
    X(task->group, serialize_char, deserialize_char) \
    X(task->hash, serialize_int, deserialize_int) \
    X(task->max, serialize_int, deserialize_int) \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.schema, serialize_int, deserialize_int) \
    X(work->oid.table, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int) \
    X(work->str.data, serialize_char, deserialize_char) \
    X(work->str.user, serialize_char, deserialize_char)
#define WORK \
    X(work->oid.data, serialize_int, deserialize_int) \
    X(work->oid.user, serialize_int, deserialize_int) \
    X(work->reset, serialize_int, deserialize_int) \
    X(work->str.data, serialize_char, deserialize_char) \
    X(work->str.partman, serialize_char_null, deserialize_char_null) \
    X(work->str.schema, serialize_char, deserialize_char) \
    X(work->str.table, serialize_char, deserialize_char) \
    X(work->str.user, serialize_char, deserialize_char) \
    X(work->timeout,  serialize_int, deserialize_int)
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
