#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define FORMAT_0(fmt, ...) "%s(%s:%d): %s", __func__, __FILE__, __LINE__, fmt
#define FORMAT_1(fmt, ...) "%s(%s:%d): " fmt,  __func__, __FILE__, __LINE__
#define GET_FORMAT(fmt, ...) GET_FORMAT_PRIVATE(fmt, 0, ##__VA_ARGS__, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
    1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
#define GET_FORMAT_PRIVATE(fmt, \
      _0,  _1,  _2,  _3,  _4,  _5,  _6,  _7,  _8,  _9, \
     _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, \
     _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, \
     _30, _31, _32, _33, _34, _35, _36, _37, _38, _39, \
     _40, _41, _42, _43, _44, _45, _46, _47, _48, _49, \
     _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, \
     _60, _61, _62, _63, _64, _65, _66, _67, _68, _69, \
     _70, format, ...) FORMAT_ ## format(fmt)

#define D1(fmt, ...) ereport(DEBUG1, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D2(fmt, ...) ereport(DEBUG2, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D3(fmt, ...) ereport(DEBUG3, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D4(fmt, ...) ereport(DEBUG4, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define D5(fmt, ...) ereport(DEBUG5, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define E(fmt, ...) ereport(ERROR, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define F(fmt, ...) ereport(FATAL, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define I(fmt, ...) ereport(INFO, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define L(fmt, ...) ereport(LOG, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define N(fmt, ...) ereport(NOTICE, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define W(fmt, ...) ereport(WARNING, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))

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
#else
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

typedef struct _SPI_plan SPI_plan;

#define serialize_bool(src) if ((len += sizeof(src)) >= sizeof(worker.bgw_extra)) E("sizeof"); else memcpy(worker.bgw_extra + len - sizeof(src), &(src), sizeof(src));
#define serialize_char_null(src) serialize_char((src) ? (src) : "")
#define serialize_char(src) if ((len += strlcpy(worker.bgw_extra + len, (src), sizeof(worker.bgw_extra)) + 1) >= sizeof(worker.bgw_extra)) E("strlcpy")
#define serialize_int(src) if ((len += sizeof(src)) >= sizeof(worker.bgw_extra)) E("sizeof"); else memcpy(worker.bgw_extra + len - sizeof(src), &(src), sizeof(src));

#define deserialize_bool(dst) (dst) = *(typeof(dst) *)p; p += sizeof(dst);
#define deserialize_char(dst) (dst) = p; p += strlen(dst) + 1;
#define deserialize_char_null(dst) deserialize_char(dst); if (p == (dst) + 1) (dst) = NULL;
#define deserialize_int(dst) (dst) = *(typeof(dst) *)p; p += sizeof(dst);

#if PG_VERSION_NUM >= 100000
#else
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#endif

#define CONF \
    X(count,  serialize_int, deserialize_int) \
    X(live, serialize_int, deserialize_int) \
    X(oid.data, serialize_int, deserialize_int) \
    X(oid.user, serialize_int, deserialize_int) \
    X(str.partman, serialize_char_null, deserialize_char_null) \
    X(str.schema, serialize_char, deserialize_char) \
    X(str.table, serialize_char, deserialize_char) \
    X(timeout,  serialize_int, deserialize_int)

#define TASK \
    X(group, serialize_char, deserialize_char) \
    X(hash, serialize_int, deserialize_int) \
    X(max, serialize_int, deserialize_int)

#define WORK \
    X(oid.data, serialize_int, deserialize_int) \
    X(oid.user, serialize_int, deserialize_int) \
    X(oid.schema, serialize_int, deserialize_int) \
    X(oid.table, serialize_int, deserialize_int)

typedef struct Task {
    bool delete;
    bool fail;
    bool header;
    bool live;
    bool lock;
    bool repeat;
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
    } str;
} Work;

bool init_check_ascii_all(BackgroundWorker *worker);
bool init_data_user_table_lock(Oid data, Oid user, Oid table);
bool init_data_user_table_unlock(Oid data, Oid user, Oid table);
bool init_oid_is_string(Oid oid);
bool init_table_id_lock(Oid table, int64 id);
bool init_table_id_unlock(Oid table, int64 id);
bool init_table_pid_hash_lock(Oid table, int pid, int hash);
bool init_table_pid_hash_unlock(Oid table, int pid, int hash);
bool task_done(Task *task);
bool task_live(Task *task);
bool task_work(Task *task);
char *TextDatumGetCStringMy(MemoryContext memoryContext, Datum datum);
Datum CStringGetTextDatumMy(MemoryContext memoryContext, const char *s);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
DestReceiver *CreateDestReceiverMy(CommandDest dest);
SPI_plan *SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest);
#else
void BeginCommandMy(const char *commandTag, CommandDest dest);
#endif
void conf_main(Datum main_arg);
void conf_work(Work *work);
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
void SPI_execute_plan_my(SPI_plan *plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void task_delete(Task *task);
void task_error(Task *task, ErrorData *edata);
void task_main(Datum main_arg);
void task_repeat(Task *task);
void work_main(Datum main_arg);

#endif // _INCLUDE_H_
