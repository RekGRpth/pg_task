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

#include <postgres.h>

#include <access/printtup.h>
#include <access/relation.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <commands/dbcommands.h>
#include <commands/prepare.h>
#include <commands/user.h>
#include <executor/spi.h>
#include <jit/jit.h>
#include <libpq-fe.h>
#include <libpq/libpq-be.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/analyze.h>
#include <parser/parse_type.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <postmaster/interrupt.h>
#include <replication/slot.h>
#include <storage/ipc.h>
#include <tcop/pquery.h>
#include <tcop/utility.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/ps_status.h>
#include <utils/regproc.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

typedef struct _SPI_plan SPI_plan;

typedef struct Work {
    char *data;
    char *pids;
    char *schema;
    char *schema_table;
    char *schema_type;
    char *table;
    char *user;
    dlist_head head;
    int _count;
    int count;
    int live;
    int reset;
    int timeout;
    Oid oid;
} Work;

typedef struct Task {
    bool append;
    bool delete;
    bool fail;
    bool header;
    bool live;
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
    int length;
    int max;
    int pid;
    int skip;
    int timeout;
    PGconn *conn;
    StringInfoData error;
    StringInfoData output;
    TimestampTz start;
    void (*socket) (struct Task *task);
    Work *work;
} Task;

bool init_data_user_table_lock(Oid data, Oid user, Oid table);
bool init_data_user_table_unlock(Oid data, Oid user, Oid table);
bool init_oid_is_string(Oid oid);
bool init_table_id_lock(Oid table, int64 id);
bool init_table_id_unlock(Oid table, int64 id);
bool task_done(Task *task);
bool task_live(Task *task);
bool task_work(Task *task);
char *TextDatumGetCStringMy(MemoryContextData *memoryContext, Datum datum);
const char *PQftypeMy(Oid oid);
const char *PQftypeMy(Oid oid);
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDescData *tupdesc, const char *fname, bool allow_null);
DestReceiver *CreateDestReceiverMy(Task *task);
SPI_plan *SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void BeginCommandMy(CommandTag commandTag, Task *task);
void conf_work(const char *user, const char *data, const char *schema, const char *table, const int reset, const int timeout, const int count, const int live);
void conf_worker(Datum main_arg);
void EndCommandMy(const QueryCompletion *qc, Task *task, bool force_undecorated_output);
void exec_simple_query_my(Task *task);
void init_escape(StringInfoData *buf, const char *data, int len, char escape);
void initStringInfoMy(MemoryContextData *memoryContext, StringInfoData *buf);
void NullCommandMy(Task *task);
void _PG_init(void);
void ReadyForQueryMy(Task *task);
void SPI_commit_my(void);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPI_plan *plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void task_delete(Task *task);
void task_error(Task *task, ErrorData *edata);
void task_repeat(Task *task);
void task_worker(Datum main_arg);
void work_worker(Datum main_arg);

#endif // _INCLUDE_H_
