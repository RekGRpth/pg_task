#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#include <postgres.h>

#include <access/printtup.h>
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
#include <replication/slot.h>
#include <tcop/pquery.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/ps_status.h>
#include <utils/regproc.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>

#include "queue.h"

typedef struct _SPI_plan SPI_plan;

typedef struct Work {
    char *data;
    char *schema;
    char *schema_table;
    char *table;
    char *user;
    int reset;
    int timeout;
    Oid oid;
    queue_t queue;
} Work;

typedef struct Task {
    bool connected;
    bool delete;
    bool live;
    bool repeat;
    bool success;
    char *group;
    char *remote;
    char *request;
    int64 id;
    int count;
    int events;
    int fd;
    int max;
    int pid;
    int timeout;
    PGconn *conn;
    queue_t queue;
    StringInfoData response;
    TimestampTz start;
    Work *work;
} Task;

bool pg_advisory_unlock_int4_my(int32 key1, int32 key2);
bool pg_advisory_unlock_int8_my(int64 key);
bool pg_try_advisory_lock_int4_my(int32 key1, int32 key2);
bool pg_try_advisory_lock_int8_my(int64 key);
bool task_done(Task *task);
bool task_live(Task *task);
bool task_work(Task *task);
char *TextDatumGetCStringMy(Datum datum);
const char *PQftypeMy(Oid oid);
Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null);
DestReceiver *CreateDestReceiverMy(StringInfoData *response);
SPI_plan *SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void exec_simple_query(const char *request, const int timeout, StringInfoData *response);
void init_sighup(SIGNAL_ARGS);
void init_sigterm(SIGNAL_ARGS);
void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker);
void SetConfigOptionMy(const char *name, const char *value);
void SPI_commit_my(void);
void SPI_connect_my(const char *src);
void SPI_execute_plan_my(SPI_plan *plan, Datum *values, const char *nulls, int res, bool commit);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res, bool commit);
void SPI_finish_my(void);
void SPI_start_transaction_my(const char *src);
void task_delete(Task *task);
void task_repeat(Task *task);
void tick_init_work(Work *work);
void tick_socket(Task *task);
void tick_timeout(Work *work);

#define Q(name) #name
#define S(macro) Q(macro)

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

#define E(fmt, ...) ereport(ERROR, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define F(fmt, ...) ereport(FATAL, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define L(fmt, ...) ereport(LOG, (errhidestmt(true), errhidecontext(true), errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))
#define W(fmt, ...) ereport(WARNING, (errmsg(GET_FORMAT(fmt, ##__VA_ARGS__), ##__VA_ARGS__)))

#endif // _INCLUDE_H_
