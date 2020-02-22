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
#include <commands/defrem.h>
#include <commands/prepare.h>
#include <commands/user.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <libpq-fe.h>
#include <libpq/libpq-be.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <parser/analyze.h>
#include <parser/parse_type.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/pmsignal.h>
#include <storage/procarray.h>
#include <tcop/pquery.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/regproc.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/timeout.h>
#include <utils/varlena.h>

#include "queue.h"

typedef struct Conf {
    char *data;
    char *p;
    char *schema;
    char *table;
    char *user;
    int period;
} Conf;

typedef struct Work {
    Conf conf;
    char *schema_table;
    MemoryContext context;
    Oid oid;
    queue_t queue;
} Work;

typedef struct Event {
    int events;
    long timeout;
    Work work;
} Event;

typedef struct Task {
    bool delete;
    bool live;
    bool remote;
    bool repeat;
    char *queue;
    char *request;
    char *state;
    int64 id;
    int count;
    int max;
    int timeout;
    StringInfoData response;
    TimestampTz start;
    Work *work;
} Task;

typedef struct Remote {
    bool send;
    PGconn *conn;
    queue_t queue;
    Task task;
    WaitEvent event;
} Remote;

bool pg_advisory_unlock_int4_my(int32 key1, int32 key2);
bool pg_advisory_unlock_int8_my(int64 key);
bool pg_try_advisory_lock_int4_my(int32 key1, int32 key2);
bool pg_try_advisory_lock_int8_my(int64 key);
bool tick_init_work(const bool is_conf, Work *work);
char *PQftypeMy(const PGresult *res, int column_number);
char *SPI_getvalue_my(HeapTuple tuple, TupleDesc tupdesc, int fnumber);
DestReceiver *CreateDestReceiverMy(CommandDest dest, Task *task);
int WaitLatchOrSocketMy(Latch *latch, WaitEvent *event, int wakeEvents, queue_t *work_queue, long timeout, uint32 wait_event_info);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void exec_simple_query(Task *task);
void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker);
void SetConfigOptionMy(const char *name, const char *value);
void SPI_commit_my(const char *command);
void SPI_connect_my(const char *command);
void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(const char *command);
void SPI_rollback_my(const char *command);
void SPI_start_transaction_my(const char *command);
void task_done(Task *task);
void task_work(Task *task);
void tick_loop(Work *work);

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
