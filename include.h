#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define countof(array) (sizeof(array)/sizeof(array[0]))
// the most of output a task may keep, which, stored in its row by the bookkeeping, must leave room within the most a single allocation may take for a copy or two more and the compression of TOAST (lz4 adds up to 1/255): a task that outputs more fails, keeping that much, rather than fail its bookkeeping and have the task run again on every reset
#define TASK_OUTPUT_MAX (MaxAllocSize - 16 * 1024 * 1024)
#define SQL(...) #__VA_ARGS__

#include <postgres.h>
#include <executor/spi.h>
// libpq is always linked as the frontend library, so see its api the frontend way too: Greengage's PQconninfoOption has the extra connofs field for the backend only
#ifdef LIBPQ_FE_H
#error "libpq-fe.h must not be included before include.h"
#endif
#define FRONTEND
#include <libpq-fe.h>
#undef FRONTEND

#if PG_VERSION_NUM < 90500
#include <lib/stringinfo.h>
#endif

#include <signal.h>

#if PG_VERSION_NUM >= 160000
#include <nodes/miscnodes.h>
#endif

#ifdef GP_VERSION_NUM
#include "cdb/cdbvars.h"
#endif

#if PG_VERSION_NUM >= 90500
#else
#define MyLatch (&MyProc->procLatch)
#endif

#if PG_VERSION_NUM >= 100000
#define WaitLatchMy(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)
#define WaitLatchOrSocketMy(latch, wakeEvents, sock, timeout) WaitLatchOrSocket(latch, wakeEvents, sock, timeout, PG_WAIT_EXTENSION)
#else
#define WaitLatchMy(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout)
#define WaitLatchOrSocketMy(latch, wakeEvents, sock, timeout) WaitLatchOrSocket(latch, wakeEvents, sock, timeout)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username, 0)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username)
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
extern PGDLLIMPORT volatile sig_atomic_t ShutdownRequestPending;
void SignalHandlerForConfigReload(SIGNAL_ARGS);
#endif

// gone from 17 on, a macro of MemoryContextReset(), which deletes the children too, from 9.5 on, and a function of its own in 9.4, where MemoryContextReset() only resets them
#if PG_VERSION_NUM >= 90500 && !defined(MemoryContextResetAndDeleteChildren)
#define MemoryContextResetAndDeleteChildren(ctx) MemoryContextReset(ctx)
#endif

typedef struct Shared {
    bool gone; // of a pg_work: its entry is no longer in pg_task.json, or another pg_work took over from it, see init_work()
    bool in_use;
    bool spi;
    char data[NAMEDATALEN];
    char owner[NAMEDATALEN];
    char schema[NAMEDATALEN];
    char table[NAMEDATALEN];
    char user[NAMEDATALEN];
    int64 id;
    int64 reg; // of a pg_work: the registration of it, which a restart of it keeps, see init_free_work()
    int64 reset;
    int64 sleep;
    int64 stop;
    int hash;
    int limit;
    int max;
    int pid; // of a pg_work, once started: see init_work()
    int run;
    Oid oid;
} Shared;

typedef struct Work {
    bool spawn;
    int restart;
    char *schema_table;
    char *schema_type;
    const char *data;
    const char *schema;
    const char *table;
    const char *user;
    dlist_node node;
    pid_t pid;
    Shared *shared;
} Work;

typedef struct Task {
    bool header;
    bool line; // of the output, the first one taken already: see task_line()
    bool lock;
    bool reserve;
    bool save;
    bool string;
    char delimiter;
    char escape;
    char *failed; // the errors of the hosts of the connection string tried so far, one at a time, see work_next()
    char *group;
    char *input;
    char *null;
    char quote;
    char *remote;
    char *user;
    dlist_node node;
    int count;
    int event;
    int host; // of them, the one tried now, in the order of hosts
    int *hosts; // the order the hosts of the connection string are tried in, one at a time, for the connect_timeout of each, see work_remote(), or NULL for libpq to try them itself
    int nhosts;
    int key; // of the lock a remote task holds the slot of its group by, see work_connect()
    uint64 rows; // of the result a remote task gets in single-row mode so far, its headers before the first, see work_result()
    int pid;
    int skip;
    int timeout;
    PGconn *conn;
    Shared *shared;
    StringInfoData error;
    StringInfoData output;
    TimestampTz deadline; // of connecting to a remote server, or to the host of it tried now, from the connect_timeout of its connection string, which libpq doesn't enforce for an asynchronous connection
    TimestampTz start;
    uint64 row;
    void (*socket) (struct Task *t);
    Work *work;
} Task;

bool dest_timeout(void);
void dest_init(void);
bool init_free_task(int slot, const char *data, Oid oid, int64 id);
bool init_free_work(int slot, int64 reg);
void init_work(int n, const char **data, const char **user, const int *hash, bool *in_use);
bool init_work_gone(Datum main_arg);
void init_work_wake(const Shared *task);
bool init_oid_is_string(Oid oid);
bool init_stop(const char *data, Oid oid, int64 id);
bool is_log_level_output(int elevel, int log_min_level);
bool lock_data_user_hash(Oid data, Oid user, int hash);
bool lock_data_make(Oid data);
bool lock_data_user(Oid data, Oid user);
bool lock_table_id(Oid table, int64 id);
bool lock_table_id_hash(Oid table, int64 id, int hash);
bool lock_table_pid_hash(Oid table, int pid, int hash);
bool task_done(Task *t, bool live);
bool task_work(Task *t);
bool unlock_data_user_hash(Oid data, Oid user, int hash);
bool unlock_data_make(Oid data);
bool unlock_data_user(Oid data, Oid user);
void unlock_advisory_all(void);
uint32 init_table_key(Oid table);
void relock_table_id(Oid table, int64 id);
void relock_table_pid_hash(Oid table, int pid, int hash);
void init_task_ids(StringInfo ids, const char *data, Oid oid);
void init_task_pids(const char *data, Oid oid, StringInfo pids, StringInfo hashes);
void init_pause(Oid oid, int hash, TimestampTz until);
TimestampTz init_pauses(Oid oid, StringInfo hashes);
void init_task_pid(int slot, const char *data, Oid oid, int64 id, int pid);
bool unlock_table_id(Oid table, int64 id);
bool unlock_table_id_hash(Oid table, int64 id, int hash);
bool unlock_table_pid_hash(Oid table, int pid, int hash);
char *TextDatumGetCStringMy(Datum datum);
const char *error_severity(int elevel);
const char *init_null(void);
const char *init_plan(void);
const char *task_search_path(void);
void task_search_path_save(void);
void task_search_path_reset(void);
Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid);
int init_arg(const Shared *ws);
int init_conf_fetch(void);
int init_task_fetch(void);
int init_work_idle(void);
int init_work_fetch(void);
int severity_error(const char *error);
PGDLLEXPORT void conf_main(Datum main_arg);
PGDLLEXPORT void task_main(Datum main_arg);
PGDLLEXPORT void work_main(Datum main_arg);
Portal SPI_cursor_open_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, bool read_only);
Shared *init_shared(Datum main_arg);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
Task *get_task(void);
void task_line(Task *t);
void appendBinaryStringInfoEscapeQuote(StringInfo buf, const char *data, int len, bool string, char escape, char quote);
void append_with_tabs(StringInfo buf, const char *str);
void dest_cancel(SIGNAL_ARGS);
void exec_simple_query_my(const char *query_string);
void initStringInfoMy(StringInfo buf);
void _PG_init(void);
void init_free(int slot);
int init_free_slots(void);
void SPI_abort_my(void);
void SPI_connect_my(const char *src, Oid userid);
void SPI_cursor_close_my(Portal portal);
void SPI_cursor_fetch_my(const char *src, Portal portal, bool forward, long count);
void SPI_execute_plan_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(void);
void stmt_timeout_active_my(bool value);
void task_error(Task *t);
void task_error_data(Task *t, const ErrorData *edata);
void task_free(Task *t);
void task_untake(Task *t);
void xact_started_my(bool value);
Work *get_work(void);
extern volatile sig_atomic_t make_lock_timeout;
void make_schema(const Work *w);
void make_type(const Work *w);
void make_table(const Work *w);
void make_user(const Work *w);
void make_data(const Work *w);

DestReceiver *CreateDestReceiverMy(CommandDest dest);
void NullCommandMy(CommandDest dest);
#if PG_VERSION_NUM >= 130000
void BeginCommandMy(CommandTag commandTag, CommandDest dest);
void EndCommandMy(const QueryCompletion *qc, CommandDest dest, bool force_undecorated_output);
#else
void BeginCommandMy(const char *commandTag, CommandDest dest);
void EndCommandMy(const char *commandTag, CommandDest dest);
#endif

#endif // _INCLUDE_H_
