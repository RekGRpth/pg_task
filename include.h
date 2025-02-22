#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define countof(array) (sizeof(array)/sizeof(array[0]))
#define SQL(...) #__VA_ARGS__

#include <postgres.h>

#include <executor/spi.h>
#if PG_VERSION_NUM < 90500
#include <lib/stringinfo.h>
#endif
#include <libpq-fe.h>
#if PG_VERSION_NUM >= 160000
#include <nodes/miscnodes.h>
#endif

#ifdef GP_VERSION_NUM
#include "cdb/cdbvars.h"
#endif

#if PG_VERSION_NUM >= 90500
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel, false)
#else
#define MyLatch (&MyProc->procLatch)
#define set_config_option_my(name, value, context, source, action, changeVal, elevel) set_config_option(name, value, context, source, action, changeVal, elevel)
#endif

#if PG_VERSION_NUM >= 100000
#define createdb_my(pstate, stmt) createdb(pstate, stmt)
#define CreateRoleMy(pstate, stmt) CreateRole(pstate, stmt)
#define makeDefElemMy(name, arg) makeDefElem(name, arg, -1)
#define shm_toc_lookup_my(toc, key) shm_toc_lookup(toc, key, false)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents) WaitEventSetWait(set, timeout, occurred_events, nevents, PG_WAIT_EXTENSION)
#define WaitLatchMy(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)
#else
#define createdb_my(pstate, stmt) createdb(stmt)
#define CreateRoleMy(pstate, stmt) CreateRole(stmt)
#define makeDefElemMy(name, arg) makeDefElem(name, arg)
#ifdef GP_VERSION_NUM
#define shm_toc_lookup_my(toc, key) shm_toc_lookup(toc, key, false)
#else
#define shm_toc_lookup_my(toc, key) shm_toc_lookup(toc, key)
#endif
#define WL_SOCKET_MASK (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)
#define WaitEventSetWaitMy(set, timeout, occurred_events, nevents) WaitEventSetWait(set, timeout, occurred_events, nevents)
#define WaitLatchMy(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout)
#endif

#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username, 0)
#else
#define BackgroundWorkerInitializeConnectionMy(dbname, username) BackgroundWorkerInitializeConnection(dbname, username)
#endif

#if PG_VERSION_NUM >= 120000
#define relation_openrv_extended_my(relation, lockmode, missing_ok) relation_openrv_extended(relation, lockmode, false)
#else
#ifdef GP_VERSION_NUM
#define relation_openrv_extended_my(relation, lockmode, missing_ok) relation_openrv_extended(relation, lockmode, missing_ok, false)
#else
#define relation_openrv_extended_my(relation, lockmode, missing_ok) relation_openrv_extended(relation, lockmode, missing_ok)
#endif
#endif

#if PG_VERSION_NUM >= 130000
#define set_ps_display_my(activity) set_ps_display(activity)
#else
#define set_ps_display_my(activity) set_ps_display(activity, false)
#endif

#if PG_VERSION_NUM >= 160000
#define parseTypeStringMy(str, typeid_p, typmod_p) parseTypeString(str, typeid_p, typmod_p, (Node *)&(ErrorSaveContext){T_ErrorSaveContext})
#define stringToQualifiedNameListMy(string) stringToQualifiedNameList(string, NULL)
#else
#define parseTypeStringMy(str, typeid_p, typmod_p) parseTypeString(str, typeid_p, typmod_p, true)
#define stringToQualifiedNameListMy(string) stringToQualifiedNameList(string)
#endif

#if PG_VERSION_NUM >= 170000
#define CreateWaitEventSetMy(nevents) CreateWaitEventSet(NULL, nevents)
#else
#define CreateWaitEventSetMy(nevents) CreateWaitEventSet(TopMemoryContext, nevents)
#endif

#ifndef MemoryContextResetAndDeleteChildren
#define MemoryContextResetAndDeleteChildren(ctx) MemoryContextReset(ctx)
#endif

typedef struct Shared {
    bool in_use;
    char data[NAMEDATALEN];
    char schema[NAMEDATALEN];
    char table[NAMEDATALEN];
    char user[NAMEDATALEN];
    int64 id;
    int64 reset;
    int64 sleep;
    int hash;
    int max;
    int run;
    Oid oid;
} Shared;

typedef struct Work {
    char *schema_table;
    char *schema_type;
    const char *columns;
    const char *data;
    const char *schema;
    const char *table;
    const char *user;
    dlist_node node;
    pid_t pid;
    Shared *shared;
} Work;

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
    int count;
    int event;
    int pid;
    int skip;
    int timeout;
    PGconn *conn;
    Shared *shared;
    StringInfoData error;
    StringInfoData output;
    TimestampTz start;
    uint64 row;
    void (*socket) (struct Task *t);
    Work *work;
} Task;

bool dest_timeout(void);
bool init_oid_is_string(Oid oid);
bool is_log_level_output(int elevel, int log_min_level);
bool lock_data_user_hash(Oid data, Oid user, int hash);
bool lock_data_user(Oid data, Oid user);
bool lock_table_id(Oid table, int64 id);
bool lock_table_pid_hash(Oid table, int pid, int hash);
bool task_done(Task *t);
bool task_live(const Task *t);
bool task_work(Task *t);
bool unlock_data_user_hash(Oid data, Oid user, int hash);
bool unlock_data_user(Oid data, Oid user);
bool unlock_table_id(Oid table, int64 id);
bool unlock_table_pid_hash(Oid table, int pid, int hash);
char *TextDatumGetCStringMy(Datum datum);
const char *error_severity(int elevel);
Datum CStringGetTextDatumMy(const char *s);
Datum SPI_getbinval_my(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid);
int init_bgw_main_arg(Shared *ws);
int severity_error(const char *error);
PGDLLEXPORT void conf_main(Datum main_arg);
PGDLLEXPORT void task_main(Datum main_arg);
PGDLLEXPORT void work_main(Datum main_arg);
Portal SPI_cursor_open_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, bool read_only);
Portal SPI_cursor_open_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, bool read_only);
Shared *init_shared(Datum main_arg);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
Task *get_task(void);
Work *get_work(void);
void appendBinaryStringInfoEscapeQuote(StringInfo buf, const char *data, int len, bool string, char escape, char quote);
void append_with_tabs(StringInfo buf, const char *str);
void init_conf(bool dynamic);
void initStringInfoMy(StringInfo buf);
void _PG_init(void);
void shared_free(int slot);
void SPI_connect_my(const char *src);
void SPI_cursor_close_my(Portal portal);
void SPI_cursor_fetch_my(const char *src, Portal portal, bool forward, long count);
void SPI_execute_plan_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(void);
void task_error(ErrorData *edata);
void task_free(Task *t);

#endif // _INCLUDE_H_
