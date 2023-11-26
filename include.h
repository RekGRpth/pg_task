#ifndef _INCLUDE_H_
#define _INCLUDE_H_

#define countof(array) (sizeof(array)/sizeof(array[0]))
#define SQL(...) #__VA_ARGS__

#include <postgres.h>

#include <executor/spi.h>
#if PG_VERSION_NUM < 90600
#include "latch.h"
#endif
#include <libpq-fe.h>
#if PG_VERSION_NUM >= 160000
#include <nodes/miscnodes.h>
#endif
#include <parser/parse_type.h>
#if PG_VERSION_NUM >= 130000
#include <postmaster/interrupt.h>
#else
extern PGDLLIMPORT volatile sig_atomic_t ShutdownRequestPending;
extern void SignalHandlerForConfigReload(SIGNAL_ARGS);
extern void SignalHandlerForShutdownRequest(SIGNAL_ARGS);
#endif
#if PG_VERSION_NUM < 90500
#include <storage/barrier.h>
#endif
#include <storage/proc.h>
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

typedef struct WorkShared {
    bool in_use;
    char data[NAMEDATALEN];
    char schema[NAMEDATALEN];
    char table[NAMEDATALEN];
    char user[NAMEDATALEN];
    int64 reset;
    int64 sleep;
    int run;
    Oid oid;
} WorkShared;

typedef struct Work {
    char *schema_table;
    char *schema_type;
    const char *columns;
    const char *data;
    const char *schema;
    const char *table;
    const char *user;
    dlist_node node;
    int hash;
    pid_t pid;
    WorkShared *shared;
} Work;

typedef struct TaskShared {
    bool in_use;
    int64 id;
    int hash;
    int max;
    int slot;
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
Datum SPI_getbinval_my(HeapTupleData *tuple, TupleDesc tupdesc, const char *fname, bool allow_null, Oid typeid);
int severity_error(const char *error);
PGDLLEXPORT void conf_main(Datum main_arg);
PGDLLEXPORT void task_main(Datum main_arg);
PGDLLEXPORT void work_main(Datum main_arg);
Portal SPI_cursor_open_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls);
Portal SPI_cursor_open_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void appendBinaryStringInfoEscapeQuote(StringInfoData *buf, const char *data, int len, bool string, char escape, char quote);
void append_with_tabs(StringInfo buf, const char *str);
void initStringInfoMy(StringInfoData *buf);
void init_conf(bool dynamic);
void _PG_init(void);
void SPI_connect_my(const char *src);
void SPI_cursor_close_my(Portal portal);
void SPI_cursor_fetch_my(const char *src, Portal portal, bool forward, long count);
void SPI_execute_plan_my(const char *src, SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_finish_my(void);
void task_catch(void);
void task_error(ErrorData *edata);
void task_execute(void);
void task_free(Task *t);
void taskshared_free(int slot);
void workshared_free(int slot);

#endif // _INCLUDE_H_
