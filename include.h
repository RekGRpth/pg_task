#include <postgres.h>

#include <access/printtup.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <commands/prepare.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <libpq/libpq-be.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/analyze.h>
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
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/timeout.h>
#include <utils/varlena.h>

DestReceiver *CreateDestReceiverMy(CommandDest dest);
SPIPlanPtr SPI_prepare_my(const char *src, int nargs, Oid *argtypes);
void exec_simple_query(const char *query_string);
void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker);
void SPI_begin_my(const char *command);
void SPI_commit_my(const char *command);
void SPI_execute_plan_my(SPIPlanPtr plan, Datum *values, const char *nulls, int res);
void SPI_execute_with_args_my(const char *src, int nargs, Oid *argtypes, Datum *values, const char *nulls, int res);
void SPI_rollback_my(const char *command);
void tick_init(const bool conf, const char *data, const char *user, const char *schema, const char *table, int period);
void tick_loop(void);

#define Q(name) #name
#define S(macro) Q(macro)
