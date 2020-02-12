#include <postgres.h>

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <libpq/libpq-be.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/pmsignal.h>
#include <storage/procarray.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/timeout.h>
#include <utils/varlena.h>

void RegisterDynamicBackgroundWorker_my(BackgroundWorker *worker);
void SPI_commit_my(const char *command);
void SPI_rollback_my(const char *command);
void SPI_start_my(const char *command, const int timeout);
void tick_init(const bool conf, const char *data, const char *user, const char *schema, const char *table, long period);
void tick_loop(void);
