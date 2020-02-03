#include <postgres.h>

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <commands/async.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>
#include <utils/varlena.h>

void SPI_connect_my(const char *command, const int timeout);
void SPI_finish_my(const char *command);
char *TextDatumGetCStringOrNULL(HeapTuple tuple, TupleDesc tupdesc, const char *fname, bool *isnull);
