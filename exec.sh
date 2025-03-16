#!/bin/sh -eux

(
cat <<EOF
#include "include.h"
#include <access/printtup.h>
#include <access/xact.h>
#include <commands/prepare.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <pgstat.h>
#include <replication/slot.h>
#include <storage/proc.h>
#include <tcop/pquery.h>
#include <tcop/tcopprot.h>
#include <tcop/utility.h>
#include <utils/guc.h>
#include <utils/memutils.h>
#include <utils/ps_status.h>
#include <utils/snapmgr.h>
#include <utils/timeout.h>
#if PG_VERSION_NUM < 90500
#define PQArgBlock undef
#endif
#if PG_VERSION_NUM >= 110000
#include <jit/jit.h>
#endif
#if PG_VERSION_NUM >= 140000
#include <utils/backend_status.h>
#endif
#if PG_VERSION_NUM >= 110000 && PG_VERSION_NUM < 130000
static bool stmt_timeout_active = false;
#endif
bool xact_started = false;
static CachedPlanSource *unnamed_stmt_psrc = NULL;
EOF
pcregrep -M '(?s)^static void\n^enable_statement_timeout\(.*?^}' postgres.c
pcregrep -M '(?s)^static void\n^start_xact_command\(.*?^}' postgres.c
pcregrep -M '(?s)^static void\n^drop_unnamed_stmt\(.*?^}' postgres.c
pcregrep -M '(?s)^static bool\n^check_log_statement\(.*?^}' postgres.c
pcregrep -M '(?s)^static int\n^errdetail_execute\(.*?^}' postgres.c
pcregrep -M '(?s)^static bool\n^IsTransactionExitStmt\(.*?^}' postgres.c
pcregrep -M '(?s)^static int\n^errdetail_abort\(.*?^}' postgres.c
pcregrep -M '(?s)^static void\n^disable_statement_timeout\(.*?^}' postgres.c
pcregrep -M '(?s)^static void\n^finish_xact_command\(.*?^}' postgres.c
echo "void"
pcregrep -M '(?s)^exec_simple_query\(.*?^}' postgres.c
) | \
sed 's/TRACE_POSTGRESQL_QUERY_/\/\/TRACE_POSTGRESQL_QUERY_/' | \
sed 's/BeginCommand/BeginCommandMy/' | \
sed 's/CreateDestReceiver/CreateDestReceiverMy/' | \
sed 's/EndCommand/EndCommandMy/' | \
sed 's/NullCommand/NullCommandMy/'
