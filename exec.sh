#!/bin/sh -ex

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
#ifdef GP_VERSION_NUM
#include "utils/faultinjector.h"
#endif
#if PG_VERSION_NUM >= 110000 && PG_VERSION_NUM < 130000
static bool stmt_timeout_active = false;
#endif
static bool xact_started = false;
static CachedPlanSource *unnamed_stmt_psrc = NULL;
EOF
cat postgres.c | pcregrep -M \
-e '(?s)^static bool CheckDebugDtmActionSqlCommandTag\(.*?\);' \
-e '(?s)^static void enable_statement_timeout\(.*?\);' \
-e '(?s)^static void start_xact_command\(.*?\);' \
-e '(?s)^static void drop_unnamed_stmt\(.*?\);' \
-e '(?s)^static bool check_log_statement\(.*?\);' \
-e '(?s)^static int\N+errdetail_execute\(.*?\);' \
-e '(?s)^static bool IsTransactionExitStmt\(.*?\);' \
-e '(?s)^static int\N+errdetail_abort\(.*?\);' \
-e '(?s)^static void disable_statement_timeout\(.*?\);' \
-e '(?s)^static void finish_xact_command\(.*?\);' \
-e '(?s)^static void exec_simple_query\(.*?\);' \
-e '(?s)^static bool\n^CheckDebugDtmActionSqlCommandTag\(.*?^}' \
-e '(?s)^static void\n^enable_statement_timeout\(.*?^}' \
-e '(?s)^static void\n^start_xact_command\(.*?^}' \
-e '(?s)^static void\n^drop_unnamed_stmt\(.*?^}' \
-e '(?s)^static bool\n^check_log_statement\(.*?^}' \
-e '(?s)^static int\n^errdetail_execute\(.*?^}' \
-e '(?s)^static bool\n^IsTransactionExitStmt\(.*?^}' \
-e '(?s)^static int\n^errdetail_abort\(.*?^}' \
-e '(?s)^static void\n^disable_statement_timeout\(.*?^}' \
-e '(?s)^static void\n^finish_xact_command\(.*?^}' \
-e '(?s)^static void\nexec_simple_query\(.*?^}' \
- | sed \
-e 's/TRACE_POSTGRESQL_QUERY_/\/\/TRACE_POSTGRESQL_QUERY_/' \
-e 's/BeginCommand/BeginCommandMy/' \
-e 's/CreateDestReceiver/CreateDestReceiverMy/' \
-e 's/EndCommand/EndCommandMy/' \
-e 's/NullCommand/NullCommandMy/'
cat <<EOF
void exec_simple_query_my(const char *query_string) {
    exec_simple_query(query_string);
}
void xact_started_my(bool value) {
    xact_started = value;
}
EOF
