#!/bin/sh -ex

if [ -z $PG_BUILD_FROM_SOURCE ]; then
	GREENPLUM="$(postgres --version | grep -i Greenplum >/dev/null && echo yes || echo no)"
	PG_MAJOR="$(pg_config --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | head -1)"
	if [ "$GREENPLUM" = "yes" ]; then
		ARENADATA="$(gppkg --version | grep -i arenadata >/dev/null && echo yes || echo no)"
		MAIN=main
		if [ "$ARENADATA" = "yes" ]; then
			REPO=arenadata/gpdb
		else
			REPO=greenplum-db/gpdb
		fi
		REL="$(test "$PG_MAJOR" -lt 10 && gppkg --version | cut -f 3 -d ' ' | cut -f 1 -d '+' || echo "main")"
	else
		MAIN=master
		REPO=postgres/postgres
		PG_VERSION="$(pg_config --version | cut -f 2 -d ' ' | tr '.' '_' | sed 's/rc/_RC/' | sed 's/beta/_BETA/')"
		REL="$(test "$PG_MAJOR" -lt 10 && echo "REL$PG_VERSION" || echo "REL_$PG_VERSION")"
		STABLE="$(test "$PG_MAJOR" -lt 10 && echo "REL9_${PG_MAJOR}_STABLE" || echo "REL_${PG_MAJOR}_STABLE")"
	fi
else
	MAIN=master
	REL="$(test "$PG_MAJOR" -lt 10 && echo "REL9_${PG_MAJOR}_STABLE" || echo "REL_${PG_MAJOR}_STABLE")"
	REPO=postgres/postgres
fi
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
static bool xact_started = false;
static CachedPlanSource *unnamed_stmt_psrc = NULL;
EOF
(curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$REL/src/backend/tcop/postgres.c" || \
curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$STABLE/src/backend/tcop/postgres.c" || \
curl --no-progress-meter -fL "https://raw.githubusercontent.com/$REPO/$MAIN/src/backend/tcop/postgres.c") | pcregrep -M \
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
