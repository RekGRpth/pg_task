PG_CONFIG = pg_config

ifeq ($(PG_BUILD_FROM_SOURCE),)
	GREENPLUM = $(shell postgres --version | grep -i Greenplum >/dev/null && echo yes || echo no)
	PG_MAJOR = $(shell $(PG_CONFIG) --version | cut -f 2 -d ' ' | grep -E -o "[[:digit:]]+" | head -1)
	ifeq ($(GREENPLUM),yes)
		ARENADATA = $(shell gppkg --version | grep -i arenadata >/dev/null && echo yes || echo no)
		MAIN = main
		ifeq ($(ARENADATA),yes)
			REPO = arenadata/gpdb
		else
			REPO = greenplum-db/gpdb
		endif
		REL = $(shell test "$(PG_MAJOR)" -lt 10 && gppkg --version | cut -f 3 -d ' ' | cut -f 1 -d '+' || echo "main")
	else
		MAIN = master
		REPO = postgres/postgres
		PG_VERSION = $(shell $(PG_CONFIG) --version | cut -f 2 -d ' ' | tr '.' '_' | sed 's/rc/_RC/' | sed 's/beta/_BETA/')
		REL = $(shell test "$(PG_MAJOR)" -lt 10 && echo "REL$(PG_VERSION)" || echo "REL_$(PG_VERSION)")
		STABLE = $(shell test "$(PG_MAJOR)" -lt 10 && echo "REL9_$(PG_MAJOR)_STABLE" || echo "REL_$(PG_MAJOR)_STABLE")
	endif
else
	MAIN = master
	REL = $(shell test "$(PG_MAJOR)" -lt 10 && echo "REL9_$(PG_MAJOR)_STABLE" || echo "REL_$(PG_MAJOR)_STABLE")
	REPO = postgres/postgres
endif

postgres.c:
	curl --no-progress-meter -OL "https://raw.githubusercontent.com/$(REPO)/$(REL)/src/backend/tcop/postgres.c" || \
	curl --no-progress-meter -OL "https://raw.githubusercontent.com/$(REPO)/$(STABLE)/src/backend/tcop/postgres.c" || \
	curl --no-progress-meter -OL "https://raw.githubusercontent.com/$(REPO)/$(MAIN)/src/backend/tcop/postgres.c"

.ONESHELL:
exec.c: postgres.c
	cat >$@ <<EOF
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
	pcregrep -M '(?s)^static void\n^enable_statement_timeout\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static void\n^start_xact_command\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static void\n^drop_unnamed_stmt\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static bool\n^check_log_statement\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static int\n^errdetail_execute\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static bool\n^IsTransactionExitStmt\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static int\n^errdetail_abort\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static void\n^disable_statement_timeout\(.*?^}' postgres.c >>$@
	pcregrep -M '(?s)^static void\n^finish_xact_command\(.*?^}' postgres.c >>$@
	echo "void" >>$@
	pcregrep -M '(?s)^exec_simple_query\(.*?^}' postgres.c >>$@
	sed -i 's/TRACE_POSTGRESQL_QUERY_/\/\/TRACE_POSTGRESQL_QUERY_/' $@
	sed -i 's/BeginCommand/BeginCommandMy/' $@
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' $@
	sed -i 's/EndCommand/EndCommandMy/' $@
	sed -i 's/NullCommand/NullCommandMy/' $@

MODULE_big = pg_task

PG9495 = $(shell $(PG_CONFIG) --version | grep -E " 9\.4| 9\.5" > /dev/null && echo yes || echo no)
ifeq ($(PG9495),yes)
work.o: latch.h
latch.h:
	curl --no-progress-meter -OL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/include/storage/latch.h"
latch.c: latch.h
	curl --no-progress-meter -OL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/storage/ipc/latch.c"
	sed -i 's/storage\/latch/latch/' $@
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o exec.o
EXTRA_CLEAN = exec.c postgres.c latch.c latch.h
PG_CFLAGS += -Wno-cpp
else
OBJS = init.o conf.o work.o task.o spi.o dest.o exec.o
EXTRA_CLEAN = exec.c postgres.c
endif

PG94 = $(shell $(PG_CONFIG) --version | grep -E " 8\.| 9\.0| 9\.1| 9\.2| 9\.3" > /dev/null && echo no || echo yes)
ifeq ($(PG94),no)
	$(error Minimum version of PostgreSQL required is 9.4.0)
endif
PG_CPPFLAGS = -I$(libpq_srcdir)
PGXS = $(shell $(PG_CONFIG) --pgxs)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --use-existing
SHLIB_LINK = $(libpq)
TESTS = $(wildcard sql/*.sql)
include $(PGXS)
