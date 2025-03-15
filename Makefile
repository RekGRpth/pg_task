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

dest.o: exec_simple_query.c

postgres.c:
	curl --no-progress-meter -fOL "https://raw.githubusercontent.com/$(REPO)/$(REL)/src/backend/tcop/postgres.c" || curl --no-progress-meter -fOL "https://raw.githubusercontent.com/$(REPO)/$(STABLE)/src/backend/tcop/postgres.c" || curl --no-progress-meter -fOL "https://raw.githubusercontent.com/$(REPO)/$(MAIN)/src/backend/tcop/postgres.c"

.ONESHELL:
exec_simple_query.c: postgres.c
	cat >$@.i <<EOF
	#include <access/printtup.h>
	#include <commands/prepare.h>
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
	pcregrep -M '(?s)^static void\n^enable_statement_timeout\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static void\n^start_xact_command\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static void\n^drop_unnamed_stmt\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static bool\n^check_log_statement\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static int\n^errdetail_execute\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static bool\n^IsTransactionExitStmt\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static int\n^errdetail_abort\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static void\n^disable_statement_timeout\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static void\n^finish_xact_command\(.*?^}' postgres.c >>$@.i
	pcregrep -M '(?s)^static void\n^exec_simple_query\(.*?^}' postgres.c >>$@.i
	sed -i 's/TRACE_POSTGRESQL_QUERY_/\/\/TRACE_POSTGRESQL_QUERY_/' $@.i
	sed -i 's/BeginCommand/BeginCommandMy/' $@.i
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' $@.i
	sed -i 's/EndCommand/EndCommandMy/' $@.i
	sed -i 's/NullCommand/NullCommandMy/' $@.i
	mv -f $@.i $@

MODULE_big = pg_task

PG9495 = $(shell $(PG_CONFIG) --version | grep -E " 9\.4| 9\.5" > /dev/null && echo yes || echo no)
ifeq ($(PG9495),yes)
work.o: latch.h
latch.h:
	curl --no-progress-meter -fOL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/include/storage/latch.h"
latch.c: latch.h
	curl --no-progress-meter -fOL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/storage/ipc/latch.c"
	sed -i 's/storage\/latch/latch/' $@
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o
EXTRA_CLEAN = exec_simple_query.c exec_simple_query.i postgres.c latch.c latch.h
PG_CFLAGS += -Wno-cpp
else
OBJS = init.o conf.o work.o task.o spi.o dest.o
EXTRA_CLEAN = exec_simple_query.c exec_simple_query.i postgres.c
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
