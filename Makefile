PG_CONFIG = pg_config

ifeq ($(PG_BUILD_FROM_SOURCE),)
	GREENPLUM = $(shell postgres --version | grep -i Greenplum >/dev/null && echo yes || echo no)
	PG_MAJOR = $(shell $(PG_CONFIG) --version | cut -f 2 -d ' ' | egrep -o "[[:digit:]]+" | head -1)
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
		PG_VERSION = $(shell $(PG_CONFIG) --version | cut -f 2 -d ' ' | tr '.' '_')
		REL = $(shell test "$(PG_MAJOR)" -lt 10 && echo "REL$(PG_VERSION)" || echo "REL_$(PG_VERSION)")
	endif
else
	MAIN = master
	REL = $(shell test "$(PG_MAJOR)" -lt 10 && echo "REL9_$(PG_MAJOR)_STABLE" || echo "REL_$(PG_MAJOR)_STABLE")
	REPO = postgres/postgres
endif

task.o: postgres.c

postgres.c:
	wget -O $@ "https://raw.githubusercontent.com/$(REPO)/$(REL)/src/backend/tcop/postgres.c" | wget -O $@ "https://raw.githubusercontent.com/$(REPO)/$(MAIN)/src/backend/tcop/postgres.c"
	sed -i 's/TRACE_POSTGRESQL_QUERY_/\/\/TRACE_POSTGRESQL_QUERY_/' $@
	sed -i 's/BeginCommand/BeginCommandMy/' $@
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' $@
	sed -i 's/EndCommand/EndCommandMy/' $@
	sed -i 's/NullCommand/NullCommandMy/' $@

EXTRA_CLEAN = postgres.c
MODULE_big = pg_task
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o
PG94 = $(shell $(PG_CONFIG) --version | egrep " 8\.| 9\.0| 9\.1| 9\.2| 9\.3" > /dev/null && echo no || echo yes)
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
