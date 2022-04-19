PG_CONFIG = pg_config

PG_MAJOR = $(shell $(PG_CONFIG) --version | egrep -o "9\.[[:digit:]]" | head -1 | cut -f 2 -d .)
ifeq ($(PG_MAJOR),)
PG_MAJOR = $(shell $(PG_CONFIG) --version | egrep -o "1[[:digit:]]" | head -1)
postgres.o: postgres.$(PG_MAJOR).c
postgres.$(PG_MAJOR).c:
	wget -O postgres.$(PG_MAJOR).i https://raw.githubusercontent.com/postgres/postgres/REL_$(PG_MAJOR)_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.$(PG_MAJOR).i
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.$(PG_MAJOR).i
	sed -i 's/EndCommand/EndCommandMy/' postgres.$(PG_MAJOR).i
	sed -i 's/NullCommand/NullCommandMy/' postgres.$(PG_MAJOR).i
	sed -i 's/static bool xact_started/bool xact_started/' postgres.$(PG_MAJOR).i
	cat postgres.$(PG_MAJOR).i | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >$@
	rm postgres.$(PG_MAJOR).i
else
postgres.o: postgres.9.$(PG_MAJOR).c
postgres.9.$(PG_MAJOR).c:
	wget -O postgres.9.$(PG_MAJOR).i https://raw.githubusercontent.com/postgres/postgres/REL9_$(PG_MAJOR)_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.9.$(PG_MAJOR).i
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.9.$(PG_MAJOR).i
	sed -i 's/EndCommand/EndCommandMy/' postgres.9.$(PG_MAJOR).i
	sed -i 's/NullCommand/NullCommandMy/' postgres.9.$(PG_MAJOR).i
	sed -i 's/static bool xact_started/bool xact_started/' postgres.9.$(PG_MAJOR).i
	cat postgres.9.$(PG_MAJOR).i | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >$@
	rm postgres.9.$(PG_MAJOR).i
endif

EXTRA_CLEAN = postgres.*.c
MODULE_big = pg_task
OBJS = init.o conf.o work.o task.o postgres.o spi.o dest.o latch.o
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
