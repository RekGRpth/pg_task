PG_CONFIG = pg_config

GREENPLUM = $(shell postgres --version | grep -i Greenplum > /dev/null && echo yes || echo no)
PG_MAJOR = $(shell $(PG_CONFIG) --version | egrep -o "9\.[[:digit:]]" | head -1 | cut -f 2 -d .)

ifeq ($(PG_MAJOR),)
PG_MAJOR = $(shell $(PG_CONFIG) --version | egrep -o "1[[:digit:]]" | head -1)
ifeq ($(GREENPLUM),yes)
REL = main
else
REL = REL_$(PG_MAJOR)_STABLE
endif
else
ifeq ($(GREENPLUM),yes)
REL = 6X_STABLE
else
REL = REL9_$(PG_MAJOR)_STABLE
endif
endif

ifeq ($(GREENPLUM),yes)
REPO = greenplum-db/gpdb
else
REPO = postgres/postgres
endif

task.o: postgres.c

postgres.c:
	wget -O $@ https://raw.githubusercontent.com/$(REPO)/$(REL)/src/backend/tcop/postgres.c
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
