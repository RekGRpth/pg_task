MODULE_big = pg_task
EXTRA_CLEAN = postgres.c latch.c latch.h latch.o latch_my.h
PG_CONFIG = pg_config
postgres.c:
	./postgres.sh >$@
PG9495 = $(shell $(PG_CONFIG) --version | grep -E " 9\.4| 9\.5" > /dev/null && echo yes || echo no)
ifeq ($(PG9495),yes)
work.o: latch_my.h
latch_my.h: latch.h
	./latch.sh >$@
latch.h:
	curl --no-progress-meter -fL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/include/storage/latch.h" | sed -e 's/InitializeLatchSupport/InitializeLatchSupportMy/' >$@
latch.c: latch.h
	curl --no-progress-meter -fL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/storage/ipc/latch.c" | sed -e 's/storage\/latch/latch/' -e 's/InitializeLatchSupport/InitializeLatchSupportMy/' >$@
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o postgres.o
PG_CFLAGS += -Wno-cpp
else
OBJS = init.o conf.o work.o task.o spi.o dest.o postgres.o
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
