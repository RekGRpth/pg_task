PG_CONFIG = pg_config

MODULE_big = pg_task

PG9495 = $(shell $(PG_CONFIG) --version | egrep " 9\.4| 9\.5" > /dev/null && echo yes || echo no)
ifeq ($(PG9495),yes)
work.o: latch.h
latch.h:
	wget -O $@ "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/include/storage/latch.h"
latch.c: latch.h
	wget -O $@ "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/storage/ipc/latch.c"
	sed -i 's/storage\/latch/latch/' $@
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o
EXTRA_CLEAN = postgres.c latch.c latch.h
PG_CFLAGS += -Wno-cpp
else
OBJS = init.o conf.o work.o task.o spi.o dest.o
EXTRA_CLEAN = postgres.c
endif

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
