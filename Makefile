MODULE_big = pg_task
EXTRA_CLEAN = postgres.c exec.c latch.c latch.h latch.o latch_my.h
PG_CONFIG = pg_config
postgres.c:
	./postgres.sh >$@
exec.c: postgres.c
	./exec.sh >$@
PG9495 = $(shell $(PG_CONFIG) --version | grep -E " 9\.4| 9\.5" > /dev/null && echo yes || echo no)
ifeq ($(PG9495),yes)
work.o: latch_my.h
latch_my.h: latch.h
	./latch.sh >$@
latch.h:
	curl --no-progress-meter -fL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/include/storage/latch.h" | sed -e 's/InitializeLatchSupport/InitializeLatchSupportMy/' >$@
latch.c: latch.h
	curl --no-progress-meter -fL "https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/storage/ipc/latch.c" | sed -e 's/storage\/latch/latch/' -e 's/InitializeLatchSupport/InitializeLatchSupportMy/' >$@
OBJS = init.o conf.o work.o task.o spi.o dest.o latch.o exec.o make.o
PG_CFLAGS += -Wno-cpp
else
OBJS = init.o conf.o work.o task.o spi.o dest.o exec.o make.o
endif
PG94 = $(shell $(PG_CONFIG) --version | grep -E " 8\.| 9\.0| 9\.1| 9\.2| 9\.3" > /dev/null && echo no || echo yes)
ifeq ($(PG94),no)
	$(error Minimum version of PostgreSQL required is 9.4.0)
endif
PG_CPPFLAGS = -I$(libpq_srcdir)
HAVE_CREATING_EXTENSION_LOCAL = $(shell grep -q creating_extension_local $(shell $(PG_CONFIG) --includedir-server)/commands/extension.h 2>/dev/null && echo yes || echo no)
ifeq ($(HAVE_CREATING_EXTENSION_LOCAL),yes)
PG_CPPFLAGS += -DHAVE_CREATING_EXTENSION_LOCAL
endif
PGXS = $(shell $(PG_CONFIG) --pgxs)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))
PG_TASK_TEST_INSTANCE ?= temp
PG_TASK_TEST_USER ?= $(shell whoami)
ifeq ($(PG_TASK_TEST_INSTANCE),existing)
REGRESS_OPTS = --use-existing --user=$(PG_TASK_TEST_USER)
else
REGRESS_OPTS = --temp-config=./test.conf --temp-instance=./tmp_check --user=$(PG_TASK_TEST_USER)
endif
SHLIB_LINK = $(libpq)
TESTS = $(wildcard sql/*.sql)
EXTRA_CLEAN += test.conf
include $(PGXS)
.DEFAULT_GOAL := all
.PHONY: test.conf
test.conf:
	echo "shared_preload_libraries = 'pg_task'" >$@
	echo "max_worker_processes = 20" >>$@
	echo "pg_task.json = '[{\"data\":\"$(CONTRIB_TESTDB)\",\"user\":\"$(PG_TASK_TEST_USER)\"}]'" >>$@
installcheck: test.conf
check: test.conf
