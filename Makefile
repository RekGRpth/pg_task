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
# Greengage's postgres executable carries its own backend build of libpq for the QD->QE dispatch and exports its symbols,
# which win over libpq.so at load time, and that copy speaks the internal protocol, which pg_hba.conf lets through unchecked,
# so link a private copy of the frontend libpq.a instead, with only its api left global and then hidden inside pg_task.so
GREENGAGE = $(shell grep -q GPDB_INTERNAL_PROTOCOL $(shell $(PG_CONFIG) --includedir-server)/libpq/pqcomm.h 2>/dev/null && echo yes || echo no)
ifeq ($(GREENGAGE),yes)
LIBPQ_DIR = $(shell $(PG_CONFIG) --libdir)
OBJCOPY ?= objcopy
SHLIB_LINK = -Wl,--exclude-libs=libpq_my.a libpq_my.a $(filter -lcrypt -ldes -lcom_err -lcrypto -lk5crypto -lkrb5 -lgssapi_krb5 -lgss -lgssapi -lssl -lsocket -lnsl -lresolv -lintl -lm, $(LIBS)) $(LDAP_LIBS_FE) $(PTHREAD_LIBS)
EXTRA_CLEAN += libpq_my.a libpq_my.o libpq_my.sym
else
SHLIB_LINK = $(libpq)
endif
TESTS = $(wildcard sql/*.sql)
EXTRA_CLEAN += test.conf
include $(PGXS)
.DEFAULT_GOAL := all
ifeq ($(GREENGAGE),yes)
$(shlib): libpq_my.a
# pqsignal() and pg_*() are exported by libpq.so too, but pg_task must keep taking its own calls of them from the backend
libpq_my.sym: $(LIBPQ_DIR)/libpq.so
	nm -D --defined-only $< | awk '{print $$3}' | sed -e 's/@.*//' | grep -v -E '^(pqsignal|pg_)' | sort -u >$@
# only the members the api needs, the frontend libpgcommon and libpgport of the newer versions included, all of them local except the api
libpq_my.o: libpq_my.sym $(LIBPQ_DIR)/libpq.a
	$(LD) -r -o $@ $$(sed -e 's/^/-u /' libpq_my.sym) --start-group $(LIBPQ_DIR)/libpq.a $(wildcard $(LIBPQ_DIR)/libpgcommon_shlib.a $(LIBPQ_DIR)/libpgport_shlib.a) --end-group
	$(OBJCOPY) --keep-global-symbols=libpq_my.sym $@
libpq_my.a: libpq_my.o
	rm -f $@ && $(AR) crs $@ $<
endif
.PHONY: test.conf
test.conf:
	echo "shared_preload_libraries = 'pg_task'" >$@
	echo "max_worker_processes = 20" >>$@
	echo "pg_task.json = '[{\"data\":\"$(CONTRIB_TESTDB)\",\"user\":\"$(PG_TASK_TEST_USER)\"}]'" >>$@
installcheck: test.conf
check: test.conf
