postgres.o: postgres.10.c postgres.11.c postgres.12.c postgres.13.c postgres.14.c postgres.9.4.c postgres.9.5.c postgres.9.6.c

postgres.%.c: postgres.%.i
	sed -i 's/BeginCommand/BeginCommandMy/' $<
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' $<
	sed -i 's/EndCommand/EndCommandMy/' $<
	sed -i 's/exec_simple_query/exec_simple_query_my/' $<
	sed -i 's/NullCommand/NullCommandMy/' $<
	sed -i 's/static bool xact_started/bool xact_started/' $<
	cat $< | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >$@
	rm $<

postgres.10.i:
	wget -O postgres.10.i https://raw.githubusercontent.com/postgres/postgres/REL_10_STABLE/src/backend/tcop/postgres.c

postgres.11.i:
	wget -O postgres.11.i https://raw.githubusercontent.com/postgres/postgres/REL_11_STABLE/src/backend/tcop/postgres.c

postgres.12.i:
	wget -O postgres.12.i https://raw.githubusercontent.com/postgres/postgres/REL_12_STABLE/src/backend/tcop/postgres.c

postgres.13.i:
	wget -O postgres.13.i https://raw.githubusercontent.com/postgres/postgres/REL_13_STABLE/src/backend/tcop/postgres.c

postgres.14.i:
	wget -O postgres.14.i https://raw.githubusercontent.com/postgres/postgres/REL_14_STABLE/src/backend/tcop/postgres.c

postgres.9.4.i:
	wget -O postgres.9.4.i https://raw.githubusercontent.com/postgres/postgres/REL9_4_STABLE/src/backend/tcop/postgres.c

postgres.9.5.i:
	wget -O postgres.9.5.i https://raw.githubusercontent.com/postgres/postgres/REL9_5_STABLE/src/backend/tcop/postgres.c

postgres.9.6.i:
	wget -O postgres.9.6.i https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/tcop/postgres.c

EXTRA_CLEAN = postgres.10.c postgres.11.c postgres.12.c postgres.13.c postgres.14.c postgres.9.4.c postgres.9.5.c postgres.9.6.c
MODULE_big = pg_task
OBJS = init.o conf.o work.o task.o postgres.o spi.o dest.o latch.o
PG_CONFIG = pg_config
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
