postgres.o: postgres.10.c postgres.11.c postgres.12.c postgres.13.c postgres.14.c postgres.9.4.c postgres.9.5.c postgres.9.6.c

postgres.10.c:
	wget -O postgres.10.c https://raw.githubusercontent.com/postgres/postgres/REL_10_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.10.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.10.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.10.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.10.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.10.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.10.c
	cat postgres.10.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.10.c.my
	mv -f postgres.10.c.my postgres.10.c

postgres.11.c:
	wget -O postgres.11.c https://raw.githubusercontent.com/postgres/postgres/REL_11_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.11.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.11.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.11.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.11.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.11.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.11.c
	cat postgres.11.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.11.c.my
	mv -f postgres.11.c.my postgres.11.c

postgres.12.c:
	wget -O postgres.12.c https://raw.githubusercontent.com/postgres/postgres/REL_12_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.12.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.12.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.12.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.12.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.12.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.12.c
	cat postgres.12.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.12.c.my
	mv -f postgres.12.c.my postgres.12.c

postgres.13.c:
	wget -O postgres.13.c https://raw.githubusercontent.com/postgres/postgres/REL_13_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.13.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.13.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.13.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.13.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.13.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.13.c
	cat postgres.13.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.13.c.my
	mv -f postgres.13.c.my postgres.13.c

postgres.14.c:
	wget -O postgres.14.c https://raw.githubusercontent.com/postgres/postgres/REL_14_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.14.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.14.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.14.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.14.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.14.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.14.c
	cat postgres.14.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.14.c.my
	mv -f postgres.14.c.my postgres.14.c

postgres.9.4.c:
	wget -O postgres.9.4.c https://raw.githubusercontent.com/postgres/postgres/REL9_4_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.9.4.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.9.4.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.9.4.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.9.4.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.9.4.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.9.4.c
	cat postgres.9.4.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.9.4.c.my
	mv -f postgres.9.4.c.my postgres.9.4.c

postgres.9.5.c:
	wget -O postgres.9.5.c https://raw.githubusercontent.com/postgres/postgres/REL9_5_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.9.5.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.9.5.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.9.5.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.9.5.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.9.5.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.9.5.c
	cat postgres.9.5.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.9.5.c.my
	mv -f postgres.9.5.c.my postgres.9.5.c

postgres.9.6.c:
	wget -O postgres.9.6.c https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/backend/tcop/postgres.c
	sed -i 's/BeginCommand/BeginCommandMy/' postgres.9.6.c
	sed -i 's/CreateDestReceiver/CreateDestReceiverMy/' postgres.9.6.c
	sed -i 's/EndCommand/EndCommandMy/' postgres.9.6.c
	sed -i 's/exec_simple_query/exec_simple_query_my/' postgres.9.6.c
	sed -i 's/NullCommand/NullCommandMy/' postgres.9.6.c
	sed -i 's/static bool xact_started/bool xact_started/' postgres.9.6.c
	cat postgres.9.6.c | tr '\n' '\f' | sed 's/static void\fexec_simple_query/void exec_simple_query/' | tr '\f' '\n' >postgres.9.6.c.my
	mv -f postgres.9.6.c.my postgres.9.6.c

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
