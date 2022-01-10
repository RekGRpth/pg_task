MODULE_big = pg_task
OBJS = init.o conf.o work.o task.o postgres.o spi.o dest.o latch.o
PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(libpq_srcdir)
PGXS = $(shell $(PG_CONFIG) --pgxs)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --use-existing
SHLIB_LINK = $(libpq)
TESTS = $(wildcard sql/*.sql)
include $(PGXS)
