MODULE_big = pg_task
OBJS = init.o conf.o tick.o task.o postgres.o spi.o lockfuncs.o guc.o latch.o queue.o dest.o
PG_CONFIG = pg_config
SHLIB_LINK = $(libpq)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
