MODULE_big = pg_task
OBJS = init.o conf.o tick.o task.o postgres.o spi.o
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
