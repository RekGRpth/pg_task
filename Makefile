MODULE_big = pg_task
OBJS = init.o conf.o work.o task.o postgres.o spi.o dest.o fe-exec.o stringinfo.o varlena.o
PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(libpq_srcdir) -lm
PG_LIBS = $(libpq_pgport)
SHLIB_LINK = $(libpq)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
