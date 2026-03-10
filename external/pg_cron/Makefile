# src/test/modules/pg_cron/Makefile

EXTENSION = pg_cron

DATA_built = $(EXTENSION)--1.0.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

# Temporarily return through superuser, and open to ordinary users after the privilege is upgraded once
REGRESS = pg_cron-test 
ORA_REGRESS = pg_cron-test 

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
PG_CPPFLAGS = -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)

SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/pg_cron
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@
