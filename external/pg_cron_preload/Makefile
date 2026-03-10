# external/pg_cron_preload/Makefile

MODULE_big = pg_cron_preload
OBJS = invalid_cache.o $(WIN32RES)

EXTENSION = pg_cron_preload
DATA = pg_cron_preload--1.0.sql
PGFILEDESC = "pg_cron_preload - polardb catalog extersions"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/pg_cron_preload
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
