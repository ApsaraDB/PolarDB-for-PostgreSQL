# contrib/polar_sys_stats/Makefile

MODULE_big = polar_sys_stats
OBJS = polar_sys_stats.o

EXTENSION = polar_sys_stats
DATA = polar_sys_stats--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

SHLIB_LINK=$(libpq) -Wl,--as-needed
ifeq ($(host_cpu),aarch64)
SHLIB_LINK += -pthread
endif
