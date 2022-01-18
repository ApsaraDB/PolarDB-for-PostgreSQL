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

override CFLAGS := -Wall -O2 -I/usr/local/polarstore/pfsd/include $(CFLAGS)
override CPPFLAGS := -Wall -O2 -I/usr/local/polarstore/pfsd/include $(CPPFLAGS)
ifeq ($(enable_polar_arm),yes)
SHLIB_LINK = $(libpq) -Wl,--as-needed  -L/usr/local/polarstore/pfsd/lib/ -lpfsd -pthread
else
SHLIB_LINK = $(libpq) -Wl,--as-needed  -L/usr/local/polarstore/pfsd/lib/ -lpfsd
endif
