# contrib/polar_vfs/Makefile

MODULE_big = polar_vfs
OBJS = polar_vfs.o polar_io_stat.o polar_directio.o polar_pfsd.o

EXTENSION = polar_vfs
DATA = polar_vfs--1.0.sql

LDFLAGS_SL += $(filter -lm, $(LIBS))

NO_INSTALLCHECK = 1

PG_CPPFLAGS = -Wall -O2 -I/usr/local/polarstore/pfsd/include

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(enable_polar_arm),yes)
SHLIB_LINK = $(libpq) -Wl,--as-needed  -L/usr/local/polarstore/pfsd/lib/ -lpfsd -pthread
else
SHLIB_LINK = $(libpq) -Wl,--as-needed  -L/usr/local/polarstore/pfsd/lib/ -lpfsd
endif
