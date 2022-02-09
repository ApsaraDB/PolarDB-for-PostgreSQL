# contrib/polar_vfs/Makefile

MODULE_big = polar_vfs
OBJS = polar_vfs.o polar_io_stat.o polar_directio.o

ifeq ($(with_pfsd),yes)
OBJS += polar_pfsd.o
PG_CPPFLAGS = -Wall -O2 -I/usr/local/polarstore/pfsd/include
endif

EXTENSION = polar_vfs
DATA = polar_vfs--1.0.sql

LDFLAGS_SL += $(filter -lm, $(LIBS))

NO_INSTALLCHECK = 1

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
ifeq ($(with_pfsd),yes)
SHLIB_LINK += -L/usr/local/polarstore/pfsd/lib/ -lpfsd
endif
