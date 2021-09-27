# contrib/polar_vfs/Makefile

MODULE_big = polar_vfs
OBJS = polar_vfs.o polar_directio.o polar_io_stat.o

EXTENSION = polar_vfs
DATA = polar_vfs--1.0.sql

LDFLAGS_SL += $(filter -lm, $(LIBS))

NO_INSTALLCHECK = 1

ifeq ($(with_pfsd),yes)
OBJS += polar_pfsd.o

override CFLAGS := -Wall -O2 -I/usr/local/polarstore/pfsd/include $(CFLAGS)
override CPPFLAGS := -Wall -O2 -I/usr/local/polarstore/pfsd/include $(CPPFLAGS)
SHLIB_LINK = $(libpq) -Wl,--as-needed  -L/usr/local/polarstore/pfsd/lib/ -lpfsd
endif # with_pfsd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_pfs
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
