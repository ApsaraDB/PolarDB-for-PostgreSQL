# contrib/polar_worker/Makefile

MODULE_big = polar_worker
OBJS = polar_worker.o

EXTENSION = polar_worker
DATA = polar_worker--1.0.sql

LDFLAGS_SL += $(filter -lm, $(LIBS))

NO_INSTALLCHECK = 1

SHLIB_LINK = $(libpq) -Wl,--as-needed  -L. -Wl,--as-needed  -L.

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/polar_worker
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
