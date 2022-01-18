# external/polar_tde_utils/Makefile

MODULE_big = polar_tde_utils
OBJS = polar_tde_utils.o

EXTENSION = polar_tde_utils
DATA = polar_tde_utils--1.0.sql

REGRESS = update_kmgr_file
NO_CHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
