# external/varbitx/Makefile

MODULE_big = varbitx
OBJS = varbitx.o

EXTENSION = varbitx
DATA = varbitx--1.0.sql varbitx--1.0--1.1.sql

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS = varbitx

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/varbitx
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
