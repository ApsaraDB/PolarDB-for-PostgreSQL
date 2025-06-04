MODULE_big = masking
OBJS = \
	$(WIN32RES) \
	masking_label.o \
	masking.o

EXTENSION = masking
DATA = masking--1.0.0.sql
PGFILEDESC = "masking for query"
REGRESS = masking

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/masking/masking.conf
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/masking
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
