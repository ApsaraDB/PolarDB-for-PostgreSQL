# src/test/modules/test_logindex/Makefile

MODULE_big = test_logindex
OBJS = test_logindex.o $(WIN32RES)
PGFILEDESC = "test_logindex - test code for log index library"

EXTENSION = test_logindex
DATA = test_logindex--1.0.sql
TEMP_CONFIG = "test_logindex.conf"
REGRESS = test_logindex \
	  test_fullpage_logindex

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_logindex
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
