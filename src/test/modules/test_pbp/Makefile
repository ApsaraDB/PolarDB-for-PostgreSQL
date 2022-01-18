# src/test/modules/test_pbp/Makefile

MODULE_big = test_pbp
OBJS = test_pbp.o $(WIN32RES)
PGFILEDESC = "test_pbp - test code for persisted buffer pool."

EXTENSION = test_pbp
DATA = test_pbp--1.0.sql
TEMP_CONFIG = "test_pbp.conf"
REGRESS = test_pbp

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pbp
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
