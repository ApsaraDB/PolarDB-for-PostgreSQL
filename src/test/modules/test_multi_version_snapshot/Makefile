# src/test/modules/test_multi_version_snapshot/Makefile

MODULE_big = test_multi_version_snapshot
OBJS = test_multi_version_snapshot.o $(WIN32RES)
PGFILEDESC = "test_multi_version_snapshot - test code for multi version snapshot library"

EXTENSION = test_multi_version_snapshot
DATA = test_multi_version_snapshot--1.0.sql
TEMP_CONFIG = "test_multi_version_snapshot.conf"
REGRESS = test_multi_version_snapshot

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_multi_version_snapshot
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif