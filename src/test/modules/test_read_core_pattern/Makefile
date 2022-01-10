# src/test/modules/test_read_core_pattern/Makefile

MODULE_big = test_read_core_pattern
OBJS = test_read_core_pattern.o $(WIN32RES)
PGFILEDESC = "test_read_core_pattern - test code for slru"

EXTENSION = test_read_core_pattern
DATA = test_read_core_pattern--1.0.sql

REGRESS = test_read_core_pattern

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_read_core_pattern
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
