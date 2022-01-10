# src/test/modules/test_px/Makefile
MODULE_big = test_px
OBJS = test_px.o \
	test_px_gpopt.o \
	test_px_gpopt_util.o \
	test_px_partition.o \
	test_adaptive_paging.o $(WIN32RES)
PGFILEDESC = "test_px - test code for Parallel Execution library"

EXTENSION = test_px
DATA = test_px--1.0.sql
TEMP_CONFIG = "test_px.conf"
REGRESS = test_px test_px_optimizer test_px_partition test_px_inject_error
NO_CHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_px
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
