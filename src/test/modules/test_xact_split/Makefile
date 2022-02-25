# src/test/modules/test_xact_split/Makefile

MODULE_big = test_xact_split
OBJS = test_xact_split.o $(WIN32RES)
PGFILEDESC = "test_xact_split - test code for xact split"

EXTENSION = test_xact_split
DATA = test_xact_split--1.0.sql
REGRESS = test_xact_split
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_xact_split
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
