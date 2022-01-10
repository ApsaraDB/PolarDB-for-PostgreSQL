# src/test/modules/test_cancel_key/Makefile

MODULE_big = test_cancel_key
OBJS = test_cancel_key.o $(WIN32RES)
PGFILEDESC = "test_cancel_key - test code for cancel key feature"

EXTENSION = test_cancel_key
DATA = test_cancel_key--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_cancel_key
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: all
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/schedule $(EXTRA_TESTS)
