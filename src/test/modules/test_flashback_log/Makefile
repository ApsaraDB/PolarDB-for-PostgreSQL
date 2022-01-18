# src/test/modules/test_flashback_log/Makefile

MODULE_big = test_flashback_log
OBJS = test_flashback_log.o $(WIN32RES)
PGFILEDESC = "test_flashback_log - test code for flashback log feature"

EXTENSION = test_flashback_log
DATA = test_flashback_log--1.0.sql
TEMP_CONFIG = "test_flashback_log.conf"
REGRESS = test_flashback_log

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_flashback_log
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
