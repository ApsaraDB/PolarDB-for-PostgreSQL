# src/test/modules/test_logindex/Makefile

MODULE_big = test_page_outdate
OBJS = test_page_outdate.o $(WIN32RES)
PGFILEDESC = "test_page_outdate - test code for page outdate feature"

EXTENSION = test_page_outdate
DATA = test_page_outdate--1.0.sql
TEMP_CONFIG = "test_page_outdate.conf"
REGRESS = test_page_outdate

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_page_outdate
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
