# src/test/modules/test_ringbuf/Makefile

MODULE_big = test_ringbuf
OBJS = test_ringbuf.o $(WIN32RES)
PGFILEDESC = "test_ringbuf - test code for ringbuf"

EXTENSION = test_ringbuf
DATA = test_ringbuf--1.0.sql

REGRESS = test_ringbuf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_ringbuf
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
