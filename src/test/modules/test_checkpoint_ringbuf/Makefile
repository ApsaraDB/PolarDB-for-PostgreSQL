# src/test/modules/test_checkpoint_ringbuf/Makefile

MODULE_big = test_checkpoint_ringbuf
OBJS = test_checkpoint_ringbuf.o $(WIN32RES)
PGFILEDESC = "test_checkpoint_ringbuf - test code for checkpoint ringbuf"

EXTENSION = test_checkpoint_ringbuf
DATA = test_checkpoint_ringbuf--1.0.sql
REGRESS = test_checkpoint_ringbuf 

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_checkpoint_ringbuf
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
