# src/test/modules/test_async_ddl_lock_replay/Makefile

MODULE_big = test_async_ddl_lock_replay
OBJS = test_async_ddl_lock_replay.o $(WIN32RES)
PGFILEDESC = "test_async_ddl_lock_replay - test code for wal prefetch feature"

EXTENSION = test_async_ddl_lock_replay
DATA = test_async_ddl_lock_replay--1.0.sql
REGRESS = test_async_ddl_lock_replay

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_async_ddl_lock_replay
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
