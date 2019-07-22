# external/polar_monitor/Makefile

MODULE_big = polar_monitor
OBJS = polar_monitor.o polar_monitor_buf.o polar_monitor_async_ddl_lock_replay.o $(WIN32RES)

EXTENSION = polar_monitor
DATA = polar_monitor--1.0.sql
PGFILEDESC = "polar_monitor - monitoring of polardb information in real-time"
REGRESS = polar_monitor
NO_CHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/polar_monitor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
