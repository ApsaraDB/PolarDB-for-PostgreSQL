# external/polar_monitor/Makefile

MODULE_big = polar_monitor
OBJS = polar_monitor.o polar_monitor_buf.o procstat.o polar_monitor_io.o $(WIN32RES)
OBJS += polar_monitor_csnlog.o
OBJS += polar_monitor_shmem.o
OBJS += polar_monitor_async_ddl_lock_replay.o
OBJS += polar_monitor_cgroup.o
OBJS += polar_monitor_dma.o
OBJS += polar_monitor_datamax.o
OBJS += polar_monitor_flashback_log.o
OBJS += polar_monitor_proxy.o
OBJS += polar_monitor_dsa.o
OBJS += polar_monitor_shared_server.o

EXTENSION = polar_monitor
DATA = polar_monitor--1.0--1.1.sql polar_monitor--1.1--1.2.sql polar_monitor--1.2.sql
PGFILEDESC = "polar_monitor - monitoring of polar db information in real-time"
REGRESS = polar_monitor
NO_CHECK = 1
NO_INSTALLCHECK = 1

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
