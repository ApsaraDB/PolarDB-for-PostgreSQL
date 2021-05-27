# contrib/polar_monitor/Makefile

MODULE_big = polar_monitor
OBJS = polar_monitor_dma.o $(WIN32RES)

EXTENSION = polar_monitor
DATA = polar_monitor--1.1.sql
PGFILEDESC = "polar_monitor - monitoring of polar db information in real-time"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/polar_monitor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

