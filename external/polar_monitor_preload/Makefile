# external/polar_monitor/Makefile

MODULE_big = polar_monitor_preload
top_builddir = ../..
OBJS = polar_monitor_preload.o polar_monitor_mcxt.o polar_monitor_lock.o polar_monitor_network.o $(WIN32RES)
OBJS += polar_monitor_backend.o polar_procstat.o
OBJS += polar_monitor_plan.o

EXTENSION = polar_monitor_preload
DATA = $(wildcard *--*.sql)
PGFILEDESC = "polar_monitor - monitoring of polar db information in real-time for preload"
REGRESS = polar_monitor_preload
NO_CHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
subdir = external/polar_monitor_preload
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

installcheck:
	$(pg_regress_installcheck) --host localhost --inputdir=. --bindir='$(bindir)' $(REGRESS_OPTS) polar_monitor_preload
