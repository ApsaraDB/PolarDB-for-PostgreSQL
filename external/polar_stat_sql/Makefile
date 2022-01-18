EXTENSION    = polar_stat_sql
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
TEMP_CONFIG = "polar_stat_sql.conf"

MODULE_big = polar_stat_sql
OBJS = polar_stat_sql.o

DATA = $(wildcard *--*.sql)
# Disabled because these tests require "shared_preload_libraries=polar_stat_sql",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_CHECK = 1
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/polar_stat_sql
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
