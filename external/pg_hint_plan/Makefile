#
# pg_hint_plan: Makefile
#
# Copyright (c) 2012-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#

MODULE_big = pg_hint_plan
OBJS = \
	$(WIN32RES) \
	pg_hint_plan.o \
	query_scan.o

HINTPLANVER = 1.7.0

REGRESS = init base_plan pg_hint_plan ut-init ut-A ut-S ut-J ut-L ut-G ut-R \
	ut-fdw ut-W ut-T ut-fini plpgsql hint_table oldextversions \
	polar_control_force_append_parallel #polar_bug_with_hypopg
REGRESS_OPTS = --encoding=UTF8 --temp-config=pg_hint_plan.conf

POLAR_NO_EXTENSION_USER = 1
NO_INSTALLCHECK = 1

EXTENSION = pg_hint_plan
DATA = \
	pg_hint_plan--1.3.0.sql \
	pg_hint_plan--1.3.0--1.3.1.sql \
	pg_hint_plan--1.3.1--1.3.2.sql \
	pg_hint_plan--1.3.2--1.3.3.sql \
	pg_hint_plan--1.3.3--1.3.4.sql \
	pg_hint_plan--1.3.5--1.3.6.sql \
	pg_hint_plan--1.3.4--1.3.5.sql \
	pg_hint_plan--1.3.6--1.3.7.sql \
	pg_hint_plan--1.3.7--1.3.8.sql \
	pg_hint_plan--1.3.8--1.3.9.sql \
	pg_hint_plan--1.3.9--1.3.10.sql \
	pg_hint_plan--1.3.10--1.4.sql \
	pg_hint_plan--1.4--1.4.1.sql \
	pg_hint_plan--1.4.1--1.4.2.sql \
	pg_hint_plan--1.4.2--1.4.3.sql \
	pg_hint_plan--1.4.3--1.5.sql \
	pg_hint_plan--1.5--1.5.1.sql \
	pg_hint_plan--1.5.1--1.5.2.sql \
	pg_hint_plan--1.5.2--1.6.0.sql \
	pg_hint_plan--1.6.0--1.6.1.sql \
	pg_hint_plan--1.6.1--1.7.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/pg_hint_plan
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CPPFLAGS := -I$(top_builddir)/src/pl/plpgsql/src $(CPPFLAGS)
override PG_CFLAGS := -I$(top_builddir)/src/pl/plpgsql/src $(PG_CFLAGS)
endif
