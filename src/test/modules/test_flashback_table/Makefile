# src/test/modules/test_flashback_table/Makefile

MODULE_big = test_flashback_table
OBJS = test_flashback_table.o $(WIN32RES)
PGFILEDESC = "test_flashback_table - test code for flashback table feature"

EXTENSION = test_flashback_table
DATA = test_flashback_table--1.0.sql
TEMP_CONFIG = "test_flashback_table.conf"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_flashback_table
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Disabled because these tests require "polar_enable_flashback_log=on" and "polar_enable_fast_recovery_area=on, 
# which typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

check: regresscheck isolationcheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-test-flashback-table:
	$(MAKE) -C $(top_builddir)/src/test/modules/test_flashback_table

REGRESSCHECKS=test_flashback_table

regresscheck: | submake-regress submake-test-flashback-table temp-install
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/src/test/modules/test_flashback_table/test_flashback_table.conf \
	    $(REGRESSCHECKS)

ISOLATIONCHECKS=flashback_table_isolation 

isolationcheck: | submake-isolation submake-test-flashback-table temp-install
	$(pg_isolation_regress_check) \
	    --temp-config $(top_srcdir)/src/test/modules/test_flashback_table/test_flashback_table.conf \
	    $(ISOLATIONCHECKS)

.PHONY: submake-test-flashback-table submake-regress check \
	regresscheck isolationcheck
