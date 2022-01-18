#-------------------------------------------------------------------------
#
# Makefile for src/test/polar_flog_repair_partial
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/polar_flog_repair_partial/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/polar_flog_repair_partial
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

EXTRA_INSTALL = contrib/faultinjector

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check