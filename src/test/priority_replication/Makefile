#-------------------------------------------------------------------------
#
# Makefile for src/test/priority_replication
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/priority_replication/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/priority_replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
