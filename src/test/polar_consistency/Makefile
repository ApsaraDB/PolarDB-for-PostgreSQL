#-------------------------------------------------------------------------
#
# Makefile for src/test/recovery
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/recovery/Makefile
#
#-------------------------------------------------------------------------

EXTRA_INSTALL=contrib/test_decoding

subdir = src/test/polar
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

build_external: temp-install
	$(shell cd $(abs_top_builddir); \
	make -C external DESTDIR='$(abs_top_builddir)'/tmp_install install >> \
	$(abs_top_builddir)/tmp_install/log/install.log 2>&1)

.PHONY: build_external

build_contrib: temp-install
	$(shell cd $(abs_top_builddir); \
	make -C contrib DESTDIR='$(abs_top_builddir)'/tmp_install install >> \
	$(abs_top_builddir)/tmp_install/log/install.log 2>&1)

.PHONY: build_contrib

check: build_external build_contrib
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
