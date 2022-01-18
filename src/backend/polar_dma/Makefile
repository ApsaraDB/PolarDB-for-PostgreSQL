#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/polar_dma
#
# IDENTIFICATION
#    src/backend/polar_dma/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/polar_dma
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I./libconsensus/polar_wrapper/include -I./libconsensus/polar_wrapper/include/easy $(CPPFLAGS)

OBJS = consensus_repl.o consensus_log.o consensus_slru.o

include $(top_srcdir)/src/backend/common.mk

install: all install-libs
install-libs: 
	$(INSTALL_DATA) $(srcdir)/libconsensus/polar_wrapper/lib/libpolar_consensus_wrapper.so '$(DESTDIR)$(libdir)/libpolar_consensus_wrapper.so'

uninstall: uninstall-libs
uninstall-libs:
	rm -f $(addprefix '$(DESTDIR)$(libdir)'/, libpolar_consensus_wrapper.so)

.PHONY: install-libs uninstall-libs
