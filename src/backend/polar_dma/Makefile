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

override CPPFLAGS := -I./include -I./include/easy $(CPPFLAGS)

OBJS = consensus_repl.o consensus_log.o consensus_slru.o

include $(top_srcdir)/src/backend/common.mk

all: copy_consensus_libs

copy_consensus_libs:
ifeq ($(host_cpu), aarch64)
	$(shell cp -f '$(srcdir)/lib/libprotobuf_arm.so.20.0.1' '$(srcdir)/lib/libprotobuf.so.20.0.1')
	$(shell cp -f '$(srcdir)/lib/libpolar_consensus_wrapper_arm.so' '$(srcdir)/lib/libpolar_consensus_wrapper.so')
endif

install: all install-libs
install-libs: 
	$(INSTALL_DATA) $(srcdir)/lib/libprotobuf.so.20.0.1 '$(DESTDIR)$(libdir)/libprotobuf.so.20.0.1'
	$(INSTALL_DATA) $(srcdir)/lib/libprotobuf.so.20 '$(DESTDIR)$(libdir)/libprotobuf.so.20'
	$(INSTALL_DATA) $(srcdir)/lib/libprotobuf.so '$(DESTDIR)$(libdir)/libprotobuf.so'
	$(INSTALL_DATA) $(srcdir)/lib/libpolar_consensus_wrapper.so '$(DESTDIR)$(libdir)/libpolar_consensus_wrapper.so'

uninstall: uninstall-libs
uninstall-libs:
	rm -f $(addprefix '$(DESTDIR)$(libdir)'/, libprotobuf.so)
	rm -f $(addprefix '$(DESTDIR)$(libdir)'/, libprotobuf.so.20)
	rm -f $(addprefix '$(DESTDIR)$(libdir)'/, libprotobuf.so.20.0.1)
	rm -f $(addprefix '$(DESTDIR)$(libdir)'/, libpolar_consensus_wrapper.so)

.PHONY: install-libs uninstall-libs
