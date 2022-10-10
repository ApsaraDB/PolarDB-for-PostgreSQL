#-------------------------------------------------------------------------
#
# Makefile for src/bin/tsc_parse
#
# Copyright (c) 1998-2018, PostgreSQL Global Development Group
#
# src/bin/tsc_parse/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "tsc_parse - Parse tsc hex log into CSV"
PGAPPICON=win32

subdir = src/bin/tsc_parse
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS=   tsc_parse.o $(WIN32RES)

all: tsc_parse

tsc_parse: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) ../../backend/utils/time/tsc_tracer.o $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
	
install: all installdirs
	$(INSTALL_SCRIPT) tsc_parse$(X) '$(DESTDIR)$(bindir)/tsc_parse$(X)'
	$(INSTALL_SCRIPT) filtertsclog.awk$(X) '$(DESTDIR)$(bindir)/filtertsclog.awk$(X)'
	$(INSTALL_SCRIPT) tracefields_example.awk$(X) '$(DESTDIR)$(bindir)/tracefields_example.awk$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/tsc_parse$(X)'

clean distclean maintainer-clean:
	rm -f tsc_parse$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
