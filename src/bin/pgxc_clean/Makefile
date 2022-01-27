#-------------------------------------------------------------------------
#
# Makefile for src/bin/pgxc_clean
#
# Portions Copyright (c) 2020, Alibaba Group Holding Limited
# Portions Copyright (c) 2011-2012 Postgres-XC Development Group
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "pgxc_clean - Resolve prepared transactions in PolarDB-X cluster"
PGAPPICON = win32

subdir = src/bin/pgxc_clean
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= pgxc_clean.o txninfo.o


PG_CPPFLAGS  = -DFRONTEND -DDLSUFFIX=\"$(DLSUFFIX)\" -I$(srcdir) -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS)

override CPPFLAGS := $(PG_CPPFLAGS) $(CPPFLAGS)
override LDFLAGS := $(PG_LIBS) $(LDFLAGS)

SUBMAKE_LIBPQ := submake-libpq

all: pgxc_clean

pgxc_clean: $(OBJS) | $(SUBMAKE_LIBPQ)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pgxc_clean$(X) '$(DESTDIR)$(bindir)/pgxc_clean$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pgxc_clean$(X)'

clean distclean maintainer-clean:
	rm -f pgxc_clean$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
