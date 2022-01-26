#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for contrib/polarx/test/regress (the polarx cluster plugin regression tests)
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# contrib/polarx/test/regress/GNUmakefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "polarx - test driver"
PGAPPICON = win32

subdir = src/test/regress
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(top_builddir)/src/test/regress $(CPPFLAGS)

# maximum simultaneous connections for parallel tests
MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

EXTRA_INSTALL = contrib/polarx

EXTRADEFS = '-DHOST_TUPLE="$(host_tuple)"' \
			'-DSHELLPROG="$(SHELL)"' \
			'-DDLSUFFIX="$(DLSUFFIX)"'

##
## Prepare for tests
##

# Build regression test driver

all: pg_regress_x$(X) autoinc$(DLSUFFIX) regress$(DLSUFFIX) refint$(DLSUFFIX)

pg_regress_x$(X): pg_regress_main.o pg_regress_x.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@


pg_regress_x.o: pg_regress_x.c $(top_builddir)/src/port/pg_config_paths.h
pg_regress_x.o: override CPPFLAGS += -I$(top_builddir)/src/port $(EXTRADEFS)

refint$(DLSUFFIX): $(top_builddir)/contrib/spi/refint$(DLSUFFIX)
	cp $< $@

autoinc$(DLSUFFIX): $(top_builddir)/contrib/spi/autoinc$(DLSUFFIX)
	cp $< $@

$(top_builddir)/contrib/spi/refint$(DLSUFFIX): | submake-contrib-spi ;

$(top_builddir)/contrib/spi/autoinc$(DLSUFFIX): | submake-contrib-spi ;

submake-contrib-spi: | submake-libpgport submake-generated-headers
	$(MAKE) -C $(top_builddir)/contrib/spi

.PHONY: submake-contrib-spi

regress$(DLSUFFIX): $(top_builddir)/src/test/regress/regress$(DLSUFFIX)
	cp $< $@

$(top_builddir)/src/test/regress/regress$(DLSUFFIX):
	$(MAKE) -C $(top_builddir)/src/test/regress all-lib


install: all installdirs
	$(INSTALL_PROGRAM) pg_regress_x$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'
	$(INSTALL_PROGRAM) regress$(DLSUFFIX) '$(pkglibdir)/regress$(DLSUFFIX)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'
	rm -f '$(pkglibdir)/regress$(DLSUFFIX)'

input_files = $(patsubst $(srcdir)/input/%.source,sql/%.sql, $(wildcard $(srcdir)/input/*.source))
output_files := $(patsubst $(srcdir)/output/%.source,expected/%.out, $(wildcard $(srcdir)/output/*.source))
# Tablespace setup

.PHONY: tablespace-setup
tablespace-setup:
	rm -rf ./testtablespace
	mkdir ./testtablespace

##
## Run tests
##
pg_regress_check = \
	$(with_temp_install) \
	$(srcdir)/pg_regress_x \
	--temp-instance=./tmp_check \
	--inputdir=$(srcdir) \
	--outputdir=$(srcdir) \
	--dbname=polarx \
	--bindir= \
	$(TEMP_CONF) \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)
pg_regress_installcheck = \
	$(srcdir)/pg_regress_x \
	--inputdir=$(srcdir) \
	--outputdir=$(srcdir) \
	--dbname=polarx \
	--bindir='$(bindir)' \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)

REGRESS_OPTS = --dlpath=$(srcdir) --max-concurrent-tests=20

check: all tablespace-setup
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule.single \
	$(MAXCONNOPT) $(EXTRA_TESTS)

check-tests: all tablespace-setup | temp-install
	$(pg_regress_check) $(REGRESS_OPTS) $(MAXCONNOPT) $(TESTS) $(EXTRA_TESTS)

installcheck: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule \
	$(EXTRA_TESTS)

installcheck-parallel: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule \
	$(MAXCONNOPT) $(EXTRA_TESTS)

installcheck-tests: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) $(TESTS) $(EXTRA_TESTS)

# old interfaces follow...

runcheck: check
runtest: installcheck
runtest-parallel: installcheck-parallel

bigtest: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule numeric_big

bigcheck: all tablespace-setup | temp-install
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(MAXCONNOPT) numeric_big


##
## Clean up
##

clean : 
# things built by `all' target
	rm -f refint$(DLSUFFIX) autoinc$(DLSUFFIX) regress$(DLSUFFIX)
	rm -f pg_regress_main.o pg_regress_x.o pg_regress_x$(X)
# things created by various check targets
	rm -f $(output_files) $(input_files)
	rm -rf testtablespace
	rm -rf $(pg_regress_clean_files)
