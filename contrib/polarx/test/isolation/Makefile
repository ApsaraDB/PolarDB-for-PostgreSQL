#
# Makefile for isolation tests
#

PGFILEDESC = "polarx_isolation_regress/isolationtester - multi-client test driver"
PGAPPICON = win32

EXTRA_INSTALL = contrib/polarx
subdir = src/test/isolation
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(top_builddir)/src/test/regress $(CPPFLAGS)

all: isolationtester$(X) pg_isolation_regress_x$(X)

# Though we don't install these binaries, build them during installation
# (including temp-install).  Otherwise, "make -j check-world" and "make -j
# installcheck-world" would spawn multiple, concurrent builds in this
# directory.  Later builds would overwrite files while earlier builds are
# reading them, causing occasional failures.
install: | all

submake-regress:
	$(MAKE) -C $(srcdir)/../regress pg_regress_x.o
submake-isolation-main:
	$(MAKE) -C $(top_builddir)/src/test/isolation isolation_main.o
submake-isolationtester:
	$(MAKE) -C $(top_builddir)/src/test/isolation isolationtester$(X)

pg_regress_x.o: | submake-regress
	rm -f $@ && $(LN_S) $(srcdir)/../regress/pg_regress_x.o .

#isolation_main.o: | submake-isolation-main
#	rm -f $@ && $(LN_S) $(top_builddir)/src/test/isolation/isolation_main.o .

pg_isolation_regress_x$(X): isolation_main.o pg_regress_x.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

isolationtester$(X): | submake-isolationtester
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/isolation/isolationtester$(X) .



# so do not clean them here
clean distclean:
	rm -f isolationtester$(X) pg_isolation_regress_x$(X) isolation_main.o
	rm -f pg_regress_x.o
	rm -rf $(pg_regress_clean_files)

pg_xisolation_regress_check = \
	$(with_temp_install) \
	$(srcdir)/pg_isolation_regress_x \
	--temp-instance=./tmp_check_iso \
	--inputdir=$(srcdir) --outputdir=output_iso \
	--bindir= \
	$(TEMP_CONF) \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)

pg_xisolation_regress_installcheck = \
	$(srcdir)/pg_isolation_regress_x \
	--inputdir=$(srcdir) --outputdir=output_iso \
	--bindir='$(bindir)' \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)

installcheck: all
	$(pg_xisolation_regress_installcheck) --schedule=$(srcdir)/isolation_schedule

check: all
	$(pg_xisolation_regress_check) --schedule=$(srcdir)/isolation_schedule

# Versions of the check tests that include the prepared_transactions test
# It only makes sense to run these if set up to use prepared transactions,
# via TEMP_CONFIG for the check case, or via the postgresql.conf for the
# installcheck case.
installcheck-prepared-txns: all temp-install
	$(pg_xisolation_regress_installcheck) --schedule=$(srcdir)/isolation_schedule prepared-transactions

check-prepared-txns: all temp-install
	$(pg_xisolation_regress_check) --schedule=$(srcdir)/isolation_schedule prepared-transactions
