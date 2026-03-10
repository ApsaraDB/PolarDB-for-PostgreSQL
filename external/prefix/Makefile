PKGNAME = prefix
EXTENSION = prefix
MODULES = prefix
DATA = prefix--1.2.0.sql prefix--unpackaged--1.2.0.sql prefix--1.1--1.2.0.sql
DOCS = $(wildcard *.md)
# "explain (costs off)" needs 9.0+ (and 9.0 needs expected/explain_1.out)
REGRESS = create_extension prefix falcon explain queries
ORA_REGRESS = create_extension prefix falcon explain queries

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/prefix
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

deb: clean
	make -f debian/rules debian/control
	dh clean
	make -f debian/rules orig
	debuild -us -uc -sa
