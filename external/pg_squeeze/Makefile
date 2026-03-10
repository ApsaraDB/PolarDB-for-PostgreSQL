MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o worker.o pgstatapprox.o $(WIN32RES)
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.2.sql pg_squeeze--1.2--1.3.sql pg_squeeze--1.3--1.4.sql \
pg_squeeze--1.4--1.5.sql pg_squeeze--1.5--1.6.sql pg_squeeze--1.6--1.7.sql \
pg_squeeze--1.7--1.8.sql pg_squeeze--1.8--1.9.sql
DOCS = pg_squeeze.md

REGRESS = squeeze
# POLAR: add pg_squeeze into shared_preload_libraries and set wal_level to logical in conf file.
REGRESS_OPTS = --temp-config $(top_srcdir)/external/pg_squeeze/pg_squeeze.conf

TAP_TESTS = 1

# POLAR: pg_squeeze must be loaded via shared_preload_libraries.
# But parameter "shared_preload_libraries" cannot be changed without restarting the server.
# So we can only do make check for this extension.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/pg_squeeze
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
