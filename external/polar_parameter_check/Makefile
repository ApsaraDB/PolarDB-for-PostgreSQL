# external/polar_parameter_check/Makefile

MODULE_big = polar_parameter_check
OBJS = polar_parameter_check.o

EXTENSION = polar_parameter_check
DATA = polar_parameter_check--1.0.sql
REGRESS = polar_parameter_check

PG_CPPFLAGS = -Wall -g -I/usr/local/include

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
