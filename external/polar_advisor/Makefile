# external/polar_advisor/Makefile

EXTENSION = polar_advisor
DATA = ${EXTENSION}--1.0.sql \
	${EXTENSION}--1.0--1.1.sql

REGRESS = init security_definer super_pg_version action_command window_stats window \
	window_whitelist window_blacklist window_blacklist_vacuum window_vacuum \
	window_log window_monitor window_existed_objects

# Disable installcheck because concurrent cases could influence vacuum.
# But we can open installcheck for developing.
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/polar_advisor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
