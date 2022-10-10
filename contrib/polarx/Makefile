# contrib/polarx/Makefile

MODULE_big = polarx
SUBDIRS = . mdcache nodemgr locator pool connection squeue commands plancache plan executor deparse utils nodes parser fdw_engines/polarx distribute_transaction metadata
polarx_srcdir = .
OBJS += \
    $(patsubst $(polarx_srcdir)/%.c,%.o,$(foreach dir,$(SUBDIRS), $(sort $(wildcard $(polarx_srcdir)/$(dir)/*.c))))
OBJS += $(WIN32RES)
PGFILEDESC = "polarx - Distribute cluster plugin for PostgreSQL"

PG_CPPFLAGS = -DPOLARDBX_SHARDING -I$(libpq_srcdir) -I ./include
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = polarx
DATA = polarx--1.0.sql

MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

regress_dir = $(polarx_srcdir)/test/regress

REGRESS = --schedule=$(regress_dir)/serial_schedule.single \
		--dbname=polarx \
		$(MAXCONNOPT) $(EXTRA_TESTS)

EXTRA_REGRESS_OPTS = --dlpath=$(regress_dir) \
					--max-concurrent-tests=20 \
					--inputdir=$(regress_dir)
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/polarx
top_builddir = ../..

include $(top_builddir)/src/Makefile.global

polarx_regress_input_files = $(patsubst $(regress_dir)/input/%.source,$(regress_dir)/sql/%.sql, $(wildcard $(regress_dir)/input/*.source))
polarx_regress_output_files = $(patsubst $(regress_dir)/output/%.source,$(regress_dir)/expected/%.out, $(wildcard $(regress_dir)/output/*.source))

polarx_regresss_clean_files =  $(regress_dir)/results/  $(regress_dir)/regression.diffs \
					$(regress_dir)/regression.out  $(regress_dir)/tmp_check/ \
					$(regress_dir)/tmp_check_iso/  $(regress_dir)/log/  \
					$(regress_dir)/output_iso/ $(regress_dir)/testtablespace

polarx_regress_obj = $(regress_dir)/regress$(DLSUFFIX) $(regress_dir)/refint$(DLSUFFIX) \
					$(regress_dir)/autoinc$(DLSUFFIX) $(regress_dir)/pg_regress_x$(X) \
					$(regress_dir)/pg_regress_main.o $(regress_dir)/pg_regress_x.o

pg_regress_clean_files = $(polarx_regress_input_files) $(polarx_regress_output_files) \
						$(polarx_regresss_clean_files) $(polarx_regress_obj)

pg_regress_check = \
	$(with_temp_install) \
	$(regress_dir)/pg_regress_x \
	--temp-instance=$(regress_dir)/tmp_check \
	--outputdir=$(regress_dir) \
	--bindir= \
	$(TEMP_CONF) \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)

pg_regress_installcheck = \
	$(regress_dir)/pg_regress_x \
	--outputdir=$(regress_dir) \
	--bindir='$(bindir)' \
	$(pg_regress_locale_flags) $(EXTRA_REGRESS_OPTS)

submake-polarx-regress:
	$(MAKE) -C $(polarx_srcdir)/test/regress all

REGRESS_PREP := submake-polarx-regress

include $(top_srcdir)/contrib/contrib-global.mk

endif
