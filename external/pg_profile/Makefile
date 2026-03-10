PGPROFILE_VERSION = 4.10
EXTENSION = pg_profile

default: all

include migration/Makefile

DATA_built = $(EXTENSION)--$(PGPROFILE_VERSION).sql $(EXTENSION).control $(MIGRATION)

EXTRA_CLEAN = $(MIGRATION) $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql $(schema) \
	$(report) data/report_templates.sql

REGRESS = \
	create_extension \
	server_management \
	samples_and_reports \
	sizes_collection \
	export_import \
	retention_and_baselines \
	drop_extension

# pg_stat_kcache tests
REGRESS += \
	kcache_create_extension \
	server_management \
	samples_and_reports \
	sizes_collection \
	kcache_stat_avail \
	export_import \
	retention_and_baselines \
	kcache_drop_extension


REGRESS_OPTS = --temp-config $(top_srcdir)/external/pg_profile/pg_profile.conf

POLAR_NO_EXTENSION_USER=1

# Disabled because these tests require "shared_preload_libraries=pg_stat_statements",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

schema = schema/schema.sql

data = data/import_queries.sql \
	data/report_templates.sql
common = management/internal.sql
adm_funcs = management/baseline.sql \
	management/server.sql \
	management/local_server.sql
export_funcs = \
	management/export.sql
sample = \
	sample/sample_pg_stat_statements.sql \
	sample/pg_wait_sampling.sql \
	sample/sample.sql \
	sample/subsample.sql \
	sample/compat.sql

report = report/report_build.sql

grants = \
	privileges/pg_profile.sql

# Extension script contents
functions = $(common) $(adm_funcs) $(export_funcs) $(sample) $(report)
script = $(schema) $(data) $(functions) $(grants)

# Common sed replacement script
sed_extension = -e 's/{pg_profile}/$(EXTENSION)/; s/{extension_version}/$(PGPROFILE_VERSION)/; /--<manual_start>/,/--<manual_end>/d; /--<extension_end>/d; /--<extension_start>/d'
sed_manual = -e 's/{pg_profile}/$(EXTENSION)/; s/{extension_version}/$(PGPROFILE_VERSION)/; /--<extension_start>/,/--<extension_end>/d; /--<manual_start>/d; /--<manual_end>/d'

ifdef USE_PGXS
PG_CONFIG := pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = external/pg_profile
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

schema/schema.sql:
	${MAKE} -C schema

data/report_templates.sql:
	${MAKE} -C data

report/report_build.sql:
	${MAKE} -C report


$(EXTENSION).control: control.tpl
	sed -e 's/{version}/$(PGPROFILE_VERSION)/' control.tpl > $(EXTENSION).control

$(EXTENSION)--$(PGPROFILE_VERSION).sql: $(script)
	sed \
	-e '1i \\\echo Use "CREATE EXTENSION $(EXTENSION)" to load this file. \\quit' \
	$(sed_extension) \
	$(script) \
	> $(EXTENSION)--$(PGPROFILE_VERSION).sql

