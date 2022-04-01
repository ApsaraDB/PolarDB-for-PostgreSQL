#-------------------------------------------------------------------------
#
# Makefile for contrib/pgxc_ctl
#
# Copyright (c) 2020, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright (c) 2013 Postgres-XC Development Group
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "pgxc_ctl - Provide XC configuration and opeation"
PGAPPICON = win32

PROGRAM= pgxc_ctl
OBJS= pgxc_ctl_bash.o bash_handler.o config.o pgxc_ctl.o variables.o pgxc_ctl_log.o do_command.o \
	 utils.o do_shell.o cm.o coord_cmd.o datanode_cmd.o monitor.o

PG_CPPFLAGS  = -DFRONTEND -DDLSUFFIX=\"$(DLSUFFIX)\" -I$(srcdir) -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport) $(PTHREAD_LIBS) $(EX_OBJS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgxc_ctl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

pgxc_ctl_bash.c: pgxc_ctl_conf_part_full pgxc_ctl_conf_part_minimal pgxc_ctl_conf_part_empty pgxc_ctl_conf_standalone \
		pgxc_ctl_conf_distributed pgxc_ctl_conf_dispaxos pgxc_ctl_bash_2
	cd $(top_srcdir)/$(subdir) && ./make_signature
ifeq ($(vpath_build),yes)
	mv $(top_srcdir)/$(subdir)/signature.h .
	mv $(top_srcdir)/$(subdir)/pgxc_ctl_bash.c .
endif

clean: clean-script

clean-script:
	rm -f pgxc_ctl_bash.c signature.h

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
