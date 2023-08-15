#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/postmaster
#
# IDENTIFICATION
#    src/backend/postmaster/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/postmaster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = autovacuum.o bgworker.o bgwriter.o checkpointer.o fork_process.o \
	pgarch.o pgstat.o postmaster.o startup.o syslogger.o walwriter.o

# POLAR objects
OBJS += polar_parallel_bgwriter.o
OBJS += polar_wal_pipeliner.o
OBJS += polar_dispatcher.o

include $(top_srcdir)/src/backend/common.mk
