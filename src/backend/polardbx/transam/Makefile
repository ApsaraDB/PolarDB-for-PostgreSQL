#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for transam
#
#  Portions Copyright (c) 2010-2012 Postgres-XC Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/polardbx/transam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = txn_coordinator.o \
	   txn_gtm.o 		\
	   txn_util.o \

include $(top_srcdir)/src/backend/common.mk
