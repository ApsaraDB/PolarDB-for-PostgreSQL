#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/smgr
#
# IDENTIFICATION
#    src/backend/storage/smgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/smgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	bulk_write.o \
	md.o \
	smgr.o

# POLAR
OBJS += polar_rsc.o
OBJS += polar_rsc_replica.o

include $(top_srcdir)/src/backend/common.mk
