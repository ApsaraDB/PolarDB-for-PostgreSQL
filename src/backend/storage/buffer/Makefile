#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/buffer
#
# IDENTIFICATION
#    src/backend/storage/buffer/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = buf_table.o buf_init.o bufmgr.o freelist.o localbuf.o

# POLAR objects
OBJS += polar_copybuf.o polar_flushlist.o polar_xlogbuf.o polar_pbp.o

include $(top_srcdir)/src/backend/common.mk
