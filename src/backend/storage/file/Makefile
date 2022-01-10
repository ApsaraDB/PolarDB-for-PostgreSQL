#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/file
#
# IDENTIFICATION
#    src/backend/storage/file/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/file
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = fd.o buffile.o copydir.o reinit.o sharedfileset.o
# POLAR objects
OBJS += polar_fd.o
OBJS += polar_io_fencing.o

include $(top_srcdir)/src/backend/common.mk
