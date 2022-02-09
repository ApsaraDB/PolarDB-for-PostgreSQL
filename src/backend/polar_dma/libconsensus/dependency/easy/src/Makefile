#-------------------------------------------------------------------------
#
# Makefile for src/backend/polar_dma/libconsensus/dependency/easy/src
#
# Copyright (c) 2022, Alibaba Group Holding Limited
#
# src/backend/polar_dma/libconsensus/dependency/easy/src/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/polar_dma/libconsensus/dependency/easy/src
top_builddir = ../../../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -D_GNU_SOURCE -Iinclude -Iio -Ithread -Iutil -Ipacket -Ipacket/http $(CPPFLAGS) \
					 -Wno-error -Wno-declaration-after-statement -Wno-missing-prototypes

OBJS = thread/easy_uthread.o \
	   util/easy_string.o \
	   util/easy_pool.o \
	   util/easy_time.o \
	   util/easy_hash.o \
	   util/easy_inet.o \
	   util/easy_array.o \
	   util/easy_buf.o \
	   io/easy_io.o \
	   io/easy_file.o \
	   io/easy_socket.o \
	   io/easy_request.o \
	   io/easy_baseth_pool.o \
	   io/easy_summary.o \
	   io/easy_ssl.o \
	   io/easy_message.o \
	   io/easy_client.o \
	   io/easy_log.o \
	   io/easy_connection.o \
	   io/ev.o \
	   packet/easy_kfc_handler.o \
	   packet/http/easy_http_handler.o \
	   packet/http/http_parser.o

include $(top_srcdir)/src/backend/common.mk
