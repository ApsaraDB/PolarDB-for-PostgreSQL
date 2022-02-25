#-------------------------------------------------------------------------
#
# Makefile for src/backend/polar_dma/libconsensus/consensus
#
# Copyright (c) 2022, Alibaba Group Holding Limited
#
# src/backend/polar_dma/libconsensus/consensus/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/polar_dma/libconsensus/consensus
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/backend/polar_dma/polar_dma.mk

OBJS = algorithm/configuration.o \
	   algorithm/paxos.o \
	   algorithm/paxos_server.o \
	   algorithm/paxos_log_cache.o \
	   algorithm/paxos_error.o \
	   algorithm/msg_compress.o \
	   algorithm/crc.o \
	   algorithm/log_meta_cache.o \
	   protocol/paxos.pb.o \
	   net/easyNet.o \
	   service/service.o \
	   service/thread_timer.o \
	   service/client_service.o \

include $(top_srcdir)/src/backend/common.mk
