#-------------------------------------------------------------------------
#
# Makefile for src/backend/polar_dma
#
# Copyright (c) 2022, Alibaba Group Holding Limited
#
# src/backend/polar_dma/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/polar_dma
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I./libconsensus/polar_wrapper/include -I./libconsensus/dependency/easy/src/io $(CPPFLAGS)

OBJS = consensus_repl.o consensus_log.o consensus_slru.o

SUBDIRS = libconsensus/consensus libconsensus/polar_wrapper libconsensus/dependency/easy/src

$(SUBDIRS:%=%-recursive): protoc

protoc:
	cd libconsensus/consensus/protocol && protoc -I. --cpp_out=. paxos.proto

clean: clean-protoc

clean-protoc:
	cd libconsensus/consensus/protocol && rm -f paxos.pb.cc paxos.pb.h

include $(top_srcdir)/src/backend/common.mk
