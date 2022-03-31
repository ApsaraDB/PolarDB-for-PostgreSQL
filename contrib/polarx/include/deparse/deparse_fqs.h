/*-------------------------------------------------------------------------
 *
 * deparse_fqs.h
 *		Declarations for deparse_fqs.c
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/include/deparse/deparse_fqs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEPARSE_FQS_H
#define DEPARSE_FQS_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


extern void polarx_deparse_query(Query *query, StringInfo buf, List *parentnamespace,
                            bool finalise_aggs, bool sortgroup_colno);
#endif							/* DEPARSE_FQS_H */
