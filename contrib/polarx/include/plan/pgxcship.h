/*-------------------------------------------------------------------------
 *
 * pgxcship.h
 *        Functionalities for the evaluation of expression shippability
 *        to remote polarx nodes
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 1996-2012 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/include/plan/pgxcship.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCSHIP_H
#define PGXCSHIP_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"
#include "utils/reltrigger.h"

/* Determine if query is shippable */
extern ExecNodes *polarx_is_query_shippable(Query *query, int query_level);

#endif
