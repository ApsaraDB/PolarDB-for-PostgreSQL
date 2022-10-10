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

/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * The rangetable is the list of actual RTEs from the query tree, and the
 * cte list is the list of actual CTEs.
 *
 * rtable_names holds the alias name to be used for each RTE (either a C
 * string, or NULL for nameless RTEs such as unnamed joins).
 * rtable_columns holds the column alias names to be used for each RTE.
 *
 * In some cases we need to make names of merged JOIN USING columns unique
 * across the whole query, not only per-RTE.  If so, unique_using is true
 * and using_names is a list of C strings representing names already assigned
 * to USING columns.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the PlanState node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * PlanState's ancestors.  In addition, we store its outer and inner subplan
 * state nodes, as well as their plan nodes' targetlists, and the index tlist
 * if the current plan node might contain INDEX_VAR Vars.  (These fields could
 * be derived on-the-fly from the current PlanState, but it seems notationally
 * clearer to set them up as separate fields.)
 */
typedef struct
{
    List	   *rtable;			/* List of RangeTblEntry nodes */
    List	   *rtable_names;	/* Parallel list of names for RTEs */
    List	   *rtable_columns; /* Parallel list of deparse_columns structs */
    List	   *ctes;			/* List of CommonTableExpr nodes */
    /* Workspace for column alias assignment: */
    bool		unique_using;	/* Are we making USING names globally unique */
    List	   *using_names;	/* List of assigned names for USING columns */
    /* Remaining fields are used only when deparsing a Plan tree: */
    PlanState  *planstate;		/* immediate parent of current expression */
    List	   *ancestors;		/* ancestors of planstate */
    PlanState  *outer_planstate;	/* outer subplan state, or NULL if none */
    PlanState  *inner_planstate;	/* inner subplan state, or NULL if none */
    List	   *outer_tlist;	/* referent for OUTER_VAR Vars */
    List	   *inner_tlist;	/* referent for INNER_VAR Vars */
    List	   *index_tlist;	/* referent for INDEX_VAR Vars */
} deparse_namespace;

extern void polarx_deparse_query(Query *query, StringInfo buf, List *parentnamespace,
                            bool finalise_aggs, bool sortgroup_colno);
#endif							/* DEPARSE_FQS_H */
