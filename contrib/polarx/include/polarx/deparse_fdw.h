/*-------------------------------------------------------------------------
 *
 * deparse_fdw.h
 *		Declarations for deparse_fdw.c
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/polarx/include/polarx/deparse_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEPARSE_FDW_H
#define DEPARSE_FDW_H

#include "nodes/relation.h"

extern void classifyConditions(PlannerInfo *root,
        RelOptInfo *baserel,
        List *input_conds,
        List **remote_conds,
        List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root,
        RelOptInfo *baserel,
        Expr *expr);
extern bool is_foreign_param(PlannerInfo *root,
        RelOptInfo *baserel,
        Expr *expr);
extern void deparseInsertSql(StringInfo buf, RangeTblEntry *rte,
        Index rtindex, Relation rel,
        List *targetAttrs, bool doNothing, List *returningList,
        List **retrieved_attrs);
extern void deparseUpdateSql(StringInfo buf, RangeTblEntry *rte,
        Index rtindex, Relation rel,
        List *targetAttrs, List *returningList,
        List **retrieved_attrs);
extern void deparseDirectUpdateSql(StringInfo buf, PlannerInfo *root,
        Index rtindex, Relation rel,
        RelOptInfo *foreignrel,
        List *targetlist,
        List *targetAttrs,
        List *remote_conds,
        List **params_list,
        List *returningList,
        List **retrieved_attrs);
extern void deparseDeleteSql(StringInfo buf, RangeTblEntry *rte,
        Index rtindex, Relation rel,
        List *returningList,
        List **retrieved_attrs);
extern void deparseDirectDeleteSql(StringInfo buf, PlannerInfo *root,
        Index rtindex, Relation rel,
        RelOptInfo *foreignrel,
        List *remote_conds,
        List **params_list,
        List *returningList,
        List **retrieved_attrs);
extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel,
        List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);
extern Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root,
        RelOptInfo *foreignrel, List *tlist,
        List *remote_conds, List *pathkeys, bool is_subquery,
        List **retrieved_attrs, List **params_list);
extern const char *get_jointype_name(JoinType jointype);
extern void reset_transmission_modes(int nestlevel);
extern int set_transmission_modes(void);
#endif							/* DEPARSE_FDW_H */
