/*-------------------------------------------------------------------------
 *
 * polarx_fdw.h
 *		  polarx Foreign-data wrapper Engine
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/polarx/include/polarx_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_FDW_H
#define POLARX_FDW_H

#include "postgres_fdw.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"
#include "commands/explain.h"

/* in polarx_fdw.c */
extern int	set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);

extern void polarxGetForeignRelSize(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);
extern void polarxGetForeignPaths(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);
extern ForeignScan *polarxGetForeignPlan(PlannerInfo *root,
        RelOptInfo *foreignrel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses,
        Plan *outer_plan);
extern void polarxBeginForeignScan(ForeignScanState *node, int eflags);
extern TupleTableSlot *polarxIterateForeignScan(ForeignScanState *node);
extern void polarxReScanForeignScan(ForeignScanState *node);
extern void polarxEndForeignScan(ForeignScanState *node);
extern void polarxAddForeignUpdateTargets(Query *parsetree,
        RangeTblEntry *target_rte,
        Relation target_relation);
extern List *polarxPlanForeignModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index);
extern void polarxBeginForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *resultRelInfo,
        List *fdw_private,
        int subplan_index,
        int eflags);
extern TupleTableSlot *polarxExecForeignInsert(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
extern TupleTableSlot *polarxExecForeignUpdate(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
extern TupleTableSlot *polarxExecForeignDelete(EState *estate,
        ResultRelInfo *resultRelInfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);
extern void polarxEndForeignModify(EState *estate,
        ResultRelInfo *resultRelInfo);
extern void polarxBeginForeignInsert(ModifyTableState *mtstate,
        ResultRelInfo *resultRelInfo);
extern void polarxEndForeignInsert(EState *estate,
        ResultRelInfo *resultRelInfo);
extern int  polarxIsForeignRelUpdatable(Relation rel);
extern bool polarxPlanDirectModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index);
extern void polarxBeginDirectModify(ForeignScanState *node, int eflags);
extern TupleTableSlot *polarxIterateDirectModify(ForeignScanState *node);
extern void polarxEndDirectModify(ForeignScanState *node);
extern void polarxExplainForeignScan(ForeignScanState *node,
        ExplainState *es);
extern void polarxExplainForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        ExplainState *es);
extern void polarxExplainDirectModify(ForeignScanState *node,
        ExplainState *es);
extern bool polarxAnalyzeForeignTable(Relation relation,
        AcquireSampleRowsFunc *func,
        BlockNumber *totalpages);
extern List *polarxImportForeignSchema(ImportForeignSchemaStmt *stmt,
        Oid serverOid);
extern void polarxGetForeignJoinPaths(PlannerInfo *root,
        RelOptInfo *joinrel,
        RelOptInfo *outerrel,
        RelOptInfo *innerrel,
        JoinType jointype,
        JoinPathExtraData *extra);
extern bool polarxRecheckForeignScan(ForeignScanState *node,
        TupleTableSlot *slot);
extern void polarxGetForeignUpperPaths(PlannerInfo *root,
        UpperRelationKind stage,
        RelOptInfo *input_rel,
        RelOptInfo *output_rel,
        void *extra);
#endif							/* POLARX_FDW_H */
