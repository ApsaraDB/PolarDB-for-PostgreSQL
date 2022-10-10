/*-------------------------------------------------------------------------
 *
 * fdwplanner_utils.c
 *
 * Query-walker utility functions to be used to change RTE in query and
 * utililty functions of changing rel cache for using FDW planner
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/utils/fdwplanner_utils.c
 *
 **----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "polarx.h"
#include "nodes/primnodes.h"

#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger.h"
#include "catalog/partition.h"
#include "commands/trigger.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"
#include "utils/reltrigger.h"
#include "utils/relcache.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/fdwplanner_utils.h"
#include "pgxc/locator.h"
#include "access/xact.h"
#include "access/transam.h"
#include "polarx/polarx_fdw.h"


typedef struct relsavedinfoent
{
    Oid         reloid;
    char        real_relkind;
    bool        need_setback;
    bool        is_foreign;
    TriggerDesc *real_trigdesc;
    TriggerDesc *foreign_trigdesc;
} RelSavedInfoEnt;

typedef struct
{
    ParamExternDataInfo *params;
    Datum hash_val;
    int sql_seq;
    int random_node;
    int target_node;
    bool fqs_valid;
    bool is_cursor;
} pc_param_context;

typedef struct
{
    bool is_invalid;
}
pc_plan_valid_context;

static HTAB *RelationSavedCache;
static bool need_setback = false;

static TriggerDesc *ExceptInternalTrigger(TriggerDesc *trigdesc);
static void SetTriggerFlags(TriggerDesc *trigdesc, Trigger *trigger);
static bool ExtractRelationRangeTableListWalker(Node *node, extract_rte_context *context);
static void  polarx_distributable_rangeTableEntry(RangeTblEntry *rangeTableEntry, extract_rte_context *context);
static void AdjustRelationToForeignTableWorker(Relation rd);
static void RelationSavedCacheInitialize(void);
static void RelationSavedCacheInvalidate(void);
static void RelationSavedCacheInvalidateEntry(Oid relationId);
static void InvalidRelationSavedCache(Datum argument, Oid relationId);
static bool IsFDWDistributeTable(Oid relid);
static void plan_tree_walker(Plan *plan,
                                void (*worker) (Plan *plan, void *context),
                                void *context);
static void get_paraminfo_from_foreignscan(Plan *plan, void *context);
static void add_paraminfo_into_foreignscan(Plan *plan, void *context);
static void is_direct_modify_plan_valid(Plan *plan, void *context);
static void exec_on_plantree(PlannedStmt *planned_stmt, void (*execute_func) (Plan *plan, void *context),
                                void *context);
static void params_add_worker(Plan *plan,  void *context);
static void params_get_worker(Plan *plan,  void *context);
static void check_plan_valid_worker(Plan *plan,  void *context);
static void eval_target_node_warker(Plan *plan,  void *context);
static void eval_target_node_foreignscan(Plan *plan, void *context);
static void add_fqsplan_into_foreignscan(Plan *plan, void *context);
static void fqs_add_worker(Plan *plan,  void *context);

#define INITRELCACHESIZE        400
#define RelationSavedCacheInsert(RELATION_SAVED)  \
    do { \
        RelSavedInfoEnt *hentry; bool found; \
        hentry = (RelSavedInfoEnt *) hash_search(RelationSavedCache, \
                (void *) &((RELATION_SAVED).reloid), \
                HASH_ENTER, &found); \
        if (!found) \
        { \
            hentry->real_relkind = RELATION_SAVED.real_relkind; \
            hentry->real_trigdesc = RELATION_SAVED.real_trigdesc; \
            hentry->foreign_trigdesc = RELATION_SAVED.foreign_trigdesc; \
            hentry->need_setback = RELATION_SAVED.need_setback; \
            hentry->is_foreign = true; \
        } \
        else \
        { \
            hentry->real_relkind = RELATION_SAVED.real_relkind; \
            hentry->real_trigdesc = RELATION_SAVED.real_trigdesc; \
            hentry->foreign_trigdesc = RELATION_SAVED.foreign_trigdesc; \
            hentry->need_setback = RELATION_SAVED.need_setback; \
            hentry->is_foreign = true; \
        } \
    } while(0)
#define RelationSavedInfoLookup(ID, RELATION_SAVED_P) \
    do { \
        RelSavedInfoEnt *hentry; \
        hentry = (RelSavedInfoEnt *) hash_search(RelationSavedCache, \
                (void *) &(ID), \
                HASH_FIND, NULL); \
        if (hentry) \
        RELATION_SAVED_P = hentry; \
        else \
        RELATION_SAVED_P = NULL; \
    } while(0)
#define RelationSavedCacheDelete(OID) \
    do { \
        RelSavedInfoEnt *hentry; \
        hentry = (RelSavedInfoEnt *) hash_search(RelationSavedCache, \
                (void *) &(OID), \
                HASH_REMOVE, NULL); \
        if (hentry == NULL) \
        elog(WARNING, "failed to delete rel saved info cache entry for OID %u", \
                OID); \
    } while(0)

/*
 * ExtractRelationRangeTableList walks over a query tree to gather relation range table entries.
 */
static bool
ExtractRelationRangeTableListWalker(Node *node, extract_rte_context *context)
{
    bool done = false;

    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, RangeTblEntry))
    {
        RangeTblEntry *rangeTable = (RangeTblEntry *) node;

        polarx_distributable_rangeTableEntry(rangeTable, context);
    }
    else if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        if (query->hasSubLinks || query->cteList || query->setOperations)
        {
            done = query_tree_walker(query,
                                    ExtractRelationRangeTableList,
                                    (void *)context,
                                    QTW_EXAMINE_RTES);
        }
        else
        {
            done = range_table_walker(query->rtable,
                                    ExtractRelationRangeTableList,
                                    (void *)context,
                                    QTW_EXAMINE_RTES);
        }
    }
    else
    {
        done = expression_tree_walker(node, ExtractRelationRangeTableList,
                                    (void *)context);
    }

    return done;
}


/*
 * polarxDistributableRangeTableEntry returns true if the input range table
 * entry is a relation and it can be used in fdw planner.
 */
static void 
polarx_distributable_rangeTableEntry(RangeTblEntry *rangeTableEntry, extract_rte_context *context)
{
	char relationKind = '\0';

    if(rangeTableEntry->relid && rangeTableEntry->relid < FirstNormalObjectId)
    {
        context->has_sys = true;
        return;
    }

	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return;
	}

	relationKind = rangeTableEntry->relkind;
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
	{
		/*
         * RELKIND_RELATION is a table, RELKIND_PARTITIONED_TABLE is partition table.
         * partition table's child table will be used as distributed table.
		 */
        context->has_foreign = true;
        context->rteList = lappend(context->rteList, rangeTableEntry);
	}
    else if(relationKind == RELKIND_FOREIGN_TABLE)
    {
        if(RelationSavedCache)
        {
           /*
            * may relation is set back temp but statement execution
            * is still under running. so should set relkind back to
            * RELKIND_RELATION for adjust relation again
            */
            RelSavedInfoEnt *rel_saved = NULL;
            RelationSavedInfoLookup(rangeTableEntry->relid,rel_saved);
            if(rel_saved)
            {
                context->has_foreign = true;
                if(!rel_saved->is_foreign)
                {
                    rangeTableEntry->relkind = RELKIND_RELATION;
                    context->rteList = lappend(context->rteList, rangeTableEntry);
                }
            }
            if(rangeTableEntry->inh)
                context->rteList = lappend(context->rteList, rangeTableEntry);
        }
    }
}

bool
ExtractRelationRangeTableList(Node *node, void *context)
{
    if(IS_PGXC_LOCAL_COORDINATOR)
        return ExtractRelationRangeTableListWalker(node, (extract_rte_context *)context);
    else
        return false;
}

static void
AdjustRelationToForeignTableWorker(Relation rd)
{
    if(rd == NULL)
        return;

    if(IS_PGXC_LOCAL_COORDINATOR)
    {
        Oid relid = RelationGetRelid(rd);

        RelSavedInfoEnt *rel_saved = NULL;

        RelationSavedCacheInitialize();
        RelationSavedInfoLookup(relid,rel_saved);
        if(rel_saved && rel_saved->real_relkind != '\0')
        {
            rd->rd_rel->relkind = RELKIND_FOREIGN_TABLE;
            rd->trigdesc = rel_saved->foreign_trigdesc;
            rel_saved->need_setback = true;
            rel_saved->is_foreign = true;
        }
        else if(rd->rd_rel->relkind == RELKIND_RELATION)
        {
            RelSavedInfoEnt saved;

            saved.reloid = relid;
            saved.real_relkind = rd->rd_rel->relkind;
            saved.real_trigdesc = rd->trigdesc;
            saved.need_setback = true;
            if(rd->trigdesc)
            {
                rd->trigdesc = ExceptInternalTrigger(rd->trigdesc);
                saved.foreign_trigdesc = rd->trigdesc;
            }
            else
                saved.foreign_trigdesc = NULL;
            rd->rd_rel->relkind = RELKIND_FOREIGN_TABLE;
            RelationSavedCacheInsert(saved);
        }
        else if(rd->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        {
            PartitionDesc partdesc = RelationGetPartitionDesc(rd);
            int partitionCount = partdesc->nparts;
            int partitionIndex = 0;

            for (partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
            {
                Relation    rd_p = relation_open(partdesc->oids[partitionIndex], NoLock);
                if(rd_p->rd_rel->relkind == RELKIND_RELATION ||
                    rd_p->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
                    AdjustRelationToForeignTableWorker(rd_p);
                relation_close(rd_p, NoLock);
            }
        }
        if(!need_setback)
            need_setback = true;

    }
    if(rd->rd_rel->relkind == RELKIND_RELATION && IsConnFromCoord() && rd->rd_rulescxt)
    {
        MemoryContextDelete(rd->rd_rulescxt);
        rd->rd_rules = NULL;
        rd->rd_rulescxt = NULL;
    }
}

void
AdjustRelationToForeignTable(List *tableList, bool *is_all_foreign)
{
    ListCell *lc = NULL;

    if(!IS_PGXC_LOCAL_COORDINATOR || !(IsTransactionState()))
        return;
    foreach(lc, tableList)
    {
        Node *tbl = (Node *) lfirst(lc);

        if(IsA(tbl, RangeTblEntry))
        {
            RangeTblEntry *rte = (RangeTblEntry *)tbl;

            if(rte->rtekind != RTE_RELATION)
                continue;
            if(IsFDWDistributeTable(rte->relid))
            {
                Oid relid = rte->relid;

                if(rte->inh)
                {
                    List       *inhOIDs;
                    ListCell   *l;

                    inhOIDs = find_all_inheritors(relid, NoLock, NULL);
                    foreach(l, inhOIDs)
                    {
                        Oid         childOID = lfirst_oid(l);
                        Relation    crd = relation_open(childOID, NoLock);

                        if(crd->rd_rel->relkind == RELKIND_RELATION)
                            AdjustRelationToForeignTableWorker(crd);
                        relation_close(crd, NoLock);
                    }
                }
                else
                {
                    Relation    rd = relation_open(relid, NoLock);

                    if(rd->rd_rel->relkind == RELKIND_RELATION ||
                            rd->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
                        AdjustRelationToForeignTableWorker(rd);
                    relation_close(rd, NoLock);
                }
                /* also ajust rangeTableEntry */
                if(IS_PGXC_COORDINATOR &&
                        rte->relkind == RELKIND_RELATION)
                    rte->relkind = RELKIND_FOREIGN_TABLE;
            }
            else if(is_all_foreign)
            {
                *is_all_foreign = false;
            }
        }
        /* used for utility process */
        else if(IsA(tbl, RangeVar))
        {
            Relation rd = heap_openrv((RangeVar *)tbl, NoLock);
            AdjustRelationToForeignTableWorker(rd);
            relation_close(rd, NoLock);
        }
    }
}

void
AdjustRelationBackToTable(bool is_durable)
{
    if (RelationSavedCache == NULL || !(IsTransactionState()))
        return;
    if(need_setback)
    {
        HASH_SEQ_STATUS status;
        RelSavedInfoEnt *hentry;

        hash_seq_init(&status, RelationSavedCache);

        while ((hentry = (RelSavedInfoEnt *) hash_seq_search(&status)) != NULL)
        {
            if(hentry->need_setback)    
            {
                Oid relid = hentry->reloid; 
                Relation    rd = relation_open(relid, NoLock); 

                rd->rd_rel->relkind = hentry->real_relkind;
                rd->trigdesc = hentry->real_trigdesc;
                hentry->is_foreign = false;
                if(is_durable)
                    hentry->need_setback = false;
                relation_close(rd, NoLock);
            }
        }
        if(is_durable)
            need_setback = false;
    }
}
void
AdjustRelationBackToForeignTable(void)
{
    if (RelationSavedCache == NULL || !(IsTransactionState()))
        return;
    if(need_setback)
    {
        HASH_SEQ_STATUS status;
        RelSavedInfoEnt *hentry;

        hash_seq_init(&status, RelationSavedCache);

        while ((hentry = (RelSavedInfoEnt *) hash_seq_search(&status)) != NULL)
        {
            if(hentry->need_setback)    
            {
                Oid relid = hentry->reloid; 
                Relation    rd = relation_open(relid, NoLock); 

                rd->rd_rel->relkind = RELKIND_FOREIGN_TABLE;
                rd->trigdesc = hentry->foreign_trigdesc;
                hentry->is_foreign = true;
                relation_close(rd, NoLock);
            }
        }
    }
}
static void
RelationSavedCacheInitialize(void)
{
    if(!RelationSavedCache)
    {
        HASHCTL     ctl;

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(RelSavedInfoEnt);
        RelationSavedCache = hash_create("RelSavedInfo by OID", INITRELCACHESIZE,
                &ctl, HASH_ELEM | HASH_BLOBS);
        CacheRegisterRelcacheCallback(InvalidRelationSavedCache, (Datum) 0);
    }
}
static TriggerDesc *
ExceptInternalTrigger(TriggerDesc *trigdesc)
{
    TriggerDesc *newdesc;
    Trigger    *trigger;
    int         i;
    int j = 0;
    MemoryContext oldContext;

    if (trigdesc == NULL || trigdesc->numtriggers <= 0)
        return NULL;

    oldContext = MemoryContextSwitchTo(CacheMemoryContext);
    newdesc = (TriggerDesc *) palloc0(sizeof(TriggerDesc));

    trigger = (Trigger *) palloc0(trigdesc->numtriggers * sizeof(Trigger));
    newdesc->triggers = trigger;

    for (i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger    *src_trigger;
        if(trigdesc->triggers[i].tgisinternal)
            continue;
        src_trigger = &(trigdesc->triggers[i]);

        memcpy(trigger, src_trigger, sizeof(Trigger));
        trigger->tgname = pstrdup(src_trigger->tgname);
        if (trigger->tgnattr > 0)
        {
            int16      *newattr;

            newattr = (int16 *) palloc(src_trigger->tgnattr * sizeof(int16));
            memcpy(newattr, src_trigger->tgattr,
                    src_trigger->tgnattr * sizeof(int16));
            trigger->tgattr = newattr;
        }
        if (src_trigger->tgnargs > 0)
        {
            char      **newargs;
            int16       j;

            newargs = (char **) palloc(src_trigger->tgnargs * sizeof(char *));
            for (j = 0; j < src_trigger->tgnargs; j++)
                newargs[j] = pstrdup(src_trigger->tgargs[j]);
            trigger->tgargs = newargs;
        }
        if (src_trigger->tgqual)
            trigger->tgqual = pstrdup(src_trigger->tgqual);
        if (src_trigger->tgoldtable)
            trigger->tgoldtable = pstrdup(src_trigger->tgoldtable);
        if (src_trigger->tgnewtable)
            trigger->tgnewtable = pstrdup(src_trigger->tgnewtable);
        trigger++;
        j++;
    }
    newdesc->numtriggers = j;
    for (i = 0; i < j; i++)
        SetTriggerFlags(newdesc, &(newdesc->triggers[i]));
    MemoryContextSwitchTo(oldContext);

    return newdesc;
}
static void
SetTriggerFlags(TriggerDesc *trigdesc, Trigger *trigger)
{
    int16       tgtype = trigger->tgtype;

    trigdesc->trig_insert_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
    trigdesc->trig_insert_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
    trigdesc->trig_insert_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT);
    trigdesc->trig_insert_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
    trigdesc->trig_insert_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
    trigdesc->trig_update_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
    trigdesc->trig_update_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
    trigdesc->trig_update_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE);
    trigdesc->trig_update_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
    trigdesc->trig_update_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
    trigdesc->trig_delete_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
    trigdesc->trig_delete_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
    trigdesc->trig_delete_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
                TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE);
    trigdesc->trig_delete_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
    trigdesc->trig_delete_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
    /* there are no row-level truncate triggers */
    trigdesc->trig_truncate_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_TRUNCATE);
    trigdesc->trig_truncate_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT,
                TRIGGER_TYPE_AFTER, TRIGGER_TYPE_TRUNCATE);

    trigdesc->trig_insert_new_table |=
        (TRIGGER_FOR_INSERT(tgtype) &&
         TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable));
    trigdesc->trig_update_old_table |=
        (TRIGGER_FOR_UPDATE(tgtype) &&
         TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable));
    trigdesc->trig_update_new_table |=
        (TRIGGER_FOR_UPDATE(tgtype) &&
         TRIGGER_USES_TRANSITION_TABLE(trigger->tgnewtable));
    trigdesc->trig_delete_old_table |=
        (TRIGGER_FOR_DELETE(tgtype) &&
         TRIGGER_USES_TRANSITION_TABLE(trigger->tgoldtable));
}
static void
RelationSavedCacheInvalidate(void)
{
    HASH_SEQ_STATUS status;
    RelSavedInfoEnt *hentry;

    hash_seq_init(&status, RelationSavedCache);

    while ((hentry = (RelSavedInfoEnt *) hash_seq_search(&status)) != NULL)
    {
        hentry->real_relkind = '\0';
        if(hentry->real_trigdesc)
        {
            if(hentry->is_foreign)
                FreeTriggerDesc(hentry->real_trigdesc);
            else
                FreeTriggerDesc(hentry->foreign_trigdesc);
        }
        hentry->foreign_trigdesc = NULL;
        hentry->real_trigdesc = NULL;
        hentry->need_setback = false;
        hentry->is_foreign = false;
    }
}

static void
RelationSavedCacheInvalidateEntry(Oid relationId)
{
    RelSavedInfoEnt *rel_saved = NULL;
    RelationSavedInfoLookup(relationId, rel_saved);
    if(rel_saved == NULL)
        return;
    if(rel_saved->real_relkind != '\0')
    {
        rel_saved->real_relkind = '\0';

        if(rel_saved->real_trigdesc)
        {
            if(rel_saved->is_foreign)
                FreeTriggerDesc(rel_saved->real_trigdesc);
            else
                FreeTriggerDesc(rel_saved->foreign_trigdesc);
        }
        rel_saved->foreign_trigdesc = NULL;
        rel_saved->real_trigdesc = NULL;
        rel_saved->need_setback = false;
        rel_saved->is_foreign = false;
    }
}

static void
InvalidRelationSavedCache(Datum argument, Oid relationId)
{
    if(relationId == 0) 
        RelationSavedCacheInvalidate();
    else
        RelationSavedCacheInvalidateEntry(relationId);
}

static bool
IsFDWDistributeTable(Oid relid)
{

    switch (GetRelationLocType(relid))  
    {
        case LOCATOR_TYPE_REPLICATED:
        case LOCATOR_TYPE_HASH:
        case LOCATOR_TYPE_RANGE:
        case LOCATOR_TYPE_RROBIN:
        case LOCATOR_TYPE_MODULO:
        case LOCATOR_TYPE_SHARD:
            return true;
        default:
            return false;
    }
}

bool
CheckPlanValid(PlannedStmt *planned_stmt)
{
    
    pc_plan_valid_context context;

    context.is_invalid = false;
    exec_on_plantree(planned_stmt, is_direct_modify_plan_valid, (void *)&context);
    if(context.is_invalid)
        return false;
    else
        return true;
}

int
CheckFQSValid(PlannedStmt *planned_stmt,
        ParamExternDataInfo *value)
{
    
    pc_param_context context;

    context.params = value;
    context.target_node = -1;
    context.random_node = -1;
    context.fqs_valid = true;

    exec_on_plantree(planned_stmt, eval_target_node_foreignscan, (void *)&context);
    if(context.fqs_valid)
    {
        if(context.target_node > -1)
            return context.target_node;
        else if(context.random_node > -1)
            return context.random_node;
        else
            return -1;
    }
    else
    {
        return -1;
    }
}

void
SetParaminfoToPlan(PlannedStmt *planned_stmt,
        ParamExternDataInfo *value, Datum hash_val, bool is_cursor)
{
    
    pc_param_context context;

    context.params = value;
    context.hash_val = hash_val;
    context.sql_seq = 1; /* 0 is used for FQS */
    context.is_cursor = is_cursor;
    exec_on_plantree(planned_stmt, add_paraminfo_into_foreignscan, (void *)&context);
}

void
SetFQSPlannedStmtToPlan(PlannedStmt *planned_stmt,
        ParamExternDataInfo *value)
{
    
    pc_param_context context;

    context.params = value;
    exec_on_plantree(planned_stmt, add_fqsplan_into_foreignscan, (void *)&context);
}

ParamExternDataInfo *
GetParaminfoFromPlan(PlannedStmt *planned_stmt)
{
    pc_param_context context; 

    context.params = NULL;
    exec_on_plantree(planned_stmt, get_paraminfo_from_foreignscan, (void *)&context);
    return context.params;
}

static void
exec_on_plantree(PlannedStmt *planned_stmt,
        void (*execute_func) (Plan *plan, void *context),
        void *context)
{
    ListCell    *lc;

    execute_func(planned_stmt->planTree, context);

    foreach (lc, planned_stmt->subplans)
    {
        Plan    *subPlan = lfirst(lc);
        execute_func(subPlan, context);
    }
}

static void
params_add_worker(Plan *plan,  void *context)
{
    pc_param_context *param_cxt = (pc_param_context *) context;

    switch(nodeTag(plan))
    {
        case T_ForeignScan:
            {
                ForeignScan *fscan = (ForeignScan *)plan;
                CmdType operation = fscan->operation;

                if(fscan->fdw_private == NULL)
                    return;
                if(operation == CMD_SELECT)
                {
                    setParamsExternIntoForeginScan(fscan->fdw_private, param_cxt->params);
                    param_cxt->sql_seq++;
                    if(param_cxt->params && !CheckForeginScanDistByParam(fscan->fdw_private))
                        param_cxt->params->may_be_fqs = false;
                }
                else if(operation == CMD_UPDATE || operation == CMD_DELETE)
                {
                    setParamsExternIntoForeginDirect(fscan->fdw_private, param_cxt->params);
                    param_cxt->sql_seq++;
                    if(param_cxt->params)
                        param_cxt->params->may_be_fqs = false;
                }
                else
                {
                    if(param_cxt->params)
                        param_cxt->params->may_be_fqs = false;
                }
                if(param_cxt->sql_seq > 1 && param_cxt->params && param_cxt->params->portal_need_name == false)
                    param_cxt->params->portal_need_name = true;

            }
            break;
        case T_ModifyTable:
            {
                int i = 0;
                ListCell *lc = NULL;
                ModifyTable   *modifyplan = (ModifyTable *) plan;
                CmdType operation = modifyplan->operation;

                foreach (lc, modifyplan->plans)
                {
                    List       *fdw_private = (List *) list_nth(modifyplan->fdwPrivLists, i);

                    if(operation == CMD_INSERT)
                    {
                        if(fdw_private == NULL)
                        {
                            ListCell *l = NULL;

                            l = list_nth_cell(modifyplan->fdwPrivLists, 0);
                            lfirst(l) = (void *)list_make1(param_cxt->params);
                        }
                        else if(list_length(fdw_private) == 1)
                        {
                            ListCell *l = NULL;

                            l = list_nth_cell(fdw_private, 0);
                            lfirst(l) = param_cxt->params; 
                        }
                        else
                        {
                            setParamsExternIntoForeginModify(fdw_private, param_cxt->params);
                        }
                        param_cxt->sql_seq++;
                    }
                    else if((operation == CMD_UPDATE ||
                                operation == CMD_DELETE) && fdw_private != NULL)
                    {
                        setParamsExternIntoForeginModify(fdw_private, param_cxt->params);
                        param_cxt->sql_seq++;
                    }
                    if(param_cxt->sql_seq > 1 && param_cxt->params && param_cxt->params->portal_need_name == false)
                        param_cxt->params->portal_need_name = true;
                    i++;
                }
                if(param_cxt->params)
                    param_cxt->params->may_be_fqs = false;
            }
            break;
        default:
            elog(ERROR, "can not operation on this node %d", nodeTag(plan));
    }
    return;
}

static void
fqs_add_worker(Plan *plan,  void *context)
{
    pc_param_context *param_cxt = (pc_param_context *) context;

    switch(nodeTag(plan))
    {
        case T_ForeignScan:
            {
                ForeignScan *fscan = (ForeignScan *)plan;
                CmdType operation = fscan->operation;

                if(fscan->fdw_private == NULL)
                    return;
                if(operation == CMD_SELECT)
                {
                    setFQSPlannedStmtIntoForeginScan(fscan->fdw_private,
                                                      param_cxt->params->fqs_plannedstmt);
                }

            }
            break;
        case T_ModifyTable:
            break;
        default:
            elog(ERROR, "can not operation on this node %d", nodeTag(plan));
    }
    return;
}

static void
params_get_worker(Plan *plan,  void *context)
{
    pc_param_context *param_cxt = (pc_param_context *) context;

    if(param_cxt->params)
        return;

    switch(nodeTag(plan))
    {
        case T_ForeignScan:
            {
                ForeignScan *fscan = (ForeignScan *)plan;
                CmdType operation = fscan->operation;

                if(fscan->fdw_private == NULL)
                    return;
                if(operation == CMD_SELECT)
                    param_cxt->params = getParamsExternFromForeginScan(fscan->fdw_private);
                else if(operation == CMD_UPDATE || operation == CMD_DELETE)
                    param_cxt->params = getParamsExternFromForeginDirect(fscan->fdw_private);
            }
            break;
        case T_ModifyTable:
            {
                int i = 0;
                ListCell *lc = NULL;
                ModifyTable   *modifyplan = (ModifyTable *) plan;
                CmdType operation = modifyplan->operation;

                foreach (lc, modifyplan->plans)
                {
                    List       *fdw_private = (List *) list_nth(modifyplan->fdwPrivLists, i);

                    if(fdw_private == NULL)
                        continue;
                    if(list_length(fdw_private) == 1 &&
                        operation == CMD_INSERT)
                    {
                        if(param_cxt->params == NULL)
                            param_cxt->params = (ParamExternDataInfo *)list_nth(fdw_private, 0);
                    }
                    else if(operation == CMD_INSERT ||
                            operation == CMD_UPDATE ||
                            operation == CMD_DELETE)
                        param_cxt->params = getParamsExternFromForeginModify(fdw_private);
                    i++;
                }
            }
            break;
        default:
            elog(ERROR, "can not operation on this node %d", nodeTag(plan));
    }
    return;
}

static void
check_plan_valid_worker(Plan *plan,  void *context)
{
    pc_plan_valid_context *valid_cxt = (pc_plan_valid_context *) context;

    if(valid_cxt->is_invalid)
        return;

    switch(nodeTag(plan))
    {
        case T_ForeignScan:
            {
                ForeignScan *fscan = (ForeignScan *)plan;
                CmdType operation = fscan->operation;

                if(fscan->fdw_private == NULL)
                    return;
                if(operation == CMD_UPDATE || operation == CMD_DELETE)
                {
                    Oid target_reloid = getTargetRelOidFromForeginDirect(fscan->fdw_private);
                    Relation    relation;
                    TriggerDesc *trigDesc;
                    bool result = false;

                    relation = heap_open(target_reloid, NoLock);

                    trigDesc = relation->trigdesc;
                    switch (operation)
                    {
                        case CMD_UPDATE:
                            if (trigDesc &&
                                    (trigDesc->trig_update_after_row ||
                                     trigDesc->trig_update_before_row))
                                result = true;
                            break;
                        case CMD_DELETE:
                            if (trigDesc &&
                                    (trigDesc->trig_delete_after_row ||
                                     trigDesc->trig_delete_before_row))
                                result = true;
                            break;
                        default:
                            break;
                    }
                    heap_close(relation, NoLock);
                    if(result)
                        valid_cxt->is_invalid = true;               
                }
            }
            break;
        case T_ModifyTable:
            break;
        default:
            elog(ERROR, "can not operation on this node %d", nodeTag(plan));
    }
    return;
}

static void
eval_target_node_warker(Plan *plan,  void *context)
{
    pc_param_context *param_cxt = (pc_param_context *) context;

    if(param_cxt->fqs_valid == false)
        return;

    switch(nodeTag(plan))
    {
        case T_ForeignScan:
            {
                ForeignScan *fscan = (ForeignScan *)plan;
                CmdType operation = fscan->operation;

                if(fscan->fdw_private == NULL)
                {
                    param_cxt->fqs_valid = false;
                    return;
                }
                if(operation == CMD_SELECT)
                {
                    int tmp_target = -1;
                    bool is_replicate = false;

                    tmp_target = evalTargetNodeFromForeginScan(fscan->fdw_private, param_cxt->params, &is_replicate);
                    if(tmp_target < 0)
                    {
                        param_cxt->fqs_valid = false;
                    }
                    else if(param_cxt->target_node < 0)
                    {
                        if(is_replicate)
                            param_cxt->random_node = tmp_target;
                        else
                            param_cxt->target_node = tmp_target;
                    }
                    else if(param_cxt->target_node != tmp_target &&
                            !is_replicate)
                    {
                            param_cxt->fqs_valid = false;
                    }
                }
                else
                {
                    param_cxt->fqs_valid = false;
                }
            }
            break;
        case T_ModifyTable:
            break;
        default:
            elog(ERROR, "can not operation on this node %d", nodeTag(plan));
    }
    return;
}

static void
is_direct_modify_plan_valid(Plan *plan, void *context)
{
    plan_tree_walker(plan, check_plan_valid_worker, context);
}

static void
add_paraminfo_into_foreignscan(Plan *plan, void *context)
{
    plan_tree_walker(plan, params_add_worker, context);
}

static void
add_fqsplan_into_foreignscan(Plan *plan, void *context)
{
    plan_tree_walker(plan, fqs_add_worker, context);
}

static void
eval_target_node_foreignscan(Plan *plan, void *context)
{
    plan_tree_walker(plan, eval_target_node_warker, context);
}

static void
get_paraminfo_from_foreignscan(Plan *plan, void *context)
{
    plan_tree_walker(plan, params_get_worker, context);
}

static void
plan_tree_walker(Plan *plan,
        void (*worker) (Plan *plan, void *context),
        void *context)
{
    ListCell   *lc;

    if (plan == NULL)
        return;

    check_stack_depth();

    switch (nodeTag(plan))
    {
        case T_ForeignScan:
            worker(plan,  context);
            break;
        case T_ModifyTable:
            {
                ModifyTable   *modifyplan = (ModifyTable *) plan;

                worker(plan, context);
                foreach (lc, modifyplan->plans)
                    plan_tree_walker((Plan *) lfirst(lc), worker, context);
            }
            break;

        case T_Append:
            foreach (lc, ((Append *) plan)->appendplans)
                plan_tree_walker((Plan *) lfirst(lc), worker, context);
            break;

        case T_MergeAppend:
            foreach (lc, ((MergeAppend *) plan)->mergeplans)
                plan_tree_walker((Plan *) lfirst(lc), worker, context);
            break;

        case T_BitmapAnd:
            foreach (lc, ((BitmapAnd *) plan)->bitmapplans)
                plan_tree_walker((Plan *) lfirst(lc), worker, context);
            break;

        case T_BitmapOr:
            foreach (lc, ((BitmapOr *) plan)->bitmapplans)
                plan_tree_walker((Plan *) lfirst(lc), worker, context);
            break;
        case T_SubqueryScan:
            plan_tree_walker(((SubqueryScan *) plan)->subplan, worker, context);
            break;
        case T_CustomScan:
            foreach (lc, ((CustomScan *) plan)->custom_plans)
                plan_tree_walker((Plan *) lfirst(lc), worker, context);
            break;
        default:
            break;
    }

    plan_tree_walker(plan->lefttree, worker, context);
    plan_tree_walker(plan->righttree, worker, context);
}
