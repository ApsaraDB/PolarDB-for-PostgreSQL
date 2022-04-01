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


typedef struct relsavedinfoent
{
    Oid         reloid;
    char        real_relkind;
    bool        need_setback;
    bool        is_foreign;
    TriggerDesc *real_trigdesc;
    TriggerDesc *foreign_trigdesc;
} RelSavedInfoEnt;

static HTAB *RelationSavedCache;
static bool need_setback = false;

static TriggerDesc *ExceptInternalTrigger(TriggerDesc *trigdesc);
static void SetTriggerFlags(TriggerDesc *trigdesc, Trigger *trigger);
static bool ExtractRelationRangeTableListWalker(Node *node, List **rangeTableEntryList);
static bool polarx_distributable_rangeTableEntry(RangeTblEntry *rangeTableEntry);
static void AdjustRelationToForeignTableWorker(Relation rd);
static void RelationSavedCacheInitialize(void);
static void RelationSavedCacheInvalidate(void);
static void RelationSavedCacheInvalidateEntry(Oid relationId);
static void InvalidRelationSavedCache(Datum argument, Oid relationId);
static void AdjustRelationCallback(XactEvent event, void *arg);
static bool IsFDWDistributeTable(Oid relid);

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
ExtractRelationRangeTableListWalker(Node *node, List **rangeTableEntryList)
{
    bool done = false;

    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, RangeTblEntry))
    {
        RangeTblEntry *rangeTable = (RangeTblEntry *) node;

        if (polarx_distributable_rangeTableEntry(rangeTable))
        {
            (*rangeTableEntryList) = lappend(*rangeTableEntryList, rangeTable);
        }
    }
    else if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        if (query->hasSubLinks || query->cteList || query->setOperations)
        {
            done = query_tree_walker(query,
                                    ExtractRelationRangeTableList,
                                    rangeTableEntryList,
                                    QTW_EXAMINE_RTES);
        }
        else
        {
            done = range_table_walker(query->rtable,
                                    ExtractRelationRangeTableList,
                                    rangeTableEntryList,
                                    QTW_EXAMINE_RTES);
        }
    }
    else
    {
        done = expression_tree_walker(node, ExtractRelationRangeTableList,
                                    rangeTableEntryList);
    }

    return done;
}


/*
 * polarxDistributableRangeTableEntry returns true if the input range table
 * entry is a relation and it can be used in fdw planner.
 */
static bool
polarx_distributable_rangeTableEntry(RangeTblEntry *rangeTableEntry)
{
	char relationKind = '\0';

	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return false;
	}

	relationKind = rangeTableEntry->relkind;
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
	{
		/*
         * RELKIND_RELATION is a table, RELKIND_PARTITIONED_TABLE is partition table.
         * partition table's child table will be used as distributed table.
		 */
		return true;
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
            if(rel_saved && !rel_saved->is_foreign)
            {
                rangeTableEntry->relkind = RELKIND_RELATION;
                return true;
            }
        }
        
    }

	return false;
}

bool
ExtractRelationRangeTableList(Node *node, List **rangeTableList)
{
    if(IS_PGXC_LOCAL_COORDINATOR)
        return ExtractRelationRangeTableListWalker(node, rangeTableList);
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
AdjustRelationToForeignTable(List *tableList)
{
    ListCell *lc = NULL;

    if(!IS_PGXC_LOCAL_COORDINATOR)
        return;
    foreach(lc, tableList)
    {
        Node *tbl = (Node *) lfirst(lc);

        if(IsA(tbl, RangeTblEntry))
        {
            RangeTblEntry *rte = (RangeTblEntry *)tbl;

            if(rte->rtekind == RTE_RELATION &&
                    IsFDWDistributeTable(rte->relid))
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
    if (RelationSavedCache == NULL)
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
    if (RelationSavedCache == NULL)
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
        RegisterXactCallback(AdjustRelationCallback, NULL);
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
            return true;
        default:
            return false;
    }
}

static void
AdjustRelationCallback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_PARALLEL_COMMIT:
        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
        case XACT_EVENT_PREPARE:
            AdjustRelationBackToTable(true);
            break;
        default:
            break;
    }
}
