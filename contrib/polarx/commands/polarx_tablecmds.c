/*-------------------------------------------------------------------------
 * polarx_tablecmds.c
 *    support for propagation of  Commands for creating and altering table 
 *    structures and settings for polarx 
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_tablecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx.h"
#include "pgxc/locator.h"
#include "commands/polarx_tablecmds.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"


static bool CheckLocalIndexColumn (char loctype, char *partcolname, char *indexcolname);
/*
 * IsTempTable
 *
 * Check if given table Oid is temporary.
 */
bool
IsTempTable(Oid relid)
{
    Relation    rel;
    bool        res;
    /*
     *      * PGXCTODO: Is it correct to open without locks?
     *           * we just check if this table is temporary though...
     *                */
    rel = relation_open(relid, NoLock);
    res = rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP;
    relation_close(rel, NoLock);
    return res;
}

bool
IsLocalTempTable(Oid relid)
{
    Relation    rel;
    bool        res;
    rel = relation_open(relid, NoLock);
    res = (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
            GetRelationLocInfo(relid) == NULL);
    relation_close(rel, NoLock);
    return res;
}

/*
 * IsIndexUsingTemp
 *
 * Check if given index relation uses temporary tables.
 */
bool
IsIndexUsingTempTable(Oid relid)
{
    bool res = false;
    HeapTuple   tuple;
    Oid parent_id = InvalidOid;

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tuple))
    {
        Form_pg_index index = (Form_pg_index) GETSTRUCT(tuple);
        parent_id = index->indrelid;

        /* Release system cache BEFORE looking at the parent table */
        ReleaseSysCache(tuple);

        res = IsTempTable(parent_id);
    }
    else
        res = false; /* Default case */

    return res;
}

void
AlterTablePrepCmds(AlterTableStmt *atstmt)
{
    List *cmds = atstmt->cmds;

    if(atstmt != NULL && atstmt->relkind == OBJECT_TABLE)
    {
        ListCell   *lcmd;
        Relation    rel;
        RelationLocInfo *rd_locator_info = NULL;

        rel = heap_openrv(atstmt->relation, AccessShareLock);
        rd_locator_info = GetRelationLocInfo(RelationGetRelid(rel));
        if(rd_locator_info == NULL)
            return;


        foreach(lcmd, cmds)
        {
            AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

            switch (cmd->subtype)
            {
                case AT_AddColumn:      /* may rewrite heap, in some cases and visible
                                         * to SELECT */
                    if (IsA(cmd->def, ColumnDef) && ((ColumnDef *)cmd->def)->constraints)
                    {
                        ListCell   *clist;
                        ColumnDef *cdf = (ColumnDef *)cmd->def;

                        foreach(clist, cdf->constraints)
                        {
                            Constraint *constraint = lfirst_node(Constraint, clist);

                            if(constraint->contype == CONSTR_PRIMARY
                                    && rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED
                                    && (rd_locator_info->partAttrName == NULL
                                        || strcmp(rd_locator_info->partAttrName, cdf->colname) != 0))
                            {
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("Alter table: primary key must contain distribute col")));
                            }
                            else if(constraint->contype == CONSTR_UNIQUE
                                    && rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED
                                    && (rd_locator_info->partAttrName == NULL
                                        || strcmp(rd_locator_info->partAttrName, cdf->colname) != 0))
                            {
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("Alter table: unique constraint must contain distribute col")));
                            }
                            else if(constraint->contype == CONSTR_FOREIGN)
                            {
                                if(strcmp(atstmt->relation->relname, constraint->pktable->relname) != 0)
                                {
                                    Relation    frel;
                                    frel = heap_openrv(constraint->pktable, AccessShareLock);
                                    switch (GetRelationLocType(RelationGetRelid(frel)))
                                    {
                                        case LOCATOR_TYPE_RROBIN:
                                            ereport(ERROR,
                                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                     errmsg("Alter table: foreign key can't referenced to a roundrobin distribute table")));
                                            break;
                                        case LOCATOR_TYPE_REPLICATED:
                                            break;
                                        case LOCATOR_TYPE_HASH:
                                            if(rd_locator_info->locatorType == LOCATOR_TYPE_HASH)
                                                break;
                                        case LOCATOR_TYPE_MODULO:
                                            if(rd_locator_info->locatorType == LOCATOR_TYPE_MODULO)
                                                break;
                                        default:
                                            ereport(ERROR,
                                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                     errmsg("Alter table: the distribute type is not same with the reference table")));
                                            break;
                                    }
                                    if(rd_locator_info->partAttrName != NULL
                                            && strcmp(rd_locator_info->partAttrName, cdf->colname) != 0)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("Alter table: foreign key constraints colume must be same with distribute col")));
                                    heap_close(frel, NoLock);
                                }
                                else if(rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED)
                                {
                                    if(rd_locator_info->partAttrName != NULL
                                            && strcmp(rd_locator_info->partAttrName, cdf->colname) != 0)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("Alter table: foreign key constraints colume must be same with distribute col")));
                                }
                            }
                        }
                    }
                    break;
                case AT_SetTableSpace:  /* must rewrite heap */
                case AT_AlterColumnType:        /* must rewrite heap */
                case AT_AddOids:        /* must rewrite heap */
                case AT_SetStorage: /* may add toast tables, see ATRewriteCatalogs() */
                case AT_DropConstraint: /* as DROP INDEX */
                case AT_DropNotNull:    /* may change some SQL plans */
                case AT_DropColumn: /* change visible to SELECT */
                case AT_AddColumnToView:        /* CREATE VIEW */
                case AT_DropOids:       /* calls AT_DropColumn */
                case AT_EnableAlwaysRule:       /* may change SELECT rules */
                case AT_EnableReplicaRule:      /* may change SELECT rules */
                case AT_EnableRule: /* may change SELECT rules */
                case AT_DisableRule:    /* may change SELECT rules */
                case AT_ChangeOwner:    /* change visible to SELECT */
                case AT_GenericOptions:
                case AT_AlterColumnGenericOptions:
                case AT_EnableTrig:
                case AT_EnableAlwaysTrig:
                case AT_EnableReplicaTrig:
                case AT_EnableTrigAll:
                case AT_EnableTrigUser:
                case AT_DisableTrig:
                case AT_DisableTrigAll:
                case AT_DisableTrigUser:
                case AT_ColumnDefault:
                case AT_AlterConstraint:
                    break;
                case AT_AddIndex:       /* from ADD CONSTRAINT */
                    {
                        IndexStmt *stmt = (IndexStmt *)cmd->def;

                        DefineIndexCheck(RelationGetRelid(rel), stmt);
                    }
                    break;
                case AT_AddIndexConstraint:
                case AT_ReplicaIdentity:
                case AT_SetNotNull:
                case AT_EnableRowSecurity:
                case AT_DisableRowSecurity:
                case AT_ForceRowSecurity:
                case AT_NoForceRowSecurity:
                case AT_AddIdentity:
                case AT_DropIdentity:
                case AT_SetIdentity:
                    break;
                case AT_AddConstraint:
                case AT_ProcessedConstraint:    /* becomes AT_AddConstraint */
                case AT_AddConstraintRecurse:   /* becomes AT_AddConstraint */
                case AT_ReAddConstraint:        /* becomes AT_AddConstraint */
                case AT_ReAddDomainConstraint:  /* becomes AT_AddConstraint */
                    if (IsA(cmd->def, Constraint))
                    {
                        Constraint *con = (Constraint *) cmd->def;

                        switch (con->contype)
                        {
                            case CONSTR_UNIQUE:
                            case CONSTR_PRIMARY:
                                if(rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("Alter table: roundrobin distribute type not support primary key or unique constraint")));
                                if(rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED)
                                {
                                    if(con->keys)
                                    {
                                        ListCell *ckcl = NULL;
                                        foreach(ckcl, con->keys)
                                        {
                                            if(strcmp(strVal(lfirst(ckcl)), rd_locator_info->partAttrName) == 0)
                                                break;
                                        }
                                        if(ckcl == NULL)
                                            ereport(ERROR,
                                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                     errmsg("Alter table: primary key or unique constraint must contain distribute col")));
                                    }
                                    else if(con->indexname != NULL)
                                    {
                                        Relation    index_rel;
                                        Form_pg_index index_form;
                                        Oid     index_oid;
                                        int         i;

                                        index_oid = get_relname_relid(con->indexname, RelationGetNamespace(rel));
                                        index_rel = index_open(index_oid, AccessShareLock);
                                        index_form = index_rel->rd_index;
                                        for (i = 0; i < index_form->indnatts; i++)
                                        {
                                            int16       attnum = index_form->indkey.values[i];
                                            Form_pg_attribute attform;
                                            if (attnum > 0)
                                            {
                                                Assert(attnum <= rel->rd_att->natts);
                                                attform = TupleDescAttr(rel->rd_att, attnum - 1);;
                                            }
                                            else
                                                attform = SystemAttributeDefinition(attnum,
                                                        rel->rd_rel->relhasoids);
                                            if(strcmp(NameStr(attform->attname), rd_locator_info->partAttrName) == 0)
                                                break;
                                        }
                                        if(i == index_form->indnatts)
                                            ereport(ERROR,
                                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                     errmsg("Alter table: primary key or unique constraint must contain distribute col")));
                                        relation_close(index_rel, NoLock);
                                    }
                                    else
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("Alter table: primary key or unique is empty")));
                                }
                                break;
                            case CONSTR_FOREIGN:
                                {
                                    if(strcmp(atstmt->relation->relname, con->pktable->relname) != 0)
                                    {
                                        Relation    frel;

                                        frel = heap_openrv(con->pktable, AccessShareLock);
                                        switch (GetRelationLocType(RelationGetRelid(frel)))
                                        {
                                            case LOCATOR_TYPE_RROBIN:
                                                ereport(ERROR,
                                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                         errmsg("Alter table: foreign key can't referenced to a roundrobin distribute table")));
                                                break;
                                            case LOCATOR_TYPE_REPLICATED:
                                                break;
                                            case LOCATOR_TYPE_HASH:
                                                if(rd_locator_info->locatorType == LOCATOR_TYPE_HASH)
                                                    break;
                                            case LOCATOR_TYPE_MODULO:
                                                if(rd_locator_info->locatorType == LOCATOR_TYPE_MODULO)
                                                    break;
                                            default:
                                                ereport(ERROR,
                                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                         errmsg("Alter table: the distribute type is not same with the reference table")));
                                                break;
                                        }

                                        if(rd_locator_info->partAttrName != NULL && con->fk_attrs)
                                        {
                                            ListCell *fkcl = NULL;
                                            foreach(fkcl, con->fk_attrs)
                                            {
                                                if(strcmp(strVal(lfirst(fkcl)), rd_locator_info->partAttrName) == 0)
                                                    break;
                                            }
                                            if(fkcl == NULL)
                                                ereport(ERROR,
                                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                         errmsg("Alter table: foreign key must contain distribute col")));
                                        }
                                        heap_close(frel, NoLock);
                                    }
                                    else if(rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED)
                                    {
                                        if(rd_locator_info->partAttrName != NULL && con->fk_attrs)
                                        {
                                            ListCell *fkcl = NULL;
                                            foreach(fkcl, con->fk_attrs)
                                            {
                                                if(strcmp(strVal(lfirst(fkcl)), rd_locator_info->partAttrName) == 0)
                                                    break;
                                            }
                                            if(fkcl == NULL)
                                                ereport(ERROR,
                                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                         errmsg("Alter table: foreign key must contain distribute col")));
                                        }
                                    }
                                    break;
                                }
                            case CONSTR_EXCLUSION:
                                if(rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("Alter table: roundrobin distribute type not support exclude constraint")));
                                if(rd_locator_info->locatorType != LOCATOR_TYPE_REPLICATED)
                                {
                                    ListCell *index_elem;

                                    foreach(index_elem, con->exclusions)
                                    {
                                        IndexElem *index_val = (IndexElem *)(linitial(lfirst(index_elem)));

                                        if(index_val->name
                                                && strcmp(index_val->name, rd_locator_info->partAttrName) == 0)
                                            break;
                                    }
                                    if(index_elem == NULL)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("exclude constraint must contain distribute col")));
                                }
                                break;

                            default:
                                break;
                        }
                    }
                    break;
                case AT_AddInherit:
                case AT_DropInherit:
                case AT_AddOf:
                case AT_DropOf:
                case AT_SetStatistics:  /* Uses MVCC in getTableAttrs() */
                case AT_ClusterOn:      /* Uses MVCC in getIndexes() */
                case AT_DropCluster:    /* Uses MVCC in getIndexes() */
                case AT_SetOptions: /* Uses MVCC in getTableAttrs() */
                case AT_ResetOptions:   /* Uses MVCC in getTableAttrs() */
                case AT_SetLogged:
                case AT_SetUnLogged:
                case AT_ValidateConstraint: /* Uses MVCC in getConstraints() */
                case AT_SetRelOptions:
                case AT_ResetRelOptions:
                case AT_ReplaceRelOptions:
                case AT_AttachPartition:
                case AT_DetachPartition:
                    break;
                default:                        /* oops */
                    elog(ERROR, "unrecognized alter table type: %d",
                            (int) cmd->subtype);
                    break;
            }
        }
        heap_close(rel, NoLock);
    }
    return;
}

/*
 * unique index must contain distributy key if it does have
 */
void
DefineIndexCheck(Oid relationId, IndexStmt *stmt)
{
    if(IS_PGXC_LOCAL_COORDINATOR &&
        stmt && (stmt->primary || stmt->unique))
    {
        Relation    rel;

        rel = heap_open(relationId, NoLock);

        if(rel->rd_rel->relkind != RELKIND_MATVIEW)
        {
            ListCell *elem;
            bool isSafe = false;
            RelationLocInfo *rd_locator_info = GetRelationLocInfo(relationId);

            foreach(elem, stmt->indexParams)
            {
                IndexElem  *key = (IndexElem *) lfirst(elem);

                if (rd_locator_info == NULL)
                {
                    isSafe = true;
                    break;
                }

                if (CheckLocalIndexColumn(rd_locator_info->locatorType, 
                            rd_locator_info->partAttrName, key->name))
                {
                    isSafe = true;
                    break;
                }
            }
            if (!isSafe)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                         errmsg("Unique index of distributed table must contain the hash/modulo distribution column.")));
            }
        }
        heap_close(rel, NoLock);
            
    }
}
static bool
CheckLocalIndexColumn (char loctype, char *partcolname, char *indexcolname)
{
    if (IsLocatorReplicated(loctype))
        /* always safe */
        return true;
    if (loctype == LOCATOR_TYPE_RROBIN)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                 errmsg("Cannot locally enforce a unique index on round robin distributed table.")));
    else if (loctype == LOCATOR_TYPE_HASH || loctype == LOCATOR_TYPE_MODULO)
    {
        if (partcolname && indexcolname && strcmp(partcolname, indexcolname) == 0)
            return true;
    }
    return false;
}
