/*-------------------------------------------------------------------------
 * parse_distribute.c
 *     parse distribute info related functionality for Create Table .
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/parser/parse_distribute.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "polarx.h"
#include "parser/parse_distribute.h"
#include "catalog/namespace.h"
#include "nodes/polarx_node.h"
#include "nodes/parsenodes.h"
#include "commands/defrem.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "pgxc/locator.h"

static DistributeBy *buildDistbyFromTableOption(List *defList);
static bool interpretDistirbuteType(char *type, DistributionType *dist_type, bool *need_dist_col);
static bool isColumeDistribute(const TypeName *typeName);
static DistributeBy * buildDistbyFromCols(CreateStmt *stmt);
static DistributeBy *buildDistbyFromInhTable(CreateStmt *stmt);
static DistributeBy *buildDistbyFromLikeStmt(List *colDefs);
static void validDistColInColDef(char *dist_col, CreateStmt *stmt);


List * 
extractPolarxTableOption(List **defList)
{
    ListCell   *lc;
    ListCell   *nextlc;
    List *res_tlist = NIL;

    
    for (lc = list_head(*defList); lc != NULL; lc = nextlc)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        nextlc = lnext(lc);

        if (strcmp(def->defname, "dist_type") == 0 ||
                strcmp(def->defname, "dist_col") == 0 ||
                strcmp(def->defname, "is_local") == 0)
        {
            res_tlist = lappend(res_tlist, def);
            *defList = list_delete_ptr(*defList, def);
            continue;
        }
    }
    if(*defList == NULL)
        defList = NULL;

    return res_tlist;
}
bool 
isCreateLocalTable(List *defList)
{
    ListCell   *cell;
    bool is_local = false;

    foreach(cell, defList)
    {
        DefElem    *def = (DefElem *) lfirst(cell);

        if (strcmp(def->defname, "is_local") == 0)
        {
            
            is_local =  defGetBoolean(def);
        }
    }

    return is_local;
}

static bool
interpretDistirbuteType(char *type, DistributionType *dist_type, bool *need_dist_col)
{
    if(type == NULL)
        return false;
    else if(strcmp(type, "hash") == 0)
    {
        if(need_dist_col)
            *need_dist_col = true;
        if(dist_type)
            *dist_type = DISTTYPE_HASH;
        return true;
    }
    else if(strcmp(type, "modulo") == 0)
    {
        if(need_dist_col)
            *need_dist_col = true;
        if(dist_type)
            *dist_type = DISTTYPE_MODULO;

        return true;
    }
    else if(strcmp(type, "replication") == 0)
    {
        if(need_dist_col)
            *need_dist_col = false;
        if(dist_type)
            *dist_type = DISTTYPE_REPLICATION;

        return true;
    }
    else if(strcmp(type, "roundrobin") == 0)
    {
        if(need_dist_col)
            *need_dist_col = false;
        if(dist_type)
            *dist_type = DISTTYPE_ROUNDROBIN;

        return true;
    }
    else
        return false;
}

static DistributeBy * 
buildDistbyFromTableOption(List *defList)
{
    ListCell   *cell;
    bool need_dist_col = false;
    DistributeBy *dist_by = NULL;
    bool is_local = false;

    /* Scan list to see if OIDS was included */
    foreach(cell, defList)
    {
        DefElem    *def = (DefElem *) lfirst(cell);

        if (def->defnamespace != NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("dist_type parameter's namespace should be NULL ")));
        }

        if (strcmp(def->defname, "dist_type") == 0)
        {
            char *value;

            /*
             * Error out if the namespace is not NULL.  A NULL namespace is
             * always needed for polarx distribute option.
             */
            if(dist_by != NULL)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("duplicate dist_type parameter")));
            }
            if(def->arg == NULL)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("dist_type parameter's value should not be NULL")));
            }

            dist_by = polarxMakeNode(DistributeBy);
            /*
             * Flatten the DefElem into a text string like "name=arg". If we
             * have just "name", assume "name=true" is meant.  Note: the
             * namespace is not output.
             */
            value = defGetString(def);

            if(!interpretDistirbuteType(value, &(dist_by->disttype) ,&need_dist_col))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("dist_type parameter's value %s is not valid, we currnetly support \
                         hash, modulo, replication, roundrobin", value)));
        }
        else if(strcmp(def->defname, "dist_col") == 0)
        {
            char *value;

            if(dist_by && dist_by->colname != NULL)            
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("dist_col %s parameter is duplicated.", dist_by->colname)));
            if(def->arg == NULL)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("dist_col parameter's value should not be NULL")));
            }

            if(dist_by && !need_dist_col)
            {
                ereport(WARNING,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("dist_col parameter is ignored, because of dist_type %d does not need it \
                             ", dist_by->disttype)));
                continue;
            }
            if(dist_by == NULL)
            {
                dist_by = polarxMakeNode(DistributeBy);
                dist_by->disttype = DISTTYPE_HASH;
            }
            value = defGetString(def);
            dist_by->colname = pstrdup(value); 
        }
        else if(strcmp(def->defname, "is_local") == 0)
        {
            is_local = defGetBoolean(def);
            if(is_local)
            {
                if(dist_by)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("local table does not support distribute option")));
                    pfree(dist_by);
                }
                return NULL;
            }
        }
    }
    if(need_dist_col &&
            dist_by->disttype == DISTTYPE_MODULO &&
            dist_by->colname == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("dist_col is needed for dist_type %d", dist_by->disttype)));


    return dist_by;
}

DistributeBy *
buildDistributeBy(List *defList, CreateStmt *stmt, List *orgColDefs)
{
    DistributeBy *dist_by = NULL;
    DistributeBy *dist_by_like = NULL;

    dist_by = buildDistbyFromTableOption(defList);

    if(dist_by && dist_by->colname)
        validDistColInColDef(dist_by->colname, stmt);

    dist_by_like = buildDistbyFromLikeStmt(orgColDefs);

    if((dist_by && dist_by_like) &&
            dist_by->disttype != DISTTYPE_REPLICATION &&
            (dist_by->disttype != dist_by_like->disttype ||
              strcmp(dist_by->colname, dist_by_like->colname) != 0))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("user set distribute options is conflict with like table's indexes")));
    }

    if(stmt->inhRelations)
    {
        if(dist_by)
            ereport(WARNING,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("Inherited/partition tables inherit"
                         " distribution from the parent"),
                     errdetail("Explicitly specified distribution will be ignored")));
        if(dist_by_like)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("distribute info from like options conflict with inherit table"),
                     errdetail("the distribute key from like is unique"
                                ",but the inherit table distribute info will replace it.")));
        dist_by = buildDistbyFromInhTable(stmt);
    }

    if(dist_by == NULL && dist_by_like)
        dist_by = dist_by_like;

    if(dist_by == NULL || 
        (dist_by->colname == NULL &&
            dist_by->disttype == DISTTYPE_HASH))
        dist_by = buildDistbyFromCols(stmt);

    return dist_by;

}

DistributeBy *
buildDistributeByForIntoClause(List *distList, List *colDefs)
{
    DistributeBy *dist_by = NULL;

    dist_by = buildDistbyFromTableOption(distList);

    if(dist_by == NULL)
    {
        ListCell *lc = NULL;

        dist_by = polarxMakeNode(DistributeBy);

        foreach(lc, colDefs)
        {
            ColumnDef  *col = (ColumnDef *)lfirst(lc);
            if(IsTypeHashDistributable(col->typeName->typeOid))
            {
                dist_by->disttype = DISTTYPE_HASH;
                dist_by->colname = col->colname;
                break;
            }
        }
        if(lc == NULL)
        {
            dist_by->disttype = DISTTYPE_ROUNDROBIN;
            dist_by->colname = NULL;
        }
    }

    return dist_by;
}
static bool
isColumeDistribute(const TypeName *typeName)
{
    char       *schemaname;
    char       *typname;
    Oid         typoid;

    if(typeName->typeOid)
    {
        typoid = typeName->typeOid;
    }
    else
    {
        /* deconstruct the name list */
        DeconstructQualifiedName(typeName->names, &schemaname, &typname);

        if (schemaname)
        {
            /* Look in specific schema only */
            Oid         namespaceId;

            namespaceId = LookupExplicitNamespace(schemaname, false);
            typoid = GetSysCacheOid2(TYPENAMENSP,
                    PointerGetDatum(typname),
                    ObjectIdGetDatum(namespaceId));
        }
        else
        {
            /* Unqualified type name, so search the search path */
            typoid = TypenameGetTypid(typname);
        }
    }

    if (typeName->arrayBounds != NIL)
        typoid = get_array_type(typoid);

    if(IsTypeHashDistributable(typoid))
        return true;
    else
        return false;
}
static DistributeBy *
buildDistbyFromLikeStmt(List *colDefs)
{
    ListCell   *cell;
    DistributeBy *dist_by = NULL;

    foreach(cell, colDefs)
    {
        Node       *element = lfirst(cell);

        if(dist_by)
            break;
        switch (nodeTag(element))
        {
            case T_TableLikeClause:
            {
                TableLikeClause *table_like_clause = (TableLikeClause *) element;

                if(table_like_clause->options & CREATE_TABLE_LIKE_INDEXES)
                {
                    Relation relation = relation_openrv(table_like_clause->relation, AccessShareLock); 

                    if(relation->rd_rel->relhasindex)
                    {
                        List       *parent_indexes;
                        ListCell   *l;

                        parent_indexes = RelationGetIndexList(relation);
                        foreach(l, parent_indexes)
                        {
                            Oid         parent_index_oid = lfirst_oid(l);
                            Relation    parent_index;

                            parent_index = index_open(parent_index_oid, AccessShareLock);
                            if(parent_index->rd_index->indisunique
                                    || parent_index->rd_index->indisprimary
                                    || parent_index->rd_index->indisexclusion)
                            {
                                dist_by = polarxMakeNode(DistributeBy);
                                switch (GetRelationLocType(RelationGetRelid(relation)))
                                {
                                    case LOCATOR_TYPE_REPLICATED:
                                        dist_by->disttype = DISTTYPE_REPLICATION;
                                        dist_by->colname = NULL;
                                        break;
                                    case LOCATOR_TYPE_HASH:
                                        dist_by->disttype = DISTTYPE_HASH;
                                        dist_by->colname = GetRelationDistColumn(RelationGetRelid(relation));
                                        break;
                                    case LOCATOR_TYPE_MODULO:
                                        dist_by->disttype = DISTTYPE_MODULO;
                                        dist_by->colname = GetRelationDistColumn(RelationGetRelid(relation));
                                        break;
                                    default:
                                        pfree(dist_by);
                                        dist_by = NULL;
                                        break;
                                }
                            }
                            index_close(parent_index, AccessShareLock);
                        }
                    }
                    heap_close(relation, NoLock);
                }
                break;
            }
            default:
                break;
        }
    }
    return dist_by;
}
static void
validDistColInColDef(char *dist_col, CreateStmt *stmt)
{
    ListCell   *cell;
    bool is_valid = false;

    Assert(stmt != NULL);

    if(dist_col == NULL)
        return;
    foreach(cell, stmt->tableElts)
    {
        Node       *element = lfirst(cell);

        switch (nodeTag(element))
        {
            case T_ColumnDef:
                if (!isColumeDistribute(((ColumnDef *)element)->typeName))
                    continue;
                if(strcmp(dist_col, ((ColumnDef *)element)->colname) == 0)
                {
                    is_valid = true;
                }
                break;
            default:
                elog(ERROR, "unrecognized node type: %d",
                        (int) nodeTag(element));
        }
        if(is_valid)
            break;
    }
    if(!is_valid)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("dist_col %s is not colume of this table",dist_col)));
}
static DistributeBy *
buildDistbyFromCols(CreateStmt *stmt)
{
    ListCell   *cell;
    DistributeBy *dist_by = NULL;
    bool is_unique = false;

    Assert(stmt != NULL);

    foreach(cell, stmt->tableElts)
    {
        Node       *element = lfirst(cell);

        switch (nodeTag(element))
        {
            case T_ColumnDef:
                if(is_unique)
                    break;
                if (!isColumeDistribute(((ColumnDef *)element)->typeName))
                    continue;
                if(((ColumnDef *) element)->constraints)
                {
                    ListCell   *clist;

                    foreach(clist, ((ColumnDef *) element)->constraints)
                    {
                        Constraint *constraint = lfirst_node(Constraint, clist);

                        if(constraint->contype == CONSTR_PRIMARY ||
                                constraint->contype == CONSTR_UNIQUE ||
                                constraint->contype == CONSTR_FOREIGN)
                        {
                            is_unique = true;
                            break;
                        }
                    }
                }
                if(dist_by == NULL)
                    dist_by = polarxMakeNode(DistributeBy);

                if(dist_by->colname == NULL 
                        || (is_unique && dist_by->colname != NULL))
                {
                    dist_by->disttype = DISTTYPE_HASH;
                    dist_by->colname = pstrdup(((ColumnDef *)element)->colname);
                }
                break;
            default:
                elog(ERROR, "unrecognized node type: %d",
                        (int) nodeTag(element));
        }
    }
    if(dist_by == NULL)
    {
        dist_by = polarxMakeNode(DistributeBy);
        dist_by->disttype = DISTTYPE_ROUNDROBIN;
        dist_by->colname = NULL;
    }
    return dist_by;
}

static DistributeBy *
buildDistbyFromInhTable(CreateStmt *stmt)
{
    DistributeBy *dist_by = NULL;
    RangeVar   *inh = (RangeVar *) linitial(stmt->inhRelations);
    Relation    rel;
    RelationLocInfo *rd_locator_info;

    Assert(stmt != NULL);
    Assert(IsA(inh, RangeVar));

    rel = heap_openrv(inh, AccessShareLock);
    if ((rel->rd_rel->relkind != RELKIND_RELATION) &&
            (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE))
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("inherited relation \"%s\" is not a table",
                     inh->relname)));

    rd_locator_info = GetRelationLocInfo(RelationGetRelid(rel));
    if (!rd_locator_info)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("parent table \"%s\" is not distributed, but "
                     "distribution is specified for the child table \"%s\"",
                     RelationGetRelationName(rel),
                     stmt->relation->relname)));
    dist_by = polarxMakeNode(DistributeBy);


    if (rd_locator_info)
    {
        switch (rd_locator_info->locatorType)
        {
            case LOCATOR_TYPE_HASH:
                dist_by->disttype = DISTTYPE_HASH;
                dist_by->colname =
                    pstrdup(rd_locator_info->partAttrName);
                break;

            case LOCATOR_TYPE_MODULO:
                dist_by->disttype = DISTTYPE_MODULO;
                dist_by->colname =
                    pstrdup(rd_locator_info->partAttrName);
                break;
            case LOCATOR_TYPE_REPLICATED:
                dist_by->disttype = DISTTYPE_REPLICATION;
                break;
            case LOCATOR_TYPE_RROBIN:
            default:
                dist_by->disttype = DISTTYPE_ROUNDROBIN;
                break;
        }
    }
    heap_close(rel, NoLock);

    return dist_by;
}
void 
validDistbyOnTableConstrants(DistributeBy *dist_by, List *colDefs, CreateStmt *stmt)
{
    ListCell   *cell;

    Assert(dist_by != NULL);
    foreach(cell, colDefs)
    {
        Node       *element = lfirst(cell);

        switch (nodeTag(element))
        {
            case T_ColumnDef:
                if(((ColumnDef *) element)->constraints)
                {
                    ListCell   *clist;
                    foreach(clist, ((ColumnDef *) element)->constraints)
                    {
                        Constraint *constraint = lfirst_node(Constraint, clist);
                        if(constraint->contype == CONSTR_PRIMARY
                                && dist_by->disttype != DISTTYPE_REPLICATION
                                && (dist_by->colname == NULL
                                    || strcmp(dist_by->colname, ((ColumnDef *)element)->colname) != 0))
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("primary key must contain distribute col")));
                        }
                        else if(constraint->contype == CONSTR_UNIQUE
                                && dist_by->disttype != DISTTYPE_REPLICATION
                                && (dist_by->colname == NULL
                                    || strcmp(dist_by->colname, ((ColumnDef *)element)->colname) != 0))
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("unique constraint must contain distribute col")));
                        }
                        else if(constraint->contype == CONSTR_FOREIGN)
                        {
                            if(strcmp(stmt->relation->relname, constraint->pktable->relname) != 0)
                            {
                                Relation    rel;
                                rel = heap_openrv(constraint->pktable, AccessShareLock);
                                switch (GetRelationLocType(RelationGetRelid(rel)))
                                {
                                    case LOCATOR_TYPE_RROBIN:
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("foreign key can't referenced to a roundrobin distribute table")));
                                        break;
                                    case LOCATOR_TYPE_REPLICATED:
                                        break;
                                    case LOCATOR_TYPE_HASH:
                                        if(dist_by->disttype == DISTTYPE_HASH)
                                            break;
                                    case LOCATOR_TYPE_MODULO:
                                        if(dist_by->disttype == DISTTYPE_MODULO)
                                            break;
                                    default:
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("the distribute type is not same with the reference table")));
                                        break;
                                }
                                if(dist_by->colname != NULL
                                        && strcmp(dist_by->colname, ((ColumnDef *)element)->colname) != 0)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("foreign key constraints colume must be same with distribute col")));
                                heap_close(rel, NoLock);
                            }
                            else if(dist_by->disttype != DISTTYPE_REPLICATION)
                            {
                                if(dist_by->colname != NULL
                                        && strcmp(dist_by->colname, ((ColumnDef *)element)->colname) != 0)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("foreign key constraints colume must be same with distribute col")));
                            }
                        }
                    }
                }
                break;
            case T_Constraint:
                {
                    Constraint *constraint = (Constraint *) element;

                    if(constraint->contype == CONSTR_PRIMARY
                            || constraint->contype == CONSTR_UNIQUE)
                    {
                        if(dist_by->disttype != DISTTYPE_REPLICATION) 
                        {
                            if(dist_by->colname == NULL)
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("roundrobin distribute type not support primary key or unique constraint")));
                            if(constraint->keys)
                            {
                                ListCell *ckcl = NULL;
                                foreach(ckcl, constraint->keys)
                                {
                                    if(strcmp(strVal(lfirst(ckcl)), dist_by->colname) == 0 )
                                        break;
                                }
                                if(ckcl == NULL)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("primary key or unique constraint must contain distribute col")));
                            }
                            else
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("primary key or unique is empty")));
                        }
                    }
                    else if(constraint->contype == CONSTR_FOREIGN)
                    {
                        if(strcmp(stmt->relation->relname, constraint->pktable->relname) != 0)
                        {
                            Relation    rel;

                            rel = heap_openrv(constraint->pktable, AccessShareLock);
                            switch (GetRelationLocType(RelationGetRelid(rel)))
                            {
                                case LOCATOR_TYPE_RROBIN:
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("foreign key can't referenced to a roundrobin distribute table")));
                                    break;
                                case LOCATOR_TYPE_REPLICATED:
                                    break;
                                case LOCATOR_TYPE_HASH:
                                    if(dist_by->disttype == DISTTYPE_HASH)
                                        break;
                                case LOCATOR_TYPE_MODULO:
                                    if(dist_by->disttype == DISTTYPE_MODULO)
                                        break;
                                default:
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("the distribute type is not same with the reference table")));
                                    break;
                            }

                            if(dist_by->colname != NULL && constraint->fk_attrs)
                            {
                                ListCell *fkcl = NULL;
                                foreach(fkcl, constraint->fk_attrs)
                                {
                                    if(strcmp(strVal(lfirst(fkcl)), dist_by->colname) == 0)
                                        break;
                                }
                                if(fkcl == NULL)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("foreign key must contain distribute col")));
                            }
                            heap_close(rel, NoLock);
                        }
                        else if(dist_by->disttype != DISTTYPE_REPLICATION)
                        {
                            if(dist_by->colname != NULL && constraint->fk_attrs)
                            {
                                ListCell *fkcl = NULL;
                                foreach(fkcl, constraint->fk_attrs)
                                {
                                    if(strcmp(strVal(lfirst(fkcl)), dist_by->colname) == 0)
                                        break;
                                }
                                if(fkcl == NULL)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("foreign key must contain distribute col")));
                            }
                        }
                    }
                    else if(constraint->contype == CONSTR_EXCLUSION)
                    {
                        if(dist_by->disttype != DISTTYPE_REPLICATION)
                        {
                            ListCell *index_elem;

                            if(dist_by->colname == NULL)
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("roundrobin distribute type not support exclude constraint")));
                            foreach(index_elem, constraint->exclusions) 
                            {
                                IndexElem *index_val =  (IndexElem *)(linitial(lfirst(index_elem)));

                                if(index_val->name
                                        && strcmp(index_val->name, dist_by->colname) == 0)
                                    break;
                            }
                            if(index_elem == NULL)
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("exclude constraint must contain distribute col")));
                        }
                    }
                }
                break;
            case T_TableLikeClause:
                break;
            default:
                elog(ERROR, "unrecognized node type: %d",
                        (int) nodeTag(element));
                break;
        }
    }
}
