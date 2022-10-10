/*-------------------------------------------------------------------------
 *
 * cache.c
 *	  Manage cache for distribute metdata.
 *	  source text
 *
 * Portions Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/polarx/metadata/cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/relcache.h"
#include "utils/memutils.h"
#include "access/relscan.h"
#include "utils/catcache.h"
#include "storage/lockdefs.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_namespace.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "commands/trigger.h"
#include "utils/syscache.h"
#include "utils/inval.h"
#include "pgxc/nodemgr.h"

#include "metadata/cache.h"
#include "metadata/pg_shard_map.h"


MetadataRelationIdCacheData MetadataRelationIdCache;
ShardMapCacheData *shardmapCache = NULL;
MemoryContext MetadataCacheMemoryContext = NULL;
int ShardCount;
bool ReceivedShardMapInvalidateMsg = false;

PG_FUNCTION_INFO_V1(shard_map_cache_invalidate);

static void CachedRelationLookup(const char *relationName, Oid *cachedOid);
static void CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace, Oid *cachedOid);
static void RegisterShardMapCacheCallbacks(void);
static void ReloadShardMapCacheCallback(Datum argument, Oid relationId);
void InvalidateRelcacheByRelid(Oid relationId);
static int GetShardMapCount(void);

/* return oid of pg_shard_map relation */
Oid ShardMapRelationId(void)
{
    CachedRelationLookup("pg_shard_map",
                         &MetadataRelationIdCache.shardmapRelationId);
    
    return MetadataRelationIdCache.shardmapRelationId;
}

void InitializeMetadataCache(void)
{
    SysScanDesc sscan;
    HeapTuple	tuple;
    MemoryContext oldContext = NULL;
    
    if (shardmapCache == NULL)
    {
        if (!CacheMemoryContext)
            CreateCacheMemoryContext();
    
        MetadataCacheMemoryContext = AllocSetContextCreate(
                CacheMemoryContext,
                "MetadataCacheMemoryContext",
                ALLOCSET_DEFAULT_SIZES);
    
        ShardCount = GetShardMapCount();
        oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);
        shardmapCache = palloc0(sizeof(ShardMapCacheData));
        shardmapCache->items = (ShardMapCacheItem*)palloc0(sizeof(ShardMapCacheItem) * ShardCount);
        MemoryContextSwitchTo(oldContext);
    }
    if (shardmapCache && shardmapCache->valid)
        return;

    Snapshot	snapshot = RegisterSnapshot(GetTransactionSnapshot());
    Relation pgShardMap = heap_open(ShardMapRelationId(), AccessShareLock);
    TupleDesc tupleDescriptor = RelationGetDescr(pgShardMap);
    Datum		values[Natts_pg_shard_map];
    bool		nulls[Natts_pg_shard_map];
    
    sscan = systable_beginscan(pgShardMap, InvalidOid, false,
                               snapshot, 0, NULL);
    
    int tuple_count = 0;
    ShardMapCacheItem *item = shardmapCache->items;
    oldContext = MemoryContextSwitchTo(MetadataCacheMemoryContext);
    while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
    {
        heap_deform_tuple(tuple, tupleDescriptor, values, nulls);
        item[tuple_count].shardId = DatumGetInt32(values[Anum_pg_shard_map_shardid - 1]);
        item[tuple_count].nodeOid = DatumGetInt32(values[Anum_pg_shard_map_nodeoid - 1]);
        item[tuple_count].shardMinValue = DatumGetInt32(values[Anum_pg_shard_map_shardminvalue - 1]);
        item[tuple_count].shardMaxValue = DatumGetInt32(values[Anum_pg_shard_map_shardmaxvalue - 1]);
        item[tuple_count].nodeId = get_pgxc_node_id(item[tuple_count].nodeOid);
        item[tuple_count].nodeIndex = get_node_index_by_nodeoid(item[tuple_count].nodeOid);
        
        tuple_count++;
    }
    MemoryContextSwitchTo(oldContext);
    Assert(tuple_count == ShardCount);
    if (tuple_count == ShardCount)
    {
        shardmapCache->valid = true;
        RegisterShardMapCacheCallbacks();
    }
        
    
    systable_endscan(sscan);
    
    heap_close(pgShardMap, AccessShareLock);

    UnregisterSnapshot(snapshot);
    
    // TODO: sort items by minValue
    
}

static void CachedRelationLookup(const char *relationName, Oid *cachedOid)
{
    CachedRelationNamespaceLookup(relationName, PG_CATALOG_NAMESPACE, cachedOid);
}

static void CachedRelationNamespaceLookup(const char *relationName, Oid relnamespace,
                          Oid *cachedOid)
{
    if (*cachedOid == InvalidOid)
    {
        *cachedOid = get_relname_relid(relationName, relnamespace);
    
        if (*cachedOid == InvalidOid)
        {
            ereport(ERROR, (errmsg(
                    "cache lookup failed for %s",
                    relationName)));
        }
    }
}

void
InvalidateRelcacheByRelid(Oid relationId)
{
    HeapTuple classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));

    if (HeapTupleIsValid(classTuple))
    {
        CacheInvalidateRelcacheByTuple(classTuple);
        ReleaseSysCache(classTuple);
    }
}

/*
 * shard_map_cache_invalidate is a trigger function that performs
 * relcache invalidations when the contents of pg_shard_map are changed
 * on the SQL level.
 *
 * NB: We decided there is little point in checking permissions here, there
 * are much easier ways to waste CPU than causing cache invalidations.
 */
Datum
shard_map_cache_invalidate(PG_FUNCTION_ARGS)
{
    Oid shardMapRelationId = ShardMapRelationId();

    if (!CALLED_AS_TRIGGER(fcinfo))
    {
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                errmsg("must be called as trigger")));
    }
    
    InvalidateRelcacheByRelid(shardMapRelationId);
    
    PG_RETURN_DATUM(PointerGetDatum(NULL));
}

static void ReloadShardMapCacheCallback(Datum argument, Oid relationId)
{
    if (relationId == ShardMapRelationId())
    {
        /* mark we have received shard map invalidate message. */
        ReceivedShardMapInvalidateMsg = true;
        elog(DEBUG1, "[TRACESHARDMAP] set ReceivedShardMapInvalidateMsg to true");
        
        if (shardmapCache && shardmapCache->valid)
            ReloadShardMapCache();
    }
}

static void RegisterShardMapCacheCallbacks(void)
{
    static bool shardmapCallbackRegistered = false;
    if (!shardmapCallbackRegistered)
    {
        shardmapCallbackRegistered = true;
        CacheRegisterRelcacheCallback(ReloadShardMapCacheCallback,
                                      (Datum) 0);
    }
}

ShardMapCacheData *GetShardMapCache(void)
{
    return shardmapCache;
}

void InvalidateShardMapCache(void)
{
    shardmapCache->valid = false;
}

void ReloadShardMapCache(void)
{
    shardmapCache->valid = false;
    InitializeMetadataCache();
}

static int GetShardMapCount(void)
{
	int shard_count = 0;
	SysScanDesc sscan;
	Snapshot	snapshot = RegisterSnapshot(GetTransactionSnapshot());
	Relation pgShardMap = heap_open(ShardMapRelationId(), AccessShareLock);

	sscan = systable_beginscan(pgShardMap, InvalidOid, false,
							   snapshot, 0, NULL);

	while (HeapTupleIsValid(systable_getnext(sscan)))
	{
		shard_count++;
	}
	
	systable_endscan(sscan);
	heap_close(pgShardMap, AccessShareLock);
	UnregisterSnapshot(snapshot);
	return shard_count;
}