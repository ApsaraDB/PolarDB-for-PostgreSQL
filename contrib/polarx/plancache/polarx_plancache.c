/*-------------------------------------------------------------------------
 *
 * polarx_plancache.c
 *
 *      Functions for plan cache.
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/plancache/polarx_plancache.c
 *
 *-------------------------------------------------------------------------
 */
#include "plancache/polarx_plancache.h"
#include "utils/memutils.h"
#include "access/hash.h"
#include "optimizer/cost.h"
#include "utils/fdwplanner_utils.h"
#include "lib/ilist.h"

int MaxPlancacheSize = 300;
bool EnablePlanCache = true;
static dlist_head polarxPlanCache = DLIST_STATIC_INIT(polarxPlanCache);
typedef enum ScanFeatureTag 
{
    T_EnableSeqscan,
    T_EnableIndexscan,
    T_EnableIndexonlyscan,
    T_EnableBitmapscan,
    T_EnableTidscan,
    T_EnableSort,
    T_EnableHasagg,
    T_EnableNestloop,
    T_EnableMaterial,
    T_EnableMergejoin,
    T_EnableHashjoin,
    T_EnableGathermerge,
    T_EnablePartitionwiseJoin,
    T_EnablePartitionwiseAggregate,
    T_EnableParallelAppend,
    T_EnableParallelHash,
    T_EnablePartitionPruning,
}ScanFeatureTag;

typedef struct planCacheKey
{
    uint32 key;
    uint32 param_key;
} planCacheKey;
typedef struct planCacheEntry
{
    planCacheKey key;
    dlist_node  node;
    /* every bit is one scan feature. such as enable_seqscan
     * if the bit is 0 means this feature is disabled
     * 1 means enable
     * */
    uint32 feature_flag;
    PlannedStmt *plan;
    MemoryContext plan_context;
} planCacheEntry;


static uint32 cached_plan_num = 0;
static HTAB *cachedPlanHash = NULL;
static uint32 generate_plan_feature_flag(void);
static void remove_plan_from_cache(planCacheKey hash_key);

Datum
get_node_hash(Node *node)
{
    Datum           result;
    Node           *copy;
    MemoryContext   tmpctx,
                    oldctx;
    char           *temp;

    tmpctx = AllocSetContextCreate(CurrentMemoryContext,
            "temporary context",
            ALLOCSET_DEFAULT_SIZES);

    oldctx = MemoryContextSwitchTo(tmpctx);
    copy = copyObject((Node *) node);
    temp = nodeToString(copy);
    result = hash_any((unsigned char *) temp, strlen(temp));
    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(tmpctx);

    return result;
}

PlannedStmt *
get_plan_from_cache(Datum hash_val, Datum param_val)
{
    planCacheKey hash_key;
    planCacheEntry *entry = NULL;
    bool found = false;

    if(cachedPlanHash == NULL)
        return NULL;
    hash_key.key = DatumGetUInt32(hash_val);
    hash_key.param_key = DatumGetUInt32(param_val);

    entry = hash_search(cachedPlanHash, &hash_key,
            HASH_FIND, &found);
    if(found)
    {
        uint32 current_feature_flag = generate_plan_feature_flag();

        if(entry->feature_flag != current_feature_flag ||
            !CheckPlanValid(entry->plan))
        {
            remove_plan_from_cache(entry->key);
            return NULL;
        }
        else
        {
            dlist_move_head(&polarxPlanCache, &entry->node);
            return entry->plan;
        }
    }
    else
        return NULL;
}

static void
remove_plan_from_cache(planCacheKey hash_key)
{
    bool found = false;
    planCacheEntry *entry;

    if(cachedPlanHash == NULL)
        return;

    entry = hash_search(cachedPlanHash, &hash_key,
            HASH_REMOVE, &found);
    if(found)
    {
        dlist_delete(&entry->node);
        cached_plan_num--;
        MemoryContextDelete(entry->plan_context);    
    }
}

PlannedStmt *
save_plan_into_cache(PlannedStmt *plan, Datum hash_val, Datum param_val)
{
    char hash_sting[10];
    bool found = false;
    planCacheEntry *entry = NULL;
    planCacheKey hash_key;

    Assert(MemoryContextIsValid(CacheMemoryContext));

    if(cached_plan_num >= MaxPlancacheSize)
    {
        dlist_node *node;
        planCacheEntry *entry;

        node = dlist_tail_node(&polarxPlanCache);
        entry = dlist_container(planCacheEntry, node, node);

        remove_plan_from_cache(entry->key);
    }

    if(cachedPlanHash == NULL)
    {
        HASHCTL     ctl;

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(planCacheKey);
        ctl.entrysize = sizeof(planCacheEntry);
        /* allocate ConnectionHash in the cache context */
        ctl.hcxt = CacheMemoryContext;
        cachedPlanHash = hash_create("polarx plan cache mapping", MaxPlancacheSize,
                &ctl,
                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }

    hash_key.key = DatumGetUInt32(hash_val);
    hash_key.param_key = DatumGetUInt32(param_val);

    entry = hash_search(cachedPlanHash, &hash_key, HASH_ENTER, &found);
    if(!found)
    {
        MemoryContext plan_context;
        MemoryContext oldcxt = CurrentMemoryContext;

        entry->feature_flag = generate_plan_feature_flag();
        sprintf(hash_sting,"%u_%d ",hash_key.key, hash_key.param_key);

        plan_context = AllocSetContextCreate(CacheMemoryContext,
                "PolarxCachedPlan",
                ALLOCSET_START_SMALL_SIZES);
        MemoryContextCopyAndSetIdentifier(plan_context, hash_sting);


        MemoryContextSwitchTo(plan_context);

        entry->plan = copyObject(plan);

        MemoryContextSwitchTo(oldcxt);
        entry->plan_context = plan_context;
        cached_plan_num++;
        dlist_push_head(&polarxPlanCache, &entry->node);
    }
    else
    {
        elog(ERROR, "cachedPlanHash enter failed: found old one, cachedPlanHash corrupt");
    }

    return entry->plan;
}

static uint32 generate_plan_feature_flag(void)
{
    uint32 flag = 0;    

    if(enable_seqscan)
        flag = flag | (1 << T_EnableSeqscan);
    if(enable_indexscan)
        flag = flag | (1 << T_EnableIndexscan);
    if(enable_indexonlyscan)
        flag = flag | (1 << T_EnableIndexonlyscan);
    if(enable_bitmapscan)
        flag = flag | (1 << T_EnableBitmapscan);
    if(enable_tidscan)
        flag = flag | (1 << T_EnableTidscan);
    if(enable_sort)
        flag = flag | (1 << T_EnableSort);
    if(enable_hashagg)
        flag = flag | (1 << T_EnableHasagg);
    if(enable_nestloop)
        flag = flag | (1 << T_EnableNestloop);
    if(enable_material)
        flag = flag | (1 << T_EnableMaterial);
    if(enable_mergejoin)
        flag = flag | (1 << T_EnableMergejoin);
    if(enable_hashjoin)
        flag = flag | (1 << T_EnableHashjoin);
    if(enable_gathermerge)
        flag = flag | (1 << T_EnableGathermerge);
    if(enable_partitionwise_join)
        flag = flag | (1 << T_EnablePartitionwiseJoin);
    if(enable_partitionwise_aggregate)
        flag = flag | (1 << T_EnablePartitionwiseAggregate);
    if(enable_parallel_append)
        flag = flag | (1 << T_EnableParallelAppend);
    if(enable_parallel_hash)
        flag = flag | (1 << T_EnableParallelHash);
    if(enable_partition_pruning)
        flag = flag | (1 << T_EnablePartitionPruning);
    
    return flag;
}
