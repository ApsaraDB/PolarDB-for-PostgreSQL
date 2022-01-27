#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pgxc/locator.h"
#include "pgxc/mdcache.h"

static HTAB *DistRelCacheHash = NULL;

static void
InitDistRelCache(void)
{
    HASHCTL info;

    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(DistRelCacheEntry);
    info.hash = tag_hash;
    info.hcxt = CacheMemoryContext;
    DistRelCacheHash =
        hash_create("Distri Rel Cache", 128, &info,
                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

DistRelCacheEntry *GetDisRelationCache(Oid relid)
{
    DistRelCacheEntry *cacheEntry = NULL;
    bool found = false;
    void *key = (void *)&relid;

    if (DistRelCacheHash == NULL)
        InitDistRelCache();

    cacheEntry = hash_search(DistRelCacheHash, key, HASH_FIND, &found);

    if (found)
    {
        Assert(cacheEntry->valid);
        return cacheEntry;
    }

    return NULL;
}

DistRelCacheEntry *CreateDisRelationCache(Oid relid)
{
    bool found = false;
    DistRelCacheEntry *cacheEntry;
    void *key = (void *)&relid;

    if (DistRelCacheHash == NULL)
        InitDistRelCache();

    cacheEntry = hash_search(DistRelCacheHash, key, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "rel cache entry exists");

    cacheEntry->locInfo = (RelationLocInfo *)MemoryContextAlloc(CacheMemoryContext, sizeof(RelationLocInfo));

    cacheEntry->valid = false;

    return cacheEntry;
}