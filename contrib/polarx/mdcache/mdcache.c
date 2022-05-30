/*-------------------------------------------------------------------------
 *
 * mdcache.c
 *        meta data cache routings 
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/mdcache/mdcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pgxc/locator.h"
#include "pgxc/mdcache.h"
#include "pgxc/connpool.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

typedef struct GlobalDataCacheData
{
    Oid polarxOwner;
    bool isPoolerStarted;
    bool polarxLoaded;
    bool dbnameIsValid;
    char dbname[NAMEDATALEN];
    bool authuserIsValid;
    char authusername[NAMEDATALEN];
} GlobalDataCacheData;

static HTAB *DistRelCacheHash = NULL;
static GlobalDataCacheData GlobalDataCache;

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

Oid
polarxExtensionOwner(void)
{
    Relation extRel;
    ScanKeyData key[1];
    SysScanDesc extScan;
    HeapTuple extTup;
    Datum   datum;
    bool    isnull;
    Oid extOwner = InvalidOid;

    if (GlobalDataCache.polarxOwner != InvalidOid)
    {
        return GlobalDataCache.polarxOwner;
    }

    /*
     * Look up the extension --- it must already exist in pg_extension
     */
    extRel = heap_open(ExtensionRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
            Anum_pg_extension_extname,
            BTEqualStrategyNumber, F_NAMEEQ,
            CStringGetDatum("polarx"));

    extScan = systable_beginscan(extRel, ExtensionNameIndexId, true,
                                    NULL, 1, key);

    extTup = systable_getnext(extScan);

    if (!HeapTupleIsValid(extTup))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("extension polarx does not exist")));

    /*
     * get the polarx extention owner.
     */
    datum = heap_getattr(extTup, Anum_pg_extension_extowner,
                            RelationGetDescr(extRel), &isnull);
    if(!isnull)
        extOwner = DatumGetObjectId(datum);
    

    if (!superuser_arg(extOwner))
    {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("polarx extension's owner should be superuser")));
    }

    GlobalDataCache.polarxOwner = extOwner;

    systable_endscan(extScan);

    heap_close(extRel, AccessShareLock);

    return GlobalDataCache.polarxOwner;
}

bool
polarx_has_been_loaded(void)
{
    Oid polarxExtOid;

    if(GlobalDataCache.polarxLoaded)
        return true;

    if (IsBinaryUpgrade)
        return false;

    polarxExtOid = get_extension_oid("polarx", true);

    if (polarxExtOid == InvalidOid ||
            (creating_extension && CurrentExtensionObject == polarxExtOid))
        return false;

    GlobalDataCache.polarxLoaded = true;
    return true;
}

bool
IsPoolerWokerStarted(void)
{
    if (!GlobalDataCache.isPoolerStarted || creating_extension)
    {
        bool polarxLoaded = polarx_has_been_loaded();

        if (polarxLoaded && !GlobalDataCache.isPoolerStarted)
        {
            StartupPooler(); 
            IsPoolerWokerService();
            GlobalDataCache.isPoolerStarted = true;
        }
    }
    return GlobalDataCache.isPoolerStarted;
}

bool
GetPoolerWorkerStartStatus(void)
{
    return GlobalDataCache.isPoolerStarted;
}

char *
GetDataBaseName(void)
{
    if (!GlobalDataCache.dbnameIsValid)
    {
        char *dbName = get_database_name(MyDatabaseId);
        if (dbName == NULL)
        {
            return NULL;
        }

        strlcpy(GlobalDataCache.dbname, dbName, NAMEDATALEN);
        GlobalDataCache.dbnameIsValid = true;
    }

    return GlobalDataCache.dbname;
}

char *
GetAuthUserName(void)
{
    if (!GlobalDataCache.authuserIsValid)
    {
        char *auName = GetUserNameFromId(GetAuthenticatedUserId(), false);
        if (auName == NULL)
        {
            return NULL;
        }

        strlcpy(GlobalDataCache.authusername, auName, NAMEDATALEN);
        GlobalDataCache.authuserIsValid = true;
    }

    return GlobalDataCache.authusername;
}
