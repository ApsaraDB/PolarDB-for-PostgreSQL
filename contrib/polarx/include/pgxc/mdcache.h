/*-------------------------------------------------------------------------
 *
 * mdcache.h
 *
 *  meta data cache
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * contrib/polarx/include/pgxc/mdcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MDCACHE_H
#define MDCACHE_H
#include "pgxc/locator.h"

typedef struct
{
    /* lookup key*/
	Oid relationId;
    RelationLocInfo *locInfo;
    bool    valid;
}DistRelCacheEntry;

extern DistRelCacheEntry * GetDisRelationCache(Oid relid);
extern DistRelCacheEntry * CreateDisRelationCache(Oid relid);
extern Oid polarxExtensionOwner(void);
extern bool IsPoolerWokerStarted(void);
extern bool polarx_has_been_loaded(void);
extern bool GetPoolerWorkerStartStatus(void);
extern Oid GetPolarxSchemaOid(void);
extern Oid GetPolarxSqlPreparedOid(void);
extern Oid GetPolarxSqlPreparedIdxOid(void);
extern Oid GetPolarxSqlQueryIdxOid(void);
extern Oid GetPolarxSqlOidsIdxOid(void);
extern char *GetDataBaseName(void);
extern char *GetAuthUserName(void);
#endif



