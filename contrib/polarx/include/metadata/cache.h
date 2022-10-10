/*-------------------------------------------------------------------------
 *
 * cache.h
 *		Declarations for cache.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CACHE_H
#define CACHE_H

#include "postgres.h"
#include "utils/lsyscache.h"
#include "metadata/pg_shard_map.h"

extern int ShardCount;

typedef struct MetadataRelationIdCacheData
{
	Oid shardmapRelationId;
} MetadataRelationIdCacheData;


typedef struct ShardMapCacheItem
{
	int shardId;
	int nodeOid;
	int shardMinValue;
	int shardMaxValue;
	int nodeId;
	int nodeIndex; /* index in dn_handles */
}ShardMapCacheItem;

typedef struct 
{
	int version;
	bool valid;
	ShardMapCacheItem *items;
}ShardMapCacheData;

extern bool ReceivedShardMapInvalidateMsg;

extern Oid ShardMapRelationId(void);
extern void InitializeMetadataCache(void);
extern ShardMapCacheData *GetShardMapCache(void);
extern void ReloadShardMapCache(void);
extern void InvalidateShardMapCache(void);
#endif							/* CACHE_H */
