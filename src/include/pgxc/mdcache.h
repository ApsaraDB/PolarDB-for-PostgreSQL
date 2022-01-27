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


#endif



