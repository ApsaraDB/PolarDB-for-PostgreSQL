/*-------------------------------------------------------------------------
 *
 * evtcache.c
 *	  Special-purpose cache for event trigger data.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/evtcache.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef EVTCACHE_H
#define EVTCACHE_H

#include "nodes/pg_list.h"

typedef enum
{
	EVT_DDLCommandStart,
	EVT_DDLCommandEnd,
	EVT_SQLDrop,
	EVT_TableRewrite
} EventTriggerEvent;

typedef struct
{
	Oid			fnoid;			/* function to be called */
	char		enabled;		/* as SESSION_REPLICATION_ROLE_* */
	int			ntags;			/* number of command tags */
	char	  **tag;			/* command tags in SORTED order */
	Oid			polar_evtowner;		/* POLAR: owner of event trigger */
} EventTriggerCacheItem;

extern List *EventCacheLookup(EventTriggerEvent event);

#endif							/* EVTCACHE_H */
