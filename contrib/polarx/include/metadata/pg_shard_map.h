/*-------------------------------------------------------------------------
 *
 * pg_shard_map.h
 *	  definition of the system "shard map" relation (pg_shard_map).
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_MAP_H
#define PG_SHARD_MAP_H

/* ----------------
 *		pg_shard_map definition.
 * ----------------
 */
typedef struct FormData_pg_shard_map
{
	int shardid;
	Oid nodeOid;
	int shardminvalue;
	int shardmaxvalue;
	
} FormData_pg_shard_map;

/* ----------------
 *      Form_pg_shard_map corresponds to a pointer to a tuple with
 *      the format of pg_shard_map relation.
 * ----------------
 */
typedef FormData_pg_shard_map *Form_pg_shard_map;

/* ----------------
 *      compiler constants for pg_shard_map
 * ----------------
 */
#define Natts_pg_shard_map 4
#define Anum_pg_shard_map_shardid 1
#define Anum_pg_shard_map_nodeoid 2
#define Anum_pg_shard_map_shardminvalue 3
#define Anum_pg_shard_map_shardmaxvalue 4


#endif   /* PG_SHARD_MAP_H */
