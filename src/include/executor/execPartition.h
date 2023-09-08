/*--------------------------------------------------------------------
 * execPartition.h
 *		POSTGRES partitioning executor interface
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/executor/execPartition.h
 *--------------------------------------------------------------------
 */

#ifndef EXECPARTITION_H
#define EXECPARTITION_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "partitioning/partprune.h"

/* See execPartition.c for the definition. */
typedef struct PartitionDispatchData *PartitionDispatch;

/*-----------------------
 * PartitionTupleRouting - Encapsulates all information required to execute
 * tuple-routing between partitions.
 *
 * partition_dispatch_info		Array of PartitionDispatch objects with one
 *								entry for every partitioned table in the
 *								partition tree.
 * num_dispatch					number of partitioned tables in the partition
 *								tree (= length of partition_dispatch_info[])
 * partition_oids				Array of leaf partitions OIDs with one entry
 *								for every leaf partition in the partition tree,
 *								initialized in full by
 *								ExecSetupPartitionTupleRouting.
 * partitions					Array of ResultRelInfo* objects with one entry
 *								for every leaf partition in the partition tree,
 *								initialized lazily by ExecInitPartitionInfo.
 * num_partitions				Number of leaf partitions in the partition tree
 *								(= 'partitions_oid'/'partitions' array length)
 * parent_child_tupconv_maps	Array of TupleConversionMap objects with one
 *								entry for every leaf partition (required to
 *								convert tuple from the root table's rowtype to
 *								a leaf partition's rowtype after tuple routing
 *								is done)
 * child_parent_tupconv_maps	Array of TupleConversionMap objects with one
 *								entry for every leaf partition (required to
 *								convert an updated tuple from the leaf
 *								partition's rowtype to the root table's rowtype
 *								so that tuple routing can be done)
 * child_parent_map_not_required  Array of bool. True value means that a map is
 *								determined to be not required for the given
 *								partition. False means either we haven't yet
 *								checked if a map is required, or it was
 *								determined to be required.
 * subplan_partition_offsets	Integer array ordered by UPDATE subplans. Each
 *								element of this array has the index into the
 *								corresponding partition in partitions array.
 * num_subplan_partition_offsets  Length of 'subplan_partition_offsets' array
 * partition_tuple_slot			TupleTableSlot to be used to manipulate any
 *								given leaf partition's rowtype after that
 *								partition is chosen for insertion by
 *								tuple-routing.
 * root_tuple_slot				TupleTableSlot to be used to transiently hold
 *								copy of a tuple that's being moved across
 *								partitions in the root partitioned table's
 *								rowtype
 *-----------------------
 */
typedef struct PartitionTupleRouting
{
	PartitionDispatch *partition_dispatch_info;
	int			num_dispatch;
	Oid		   *partition_oids;
	ResultRelInfo **partitions;
	int			num_partitions;
	TupleConversionMap **parent_child_tupconv_maps;
	TupleConversionMap **child_parent_tupconv_maps;
	bool	   *child_parent_map_not_required;
	int		   *subplan_partition_offsets;
	int			num_subplan_partition_offsets;
	TupleTableSlot *partition_tuple_slot;
	TupleTableSlot *root_tuple_slot;
} PartitionTupleRouting;

/*
 * PartitionedRelPruningData - Per-partitioned-table data for run-time pruning
 * of partitions.  For a multilevel partitioned table, we have one of these
 * for the topmost partition plus one for each non-leaf child partition.
 *
 * subplan_map[] and subpart_map[] have the same definitions as in
 * PartitionedRelPruneInfo (see plannodes.h); though note that here,
 * subpart_map contains indexes into PartitionPruningData.partrelprunedata[].
 *
 * nparts						Length of subplan_map[] and subpart_map[].
 * subplan_map					Subplan index by partition index, or -1.
 * subpart_map					Subpart index by partition index, or -1.
 * present_parts				A Bitmapset of the partition indexes that we
 *								have subplans or subparts for.
 * initial_pruning_steps		List of PartitionPruneSteps used to
 *								perform executor startup pruning.
 * exec_pruning_steps			List of PartitionPruneSteps used to
 *								perform per-scan pruning.
 * initial_context				If initial_pruning_steps isn't NIL, contains
 *								the details needed to execute those steps.
 * exec_context					If exec_pruning_steps isn't NIL, contains
 *								the details needed to execute those steps.
 */
typedef struct PartitionedRelPruningData
{
	int			nparts;
	int		   *subplan_map;
	int		   *subpart_map;
	Bitmapset  *present_parts;
	List	   *initial_pruning_steps;
	List	   *exec_pruning_steps;
	PartitionPruneContext initial_context;
	PartitionPruneContext exec_context;
} PartitionedRelPruningData;

/*
 * PartitionPruningData - Holds all the run-time pruning information for
 * a single partitioning hierarchy containing one or more partitions.
 * partrelprunedata[] is an array ordered such that parents appear before
 * their children; in particular, the first entry is the topmost partition,
 * which was actually named in the SQL query.
 */
typedef struct PartitionPruningData
{
	int			num_partrelprunedata;	/* number of array entries */
	PartitionedRelPruningData partrelprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruningData;

/*
 * PartitionPruneState - State object required for plan nodes to perform
 * run-time partition pruning.
 *
 * This struct can be attached to plan types which support arbitrary Lists of
 * subplans containing partitions, to allow subplans to be eliminated due to
 * the clauses being unable to match to any tuple that the subplan could
 * possibly produce.
 *
 * execparamids			Contains paramids of PARAM_EXEC Params found within
 *						any of the partprunedata structs.  Pruning must be
 *						done again each time the value of one of these
 *						parameters changes.
 * other_subplans		Contains indexes of subplans that don't belong to any
 *						"partprunedata", e.g UNION ALL children that are not
 *						partitioned tables, or a partitioned table that the
 *						planner deemed run-time pruning to be useless for.
 *						These must not be pruned.
 * prune_context		A short-lived memory context in which to execute the
 *						partition pruning functions.
 * do_initial_prune		true if pruning should be performed during executor
 *						startup (at any hierarchy level).
 * do_exec_prune		true if pruning should be performed during
 *						executor run (at any hierarchy level).
 * num_partprunedata	Number of items in "partprunedata" array.
 * partprunedata		Array of PartitionPruningData pointers for the plan's
 *						partitioned relation(s), one for each partitioning
 *						hierarchy that requires run-time pruning.
 */
typedef struct PartitionPruneState
{
	Bitmapset  *execparamids;
	Bitmapset  *other_subplans;
	MemoryContext prune_context;
	bool		do_initial_prune;
	bool		do_exec_prune;
	int			num_partprunedata;
	PartitionPruningData *partprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruneState;

extern PartitionTupleRouting *ExecSetupPartitionTupleRouting(ModifyTableState *mtstate,
							   ResultRelInfo *rootResultRelInfo);
extern int ExecFindPartition(ResultRelInfo *resultRelInfo,
				  PartitionDispatch *pd,
				  TupleTableSlot *slot,
				  EState *estate);
extern ResultRelInfo *ExecInitPartitionInfo(ModifyTableState *mtstate,
					  ResultRelInfo *rootResultRelInfo,
					  PartitionTupleRouting *proute,
					  EState *estate, int partidx);
extern void ExecInitRoutingInfo(ModifyTableState *mtstate,
					EState *estate,
					PartitionTupleRouting *proute,
					ResultRelInfo *partRelInfo,
					int partidx);
extern void ExecSetupChildParentMapForLeaf(PartitionTupleRouting *proute);
extern TupleConversionMap *TupConvMapForLeaf(PartitionTupleRouting *proute,
				  ResultRelInfo *rootRelInfo, int leaf_index);
extern HeapTuple ConvertPartitionTupleSlot(TupleConversionMap *map,
						  HeapTuple tuple,
						  TupleTableSlot *new_slot,
						  TupleTableSlot **p_my_slot);
extern void ExecCleanupTupleRouting(ModifyTableState *mtstate,
						PartitionTupleRouting *proute);
extern PartitionPruneState *ExecCreatePartitionPruneState(PlanState *planstate,
							  PartitionPruneInfo *partitionpruneinfo);
extern void ExecDestroyPartitionPruneState(PartitionPruneState *prunestate);
extern Bitmapset *ExecFindMatchingSubPlans(PartitionPruneState *prunestate,
										   EState *estate,
										   int nplans, List *join_prune_paramids);
extern Bitmapset *ExecFindInitialMatchingSubPlans(PartitionPruneState *prunestate,
								int nsubplans);

extern Bitmapset *ExecAddMatchingSubPlans(PartitionPruneState *prunestate, Bitmapset *result);

#endif							/* EXECPARTITION_H */
