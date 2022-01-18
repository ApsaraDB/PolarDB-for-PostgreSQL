//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//  Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CTranslatorRelcacheToDXL.h
//
//	@doc:
//		Class for translating GPDB's relcache entries into DXL MD objects
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CTranslatorRelcacheToDXL_H
#define GPDXL_CTranslatorRelcacheToDXL_H

#include "gpos/base.h"
#include "c.h"
#include "postgres.h"
#include "access/tupdesc.h"
#include "catalog/px_policy.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDRelationExternalGPDB.h"
#include "naucrates/md/CMDAggregateGPDB.h"
#include "naucrates/md/CMDFunctionGPDB.h"
#include "naucrates/md/CMDTriggerGPDB.h"
#include "naucrates/md/CMDCheckConstraintGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDScalarOpGPDB.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/md/IMDIndex.h"

// fwd decl
struct RelationData;
typedef struct RelationData* Relation;
struct LogicalIndexes;
struct LogicalIndexInfo;

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorRelcacheToDXL
	//
	//	@doc:
	//		Class for translating GPDB's relcache entries into DXL MD objects
	//
	//---------------------------------------------------------------------------
	class CTranslatorRelcacheToDXL
	{
		private:
			// lookup function properties
			static
			void LookupFuncProps
				(
				OID func_oid,
				IMDFunction::EFuncStbl *stability, // output: function stability
				IMDFunction::EFuncDataAcc *access, // output: function data access
				BOOL *is_strict, // output: is function strict?
				BOOL *ReturnsSet // output: does function return set?
				);

			// check and fall back for unsupported relations
			static
			void CheckUnsupportedRelation(OID rel_oid);

			// get type name from the relcache
			static
			CMDName *GetTypeName(CMemoryPool *mp, IMDId *mdid);

			// get function stability property from the GPDB character representation
			static
			CMDFunctionGPDB::EFuncStbl GetFuncStability(CHAR c);

			// get function data access property from the GPDB character representation
			static
			CMDFunctionGPDB::EFuncDataAcc GetEFuncDataAccess(CHAR c);

			// get type of aggregate's intermediate result from the relcache
			static
			IMDId *RetrieveAggIntermediateResultType(CMemoryPool *mp, IMDId *mdid);

			// retrieve a GPDB metadata object from the relcache
			static
			IMDCacheObject *RetrieveObjectGPDB(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

			// retrieve relstats object from the relcache
			static
			IMDCacheObject *RetrieveRelStats(CMemoryPool *mp, IMDId *mdid);

			// retrieve column stats object from the relcache
			static
			IMDCacheObject *RetrieveColStats(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

			// retrieve cast object from the relcache
			static
			IMDCacheObject *RetrieveCast(CMemoryPool *mp, IMDId *mdid);

			// retrieve scalar comparison object from the relcache
			static
			IMDCacheObject *RetrieveScCmp(CMemoryPool *mp, IMDId *mdid);

			// transform GPDB's MCV information to optimizer's histogram structure
			static
			CHistogram *TransformMcvToOrcaHistogram
								(
								CMemoryPool *mp,
								const IMDType *md_type,
								const Datum *mcv_values,
								const float4 *mcv_frequencies,
								ULONG num_mcv_values
								);

			// transform GPDB's hist information to optimizer's histogram structure
			static
			CHistogram *TransformHistToOrcaHistogram
								(
								CMemoryPool *mp,
								const IMDType *md_type,
								const Datum *hist_values,
								ULONG num_hist_values,
								CDouble num_distinct,
								CDouble hist_freq
								);

			// histogram to array of dxl buckets
			static
			CDXLBucketArray *TransformHistogramToDXLBucketArray
								(
								CMemoryPool *mp,
								const IMDType *md_type,
								const CHistogram *hist
								);

			// transform stats from pg_stats form to optimizer's preferred form
			static
			CDXLBucketArray *TransformStatsToDXLBucketArray
								(
								CMemoryPool *mp,
								OID att_type,
								CDouble num_distinct,
								CDouble null_freq,
								const Datum *mcv_values,
								const float4 *mcv_frequencies,
								ULONG num_mcv_values,
								const Datum *hist_values,
								ULONG num_hist_values
								);

			// get partition keys and types for a relation
			static
			void RetrievePartKeysAndTypes(CMemoryPool *mp,
										Relation rel,
										OID oid,
										ULongPtrArray **part_keys,
										ULongPtrArray **part_schemes,
										CharPtrArray **part_types);

			// get keysets for relation
			static
			ULongPtr2dArray *RetrieveRelKeysets(CMemoryPool *mp, OID oid, BOOL should_add_default_keys, BOOL is_partitioned, ULONG *attno_mapping);

			// storage type for a relation
			static
			IMDRelation::Erelstoragetype RetrieveRelStorageType(Relation rel);

			// fix frequencies if they add up to more than 1.0
			static
			void NormalizeFrequencies(float4 *pdrgf, ULONG length, CDouble *null_freq);

			// get the relation columns
			static
			CMDColumnArray *RetrieveRelColumns(CMemoryPool *mp, CMDAccessor *md_accessor, Relation rel, IMDRelation::Erelstoragetype rel_storage_type);

			// return the dxl representation of the column's default value
			static
			CDXLNode *GetDefaultColumnValue(CMemoryPool *mp, CMDAccessor *md_accessor, TupleDesc rd_att, AttrNumber attrno);


			// get the distribution columns
			static
			ULongPtrArray *RetrieveRelDistributionCols(CMemoryPool *mp, PxPolicy *px_policy, CMDColumnArray *mdcol_array, ULONG size);

			// construct a mapping GPDB attnos -> position in the column array
			static
			ULONG *ConstructAttnoMapping(CMemoryPool *mp, CMDColumnArray *mdcol_array, ULONG max_cols);

			// check if index is supported
			static
			BOOL IsIndexSupported(Relation index_rel);

			// retrieve index info list of partitioned table
			static
			List *RetrievePartTableIndexInfo(Relation rel);

			// compute the array of included columns
			static
			ULongPtrArray *ComputeIncludedCols(CMemoryPool *mp, const IMDRelation *md_rel);

			// is given level included in the default partitions
			static
			BOOL LevelHasDefaultPartition(List *default_levels, ULONG level);

			// retrieve part constraint for relation
			static CDXLNode *RetrievePartConstraintForRel(CMemoryPool *mp,
														CMDAccessor *md_accessor,
														Relation rel,
														CMDColumnArray *mdcol_array);

			// return relation name
			static
			CMDName *GetRelName(CMemoryPool *mp, Relation rel);

			// return the index info list defined on the given relation
			static
			CMDIndexInfoArray *RetrieveRelIndexInfo(CMemoryPool *mp, Relation rel);

			// return index info list of indexes defined on a partitoned table
			static
			CMDIndexInfoArray *RetrieveRelIndexInfoForMultiLevelPartTable(CMemoryPool *mp, Relation root_rel);

			// return index info list of indexes defined on regular, external tables or leaf partitions
			static
			CMDIndexInfoArray *RetrieveRelIndexInfoForNonPartTable(CMemoryPool *mp, Relation rel);

			// retrieve an index over a partitioned table from the relcache
			static
			IMDIndex *RetrievePartTableIndex(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid_index, const IMDRelation *md_rel, LogicalIndexes *logical_indexes);

			// lookup an index given its id from the logical indexes structure
			static
			LogicalIndexInfo *LookupLogicalIndexById(LogicalIndexes *logical_indexes, OID oid);

			// construct an MD cache index object given its logical index representation
			static
			IMDIndex *RetrievePartTableIndex(CMemoryPool *mp, CMDAccessor *md_accessor, LogicalIndexInfo *index_info, IMDId *mdid_index, const IMDRelation *md_rel);

			// return the triggers defined on the given relation
			static
			IMdIdArray *RetrieveRelTriggers(CMemoryPool *mp, Relation rel);

			// return the check constraints defined on the relation with the given oid
			static
			IMdIdArray *RetrieveRelCheckConstraints(CMemoryPool *mp, OID oid);

			// does attribute number correspond to a transaction visibility attribute
			static
			BOOL IsTransactionVisibilityAttribute(INT attrnum);

			// does relation type have system columns
			static
			BOOL RelHasSystemColumns(char	rel_kind);

			// translate Optimizer comparison types to GPDB
			static
			ULONG GetComparisonType(IMDType::ECmpType cmp_type);

			// retrieve the opfamilies mdids for the given scalar op
			static
			IMdIdArray *RetrieveScOpOpFamilies(CMemoryPool *mp, IMDId *mdid_scalar_op);

			// retrieve the opfamilies mdids for the given index
			static
			IMdIdArray *RetrieveIndexOpFamilies(CMemoryPool *mp, IMDId *mdid_index);

            // for non-leaf partition tables return the number of child partitions
            // else return 1
            static
            ULONG RetrieveNumChildPartitions(OID rel_oid);

            // generate statistics for the system level columns
            static
            CDXLColStats *GenerateStatsForSystemCols
                              (
                              CMemoryPool *mp,
                              OID rel_oid,
                              CMDIdColStats *mdid_col_stats,
                              CMDName *md_colname,
                              OID att_type,
                              AttrNumber attrnum,
                              CDXLBucketArray *dxl_stats_bucket_array,
                              CDouble rows
                              );

			static IMdIdArray *RetrieveRelDistributionOpFamilies(CMemoryPool *mp,
															PxPolicy *px_policy);

			static IMdIdArray *RetrieveIndexPartitions(CMemoryPool *mp, OID rel_oid);

		public:
			// retrieve a metadata object from the relcache
			static
			IMDCacheObject *RetrieveObject(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

			// retrieve a relation from the relcache
			static
			IMDRelation *RetrieveRel(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

			// add system columns (oid, tid, xmin, etc) in table descriptors
			static
			void AddSystemColumns(CMemoryPool *mp, CMDColumnArray *mdcol_array, Relation rel);

			// retrieve an index from the relcache
			static
			IMDIndex *RetrieveIndex(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid_index);

			// retrieve a check constraint from the relcache
			static
			CMDCheckConstraintGPDB *RetrieveCheckConstraints(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid);

			// populate the attribute number to position mapping
			static
			ULONG *PopulateAttnoPositionMap(CMemoryPool *mp, const IMDRelation *md_rel, ULONG size);

			// return the position of a given attribute number
			static
			ULONG GetAttributePosition(INT attno, ULONG *attno_mapping);

			// retrieve a type from the relcache
			static
			IMDType *RetrieveType(CMemoryPool *mp, IMDId *mdid);

			// retrieve a scalar operator from the relcache
			static
			CMDScalarOpGPDB *RetrieveScOp(CMemoryPool *mp, IMDId *mdid);

			// retrieve a function from the relcache
			static
			CMDFunctionGPDB *RetrieveFunc(CMemoryPool *mp, IMDId *mdid);

			// retrieve an aggregate from the relcache
			static
			CMDAggregateGPDB *RetrieveAgg(CMemoryPool *mp, IMDId *mdid);

			// retrieve a trigger from the relcache
			static
			CMDTriggerGPDB *RetrieveTrigger(CMemoryPool *mp, IMDId *mdid);

			// translate GPDB comparison type
			static
			IMDType::ECmpType ParseCmpType(ULONG cmpt);

			// get the distribution policy of the relation
			static
			IMDRelation::Ereldistrpolicy GetRelDistribution(PxPolicy *px_policy);
	};
}



#endif // !GPDXL_CTranslatorRelcacheToDXL_H

// EOF
