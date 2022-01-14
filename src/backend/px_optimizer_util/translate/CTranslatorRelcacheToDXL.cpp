/*-------------------------------------------------------------------------
*	Greenplum Database
*
*	Copyright (C) 2011 EMC Corp.
*	Copyright (C) 2021, Alibaba Group Holding Limiteds
*
*	@filename:
*		CTranslatorRelcacheToDXL.cpp
*
*	@doc:
*		Class translating relcache entries into DXL objects
*
*	@test:
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/datum.h"
extern "C" {
#include "utils/elog.h"
#include "utils/partcache.h"
#include "catalog/partition.h"
#include "partitioning/partbounds.h"/* POLAR px */
}
#include "utils/guc.h"
#include "nodes/plannodes.h"
#include "px/px_hash.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"

#include "catalog/namespace.h"
#include "catalog/pg_statistic.h"

#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDScCmpGPDB.h"

#include "px_optimizer_util/translate/CTranslatorUtils.h"
#include "px_optimizer_util/translate/CTranslatorRelcacheToDXL.h"
#include "px_optimizer_util/translate/CTranslatorScalarToDXL.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/common/CAutoRef.h"
#include "naucrates/exception.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CDXLColStats.h"

#include "gpopt/base/CUtils.h"

#include "px_optimizer_util/px_wrappers.h"
#include "px_optimizer_util/utils/RelationWrapper.h"

using namespace gpdxl;
using namespace gpopt;


static 
const ULONG cmp_type_mappings[][2] =
{
	{IMDType::EcmptEq, CmptEq},
	{IMDType::EcmptNEq, CmptNEq},
	{IMDType::EcmptL, CmptLT},
	{IMDType::EcmptG, CmptGT},
	{IMDType::EcmptGEq, CmptGEq},
	{IMDType::EcmptLEq, CmptLEq}
};

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveObject
*
*	@doc:
*		Retrieve a metadata object from the relcache given its metadata id.
*
*-------------------------------------------------------------------------
*/
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveObject
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	IMDCacheObject *md_obj = NULL;
	GPOS_ASSERT(NULL != md_accessor);

	switch(mdid->MdidType())
	{
		case IMDId::EmdidGPDB:
			md_obj = RetrieveObjectGPDB(mp, md_accessor, mdid);
			break;
		
		case IMDId::EmdidRelStats:
			md_obj = RetrieveRelStats(mp, mdid);
			break;
		
		case IMDId::EmdidColStats:
			md_obj = RetrieveColStats(mp, md_accessor, mdid);
			break;
		
		case IMDId::EmdidCastFunc:
			md_obj = RetrieveCast(mp, mdid);
			break;
		
		case IMDId::EmdidScCmp:
			md_obj = RetrieveScCmp(mp, mdid);
			break;
			
		default:
			break;
	}

	if (NULL == md_obj)
	{
		// no match found
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	return md_obj;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveMDObjGPDB
*
*	@doc:
*		Retrieve a GPDB metadata object from the relcache given its metadata id.
*
*-------------------------------------------------------------------------
*/
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveObjectGPDB
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	GPOS_ASSERT(mdid->MdidType() == CMDIdGPDB::EmdidGPDB);

	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(0 != oid);

	// find out what type of object this oid stands for

	/* POLAR px */
	if (mdid->polar_get_retrieve_op() && px::OperatorExists(oid))
	{
		return RetrieveScOp(mp, mdid);
	}

	if (px::IndexExists(oid))
	{
		return RetrieveIndex(mp, md_accessor, mdid);
	}

	if (px::TypeExists(oid))
	{
		return RetrieveType(mp, mdid);
	}

	if (px::RelationExists(oid))
	{
		return RetrieveRel(mp, md_accessor, mdid);
	}

	if (px::OperatorExists(oid))
	{
		return RetrieveScOp(mp, mdid);
	}

	if (px::AggregateExists(oid))
	{
		return RetrieveAgg(mp, mdid);
	}

	if (px::FunctionExists(oid))
	{
		return RetrieveFunc(mp, mdid);
	}

	// if (px::TriggerExists(oid))
	// {
	// 	return RetrieveTrigger(mp, mdid);
	// }

	if (px::CheckConstraintExists(oid))
	{
		return RetrieveCheckConstraints(mp, md_accessor, mdid);
	}

	// no match found
	return NULL;

}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetRelName
*
*	@doc:
*		Return a relation name
*
*-------------------------------------------------------------------------
*/
CMDName *
CTranslatorRelcacheToDXL::GetRelName
	(
	CMemoryPool *mp,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);
	CHAR *relname = NameStr(rel->rd_rel->relname);
	CWStringDynamic *relname_str = CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
	GPOS_DELETE(relname_str);
	return mdname;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelIndexInfo
*
*	@doc:
*		Return the indexes defined on the given relation.
* 		If the relation is partitioned table or multi-level partitioned
*       table, we only retrieve all the leaf partitioned table indexes
*
*-------------------------------------------------------------------------
*/
CMDIndexInfoArray *
CTranslatorRelcacheToDXL::RetrieveRelIndexInfo
	(
	CMemoryPool *mp,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);
	
	/* POLAR px */
	int level = 0;
	px::GetPartTableHegiht(rel->rd_id, &level, 0);

	if (px::RelIsPartitioned(rel->rd_id) && level >= 2)
	{
		if (!px_optimizer_multilevel_partitioning)
		{
			// Multi-level partitioned tables are unsupported - fall back
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
						GPOS_WSZ_LIT("Multi-level partitioned tables"));
		}
		return RetrieveRelIndexInfoForMultiLevelPartTable(mp, rel);
	}
	/* POLAR end */

	return RetrieveRelIndexInfoForNonPartTable(mp, rel);
}

/* POLAR px */
CMDIndexInfoArray *
CTranslatorRelcacheToDXL:: RetrieveRelIndexInfoForMultiLevelPartTable(
	CMemoryPool *mp,
	Relation root_rel)
{
	CMDIndexInfoArray *md_index_info_array = GPOS_NEW(mp) CMDIndexInfoArray(mp);

	List * oids = NIL;
	px::GetRelLeafPartIndexes(root_rel->rd_id, &oids);

	ListCell *lc = NULL;
	ForEach(lc, oids)
	{
		// only add supported indexes
		OID index_oid = lfirst_oid(lc);
		px::RelationWrapper index_rel = px::GetRelation(index_oid);

		if (!index_rel)
		{
			WCHAR wstr[1024];
			CWStringStatic str(wstr, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, str.GetBuffer());
		}

		GPOS_ASSERT(NULL != index_rel->rd_indextuple);

		if (IsIndexSupported(index_rel.get()))
		{
			CMDIdGPDB *mdid_index = GPOS_NEW(mp) CMDIdGPDB(index_oid);
			BOOL is_partial = false;
			CMDIndexInfo *md_index_info =
				GPOS_NEW(mp) CMDIndexInfo(mdid_index, is_partial);
			md_index_info_array->Append(md_index_info);
		}
	}
	return md_index_info_array;
}
/* POLAR end */

// return index info list of indexes defined on regular, external tables or leaf partitions
CMDIndexInfoArray *
CTranslatorRelcacheToDXL::RetrieveRelIndexInfoForNonPartTable
	(
	CMemoryPool *mp,
	Relation rel
	)
{
	CMDIndexInfoArray *md_index_info_array = GPOS_NEW(mp) CMDIndexInfoArray(mp);

	// not a partitioned table: obtain indexes directly from the catalog
	List *index_oids = px::GetRelationIndexes(rel);

	ListCell *lc = NULL;

	ForEach (lc, index_oids)
	{
		OID index_oid = lfirst_oid(lc);

		// only add supported indexes
		px::RelationWrapper index_rel = px::GetRelation(index_oid);

		if (!index_rel)
		{
			WCHAR wstr[1024];
			CWStringStatic str(wstr, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, str.GetBuffer());
		}

		GPOS_ASSERT(NULL != index_rel->rd_indextuple);

		if (IsIndexSupported(index_rel.get()))
		{
			CMDIdGPDB *mdid_index = GPOS_NEW(mp) CMDIdGPDB(index_oid);
			// for a regular table, external table or leaf partition, an index is always complete
			CMDIndexInfo *md_index_info =
				GPOS_NEW(mp) CMDIndexInfo(mdid_index, false /* is_partial */);
			md_index_info_array->Append(md_index_info);
		}
	}

	return md_index_info_array;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelTriggers
*
*	@doc:
*		Return the triggers defined on the given relation
*
*-------------------------------------------------------------------------
*/
IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveRelTriggers
	(
	CMemoryPool *mp,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);
	if (rel->rd_rel->relhastriggers && NULL == rel->trigdesc)
	{
		// px::BuildRelationTriggers(rel);
		if (NULL == rel->trigdesc)
		{
			rel->rd_rel->relhastriggers = false;
		}
	}

	IMdIdArray *mdid_triggers_array = GPOS_NEW(mp) IMdIdArray(mp);
	if (rel->rd_rel->relhastriggers)
	{
		const ULONG ulTriggers = rel->trigdesc->numtriggers;

		for (ULONG ul = 0; ul < ulTriggers; ul++)
		{
			Trigger trigger = rel->trigdesc->triggers[ul];
			OID trigger_oid = trigger.tgoid;
			CMDIdGPDB *mdid_trigger = GPOS_NEW(mp) CMDIdGPDB(trigger_oid);
			mdid_triggers_array->Append(mdid_trigger);
		}
	}

	return mdid_triggers_array;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelCheckConstraints
*
*	@doc:
*		Return the check constraints defined on the relation with the given oid
*
*-------------------------------------------------------------------------
*/
IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveRelCheckConstraints
	(
	CMemoryPool *mp,
	OID oid
	)
{
	IMdIdArray *check_constraint_mdids = GPOS_NEW(mp) IMdIdArray(mp);
	List *check_constraints = px::GetCheckConstraintOids(oid);

	ListCell *lc = NULL;
	ForEach (lc, check_constraints)
	{
		OID check_constraint_oid = lfirst_oid(lc);
		GPOS_ASSERT(0 != check_constraint_oid);
		CMDIdGPDB *mdid_check_constraint = GPOS_NEW(mp) CMDIdGPDB(check_constraint_oid);
		check_constraint_mdids->Append(mdid_check_constraint);
	}

	return check_constraint_mdids;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::CheckUnsupportedRelation
*
*	@doc:
*		Check and fall back to planner for unsupported relations
*
*-------------------------------------------------------------------------
*/
void
CTranslatorRelcacheToDXL::CheckUnsupportedRelation
	(
	OID rel_oid
	)
{
#if 0
	if (px::RelPartIsInterior(rel_oid))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Query on intermediate partition"));
	}
#endif

#if 0
	List *part_keys = px::GetPartitionAttrs(rel_oid);
	ULONG num_of_levels = px::ListLength(part_keys);
	ULONG num_of_levels = 0;
#endif

	if (!px::RelIsPartitioned(rel_oid) && px::HasSubclassSlow(rel_oid))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				   GPOS_WSZ_LIT("Inherited tables"));
	}

#if 0
	if (1 < num_of_levels)
	{
		if (!optimizer_multilevel_partitioning)
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Multi-level partitioned tables"));
		}

		if (!px::IsMultilevelPartitionUniform(rel_oid))
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Multi-level partitioned tables with non-uniform partitioning structure"));
		}
	}
#endif
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRel
*
*	@doc:
*		Retrieve a relation from the relcache given its metadata id.
*
*-------------------------------------------------------------------------
*/
IMDRelation *
CTranslatorRelcacheToDXL::RetrieveRel
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != oid);

	CheckUnsupportedRelation(oid);

	px::RelationWrapper rel = px::GetRelation(oid);

	if (!rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CMDName *mdname = NULL;
	IMDRelation::Erelstoragetype rel_storage_type = IMDRelation::ErelstorageSentinel;
	CMDColumnArray *mdcol_array = NULL;
	IMDRelation::Ereldistrpolicy dist = IMDRelation::EreldistrSentinel;
	ULongPtrArray *distr_cols = NULL;
	CMDIndexInfoArray *md_index_info_array = NULL;
	ULongPtrArray *part_keys = NULL;
	ULongPtrArray *part_scheme = NULL;/* POLAR px */
	CharPtrArray *part_types = NULL;
	ULONG num_leaf_partitions = 0;
	BOOL convert_hash_to_random = false;
	ULongPtr2dArray *keyset_array = NULL;
	IMdIdArray *check_constraint_mdids = NULL;
	BOOL is_temporary = false;
	BOOL has_oids = false;
	BOOL is_partitioned = false;
	IMDRelation *md_rel = NULL;
	IMdIdArray *distr_op_families = nullptr;
	IMdIdArray *partition_oids = nullptr;


	/*
	 * Pretend that there are no triggers, because we don't want ORCA to handle
	 * them. The executor can run them fine on its own.
	 */
	IMdIdArray *mdid_triggers_array = GPOS_NEW(mp) IMdIdArray(mp);

	// get rel name
	mdname = GetRelName(mp, rel.get());

	/* polar only support heap */
	rel_storage_type = IMDRelation::ErelstorageHeap;

	// get relation columns
	mdcol_array =
		RetrieveRelColumns(mp, md_accessor, rel.get(), rel_storage_type);
	const ULONG max_cols =
		GPDXL_SYSTEM_COLUMNS + (ULONG) rel->rd_att->natts + 1;
	ULONG *attno_mapping = ConstructAttnoMapping(mp, mdcol_array, max_cols);

	// get distribution policy
	PxPolicy *px_policy = px::GetDistributionPolicy(rel.get());
	dist = GetRelDistribution(px_policy);

	// get distribution columns
	if (IMDRelation::EreldistrHash == dist)
	{
		distr_cols =
			RetrieveRelDistributionCols(mp, px_policy, mdcol_array, max_cols);
		distr_op_families = RetrieveRelDistributionOpFamilies(mp, px_policy);
	}

#if 0
	convert_hash_to_random = px::IsChildPartDistributionMismatched(rel.get());
#endif
	convert_hash_to_random = false;

	// collect relation indexes
	md_index_info_array = RetrieveRelIndexInfo(mp, rel.get());

	// get partition keys
	if (IMDRelation::ErelstorageExternal != rel_storage_type)
	{
		RetrievePartKeysAndTypes(mp, rel.get(), oid, &part_keys, &part_scheme, &part_types);
	}
	is_partitioned = (nullptr != part_keys && 0 < part_keys->Size());

	int level = 0;
	px::GetPartTableHegiht(oid, &level, 0);

	// get number of leaf partitions
	partition_oids = GPOS_NEW(mp) IMdIdArray(mp);

	if (level == 1)
	{
		num_leaf_partitions = rel->rd_partdesc->nparts;
		for (int i = 0; i < rel->rd_partdesc->nparts; ++i)
		{
			Oid oid = rel->rd_partdesc->oids[i];
			partition_oids->Append(GPOS_NEW(mp) CMDIdGPDB(oid));
		}
	}
	else if (level >= 2)
	{
		if (!px_optimizer_multilevel_partitioning)
		{
			// Multi-level partitioned tables are unsupported - fall back
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					GPOS_WSZ_LIT("Multi-level partitioned tables"));
		}
		else 
		{
			num_leaf_partitions = rel->rd_partdesc->nparts;
			partition_oids = GPOS_NEW(mp) IMdIdArray(mp);
			List * oids = NIL;
			px::GetRelLeafPartTables(oid, &oids);
			ListCell *l;
			foreach(l, oids)
			{
				Oid *oid = (Oid *) lfirst(l);
				partition_oids->Append(GPOS_NEW(mp) CMDIdGPDB(*oid));
			}
		}	
	}
	/* POLAR end */
	

	// get key sets
	BOOL should_add_default_keys = RelHasSystemColumns(rel->rd_rel->relkind);
	keyset_array = RetrieveRelKeysets(mp, oid, should_add_default_keys,
									  is_partitioned, attno_mapping);

	// collect all check constraints
	check_constraint_mdids = RetrieveRelCheckConstraints(mp, oid);

	is_temporary = (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP);
	has_oids = false;

	GPOS_DELETE_ARRAY(attno_mapping);

	GPOS_ASSERT(IMDRelation::ErelstorageSentinel != rel_storage_type);
	GPOS_ASSERT(IMDRelation::EreldistrSentinel != dist);

	mdid->AddRef();

	if (IMDRelation::ErelstorageExternal != rel_storage_type)
	{
		CDXLNode *mdpart_constraint = nullptr;

		// retrieve the part constraints if relation is partitioned
		// FIMXE: Do this only if Relation::rd_rel::relispartition is true
		mdpart_constraint = RetrievePartConstraintForRel(
			mp, md_accessor, rel.get(), mdcol_array);

		// GPDB_12_MERGE_FIXME: this leaves dead code in CMDRelationGPDB. We
		// should gut it all the way
		md_rel = GPOS_NEW(mp) CMDRelationGPDB(
			mp, mdid, mdname, is_temporary, rel_storage_type, dist, mdcol_array,
			distr_cols, distr_op_families, part_keys, part_scheme, part_types,
			num_leaf_partitions, partition_oids, convert_hash_to_random,
			keyset_array, md_index_info_array, mdid_triggers_array,
			check_constraint_mdids, mdpart_constraint, has_oids);
	}

	return md_rel;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelColumns
*
*	@doc:
*		Get relation columns
*
*-------------------------------------------------------------------------
*/
CMDColumnArray *
CTranslatorRelcacheToDXL::RetrieveRelColumns
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	Relation rel,
	IMDRelation::Erelstoragetype rel_storage_type
	)
{
	CMDColumnArray *mdcol_array = GPOS_NEW(mp) CMDColumnArray(mp);

	for (ULONG ul = 0;  ul < (ULONG) rel->rd_att->natts; ul++)
	{
		Form_pg_attribute att = &rel->rd_att->attrs[ul];
		CMDName *md_colname = CDXLUtils::CreateMDNameFromCharArray(mp, NameStr(att->attname));
	
		// translate the default column value
		CDXLNode *dxl_default_col_val = NULL;
		
		if (!att->attisdropped)
		{
			dxl_default_col_val = GetDefaultColumnValue(mp, md_accessor, rel->rd_att, att->attnum);
		}

		ULONG col_len = gpos::ulong_max;
		CMDIdGPDB *mdid_col = GPOS_NEW(mp) CMDIdGPDB(att->atttypid);
		HeapTuple stats_tup = px::GetAttStats(rel->rd_id, ul+1);

		// Column width priority:
		// 1. If there is average width kept in the stats for that column, pick that value.
		// 2. If not, if it is a fixed length text type, pick the size of it. E.g if it is
		//    varchar(10), assign 10 as the column length.
		// 3. Else if it not dropped and a fixed length type such as int4, assign the fixed
		//    length.
		// 4. Otherwise, assign it to default column width which is 8.
		if(HeapTupleIsValid(stats_tup))
		{
			Form_pg_statistic form_pg_stats = (Form_pg_statistic) GETSTRUCT(stats_tup);

			// column width
			col_len = form_pg_stats->stawidth;
			px::FreeHeapTuple(stats_tup);
		}
		else if ((mdid_col->Equals(&CMDIdGPDB::m_mdid_bpchar) || mdid_col->Equals(&CMDIdGPDB::m_mdid_varchar)) && (VARHDRSZ < att->atttypmod))
		{
			col_len = (ULONG) att->atttypmod - VARHDRSZ;
		}
		else
		{
			DOUBLE width = CStatistics::DefaultColumnWidth.Get();
			col_len = (ULONG) width;

			if (!att->attisdropped)
			{
				IMDType *md_type = CTranslatorRelcacheToDXL::RetrieveType(mp, mdid_col);
				if(md_type->IsFixedLength())
				{
					col_len = md_type->Length();
				}
				md_type->Release();
			}
		}

		CMDColumn *md_col = GPOS_NEW(mp) CMDColumn
										(
										md_colname,
										att->attnum,
										mdid_col,
										att->atttypmod,
										!att->attnotnull,
										att->attisdropped,
										dxl_default_col_val /* default value */,
										col_len
										);

		mdcol_array->Append(md_col);
	}

	// add system columns
	if (RelHasSystemColumns(rel->rd_rel->relkind))
	{
		AddSystemColumns(mp, mdcol_array, rel);
	}

	return mdcol_array;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetDefaultColumnValue
*
*	@doc:
*		Return the dxl representation of column's default value
*
*-------------------------------------------------------------------------
*/
CDXLNode *
CTranslatorRelcacheToDXL::GetDefaultColumnValue
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	TupleDesc rd_att,
	AttrNumber attno
	)
{
	GPOS_ASSERT(attno > 0);

	Node *node = NULL;

	// Scan to see if relation has a default for this column
	if (NULL != rd_att->constr && 0 < rd_att->constr->num_defval)
	{
		AttrDefault *defval = rd_att->constr->defval;
		INT	num_def = rd_att->constr->num_defval;

		GPOS_ASSERT(NULL != defval);
		for (ULONG ul = 0; ul < (ULONG) num_def; ul++)
		{
			if (attno == defval[ul].adnum)
			{
				// found it, convert string representation to node tree.
				node = px::StringToNode(defval[ul].adbin);
				break;
			}
		}
	}

	if (NULL == node)
	{
		// get the default value for the type
		Form_pg_attribute att_tup = &rd_att->attrs[attno - 1];
		node = px::GetTypeDefault(att_tup->atttypid);
	}

	if (NULL == node)
	{
		return NULL;
	}

	// translate the default value expression
	return CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(mp, md_accessor,
								    NULL, /* var_colid_mapping --- subquery or external variable are not supported in default expression */
								    (Expr *) node);
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetRelDistribution
*
*	@doc:
*		Return the distribution policy of the relation
*
-------------------------------------------------------------------------*/
IMDRelation::Ereldistrpolicy
CTranslatorRelcacheToDXL::GetRelDistribution
	(
	PxPolicy *px_policy
	)
{
	if (NULL == px_policy)
	{
		return IMDRelation::EreldistrMasterOnly;
	}

	if (POLICYTYPE_REPLICATED == px_policy->ptype)
	{
		return IMDRelation::EreldistrReplicated;
	}

	if (POLICYTYPE_PARTITIONED == px_policy->ptype)
	{
		if (0 == px_policy->nattrs)
		{
			return IMDRelation::EreldistrRandom;
		}

		return IMDRelation::EreldistrHash;
	}

	if (POLICYTYPE_ENTRY == px_policy->ptype)
	{
		return IMDRelation::EreldistrMasterOnly;
	}

	GPOS_RAISE(gpdxl::ExmaMD, ExmiDXLUnrecognizedType, GPOS_WSZ_LIT("unrecognized distribution policy"));
	return IMDRelation::EreldistrSentinel;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelDistributionCols
*
*	@doc:
*		Get distribution columns
*
-------------------------------------------------------------------------*/
ULongPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelDistributionCols
	(
	CMemoryPool *mp,
	PxPolicy *px_policy,
	CMDColumnArray *mdcol_array,
	ULONG size
	)
{
	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp , ULONG, size);

	for (ULONG ul = 0;  ul < mdcol_array->Size(); ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		INT attno = md_col->AttrNum();

		ULONG idx = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		attno_mapping[idx] = ul;
	}

	ULongPtrArray *distr_cols = GPOS_NEW(mp) ULongPtrArray(mp);

	for (ULONG ul = 0; ul < (ULONG) px_policy->nattrs; ul++)
	{
		AttrNumber attno = px_policy->attrs[ul];

		distr_cols->Append(GPOS_NEW(mp) ULONG(GetAttributePosition(attno, attno_mapping)));
	}

	GPOS_DELETE_ARRAY(attno_mapping);
	return distr_cols;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::AddSystemColumns
*
*	@doc:
*		Adding system columns (oid, tid, xmin, etc) in table descriptors
*
-------------------------------------------------------------------------*/
void
CTranslatorRelcacheToDXL::AddSystemColumns
	(
	CMemoryPool *mp,
	CMDColumnArray *mdcol_array,
	Relation rel
	)
{
	BOOL has_oids = rel->rd_att->tdhasoid;

	for (INT i= SelfItemPointerAttributeNumber; i > FirstLowInvalidHeapAttributeNumber; i--)
	{
		AttrNumber attno = AttrNumber(i);
		GPOS_ASSERT(0 != attno);

		if (ObjectIdAttributeNumber == i && !has_oids)
		{
			continue;
		}

		// get system name for that attribute
		const CWStringConst *sys_colname = CTranslatorUtils::GetSystemColName(attno);
		GPOS_ASSERT(NULL != sys_colname);

		// copy string into column name
		CMDName *md_colname = GPOS_NEW(mp) CMDName(mp, sys_colname);

		CMDColumn *md_col = GPOS_NEW(mp) CMDColumn
										(
										md_colname,
										attno,
										CTranslatorUtils::GetSystemColType(mp, attno),
										default_type_modifier,
										false,	// is_nullable
										false,	// is_dropped
										NULL,	// default value
										CTranslatorUtils::GetSystemColLength(attno)
										);

		mdcol_array->Append(md_col);
	}
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveIndex
*
*	@doc:
*		Retrieve an index from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
IMDIndex *
CTranslatorRelcacheToDXL::RetrieveIndex
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid_index
	)
{
	OID index_oid = CMDIdGPDB::CastMdid(mdid_index)->Oid();
	GPOS_ASSERT(0 != index_oid);
	px::RelationWrapper index_rel = px::GetRelation(index_oid);

	if (!index_rel)
	{
		 GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid_index->GetBuffer());
	}

	const IMDRelation *md_rel = NULL;
	Form_pg_index form_pg_index = NULL;
	CMDName *mdname = NULL;
	IMDIndex::EmdindexType index_type = IMDIndex::EmdindSentinel;
	IMDId *mdid_item_type = NULL;
	bool index_clustered = false;
	ULongPtrArray *index_key_cols_array = NULL;
	ULONG *attno_mapping = NULL;
	bool index_partitioned = false;

	if (!IsIndexSupported(index_rel.get()))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				   GPOS_WSZ_LIT("Index type"));
	}

	form_pg_index = index_rel->rd_index;
	GPOS_ASSERT(nullptr != form_pg_index);
	index_clustered = form_pg_index->indisclustered;

	OID rel_oid = form_pg_index->indrelid;

	CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(rel_oid);

	md_rel = md_accessor->RetrieveRel(mdid_rel);
	mdid_item_type = GPOS_NEW(mp) CMDIdGPDB(GPDB_ANY);
	switch (index_rel->rd_rel->relam)
	{
		case BTREE_AM_OID:
			index_type = IMDIndex::EmdindBtree;
			break;
/*
 		case BITMAP_AM_OID:
			index_type = IMDIndex::EmdindBitmap;
			break;
*/
		case BRIN_AM_OID:
			index_type = IMDIndex::EmdindBrin;
			break;
		case GIN_AM_OID:
			index_type = IMDIndex::EmdindGin;
			break;
		case GIST_AM_OID:
			index_type = IMDIndex::EmdindGist;
			break;
		default:
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					   GPOS_WSZ_LIT("Index access method"));
	}

	// get the index name
	CHAR *index_name = NameStr(index_rel->rd_rel->relname);
	CWStringDynamic *str_name =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, index_name);
	mdname = GPOS_NEW(mp) CMDName(mp, str_name);
	GPOS_DELETE(str_name);

	Oid table_oid = CMDIdGPDB::CastMdid(md_rel->MDId())->Oid();
	ULONG size = GPDXL_SYSTEM_COLUMNS +
				 (ULONG) px::GetRelation(table_oid)->rd_att->natts + 1;

	attno_mapping = PopulateAttnoPositionMap(mp, md_rel, size);

	// extract the position of the key columns
	index_key_cols_array = GPOS_NEW(mp) ULongPtrArray(mp);

	for (int i = 0; i < form_pg_index->indnatts; i++)
	{
		INT attno = form_pg_index->indkey.values[i];
		GPOS_ASSERT(0 != attno && "Index expressions not supported");

		index_key_cols_array->Append(
			GPOS_NEW(mp) ULONG(GetAttributePosition(attno, attno_mapping)));
	}
	mdid_rel->Release();

	ULongPtrArray *included_cols = ComputeIncludedCols(mp, md_rel);
	mdid_index->AddRef();
	IMdIdArray *op_families_mdids = RetrieveIndexOpFamilies(mp, mdid_index);

	// get child indexes
	IMdIdArray *child_index_oids = nullptr;
	if (px::IndexIsPartitioned(index_oid))
	{
		index_partitioned = true;
		child_index_oids = RetrieveIndexPartitions(mp, index_oid);
	}
	else
	{
		child_index_oids = GPOS_NEW(mp) IMdIdArray(mp);
	}

	CMDIndexGPDB *index = GPOS_NEW(mp) CMDIndexGPDB
		(
		 mp,
		 mdid_index,
		 mdname,
		 index_clustered,
		 index_partitioned,
		 index_type,
		 mdid_item_type,
		 index_key_cols_array,
		 included_cols,
		 op_families_mdids,
		 NULL, // mdpart_constraint
		 child_index_oids
		);

	GPOS_DELETE_ARRAY(attno_mapping);
	return index;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::ComputeIncludedCols
*
*	@doc:
*		Compute the included columns in an index
*
-------------------------------------------------------------------------*/
ULongPtrArray *
CTranslatorRelcacheToDXL::ComputeIncludedCols
	(
	CMemoryPool *mp,
	const IMDRelation *md_rel
	)
{
	// TODO: 3/19/2012; currently we assume that all the columns
	// in the table are available from the index.

	ULongPtrArray *included_cols = GPOS_NEW(mp) ULongPtrArray(mp);
	const ULONG num_included_cols = md_rel->ColumnCount();
	for (ULONG ul = 0;  ul < num_included_cols; ul++)
	{
		if (!md_rel->GetMdCol(ul)->IsDropped())
		{
			included_cols->Append(GPOS_NEW(mp) ULONG(ul));
		}
	}
	
	return included_cols;
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetAttributePosition
*
*	@doc:
*		Return the position of a given attribute
*
-------------------------------------------------------------------------*/
ULONG
CTranslatorRelcacheToDXL::GetAttributePosition
	(
	INT attno,
	ULONG *GetAttributePosition
	)
{
	ULONG idx = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
	ULONG pos = GetAttributePosition[idx];
	GPOS_ASSERT(gpos::ulong_max != pos);

	return pos;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::PopulateAttnoPositionMap
*
*	@doc:
*		Populate the attribute to position mapping
*
-------------------------------------------------------------------------*/
ULONG *
CTranslatorRelcacheToDXL::PopulateAttnoPositionMap
	(
	CMemoryPool *mp,
	const IMDRelation *md_rel,
	ULONG size
	)
{
	GPOS_ASSERT(NULL != md_rel);
	const ULONG num_included_cols = md_rel->ColumnCount();

	GPOS_ASSERT(num_included_cols <= size);
	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp , ULONG, size);

	for (ULONG ul = 0; ul < size; ul++)
	{
		attno_mapping[ul] = gpos::ulong_max;
	}

	for (ULONG ul = 0;  ul < num_included_cols; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);

		INT attno = md_col->AttrNum();

		ULONG idx = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		GPOS_ASSERT(size > idx);
		attno_mapping[idx] = ul;
	}

	return attno_mapping;
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveType
*
*	@doc:
*		Retrieve a type from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
IMDType *
CTranslatorRelcacheToDXL::RetrieveType
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != oid_type);
	
	// check for supported base types
	switch (oid_type)
	{
		case GPDB_INT2_OID:
			return GPOS_NEW(mp) CMDTypeInt2GPDB(mp);

		case GPDB_INT4_OID:
			return GPOS_NEW(mp) CMDTypeInt4GPDB(mp);

		case GPDB_INT8_OID:
			return GPOS_NEW(mp) CMDTypeInt8GPDB(mp);

		case GPDB_BOOL:
			return GPOS_NEW(mp) CMDTypeBoolGPDB(mp);

		case GPDB_OID_OID:
			return GPOS_NEW(mp) CMDTypeOidGPDB(mp);
	}

	// continue to construct a generic type
	INT iFlags = TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
				 TYPECACHE_CMP_PROC | TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO | TYPECACHE_TUPDESC;

	TypeCacheEntry *ptce = px::LookupTypeCache(oid_type, iFlags);

	// get type name
	CMDName *mdname = GetTypeName(mp, mdid);

	BOOL is_fixed_length = false;
	ULONG length = 0;

	if (0 < ptce->typlen)
	{
		is_fixed_length = true;
		length = ptce->typlen;
	}

	BOOL is_passed_by_value = ptce->typbyval;

	// collect ids of different comparison operators for types
	CMDIdGPDB *mdid_op_eq = GPOS_NEW(mp) CMDIdGPDB(ptce->eq_opr);
	CMDIdGPDB *mdid_op_neq = GPOS_NEW(mp) CMDIdGPDB(px::GetInverseOp(ptce->eq_opr));
	CMDIdGPDB *mdid_op_lt = GPOS_NEW(mp) CMDIdGPDB(ptce->lt_opr);
	CMDIdGPDB *mdid_op_leq = GPOS_NEW(mp) CMDIdGPDB(px::GetInverseOp(ptce->gt_opr));
	CMDIdGPDB *mdid_op_gt = GPOS_NEW(mp) CMDIdGPDB(ptce->gt_opr);
	CMDIdGPDB *mdid_op_geq = GPOS_NEW(mp) CMDIdGPDB(px::GetInverseOp(ptce->lt_opr));
	CMDIdGPDB *mdid_op_cmp = GPOS_NEW(mp) CMDIdGPDB(ptce->cmp_proc);
	BOOL is_hashable = px::IsOpHashJoinable(ptce->eq_opr, oid_type);
	BOOL is_merge_joinable = px::IsOpMergeJoinable(ptce->eq_opr, oid_type);
	BOOL is_composite_type = px::IsCompositeType(oid_type);
	BOOL is_text_related_type = px::IsTextRelatedType(oid_type);


	// get standard aggregates
	CMDIdGPDB *mdid_min = GPOS_NEW(mp) CMDIdGPDB(px::GetAggregate("min", oid_type));
	CMDIdGPDB *mdid_max = GPOS_NEW(mp) CMDIdGPDB(px::GetAggregate("max", oid_type));
	CMDIdGPDB *mdid_avg = GPOS_NEW(mp) CMDIdGPDB(px::GetAggregate("avg", oid_type));
	CMDIdGPDB *mdid_sum = GPOS_NEW(mp) CMDIdGPDB(px::GetAggregate("sum", oid_type));

	// count aggregate is the same for all types
	CMDIdGPDB *mdid_count = GPOS_NEW(mp) CMDIdGPDB(COUNT_ANY_OID);

	// check if type is composite
	CMDIdGPDB *mdid_type_relid = NULL;
	if (is_composite_type)
	{
		mdid_type_relid = GPOS_NEW(mp) CMDIdGPDB(px::GetTypeRelid(oid_type));
	}

	// get array type mdid
	CMDIdGPDB *mdid_type_array = GPOS_NEW(mp) CMDIdGPDB(px::GetArrayType(oid_type));

	OID distr_opfamily = px::GetDefaultDistributionOpfamilyForType(oid_type);

	BOOL is_redistributable = false;
	CMDIdGPDB *mdid_distr_opfamily = NULL;
	if (distr_opfamily != InvalidOid)
	{
		mdid_distr_opfamily = GPOS_NEW(mp) CMDIdGPDB(distr_opfamily);
		is_redistributable = true;
	}

	CMDIdGPDB *mdid_legacy_distr_opfamily = NULL;
	// OID legacy_opclass = px::GetLegacyCdbHashOpclassForBaseType(oid_type);
	OID legacy_opclass = InvalidOid;
	if (legacy_opclass != InvalidOid)
	{
		OID legacy_opfamily = px::GetOpclassFamily(legacy_opclass);
		mdid_legacy_distr_opfamily = GPOS_NEW(mp) CMDIdGPDB(legacy_opfamily);
	}

	mdid->AddRef();

		return GPOS_NEW(mp) CMDTypeGenericGPDB
						 (
						 mp,
						 mdid,
						 mdname,
						 is_redistributable,
						 is_fixed_length,
						 length,
						 is_passed_by_value,
						 mdid_distr_opfamily,
						 mdid_legacy_distr_opfamily,
						 mdid_op_eq,
						 mdid_op_neq,
						 mdid_op_lt,
						 mdid_op_leq,
						 mdid_op_gt,
						 mdid_op_geq,
						 mdid_op_cmp,
						 mdid_min,
						 mdid_max,
						 mdid_avg,
						 mdid_sum,
						 mdid_count,
						 is_hashable,
						 is_merge_joinable,
						 is_composite_type,
						 is_text_related_type,
						 mdid_type_relid,
						 mdid_type_array,
						 ptce->typlen
						 );
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveScOp
*
*	@doc:
*		Retrieve a scalar operator from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
CMDScalarOpGPDB *
CTranslatorRelcacheToDXL::RetrieveScOp
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID op_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != op_oid);

	// get operator name
	CHAR *name = px::GetOpName(op_oid);

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, name);
	
	OID left_oid = InvalidOid;
	OID right_oid = InvalidOid;

	// get operator argument types
	px::GetOpInputTypes(op_oid, &left_oid, &right_oid);

	CMDIdGPDB *mdid_type_left = NULL;
	CMDIdGPDB *mdid_type_right = NULL;

	if (InvalidOid != left_oid)
	{
		mdid_type_left = GPOS_NEW(mp) CMDIdGPDB(left_oid);
	}

	if (InvalidOid != right_oid)
	{
		mdid_type_right = GPOS_NEW(mp) CMDIdGPDB(right_oid);
	}

	// get comparison type
	CmpType cmpt = (CmpType) px::GetComparisonType(op_oid);
	IMDType::ECmpType cmp_type = ParseCmpType(cmpt);
	
	// get func oid
	OID func_oid = px::GetOpFunc(op_oid);
	GPOS_ASSERT(InvalidOid != func_oid);

	CMDIdGPDB *mdid_func = GPOS_NEW(mp) CMDIdGPDB(func_oid);

	// get result type
	OID result_oid = px::GetFuncRetType(func_oid);

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(mp) CMDIdGPDB(result_oid);

	// get commutator and inverse
	CMDIdGPDB *mdid_commute_opr = NULL;

	OID commute_oid = px::GetCommutatorOp(op_oid);

	if(InvalidOid != commute_oid)
	{
		mdid_commute_opr = GPOS_NEW(mp) CMDIdGPDB(commute_oid);
	}

	CMDIdGPDB *m_mdid_inverse_opr = NULL;

	OID inverse_oid = px::GetInverseOp(op_oid);

	if(InvalidOid != inverse_oid)
	{
		m_mdid_inverse_opr = GPOS_NEW(mp) CMDIdGPDB(inverse_oid);
	}

	BOOL returns_null_on_null_input = px::IsOpStrict(op_oid);
	BOOL is_ndv_preserving = px::IsOpNDVPreserving(op_oid);

	CMDIdGPDB *mdid_hash_opfamily = NULL;
	OID distr_opfamily = px::GetCompatibleHashOpFamily(op_oid);
	if (InvalidOid != distr_opfamily)
	{
		mdid_hash_opfamily = GPOS_NEW(mp) CMDIdGPDB(distr_opfamily);
	}

	CMDIdGPDB *mdid_legacy_hash_opfamily = NULL;
	OID legacy_distr_opfamily = px::GetCompatibleLegacyHashOpFamily(op_oid);
	if (InvalidOid != legacy_distr_opfamily)
	{
		mdid_legacy_hash_opfamily =
			GPOS_NEW(mp) CMDIdGPDB(legacy_distr_opfamily);
	}
	mdid->AddRef();
	CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) CMDScalarOpGPDB(
		mp, mdid, mdname, mdid_type_left, mdid_type_right, result_type_mdid,
		mdid_func, mdid_commute_opr, m_mdid_inverse_opr, cmp_type,
		returns_null_on_null_input, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);
	return md_scalar_op;
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::LookupFuncProps
*
*	@doc:
*		Lookup function properties
*
-------------------------------------------------------------------------*/
void
CTranslatorRelcacheToDXL::LookupFuncProps
	(
	OID func_oid,
	IMDFunction::EFuncStbl *stability, // output: function stability
	IMDFunction::EFuncDataAcc *access, // output: function datya access
	BOOL *is_strict, // output: is function strict?
	BOOL *returns_set // output: does function return set?
	)
{
	GPOS_ASSERT(NULL != stability);
	GPOS_ASSERT(NULL != access);
	GPOS_ASSERT(NULL != is_strict);
	GPOS_ASSERT(NULL != returns_set);

	*stability = GetFuncStability(px::FuncStability(func_oid));
	*access = GetEFuncDataAccess(px::FuncDataAccess(func_oid));

    /* TODO */
    /*
	if (px::FuncExecLocation(func_oid) != PRODATAACCESS_ANY)
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("unsupported exec location"));
    */

	*returns_set = px::GetFuncRetset(func_oid);
	*is_strict = px::FuncStrict(func_oid);
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveFunc
*
*	@doc:
*		Retrieve a function from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
CMDFunctionGPDB *
CTranslatorRelcacheToDXL::RetrieveFunc
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID func_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != func_oid);

	// get func name
	CHAR *name = px::GetFuncName(func_oid);

	if (NULL == name)
	{

		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CWStringDynamic *func_name_str = CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, func_name_str);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(func_name_str);

	// get result type
	OID result_oid = px::GetFuncRetType(func_oid);

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(mp) CMDIdGPDB(result_oid);

	// get output argument types if any
	List *out_arg_types_list = px::GetFuncOutputArgTypes(func_oid);

	IMdIdArray *arg_type_mdids = NULL;
	if (NULL != out_arg_types_list)
	{
		ListCell *lc = NULL;
		arg_type_mdids = GPOS_NEW(mp) IMdIdArray(mp);

		ForEach (lc, out_arg_types_list)
		{
			OID oidArgType = lfirst_oid(lc);
			GPOS_ASSERT(InvalidOid != oidArgType);
			CMDIdGPDB *pmdidArgType = GPOS_NEW(mp) CMDIdGPDB(oidArgType);
			arg_type_mdids->Append(pmdidArgType);
		}

		px::GPDBFree(out_arg_types_list);
	}

	IMDFunction::EFuncStbl stability = IMDFunction::EfsImmutable;
	IMDFunction::EFuncDataAcc access = IMDFunction::EfdaNoSQL;
	BOOL is_strict = true;
	BOOL returns_set = true;
	BOOL is_ndv_preserving = true;
	BOOL is_allowed_for_PS = false;
	LookupFuncProps(func_oid, &stability, &access, &is_strict, &returns_set);

	mdid->AddRef();
	CMDFunctionGPDB *md_func = GPOS_NEW(mp) CMDFunctionGPDB(
		mp, mdid, mdname, result_type_mdid, arg_type_mdids, returns_set,
		stability, access, is_strict, is_ndv_preserving, is_allowed_for_PS);

	return md_func;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveAgg
*
*	@doc:
*		Retrieve an aggregate from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
CMDAggregateGPDB *
CTranslatorRelcacheToDXL::RetrieveAgg
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != agg_oid);

	// get agg name
	CHAR *name = px::GetFuncName(agg_oid);

	if (NULL == name)
	{

		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CWStringDynamic *agg_name_str = CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, agg_name_str);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(agg_name_str);

	// get result type
	OID result_oid = px::GetFuncRetType(agg_oid);

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(mp) CMDIdGPDB(result_oid);
	IMDId *intermediate_result_type_mdid = RetrieveAggIntermediateResultType(mp, mdid);

	mdid->AddRef();
	
	BOOL is_ordered = px::IsOrderedAgg(agg_oid);
	
	// GPDB does not support splitting of ordered aggs and aggs without a
	// combine function
	BOOL is_splittable = !is_ordered && px::IsAggPartialCapable(agg_oid);
	
	// cannot use hash agg for ordered aggs or aggs without a combine func
	// due to the fact that hashAgg may spill
	BOOL is_hash_agg_capable = !is_ordered && px::IsAggPartialCapable(agg_oid);

	CMDAggregateGPDB *pmdagg = GPOS_NEW(mp) CMDAggregateGPDB
											(
											mp,
											mdid,
											mdname,
											result_type_mdid,
											intermediate_result_type_mdid,
											is_ordered,
											is_splittable,
											is_hash_agg_capable
											);
	return pmdagg;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveCheckConstraints
*
*	@doc:
*		Retrieve a check constraint from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
CMDCheckConstraintGPDB *
CTranslatorRelcacheToDXL::RetrieveCheckConstraints
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	OID check_constraint_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != check_constraint_oid);

	// get name of the check constraint
	CHAR *name = px::GetCheckConstraintName(check_constraint_oid);
	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}
	CWStringDynamic *check_constr_name = CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, check_constr_name);
	GPOS_DELETE(check_constr_name);

	// get relation oid associated with the check constraint
	OID rel_oid = px::GetCheckConstraintRelid(check_constraint_oid);
	GPOS_ASSERT(InvalidOid != rel_oid);
	CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(rel_oid);

	// translate the check constraint expression
	Node *node = px::PnodeCheckConstraint(check_constraint_oid);
	GPOS_ASSERT(NULL != node);

	// generate a mock mapping between var to column information
	CMappingVarColId *var_colid_mapping = GPOS_NEW(mp) CMappingVarColId(mp);
	CDXLColDescrArray *dxl_col_descr_array = GPOS_NEW(mp) CDXLColDescrArray(mp);
	const IMDRelation *md_rel = md_accessor->RetrieveRel(mdid_rel);
	const ULONG length = md_rel->ColumnCount();
	for (ULONG ul = 0; ul < length; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);
		CMDName *md_colname = GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
		CMDIdGPDB *mdid_col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
		mdid_col_type->AddRef();

		// create a column descriptor for the column
		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
			md_colname, ul + 1 /*colid*/, md_col->AttrNum(), mdid_col_type,
			md_col->TypeModifier(), false /* fColDropped */
		);
		dxl_col_descr_array->Append(dxl_col_descr);
	}
	var_colid_mapping->LoadColumns(0 /*query_level */, 1 /* rteIndex */, dxl_col_descr_array);

	// translate the check constraint expression
	CDXLNode *scalar_dxlnode = CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(mp, md_accessor, var_colid_mapping, (Expr *) node);

	// cleanup
	dxl_col_descr_array->Release();
	GPOS_DELETE(var_colid_mapping);

	mdid->AddRef();

	return GPOS_NEW(mp) CMDCheckConstraintGPDB
						(
						mp,
						mdid,
						mdname,
						mdid_rel,
						scalar_dxlnode
						);
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetTypeName
*
*	@doc:
*		Retrieve a type's name from the relcache given its metadata id.
*
-------------------------------------------------------------------------*/
CMDName *
CTranslatorRelcacheToDXL::GetTypeName
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != oid_type);

	CHAR *typename_str = px::GetTypeName(oid_type);
	GPOS_ASSERT(NULL != typename_str);

	CWStringDynamic *str_name = CDXLUtils::CreateDynamicStringFromCharArray(mp, typename_str);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetFuncStability
*
*	@doc:
*		Get function stability property from the GPDB character representation
*
-------------------------------------------------------------------------*/
CMDFunctionGPDB::EFuncStbl
CTranslatorRelcacheToDXL::GetFuncStability
	(
	CHAR c
	)
{
	CMDFunctionGPDB::EFuncStbl efuncstbl = CMDFunctionGPDB::EfsSentinel;

	switch (c)
	{
		case 's':
			efuncstbl = CMDFunctionGPDB::EfsStable;
			break;
		case 'i':
			efuncstbl = CMDFunctionGPDB::EfsImmutable;
			break;
		case 'v':
			efuncstbl = CMDFunctionGPDB::EfsVolatile;
			break;
		default:
			GPOS_ASSERT(!"Invalid stability property");
	}

	return efuncstbl;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::GetEFuncDataAccess
*
*	@doc:
*		Get function data access property from the GPDB character representation
*
-------------------------------------------------------------------------*/
CMDFunctionGPDB::EFuncDataAcc
CTranslatorRelcacheToDXL::GetEFuncDataAccess
	(
	CHAR c
	)
{
	CMDFunctionGPDB::EFuncDataAcc access = CMDFunctionGPDB::EfdaSentinel;

	switch (c)
	{
		case 'n':
			access = CMDFunctionGPDB::EfdaNoSQL;
			break;
		case 'c':
			access = CMDFunctionGPDB::EfdaContainsSQL;
			break;
		case 'r':
			access = CMDFunctionGPDB::EfdaReadsSQLData;
			break;
		case 'm':
			access = CMDFunctionGPDB::EfdaModifiesSQLData;
			break;
		case 's':
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("unknown data access"));
		default:
			GPOS_ASSERT(!"Invalid data access property");
	}

	return access;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveAggIntermediateResultType
*
*	@doc:
*		Retrieve the type id of an aggregate's intermediate results
*
-------------------------------------------------------------------------*/
IMDId *
CTranslatorRelcacheToDXL::RetrieveAggIntermediateResultType
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	OID intermediate_type_oid;

	GPOS_ASSERT(InvalidOid != agg_oid);
	intermediate_type_oid = px::GetAggIntermediateResultType(agg_oid);

	/*
	 * If the transition type is 'internal', we will use the
	 * serial/deserial type to convert it to a bytea, for transfer
	 * between the segments. Therefore return 'bytea' as the
	 * intermediate type, so that any Motion nodes in between use the
	 * right datatype.
	 */

	if (intermediate_type_oid == INTERNALOID)
		intermediate_type_oid = BYTEAOID;

	return GPOS_NEW(mp) CMDIdGPDB(intermediate_type_oid);
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelStats
*
*	@doc:
*		Retrieve relation statistics from relcache
*
-------------------------------------------------------------------------*/
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveRelStats
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	CMDIdRelStats *m_rel_stats_mdid = CMDIdRelStats::CastMdid(mdid);
	IMDId *mdid_rel = m_rel_stats_mdid->GetRelMdId();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	px::RelationWrapper rel = px::GetRelation(rel_oid);
	if (!rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	double num_rows = 0.0;
	CMDName *mdname = NULL;
	BOOL stats_empty = false;

	// get rel name
	CHAR *relname = NameStr(rel->rd_rel->relname);
	CWStringDynamic *relname_str = CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
	mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
	// CMDName ctor created a copy of the string
	GPOS_DELETE(relname_str);

	num_rows = px::PxEstimatePartitionedNumTuples(rel.get());

	m_rel_stats_mdid->AddRef();

	if (num_rows == 0.0)
	{
		stats_empty = true;
	}

	ULONG relpages = rel->rd_rel->relpages;
	ULONG relallvisible = rel->rd_rel->relallvisible;

	CDXLRelStats *dxl_rel_stats = GPOS_NEW(mp) CDXLRelStats
												(
												mp,
												m_rel_stats_mdid,
												mdname,
												CDouble(num_rows),
												stats_empty,
												relpages, 
												relallvisible
												);


	return dxl_rel_stats;
}

// Retrieve column statistics from relcache
// If all statistics are missing, create dummy statistics
// Also, if the statistics are broken, create dummy statistics
// However, if any statistics are present and not broken,
// create column statistics using these statistics
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveColStats
	(
	CMemoryPool *mp,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);
	IMDId *mdid_rel = mdid_col_stats->GetRelMdId();
	ULONG pos = mdid_col_stats->Position();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	px::RelationWrapper rel = px::GetRelation(rel_oid);
	if (!rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	const IMDRelation *md_rel = md_accessor->RetrieveRel(mdid_rel);
	const IMDColumn *md_col = md_rel->GetMdCol(pos);
	AttrNumber attno = (AttrNumber) md_col->AttrNum();

	// number of rows from pg_class
	double num_rows = 0;
	num_rows = px::PxEstimatePartitionedNumTuples(rel.get());

	// extract column name and type
	CMDName *md_colname = GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
	OID att_type = CMDIdGPDB::CastMdid(md_col->MdidType())->Oid();

	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);

	if (0 > attno)
	{
		mdid_col_stats->AddRef();
		return GenerateStatsForSystemCols
				(
				mp,
				rel_oid,
				mdid_col_stats,
				md_colname,
				att_type,
				attno,
				dxl_stats_bucket_array,
				num_rows
				);
	}

	// extract out histogram and mcv information from pg_statistic
	HeapTuple stats_tup = px::GetAttStats(rel_oid, attno);

	// if there is no colstats
	if (!HeapTupleIsValid(stats_tup))
	{
		dxl_stats_bucket_array->Release();
		mdid_col_stats->AddRef();

		CDouble width = CStatistics::DefaultColumnWidth;

		if (!md_col->IsDropped())
		{
			CMDIdGPDB *mdid_atttype = GPOS_NEW(mp) CMDIdGPDB(att_type);
			IMDType *md_type = RetrieveType(mp, mdid_atttype);
			width = CStatisticsUtils::DefaultColumnWidth(md_type);
			md_type->Release();
			mdid_atttype->Release();
		}

		return CDXLColStats::CreateDXLDummyColStats(mp, mdid_col_stats, md_colname, width);
	}

	Form_pg_statistic form_pg_stats = (Form_pg_statistic) GETSTRUCT(stats_tup);

	// null frequency and NDV
	CDouble null_freq(0.0);
	if (CStatistics::Epsilon < form_pg_stats->stanullfrac)
	{
		null_freq = form_pg_stats->stanullfrac;
	}

	// column width
	CDouble width = CDouble(form_pg_stats->stawidth);

	// calculate total number of distinct values
	CDouble num_distinct(1.0);
	if (form_pg_stats->stadistinct < 0)
	{
		GPOS_ASSERT(form_pg_stats->stadistinct > -1.01);
		num_distinct = num_rows * (1 - null_freq ) * CDouble(-form_pg_stats->stadistinct);
	}
	else
	{
		num_distinct = CDouble(form_pg_stats->stadistinct);
	}
	num_distinct = num_distinct.Ceil();

	BOOL is_dummy_stats = false;
	// most common values and their frequencies extracted from the pg_statistic
	// tuple for a given column
	AttStatsSlot mcv_slot;

	(void)	px::GetAttrStatsSlot
			(
					&mcv_slot,
					stats_tup,
					STATISTIC_KIND_MCV,
					InvalidOid,
					ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS
			);
	if (InvalidOid != mcv_slot.valuetype && mcv_slot.valuetype != att_type)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer(), att_type, mcv_slot.valuetype);
		PxEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		px::FreeAttrStatsSlot(&mcv_slot);
		is_dummy_stats = true;
	}

	else if (mcv_slot.nvalues != mcv_slot.nnumbers)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "The number of most common values and frequencies do not match on column %ls of table %ls.",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer());
		PxEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		// if the number of MCVs(nvalues) and number of MCFs(nnumbers) do not match, we discard the MCVs and MCFs
		px::FreeAttrStatsSlot(&mcv_slot);
		is_dummy_stats = true;
	}
	else
	{
		// fix mcv and null frequencies (sometimes they can add up to more than 1.0)
		NormalizeFrequencies(mcv_slot.numbers, (ULONG) mcv_slot.nvalues, &null_freq);

		// total MCV frequency
		CDouble sum_mcv_freq = 0.0;
		for (int i = 0; i < mcv_slot.nvalues; i++)
		{
			sum_mcv_freq = sum_mcv_freq + CDouble(mcv_slot.numbers[i]);
		}
	}

	// histogram values extracted from the pg_statistic tuple for a given column
	AttStatsSlot hist_slot;

	// get histogram datums from pg_statistic entry
	(void) px::GetAttrStatsSlot
			(
					&hist_slot,
					stats_tup,
					STATISTIC_KIND_HISTOGRAM,
					InvalidOid,
					ATTSTATSSLOT_VALUES
			);

	if (InvalidOid != hist_slot.valuetype && hist_slot.valuetype != att_type)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer(), att_type, hist_slot.valuetype);
		PxEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		px::FreeAttrStatsSlot(&hist_slot);
		is_dummy_stats = true;
	}

	if (is_dummy_stats)
	{
		dxl_stats_bucket_array->Release();
		mdid_col_stats->AddRef();

		CDouble col_width = CStatistics::DefaultColumnWidth;
		px::FreeHeapTuple(stats_tup);
		return CDXLColStats::CreateDXLDummyColStats(mp, mdid_col_stats, md_colname, col_width);
	}

	CDouble num_ndv_buckets(0.0);
	CDouble num_freq_buckets(0.0);
	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);

	// transform all the bits and pieces from pg_statistic
	// to a single bucket structure
	CDXLBucketArray *dxl_stats_bucket_array_transformed =
	TransformStatsToDXLBucketArray
	(
	 mp,
	 att_type,
	 num_distinct,
	 null_freq,
	 mcv_slot.values,
	 mcv_slot.numbers,
	 ULONG(mcv_slot.nvalues),
	 hist_slot.values,
	 ULONG(hist_slot.nvalues)
	 );

	GPOS_ASSERT(NULL != dxl_stats_bucket_array_transformed);

	const ULONG num_buckets = dxl_stats_bucket_array_transformed->Size();
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		CDXLBucket *dxl_bucket = (*dxl_stats_bucket_array_transformed)[ul];
		num_ndv_buckets = num_ndv_buckets + dxl_bucket->GetNumDistinct();
		num_freq_buckets = num_freq_buckets + dxl_bucket->GetFrequency();
	}

	CUtils::AddRefAppend(dxl_stats_bucket_array, dxl_stats_bucket_array_transformed);
	dxl_stats_bucket_array_transformed->Release();

	// there will be remaining tuples if the merged histogram and the NULLS do not cover
	// the total number of distinct values
	if ((1 - CStatistics::Epsilon > num_freq_buckets + null_freq) &&
		(0 < num_distinct - num_ndv_buckets))
	{
		distinct_remaining = std::max(CDouble(0.0), (num_distinct - num_ndv_buckets));
		freq_remaining = std::max(CDouble(0.0), (1 - num_freq_buckets - null_freq));
	}

	// free up allocated datum and float4 arrays
	px::FreeAttrStatsSlot(&mcv_slot);
	px::FreeAttrStatsSlot(&hist_slot);

	px::FreeHeapTuple(stats_tup);

	// create col stats object
	mdid_col_stats->AddRef();
	CDXLColStats *dxl_col_stats = GPOS_NEW(mp) CDXLColStats
											(
											mp,
											mdid_col_stats,
											md_colname,
											width,
											null_freq,
											distinct_remaining,
											freq_remaining,
											dxl_stats_bucket_array,
											false /* is_col_stats_missing */
											);

	return dxl_col_stats;
}


/*-------------------------------------------------------------------------
*      @function:
//              CTranslatorRelcacheToDXL::GenerateStatsForSystemCols
*
*      @doc:
//              Generate statistics for the system level columns
*
-------------------------------------------------------------------------*/
CDXLColStats *
CTranslatorRelcacheToDXL::GenerateStatsForSystemCols
       (
       CMemoryPool *mp,
       OID rel_oid,
       CMDIdColStats *mdid_col_stats,
       CMDName *md_colname,
       OID att_type,
       AttrNumber attno,
       CDXLBucketArray *dxl_stats_bucket_array,
       CDouble num_rows
       )
{
       GPOS_ASSERT(NULL != mdid_col_stats);
       GPOS_ASSERT(NULL != md_colname);
       GPOS_ASSERT(InvalidOid != att_type);
       GPOS_ASSERT(0 > attno);
       GPOS_ASSERT(NULL != dxl_stats_bucket_array);

       CMDIdGPDB *mdid_atttype = GPOS_NEW(mp) CMDIdGPDB(att_type);
       IMDType *md_type = RetrieveType(mp, mdid_atttype);
       GPOS_ASSERT(md_type->IsFixedLength());

       BOOL is_col_stats_missing = true;
       CDouble null_freq(0.0);
       CDouble width(md_type->Length());
       CDouble distinct_remaining(0.0);
       CDouble freq_remaining(0.0);

       if (CStatistics::MinRows <= num_rows)
	   {
		   switch(attno)
			{
				case PxWorkerIdAttributeNumber:  /* _px_worker_id */
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = CDouble(px::GetPxWorkerCount());
						break;
					}
				case TableOidAttributeNumber: // tableoid
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = CDouble(RetrieveNumChildPartitions(rel_oid));
						break;
					}
				case SelfItemPointerAttributeNumber: // ctid
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = num_rows;
						break;
					}
				case RootSelfItemPointerAttributeNumber: // _root_ctid
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = num_rows;
						break;
					}
				default:
					break;
			}
        }

       // cleanup
       mdid_atttype->Release();
       md_type->Release();

       return GPOS_NEW(mp) CDXLColStats
                       (
                       mp,
                       mdid_col_stats,
                       md_colname,
                       width,
                       null_freq,
                       distinct_remaining,
                       freq_remaining,
                       dxl_stats_bucket_array,
                       is_col_stats_missing
                       );
}


/*-------------------------------------------------------------------------
*     @function:
//     CTranslatorRelcacheToDXL::RetrieveNumChildPartitions
*
*  @doc:
//      For non-leaf partition tables return the number of child partitions
//      else return 1
*
-------------------------------------------------------------------------*/
ULONG
CTranslatorRelcacheToDXL::RetrieveNumChildPartitions
       (
       OID rel_oid
       )
{
       GPOS_ASSERT(InvalidOid != rel_oid);

       ULONG num_part_tables = gpos::ulong_max;

       return num_part_tables;
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveCast
*
*	@doc:
*		Retrieve a cast function from relcache
*
-------------------------------------------------------------------------*/
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveCast
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	CMDIdCast *mdid_cast = CMDIdCast::CastMdid(mdid);
	IMDId *mdid_src = mdid_cast->MdidSrc();
	IMDId *mdid_dest = mdid_cast->MdidDest();

	OID src_oid = CMDIdGPDB::CastMdid(mdid_src)->Oid();
	OID dest_oid = CMDIdGPDB::CastMdid(mdid_dest)->Oid();
	CoercionPathType	pathtype;

	OID cast_fn_oid = 0;
	BOOL is_binary_coercible = false;
	
	BOOL cast_exists = px::GetCastFunc(src_oid, dest_oid, &is_binary_coercible, &cast_fn_oid, &pathtype);
	
	if (!cast_exists)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	} 
	
	CHAR *func_name = NULL;
	if (InvalidOid != cast_fn_oid)
	{
		func_name = px::GetFuncName(cast_fn_oid);
	}
	else
	{
		// no explicit cast function: use the destination type name as the cast name
		func_name = px::GetTypeName(dest_oid);
	}
	
	if (NULL == func_name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	mdid->AddRef();
	mdid_src->AddRef();
	mdid_dest->AddRef();

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, func_name);
	
	switch (pathtype) {
		case COERCION_PATH_ARRAYCOERCE:
			return GPOS_NEW(mp) CMDArrayCoerceCastGPDB(mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible, GPOS_NEW(mp) CMDIdGPDB(cast_fn_oid), IMDCast::EmdtArrayCoerce, default_type_modifier, false, EdxlcfImplicitCast, -1);
			break;
		case COERCION_PATH_FUNC:
			return GPOS_NEW(mp) CMDCastGPDB(mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible, GPOS_NEW(mp) CMDIdGPDB(cast_fn_oid), IMDCast::EmdtFunc);
			break;
		case COERCION_PATH_COERCEVIAIO:
			// uses IO functions from types, no function in the cast
			GPOS_ASSERT(cast_fn_oid == 0);
			return GPOS_NEW(mp) CMDCastGPDB(
				mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible,
				GPOS_NEW(mp) CMDIdGPDB(cast_fn_oid), IMDCast::EmdtCoerceViaIO);
		default:
			break;
	}

	// fall back for none path types
	return GPOS_NEW(mp) CMDCastGPDB(mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible, GPOS_NEW(mp) CMDIdGPDB(cast_fn_oid));
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveScCmp
*
*	@doc:
*		Retrieve a scalar comparison from relcache
*
-------------------------------------------------------------------------*/
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveScCmp
	(
	CMemoryPool *mp,
	IMDId *mdid
	)
{
	CMDIdScCmp *mdid_scalar_cmp = CMDIdScCmp::CastMdid(mdid);
	IMDId *mdid_left = mdid_scalar_cmp->GetLeftMdid();
	IMDId *mdid_right = mdid_scalar_cmp->GetRightMdid();
	
	IMDType::ECmpType cmp_type = mdid_scalar_cmp->ParseCmpType();

	OID left_oid = CMDIdGPDB::CastMdid(mdid_left)->Oid();
	OID right_oid = CMDIdGPDB::CastMdid(mdid_right)->Oid();
	CmpType cmpt = (CmpType) GetComparisonType(cmp_type);
	
	OID scalar_cmp_oid = px::GetComparisonOperator(left_oid, right_oid, cmpt);
	
	if (InvalidOid == scalar_cmp_oid)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	} 

	CHAR *name = px::GetOpName(scalar_cmp_oid);

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	mdid->AddRef();
	mdid_left->AddRef();
	mdid_right->AddRef();

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, name);

	return GPOS_NEW(mp) CMDScCmpGPDB(mp, mdid, mdname, mdid_left, mdid_right, cmp_type, GPOS_NEW(mp) CMDIdGPDB(scalar_cmp_oid));
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::TransformStatsToDXLBucketArray
*
*	@doc:
*		transform stats from pg_stats form to optimizer's preferred form
*
-------------------------------------------------------------------------*/
CDXLBucketArray *
CTranslatorRelcacheToDXL::TransformStatsToDXLBucketArray
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
	)
{
	CMDIdGPDB *mdid_atttype = GPOS_NEW(mp) CMDIdGPDB(att_type);
	IMDType *md_type = RetrieveType(mp, mdid_atttype);

	// translate MCVs to Orca histogram. Create an empty histogram if there are no MCVs.
	CHistogram *gpdb_mcv_hist = TransformMcvToOrcaHistogram
							(
							mp,
							md_type,
							mcv_values,
							mcv_frequencies,
							num_mcv_values
							);

	GPOS_ASSERT(gpdb_mcv_hist->IsValid());

	CDouble mcv_freq = gpdb_mcv_hist->GetFrequency();
	BOOL has_mcv = 0 < num_mcv_values && CStatistics::Epsilon < mcv_freq;

	CDouble hist_freq = 0.0;
	if (1 < num_hist_values)
	{
		hist_freq = CDouble(1.0) - null_freq - mcv_freq;
	}
	
	BOOL is_text_type = mdid_atttype->Equals(&CMDIdGPDB::m_mdid_varchar)
			 || mdid_atttype->Equals(&CMDIdGPDB::m_mdid_bpchar)
			 || mdid_atttype->Equals(&CMDIdGPDB::m_mdid_text);
	BOOL has_hist = !is_text_type && 1 < num_hist_values && CStatistics::Epsilon < hist_freq;

	CHistogram *histogram = NULL;

	// if histogram has any significant information, then extract it
	if (has_hist)
	{
		// histogram from gpdb histogram
		histogram = TransformHistToOrcaHistogram
						(
						mp,
						md_type,
						hist_values,
						num_hist_values,
						num_distinct,
						hist_freq
						);
		if (0 == histogram->GetNumBuckets())
		{
			has_hist = false;
		}
	}

	CDXLBucketArray *dxl_stats_bucket_array = NULL;

	if (has_hist && !has_mcv)
	{
		// if histogram exists and dominates, use histogram only
		dxl_stats_bucket_array = TransformHistogramToDXLBucketArray(mp, md_type, histogram);
	}
	else if (!has_hist && has_mcv)
	{
		// if MCVs exist and dominate, use MCVs only
		dxl_stats_bucket_array = TransformHistogramToDXLBucketArray(mp, md_type, gpdb_mcv_hist);
	}
	else if (has_hist && has_mcv)
	{
		// both histogram and MCVs exist and have significant info, merge MCV and histogram buckets
		CHistogram *merged_hist = CStatisticsUtils::MergeMCVHist(mp, gpdb_mcv_hist, histogram);
		dxl_stats_bucket_array = TransformHistogramToDXLBucketArray(mp, md_type, merged_hist);
		GPOS_DELETE(merged_hist);
	}
	else
	{
		// no MCVs nor histogram
		GPOS_ASSERT(!has_hist && !has_mcv);
		dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);
	}

	// cleanup
	mdid_atttype->Release();
	md_type->Release();
	GPOS_DELETE(gpdb_mcv_hist);

	if (NULL != histogram)
	{
		GPOS_DELETE(histogram);
	}

	return dxl_stats_bucket_array;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::TransformMcvToOrcaHistogram
*
*	@doc:
*		Transform gpdb's mcv info to optimizer histogram
*
-------------------------------------------------------------------------*/
CHistogram *
CTranslatorRelcacheToDXL::TransformMcvToOrcaHistogram
	(
	CMemoryPool *mp,
	const IMDType *md_type,
	const Datum *mcv_values,
	const float4 *mcv_frequencies,
	ULONG num_mcv_values
	)
{
	IDatumArray *datums = GPOS_NEW(mp) IDatumArray(mp);
	CDoubleArray *freqs = GPOS_NEW(mp) CDoubleArray(mp);

	for (ULONG ul = 0; ul < num_mcv_values; ul++)
	{
		Datum datumMCV = mcv_values[ul];
		IDatum *datum = CTranslatorScalarToDXL::CreateIDatumFromPxDatum(mp, md_type, false /* is_null */, datumMCV);
		datums->Append(datum);
		freqs->Append(GPOS_NEW(mp) CDouble(mcv_frequencies[ul]));

		if (!datum->StatsAreComparable(datum))
		{
			// if less than operation is not supported on this datum, then no point
			// building a histogram. return an empty histogram
			datums->Release();
			freqs->Release();
			return GPOS_NEW(mp) CHistogram(mp, GPOS_NEW(mp) CBucketArray(mp));
		}
	}

	CHistogram *hist = CStatisticsUtils::TransformMCVToHist
												(
												mp,
												md_type,
												datums,
												freqs,
												num_mcv_values
												);

	datums->Release();
	freqs->Release();
	return hist;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::TransformHistToOrcaHistogram
*
*	@doc:
*		Transform GPDB's hist info to optimizer's histogram
*
-------------------------------------------------------------------------*/
CHistogram *
CTranslatorRelcacheToDXL::TransformHistToOrcaHistogram
	(
	CMemoryPool *mp,
	const IMDType *md_type,
	const Datum *hist_values,
	ULONG num_hist_values,
	CDouble num_distinct,
	CDouble hist_freq
	)
{
	GPOS_ASSERT(1 < num_hist_values);

	const ULONG num_buckets = num_hist_values - 1;
	CDouble distinct_per_bucket = num_distinct / CDouble(num_buckets);
	CDouble freq_per_bucket = hist_freq / CDouble(num_buckets);

	BOOL last_bucket_was_singleton = false;
	// create buckets
	CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		IDatum *min_datum = CTranslatorScalarToDXL::CreateIDatumFromPxDatum(mp, md_type, false /* is_null */, hist_values[ul]);
		IDatum *max_datum = CTranslatorScalarToDXL::CreateIDatumFromPxDatum(mp, md_type, false /* is_null */, hist_values[ul + 1]);
		BOOL is_lower_closed, is_upper_closed;

		if (min_datum->StatsAreEqual(max_datum))
		{
			// Singleton bucket !!!!!!!!!!!!!
			is_lower_closed = true;
			is_upper_closed = true;
			last_bucket_was_singleton = true;
		}
		else if (last_bucket_was_singleton)
		{
			// Last bucket was a singleton, so lower must be open now.
			is_lower_closed = false;
			is_upper_closed = false;
			last_bucket_was_singleton = false;
		}
		else
		{
			// Normal bucket
			// GPDB histograms assumes lower bound to be closed and upper bound to be open
			is_lower_closed = true;
			is_upper_closed = false;
		}

		if (ul == num_buckets - 1)
		{
			// last bucket upper bound is also closed
			is_upper_closed = true;
		}

		CBucket *bucket = GPOS_NEW(mp) CBucket
									(
									GPOS_NEW(mp) CPoint(min_datum),
									GPOS_NEW(mp) CPoint(max_datum),
									is_lower_closed,
									is_upper_closed,
									freq_per_bucket,
									distinct_per_bucket
									);
		buckets->Append(bucket);

		if (!min_datum->StatsAreComparable(max_datum) || !min_datum->StatsAreLessThan(max_datum))
		{
			// if less than operation is not supported on this datum,
			// or the translated histogram does not conform to GPDB sort order (e.g. text column in Linux platform),
			// then no point building a histogram. return an empty histogram

			// TODO: 03/01/2014 translate histogram into Orca even if sort
			// order is different in GPDB, and use const expression eval to compare
			// datums in Orca (PX-22780)
			buckets->Release();
			return GPOS_NEW(mp) CHistogram(mp, GPOS_NEW(mp) CBucketArray(mp));
		}
	}

	CHistogram *hist = GPOS_NEW(mp) CHistogram(mp, buckets);
	return hist;
}


/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::TransformHistogramToDXLBucketArray
*
*	@doc:
*		Histogram to array of dxl buckets
*
-------------------------------------------------------------------------*/
CDXLBucketArray *
CTranslatorRelcacheToDXL::TransformHistogramToDXLBucketArray
	(
	CMemoryPool *mp,
	const IMDType *md_type,
	const CHistogram *hist
	)
{
	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);
	const CBucketArray *buckets = hist->GetBuckets();
	ULONG num_buckets = buckets->Size();
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		CBucket *bucket = (*buckets)[ul];
		IDatum *datum_lower = bucket->GetLowerBound()->GetDatum();
		CDXLDatum *dxl_lower = md_type->GetDatumVal(mp, datum_lower);
		IDatum *datum_upper = bucket->GetUpperBound()->GetDatum();
		CDXLDatum *dxl_upper = md_type->GetDatumVal(mp, datum_upper);
		CDXLBucket *dxl_bucket = GPOS_NEW(mp) CDXLBucket
											(
											dxl_lower,
											dxl_upper,
											bucket->IsLowerClosed(),
											bucket->IsUpperClosed(),
											bucket->GetFrequency(),
											bucket->GetNumDistinct()
											);
		dxl_stats_bucket_array->Append(dxl_bucket);
	}
	return dxl_stats_bucket_array;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelStorageType
*
*	@doc:
*		Get relation storage type
*
-------------------------------------------------------------------------*/
IMDRelation::Erelstoragetype
CTranslatorRelcacheToDXL::RetrieveRelStorageType
	(
	Relation rel
	)
{
	IMDRelation::Erelstoragetype rel_storage_type =
		IMDRelation::ErelstorageSentinel;

#if 0
	switch (rel->rd_rel->relam)
	{
		case HEAP_TABLE_AM_OID:
			rel_storage_type = IMDRelation::ErelstorageHeap;
			break;
		case AO_COLUMN_TABLE_AM_OID:
			rel_storage_type = IMDRelation::ErelstorageAppendOnlyCols;
			break;
		case AO_ROW_TABLE_AM_OID:
			rel_storage_type = IMDRelation::ErelstorageAppendOnlyRows;
			break;
// GPDB_12_MERGE_FIXME: why did ORCA even care about relstorage = 'v'??? DEAD CODE!
		case RELSTORAGE_VIRTUAL:
			rel_storage_type = IMDRelation::ErelstorageVirtual;
			break;
		case 0:
			// GPDB_12_MERGE_FIXME: pretend partitioned tables are heap
			// ORCA should stop asking about storage types of a partitioned table
			if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
				rel_storage_type = IMDRelation::ErelstorageHeap;
			else
			{
				// GPORCA does not support foreign data wrappers
				GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
						   GPOS_WSZ_LIT("Foreign Data"));
			}
			break;
		default:
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					   GPOS_WSZ_LIT("Unsupported table AM"));
	}
#endif

	return rel_storage_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartKeysAndTypes
//
//	@doc:
//		Get partition keys and types for relation or NULL if relation not partitioned.
//		Caller responsible for closing the relation if an exception is raised
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::RetrievePartKeysAndTypes(CMemoryPool *mp,
												   Relation rel, OID oid,
												   ULongPtrArray **part_keys,
												   ULongPtrArray **part_scheme,/* POLAR px */
												   CharPtrArray **part_types)
{
	GPOS_ASSERT(nullptr != rel);

	// FIXME: isn't it faster to check rel.rd_partkey?
	if (!px::RelIsPartitioned(oid))
	{
		// not a partitioned table
		*part_keys = nullptr;
		*part_scheme = nullptr;/* POLAR px */
		*part_types = nullptr;
		return;
	}

	*part_keys = GPOS_NEW(mp) ULongPtrArray(mp);
	*part_scheme = GPOS_NEW(mp) ULongPtrArray(mp);/* POLAR px */
	*part_types = GPOS_NEW(mp) CharPtrArray(mp);

	PartitionKeyData *partkey = rel->rd_partkey;
	PartitionDescData *partdesc = rel->rd_partdesc;

	if (1 < partkey->partnatts)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				   GPOS_WSZ_LIT("Composite part key"));
	}

	AttrNumber attno = partkey->partattrs[0];
	CHAR part_type = (CHAR) partkey->strategy;
	if (attno == 0)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				   GPOS_WSZ_LIT("partitioning by expression"));
	}

	(*part_keys)->Append(GPOS_NEW(mp) ULONG(attno - 1));
	(*part_types)->Append(GPOS_NEW(mp) CHAR(part_type));

	/* POLAR px */
	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partkey->partopfamily[0]));
	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partkey->partopcintype[0]));
	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partkey->partcollation[0]));
	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partkey->parttyplen[0]));
	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partkey->parttypbyval[0]));

	(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->nparts));
	if (partdesc->boundinfo != NULL)
	{
		(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->null_index));
		(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->default_index));
		(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->strategy));
		(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->ndatums));

		for (int ul = 0;  ul < partdesc->boundinfo->ndatums; ul++)
		{
			(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->datums[ul][0]));
			(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->indexes[ul]));
		}

		if (partdesc->boundinfo->kind != NULL)
			for (int ul = 0;  ul < partdesc->boundinfo->ndatums; ul++)
				(*part_scheme)->Append(GPOS_NEW(mp) ULONG(partdesc->boundinfo->kind[ul][0]));
	}
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::ConstructAttnoMapping
*
*	@doc:
*		Construct a mapping for GPDB attnos to positions in the columns array
*
-------------------------------------------------------------------------*/
ULONG *
CTranslatorRelcacheToDXL::ConstructAttnoMapping
	(
	CMemoryPool *mp,
	CMDColumnArray *mdcol_array,
	ULONG max_cols
	)
{
	GPOS_ASSERT(NULL != mdcol_array);
	GPOS_ASSERT(0 < mdcol_array->Size());
	GPOS_ASSERT(max_cols > mdcol_array->Size());

	// build a mapping for attnos->positions
	const ULONG num_of_cols = mdcol_array->Size();
	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp, ULONG, max_cols);

	// initialize all positions to gpos::ulong_max
	for (ULONG ul = 0;  ul < max_cols; ul++)
	{
		attno_mapping[ul] = gpos::ulong_max;
	}
	
	for (ULONG ul = 0;  ul < num_of_cols; ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		INT attno = md_col->AttrNum();

		ULONG idx = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		attno_mapping[idx] = ul;
	}

	return attno_mapping;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RetrieveRelKeysets
*
*	@doc:
*		Get key sets for relation
*
-------------------------------------------------------------------------*/
ULongPtr2dArray *
CTranslatorRelcacheToDXL::RetrieveRelKeysets
	(
	CMemoryPool *mp,
	OID oid,
	BOOL should_add_default_keys,
	BOOL is_partitioned,
	ULONG *attno_mapping
	)
{
	ULongPtr2dArray *key_sets = GPOS_NEW(mp) ULongPtr2dArray(mp);

	List *rel_keys = NIL;

	ListCell *lc_key = NULL;
	ForEach (lc_key, rel_keys)
	{
		List *key_elem_list = (List *) lfirst(lc_key);

		ULongPtrArray *key_set = GPOS_NEW(mp) ULongPtrArray(mp);

		ListCell *lc_key_elem = NULL;
		ForEach (lc_key_elem, key_elem_list)
		{
			INT key_idx = lfirst_int(lc_key_elem);
			ULONG pos = GetAttributePosition(key_idx, attno_mapping);
			key_set->Append(GPOS_NEW(mp) ULONG(pos));
		}
		GPOS_ASSERT(0 < key_set->Size());

		key_sets->Append(key_set);
	}
	
	// add {segid, ctid} as a key
	
	if (should_add_default_keys)
	{
		ULongPtrArray *key_set = GPOS_NEW(mp) ULongPtrArray(mp);
		if (is_partitioned)
		{
			// TableOid is part of default key for partitioned tables
			ULONG table_oid_pos = GetAttributePosition(TableOidAttributeNumber, attno_mapping);
			key_set->Append(GPOS_NEW(mp) ULONG(table_oid_pos));
		}
		ULONG seg_id_pos= GetAttributePosition(PxWorkerIdAttributeNumber, attno_mapping);
		ULONG ctid_pos = GetAttributePosition(SelfItemPointerAttributeNumber, attno_mapping);
		ULONG root_ctid_pos = GetAttributePosition(RootSelfItemPointerAttributeNumber, attno_mapping);
		key_set->Append(GPOS_NEW(mp) ULONG(seg_id_pos));
		key_set->Append(GPOS_NEW(mp) ULONG(ctid_pos));
		key_set->Append(GPOS_NEW(mp) ULONG(root_ctid_pos));

		key_sets->Append(key_set);
	}
	
	return key_sets;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::NormalizeFrequencies
*
*	@doc:
*		Sometimes a set of frequencies can add up to more than 1.0.
*		Fix these cases
*
-------------------------------------------------------------------------*/
void
CTranslatorRelcacheToDXL::NormalizeFrequencies
	(
	float4 *freqs,
	ULONG length,
	CDouble *null_freq
	)
{
	if (length == 0 && (*null_freq) < 1.0)
	{
		return;
	}

	CDouble total = *null_freq;
	for (ULONG ul = 0; ul < length; ul++)
	{
		total = total + CDouble(freqs[ul]);
	}

	if (total > CDouble(1.0))
	{
		float4 denom = (float4) (total + CStatistics::Epsilon).Get();

		// divide all values by the total
		for (ULONG ul = 0; ul < length; ul++)
		{
			freqs[ul] = freqs[ul] / denom;
		}
		*null_freq = *null_freq / denom;
	}

#ifdef GPOS_DEBUG
	// recheck
	CDouble recheck_total = *null_freq;
	for (ULONG ul = 0; ul < length; ul++)
	{
		recheck_total = recheck_total + CDouble(freqs[ul]);
	}
	GPOS_ASSERT(recheck_total <= CDouble(1.0));
#endif
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::IsIndexSupported
*
*	@doc:
*		Check if index type is supported
*
-------------------------------------------------------------------------*/
BOOL
CTranslatorRelcacheToDXL::IsIndexSupported
	(
	Relation index_rel
	)
{
	HeapTupleData *tup = index_rel->rd_indextuple;

	// covering index -- it has INCLUDE (...) columns
	GPOS_ASSERT(index_rel->rd_index);
	if (index_rel->rd_index->indnatts > index_rel->rd_index->indnkeyatts)
		return false;

	// index expressions and index constraints not supported
	return px::HeapAttIsNull(tup, Anum_pg_index_indexprs) &&
		px::HeapAttIsNull(tup, Anum_pg_index_indpred) &&
		(/* BITMAP_AM_OID == index_rel->rd_rel->relam || */
		 GIST_AM_OID == index_rel->rd_rel->relam ||
		 GIN_AM_OID == index_rel->rd_rel->relam ||
		 (px_optimizer_enable_brinscan && BRIN_AM_OID == index_rel->rd_rel->relam) ||
		 (BTREE_AM_OID == index_rel->rd_rel->relam 
		 /* TODO:: not support global index for px now @yanhua */
		 //TODO:: need open for oracle branch
		 //  && (NULL == index_rel->rd_options || 
		 //  	 !((StdRdOptions *)(index_rel->rd_options))->global_index)
		 ));
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorRelcacheToDXL::RelHasSystemColumns
*
*	@doc:
*		Does given relation type have system columns.
//		Currently only regular relations, sequences, toast values relations and
//		AO segment relations have system columns
*
-------------------------------------------------------------------------*/
BOOL
CTranslatorRelcacheToDXL::RelHasSystemColumns
	(
	char rel_kind
	)
{
	return RELKIND_RELATION == rel_kind ||
			RELKIND_SEQUENCE == rel_kind ||
      //RELKIND_AOSEGMENTS == rel_kind ||
			RELKIND_TOASTVALUE == rel_kind ||
			RELKIND_PARTITIONED_TABLE == rel_kind;
}

/*-------------------------------------------------------------------------
*	@function:
//		CTranslatorRelcacheToDXL::ParseCmpType
*
*	@doc:
//		Translate GPDB comparison types into optimizer comparison types
*
-------------------------------------------------------------------------*/
IMDType::ECmpType
CTranslatorRelcacheToDXL::ParseCmpType
	(
	ULONG cmpt
	)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(cmp_type_mappings); ul++)
	{
		const ULONG *mapping = cmp_type_mappings[ul];
		if (mapping[1] == cmpt)
		{
			return (IMDType::ECmpType) mapping[0];
		}
	}
	
	return IMDType::EcmptOther;
}

/*-------------------------------------------------------------------------
*	@function:
//		CTranslatorRelcacheToDXL::GetComparisonType
*
*	@doc:
//		Translate optimizer comparison types into GPDB comparison types
*
-------------------------------------------------------------------------*/
ULONG 
CTranslatorRelcacheToDXL::GetComparisonType
	(
	IMDType::ECmpType cmp_type
	)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(cmp_type_mappings); ul++)
	{
		const ULONG *mapping = cmp_type_mappings[ul];
		if (mapping[0] == cmp_type)
		{
			return (ULONG) mapping[1];
		}
	}
	
	return CmptOther;
}

/*-------------------------------------------------------------------------
*	@function:
//		CTranslatorRelcacheToDXL::RetrieveIndexOpFamilies
*
*	@doc:
//		Retrieve the opfamilies for the keys of the given index
*
-------------------------------------------------------------------------*/
IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveIndexOpFamilies
	(
	CMemoryPool *mp,
	IMDId *mdid_index
	)
{
	List *op_families = px::GetIndexOpFamilies(CMDIdGPDB::CastMdid(mdid_index)->Oid());
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);
	
	ListCell *lc = NULL;
	
	ForEach(lc, op_families)
	{
		OID op_family_oid = lfirst_oid(lc);
		input_col_mdids->Append(GPOS_NEW(mp) CMDIdGPDB(op_family_oid));
	}
	
	return input_col_mdids;
}

/*-------------------------------------------------------------------------
*	@function:
//		CTranslatorRelcacheToDXL::RetrieveScOpOpFamilies
*
*	@doc:
//		Retrieve the families for the keys of the given scalar operator
*
-------------------------------------------------------------------------*/
IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveScOpOpFamilies
	(
	CMemoryPool *mp,
	IMDId *mdid_scalar_op
	)
{
	List *op_families = px::GetOpFamiliesForScOp(CMDIdGPDB::CastMdid(mdid_scalar_op)->Oid());
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);
	
	ListCell *lc = NULL;
	
	ForEach(lc, op_families)
	{
		OID op_family_oid = lfirst_oid(lc);
		input_col_mdids->Append(GPOS_NEW(mp) CMDIdGPDB(op_family_oid));
	}
	
	return input_col_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartConstraintForRel
//
//	@doc:
//		Retrieve part constraint for relation
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorRelcacheToDXL::RetrievePartConstraintForRel(
	CMemoryPool *mp, CMDAccessor *md_accessor, Relation rel,
	CMDColumnArray *mdcol_array)
{
	// get the part constraints
	Node *node = px::GetRelationPartConstraints(rel);
	if (nullptr == node)
	{
		return nullptr;
	}
	// create var-colid mapping for translating part constraints
	CAutoRef<CDXLColDescrArray> dxl_col_descr_array(GPOS_NEW(mp)
														CDXLColDescrArray(mp));
	const ULONG num_columns = mdcol_array->Size();
	for (ULONG ul = 0, idx = 0; ul < num_columns; ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		if (md_col->IsDropped())
		{
			continue;
		}
		CMDName *md_colname =
			GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
		CMDIdGPDB *mdid_col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
		mdid_col_type->AddRef();
		// create a column descriptor for the column
		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
			md_colname,
			idx + 1,  // colid
			md_col->AttrNum(), mdid_col_type, md_col->TypeModifier(),
			false  // fColDropped
		);
		dxl_col_descr_array->Append(dxl_col_descr);
		++idx;
	}
	CMappingVarColId var_colid_mapping(mp);
	var_colid_mapping.LoadColumns(0 /*query_level */, 1 /* rteIndex */,
								  dxl_col_descr_array.Value());
	CDXLNode *scalar_dxlnode =
		CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(
			mp, md_accessor, &var_colid_mapping, (Expr *) node);
	return scalar_dxlnode;
}

IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveIndexPartitions(CMemoryPool *mp, OID rel_oid)
{
	IMdIdArray *partition_oids = GPOS_NEW(mp) IMdIdArray(mp);
	List *partition_oid_list = px::GetAllChildPartIndexes(rel_oid);
	ListCell *lc;
	foreach (lc, partition_oid_list)
	{
		OID oid = lfirst_oid(lc);
		partition_oids->Append(GPOS_NEW(mp) CMDIdGPDB(oid));
	}
	return partition_oids;
}

IMdIdArray *
CTranslatorRelcacheToDXL::RetrieveRelDistributionOpFamilies(CMemoryPool *mp,
															PxPolicy *px_policy)
{
	IMdIdArray *distr_op_classes = GPOS_NEW(mp) IMdIdArray(mp);

	Oid *opclasses = px_policy->opclasses;
	for (ULONG ul = 0; ul < (ULONG) px_policy->nattrs; ul++)
	{
		Oid opfamily = px::GetOpclassFamily(opclasses[ul]);
		distr_op_classes->Append(GPOS_NEW(mp) CMDIdGPDB(opfamily));
	}

	return distr_op_classes;
}

// EOF

