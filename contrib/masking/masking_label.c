/*-------------------------------------------------------------------------
*
* masking_label.c
*
* IDENTIFICATION
*    contrib/masking/masking_label.c
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"
#include "masking.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

static int	check_label_on_table_or_column(Relation label_tab_rel, Relation label_col_rel, Oid relid, AttrNumber colid);

/*
 * check if table or column is set a label
 */
static int
check_label_on_table_or_column(Relation label_tab_rel, Relation label_col_rel, Oid relid, AttrNumber colid)
{
	HeapTuple	tuple;
	ScanKeyData scankey[2];
	SysScanDesc sscan;
	bool		isNull;
	int			labelid = InvalidMaskingLabelId;
	Oid			label_tab_relid_idx_oid;

	ScanKeyInit(&scankey[0],
				Anum_polar_masking_label_tab_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	if (OidIsValid(label_tab_relid_idx_oid = GetPolarMaskingLabelTabRelidIdxid()))
	{
		sscan = systable_beginscan(label_tab_rel, label_tab_relid_idx_oid, true,
								   NULL, 1, scankey);
	}
	else
	{
		sscan = systable_beginscan(label_tab_rel, InvalidOid, false,
								   NULL, 1, scankey);
	}

	if (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		labelid = DatumGetInt32(heap_getattr(tuple, Anum_polar_masking_label_tab_labelid,
											 RelationGetDescr(label_tab_rel), &isNull));
		systable_endscan(sscan);
		return labelid;
	}
	systable_endscan(sscan);

	if (colid == InvalidAttrNumber)
	{
		ScanKeyInit(&scankey[0],
					Anum_polar_masking_label_col_relid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));

		sscan = systable_beginscan(label_col_rel, InvalidOid, false,
								   NULL, 1, scankey);
	}
	else
	{
		Oid			label_col_relcolid_idxid;

		ScanKeyInit(&scankey[0],
					Anum_polar_masking_label_col_relid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));
		ScanKeyInit(&scankey[1],
					Anum_polar_masking_label_col_colid,
					BTEqualStrategyNumber, F_INT2EQ,
					Int16GetDatum(colid));
		if (OidIsValid(label_col_relcolid_idxid = GetPolarMaskingLabelColRelidColidIdxid()))
		{
			sscan = systable_beginscan(label_col_rel, label_col_relcolid_idxid, true,
									   NULL, 2, scankey);
		}
		else
		{
			sscan = systable_beginscan(label_col_rel, InvalidOid, false,
									   NULL, 2, scankey);
		}

	}

	if (HeapTupleIsValid(tuple = systable_getnext(sscan)))
		labelid = DatumGetInt32(heap_getattr(tuple, Anum_polar_masking_label_col_labelid,
											 RelationGetDescr(label_col_rel), &isNull));

	systable_endscan(sscan);
	return labelid;
}

/*
 * check column's masking label and operator
 */
bool
check_masking_for_var(MaskingInfo * maskinfo)
{

	HeapTuple	tuple;
	bool		IsNull;
	int			labelid;
	Relation	label_tab_rel;
	Relation	label_col_rel;
	Relation	policy_rel;
	SysScanDesc scandesc;
	ScanKeyData scankey;
	Oid			label_tab_oid = GetPolarMaskingLabelTabRelid();
	Oid			label_col_oid = GetPolarMaskingLabelColRelid();
	Oid			policy_oid = GetPolarMaskingPolicyRelid();
	Oid			policy_id_idxid;

	if (!OidIsValid(maskinfo->relid) || !OidIsValid(label_tab_oid) ||
		!OidIsValid(label_col_oid) || !OidIsValid(policy_oid))
	{
		return false;
	}

	label_tab_rel = table_open(label_tab_oid, RowExclusiveLock);
	label_col_rel = table_open(label_col_oid, RowExclusiveLock);
	policy_rel = table_open(policy_oid, RowExclusiveLock);

	labelid = check_label_on_table_or_column(label_tab_rel, label_col_rel, maskinfo->relid, maskinfo->attnum);

	if (labelid == InvalidMaskingLabelId)
	{
		table_close(policy_rel, RowExclusiveLock);
		table_close(label_col_rel, RowExclusiveLock);
		table_close(label_tab_rel, RowExclusiveLock);
		return false;
	}

	ScanKeyInit(&scankey,
				Anum_polar_masking_policy_labelid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(labelid));

	if (OidIsValid(policy_id_idxid = GetPolarMaskingPolicyLabelidIdxid()))
	{
		scandesc = systable_beginscan(policy_rel, policy_id_idxid, true, NULL, 1, &scankey);
	}
	else
	{
		scandesc = systable_beginscan(policy_rel, InvalidOid, false, NULL, 1, &scankey);
	}
	tuple = systable_getnext(scandesc);

	Assert(HeapTupleIsValid(tuple));

	maskinfo->masking_op = DatumGetInt16(heap_getattr(tuple, Anum_polar_masking_policy_operator, RelationGetDescr(policy_rel), &IsNull));

	if (maskinfo->masking_op == MASKING_UNKNOWN)
	{
		systable_endscan(scandesc);
		table_close(policy_rel, RowExclusiveLock);
		table_close(label_col_rel, RowExclusiveLock);
		table_close(label_tab_rel, RowExclusiveLock);
		return false;
	}

	systable_endscan(scandesc);

	table_close(policy_rel, RowExclusiveLock);
	table_close(label_col_rel, RowExclusiveLock);
	table_close(label_tab_rel, RowExclusiveLock);
	return true;
}
