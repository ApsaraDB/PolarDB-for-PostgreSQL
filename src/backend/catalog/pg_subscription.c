/*-------------------------------------------------------------------------
 *
 * pg_subscription.c
 *		replication subscriptions
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_subscription.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "nodes/makefuncs.h"

#include "storage/lmgr.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#include "utils/tqual.h"
#endif


static List *textarray_to_stringlist(ArrayType *textarray);

/*
 * Fetch the subscription from the syscache.
 */
Subscription *
GetSubscription(Oid subid, bool missing_ok)
{
	HeapTuple	tup;
	Subscription *sub;
	Form_pg_subscription subform;
	Datum		datum;
	bool		isnull;

	tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
			return NULL;

		elog(ERROR, "cache lookup failed for subscription %u", subid);
	}

	subform = (Form_pg_subscription) GETSTRUCT(tup);

	sub = (Subscription *) palloc(sizeof(Subscription));
	sub->oid = subid;
	sub->dbid = subform->subdbid;
	sub->name = pstrdup(NameStr(subform->subname));
	sub->owner = subform->subowner;
	sub->enabled = subform->subenabled;

	/* Get conninfo */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subconninfo,
							&isnull);
	Assert(!isnull);
	sub->conninfo = TextDatumGetCString(datum);

	/* Get slotname */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subslotname,
							&isnull);
	if (!isnull)
		sub->slotname = pstrdup(NameStr(*DatumGetName(datum)));
	else
		sub->slotname = NULL;

	/* Get synccommit */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subsynccommit,
							&isnull);
	Assert(!isnull);
	sub->synccommit = TextDatumGetCString(datum);

	/* Get publications */
	datum = SysCacheGetAttr(SUBSCRIPTIONOID,
							tup,
							Anum_pg_subscription_subpublications,
							&isnull);
	Assert(!isnull);
	sub->publications = textarray_to_stringlist(DatumGetArrayTypeP(datum));

	ReleaseSysCache(tup);

	return sub;
}

/*
 * Return number of subscriptions defined in given database.
 * Used by dropdb() to check if database can indeed be dropped.
 */
int
CountDBSubscriptions(Oid dbid)
{
	int			nsubs = 0;
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tup;

	rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_subscription_subdbid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dbid));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
		nsubs++;

	systable_endscan(scan);

	heap_close(rel, NoLock);

	return nsubs;
}

/*
 * Free memory allocated by subscription struct.
 */
void
FreeSubscription(Subscription *sub)
{
	pfree(sub->name);
	pfree(sub->conninfo);
	if (sub->slotname)
		pfree(sub->slotname);
	list_free_deep(sub->publications);
	pfree(sub);
}

/*
 * get_subscription_oid - given a subscription name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_subscription_oid(const char *subname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid2(SUBSCRIPTIONNAME, MyDatabaseId,
						  CStringGetDatum(subname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("subscription \"%s\" does not exist", subname)));
	return oid;
}

/*
 * get_subscription_name - given a subscription OID, look up the name
 */
char *
get_subscription_name(Oid subid)
{
	HeapTuple	tup;
	char	   *subname;
	Form_pg_subscription subform;

	tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for subscription %u", subid);

	subform = (Form_pg_subscription) GETSTRUCT(tup);
	subname = pstrdup(NameStr(subform->subname));

	ReleaseSysCache(tup);

	return subname;
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list of strings is pallocated here.
 */
static List *
textarray_to_stringlist(ArrayType *textarray)
{
	Datum	   *elems;
	int			nelems,
				i;
	List	   *res = NIL;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return NIL;

	for (i = 0; i < nelems; i++)
		res = lappend(res, makeString(TextDatumGetCString(elems[i])));

	return res;
}

/*
 * Add new state record for a subscription table.
 */
Oid
AddSubscriptionRelState(Oid subid, Oid relid, char state,
						XLogRecPtr sublsn)
{
	Relation	rel;
	HeapTuple	tup;
	Oid			subrelid;
	bool		nulls[Natts_pg_subscription_rel];
	Datum		values[Natts_pg_subscription_rel];

	LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	/* Try finding existing mapping. */
	tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP,
							  ObjectIdGetDatum(relid),
							  ObjectIdGetDatum(subid));
	if (HeapTupleIsValid(tup))
		elog(ERROR, "subscription table %u in subscription %u already exists",
			 relid, subid);

	/* Form the tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	values[Anum_pg_subscription_rel_srsubid - 1] = ObjectIdGetDatum(subid);
	values[Anum_pg_subscription_rel_srrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);
	if (sublsn != InvalidXLogRecPtr)
		values[Anum_pg_subscription_rel_srsublsn - 1] = LSNGetDatum(sublsn);
	else
		nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	/**
        * To fix subscription regression failure.
        * If create subscription with copy_data=false, the default srsubstartts will be 0 which is invalid,
        * but the apply of DML on subscription expects a valid srsubstartts. So we set MinValidCommitSeqNo which is 1
        * as srsubstartts.
        *
        * If create subscription with copy_data=true, the default srsubstartts works fine since the srsubstate initially
        * is 'i', and applyMainWorker will connect to publication instance, get valid srsubstartts, and update pg_subscription_rel.srsubstartts.
        * But if copy_data=false, the srsubstate initially is 'r', applyMainWorker will not update pg_subscription_rel.srsubstartts to a valid value.
        */
	values[Anum_pg_subscription_rel_srsubstartts - 1] = Int64GetDatum((int64) MinValidCommitSeqNo);
#endif
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	subrelid = CatalogTupleInsert(rel, tup);

	heap_freetuple(tup);

	/* Cleanup. */
	heap_close(rel, NoLock);

	return subrelid;
}

/*
 * Update the state of a subscription table.
 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
Oid
UpdateSubscriptionRelStateExtend(Oid subid, Oid relid, char state,
								 XLogRecPtr sublsn
								 ,GlobalTimestamp startts)
#else
UpdateSubscriptionRelState(Oid subid, Oid relid, char state,
						   XLogRecPtr sublsn)
#endif
{
	Relation	rel;
	HeapTuple	tup;
	Oid			subrelid;
	bool		nulls[Natts_pg_subscription_rel];
	Datum		values[Natts_pg_subscription_rel];
	bool		replaces[Natts_pg_subscription_rel];

	LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	/* Try finding existing mapping. */
	tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP,
							  ObjectIdGetDatum(relid),
							  ObjectIdGetDatum(subid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "subscription table %u in subscription %u does not exist",
			 relid, subid);

	/* Update the tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_pg_subscription_rel_srsubstate - 1] = true;
	values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);

	replaces[Anum_pg_subscription_rel_srsublsn - 1] = true;
	if (sublsn != InvalidXLogRecPtr)
		values[Anum_pg_subscription_rel_srsublsn - 1] = LSNGetDatum(sublsn);
	else
		nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	replaces[Anum_pg_subscription_rel_srsubstartts - 1] = true;
	if (startts != InvalidCommitSeqNo)
		values[Anum_pg_subscription_rel_srsubstartts - 1] = Int64GetDatum((int64) startts);
	else
		nulls[Anum_pg_subscription_rel_srsubstartts - 1] = true;

	if (enable_distri_print)
		elog(LOG, "logical replication update sub oid %d, relid %d, start ts " UINT64_FORMAT
			 " null %d LSN " UINT64_FORMAT,
			 subid, relid, startts, nulls[Anum_pg_subscription_rel_srsubstartts - 1], sublsn);
#endif

	tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
							replaces);

	/* Update the catalog. */
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	subrelid = HeapTupleGetOid(tup);

	/* Cleanup. */
	heap_close(rel, NoLock);

	return subrelid;
}

/*
 * Get state of subscription table.
 *
 * Returns SUBREL_STATE_UNKNOWN when not found and missing_ok is true.
 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
char
GetSubscriptionRelStateExtend(Oid subid, Oid relid, XLogRecPtr *sublsn, GlobalTimestamp * startts,
							  bool missing_ok)
#else
char
GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn,
						bool missing_ok)
#endif
{
	Relation	rel;
	HeapTuple	tup;
	char		substate;
	bool		isnull;
	Datum		d;

	rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

	/* Try finding the mapping. */
	tup = SearchSysCache2(SUBSCRIPTIONRELMAP,
						  ObjectIdGetDatum(relid),
						  ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
		{
			heap_close(rel, AccessShareLock);
			*sublsn = InvalidXLogRecPtr;
			return SUBREL_STATE_UNKNOWN;
		}

		elog(ERROR, "subscription table %u in subscription %u does not exist",
			 relid, subid);
	}

	/* Get the state. */
	d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
						Anum_pg_subscription_rel_srsubstate, &isnull);
	Assert(!isnull);
	substate = DatumGetChar(d);
	d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
						Anum_pg_subscription_rel_srsublsn, &isnull);
	if (isnull)
		*sublsn = InvalidXLogRecPtr;
	else
		*sublsn = DatumGetLSN(d);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (startts)
	{
		d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
							Anum_pg_subscription_rel_srsubstartts, &isnull);
		if (isnull)
			*startts = InvalidCommitSeqNo;
		else
			*startts = (GlobalTimestamp) DatumGetInt64(d);

		if (enable_distri_print)
			elog(LOG, "logical replication get sub subid %d relid %d start ts " UINT64_FORMAT
				 " isnull %d LSN " UINT64_FORMAT
				 ,subid, relid, *startts, isnull, *sublsn);
	}
#endif

	/* Cleanup */
	ReleaseSysCache(tup);
	heap_close(rel, AccessShareLock);

	return substate;
}

/*
 * Drop subscription relation mapping. These can be for a particular
 * subscription, or for a particular relation, or both.
 */
void
RemoveSubscriptionRel(Oid subid, Oid relid)
{
	Relation	rel;
	HeapScanDesc scan;
	ScanKeyData skey[2];
	HeapTuple	tup;
	int			nkeys = 0;

	rel = heap_open(SubscriptionRelRelationId, RowExclusiveLock);

	if (OidIsValid(subid))
	{
		ScanKeyInit(&skey[nkeys++],
					Anum_pg_subscription_rel_srsubid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(subid));
	}

	if (OidIsValid(relid))
	{
		ScanKeyInit(&skey[nkeys++],
					Anum_pg_subscription_rel_srrelid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(relid));
	}

	/* Do the search and delete what we found. */
	scan = heap_beginscan_catalog(rel, nkeys, skey);
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		CatalogTupleDelete(rel, &tup->t_self);
	}
	heap_endscan(scan);

	heap_close(rel, RowExclusiveLock);
}


/*
 * Get all relations for subscription.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetSubscriptionRelations(Oid subid)
{
	List	   *res = NIL;
	Relation	rel;
	HeapTuple	tup;
	int			nkeys = 0;
	ScanKeyData skey[2];
	SysScanDesc scan;

	rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

	ScanKeyInit(&skey[nkeys++],
				Anum_pg_subscription_rel_srsubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, nkeys, skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_subscription_rel subrel;
		SubscriptionRelState *relstate;

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		relstate = (SubscriptionRelState *) palloc(sizeof(SubscriptionRelState));
		relstate->relid = subrel->srrelid;
		relstate->state = subrel->srsubstate;
		relstate->lsn = subrel->srsublsn;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		relstate->start_ts = subrel->srsubstartts;
#endif

		res = lappend(res, relstate);
	}

	/* Cleanup */
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return res;
}

/*
 * Get all relations for subscription that are not in a ready state.
 *
 * Returned list is palloc'ed in current memory context.
 */
List *
GetSubscriptionNotReadyRelations(Oid subid)
{
	List	   *res = NIL;
	Relation	rel;
	HeapTuple	tup;
	int			nkeys = 0;
	ScanKeyData skey[2];
	SysScanDesc scan;

	rel = heap_open(SubscriptionRelRelationId, AccessShareLock);

	ScanKeyInit(&skey[nkeys++],
				Anum_pg_subscription_rel_srsubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	ScanKeyInit(&skey[nkeys++],
				Anum_pg_subscription_rel_srsubstate,
				BTEqualStrategyNumber, F_CHARNE,
				CharGetDatum(SUBREL_STATE_READY));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, nkeys, skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_subscription_rel subrel;
		SubscriptionRelState *relstate;

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		relstate = (SubscriptionRelState *) palloc(sizeof(SubscriptionRelState));
		relstate->relid = subrel->srrelid;
		relstate->state = subrel->srsubstate;
		relstate->lsn = subrel->srsublsn;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		relstate->start_ts = subrel->srsubstartts;
#endif

		res = lappend(res, relstate);
	}

	/* Cleanup */
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return res;
}
