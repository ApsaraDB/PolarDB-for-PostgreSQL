/*-------------------------------------------------------------------------
 *
 * pg_depend.c
 *	  routines to support manipulation of the pg_depend relation
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_depend.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/tqual.h"


static bool isObjectPinned(const ObjectAddress *object, Relation rel);


/*
 * Record a dependency between 2 objects via their respective objectAddress.
 * The first argument is the dependent object, the second the one it
 * references.
 *
 * This simply creates an entry in pg_depend, without any other processing.
 */
void
recordDependencyOn(const ObjectAddress *depender,
				   const ObjectAddress *referenced,
				   DependencyType behavior)
{
	recordMultipleDependencies(depender, referenced, 1, behavior);
}

/*
 * Record multiple dependencies (of the same kind) for a single dependent
 * object.  This has a little less overhead than recording each separately.
 */
void
recordMultipleDependencies(const ObjectAddress *depender,
						   const ObjectAddress *referenced,
						   int nreferenced,
						   DependencyType behavior)
{
	Relation	dependDesc;
	CatalogIndexState indstate;
	HeapTuple	tup;
	int			i;
	bool		nulls[Natts_pg_depend];
	Datum		values[Natts_pg_depend];

	if (nreferenced <= 0)
		return;					/* nothing to do */

	/*
	 * During bootstrap, do nothing since pg_depend may not exist yet. initdb
	 * will fill in appropriate pg_depend entries after bootstrap.
	 */
	if (IsBootstrapProcessingMode())
		return;

	dependDesc = heap_open(DependRelationId, RowExclusiveLock);

	/* Don't open indexes unless we need to make an update */
	indstate = NULL;

	memset(nulls, false, sizeof(nulls));

	for (i = 0; i < nreferenced; i++, referenced++)
	{
		/*
		 * If the referenced object is pinned by the system, there's no real
		 * need to record dependencies on it.  This saves lots of space in
		 * pg_depend, so it's worth the time taken to check.
		 */
		if (!isObjectPinned(referenced, dependDesc))
		{
			/*
			 * Record the Dependency.  Note we don't bother to check for
			 * duplicate dependencies; there's no harm in them.
			 */
			values[Anum_pg_depend_classid - 1] = ObjectIdGetDatum(depender->classId);
			values[Anum_pg_depend_objid - 1] = ObjectIdGetDatum(depender->objectId);
			values[Anum_pg_depend_objsubid - 1] = Int32GetDatum(depender->objectSubId);

			values[Anum_pg_depend_refclassid - 1] = ObjectIdGetDatum(referenced->classId);
			values[Anum_pg_depend_refobjid - 1] = ObjectIdGetDatum(referenced->objectId);
			values[Anum_pg_depend_refobjsubid - 1] = Int32GetDatum(referenced->objectSubId);

			values[Anum_pg_depend_deptype - 1] = CharGetDatum((char) behavior);

			tup = heap_form_tuple(dependDesc->rd_att, values, nulls);

			/* fetch index info only when we know we need it */
			if (indstate == NULL)
				indstate = CatalogOpenIndexes(dependDesc);

			CatalogTupleInsertWithInfo(dependDesc, tup, indstate);

			heap_freetuple(tup);
		}
	}

	if (indstate != NULL)
		CatalogCloseIndexes(indstate);

	heap_close(dependDesc, RowExclusiveLock);
}

/*
 * If we are executing a CREATE EXTENSION operation, mark the given object
 * as being a member of the extension, or check that it already is one.
 * Otherwise, do nothing.
 *
 * This must be called during creation of any user-definable object type
 * that could be a member of an extension.
 *
 * isReplace must be true if the object already existed, and false if it is
 * newly created.  In the former case we insist that it already be a member
 * of the current extension.  In the latter case we can skip checking whether
 * it is already a member of any extension.
 *
 * Note: isReplace = true is typically used when updating a object in
 * CREATE OR REPLACE and similar commands.  We used to allow the target
 * object to not already be an extension member, instead silently absorbing
 * it into the current extension.  However, this was both error-prone
 * (extensions might accidentally overwrite free-standing objects) and
 * a security hazard (since the object would retain its previous ownership).
 */
void
recordDependencyOnCurrentExtension(const ObjectAddress *object,
								   bool isReplace)
{
	/* Only whole objects can be extension members */
	Assert(object->objectSubId == 0);

	if (creating_extension)
	{
		ObjectAddress extension;

		/* Only need to check for existing membership if isReplace */
		if (isReplace)
		{
			Oid			oldext;

			/*
			 * Side note: these catalog lookups are safe only because the
			 * object is a pre-existing one.  In the not-isReplace case, the
			 * caller has most likely not yet done a CommandCounterIncrement
			 * that would make the new object visible.
			 */
			oldext = getExtensionOfObject(object->classId, object->objectId);
			if (OidIsValid(oldext))
			{
				/* If already a member of this extension, nothing to do */
				if (oldext == CurrentExtensionObject)
					return;
				/* Already a member of some other extension, so reject */
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("%s is already a member of extension \"%s\"",
								getObjectDescription(object),
								get_extension_name(oldext))));
			}
			/* It's a free-standing object, so reject */
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("%s is not a member of extension \"%s\"",
							getObjectDescription(object),
							get_extension_name(CurrentExtensionObject)),
					 errdetail("An extension is not allowed to replace an object that it does not own.")));
		}

		/* OK, record it as a member of CurrentExtensionObject */
		extension.classId = ExtensionRelationId;
		extension.objectId = CurrentExtensionObject;
		extension.objectSubId = 0;

		recordDependencyOn(object, &extension, DEPENDENCY_EXTENSION);
	}
}

/*
 * If we are executing a CREATE EXTENSION operation, check that the given
 * object is a member of the extension, and throw an error if it isn't.
 * Otherwise, do nothing.
 *
 * This must be called whenever a CREATE IF NOT EXISTS operation (for an
 * object type that can be an extension member) has found that an object of
 * the desired name already exists.  It is insecure for an extension to use
 * IF NOT EXISTS except when the conflicting object is already an extension
 * member; otherwise a hostile user could substitute an object with arbitrary
 * properties.
 */
void
checkMembershipInCurrentExtension(const ObjectAddress *object)
{
	/*
	 * This is actually the same condition tested in
	 * recordDependencyOnCurrentExtension; but we want to issue a
	 * differently-worded error, and anyway it would be pretty confusing to
	 * call recordDependencyOnCurrentExtension in these circumstances.
	 */

	/* Only whole objects can be extension members */
	Assert(object->objectSubId == 0);

	if (creating_extension)
	{
		Oid			oldext;

		oldext = getExtensionOfObject(object->classId, object->objectId);
		/* If already a member of this extension, OK */
		if (oldext == CurrentExtensionObject)
			return;
		/* Else complain */
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s is not a member of extension \"%s\"",
						getObjectDescription(object),
						get_extension_name(CurrentExtensionObject)),
				 errdetail("An extension may only use CREATE ... IF NOT EXISTS to skip object creation if the conflicting object is one that it already owns.")));
	}
}

/*
 * deleteDependencyRecordsFor -- delete all records with given depender
 * classId/objectId.  Returns the number of records deleted.
 *
 * This is used when redefining an existing object.  Links leading to the
 * object do not change, and links leading from it will be recreated
 * (possibly with some differences from before).
 *
 * If skipExtensionDeps is true, we do not delete any dependencies that
 * show that the given object is a member of an extension.  This avoids
 * needing a lot of extra logic to fetch and recreate that dependency.
 */
long
deleteDependencyRecordsFor(Oid classId, Oid objectId,
						   bool skipExtensionDeps)
{
	long		count = 0;
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		if (skipExtensionDeps &&
			((Form_pg_depend) GETSTRUCT(tup))->deptype == DEPENDENCY_EXTENSION)
			continue;

		CatalogTupleDelete(depRel, &tup->t_self);
		count++;
	}

	systable_endscan(scan);

	heap_close(depRel, RowExclusiveLock);

	return count;
}

/*
 * deleteDependencyRecordsForClass -- delete all records with given depender
 * classId/objectId, dependee classId, and deptype.
 * Returns the number of records deleted.
 *
 * This is a variant of deleteDependencyRecordsFor, useful when revoking
 * an object property that is expressed by a dependency record (such as
 * extension membership).
 */
long
deleteDependencyRecordsForClass(Oid classId, Oid objectId,
								Oid refclassId, char deptype)
{
	long		count = 0;
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == refclassId && depform->deptype == deptype)
		{
			CatalogTupleDelete(depRel, &tup->t_self);
			count++;
		}
	}

	systable_endscan(scan);

	heap_close(depRel, RowExclusiveLock);

	return count;
}

/*
 * Adjust dependency record(s) to point to a different object of the same type
 *
 * classId/objectId specify the referencing object.
 * refClassId/oldRefObjectId specify the old referenced object.
 * newRefObjectId is the new referenced object (must be of class refClassId).
 *
 * Note the lack of objsubid parameters.  If there are subobject references
 * they will all be readjusted.
 *
 * Returns the number of records updated.
 */
long
changeDependencyFor(Oid classId, Oid objectId,
					Oid refClassId, Oid oldRefObjectId,
					Oid newRefObjectId)
{
	long		count = 0;
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;
	ObjectAddress objAddr;
	bool		newIsPinned;

	depRel = heap_open(DependRelationId, RowExclusiveLock);

	/*
	 * If oldRefObjectId is pinned, there won't be any dependency entries on
	 * it --- we can't cope in that case.  (This isn't really worth expending
	 * code to fix, in current usage; it just means you can't rename stuff out
	 * of pg_catalog, which would likely be a bad move anyway.)
	 */
	objAddr.classId = refClassId;
	objAddr.objectId = oldRefObjectId;
	objAddr.objectSubId = 0;

	if (isObjectPinned(&objAddr, depRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot remove dependency on %s because it is a system object",
						getObjectDescription(&objAddr))));

	/*
	 * We can handle adding a dependency on something pinned, though, since
	 * that just means deleting the dependency entry.
	 */
	objAddr.objectId = newRefObjectId;

	newIsPinned = isObjectPinned(&objAddr, depRel);

	/* Now search for dependency records */
	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == refClassId &&
			depform->refobjid == oldRefObjectId)
		{
			if (newIsPinned)
				CatalogTupleDelete(depRel, &tup->t_self);
			else
			{
				/* make a modifiable copy */
				tup = heap_copytuple(tup);
				depform = (Form_pg_depend) GETSTRUCT(tup);

				depform->refobjid = newRefObjectId;

				CatalogTupleUpdate(depRel, &tup->t_self, tup);

				heap_freetuple(tup);
			}

			count++;
		}
	}

	systable_endscan(scan);

	heap_close(depRel, RowExclusiveLock);

	return count;
}

/*
 * isObjectPinned()
 *
 * Test if an object is required for basic database functionality.
 * Caller must already have opened pg_depend.
 *
 * The passed subId, if any, is ignored; we assume that only whole objects
 * are pinned (and that this implies pinning their components).
 */
static bool
isObjectPinned(const ObjectAddress *object, Relation rel)
{
	bool		ret = false;
	SysScanDesc scan;
	HeapTuple	tup;
	ScanKeyData key[2];

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(object->classId));

	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(object->objectId));

	scan = systable_beginscan(rel, DependReferenceIndexId, true,
							  NULL, 2, key);

	/*
	 * Since we won't generate additional pg_depend entries for pinned
	 * objects, there can be at most one entry referencing a pinned object.
	 * Hence, it's sufficient to look at the first returned tuple; we don't
	 * need to loop.
	 */
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

		if (foundDep->deptype == DEPENDENCY_PIN)
			ret = true;
	}

	systable_endscan(scan);

	return ret;
}


/*
 * Various special-purpose lookups and manipulations of pg_depend.
 */


/*
 * Find the extension containing the specified object, if any
 *
 * Returns the OID of the extension, or InvalidOid if the object does not
 * belong to any extension.
 *
 * Extension membership is marked by an EXTENSION dependency from the object
 * to the extension.  Note that the result will be indeterminate if pg_depend
 * contains links from this object to more than one extension ... but that
 * should never happen.
 */
Oid
getExtensionOfObject(Oid classId, Oid objectId)
{
	Oid			result = InvalidOid;
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == ExtensionRelationId &&
			depform->deptype == DEPENDENCY_EXTENSION)
		{
			result = depform->refobjid;
			break;				/* no need to keep scanning */
		}
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	return result;
}

/*
 * Return (possibly NIL) list of extensions that the given object depends on
 * in DEPENDENCY_AUTO_EXTENSION mode.
 */
List *
getAutoExtensionsOfObject(Oid classId, Oid objectId)
{
	List	   *result = NIL;
	Relation	depRel;
	ScanKeyData	key[2];
	SysScanDesc	scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == ExtensionRelationId &&
			depform->deptype == DEPENDENCY_AUTO_EXTENSION)
			result = lappend_oid(result, depform->refobjid);
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	return result;
}

/*
 * Detect whether a sequence is marked as "owned" by a column
 *
 * An ownership marker is an AUTO or INTERNAL dependency from the sequence to the
 * column.  If we find one, store the identity of the owning column
 * into *tableId and *colId and return true; else return false.
 *
 * Note: if there's more than one such pg_depend entry then you get
 * a random one of them returned into the out parameters.  This should
 * not happen, though.
 */
bool
sequenceIsOwned(Oid seqId, char deptype, Oid *tableId, int32 *colId)
{
	bool		ret = false;
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqId));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == RelationRelationId &&
			depform->deptype == deptype)
		{
			*tableId = depform->refobjid;
			*colId = depform->refobjsubid;
			ret = true;
			break;				/* no need to keep scanning */
		}
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	return ret;
}

/*
 * Collect a list of OIDs of all sequences owned by the specified relation,
 * and column if specified.
 */
List *
getOwnedSequences(Oid relid, AttrNumber attnum)
{
	List	   *result = NIL;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	tup;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	if (attnum)
		ScanKeyInit(&key[2],
					Anum_pg_depend_refobjsubid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(attnum));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, attnum ? 3 : 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any auto or internal dependency of a sequence on a column
		 * must be what we are looking for.  (We need the relkind test because
		 * indexes can also have auto dependencies on columns.)
		 */
		if (deprec->classid == RelationRelationId &&
			deprec->objsubid == 0 &&
			deprec->refobjsubid != 0 &&
			(deprec->deptype == DEPENDENCY_AUTO || deprec->deptype == DEPENDENCY_INTERNAL) &&
			get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE)
		{
			result = lappend_oid(result, deprec->objid);
		}
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	return result;
}

/*
 * Get owned sequence, error if not exactly one.
 */
Oid
getOwnedSequence(Oid relid, AttrNumber attnum)
{
	List	   *seqlist = getOwnedSequences(relid, attnum);

	if (list_length(seqlist) > 1)
		elog(ERROR, "more than one owned sequence found");
	else if (list_length(seqlist) < 1)
		elog(ERROR, "no owned sequence found");

	return linitial_oid(seqlist);
}

/*
 * get_constraint_index
 *		Given the OID of a unique, primary-key, or exclusion constraint,
 *		return the OID of the underlying index.
 *
 * Return InvalidOid if the index couldn't be found; this suggests the
 * given OID is bogus, but we leave it to caller to decide what to do.
 */
Oid
get_constraint_index(Oid constraintId)
{
	Oid			indexId = InvalidOid;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	tup;

	/* Search the dependency table for the dependent index */
	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ConstraintRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(constraintId));
	ScanKeyInit(&key[2],
				Anum_pg_depend_refobjsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(0));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any internal dependency of an index on the constraint
		 * must be what we are looking for.
		 */
		if (deprec->classid == RelationRelationId &&
			deprec->objsubid == 0 &&
			deprec->deptype == DEPENDENCY_INTERNAL)
		{
			char		relkind = get_rel_relkind(deprec->objid);

			/*
			 * This is pure paranoia; there shouldn't be any other relkinds
			 * dependent on a constraint.
			 */
			if (relkind != RELKIND_INDEX &&
				relkind != RELKIND_PARTITIONED_INDEX)
				continue;

			indexId = deprec->objid;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(depRel, AccessShareLock);

	return indexId;
}

/*
 * get_index_constraint
 *		Given the OID of an index, return the OID of the owning unique,
 *		primary-key, or exclusion constraint, or InvalidOid if there
 *		is no owning constraint.
 */
Oid
get_index_constraint(Oid indexId)
{
	Oid			constraintId = InvalidOid;
	Relation	depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple	tup;

	/* Search the dependency table for the index */
	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(indexId));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(0));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(tup);

		/*
		 * We assume any internal dependency on a constraint must be what we
		 * are looking for.
		 */
		if (deprec->refclassid == ConstraintRelationId &&
			deprec->refobjsubid == 0 &&
			deprec->deptype == DEPENDENCY_INTERNAL)
		{
			constraintId = deprec->refobjid;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(depRel, AccessShareLock);

	return constraintId;
}
