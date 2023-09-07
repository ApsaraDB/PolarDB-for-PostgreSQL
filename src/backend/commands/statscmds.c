/*-------------------------------------------------------------------------
 *
 * statscmds.c
 *	  Commands for creating and altering extended statistics objects
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/statscmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


static char *ChooseExtendedStatisticName(const char *name1, const char *name2,
							const char *label, Oid namespaceid);
static char *ChooseExtendedStatisticNameAddition(List *exprs);


/* qsort comparator for the attnums in CreateStatistics */
static int
compare_int16(const void *a, const void *b)
{
	int			av = *(const int16 *) a;
	int			bv = *(const int16 *) b;

	/* this can't overflow if int is wider than int16 */
	return (av - bv);
}

/*
 *		CREATE STATISTICS
 */
ObjectAddress
CreateStatistics(CreateStatsStmt *stmt)
{
	int16		attnums[STATS_MAX_DIMENSIONS];
	int			numcols = 0;
	char	   *namestr;
	NameData	stxname;
	Oid			statoid;
	Oid			namespaceId;
	Oid			stxowner = GetUserId();
	HeapTuple	htup;
	Datum		values[Natts_pg_statistic_ext];
	bool		nulls[Natts_pg_statistic_ext];
	int2vector *stxkeys;
	Relation	statrel;
	Relation	rel = NULL;
	Oid			relid;
	ObjectAddress parentobject,
				myself;
	Datum		types[2];		/* one for each possible type of statistic */
	int			ntypes;
	ArrayType  *stxkind;
	bool		build_ndistinct;
	bool		build_dependencies;
	bool		requested_type = false;
	int			i;
	ListCell   *cell;

	Assert(IsA(stmt, CreateStatsStmt));

	/*
	 * Examine the FROM clause.  Currently, we only allow it to be a single
	 * simple table, but later we'll probably allow multiple tables and JOIN
	 * syntax.  The grammar is already prepared for that, so we have to check
	 * here that what we got is what we can support.
	 */
	if (list_length(stmt->relations) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only a single relation is allowed in CREATE STATISTICS")));

	foreach(cell, stmt->relations)
	{
		Node	   *rln = (Node *) lfirst(cell);

		if (!IsA(rln, RangeVar))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only a single relation is allowed in CREATE STATISTICS")));

		/*
		 * CREATE STATISTICS will influence future execution plans but does
		 * not interfere with currently executing plans.  So it should be
		 * enough to take only ShareUpdateExclusiveLock on relation,
		 * conflicting with ANALYZE and other DDL that sets statistical
		 * information, but not with normal queries.
		 */
		rel = relation_openrv((RangeVar *) rln, ShareUpdateExclusiveLock);

		/* Restrict to allowed relation types */
		if (rel->rd_rel->relkind != RELKIND_RELATION &&
			rel->rd_rel->relkind != RELKIND_MATVIEW &&
			rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
			rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("relation \"%s\" is not a table, foreign table, or materialized view",
							RelationGetRelationName(rel))));

		/* You must own the relation to create stats on it */
		if (!pg_class_ownercheck(RelationGetRelid(rel), stxowner))
			aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(rel->rd_rel->relkind),
						   RelationGetRelationName(rel));
	}

	Assert(rel);
	relid = RelationGetRelid(rel);

	/*
	 * If the node has a name, split it up and determine creation namespace.
	 * If not (a possibility not considered by the grammar, but one which can
	 * occur via the "CREATE TABLE ... (LIKE)" command), then we put the
	 * object in the same namespace as the relation, and cons up a name for
	 * it.
	 */
	if (stmt->defnames)
		namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames,
														&namestr);
	else
	{
		namespaceId = RelationGetNamespace(rel);
		namestr = ChooseExtendedStatisticName(RelationGetRelationName(rel),
											  ChooseExtendedStatisticNameAddition(stmt->exprs),
											  "stat",
											  namespaceId);
	}
	namestrcpy(&stxname, namestr);

	/*
	 * Deal with the possibility that the statistics object already exists.
	 */
	if (SearchSysCacheExists2(STATEXTNAMENSP,
							  CStringGetDatum(namestr),
							  ObjectIdGetDatum(namespaceId)))
	{
		if (stmt->if_not_exists)
		{
			/*
			 * Since stats objects aren't members of extensions (see comments
			 * below), no need for checkMembershipInCurrentExtension here.
			 */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("statistics object \"%s\" already exists, skipping",
							namestr)));
			relation_close(rel, NoLock);
			return InvalidObjectAddress;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("statistics object \"%s\" already exists", namestr)));
	}

	/*
	 * Currently, we only allow simple column references in the expression
	 * list.  That will change someday, and again the grammar already supports
	 * it so we have to enforce restrictions here.  For now, we can convert
	 * the expression list to a simple array of attnums.  While at it, enforce
	 * some constraints.
	 */
	foreach(cell, stmt->exprs)
	{
		Node	   *expr = (Node *) lfirst(cell);
		ColumnRef  *cref;
		char	   *attname;
		HeapTuple	atttuple;
		Form_pg_attribute attForm;
		TypeCacheEntry *type;

		if (!IsA(expr, ColumnRef))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only simple column references are allowed in CREATE STATISTICS")));
		cref = (ColumnRef *) expr;

		if (list_length(cref->fields) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only simple column references are allowed in CREATE STATISTICS")));
		attname = strVal((Value *) linitial(cref->fields));

		atttuple = SearchSysCacheAttName(relid, attname);
		if (!HeapTupleIsValid(atttuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist",
							attname)));
		attForm = (Form_pg_attribute) GETSTRUCT(atttuple);

		/* Disallow use of system attributes in extended stats */
		if (attForm->attnum <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("statistics creation on system columns is not supported")));

		/* Disallow data types without a less-than operator */
		type = lookup_type_cache(attForm->atttypid, TYPECACHE_LT_OPR);
		if (type->lt_opr == InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" cannot be used in statistics because its type %s has no default btree operator class",
							attname, format_type_be(attForm->atttypid))));

		/* Make sure no more than STATS_MAX_DIMENSIONS columns are used */
		if (numcols >= STATS_MAX_DIMENSIONS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("cannot have more than %d columns in statistics",
							STATS_MAX_DIMENSIONS)));

		attnums[numcols] = attForm->attnum;
		numcols++;
		ReleaseSysCache(atttuple);
	}

	/*
	 * Check that at least two columns were specified in the statement. The
	 * upper bound was already checked in the loop above.
	 */
	if (numcols < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("extended statistics require at least 2 columns")));

	/*
	 * Sort the attnums, which makes detecting duplicates somewhat easier, and
	 * it does not hurt (it does not affect the efficiency, unlike for
	 * indexes, for example).
	 */
	qsort(attnums, numcols, sizeof(int16), compare_int16);

	/*
	 * Check for duplicates in the list of columns. The attnums are sorted so
	 * just check consecutive elements.
	 */
	for (i = 1; i < numcols; i++)
	{
		if (attnums[i] == attnums[i - 1])
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("duplicate column name in statistics definition")));
	}

	/* Form an int2vector representation of the sorted column list */
	stxkeys = buildint2vector(attnums, numcols);

	/*
	 * Parse the statistics kinds.
	 */
	build_ndistinct = false;
	build_dependencies = false;
	foreach(cell, stmt->stat_types)
	{
		char	   *type = strVal((Value *) lfirst(cell));

		if (strcmp(type, "ndistinct") == 0)
		{
			build_ndistinct = true;
			requested_type = true;
		}
		else if (strcmp(type, "dependencies") == 0)
		{
			build_dependencies = true;
			requested_type = true;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized statistics kind \"%s\"",
							type)));
	}
	/* If no statistic type was specified, build them all. */
	if (!requested_type)
	{
		build_ndistinct = true;
		build_dependencies = true;
	}

	/* construct the char array of enabled statistic types */
	ntypes = 0;
	if (build_ndistinct)
		types[ntypes++] = CharGetDatum(STATS_EXT_NDISTINCT);
	if (build_dependencies)
		types[ntypes++] = CharGetDatum(STATS_EXT_DEPENDENCIES);
	Assert(ntypes > 0 && ntypes <= lengthof(types));
	stxkind = construct_array(types, ntypes, CHAROID, 1, true, 'c');

	/*
	 * Everything seems fine, so let's build the pg_statistic_ext tuple.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	values[Anum_pg_statistic_ext_stxrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_statistic_ext_stxname - 1] = NameGetDatum(&stxname);
	values[Anum_pg_statistic_ext_stxnamespace - 1] = ObjectIdGetDatum(namespaceId);
	values[Anum_pg_statistic_ext_stxowner - 1] = ObjectIdGetDatum(stxowner);
	values[Anum_pg_statistic_ext_stxkeys - 1] = PointerGetDatum(stxkeys);
	values[Anum_pg_statistic_ext_stxkind - 1] = PointerGetDatum(stxkind);

	/* no statistics built yet */
	nulls[Anum_pg_statistic_ext_stxndistinct - 1] = true;
	nulls[Anum_pg_statistic_ext_stxdependencies - 1] = true;

	/* insert it into pg_statistic_ext */
	statrel = heap_open(StatisticExtRelationId, RowExclusiveLock);
	htup = heap_form_tuple(statrel->rd_att, values, nulls);
	statoid = CatalogTupleInsert(statrel, htup);
	heap_freetuple(htup);
	relation_close(statrel, RowExclusiveLock);

	/*
	 * Invalidate relcache so that others see the new statistics object.
	 */
	CacheInvalidateRelcache(rel);

	relation_close(rel, NoLock);

	/*
	 * Add an AUTO dependency on each column used in the stats, so that the
	 * stats object goes away if any or all of them get dropped.
	 */
	ObjectAddressSet(myself, StatisticExtRelationId, statoid);

	for (i = 0; i < numcols; i++)
	{
		ObjectAddressSubSet(parentobject, RelationRelationId, relid, attnums[i]);
		recordDependencyOn(&myself, &parentobject, DEPENDENCY_AUTO);
	}

	/*
	 * Also add dependencies on namespace and owner.  These are required
	 * because the stats object might have a different namespace and/or owner
	 * than the underlying table(s).
	 */
	ObjectAddressSet(parentobject, NamespaceRelationId, namespaceId);
	recordDependencyOn(&myself, &parentobject, DEPENDENCY_NORMAL);

	recordDependencyOnOwner(StatisticExtRelationId, statoid, stxowner);

	/*
	 * XXX probably there should be a recordDependencyOnCurrentExtension call
	 * here too, but we'd have to add support for ALTER EXTENSION ADD/DROP
	 * STATISTICS, which is more work than it seems worth.
	 */

	/* Add any requested comment */
	if (stmt->stxcomment != NULL)
		CreateComments(statoid, StatisticExtRelationId, 0,
					   stmt->stxcomment);

	/* Return stats object's address */
	return myself;
}

/*
 * Guts of statistics object deletion.
 */
void
RemoveStatisticsById(Oid statsOid)
{
	Relation	relation;
	HeapTuple	tup;
	Form_pg_statistic_ext statext;
	Oid			relid;

	/*
	 * Delete the pg_statistic_ext tuple.  Also send out a cache inval on the
	 * associated table, so that dependent plans will be rebuilt.
	 */
	relation = heap_open(StatisticExtRelationId, RowExclusiveLock);

	tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for statistics object %u", statsOid);

	statext = (Form_pg_statistic_ext) GETSTRUCT(tup);
	relid = statext->stxrelid;

	CacheInvalidateRelcacheByRelid(relid);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

/*
 * Update a statistics object for ALTER COLUMN TYPE on a source column.
 *
 * This could throw an error if the type change can't be supported.
 * If it can be supported, but the stats must be recomputed, a likely choice
 * would be to set the relevant column(s) of the pg_statistic_ext tuple to
 * null until the next ANALYZE.  (Note that the type change hasn't actually
 * happened yet, so one option that's *not* on the table is to recompute
 * immediately.)
 */
void
UpdateStatisticsForTypeChange(Oid statsOid, Oid relationOid, int attnum,
							  Oid oldColumnType, Oid newColumnType)
{
	/*
	 * Currently, we don't actually need to do anything here.  For both
	 * ndistinct and functional-dependencies stats, the on-disk representation
	 * is independent of the source column data types, and it is plausible to
	 * assume that the old statistic values will still be good for the new
	 * column contents.  (Obviously, if the ALTER COLUMN TYPE has a USING
	 * expression that substantially alters the semantic meaning of the column
	 * values, this assumption could fail.  But that seems like a corner case
	 * that doesn't justify zapping the stats in common cases.)
	 *
	 * Future types of extended stats will likely require us to work harder.
	 */
}

/*
 * Select a nonconflicting name for a new statistics.
 *
 * name1, name2, and label are used the same way as for makeObjectName(),
 * except that the label can't be NULL; digits will be appended to the label
 * if needed to create a name that is unique within the specified namespace.
 *
 * Returns a palloc'd string.
 *
 * Note: it is theoretically possible to get a collision anyway, if someone
 * else chooses the same name concurrently.  This is fairly unlikely to be
 * a problem in practice, especially if one is holding a share update
 * exclusive lock on the relation identified by name1.  However, if choosing
 * multiple names within a single command, you'd better create the new object
 * and do CommandCounterIncrement before choosing the next one!
 */
static char *
ChooseExtendedStatisticName(const char *name1, const char *name2,
							const char *label, Oid namespaceid)
{
	int			pass = 0;
	char	   *stxname = NULL;
	char		modlabel[NAMEDATALEN];

	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));

	for (;;)
	{
		Oid			existingstats;

		stxname = makeObjectName(name1, name2, modlabel);

		existingstats = GetSysCacheOid2(STATEXTNAMENSP,
										PointerGetDatum(stxname),
										ObjectIdGetDatum(namespaceid));
		if (!OidIsValid(existingstats))
			break;

		/* found a conflict, so try a new name component */
		pfree(stxname);
		snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
	}

	return stxname;
}

/*
 * Generate "name2" for a new statistics given the list of column names for it
 * This will be passed to ChooseExtendedStatisticName along with the parent
 * table name and a suitable label.
 *
 * We know that less than NAMEDATALEN characters will actually be used,
 * so we can truncate the result once we've generated that many.
 *
 * XXX see also ChooseIndexNameAddition.
 */
static char *
ChooseExtendedStatisticNameAddition(List *exprs)
{
	char		buf[NAMEDATALEN * 2];
	int			buflen = 0;
	ListCell   *lc;

	buf[0] = '\0';
	foreach(lc, exprs)
	{
		ColumnRef  *cref = (ColumnRef *) lfirst(lc);
		const char *name;

		/* It should be one of these, but just skip if it happens not to be */
		if (!IsA(cref, ColumnRef))
			continue;

		name = strVal((Value *) linitial(cref->fields));

		if (buflen > 0)
			buf[buflen++] = '_';	/* insert _ between names */

		/*
		 * At this point we have buflen <= NAMEDATALEN.  name should be less
		 * than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
			break;
	}
	return pstrdup(buf);
}
