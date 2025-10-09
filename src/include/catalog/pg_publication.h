/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the "publication" system catalog (pg_publication)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_H
#define PG_PUBLICATION_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_publication_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_publication definition.  cpp turns this into
 *		typedef struct FormData_pg_publication
 * ----------------
 */
CATALOG(pg_publication,6104,PublicationRelationId)
{
	Oid			oid;			/* oid */

	NameData	pubname;		/* name of the publication */

	Oid			pubowner BKI_LOOKUP(pg_authid); /* publication owner */

	/*
	 * indicates that this is special publication which should encompass all
	 * tables in the database (except for the unlogged and temp ones)
	 */
	bool		puballtables;

	/*
	 * indicates that this is special publication which should encompass all
	 * sequences in the database (except for the unlogged and temp ones)
	 */
	bool		puballsequences;

	/* true if inserts are published */
	bool		pubinsert;

	/* true if updates are published */
	bool		pubupdate;

	/* true if deletes are published */
	bool		pubdelete;

	/* true if truncates are published */
	bool		pubtruncate;

	/* true if partition changes are published using root schema */
	bool		pubviaroot;

	/*
	 * 'n'(none) if generated column data should not be published. 's'(stored)
	 * if stored generated column data should be published.
	 */
	char		pubgencols;
} FormData_pg_publication;

/* ----------------
 *		Form_pg_publication corresponds to a pointer to a tuple with
 *		the format of pg_publication relation.
 * ----------------
 */
typedef FormData_pg_publication *Form_pg_publication;

DECLARE_UNIQUE_INDEX_PKEY(pg_publication_oid_index, 6110, PublicationObjectIndexId, pg_publication, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_publication_pubname_index, 6111, PublicationNameIndexId, pg_publication, btree(pubname name_ops));

MAKE_SYSCACHE(PUBLICATIONOID, pg_publication_oid_index, 8);
MAKE_SYSCACHE(PUBLICATIONNAME, pg_publication_pubname_index, 8);

typedef struct PublicationActions
{
	bool		pubinsert;
	bool		pubupdate;
	bool		pubdelete;
	bool		pubtruncate;
} PublicationActions;

typedef struct PublicationDesc
{
	PublicationActions pubactions;

	/*
	 * true if the columns referenced in row filters which are used for UPDATE
	 * or DELETE are part of the replica identity or the publication actions
	 * do not include UPDATE or DELETE.
	 */
	bool		rf_valid_for_update;
	bool		rf_valid_for_delete;

	/*
	 * true if the columns are part of the replica identity or the publication
	 * actions do not include UPDATE or DELETE.
	 */
	bool		cols_valid_for_update;
	bool		cols_valid_for_delete;

	/*
	 * true if all generated columns that are part of replica identity are
	 * published or the publication actions do not include UPDATE or DELETE.
	 */
	bool		gencols_valid_for_update;
	bool		gencols_valid_for_delete;
} PublicationDesc;

#ifdef EXPOSE_TO_CLIENT_CODE

typedef enum PublishGencolsType
{
	/* Generated columns present should not be replicated. */
	PUBLISH_GENCOLS_NONE = 'n',

	/* Generated columns present should be replicated. */
	PUBLISH_GENCOLS_STORED = 's',

} PublishGencolsType;

#endif							/* EXPOSE_TO_CLIENT_CODE */

typedef struct Publication
{
	Oid			oid;
	char	   *name;
	bool		alltables;
	bool		allsequences;
	bool		pubviaroot;
	PublishGencolsType pubgencols_type;
	PublicationActions pubactions;
} Publication;

typedef struct PublicationRelInfo
{
	Relation	relation;
	Node	   *whereClause;
	List	   *columns;
} PublicationRelInfo;

extern Publication *GetPublication(Oid pubid);
extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);

/*---------
 * Expected values for pub_partopt parameter of GetPublicationRelations(),
 * which allows callers to specify which partitions of partitioned tables
 * mentioned in the publication they expect to see.
 *
 *	ROOT:	only the table explicitly mentioned in the publication
 *	LEAF:	only leaf partitions in given tree
 *	ALL:	all partitions in given tree
 */
typedef enum PublicationPartOpt
{
	PUBLICATION_PART_ROOT,
	PUBLICATION_PART_LEAF,
	PUBLICATION_PART_ALL,
} PublicationPartOpt;

extern List *GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt);
extern List *GetAllTablesPublications(void);
extern List *GetAllPublicationRelations(char relkind, bool pubviaroot);
extern List *GetPublicationSchemas(Oid pubid);
extern List *GetSchemaPublications(Oid schemaid);
extern List *GetSchemaPublicationRelations(Oid schemaid,
										   PublicationPartOpt pub_partopt);
extern List *GetAllSchemaPublicationRelations(Oid pubid,
											  PublicationPartOpt pub_partopt);
extern List *GetPubPartitionOptionRelations(List *result,
											PublicationPartOpt pub_partopt,
											Oid relid);
extern Oid	GetTopMostAncestorInPublication(Oid puboid, List *ancestors,
											int *ancestor_level);

extern bool is_publishable_relation(Relation rel);
extern bool is_schema_publication(Oid pubid);
extern bool check_and_fetch_column_list(Publication *pub, Oid relid,
										MemoryContext mcxt, Bitmapset **cols);
extern ObjectAddress publication_add_relation(Oid pubid, PublicationRelInfo *pri,
											  bool if_not_exists);
extern Bitmapset *pub_collist_validate(Relation targetrel, List *columns);
extern ObjectAddress publication_add_schema(Oid pubid, Oid schemaid,
											bool if_not_exists);

extern Bitmapset *pub_collist_to_bitmapset(Bitmapset *columns, Datum pubcols,
										   MemoryContext mcxt);
extern Bitmapset *pub_form_cols_map(Relation relation,
									PublishGencolsType include_gencols_type);

#endif							/* PG_PUBLICATION_H */
