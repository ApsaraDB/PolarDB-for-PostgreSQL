/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the "publication" system catalog (pg_publication)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
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
#include "catalog/pg_publication_d.h"

#include "catalog/objectaddress.h"

/* ----------------
 *		pg_publication definition.  cpp turns this into
 *		typedef struct FormData_pg_publication
 * ----------------
 */
CATALOG(pg_publication,6104,PublicationRelationId)
{
	NameData	pubname;		/* name of the publication */

	Oid			pubowner;		/* publication owner */

	/*
	 * indicates that this is special publication which should encompass all
	 * tables in the database (except for the unlogged and temp ones)
	 */
	bool		puballtables;

	/* true if inserts are published */
	bool		pubinsert;

	/* true if updates are published */
	bool		pubupdate;

	/* true if deletes are published */
	bool		pubdelete;

	/* true if truncates are published */
	bool		pubtruncate;

} FormData_pg_publication;

/* ----------------
 *		Form_pg_publication corresponds to a pointer to a tuple with
 *		the format of pg_publication relation.
 * ----------------
 */
typedef FormData_pg_publication *Form_pg_publication;

typedef struct PublicationActions
{
	bool		pubinsert;
	bool		pubupdate;
	bool		pubdelete;
	bool		pubtruncate;
} PublicationActions;

typedef struct Publication
{
	Oid			oid;
	char	   *name;
	bool		alltables;
	PublicationActions pubactions;
} Publication;

extern Publication *GetPublication(Oid pubid);
extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);

/*---------
 * Expected values for pub_partopt parameter of GetRelationPublications(),
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
extern List *GetAllTablesPublicationRelations(bool pubviaroot);

extern bool is_publishable_relation(Relation rel);
extern ObjectAddress publication_add_relation(Oid pubid, Relation targetrel,
						 bool if_not_exists);

extern Oid	get_publication_oid(const char *pubname, bool missing_ok);
extern char *get_publication_name(Oid pubid);

extern Datum pg_get_publication_tables(PG_FUNCTION_ARGS);

#endif							/* PG_PUBLICATION_H */
