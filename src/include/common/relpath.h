/*-------------------------------------------------------------------------
 *
 * relpath.h
 *		Declarations for GetRelationPath() and friends
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/relpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELPATH_H
#define RELPATH_H

/*
 *	'pgrminclude ignore' needed here because CppAsString2() does not throw
 *	an error if the symbol is not defined.
 */
#include "catalog/catversion.h" /* pgrminclude ignore */


/*
 * Name of major-version-specific tablespace subdirectories
 */
#define TABLESPACE_VERSION_DIRECTORY	"PG_" PG_MAJORVERSION "_" \
									CppAsString2(CATALOG_VERSION_NO)

/* Characters to allow for an OID in a relation path */
#define OIDCHARS		10		/* max chars printed by %u */

/*
 * Stuff for fork names.
 *
 * The physical storage of a relation consists of one or more forks.
 * The main fork is always created, but in addition to that there can be
 * additional forks for storing various metadata. ForkNumber is used when
 * we need to refer to a specific fork in a relation.
 */
typedef enum ForkNumber
{
	InvalidForkNumber = -1,
	MAIN_FORKNUM = 0,
	FSM_FORKNUM,
	VISIBILITYMAP_FORKNUM,
	INIT_FORKNUM

	/*
	 * NOTE: if you add a new fork, change MAX_FORKNUM and possibly
	 * FORKNAMECHARS below, and update the forkNames array in
	 * src/common/relpath.c
	 */
} ForkNumber;

#define MAX_FORKNUM		INIT_FORKNUM

#define FORKNAMECHARS	4		/* max chars for a fork name */

extern const char *const forkNames[];

extern ForkNumber forkname_to_number(const char *forkName);
extern int	forkname_chars(const char *str, ForkNumber *fork);

/*
 * Stuff for computing filesystem pathnames for relations.
 */
extern char *GetDatabasePath(Oid dbNode, Oid spcNode, bool polar_vfs);

extern char *GetRelationPath(Oid dbNode, Oid spcNode, Oid relNode,
				int backendId, ForkNumber forkNumber);

/*
 * Wrapper macros for GetRelationPath.  Beware of multiple
 * evaluation of the RelFileNode or RelFileNodeBackend argument!
 */

/* First argument is a RelFileNode */
#define relpathbackend(rnode, backend, forknum) \
	GetRelationPath((rnode).dbNode, (rnode).spcNode, (rnode).relNode, \
					backend, forknum)

/* First argument is a RelFileNode */
#define relpathperm(rnode, forknum) \
	relpathbackend(rnode, InvalidBackendId, forknum)

/* First argument is a RelFileNodeBackend */
#define relpath(rnode, forknum) \
	relpathbackend((rnode).node, (rnode).backend, forknum)

/* POLAR */
#define POLAR_TEMP_TABLE_FILE_IN_SHARED_STORAGE(backendId) \
	( \
		polar_enable_shared_storage_mode && \
	 	backendId != InvalidBackendId && \
		polar_temp_relation_file_in_shared_storage \
	)

#define POLAR_TEMP_TABLE_FILE_IN_LOCAL_STORAGE(backendId) \
	( \
	 	backendId != InvalidBackendId && \
		!polar_temp_relation_file_in_shared_storage \
	)

#define POLAR_NORMAL_TABLE_FILE_IN_SHARED_STORAGE(backendId) \
	( \
		polar_enable_shared_storage_mode && \
	 	backendId == InvalidBackendId \
	)

extern char *polar_get_database_path(Oid dbNode, Oid spcNode);

#endif							/* RELPATH_H */
