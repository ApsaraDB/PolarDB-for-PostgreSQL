/*-------------------------------------------------------------------------
 *
 * extension.h
 *		Extension management commands (create/drop extension).
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/extension.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXTENSION_H
#define EXTENSION_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"


/*
 * creating_extension is only true while running a CREATE EXTENSION or ALTER
 * EXTENSION UPDATE command.  It instructs recordDependencyOnCurrentExtension()
 * to register a dependency on the current pg_extension object for each SQL
 * object created by an extension script.  It also instructs performDeletion()
 * to remove such dependencies without following them, so that extension
 * scripts can drop member objects without having to explicitly dissociate
 * them from the extension first.
 */
extern PGDLLIMPORT bool creating_extension;
extern PGDLLIMPORT Oid CurrentExtensionObject;

/* POLAR */
extern bool polar_enable_promoting_privilege;

extern ObjectAddress CreateExtension(ParseState *pstate, CreateExtensionStmt *stmt);

extern void RemoveExtensionById(Oid extId);

extern ObjectAddress InsertExtensionTuple(const char *extName, Oid extOwner,
					 Oid schemaOid, bool relocatable, const char *extVersion,
					 Datum extConfig, Datum extCondition,
					 List *requiredExtensions);

extern ObjectAddress ExecAlterExtensionStmt(ParseState *pstate, AlterExtensionStmt *stmt);

extern ObjectAddress ExecAlterExtensionContentsStmt(AlterExtensionContentsStmt *stmt,
							   ObjectAddress *objAddress);

extern Oid	get_extension_oid(const char *extname, bool missing_ok);
extern char *get_extension_name(Oid ext_oid);

extern ObjectAddress AlterExtensionNamespace(const char *extensionName, const char *newschema,
						Oid *oldschema);

extern void AlterExtensionOwner_oid(Oid extensionOid, Oid newOwnerId);

/* POLAR */
extern bool polar_find_in_string_list(const char *itemname, const char *stringlist);

#endif   /* EXTENSION_H */
