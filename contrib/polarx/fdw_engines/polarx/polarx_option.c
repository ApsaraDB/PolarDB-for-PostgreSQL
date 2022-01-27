/*-------------------------------------------------------------------------
 *
 * polarx_option.c
 *		  FDW option handling for polarx_fdw
 *
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *		  contrib/polarx/fdw_engines/polarx/polarx_option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "nodes/makefuncs.h"
#include "libpq-fe.h"
#include "polarx/polarx_option.h"


/*
 * Describes the valid options for objects that this wrapper uses.
 */
typedef struct PgFdwOption
{
	const char *keyword;
	Oid			optcontext;		/* OID of catalog in which option may appear */
	bool		is_libpq_opt;	/* true if it's used in libpq */
} PgFdwOption;

/*
 * Valid options for polarx_fdw.
 * Allocated and filled in InitPgFdwOptions.
 */
static PgFdwOption *polarx_fdw_options;

/*
 * Valid options for libpq.
 * Allocated and filled in InitPgFdwOptions.
 */
static PQconninfoOption *libpq_options;

/*
 * Helper functions
 */
static void InitPgFdwOptions(void);
static bool is_valid_option(const char *keyword, Oid context);
static bool is_libpq_option(const char *keyword);
static char *get_option_from_liststr(char *list_str, int str_index);
static List *set_libpq_option(List *defelems, char *name, char *val);


/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses polarx_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
PG_FUNCTION_INFO_V1(polarx_fdw_validator);

Datum
polarx_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/* Build our options lists if we didn't yet. */
	InitPgFdwOptions();

	/*
	 * Check that only options supported by polarx_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			PgFdwOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = polarx_fdw_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->keyword);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.data)));
		}

		/*
		 * Validate option value, when we can do so without any context.
		 */
		if (strcmp(def->defname, "use_remote_estimate") == 0 ||
			strcmp(def->defname, "updatable") == 0 ||
			strcmp(def->defname, "nodeis_primary") == 0 ||
			strcmp(def->defname, "nodeis_preferred") == 0 ||
			strcmp(def->defname, "nodeis_local") == 0)
		{
			/* these accept only boolean values */
			(void) defGetBoolean(def);
		}
		else if (strcmp(def->defname, "fdw_startup_cost") == 0 ||
				 strcmp(def->defname, "fdw_tuple_cost") == 0)
		{
			/* these must have a non-negative numeric value */
			double		val;
			char	   *endp;

			val = strtod(defGetString(def), &endp);
			if (*endp || val < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative numeric value",
								def->defname)));
		}
		else if (strcmp(def->defname, "extensions") == 0)
		{
			/* check list syntax, warn about uninstalled extensions */
			(void) ExtractExtensionList(defGetString(def), true);
		}
		else if (strcmp(def->defname, "fetch_size") == 0)
		{
			int			fetch_size;

			fetch_size = strtol(defGetString(def), NULL, 10);
			if (fetch_size <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative integer value",
								def->defname)));
		}
        else if (strcmp(def->defname, "node_id") == 0)
		{
			int			node_id;

			node_id = strtol(defGetString(def), NULL, 10);
			if (node_id < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative integer value",
								def->defname)));
		}
	}

	PG_RETURN_VOID();
}

/*
 * Initialize option lists.
 */
static void
InitPgFdwOptions(void)
{
	int			num_libpq_opts;
	PQconninfoOption *lopt;
	PgFdwOption *popt;

	/* non-libpq FDW-specific FDW options */
	static const PgFdwOption non_libpq_options[] = {
		{"schema_name", ForeignTableRelationId, false},
		{"table_name", ForeignTableRelationId, false},
		{"column_name", AttributeRelationId, false},
		/* use_remote_estimate is available on both server and table */
		{"use_remote_estimate", ForeignServerRelationId, false},
		{"use_remote_estimate", ForeignTableRelationId, false},
		/* cost factors */
		{"fdw_startup_cost", ForeignServerRelationId, false},
		{"fdw_tuple_cost", ForeignServerRelationId, false},
		/* shippable extensions */
		{"extensions", ForeignServerRelationId, false},
		/* updatable is available on both server and table */
		{"updatable", ForeignServerRelationId, false},
		{"updatable", ForeignTableRelationId, false},
		/* fetch_size is available on both server and table */
		{"fetch_size", ForeignServerRelationId, false},
		{"fetch_size", ForeignTableRelationId, false},
		/* polardb-X distributed cluster server */
		{"ip_list", ForeignServerRelationId, false},
		{"port_list", ForeignServerRelationId, false},
		{"nodeis_primary", ForeignServerRelationId, false},
		{"nodeis_preferred", ForeignServerRelationId, false},
		{"node_id", ForeignServerRelationId, false},
		{"node_cluster_name", ForeignServerRelationId, false},
		{"nodeis_local", ForeignServerRelationId, false},
		/* polardb-X distributed cluster table */
		{"locator_type", ForeignTableRelationId, false},
		{"part_attr_num", ForeignTableRelationId, false},
		{"dist_col_name", ForeignTableRelationId, false},
		{NULL, InvalidOid, false}
	};

	/* Prevent redundant initialization. */
	if (polarx_fdw_options)
		return;

	/*
	 * Get list of valid libpq options.
	 *
	 * To avoid unnecessary work, we get the list once and use it throughout
	 * the lifetime of this backend process.  We don't need to care about
	 * memory context issues, because PQconndefaults allocates with malloc.
	 */
	libpq_options = PQconndefaults();
	if (!libpq_options)			/* assume reason for failure is OOM */
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Could not get libpq's default connection options.")));

	/* Count how many libpq options are available. */
	num_libpq_opts = 0;
	for (lopt = libpq_options; lopt->keyword; lopt++)
		num_libpq_opts++;

	/*
	 * Construct an array which consists of all valid options for
	 * polarx_fdw, by appending FDW-specific options to libpq options.
	 *
	 * We use plain malloc here to allocate polarx_fdw_options because it
	 * lives as long as the backend process does.  Besides, keeping
	 * libpq_options in memory allows us to avoid copying every keyword
	 * string.
	 */
	polarx_fdw_options = (PgFdwOption *)
		malloc(sizeof(PgFdwOption) * num_libpq_opts +
			   sizeof(non_libpq_options));
	if (polarx_fdw_options == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	popt = polarx_fdw_options;
	for (lopt = libpq_options; lopt->keyword; lopt++)
	{
		/* Hide debug options, as well as settings we override internally. */
		if (strchr(lopt->dispchar, 'D') ||
			strcmp(lopt->keyword, "fallback_application_name") == 0 ||
			strcmp(lopt->keyword, "client_encoding") == 0)
			continue;

		/* We don't have to copy keyword string, as described above. */
		popt->keyword = lopt->keyword;

		/*
		 * "user" and any secret options are allowed only on user mappings.
		 * Everything else is a server option.
		 */
		if (strcmp(lopt->keyword, "user") == 0 || strchr(lopt->dispchar, '*'))
			popt->optcontext = UserMappingRelationId;
		else
			popt->optcontext = ForeignServerRelationId;
		popt->is_libpq_opt = true;

		popt++;
	}

	/* Append FDW-specific options and dummy terminator. */
	memcpy(popt, non_libpq_options, sizeof(non_libpq_options));
}

/*
 * Check whether the given option is one of the valid polarx_fdw options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *keyword, Oid context)
{
	PgFdwOption *opt;

	Assert(polarx_fdw_options);	/* must be initialized already */

	for (opt = polarx_fdw_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}

/*
 * Check whether the given option is one of the valid libpq options.
 */
static bool
is_libpq_option(const char *keyword)
{
	PgFdwOption *opt;

	Assert(polarx_fdw_options);	/* must be initialized already */

	for (opt = polarx_fdw_options; opt->keyword; opt++)
	{
		if (opt->is_libpq_opt && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}

/*
 * Generate key-value arrays which include only libpq options from the
 * given list (which can contain any kind of options).  Caller must have
 * allocated large-enough arrays.  Returns number of options found.
 */
int
ExtractConnectionOptions(List *defelems, const char **keywords,
						 const char **values)
{
	ListCell   *lc;
	int			i;

	/* Build our options lists if we didn't yet. */
	InitPgFdwOptions();

	i = 0;
	foreach(lc, defelems)
	{
		DefElem    *d = (DefElem *) lfirst(lc);

		if (is_libpq_option(d->defname))
		{
			keywords[i] = d->defname;
			values[i] = defGetString(d);
			i++;
		}
	}
	return i;
}

/*
 * Parse a comma-separated string and return a List of the OIDs of the
 * extensions named in the string.  If any names in the list cannot be
 * found, report a warning if warnOnMissing is true, else just silently
 * ignore them.
 */
List *
ExtractExtensionList(const char *extensionsString, bool warnOnMissing)
{
	List	   *extensionOids = NIL;
	List	   *extlist;
	ListCell   *lc;

	/* SplitIdentifierString scribbles on its input, so pstrdup first */
	if (!SplitIdentifierString(pstrdup(extensionsString), ',', &extlist))
	{
		/* syntax error in name list */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s\" must be a list of extension names",
						"extensions")));
	}

	foreach(lc, extlist)
	{
		const char *extension_name = (const char *) lfirst(lc);
		Oid			extension_oid = get_extension_oid(extension_name, true);

		if (OidIsValid(extension_oid))
		{
			extensionOids = lappend_oid(extensionOids, extension_oid);
		}
		else if (warnOnMissing)
		{
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("extension \"%s\" is not installed",
							extension_name)));
		}
	}

	list_free(extlist);
	return extensionOids;
}

static char *
get_option_from_liststr(char *list_str, int str_index)
{
	char	*token = strtok(list_str, " ");
	int	num = 0;

	while(token)
	{
		if(num == str_index)
			break;
		num++;
		token = strtok(NULL, " ");
	}
	
	return token;
}

static List *
set_libpq_option(List *defelems, char *name, char *val)
{
	ListCell	*lc;
	int		i;

	i = 0;
	foreach(lc, defelems)
	{
		DefElem    *d = (DefElem *) lfirst(lc);

		if (strcmp(d->defname, name) == 0 && is_libpq_option(d->defname))
		{
			Node *valNode = (Node *) makeString(pstrdup(val));
			d->arg = valNode;
			break;
		}
		i++;
	}
	if(i == list_length(defelems))
	{
		defelems = lappend(defelems, makeDefElem(pstrdup(name), (Node *) makeString(pstrdup(val)), -1));	
	}
	
	return defelems;
}

char *
GetClusterTableOption(List *options, TableOption type)
{
	ListCell	*lc;
	char		*option_name = NULL;
	char		*option_val = NULL;

	switch (type)
	{
		case TABLE_OPTION_LOCATOR_TYPE:
			option_name = "locator_type";
			break;
		case TABLE_OPTION_PART_ATTR_NUM:
			option_name = "part_attr_num";
			break;
		case TABLE_OPTION_PART_ATTR_NAME:
			option_name = "dist_col_name";
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("table option type %d is invalid for polarx",
						 type)));
	}

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, option_name) == 0)
			option_val = defGetString(def);
	}
	if(option_val == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s\" is not set up for polarx",
					 option_name)));
	return option_val;
}

char *
GetClusterServerOption(List *options, int nodeIndex, ServerOption type)
{
	ListCell	*lc;
	char		*list_option_name = NULL;
	char		*list_option_val = NULL;

	switch (type)
	{
		case SERVER_OPTION_HOST:
			list_option_name = "ip_list";
			break;
		case SERVER_OPTION_PORT:
			list_option_name = "port_list";
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("node index %d: option type %d is invalid for polarx",
						 nodeIndex, type)));
	}

	foreach(lc, options)
        {
                DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, list_option_name) == 0)
		{
			list_option_val = pstrdup(defGetString(def));
		}
	}
	if(list_option_val == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node index %d: parameter \"%s\" is not set up for polarx",
					 nodeIndex, list_option_name)));
	return get_option_from_liststr(list_option_val, nodeIndex);
}

List *
GetSingleNodeLibpgOption(List *options, int nodeIndex)
{
	options = set_libpq_option(options, "host", 
			GetClusterServerOption(options, nodeIndex, SERVER_OPTION_HOST));
	options = set_libpq_option(options, "port",
                        GetClusterServerOption(options, nodeIndex, SERVER_OPTION_PORT));
	
	return options;
}

int
GetNodesNum(List *options)
{
	int     num = 0;
	ListCell   *lc;
	char    *ip_list_str = NULL;

	if(options == NIL)
		return num;	

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "ip_list") == 0)
		{
			ip_list_str = pstrdup(defGetString(def));
			break;
		}
	}
	if(ip_list_str)
	{
		char    *token = strtok(ip_list_str, " ");

		while (token)
		{
			num++;
			token = strtok(NULL, " ");
		}
	}
	return num;	
}
