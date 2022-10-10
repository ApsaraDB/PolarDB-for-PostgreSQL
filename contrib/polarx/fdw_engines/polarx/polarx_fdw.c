/*-------------------------------------------------------------------------
 *
 * polarx_fdw.c
 *		  Foreign-data wrapper for remote polarx cluster servers
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/polarx/fdw_engines/polarx/polarx_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"

#include "polarx/polarx_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/hash.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "executor/nodeHash.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "executor/recvRemote.h"
#include "executor/execRemoteQuery.h"
#include "plan/polarx_planner.h"
#include "distribute_transaction/txn.h"
#include "lib/binaryheap.h"
#include "parser/parse_func.h"
#include "access/tuptoaster.h"
#include "commands/polarx_utility.h"
#include "catalog/objectaccess.h"
#include "catalog/dependency.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/indexing.h"
#include "utils/syscache.h"
#include "polarx/polarx_locator.h"
#include "polarx/deparse_fdw.h"
#include "polarx/polarx_shippable.h"
#include "polarx/polarx_connection.h"
#include "polarx/polarx_option.h"
#include "executor/execPartition.h"
#include "pool/poolnodes.h"
#include "pgxc/mdcache.h"
#include "utils/fmgroids.h"
#include "catalog/index.h"
#include "storage/lmgr.h"
#include "common/md5.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

#if PG_VERSION_NUM >= 100000
#define index_insert_compat(rel,v,n,t,h,u) \
    index_insert(rel,v,n,t,h,u, BuildIndexInfo(rel))
#else
#define index_insert_compat(rel,v,n,t,h,u) index_insert(rel,v,n,t,h,u)
#endif
enum FdwPathPrivateIndex
{
	FdwPathPrivateNodeList
};

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,
	FdwScanPrivateNodeList,
	FdwScanPrivateAccessType,
	FdwScanPrivateSimpleSort,
	FdwScanPrivateAddNodeInx,
	FdwScanPrivateHaveWholerow,
	FdwScanPrivateHasOid,
    FdwScanPrivateParamsExtern,
    FdwScanPrivateQueryTreeHash,
    FdwScanPrivateDistByParam,
    FdwScanPrivateFQSPlannedStmt,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a polarx_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs,
	FdwModifyPrivateAccessType,
	FdwModifyPrivateParamsExtern,
	FdwModifyPrivateQueryTreeHash
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ForeignScan node that modifies a foreign table directly.  We store:
 *
 * 1) UPDATE/DELETE statement text to be sent to the remote server
 * 2) Boolean flag showing if the remote query has a RETURNING clause
 * 3) Integer list of attribute numbers retrieved by RETURNING, if any
 * 4) Boolean flag showing if we set the command es_processed
 */
enum FdwDirectModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwDirectModifyPrivateUpdateSql,
	/* has-returning flag (as an integer Value node) */
	FdwDirectModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwDirectModifyPrivateRetrievedAttrs,
	/* set-processed flag (as an integer Value node) */
	FdwDirectModifyPrivateSetProcessed,
	FdwDirectModifyPrivateNodeList,
	FdwDirectModifyPrivateLocatorType,
    FdwDirectModifyPrivateTargetAttnums,
    FdwDirectModifyPrivateTargetRelOid,
	FdwDirectModifyPrivateParamsExtern,
	FdwDirectModifyPrivateQueryTreeHash
};

/*
 * Execution state of a foreign scan using polarx_fdw.
 */
typedef struct PgFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc	tupdesc;		/* tuple descriptor of scan */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	/* for remote query execution */
	PGconn	   *conn;			/* connection for the scan */
	PGconn     **conns;
	ResponseCombiner combiner;
	int	   conn_num;
	int		current_node;
	int		*conn_node_map;
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool		cursor_exists;	/* have we created the cursor? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid	*param_types;
	bool	query_done;
	RelationAccessType rel_access_type;
	List	*node_list;
	bool	is_prepared;
	int            ms_nkeys;
	SortSupport ms_sortkeys;
	struct binaryheap *ms_heap;
	char  *query_hash; /* query tree hash value */

	/* for storing result tuples */
	HeapTuple  *tuples;			/* array of currently-retrieved tuples */
	int			*num_tuples;		/* # of tuples in array */
	int			*next_tuple;		/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int			fetch_ct_2;		/* Min(# of fetches done, 2) */
	bool		*eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext *batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	int			fetch_size;		/* number of tuples per fetch */
	int have_wholerow;
	bool wholerow_first;
	TupleDesc       tupdesc_wlr;
	bool has_oid;
} PgFdwScanState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct PgFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
	int		part_attr_num;
	UserMapping	*user;
	char	locator_type;
	bool		*is_prepared;
	int		current_node;
	RelationAccessType rel_access_type;
	int		total_conn_num;
	char  *query_hash; /* query tree hash value */

	/* for remote query execution */
	PGconn	   *conn;			/* connection for the scan */
	PGconn		**conns;
	char	   *p_name;			/* name of prepared statement, if created */

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber	ctidAttno;		/* attnum of input resjunk ctid column */
	AttrNumber	nodeIndexAttno;
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */
	Oid     *param_types;

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* for update row movement if subplan result rel */
	struct PgFdwModifyState *aux_fmstate;	/* foreign-insert state, if
											 * created */
} PgFdwModifyState;

/*
 * Execution state of a foreign scan that modifies a foreign table directly.
 */
typedef struct PgFdwDirectModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
	char  *query_hash; /* query tree hash value */

	/* extracted fdw_private data */
	char	   *query;			/* text of UPDATE/DELETE command */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */
	bool		set_processed;	/* do we set the command es_processed? */

	/* for remote query execution */
	PGconn	   *conn;			/* connection for the update */
	PGconn **conns;
	int conn_num;
	char locator_type;
	List *node_list;
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid	*param_types;
	ResponseCombiner combiner;

	/* for storing result tuples */
	PGresult   *result;			/* result for query */
	PGresult   **results;
	int current_result;
	int current_index;
	int			num_tuples;		/* # of result tuples */
	int			next_tuple;		/* index of next one to return */
	Relation	resultRel;		/* relcache entry for the target relation */
	AttrNumber *attnoMap;		/* array of attnums of input user columns */
	AttrNumber	ctidAttno;		/* attnum of input ctid column */
	AttrNumber	oidAttno;		/* attnum of input oid column */
	bool		hasSystemCols;	/* are there system columns of resultRel? */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} PgFdwDirectModifyState;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct PgFdwAnalyzeState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */
	List	   *retrieved_attrs;	/* attr numbers retrieved by query */

	/* collected sample rows */
	HeapTuple  *rows;			/* array of size targrows */
	int			targrows;		/* target # of sample rows */
	int			numrows;		/* # of sample rows collected */

	/* for random sampling */
	double		samplerows;		/* # of rows fetched */
	double		rowstoskip;		/* # of rows to skip before next sample */
	ReservoirStateData rstate;	/* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} PgFdwAnalyzeState;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
	Relation	rel;			/* foreign table's relcache entry. */
	AttrNumber	cur_attno;		/* attribute number being processed, or 0 */

	/*
	 * In case of foreign join push down, fdw_scan_tlist is used to identify
	 * the Var node corresponding to the error location and
	 * fsstate->ss.ps.state gives access to the RTEs of corresponding relation
	 * to get the relation name and attribute name.
	 */
	ForeignScanState *fsstate;
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
	Expr	   *current;		/* current expr, or NULL if not yet found */
	List	   *already_used;	/* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * Helper functions
 */
static void estimate_path_cost_size(PlannerInfo *root,
						            RelOptInfo *foreignrel,
                                    List *param_join_conds,
                                    List *pathkeys,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cost, Cost *p_total_cost,
                                    Distribution *distribution);
static void get_remote_estimate(const char *sql, List *node_list,
						        double *rows, int *width,
                                Cost *startup_cost, Cost *total_cost);
static bool ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
						            EquivalenceClass *ec, EquivalenceMember *em,
                                    void *arg);
static PgFdwModifyState *create_foreign_modify(EState *estate,
		                        			  RangeTblEntry *rte,
                                              ResultRelInfo *resultRelInfo,
                                              CmdType operation,
                                              Plan *subplan,
                                              char *query,
                                              List *target_attrs,
                                              bool has_returning,
                                              List *retrieved_attrs);
static const char **convert_prep_stmt_params(PgFdwModifyState *fmstate,
						                    ItemPointer tupleid,
                                            TupleTableSlot *slot);
static void finish_foreign_modify(PgFdwModifyState *fmstate);
static List *build_remote_returning(Index rtindex, Relation rel,
					                List *returningList);
static void rebuild_fdw_scan_tlist(ForeignScan *fscan, List *tlist);
static TupleTableSlot *execute_dml_stmt(ForeignScanState *node);
static void init_returning_filter(PgFdwDirectModifyState *dmstate,
					                List *fdw_scan_tlist,
					                Index rtindex);
static TupleTableSlot *apply_returning_filter(PgFdwDirectModifyState *dmstate,
					                            TupleTableSlot *slot,
                                                EState *estate);
static void prepare_query_params(PlanState *node,
					            List *fdw_exprs,
                                int numParams,
                                FmgrInfo **param_flinfo,
                                List **param_exprs,
                                const char ***param_values,
                                Oid **param_types);
static void process_query_params(ExprContext *econtext,
					            FmgrInfo *param_flinfo,
                                List *param_exprs,
                                const char **param_values);
static int polarxAcquireSampleRowsFunc(Relation relation, int elevel,
							             HeapTuple *rows, int targrows,
                                         double *totalrows,
                                         double *totaldeadrows);
static void analyze_row_processor(RemoteDataRow datarow,
					            PgFdwAnalyzeState *astate);
static HeapTuple make_tuple_from_datarow(char *msg,
						                Relation rel,
                                        AttInMetadata *attinmeta,
                                        List *retrieved_attrs,
                                        ForeignScanState *fsstate,
                                        MemoryContext temp_context,
                                        bool is_scan);
static void conversion_error_callback(void *arg);
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
				            JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel,
                            JoinPathExtraData *extra);
static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
							            	 RelOptInfo *rel);
static List *get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel);
static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								            Path *epq_path);
static void apply_server_options(PgFdwRelationInfo *fpinfo);
static void apply_table_options(PgFdwRelationInfo *fpinfo);
static void merge_fdw_options(PgFdwRelationInfo *fpinfo,
				            const PgFdwRelationInfo *fpinfo_o,
                            const PgFdwRelationInfo *fpinfo_i);
static int compute_modulo(uint64 numerator, uint64 denominator);
static void relation_build_locator(RelOptInfo *baserel, Oid foreigntableid);
static void set_path_distribution(LocInfo *locator, Distribution *distribution);
static void restrict_distribution(PlannerInfo *root, RestrictInfo *ri, Distribution *distribution);
static bool joinrelation_build_locator(RelOptInfo *joinrel, JoinType jointype,
                                        RelOptInfo *outerrel, RelOptInfo *innerrel);
static RelationAccessType get_relation_access_type(Query *query);
static void estimate_hash_bucket_stats_cluster(PlannerInfo *root, Node *hashkey, double nbuckets,
						                        Selectivity *mcv_freq,
                                                Selectivity *bucketsize_frac, int node_num);
static double approx_tuple_count_cluster(PlannerInfo *root, PgFdwRelationInfo *join_fpinfo);
static void get_hash_join_cost(PlannerInfo *root, PgFdwRelationInfo *fpinfo,
			                	Cost *p_startup_cost, Cost *p_total_cost,
                                int run_node_num, int retrieved_rows);
static bool polarx_start_command_on_connection(PGXCNodeHandle **connections,
                                                ForeignScanState *node, Snapshot snapshot);
static void polarx_scan_begin(bool read_only, List *node_list, bool need_tran_block);
static void close_node_cursors(char *cursor, List *nodelist);
static void prepare_foreign_modify(PgFdwModifyState *fmstate, List *node_list);
static void polarx_send_query_prepared(PgFdwModifyState *fmstate, List *nodelist,
					                    const char **values,  TupleTableSlot *slot, int *nrows);
static void store_returning_result_with_datarow(PgFdwModifyState *fmstate,
		                                        TupleTableSlot *slot, char *res);
static void prepare_simple_sort_from_pathkeys(PlannerInfo *root, List **scan_tlist, List *pathkeys,
						                        RelOptInfo *foreignrel,
                                                const AttrNumber *reqColIdx,
                                                MergeAppend *simple_sort);
static EquivalenceMember *find_ec_member_for_tle(EquivalenceClass *ec,
							TargetEntry *tle,
							Relids relids);
static int get_col_index(PlannerInfo *root, RelOptInfo *foreignrel, TargetEntry *tle);
static int32 fdw_heap_compare_slots(Datum a, Datum b, void *arg);
static TargetEntry *get_node_index(Plan *plan, bool *is_exist);
static void change_wholerow(Plan *plan);
static bool reloptinfo_contain_targetrel_inx(int targetrel_inx, RelOptInfo *rel);
static bool pathkeys_contain_const(List *pathkeys, RelOptInfo *rel);
static int prepare_foreign_sql(const char *p_name, const char *query, List *node_list,
                                int *prep_conns);
static bool check_hashjoinable(Expr *clause);

static char *get_preapred_name(Snapshot snapshot, Relation sql_preapred_rel,
                                 Relation sql_index_rel, ScanKey key,
                                 char *sql, int *max_version, bool *is_deleted);
static void set_preapred_name(Relation sql_preapred_rel,
                    Relation sql_index_rel,
                    Relation sql_query_idx_rel,
                    Relation sql_reloids_idx_rel,
                    Datum sql_hash, Datum version,
                    Datum reloids,char *sql);
static Datum md5_text_local(text *in_text);
static List *get_all_reloids(PlannerInfo *root, RelOptInfo *rel);
static void search_and_update_reloids(Snapshot snapshot, Relation sql_preapred_rel,
                                    Relation sql_index_rel,
                                    ScanKey key, char *sql, Datum new_reloids);
static void update_sql_prepared_reloids(HeapTuple old_tup,
                            Relation rel, Datum new_reloids);
/*
 * polarxGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
void
polarxGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;
	Distribution *distribution;

	/*
	 * We use PgFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;
	fpinfo->fetch_size = 100;

	apply_server_options(fpinfo);
	apply_table_options(fpinfo);

	relation_build_locator(baserel, foreigntableid);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
		fpinfo->user = NULL;
	}
	else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	distribution = palloc0(sizeof(Distribution));

	set_path_distribution(fpinfo->rd_locator_info, distribution);
	if(fpinfo->remote_conds)
	{
		ListCell *rmlc;

		foreach (rmlc, fpinfo->remote_conds)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(rmlc);
			restrict_distribution(root, ri, distribution);
		}
	}

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		estimate_path_cost_size(root, baserel, NIL, NIL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost,
								distribution);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
		if (baserel->pages == 0 && baserel->tuples == 0)
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/* Estimate baserel size as best we can with local statistics. */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		estimate_path_cost_size(root, baserel, NIL, NIL,
								&fpinfo->rows, &fpinfo->width,
								&fpinfo->startup_cost, &fpinfo->total_cost, distribution);
	}

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(foreigntableid));
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}

/*
 * get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr	   *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile ||
				!(em_expr = find_em_expr_for_rel(pathkey_ec, rel)) ||
				!is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
	}

	/*
	 * Even if we're not using remote estimates, having the remote side do the
	 * sort generally won't be any worse than doing it locally, and it might
	 * be much better if the remote side can generate data in the right order
	 * without needing a sort at all.  However, what we're going to do next is
	 * try to generate pathkeys that seem promising for possible merge joins,
	 * and that's more speculative.  A wrong choice might hurt quite a bit, so
	 * bail out if we can't use remote estimates.
	 */
	if (!fpinfo->use_remote_estimate)
		return useful_pathkeys_list;

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceClass *cur_ec = lfirst(lc);
		Expr	   *em_expr;
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		/* If no pushable expression for this rel, skip it. */
		em_expr = find_em_expr_for_rel(cur_ec, rel);
		if (em_expr == NULL || !is_foreign_expr(root, rel, em_expr))
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);
		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * polarxGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
void
polarxGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;
	List	   *ppi_list;
	ListCell   *lc;
	Distribution *distribution = palloc0(sizeof(Distribution));

	set_path_distribution(fpinfo->rd_locator_info, distribution);

	if(fpinfo->remote_conds)
	{
		ListCell *rmlc;
		foreach (rmlc, fpinfo->remote_conds)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(rmlc);
			restrict_distribution(root, ri, distribution);
		}
	}

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 *
	 * Although this path uses no join clauses, it could still have required
	 * parameterization due to LATERAL refs in its tlist.
	 */

	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   list_make1(distribution));	/* no fdw_private list */
	add_path(baserel, (Path *) path);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * It's convenient to manage this by representing each candidate outer
	 * relation by the ParamPathInfo node for it.  We can then use the
	 * ppi_clauses list in the ParamPathInfo node directly as a list of the
	 * interesting join clauses for that rel.  This takes care of the
	 * possibility that there are multiple safe join clauses for such a rel,
	 * and also ensures that we account for unsafe join clauses that we'll
	 * still have to enforce locally (since the parameterized-path machinery
	 * insists that we handle all movable clauses).
	 */
	ppi_list = NIL;
	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Relids		required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, baserel))
			continue;

		/* See if it is safe to send to remote */
		if (!is_foreign_expr(root, baserel, rinfo->clause))
			continue;

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids,
								   baserel->lateral_relids);
		/* We do not want the foreign rel itself listed in required_outer */
		required_outer = bms_del_member(required_outer, baserel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
			continue;

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, baserel,
											   required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (baserel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the foreign rel.  Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List	   *clauses;

			/* Make clauses, skipping any that join to lateral_referencers */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 baserel,
															 ec_member_matches_foreign,
															 (void *) &arg,
															 baserel->lateral_referencers);

			/* Done if there are no more expressions in the foreign rel */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach(lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
				Relids		required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, baserel))
					continue;

				/* See if it is safe to send to remote */
				if (!is_foreign_expr(root, baserel, rinfo->clause))
					continue;

				/* Calculate required outer rels for the resulting path */
				required_outer = bms_union(rinfo->clause_relids,
										   baserel->lateral_relids);
				required_outer = bms_del_member(required_outer, baserel->relid);
				if (bms_is_empty(required_outer))
					continue;

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, baserel,
													   required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/* Try again, now ignoring the expression we found this time */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	/*
	 * Now build a path for each useful outer relation.
	 */
	foreach(lc, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *) lfirst(lc);
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;
		List		*remote_param_join_conds;
		List		*local_param_join_conds;
		List		*remote_conds;

		classifyConditions(root, baserel, param_info->ppi_clauses,
                                                   &remote_param_join_conds, &local_param_join_conds);
		remote_conds = list_concat(list_copy(remote_param_join_conds),
                                                                   fpinfo->remote_conds);


		distribution = palloc0(sizeof(Distribution));
		set_path_distribution(fpinfo->rd_locator_info, distribution);

		if(remote_conds)
		{
			ListCell *rmlc;
			foreach (rmlc, remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(rmlc);
				restrict_distribution(root, ri, distribution);
			}
		}
		/* Get a cost estimate from the remote */
		estimate_path_cost_size(root, baserel,
								param_info->ppi_clauses, NIL,
								&rows, &width,
								&startup_cost, &total_cost,
								distribution);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = create_foreignscan_path(root, baserel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   param_info->ppi_req_outer,
									   NULL,
									   list_make1(distribution));	/* no fdw_private list */
		add_path(baserel, (Path *) path);
	}
}

/*
 * polarxGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
ForeignScan *
polarxGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	Distribution *dist = NULL;
	MergeAppend *simple_sort = NULL;
	List	*node_list;
	bool	add_node_inx = false;
	bool have_wholerow = false;
	bool has_oid = false;
	bool is_target_rel = false;
    DistributionForParam *dist_for_param = NULL;
    List *reloids_list = NIL;

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = foreignrel->relid;
		if(root->parse->resultRelation == scan_relid)
			is_target_rel = true;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by polarxGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;
		is_target_rel = reloptinfo_contain_targetrel_inx(root->parse->resultRelation,
				foreignrel);

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see polarxGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist,
												best_path->path.parallel_safe);
		}
	}

	dist = (Distribution *)list_nth(best_path->fdw_private,FdwPathPrivateNodeList);

	if(root->parse->commandType != CMD_SELECT && !is_target_rel)
		node_list = GetRelationNodesWithDistribution(dist, 0, true, RELATION_ACCESS_READ);
	else
		node_list = GetRelationNodesWithDistribution(dist, 0, true, get_relation_access_type(root->parse));

	if(root->parse->commandType == CMD_UPDATE
				|| root->parse->commandType == CMD_DELETE)
	{
		List *tmp_tlist = NIL;

		if(root->parse->resultRelation == scan_relid)
		{
			RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
			Relation	rel = heap_open(rte->relid, NoLock);
			TupleDesc	tupdesc = RelationGetDescr(rel);
			int i;
			bool oid_is_in = false;
			int j = 1;

			Assert(fdw_scan_tlist == NULL);

			has_oid = rel->rd_att->tdhasoid;
			have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
					fpinfo->attrs_used);

			for (i = 1; i <= tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

				/* Ignore dropped attributes. */
				if (attr->attisdropped)
					continue;

				if (have_wholerow ||
						bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
							fpinfo->attrs_used))
				{
					Node	   *new_expr;
					TargetEntry *new_tle;

					new_expr = (Node *) makeVar(foreignrel->relid,
							i,
							attr->atttypid,
							attr->atttypmod,
							attr->attcollation,
							0);

					new_tle = makeTargetEntry((Expr *) new_expr,
							j++,
							pstrdup(NameStr(attr->attname)),
							false);
					if(tmp_tlist == NIL)
						tmp_tlist = list_make1(new_tle);
					else
						tmp_tlist = lappend(tmp_tlist, new_tle);
				}
			}
			if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber,
						fpinfo->attrs_used))
			{
				Node       *new_expr;
				TargetEntry *new_tle;

				new_expr = (Node *) makeVar(foreignrel->relid,
						SelfItemPointerAttributeNumber,
						TIDOID,
						-1,
						InvalidOid,
						0);

				new_tle = makeTargetEntry((Expr *) new_expr,
						j++,
						pstrdup("ctid"),
						true);
				if(tmp_tlist == NIL)
					tmp_tlist = list_make1(new_tle);
				else
					tmp_tlist = lappend(tmp_tlist, new_tle);
			}
			if (bms_is_member(ObjectIdAttributeNumber - FirstLowInvalidHeapAttributeNumber,
						fpinfo->attrs_used))
			{
				Node       *new_expr;
				TargetEntry *new_tle;

				new_expr = (Node *) makeVar(foreignrel->relid,
						ObjectIdAttributeNumber,
						OIDOID,
						-1,
						InvalidOid,
						0);

				new_tle = makeTargetEntry((Expr *) new_expr,
						j++,
						pstrdup("oid"),
						true);
				if(tmp_tlist == NIL)
					tmp_tlist = list_make1(new_tle);
				else
					tmp_tlist = lappend(tmp_tlist, new_tle);
			}
			if((!have_wholerow || oid_is_in == false) && fdw_recheck_quals)
			{
				ListCell * lc;
				foreach(lc, fdw_recheck_quals)
				{
					Expr *clause = (Expr *) lfirst(lc);
					List *recheck_list = NIL;
					ListCell *tmp;
					recheck_list = add_to_flat_tlist(recheck_list,
							pull_var_clause((Node *) clause,
								PVC_RECURSE_PLACEHOLDERS));
					foreach(tmp, recheck_list)
					{
						TargetEntry *tmp_tl = lfirst(tmp);
						if(!bms_is_member(((Var *)tmp_tl->expr)->varattno
									- FirstLowInvalidHeapAttributeNumber,
									fpinfo->attrs_used))
						{
							Node       *new_expr;
							TargetEntry *new_tle;
							bool is_junk = false;
							char *att_name = NULL;

							if(((Var *)tmp_tl->expr)->varattno
									== ObjectIdAttributeNumber
									&& oid_is_in == false)
							{
								new_expr = (Node *) makeVar(foreignrel->relid,
										ObjectIdAttributeNumber,
										OIDOID,
										-1,
										InvalidOid,
										0);
								is_junk = true;
								att_name = "oid";
							}
							else if(!have_wholerow)
							{
								Form_pg_attribute attr = TupleDescAttr(tupdesc,
										((Var *)tmp_tl->expr)->varattno - 1);

								new_expr = (Node *) makeVar(foreignrel->relid,
										((Var *)tmp_tl->expr)->varattno,
										attr->atttypid,
										attr->atttypmod,
										attr->attcollation,
										0);
								att_name = NameStr(attr->attname);
							}
							else
							{
								continue;
							}
							new_tle = makeTargetEntry((Expr *) new_expr,
									j++,
									pstrdup(att_name),
									is_junk);
							if(tmp_tlist == NIL)
								tmp_tlist = list_make1(new_tle);
							else
								tmp_tlist = lappend(tmp_tlist, new_tle);	
						}
					}
				}
			}
			heap_close(rel, NoLock);
			fdw_scan_tlist = tmp_tlist;
		}

		if(is_target_rel)
		{
			FuncExpr   *fexpr;
			char *funcname = "self_node_inx";
			Oid funcargtypes[1];
			Oid nodefunOid;
			PlaceHolderVar *phv;
			TargetEntry *tle = NULL;

			nodefunOid = LookupFuncName(list_make1(makeString(funcname)), 0, funcargtypes, false);
			fexpr = makeFuncExpr(nodefunOid,
					get_func_rettype(nodefunOid),
					NIL,
					InvalidOid,
					InvalidOid,
					COERCE_EXPLICIT_CALL);
			phv = makeNode(PlaceHolderVar);

			phv->phexpr = (Expr *)fexpr;
			phv->phrels = bms_make_singleton(scan_relid);
			phv->phid = ++(root->glob->lastPHId);
			phv->phlevelsup = 0;
			tle = makeTargetEntry((Expr *)phv,
					list_length(fdw_scan_tlist) + 1,
					"node_inx",
					true);
			if(fdw_scan_tlist)
				fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
			else
				fdw_scan_tlist = list_make1(tle);
		}

		add_node_inx = true;
	}
	if(best_path->path.pathkeys && list_length(node_list) > 1)
	{
		simple_sort = makeNode(MergeAppend);

		prepare_simple_sort_from_pathkeys(root, &fdw_scan_tlist, best_path->path.pathkeys,
				foreignrel,
				NULL,
				simple_sort);

	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
							remote_exprs, best_path->path.pathkeys,
							false, &retrieved_attrs, &params_list);
    if(reloids_list == NIL)
    {
       reloids_list = get_all_reloids(root, foreignrel); 
    }
	if(have_wholerow)
	{
		Node       *new_expr;
		TargetEntry *new_tle;

		new_expr = (Node *) makeVar(foreignrel->relid,
				0,
				RECORDOID,
				-1,
				InvalidOid,
				0);

		new_tle = makeTargetEntry((Expr *) new_expr,
				list_length(fdw_scan_tlist) + 1,
				pstrdup("wholerow"),
				true);
		fdw_scan_tlist = lappend(fdw_scan_tlist, new_tle);
	}

	/* Remember remote_exprs for possible use by polarxPlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;


	if(get_relation_access_type(root->parse) == RELATION_ACCESS_UPDATE)
	{
		fpinfo->node_list = node_list;
	}
	else
	{
		fpinfo->node_list = NIL;
	}

    dist_for_param = polarxMakeNode(DistributionForParam);
    dist_for_param->distributionType =  dist->distributionType;
    dist_for_param->accessType = get_relation_access_type(root->parse);
    dist_for_param->paramId = -1; /* -1 means this foreign scan can not be pushed into one node*/
    dist_for_param->distributionExpr = copyObject(dist->distributionExpr);
    if(IsLocatorReplicated(dist_for_param->distributionType))
    {
        dist_for_param->paramId = 0; /*0 means this is replication table, can be pushed any one node*/
    }
    else if((remote_exprs || (IS_JOIN_REL(foreignrel) && fpinfo->joinclauses))
                && IsLocatorDistributedByValue(dist_for_param->distributionType))
    {
        ListCell   *lc = NULL;
        foreach(lc, remote_exprs)
        {
            Expr       *expr = (Expr *) lfirst(lc);

            /* Extract clause from RestrictInfo, if required */
            if (IsA(expr, RestrictInfo))
                expr = ((RestrictInfo *) expr)->clause;

            if(!check_hashjoinable(expr))
            {
                continue;
            }
            else
            {
                OpExpr *op_node = (OpExpr *)expr;

                Node       *leftarg  = linitial(op_node->args);
                Node       *rightarg  = lsecond(op_node->args);

                if(equal(leftarg, dist_for_param->distributionExpr) && 
                    IsA(rightarg,Param))
                {
                    dist_for_param->paramId = ((Param *)rightarg)->paramid; 
                    break;
                }
                else if(equal(rightarg, dist_for_param->distributionExpr) &&
                        IsA(leftarg,Param))
                {
                    dist_for_param->paramId = ((Param *)leftarg)->paramid;
                    break;
                }
            }
        }

        if(dist_for_param->paramId < 0)
        {
            foreach(lc, fpinfo->joinclauses)
            {
                Expr       *expr = (Expr *) lfirst(lc);

                /* Extract clause from RestrictInfo, if required */
                if (IsA(expr, RestrictInfo))
                    expr = ((RestrictInfo *) expr)->clause;

                if(!check_hashjoinable(expr))
                {
                    continue;
                }
                else
                {
                    OpExpr *op_node = (OpExpr *)expr;

                    Node       *leftarg  = linitial(op_node->args);
                    Node       *rightarg  = lsecond(op_node->args);

                    if(equal(leftarg, dist_for_param->distributionExpr) && 
                            IsA(rightarg,Param))
                    {
                        dist_for_param->paramId = ((Param *)rightarg)->paramid; 
                        break;
                    }
                    else if(equal(rightarg, dist_for_param->distributionExpr) &&
                                                IsA(leftarg,Param))
                    {
                        dist_for_param->paramId = ((Param *)leftarg)->paramid;
                        break;
                    }
                }
            }
        }
    }
	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(sql.data),
			retrieved_attrs,
			makeInteger(fpinfo->fetch_size),
			node_list,
			makeInteger(get_relation_access_type(root->parse)));
	fdw_private = lappend(fdw_private, simple_sort);
	fdw_private = lappend(fdw_private, makeInteger(add_node_inx));
	if(have_wholerow)
		fdw_private = lappend(fdw_private, makeInteger(list_length(fdw_scan_tlist)));
	else
		fdw_private = lappend(fdw_private, makeInteger(0));
	fdw_private = lappend(fdw_private, makeInteger(has_oid));
    /* params extern will be set in polarx_planner for cached plan*/
	fdw_private = lappend(fdw_private, NULL);
	fdw_private = lappend(fdw_private, makeString(getPreparedStmtName(sql.data, reloids_list)));
	fdw_private = lappend(fdw_private, dist_for_param);
	fdw_private = lappend(fdw_private, NULL);

	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}

/*
 * polarxBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
void
polarxBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
	PgFdwScanState *fsstate;
	int			numParams;
	int			i = 0;
	ListCell		*lc = NULL;
	MergeAppend *simple_sort = NULL;
	bool add_node_inx = false;
    ParamExternDataInfo *paramsExtern = NULL;
    char    portal_name[NAMEDATALEN];

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (PgFdwScanState *) palloc0(sizeof(PgFdwScanState));
	node->fdw_state = (void *) fsstate;


	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateFetchSize));
	fsstate->node_list = (List *) list_nth(fsplan->fdw_private, FdwScanPrivateNodeList);
	fsstate->rel_access_type = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateAccessType));
	fsstate->conn_num = list_length(fsstate->node_list);
	simple_sort = (MergeAppend *)list_nth(fsplan->fdw_private, FdwScanPrivateSimpleSort);
	add_node_inx = intVal(list_nth(fsplan->fdw_private,FdwScanPrivateAddNodeInx));
	fsstate->have_wholerow = intVal(list_nth(fsplan->fdw_private,FdwScanPrivateHaveWholerow));
	fsstate->has_oid = intVal(list_nth(fsplan->fdw_private,FdwScanPrivateHasOid));
    paramsExtern = (ParamExternDataInfo *) list_nth(fsplan->fdw_private, FdwScanPrivateParamsExtern);
	fsstate->query_hash = strVal(list_nth(fsplan->fdw_private,FdwScanPrivateQueryTreeHash));
	fsstate->cursor_exists = paramsExtern ? paramsExtern->is_cursor : false;

    if (paramsExtern && econtext && econtext->ecxt_param_list_info == NULL)
    {
        econtext->ecxt_param_list_info = paramsExtern->param_list_info;
    }

	if(fsstate->have_wholerow > 0)
		fsstate->wholerow_first = true;
	else
		fsstate->wholerow_first = false;

	InitResponseCombiner(&fsstate->combiner, 0, COMBINE_TYPE_NONE);
	fsstate->combiner.request_type = REQUEST_TYPE_QUERY;

	if(simple_sort && fsstate->conn_num > 1)
	{
		fsstate->ms_nkeys = simple_sort->numCols;
		fsstate->ms_sortkeys = palloc0(sizeof(SortSupportData) * simple_sort->numCols);
		fsstate->combiner.merge_sort = true;

		for (i = 0; i < simple_sort->numCols; i++)
		{
			SortSupport sortKey = fsstate->ms_sortkeys + i;

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = simple_sort->collations[i];
			sortKey->ssup_nulls_first = simple_sort->nullsFirst[i];
			sortKey->ssup_attno = simple_sort->sortColIdx[i];

			/*
			 * It isn't feasible to perform abbreviated key conversion, since
			 * tuples are pulled into mergestate's binary heap as needed.  It
			 * would likely be counter-productive to convert tuples into an
			 * abbreviated representation as they're pulled up, so opt out of that
			 * additional optimization entirely.
			 */
			sortKey->abbreviate = false;

			PrepareSortSupportFromOrderingOp(simple_sort->sortOperators[i], sortKey);
		}
		fsstate->ms_heap = binaryheap_allocate(fsstate->conn_num, fdw_heap_compare_slots,
                                              fsstate);
	}

	fsstate->conn_node_map = palloc0(fsstate->conn_num * sizeof(int));

	i = 0;
	foreach(lc, fsstate->node_list)
	{
		fsstate->conn_node_map[i++] = lfirst_int(lc);
	}
	fsstate->current_node = -1;

	/* Assign a unique ID for my cursor */
	fsstate->is_prepared = false;
    if(fsstate->cursor_exists)
    {
        fsstate->cursor_number = GetCursorNumber();
        snprintf(portal_name, sizeof(portal_name),
                    "%s_0_%u", fsstate->query_hash, fsstate->cursor_number);
    }
    else
    {
        fsstate->cursor_number = GetPrepStmtNumber();
        snprintf(portal_name, sizeof(portal_name),
                "%s_1_%u", fsstate->query_hash, fsstate->cursor_number);
    }

    if(paramsExtern ? paramsExtern->portal_need_name : true)
    {
        fsstate->combiner.cursor = pstrdup(portal_name);
    }
    else
    {
        fsstate->combiner.cursor = NULL;
    }
    fsstate->combiner.prep_name = pstrdup(fsstate->query_hash);

	polarx_scan_begin(fsstate->rel_access_type == RELATION_ACCESS_READ, fsstate->node_list, true);
	if(fsstate->combiner.merge_sort)
	{
		fsstate->tuples = (HeapTuple *) palloc0(fsstate->conn_num * sizeof(HeapTuple));
		fsstate->batch_cxt = palloc0(fsstate->conn_num *  sizeof(MemoryContext));
		for(i = 0; i< fsstate->conn_num; i++)
		{
			fsstate->batch_cxt[i] = AllocSetContextCreate(estate->es_query_cxt,
									"polarx_fdw tuple data",
									ALLOCSET_DEFAULT_SIZES);
		}
	}
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
							"polarx_fdw temporary data",
							ALLOCSET_SMALL_SIZES);

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if(fsstate->combiner.merge_sort)
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}
	else if (fsplan->scan.scanrelid > 0)
	{
		if(add_node_inx)
		{
			fsstate->rel = NULL;
			fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
		}
		else
		{
			fsstate->rel = node->ss.ss_currentRelation;
			fsstate->tupdesc = RelationGetDescr(fsstate->rel);
		}
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	fsstate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &fsstate->param_flinfo,
							 &fsstate->param_exprs,
							 &fsstate->param_values,
							 &fsstate->param_types);
}

/*
 * polarxIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
TupleTableSlot *
polarxIterateForeignScan(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ResponseCombiner *combiner = &(fsstate->combiner);
	int i = 0;

	combiner->ss = node->ss;

	if(!fsstate->query_done)
	{
		Snapshot        snapshot = GetActiveSnapshot();
		PGXCNodeHandle **connections = NULL;
		PGXCNodeAllHandles *all_handles;
		int                regular_conn_count = 0;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
		all_handles = get_handles(fsstate->node_list, NIL, false, true);
		MemoryContextSwitchTo(oldcontext);

		regular_conn_count = all_handles->dn_conn_count;
		connections = all_handles->datanode_handles;

		combiner->recv_node_count = regular_conn_count;
		combiner->recv_tuples      = 0;
		combiner->recv_total_time = -1;
		combiner->recv_datarows = 0;
		combiner->node_count = regular_conn_count;

		if (regular_conn_count > 0)
		{
			combiner->connections = connections;
			combiner->conn_count = regular_conn_count;
			combiner->current_conn = 0;
		}
        if(!fsstate->is_prepared)
            prepare_foreign_sql(fsstate->query_hash, fsstate->query,
                                     fsstate->node_list, NULL);
        if(!fsstate->is_prepared)
            fsstate->is_prepared = true;
		if (!polarx_start_command_on_connection(connections, node, snapshot))
		{
			pfree_pgxc_all_handles(all_handles);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}

		fsstate->query_done = true;

		if(combiner->merge_sort)
		{
			for (i = 0; i < regular_conn_count; i++)
			{
                RemoteDataRow datarow = NULL;

				combiner->current_conn = i;
				datarow = FetchDatarow(combiner);
				if (datarow)
				{
					oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt[i]);
					fsstate->tuples[i] = make_tuple_from_datarow(datarow->msg,
											fsstate->rel,
											fsstate->attinmeta,
											fsstate->retrieved_attrs,
											node,
											fsstate->temp_cxt, true);
					MemoryContextSwitchTo(oldcontext);
					binaryheap_add_unordered(fsstate->ms_heap, Int32GetDatum(i));
                    pfree(datarow);
				}
			}
			binaryheap_build(fsstate->ms_heap);
		}
	}
	else if(combiner->merge_sort)
	{
		MemoryContext oldcontext;
        RemoteDataRow datarow = NULL;

		i = DatumGetInt32(binaryheap_first(fsstate->ms_heap));
		combiner->current_conn = i;
		datarow = FetchDatarow(combiner);
		fsstate->tuples[i] = NULL;
		MemoryContextReset(fsstate->batch_cxt[i]);
		if (datarow)
		{
			oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt[i]);
			fsstate->tuples[i] = make_tuple_from_datarow(datarow->msg,
									fsstate->rel,
									fsstate->attinmeta,
									fsstate->retrieved_attrs,
									node,
									fsstate->temp_cxt, true);
			MemoryContextSwitchTo(oldcontext);
			binaryheap_replace_first(fsstate->ms_heap, Int32GetDatum(i));
            pfree(datarow);
		}
		else
			(void) binaryheap_remove_first(fsstate->ms_heap);
	}

	for (i = 0; i < combiner->conn_count; i++)
	{
		if(combiner->connections[i] && combiner->connections[i]->combiner == NULL)
			combiner->connections[i]->combiner = combiner;
	}

	if(combiner->merge_sort)
	{
		if (!binaryheap_empty(fsstate->ms_heap))
		{
			i = DatumGetInt32(binaryheap_first(fsstate->ms_heap));
			if(TupIsNull(slot))
				slot = node->ss.ss_ScanTupleSlot;

			ExecStoreTuple(fsstate->tuples[i],
					slot,
					InvalidBuffer,
					false);
			fsstate->current_node = fsstate->conn_node_map[i];
			return slot;
		}

	}
	else
	{
        RemoteDataRow datarow = NULL;

		datarow = FetchDatarow(combiner);

		if(datarow)
		{
			HeapTuple tmp_tuple;

			tmp_tuple = make_tuple_from_datarow(datarow->msg,
								fsstate->rel,
								fsstate->attinmeta,
								fsstate->retrieved_attrs,
								node,
								fsstate->temp_cxt, true);
			fsstate->current_node = PGXCNodeGetNodeId(datarow->msgnode, NULL);
			ExecStoreTuple(tmp_tuple,
					slot,
					InvalidBuffer,
					false);
            pfree(datarow);

			return slot;
		}
	}

	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	if(node->ss.ss_ScanTupleSlot)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);
	return NULL;
}

/*
 * polarxReScanForeignScan
 *		Restart the scan.
 */
void
polarxReScanForeignScan(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->query_done)
		return;

	if(fsstate->combiner.cursor)
	{
		close_node_cursors(fsstate->combiner.cursor,
                            fsstate->node_list);
	}

	fsstate->query_done = false;
	if(fsstate->combiner.merge_sort)
		binaryheap_reset(fsstate->ms_heap);
    pgxc_connections_cleanup(&(fsstate->combiner));
	fsstate->combiner.command_complete_count = 0;
	fsstate->combiner.description_count = 0;


	/* Now force a fresh FETCH. */
	fsstate->current_node = -1;
}

/*
 * polarxEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
void
polarxEndForeignScan(ForeignScanState *node)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;
	if(fsstate->combiner.cursor)
	{
		//DropTxnDatanodeStatement(fsstate->combiner.cursor);
		close_node_cursors(fsstate->combiner.cursor,
                            fsstate->node_list);
	}
    pgxc_connections_cleanup(&(fsstate->combiner));
	fsstate->query_done = false;
	if(fsstate->combiner.merge_sort)
		binaryheap_reset(fsstate->ms_heap);
}

/*
 * polarxAddForeignUpdateTargets
 *		Add resjunk column(s) needed for update/delete on a foreign table
 */
void
polarxAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation)
{
	Var		   *var;
	const char *attrname;
	TargetEntry *tle;

	/*
	 * In polarx_fdw, what we need is the ctid, same as for a regular table.
	 */

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  SelfItemPointerAttributeNumber,
				  TIDOID,
				  -1,
				  InvalidOid,
				  0);

	/* Wrap it in a resjunk TLE with the right name ... */
	attrname = "ctid";

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}

/*
 * polarxPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 */
List *
polarxPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	StringInfoData sql;
	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;
    List       *res = NIL;
	bool		doNothing = false;
	PgFdwRelationInfo *fpinfo = NULL;
    List       *reloids_list = NIL;

	if(operation != CMD_INSERT)
		fpinfo =  (PgFdwRelationInfo *)root->simple_rel_array[resultRelation]->fdw_private;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);
	if(operation == CMD_UPDATE ||  operation == CMD_DELETE)
	{
		TargetEntry *tle = NULL;
		bool is_exist = false;
		Plan *sub_plan = list_nth(plan->plans, subplan_index);
		change_wholerow(sub_plan);
		tle = get_node_index(sub_plan, &is_exist);

		if(!is_exist)
		{
			TargetEntry *new_tle = NULL;
			if(tle != NULL)
			{
				new_tle = copyObject(tle);
				new_tle->resno = list_length(sub_plan->targetlist) + 1;
				sub_plan->targetlist = lappend(sub_plan->targetlist, new_tle);
			}
		}
	}
	if (operation == CMD_UPDATE)
	{
		int                     col;
		Plan *subplan = list_nth(plan->plans, subplan_index);

		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber      attno = col + FirstLowInvalidHeapAttributeNumber;
			TargetEntry *tle;

			tle = get_tle_by_resno(subplan->targetlist, attno);

			if (!tle)
				elog(ERROR, "attribute number %d not found in subplan targetlist",
						attno);
			if(fpinfo && IsLocatorReplicated(fpinfo->rd_locator_info->locatorType)
					&& contain_volatile_functions((Node *)tle->expr))
				elog(ERROR, "update replication table with volatile function is not support yet");
		}
	}

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
	 * foreign table, we transmit all columns like INSERT; else we transmit
	 * only columns that were explicitly targets of the UPDATE, so as to avoid
	 * unnecessary data transmission.  (We can't do that for INSERT since we
	 * would miss sending default values for columns not listed in the source
	 * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
	 * those triggers might change values for non-target columns, in which
	 * case we would miss sending changed values for those columns.)
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		int			col;

		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
			 (int) plan->onConflictAction);

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			deparseInsertSql(&sql, rte, resultRelation, rel,
							 targetAttrs, doNothing, returningList,
							 &retrieved_attrs);
			break;
		case CMD_UPDATE:
			deparseUpdateSql(&sql, rte, resultRelation, rel,
							 targetAttrs, returningList,
							 &retrieved_attrs);
			break;
		case CMD_DELETE:
			deparseDeleteSql(&sql, rte, resultRelation, rel,
							 returningList,
							 &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	res = list_make5(makeString(sql.data),
			targetAttrs,
			makeInteger((retrieved_attrs != NIL)),
			retrieved_attrs,
			makeInteger(get_relation_access_type(root->parse)));
    res = lappend(res, NULL);
    if(rte)
    {
        reloids_list = list_make1_oid(rte->relid);
    }
    res = lappend(res, makeString(getPreparedStmtName(sql.data, reloids_list)));
    return res;
}

/*
 * polarxBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
void
polarxBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
	PgFdwModifyState *fmstate;
	char	   *query;
	List	   *target_attrs;
	bool		has_returning;
	List	   *retrieved_attrs;
	RangeTblEntry *rte;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private,
							FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private,
									 FdwModifyPrivateTargetAttnums);
	has_returning = intVal(list_nth(fdw_private,
									FdwModifyPrivateHasReturning));
	retrieved_attrs = (List *) list_nth(fdw_private,
										FdwModifyPrivateRetrievedAttrs);
	/* Find RTE. */
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex,
				   mtstate->ps.state->es_range_table);

	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									mtstate->operation,
									mtstate->mt_plans[subplan_index]->plan,
									query,
									target_attrs,
									has_returning,
									retrieved_attrs);
    fmstate->query_hash = strVal(list_nth(fdw_private,FdwModifyPrivateQueryTreeHash));
	fmstate->rel_access_type = intVal(list_nth(fdw_private,FdwModifyPrivateAccessType));
    /* check it again due to plan cache feature */
    if(mtstate->operation == CMD_UPDATE)
    {
        ListCell *lc = NULL;
        Plan *subplan = mtstate->mt_plans[subplan_index]->plan;

        foreach(lc, target_attrs)
        {
            int         attno = lfirst_int(lc);
            TargetEntry *tle;

            tle = get_tle_by_resno(subplan->targetlist, attno);
            if(IsLocatorReplicated(fmstate->locator_type)
                && contain_volatile_functions((Node *)tle->expr))
            elog(ERROR, "update replication table with volatile function is not support yet");
        }
    }

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * polarxExecForeignInsert
 *		Insert one row into a foreign table
 */
TupleTableSlot *
polarxExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;
	const char **p_values;
	int	n_rows = 0;
	List *node_list = NIL;
	Datum dist_val = 0;
	bool isNull = false;
    ListCell        *lc = NULL;

	/*
	 * If the fmstate has aux_fmstate set, use the aux_fmstate (see
	 * polarxBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		fmstate = fmstate->aux_fmstate;

	if(IsLocatorDistributedByValue(fmstate->locator_type))
		dist_val = slot_getattr(slot, fmstate->part_attr_num, &isNull);
	node_list = GetRelationNodesWithRelation(fmstate->rel,
			isNull ? 0 : dist_val, isNull, fmstate->rel_access_type, NumDataNodes);
    foreach(lc, node_list)
    {
        int index = lfirst_int(lc);

        if(fmstate->is_prepared[index])
            continue;
        polarx_scan_begin(false, node_list, true);
    }
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
	SetSendCommandId(true);
#endif

	/* Set up the prepared statement on the remote server, if we didn't yet */
    prepare_foreign_modify(fmstate, node_list);

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, NULL, slot);

	polarx_send_query_prepared(fmstate, node_list, p_values, slot, &n_rows);
	
	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * polarxExecForeignUpdate
 *		Update one row in a foreign table
 */
TupleTableSlot *
polarxExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;
	Datum		datum;
	bool		isNull;
	const char **p_values;
	int	n_rows = 0;
	List *node_list = NIL;
    ListCell        *lc = NULL;

	if(fmstate->nodeIndexAttno)
	{
		Datum node_inx_datum;
		bool node_inx_isNull;

		node_inx_datum = ExecGetJunkAttribute(planSlot,
				fmstate->nodeIndexAttno,
				&node_inx_isNull);
		if(node_inx_isNull)
			elog(ERROR, "node index is missing");
		fmstate->current_node = DatumGetInt32(node_inx_datum);

        if(IsLocatorDistributedByValue(fmstate->locator_type) &&
           list_member_int(fmstate->target_attrs, fmstate->part_attr_num))
        {
            Datum dist_val = ExecGetJunkAttribute(planSlot, fmstate->part_attr_num, &isNull);

            /* if the value is NULL, defalut this record should be in node 0*/
            if(isNull)
                node_list = lappend_int(node_list, 0);
            else
                node_list = GetRelationNodesWithRelation(fmstate->rel,
                                                        dist_val,
                                                        false,
                                                        fmstate->rel_access_type,
                                                        NumDataNodes);
            if(!list_member_int(node_list, fmstate->current_node))
                elog(ERROR, "distribute-column does not support cross update");
        }
        else
        {
            node_list = lappend_int(node_list, fmstate->current_node);
        }
	}
    foreach(lc, node_list)
    {
        int index = lfirst_int(lc);

        if(fmstate->is_prepared[index])
            continue;
        polarx_scan_begin(false, node_list, true);
    }
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
	SetSendCommandId(true);
#endif

    prepare_foreign_modify(fmstate, node_list);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
					fmstate->ctidAttno,
					&isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, (ItemPointer) DatumGetPointer(datum), slot);

	polarx_send_query_prepared(fmstate, node_list, p_values, slot, &n_rows);
	
	MemoryContextReset(fmstate->temp_cxt);

	if((IsLocatorReplicated(fmstate->locator_type) && fmstate->current_node == 0)
			|| IsLocatorColumnDistributed(fmstate->locator_type))
	{
		if(n_rows == 0 && estate->es_trig_tuple_slot == slot)
			ereport(ERROR,
					(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
					 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
					 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
		return (n_rows > 0) ? slot : NULL;
	}
	else
	{
		return NULL;
	}
}

/*
 * polarxExecForeignDelete
 *		Delete one row from a foreign table
 */
TupleTableSlot *
polarxExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;
	Datum		datum;
	bool		isNull;
	const char **p_values;
	int	n_rows = 0;
	List *node_list = NIL;
    ListCell        *lc = NULL;

	if(fmstate->nodeIndexAttno)
	{
		Datum node_inx_datum;
		bool node_inx_isNull;

		node_inx_datum = ExecGetJunkAttribute(planSlot,
				fmstate->nodeIndexAttno,
				&node_inx_isNull);
		if(node_inx_isNull)
			elog(ERROR, "node index is missing");
		fmstate->current_node = DatumGetInt32(node_inx_datum);
		node_list = lappend_int(node_list, fmstate->current_node);
	}
    foreach(lc, node_list)
    {
        int index = lfirst_int(lc);

        if(fmstate->is_prepared[index])
            continue;
        polarx_scan_begin(false, node_list, true);
    }
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
	SetSendCommandId(true);
#endif

    prepare_foreign_modify(fmstate, node_list);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot,
					fmstate->ctidAttno,
					&isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, (ItemPointer) DatumGetPointer(datum), NULL);

	polarx_send_query_prepared(fmstate, node_list, p_values, slot, &n_rows);
	
	MemoryContextReset(fmstate->temp_cxt);
	if((IsLocatorReplicated(fmstate->locator_type) && fmstate->current_node == 0)
			|| IsLocatorColumnDistributed(fmstate->locator_type))
	{
		if(n_rows == 0 && estate->es_trig_tuple_slot == slot)
			ereport(ERROR,
					(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
					 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
					 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
		return (n_rows > 0) ? slot : NULL;
	}
	else
	{
		return NULL;
	}
}

/*
 * polarxEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
void
polarxEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* Destroy the execution state */
	finish_foreign_modify(fmstate);
}

/*
 * polarxBeginForeignInsert
 *		Begin an insert operation on a foreign table
 */
void
polarxBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo)
{
	PgFdwModifyState *fmstate;
	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	EState	   *estate = mtstate->ps.state;
	Index		resultRelation = resultRelInfo->ri_RangeTableIndex;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	RangeTblEntry *rte;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			attnum;
	StringInfoData sql;
	List	   *targetAttrs = NIL;
	List	   *retrieved_attrs = NIL;
	bool		doNothing = false;
    List        *reloids_list = NIL;

	/*
	 * If the foreign table we are about to insert routed rows into is also
	 * an UPDATE subplan result rel that will be updated later, proceeding
	 * with the INSERT will result in the later UPDATE incorrectly modifying
	 * those routed rows, so prevent the INSERT --- it would be nice if we
	 * could handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(resultRelInfo->ri_usesFdwDirectModify ||
		 resultRelInfo->ri_FdwState) &&
		resultRelInfo > mtstate->resultRelInfo + mtstate->mt_whichplan)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated \"%s\"",
						RelationGetRelationName(rel))));

	initStringInfo(&sql);

	/* We transmit all columns that are defined in the foreign table. */
	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (!attr->attisdropped)
			targetAttrs = lappend_int(targetAttrs, attnum);
	}

	/* Check if we add the ON CONFLICT clause to the remote query. */
	if (plan)
	{
		OnConflictAction onConflictAction = plan->onConflictAction;

		/* We only support DO NOTHING without an inference specification. */
		if (onConflictAction == ONCONFLICT_NOTHING)
			doNothing = true;
		else if (onConflictAction != ONCONFLICT_NONE)
			elog(ERROR, "unexpected ON CONFLICT specification: %d",
				 (int) onConflictAction);
	}

	/*
	 * If the foreign table is a partition, we need to create a new RTE
	 * describing the foreign table for use by deparseInsertSql and
	 * create_foreign_modify() below, after first copying the parent's RTE and
	 * modifying some fields to describe the foreign partition to work on.
	 * However, if this is invoked by UPDATE, the existing RTE may already
	 * correspond to this partition if it is one of the UPDATE subplan target
	 * rels; in that case, we can just use the existing RTE as-is.
	 */
	rte = list_nth(estate->es_range_table, resultRelation - 1);
	if (rte->relid != RelationGetRelid(rel))
	{
		rte = copyObject(rte);
		rte->relid = RelationGetRelid(rel);
		rte->relkind = RELKIND_FOREIGN_TABLE;

		/*
		 * For UPDATE, we must use the RT index of the first subplan target
		 * rel's RTE, because the core code would have built expressions for
		 * the partition, such as RETURNING, using that RT index as varno of
		 * Vars contained in those expressions.
		 */
		if (plan && plan->operation == CMD_UPDATE &&
			resultRelation == plan->nominalRelation)
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
	}

	/* Construct the SQL command string. */
	deparseInsertSql(&sql, rte, resultRelation, rel, targetAttrs, doNothing,
					 resultRelInfo->ri_returningList, &retrieved_attrs);

	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									CMD_INSERT,
									NULL,
									sql.data,
									targetAttrs,
									retrieved_attrs != NIL,
									retrieved_attrs);
    if(rte)
    {
        reloids_list = list_make1_oid(rte->relid);
    }
	fmstate->query_hash = getPreparedStmtName(sql.data, reloids_list);
	fmstate->rel_access_type = RELATION_ACCESS_INSERT;

	/*
	 * If the given resultRelInfo already has PgFdwModifyState set, it means
	 * the foreign table is an UPDATE subplan result rel; in which case, store
	 * the resulting state into the aux_fmstate of the PgFdwModifyState.
	 */
	if (resultRelInfo->ri_FdwState)
	{
		Assert(plan && plan->operation == CMD_UPDATE);
		Assert(resultRelInfo->ri_usesFdwDirectModify == false);
		((PgFdwModifyState *) resultRelInfo->ri_FdwState)->aux_fmstate = fmstate;
	}
	else
		resultRelInfo->ri_FdwState = fmstate;
}

/*
 * polarxEndForeignInsert
 *		Finish an insert operation on a foreign table
 */
void
polarxEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;

	Assert(fmstate != NULL);

	/*
	 * If the fmstate has aux_fmstate set, get the aux_fmstate (see
	 * polarxBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		fmstate = fmstate->aux_fmstate;

	/* Destroy the execution state */
	finish_foreign_modify(fmstate);
}

/*
 * polarxIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
int
polarxIsForeignRelUpdatable(Relation rel)
{
	bool		updatable;
	ForeignTable *table;
	ForeignServer *server;
	ListCell   *lc;

	/*
	 * By default, all polarx_fdw foreign tables are assumed updatable. This
	 * can be overridden by a per-server setting, which in turn can be
	 * overridden by a per-table setting.
	 */
	updatable = true;

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
			updatable = defGetBoolean(def);
	}
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
			updatable = defGetBoolean(def);
	}

	/*
	 * Currently "updatable" means support for INSERT, UPDATE and DELETE.
	 */
	return updatable ?
		(1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}

/*
 * polarxRecheckForeignScan
 *		Execute a local join execution plan for a foreign join
 */
bool
polarxRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
	Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	PlanState  *outerPlan = outerPlanState(node);
	TupleTableSlot *result;

	/* For base foreign relations, it suffices to set fdw_recheck_quals */
	if (scanrelid > 0)
		return true;

	Assert(outerPlan != NULL);

	/* Execute a local join execution plan */
	result = ExecProcNode(outerPlan);
	if (TupIsNull(result))
		return false;

	/* Store result in the given slot */
	ExecCopySlot(slot, result);

	return true;
}

/*
 * polarxPlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
bool
polarxPlanDirectModify(PlannerInfo *root,
						 ModifyTable *plan,
						 Index resultRelation,
						 int subplan_index)
{
	CmdType		operation = plan->operation;
	Plan	   *subplan;
	RelOptInfo *foreignrel;
	RangeTblEntry *rte;
	PgFdwRelationInfo *fpinfo;
	Relation	rel;
	StringInfoData sql;
	ForeignScan *fscan;
	List	   *targetAttrs = NIL;
	List	   *remote_exprs;
	List	   *params_list = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;
	List	   *reloids_list = NIL;

	/*
	 * Decide whether it is safe to modify a foreign table directly.
	 */

	/*
	 * The table modification must be an UPDATE or DELETE.
	 */
	if (operation != CMD_UPDATE && operation != CMD_DELETE)
		return false;

	/*
	 * It's unsafe to modify a foreign table directly if there are any local
	 * joins needed.
	 */
	subplan = (Plan *) list_nth(plan->plans, subplan_index);
	if (!IsA(subplan, ForeignScan))
		return false;
	fscan = (ForeignScan *) subplan;

	/*
	 * It's unsafe to modify a foreign table directly if there are any quals
	 * that should be evaluated locally.
	 */
	if (subplan->qual != NIL)
		return false;

	/* Safe to fetch data about the target foreign rel */
	if (fscan->scan.scanrelid == 0)
	{
		RelOptInfo *originalrel = root->simple_rel_array[resultRelation];

		foreignrel = find_join_rel(root, fscan->fs_relids);
		/* the last foreignrel locator type must be same with target rel */
		if(originalrel && foreignrel
				&& ((PgFdwRelationInfo *) foreignrel->fdw_private)->rd_locator_info->locatorType
				!= ((PgFdwRelationInfo *) originalrel->fdw_private)->rd_locator_info->locatorType)
			return false;
		/* We should have a rel for this foreign join. */
		Assert(foreignrel);
	}
	else
		foreignrel = root->simple_rel_array[resultRelation];
	rte = root->simple_rte_array[resultRelation];
	fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;

	Assert(fpinfo->node_list != NIL);

	/*
	 * It's unsafe to update a foreign table directly, if any expressions to
	 * assign to the target columns are unsafe to evaluate remotely.
	 */
	if (operation == CMD_UPDATE)
	{
		int			col;

		/*
		 * We transmit only columns that were explicitly targets of the
		 * UPDATE, so as to avoid unnecessary data transmission.
		 */
		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;
			TargetEntry *tle;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
            /* update distribute column not support diretct update*/
            if (IsLocatorDistributedByValue(fpinfo->rd_locator_info->locatorType)
                    && attno == fpinfo->rd_locator_info->partAttrNum)
                return false;

			tle = get_tle_by_resno(subplan->targetlist, attno);

			if (!tle)
				elog(ERROR, "attribute number %d not found in subplan targetlist",
					 attno);
			if(IsLocatorReplicated(fpinfo->rd_locator_info->locatorType) 
					&& contain_volatile_functions((Node *)tle->expr))
				elog(ERROR, "update replication table with volatile function is not support yet");

			if (!is_foreign_expr(root, foreignrel, (Expr *) tle->expr))
				return false;

			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

    /*
	 * Ok, rewrite subplan so as to modify the foreign table directly.
	 */
	initStringInfo(&sql);


	/*
	 * Recall the qual clauses that must be evaluated remotely.  (These are
	 * bare clauses not RestrictInfos, but deparse.c's appendConditions()
	 * doesn't care.)
	 */
	remote_exprs = fpinfo->final_remote_exprs;

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
	{
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

		/*
		 * When performing an UPDATE/DELETE .. RETURNING on a join directly,
		 * we fetch from the foreign server any Vars specified in RETURNING
		 * that refer not only to the target relation but to non-target
		 * relations.  So we'll deparse them into the RETURNING clause of the
		 * remote query; use a targetlist consisting of them instead, which
		 * will be adjusted to be new fdw_scan_tlist of the foreign-scan plan
		 * node below.
		 */
		if (fscan->scan.scanrelid == 0)
			returningList = build_remote_returning(resultRelation, rel,
												   returningList);
	}

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_UPDATE:
			deparseDirectUpdateSql(&sql, root, resultRelation, rel,
								   foreignrel,
								   ((Plan *) fscan)->targetlist,
								   targetAttrs,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		case CMD_DELETE:
			deparseDirectDeleteSql(&sql, root, resultRelation, rel,
								   foreignrel,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}
    if(reloids_list == NIL)
    {
        RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);

        reloids_list = get_all_reloids(root, foreignrel);
        if(reloids_list == NIL)
            reloids_list = list_make1_oid(rte->relid);
        else
            reloids_list = list_append_unique_oid(reloids_list, rte->relid);
    }
	/*
	 * Update the operation info.
	 */
	fscan->operation = operation;

	/*
	 * Update the fdw_exprs list that will be available to the executor.
	 */
	fscan->fdw_exprs = params_list;

	/*
	 * Update the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwDirectModifyPrivateIndex, above.
	 */
	fscan->fdw_private = list_make5(makeString(sql.data),
									makeInteger((retrieved_attrs != NIL)),
									retrieved_attrs,
									makeInteger(plan->canSetTag),
                                    fpinfo->node_list);
    fscan->fdw_private = lappend(fscan->fdw_private, makeInteger(fpinfo->rd_locator_info->locatorType));
    fscan->fdw_private = lappend(fscan->fdw_private, targetAttrs);
    fscan->fdw_private = lappend(fscan->fdw_private, makeInteger(rte->relid));
    fscan->fdw_private = lappend(fscan->fdw_private, NULL);
    fscan->fdw_private = lappend(fscan->fdw_private,
                            makeString(getPreparedStmtName(sql.data, reloids_list)));

	/*
	 * Update the foreign-join-related fields.
	 */
	if (fscan->scan.scanrelid == 0)
	{
		/* No need for the outer subplan. */
		fscan->scan.plan.lefttree = NULL;

		/* Build new fdw_scan_tlist if UPDATE/DELETE .. RETURNING. */
		if (returningList)
			rebuild_fdw_scan_tlist(fscan, returningList);
	}
	else if(fscan->fdw_scan_tlist)
	{
		fscan->fdw_scan_tlist = NIL;
	}

	heap_close(rel, NoLock);
	return true;
}

/*
 * polarxBeginDirectModify
 *		Prepare a direct foreign table modification
 */
void
polarxBeginDirectModify(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	PgFdwDirectModifyState *dmstate;
	Index		rtindex;
	int			numParams;
    List       *target_attrs;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	dmstate = (PgFdwDirectModifyState *) palloc0(sizeof(PgFdwDirectModifyState));
	node->fdw_state = (void *) dmstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rtindex = estate->es_result_relation_info->ri_RangeTableIndex;

	/* Get info about foreign table. */
	if (fsplan->scan.scanrelid == 0)
		dmstate->rel = ExecOpenScanRelation(estate, rtindex, eflags);
	else
		dmstate->rel = node->ss.ss_currentRelation;

	dmstate->conn = NULL;

	/* Update the foreign-join-related fields. */
	if (fsplan->scan.scanrelid == 0)
	{
		/* Save info about foreign table. */
		dmstate->resultRel = dmstate->rel;

		dmstate->rel = NULL;
	}

	/* Initialize state variable */
	dmstate->num_tuples = -1;	/* -1 means not set yet */

	/* Get private info created by planner functions. */
	dmstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwDirectModifyPrivateUpdateSql));
	dmstate->has_returning = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateHasReturning));
	dmstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwDirectModifyPrivateRetrievedAttrs);
	dmstate->set_processed = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateSetProcessed));
	dmstate->node_list = (List *) list_nth(fsplan->fdw_private,
											FdwDirectModifyPrivateNodeList);

	polarx_scan_begin(false, dmstate->node_list, true);
	dmstate->query_hash = strVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateQueryTreeHash));
	dmstate->locator_type = (char)intVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateLocatorType));
    target_attrs = (List *) list_nth(fsplan->fdw_private,
											FdwDirectModifyPrivateTargetAttnums);

    /* check it again due to plan cache feature */
    if(fsplan->operation == CMD_UPDATE)
    {
        ListCell *lc = NULL;
        Plan *subplan = (Plan *)fsplan;

        foreach(lc, target_attrs)
        {
            int         attno = lfirst_int(lc);
            TargetEntry *tle;

            tle = get_tle_by_resno(subplan->targetlist, attno);
            if(IsLocatorReplicated(dmstate->locator_type)
                    && contain_volatile_functions((Node *)tle->expr))
                elog(ERROR, "update replication table with volatile function is not support yet");
        }
    }
	dmstate->current_result = 0;
	dmstate->current_index = 0;

	/* Create context for per-tuple temp workspace. */
	dmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "polarx_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (dmstate->has_returning)
	{
		TupleDesc	tupdesc;

		if (fsplan->scan.scanrelid == 0)
			tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
		else
			tupdesc = RelationGetDescr(dmstate->rel);

		dmstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/*
		 * When performing an UPDATE/DELETE .. RETURNING on a join directly,
		 * initialize a filter to extract an updated/deleted tuple from a scan
		 * tuple.
		 */
		if (fsplan->scan.scanrelid == 0)
			init_returning_filter(dmstate, fsplan->fdw_scan_tlist, rtindex);
	}

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	dmstate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &dmstate->param_flinfo,
							 &dmstate->param_exprs,
							 &dmstate->param_values,
							 &dmstate->param_types);
}

/*
 * polarxIterateDirectModify
 *		Execute a direct foreign table modification
 */
TupleTableSlot *
polarxIterateDirectModify(ForeignScanState *node)
{
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
	SetSendCommandId(true);
#endif
	return execute_dml_stmt(node);
}

/*
 * polarxEndDirectModify
 *		Finish a direct foreign table modification
 */
void
polarxEndDirectModify(ForeignScanState *node)
{
	PgFdwDirectModifyState *dmstate = (PgFdwDirectModifyState *) node->fdw_state;

	/* if dmstate is NULL, we are in EXPLAIN; nothing to do */
	if (dmstate == NULL)
		return;

    pgxc_connections_cleanup(&(dmstate->combiner));

	/* close the target relation. */
	if (dmstate->resultRel)
		ExecCloseScanRelation(dmstate->resultRel);

	/* MemoryContext will be deleted automatically. */
}

/*
 * polarxExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
void
polarxExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;
	char	   *relations;

	fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	/*
	 * Add names of relation handled by the foreign scan when the scan is a
	 * join
	 */
	if (list_length(fdw_private) > FdwScanPrivateRelations)
	{
		relations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));
		ExplainPropertyText("Relations", relations, es);
	}

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * polarxExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
void
polarxExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es)
{
	if (es->verbose)
	{
		char	   *sql = strVal(list_nth(fdw_private,
										  FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * polarxExplainDirectModify
 *		Produce extra output for EXPLAIN of a ForeignScan that modifies a
 *		foreign table directly
 */
void
polarxExplainDirectModify(ForeignScanState *node, ExplainState *es)
{
	List	   *fdw_private;
	char	   *sql;

	if (es->verbose)
	{
		fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, FdwDirectModifyPrivateUpdateSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}


/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost,
						Distribution *distribution)
{
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		cpu_per_tuple;
	LocInfo *locator = fpinfo->rd_locator_info;
	List            *node_list = NIL;

	Assert(list_length(locator->rl_nodeList) > 0);

	if(distribution)
	{
		node_list = GetRelationNodesWithDistribution(distribution, 0, true, RELATION_ACCESS_READ);
	}
	else
		node_list = locator->rl_nodeList;
	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List	   *remote_param_join_conds;
		List	   *local_param_join_conds;
		StringInfoData sql;
		Selectivity local_sel;
		QualCost	local_cost;
		List	   *fdw_scan_tlist = NIL;
		List	   *remote_conds;

		/* Required only to be passed to deparseSelectStmtForRel */
		List	   *retrieved_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classifyConditions(root, foreignrel, param_join_conds,
						   &remote_param_join_conds, &local_param_join_conds);

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(list_copy(remote_param_join_conds),
								   fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
								remote_conds, pathkeys, false,
								&retrieved_attrs, NULL);

		/* Get the remote estimate */
		get_remote_estimate(sql.data, node_list, &rows, &width,
					&startup_cost, &total_cost);

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
										   local_param_join_conds,
										   foreignrel->relid,
										   JOIN_INNER,
										   NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;
	}
	else
	{
		Cost		run_cost = 0;
		int		run_node_num = list_length(node_list);

		Assert(run_node_num > 0);

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations and set_joinrel_size_estimates() for join
		 * between foreign relations.
		 */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/* Back into an estimate of the number of retrieved rows. */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

		/*
		 * We will come here again and again with different set of pathkeys
		 * that caller wants to cost. We don't need to calculate the cost of
		 * bare scan each time. Instead, use the costs if we have cached them
		 * already.
		 */
		if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
		{
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		}
		else if (IS_JOIN_REL(foreignrel))
		{
			PgFdwRelationInfo *fpinfo_i;
			PgFdwRelationInfo *fpinfo_o;
			QualCost	join_cost;
			QualCost	remote_conds_cost;
			double		nrows;
			int		node_num_i = 0;
			int		node_num_o = 0;
			int		num_hashclauses = list_length(fpinfo->hashclauses);

			/* For join we expect inner and outer relations set */
			Assert(fpinfo->innerrel && fpinfo->outerrel);

			fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
			fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
			node_num_i  = IsLocatorReplicated(fpinfo_i->rd_locator_info->locatorType)
				? 1: list_length(fpinfo_i->rd_locator_info->rl_nodeList);
			node_num_o = IsLocatorReplicated(fpinfo_o->rd_locator_info->locatorType)
				? 1: list_length(fpinfo_o->rd_locator_info->rl_nodeList);
			Assert(node_num_i > 0 && node_num_o > 0);

			/* Estimate of number of rows in cross product */
			nrows = clamp_row_est((fpinfo_i->rows/node_num_i) * (fpinfo_o->rows/node_num_o) * run_node_num);
			/* Clamp retrieved rows estimate to at most size of cross product */
			retrieved_rows = Min(retrieved_rows, nrows);

			/*
			 * The cost of foreign join is estimated as cost of generating
			 * rows for the joining relations + cost for applying quals on the
			 * rows.
			 */

			/*
			 * Calculate the cost of clauses pushed down to the foreign server
			 */
			cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
			/* Calculate the cost of applying join clauses */
			cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

			/*
			 * Startup cost includes startup cost of joining relations and the
			 * startup cost for join and other clauses. We do not include the
			 * startup cost specific to join strategy (e.g. setting up hash
			 * tables) since we do not know what strategy the foreign server
			 * is going to use.
			 */
			startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
			startup_cost += join_cost.startup;
			startup_cost += remote_conds_cost.startup;
			startup_cost += fpinfo->local_conds_cost.startup;

			/*
			 * Run time cost includes:
			 *
			 * 1. Run time cost (total_cost - startup_cost) of relations being
			 * joined
			 *
			 * 2. Run time cost of applying join clauses on the cross product
			 * of the joining relations.
			 *
			 * 3. Run time cost of applying pushed down other clauses on the
			 * result of join
			 *
			 * 4. Run time cost of applying nonpushable other clauses locally
			 * on the result fetched from the foreign server.
			 */
			run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
			run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
			run_cost += clamp_row_est(nrows/run_node_num) * join_cost.per_tuple;
			nrows = clamp_row_est((nrows/run_node_num) * fpinfo->joinclause_sel);
			run_cost += nrows * remote_conds_cost.per_tuple;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

			if(num_hashclauses > 0)
			{
				Cost startup_cost_hash = 0.0;
				Cost total_cost_hash = 0.0;
				double retrieved_rows_hash = clamp_row_est(rows / fpinfo->local_conds_sel);

				get_hash_join_cost(root, fpinfo, &startup_cost_hash,
							 &total_cost_hash, run_node_num, retrieved_rows_hash);
				retrieved_rows_hash = Min(retrieved_rows_hash, 
						((fpinfo_i->rows/node_num_i) * (fpinfo_o->rows/node_num_o) * run_node_num));
				if(total_cost_hash < (run_cost + startup_cost))
				{
					startup_cost = startup_cost_hash;
					run_cost = total_cost_hash - startup_cost_hash;
					retrieved_rows = retrieved_rows_hash;
				}
			}
		}
		else if (IS_UPPER_REL(foreignrel))
		{
			PgFdwRelationInfo *ofpinfo;
			PathTarget *ptarget = foreignrel->reltarget;
			AggClauseCosts aggcosts;
			double		input_rows;
			int			numGroupCols;
			double		numGroups = 1;
			int     o_node_num = 0;

			/* Make sure the core code set the pathtarget. */
			Assert(ptarget != NULL);

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 *
			 * Also, core does not care about costing HAVING expressions and
			 * adding that to the costs.  So similarly, here too we are not
			 * considering remote and local conditions for costing.
			 */

			ofpinfo = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
			o_node_num = IsLocatorReplicated(ofpinfo->rd_locator_info->locatorType)
					? 1 : list_length(ofpinfo->rd_locator_info->rl_nodeList);

			/* Get rows and width from input rel */
			input_rows = clamp_row_est(ofpinfo->rows/o_node_num);
			width = ofpinfo->width;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
				get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE, &aggcosts);

				/*
				 * The cost of aggregates in the HAVING qual will be the same
				 * for each child as it is for the parent, so there's no need
				 * to use a translated version of havingQual.
				 */
				get_agg_clause_costs(root, (Node *) root->parse->havingQual,
									 AGGSPLIT_SIMPLE, &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
											get_sortgrouplist_exprs(root->parse->groupClause,
																	fpinfo->grouped_tlist),
											input_rows, NULL);

			/*
			 * Number of rows expected from foreign server will be same as
			 * that of number of groups.
			 */
			rows = retrieved_rows = numGroups * run_node_num;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input relation
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *	  3. Startup cost for PathTarget eval
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;
			startup_cost += ptarget->cost.startup;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *	  3. PathTarget eval cost for each output row
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += aggcosts.finalCost * numGroups;
			run_cost += cpu_tuple_cost * numGroups;
			run_cost += ptarget->cost.per_tuple * numGroups;
		}
		else
		{
			int	node_num = IsLocatorReplicated(locator->locatorType) ? 1 : list_length(locator->rl_nodeList);

			Assert(node_num != 0);
			/* Clamp retrieved rows estimates to at most foreignrel->tuples. */
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);
			retrieved_rows = (retrieved_rows/node_num) * run_node_num;

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * (foreignrel->pages/node_num);

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * (foreignrel->tuples/node_num);
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}

		total_cost = startup_cost + run_cost;
	}

	/*
	 * Cache the costs for scans without any pathkeys or parameterization
	 * before adding the costs for transferring data from the foreign server.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * foreign server. This function will be called at least once for every
	 * foreign relation without pathkeys and parameterization.
	 */
	if (pathkeys == NIL && param_join_conds == NIL)
	{
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	retrieved_rows = clamp_row_est(retrieved_rows);

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * Estimate costs of executing a SQL statement remotely.
 * The given "sql" must be an EXPLAIN command.
 */
static void
get_remote_estimate(const char *sql, List *node_list,
			double *rows, int *width,
			Cost *startup_cost, Cost *total_cost)
{
	int conn_num = 0;
	PGXCNodeAllHandles *all_handles = NULL;
	PGXCNodeHandle **connections = NULL;
	ResponseCombiner combiner;
	int i = 0;
	char       *p;
	int             n;
	double          *rows_arr = NULL;
	int             *width_arr = NULL;
	Cost            *startup_cost_arr = NULL;
	Cost            *total_cost_arr = NULL;
	bool		*has_res = NULL;
	int             total_width = 0;
	int             total_row = 0;
	int             max_cost_inx = -1;
	int		*conn_map = NULL;
	bool need_tran_block = TransactionBlockStatusCode() == 'T';
	
	polarx_scan_begin(true, node_list, need_tran_block);
	all_handles = get_handles(node_list, NIL, false, true);
	connections = all_handles->datanode_handles;
	conn_num = all_handles->dn_conn_count;

	Assert(conn_num > 0);

	rows_arr = palloc0(conn_num * sizeof(double));
	width_arr = palloc0(conn_num * sizeof(int));
	startup_cost_arr = palloc0(conn_num * sizeof(Cost));
	total_cost_arr = palloc0(conn_num * sizeof(Cost));
	has_res =  palloc0(conn_num * sizeof(bool));
	conn_map = palloc0(conn_num * sizeof(int));

	InitResponseCombiner(&combiner, conn_num, COMBINE_TYPE_NONE);

	for(i = 0; i< conn_num; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
		{
			BufferConnection(connections[i]);
		}
		if (pgxc_node_send_query(connections[i], sql) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send explain to Datanode %d", connections[i]->nodeoid)));
		}
		conn_map[i] = i;
	}
	memset(&combiner, 0, sizeof(ScanState));

	while (conn_num > 0)
	{
		if (pgxc_node_receive(conn_num, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to received explain result")));
		i = 0;
		while (i < conn_num)
		{
			int res_type = handle_response(connections[i], &combiner);
			if (res_type == RESPONSE_EOF)
			{
				i++;
			}
			else if (res_type == RESPONSE_READY ||
					connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
			{
				if (--conn_num > i)
				{
					connections[i] = connections[conn_num];
					conn_map[i] = conn_num;
				}
			}
			else if(res_type == RESPONSE_DATAROW)
			{
				p = strstr(&(combiner.currentRow->msg[5]), "(cost=");
				if (p != NULL && !has_res[conn_map[i]])
				{
					n = sscanf(p, "(cost=%lf..%lf rows=%lf width=%d)",
							&startup_cost_arr[conn_map[i]], &total_cost_arr[conn_map[i]], 
							&rows_arr[conn_map[i]], &width_arr[conn_map[i]]);
					if (n == 4)
						has_res[conn_map[i]] = true;
				}
				pfree(combiner.currentRow);
				combiner.currentRow = NULL;

			}
			else if(res_type == RESPONSE_ERROR)
			{
				if (combiner.errorMessage)
					pgxc_node_report_error(&combiner);
			}
		}
	}

	ValidateAndCloseCombiner(&combiner);
	conn_num = all_handles->dn_conn_count;
	pfree_pgxc_all_handles(all_handles);

	for(i = 0; i < conn_num; i++)
	{
		if(has_res[i])
		{
			int row_int = clamp_row_est(rows_arr[i]);

			total_row = total_row + row_int;
			total_width = total_width +  row_int * width_arr[i];

			if(max_cost_inx == -1)
				max_cost_inx = i;
			else if(total_cost_arr[i] > total_cost_arr[max_cost_inx])
				max_cost_inx = i;
		}
		else
			elog(ERROR, "could not interpret EXPLAIN output from node: \"%d\"", i);
	}

	*rows = total_row;
	*width = (int)total_width/total_row;
	*startup_cost = startup_cost_arr[max_cost_inx];
	*total_cost = total_cost_arr[max_cost_inx];

	pfree(rows_arr);
	pfree(width_arr);
	pfree(startup_cost_arr);
	pfree(total_cost_arr);
	pfree(conn_map);
	pfree(has_res);
}

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
	Expr	   *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static PgFdwModifyState *
create_foreign_modify(EState *estate,
					  RangeTblEntry *rte,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  Plan *subplan,
					  char *query,
					  List *target_attrs,
					  bool has_returning,
					  List *retrieved_attrs)
{
	PgFdwModifyState *fmstate;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	ForeignTable *table;
	AttrNumber	n_params;
	Oid			typefnoid;
	bool		isvarlena;
	ListCell   *lc;

	/* Begin constructing PgFdwModifyState. */
	fmstate = (PgFdwModifyState *) palloc0(sizeof(PgFdwModifyState));
	fmstate->rel = rel;

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	fmstate->user = NULL;
	fmstate->part_attr_num = GetRelationPartAttrNum(table);
	fmstate->locator_type = *(GetClusterTableOption(table->options,
				TABLE_OPTION_LOCATOR_TYPE));
	fmstate->total_conn_num = NumDataNodes;

	fmstate->is_prepared = palloc0(fmstate->total_conn_num * sizeof(bool));
	fmstate->conns = (PGconn **) palloc0(fmstate->total_conn_num * sizeof(PGconn *));

	/* Open connection; report that we'll create a prepared statement. */
	//fmstate->conn = GetConnection(user, true);
	fmstate->conn = NULL;

	fmstate->p_name = NULL;		/* prepared statement not made yet */

	/* Set up remote query information. */
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->retrieved_attrs = retrieved_attrs;

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "polarx_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->param_types = (Oid *) palloc0(sizeof(Oid) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Assert(subplan != NULL);

		/* Find the ctid resjunk column in the subplan's result */
		fmstate->ctidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
		fmstate->nodeIndexAttno = ExecFindJunkAttributeInTlist(subplan->targetlist, "node_inx");
		if (!AttributeNumberIsValid(fmstate->ctidAttno))
			elog(ERROR, "could not find junk ctid column");

		/* First transmittable parameter will be ctid */
		getTypeOutputInfo(TIDOID, &typefnoid, &isvarlena);
		fmstate->param_types[fmstate->p_nums] = TIDOID;
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	else
	{
		fmstate->current_node = -1;
		fmstate->nodeIndexAttno = 0;
	}

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			Assert(!attr->attisdropped);

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
			fmstate->param_types[fmstate->p_nums] = attr->atttypid;
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
	}

	Assert(fmstate->p_nums <= n_params);

	/* Initialize auxiliary state */
	fmstate->aux_fmstate = NULL;

	return fmstate;
}

static void
prepare_foreign_modify(PgFdwModifyState *fmstate, List *node_list)
{
	char		*p_name;
	int		conn_num = list_length(node_list);
	int             *conn_map = NULL;

	Assert(conn_num > 0);
	conn_map = palloc0(conn_num * sizeof(int));

    /* Construct name we'll use for the prepared statement. */
	if(!fmstate->p_name && fmstate->query_hash)
        p_name = fmstate->query_hash;
	else
		p_name= fmstate->p_name;

    prepare_foreign_sql(p_name, fmstate->query,
                        node_list, conn_map);

    if(p_name && !fmstate->p_name)
    {
        fmstate->p_name = p_name;
    }
}
/*
 * convert_prep_stmt_params
 *		Create array of text strings representing parameter values
 *
 * tupleid is ctid to send, or NULL if none
 * slot is slot to get remaining parameters from, or NULL if none
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
convert_prep_stmt_params(PgFdwModifyState *fmstate,
						 ItemPointer tupleid,
						 TupleTableSlot *slot)
{
	const char **p_values;
	int			pindex = 0;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	p_values = (const char **) palloc(sizeof(char *) * fmstate->p_nums);

	/* 1st parameter should be ctid, if it's in use */
	if (tupleid != NULL)
	{
		/* don't need set_transmission_modes for TID output */
		p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
											  PointerGetDatum(tupleid));
		pindex++;
	}

	/* get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		int			nestlevel;
		ListCell   *lc;

		nestlevel = set_transmission_modes();

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Datum		value;
			bool		isnull;

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				p_values[pindex] = NULL;
			else
				p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
													  value);
			pindex++;
		}

		reset_transmission_modes(nestlevel);
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}

/*
 * finish_foreign_modify
 *		Release resources for a foreign insert/update/delete operation
 */
static void
finish_foreign_modify(PgFdwModifyState *fmstate)
{
	Assert(fmstate != NULL);

	/* If we created a prepared statement, destroy it */
	if (fmstate->p_name)
	{
		//DropTxnDatanodeStatement(fmstate->p_name);
		fmstate->p_name = NULL;
	}
}

/*
 * build_remote_returning
 *		Build a RETURNING targetlist of a remote query for performing an
 *		UPDATE/DELETE .. RETURNING on a join directly
 */
static List *
build_remote_returning(Index rtindex, Relation rel, List *returningList)
{
	bool		have_wholerow = false;
	List	   *tlist = NIL;
	List	   *vars;
	ListCell   *lc;

	Assert(returningList);

	vars = pull_var_clause((Node *) returningList, PVC_INCLUDE_PLACEHOLDERS);

	/*
	 * If there's a whole-row reference to the target relation, then we'll
	 * need all the columns of the relation.
	 */
	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		if (IsA(var, Var) &&
			var->varno == rtindex &&
			var->varattno == InvalidAttrNumber)
		{
			have_wholerow = true;
			break;
		}
	}

	if (have_wholerow)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			i;

		for (i = 1; i <= tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
			Var		   *var;

			/* Ignore dropped attributes. */
			if (attr->attisdropped)
				continue;

			var = makeVar(rtindex,
						  i,
						  attr->atttypid,
						  attr->atttypmod,
						  attr->attcollation,
						  0);

			tlist = lappend(tlist,
							makeTargetEntry((Expr *) var,
											list_length(tlist) + 1,
											NULL,
											false));
		}
	}

	/* Now add any remaining columns to tlist. */
	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		/*
		 * No need for whole-row references to the target relation.  We don't
		 * need system columns other than ctid and oid either, since those are
		 * set locally.
		 */
		if (IsA(var, Var) &&
			var->varno == rtindex &&
			var->varattno <= InvalidAttrNumber &&
			var->varattno != SelfItemPointerAttributeNumber &&
			var->varattno != ObjectIdAttributeNumber)
			continue;			/* don't need it */

		if (tlist_member((Expr *) var, tlist))
			continue;			/* already got it */

		tlist = lappend(tlist,
						makeTargetEntry((Expr *) var,
										list_length(tlist) + 1,
										NULL,
										false));
	}

	list_free(vars);

	return tlist;
}

/*
 * rebuild_fdw_scan_tlist
 *		Build new fdw_scan_tlist of given foreign-scan plan node from given
 *		tlist
 *
 * There might be columns that the fdw_scan_tlist of the given foreign-scan
 * plan node contains that the given tlist doesn't.  The fdw_scan_tlist would
 * have contained resjunk columns such as 'ctid' of the target relation and
 * 'wholerow' of non-target relations, but the tlist might not contain them,
 * for example.  So, adjust the tlist so it contains all the columns specified
 * in the fdw_scan_tlist; else setrefs.c will get confused.
 */
static void
rebuild_fdw_scan_tlist(ForeignScan *fscan, List *tlist)
{
	List	   *new_tlist = tlist;
	List	   *old_tlist = fscan->fdw_scan_tlist;
	ListCell   *lc;

	foreach(lc, old_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tlist_member(tle->expr, new_tlist))
			continue;			/* already got it */

		new_tlist = lappend(new_tlist,
							makeTargetEntry(tle->expr,
											list_length(new_tlist) + 1,
											NULL,
											false));
	}
	fscan->fdw_scan_tlist = new_tlist;
}

/*
 * Execute a direct UPDATE/DELETE statement.
 */
static TupleTableSlot *
execute_dml_stmt(ForeignScanState *node)
{
	PgFdwDirectModifyState *dmstate = (PgFdwDirectModifyState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int			numParams = dmstate->numParams;
	const char **values = dmstate->param_values;
	int i = 0;
	ResponseCombiner *combiner = &(dmstate->combiner);
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	EState	   *estate = node->ss.ps.state;
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	CommandId    cid = GetCurrentCommandId(false);
    RemoteDataRow datarow = NULL;
    char *prep_name;


	Assert(list_length(dmstate->node_list) > 0);

    GetPrepStmtNameAndSelfVersion(dmstate->query_hash, &prep_name);
	if(dmstate->num_tuples == -1)
	{
		PGXCNodeAllHandles *all_handles;
		PGXCNodeHandle      **connections;
		int                 conn_count;
		StringInfoData buf;
		uint16 n16;
		MemoryContext oldcontext;

		if (numParams > 0)
			process_query_params(econtext,
					dmstate->param_flinfo,
					dmstate->param_exprs,
					values);
		/* get needed Datanode connections */
		oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
		all_handles = get_handles(dmstate->node_list, NIL, false, true);
		MemoryContextSwitchTo(oldcontext);
		conn_count = all_handles->dn_conn_count;
		connections = all_handles->datanode_handles;

		initStringInfo(&buf);
		n16 = htons(numParams);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);

		for (i = 0; i < numParams; i++)
		{
			uint32 n32;
			if (values && values[i])
			{
				int     len;

				len  = strlen(values[i]);
				n32 = htonl(len);
				appendBinaryStringInfo(&buf, (char *) &n32, 4);
				appendBinaryStringInfo(&buf, values[i], len);
			}
			else 
			{
				n32 = htonl(-1);
				appendBinaryStringInfo(&buf, (char *) &n32, 4);
			}
		}

        prepare_foreign_sql(dmstate->query_hash, dmstate->query,
                dmstate->node_list, NULL);
		for (i = 0; i < conn_count; i++)
		{
			if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(connections[i]);
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
			if (pgxc_node_send_cmd_id(connections[i], cid) < 0 )
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command ID to Datanodes")));
#endif
			if (pgxc_node_send_query_extended(connections[i],
						NULL,
						prep_name,
                       // NULL,
                        NULL,
						numParams,
						dmstate->param_types,
						buf.len,
						buf.data,
						false,
						0) != 0)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send prepared statement: %s", dmstate->query)));
			}
			connections[i]->combiner = NULL;
		}

		InitResponseCombiner(combiner, conn_count, COMBINE_TYPE_NONE);
		memset(combiner, 0, sizeof(ScanState));

		combiner->request_type = REQUEST_TYPE_QUERY;
		combiner->ss.ss_ScanTupleSlot = slot;
		combiner->recv_node_count = conn_count;
		combiner->recv_tuples      = 0;
		combiner->recv_total_time = -1;
		combiner->recv_datarows = 0;
		combiner->node_count = conn_count;
		combiner->DML_processed = 0;
		combiner->extended_query = true;
        combiner->cursor = NULL;
		if(IsLocatorReplicated(dmstate->locator_type))
			combiner->combine_type = COMBINE_TYPE_SAME;
		else
			combiner->combine_type = COMBINE_TYPE_SUM;

		if (conn_count > 0)
		{
			combiner->connections = connections;
			combiner->conn_count = conn_count;
			combiner->current_conn = 0;
		}
		if(dmstate->num_tuples == -1)
			dmstate->num_tuples = 0;

		if (!resultRelInfo->ri_projectReturning)
		{
			Instrumentation *instr = node->ss.ps.instrument;

			Assert(!dmstate->has_returning);

			while(true)
			{
                RemoteDataRow datarow_tmp = FetchDatarow(combiner);
				if (datarow_tmp == NULL)
					break;
                pfree(datarow_tmp);
			}
			if (combiner->errorMessage)
				pgxc_node_report_error(combiner);

			slot = node->ss.ss_ScanTupleSlot;
			if(combiner->combine_type == COMBINE_TYPE_SAME)
				dmstate->num_tuples += combiner->DML_processed / conn_count;
			else
				dmstate->num_tuples += combiner->DML_processed;

			/* Increment the command es_processed count if necessary. */
			if (dmstate->set_processed)
				estate->es_processed = dmstate->num_tuples;
			else
				estate->es_processed = 0;

			/* Increment the tuple count for EXPLAIN ANALYZE if necessary. */
			if (instr)
			{
				instr->tuplecount += dmstate->num_tuples;
			}

			CloseCombiner(combiner);
			return ExecClearTuple(slot);
		}
	}

	Assert(resultRelInfo->ri_projectReturning);

	datarow = FetchDatarow(combiner);

	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	if(datarow == NULL)
	{
		if(combiner->combine_type == COMBINE_TYPE_SAME)
			dmstate->num_tuples = combiner->DML_processed / combiner->node_count;
		else
			dmstate->num_tuples = combiner->DML_processed;

		if (dmstate->set_processed)
			estate->es_processed = dmstate->num_tuples;
		else
			estate->es_processed = 0;

		CloseCombiner(combiner);
		return ExecClearTuple(node->ss.ss_ScanTupleSlot);
	}
	else
	{
		TupleTableSlot *resultSlot;
		HeapTuple	newtup;

		newtup = make_tuple_from_datarow(datarow->msg,
				dmstate->rel,
				dmstate->attinmeta,
				dmstate->retrieved_attrs,
				node,
				dmstate->temp_cxt, false);
		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreTuple(newtup, slot, InvalidBuffer, false);
		if (dmstate->rel)
			resultSlot = slot;
		else
			resultSlot = apply_returning_filter(dmstate, slot, estate);
		resultRelInfo->ri_projectReturning->pi_exprContext->ecxt_scantuple = resultSlot;
        pfree(datarow);

		return slot;
	}
}

/*
 * Initialize a filter to extract an updated/deleted tuple from a scan tuple.
 */
static void
init_returning_filter(PgFdwDirectModifyState *dmstate,
					  List *fdw_scan_tlist,
					  Index rtindex)
{
	TupleDesc	resultTupType = RelationGetDescr(dmstate->resultRel);
	ListCell   *lc;
	int			i;

	/*
	 * Calculate the mapping between the fdw_scan_tlist's entries and the
	 * result tuple's attributes.
	 *
	 * The "map" is an array of indexes of the result tuple's attributes in
	 * fdw_scan_tlist, i.e., one entry for every attribute of the result
	 * tuple.  We store zero for any attributes that don't have the
	 * corresponding entries in that list, marking that a NULL is needed in
	 * the result tuple.
	 *
	 * Also get the indexes of the entries for ctid and oid if any.
	 */
	dmstate->attnoMap = (AttrNumber *)
		palloc0(resultTupType->natts * sizeof(AttrNumber));

	dmstate->ctidAttno = dmstate->oidAttno = 0;

	i = 1;
	dmstate->hasSystemCols = false;
	foreach(lc, fdw_scan_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Var		   *var = (Var *) tle->expr;

		Assert(IsA(var, Var));

		/*
		 * If the Var is a column of the target relation to be retrieved from
		 * the foreign server, get the index of the entry.
		 */
		if (var->varno == rtindex &&
			list_member_int(dmstate->retrieved_attrs, i))
		{
			int			attrno = var->varattno;

			if (attrno < 0)
			{
				/*
				 * We don't retrieve system columns other than ctid and oid.
				 */
				if (attrno == SelfItemPointerAttributeNumber)
					dmstate->ctidAttno = i;
				else if (attrno == ObjectIdAttributeNumber)
					dmstate->oidAttno = i;
				else
					Assert(false);
				dmstate->hasSystemCols = true;
			}
			else
			{
				/*
				 * We don't retrieve whole-row references to the target
				 * relation either.
				 */
				Assert(attrno > 0);

				dmstate->attnoMap[attrno - 1] = i;
			}
		}
		i++;
	}
}

/*
 * Extract and return an updated/deleted tuple from a scan tuple.
 */
static TupleTableSlot *
apply_returning_filter(PgFdwDirectModifyState *dmstate,
					   TupleTableSlot *slot,
					   EState *estate)
{
	TupleDesc	resultTupType = RelationGetDescr(dmstate->resultRel);
	TupleTableSlot *resultSlot;
	Datum	   *values;
	bool	   *isnull;
	Datum	   *old_values;
	bool	   *old_isnull;
	int			i;

	/*
	 * Use the trigger tuple slot as a place to store the result tuple.
	 */
	resultSlot = estate->es_trig_tuple_slot;
	if (resultSlot->tts_tupleDescriptor != resultTupType)
		ExecSetSlotDescriptor(resultSlot, resultTupType);

	/*
	 * Extract all the values of the scan tuple.
	 */
	slot_getallattrs(slot);
	old_values = slot->tts_values;
	old_isnull = slot->tts_isnull;

	/*
	 * Prepare to build the result tuple.
	 */
	ExecClearTuple(resultSlot);
	values = resultSlot->tts_values;
	isnull = resultSlot->tts_isnull;

	/*
	 * Transpose data into proper fields of the result tuple.
	 */
	for (i = 0; i < resultTupType->natts; i++)
	{
		int			j = dmstate->attnoMap[i];

		if (j == 0)
		{
			values[i] = (Datum) 0;
			isnull[i] = true;
		}
		else
		{
			values[i] = old_values[j - 1];
			isnull[i] = old_isnull[j - 1];
		}
	}

	/*
	 * Build the virtual tuple.
	 */
	ExecStoreVirtualTuple(resultSlot);

	/*
	 * If we have any system columns to return, install them.
	 */
	if (dmstate->hasSystemCols)
	{
		HeapTuple	resultTup = ExecMaterializeSlot(resultSlot);

		/* ctid */
		if (dmstate->ctidAttno)
		{
			ItemPointer ctid = NULL;

			ctid = (ItemPointer) DatumGetPointer(old_values[dmstate->ctidAttno - 1]);
			resultTup->t_self = *ctid;
		}

		/* oid */
		if (dmstate->oidAttno)
		{
			Oid			oid = InvalidOid;

			oid = DatumGetObjectId(old_values[dmstate->oidAttno - 1]);
			HeapTupleSetOid(resultTup, oid);
		}

		/*
		 * And remaining columns
		 *
		 * Note: since we currently don't allow the target relation to appear
		 * on the nullable side of an outer join, any system columns wouldn't
		 * go to NULL.
		 *
		 * Note: no need to care about tableoid here because it will be
		 * initialized in ExecProcessReturning().
		 */
		HeapTupleHeaderSetXmin(resultTup->t_data, InvalidTransactionId);
		HeapTupleHeaderSetXmax(resultTup->t_data, InvalidTransactionId);
		HeapTupleHeaderSetCmin(resultTup->t_data, InvalidTransactionId);
	}

	/*
	 * And return the result tuple.
	 */
	return resultSlot;
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 int numParams,
					 FmgrInfo **param_flinfo,
					 List **param_exprs,
					 const char ***param_values,
					 Oid **param_types)
{
	int			i;
	ListCell   *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);
	*param_types = (Oid *)palloc0(sizeof(Oid) * numParams);

	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		(*param_types)[i] = exprType(param_expr);
		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require polarx_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
					 FmgrInfo *param_flinfo,
					 List *param_exprs,
					 const char **param_values)
{
	int			nestlevel;
	int			i;
	ListCell   *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);

		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * polarxAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
bool
polarxAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages)
{
	StringInfoData sql;
	List *node_list = NIL;
	int i = 0;
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle      **connections;
	ResponseCombiner    combiner;
	int                 conn_num;

	/* Return the row-analysis function pointer */
	*func = polarxAcquireSampleRowsFunc;

	node_list = GetRelationNodesWithRelation(relation, 0, true, RELATION_ACCESS_READ, NumDataNodes);

	if (list_length(node_list) == 0)
                return false;

	initStringInfo(&sql);
	deparseAnalyzeSizeSql(&sql, relation);

	all_handles = get_handles(node_list, NIL, false, true);
        conn_num = all_handles->dn_conn_count;
        connections = all_handles->datanode_handles;

	InitResponseCombiner(&combiner, conn_num, COMBINE_TYPE_NONE);

	for(i = 0; i< conn_num; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
		{
			BufferConnection(connections[i]);
		}
		if (pgxc_node_send_query(connections[i], sql.data) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send Analyze to Datanode %d", connections[i]->nodeoid)));
		}
	}
	memset(&combiner, 0, sizeof(ScanState));

	while (conn_num > 0)
	{
		if (pgxc_node_receive(conn_num, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to received Analyze result")));
		i = 0;
		while (i < conn_num)
		{
			int res_type = handle_response(connections[i], &combiner);
			if (res_type == RESPONSE_EOF)
			{
				i++;
			}
			else if (res_type == RESPONSE_READY ||
					connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
			{
				if (--conn_num > i)
				{
					connections[i] = connections[conn_num];
				}
			}
			else if(res_type == RESPONSE_DATAROW)
			{
				int n = 0;
				int row_num = 0;

				n = sscanf(&(combiner.currentRow->msg[6]), "%d", &row_num);
				if (row_num < 0 || n != 1)	
					elog(ERROR, "could not interpret Analyze output from node: \"%d\", output is: %s", 
							connections[i]->nodeoid, &(combiner.currentRow->msg[6]));	
				*totalpages = *totalpages + row_num;
				pfree(combiner.currentRow);
				combiner.currentRow = NULL;

			}
		}
	}

	ValidateAndCloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);

	return true;
}

/*
 * Acquire a random sample of rows from foreign table managed by polarx_fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
polarxAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows,
							  double *totaldeadrows)
{
	PgFdwAnalyzeState astate;
	StringInfoData sql;
	List *node_list = NIL;
	int conn_num = 0;
	int i = 0;
	PGXCNodeHandle **connections = NULL;
	PGXCNodeAllHandles *all_handles;
	ResponseCombiner    combiner;

	/* Initialize workspace state */
	astate.rel = relation;
	astate.attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(relation));

	astate.rows = rows;
	astate.targrows = targrows;
	astate.numrows = 0;
	astate.samplerows = 0;
	astate.rowstoskip = -1;		/* -1 means not set yet */
	reservoir_init_selection_state(&astate.rstate, targrows);

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
											"polarx_fdw temporary data",
											ALLOCSET_SMALL_SIZES);

	node_list = GetRelationNodesWithRelation(relation, 0, true, RELATION_ACCESS_READ, NumDataNodes);

	all_handles = get_handles(node_list, NIL, false, true);
	conn_num = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	if (pgxc_node_begin(conn_num, connections, 0, true,
				true, PGXC_NODE_DATANODE))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not begin transaction on data node")));

	InitResponseCombiner(&combiner, conn_num, COMBINE_TYPE_NONE);
	memset(&combiner, 0, sizeof(ScanState));
	combiner.request_type = REQUEST_TYPE_QUERY;
	combiner.ss.ss_ScanTupleSlot = MakeTupleTableSlot(RelationGetDescr(relation));
	combiner.recv_node_count = conn_num;
	combiner.recv_tuples      = 0;
	combiner.recv_total_time = -1;
	combiner.recv_datarows = 0;
	combiner.node_count = conn_num;

	if (conn_num > 0)
	{
		combiner.connections = connections;
		combiner.conn_count = conn_num;
		combiner.current_conn = 0;
	}	

	initStringInfo(&sql);
	deparseAnalyzeSql(&sql, relation, &astate.retrieved_attrs);

	for(i = 0; i< conn_num; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
		{
			BufferConnection(connections[i]);
		}
		if (pgxc_node_send_query(connections[i], sql.data) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send Analyze to Datanode %d", connections[i]->nodeoid)));
		}
		connections[i]->combiner = &combiner;
	}

	while(true)
	{
        RemoteDataRow datarow = FetchDatarow(&combiner);
		if (datarow == NULL)
			break;
		analyze_row_processor(datarow, &astate);
        pfree(datarow);
	}

	ExecClearTuple(combiner.ss.ss_ScanTupleSlot);
	ReleaseTupleDesc(RelationGetDescr(relation));
	if (combiner.errorMessage)
	{
		char *tmp_errmsg = pstrdup(combiner.errorMessage);

		pgxc_connections_cleanup(&combiner);
		combiner.errorMessage = tmp_errmsg;
		pgxc_node_report_error(&combiner);
	}

	CloseCombiner(&combiner);	
	pfree_pgxc_all_handles(all_handles);

	/* We assume that we have no dead tuple. */
	*totaldeadrows = 0.0;

	/* We've retrieved all living tuples from foreign server. */
	*totalrows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
					RelationGetRelationName(relation),
					astate.samplerows, astate.numrows)));

	return astate.numrows;
}

/*
 * Collect sample rows from the result of query.
 *	 - Use all tuples in sample until target # of samples are collected.
 *	 - Subsequently, replace already-sampled tuples randomly.
 */
static void
analyze_row_processor(RemoteDataRow datarow, PgFdwAnalyzeState *astate)
{
	int			targrows = astate->targrows;
	int			pos;			/* array index to store tuple in */
	MemoryContext oldcontext;

	/* Always increment sample row counter. */
	astate->samplerows += 1;

	/*
	 * Determine the slot where this sample row should be stored.  Set pos to
	 * negative value to indicate the row should be skipped.
	 */
	if (astate->numrows < targrows)
	{
		/* First targrows rows are always included into the sample */
		pos = astate->numrows++;
	}
	else
	{
		/*
		 * Now we start replacing tuples in the sample until we reach the end
		 * of the relation.  Same algorithm as in acquire_sample_rows in
		 * analyze.c; see Jeff Vitter's paper.
		 */
		if (astate->rowstoskip < 0)
			astate->rowstoskip = reservoir_get_next_S(&astate->rstate, astate->samplerows, targrows);

		if (astate->rowstoskip <= 0)
		{
			/* Choose a random reservoir element to replace. */
			pos = (int) (targrows * sampler_random_fract(astate->rstate.randstate));
			Assert(pos >= 0 && pos < targrows);
			heap_freetuple(astate->rows[pos]);
		}
		else
		{
			/* Skip this tuple. */
			pos = -1;
		}

		astate->rowstoskip -= 1;
	}

	if (pos >= 0)
	{
		/*
		 * Create sample tuple from current result row, and store it in the
		 * position determined above.  The tuple has to be created in anl_cxt.
		 */
		oldcontext = MemoryContextSwitchTo(astate->anl_cxt);

		astate->rows[pos] = make_tuple_from_datarow(datarow->msg, 
													   astate->rel,
													   astate->attinmeta,
													   astate->retrieved_attrs,
													   NULL,
													   astate->temp_cxt, true);

		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to PgFdwRelationInfo passed in.
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
				RelOptInfo *outerrel, RelOptInfo *innerrel,
				JoinPathExtraData *extra)
{
	PgFdwRelationInfo *fpinfo;
	PgFdwRelationInfo *fpinfo_o;
	PgFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;
	List	   *hashclauses;

	/*
	 * We support pushing down INNER, LEFT, RIGHT and FULL OUTER joins.
	 * Constructing queries representing SEMI and ANTI joins is hard, hence
	 * not considered right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = (PgFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (PgFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (PgFdwRelationInfo *) innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	if (root->parse->commandType == CMD_DELETE ||
			root->parse->commandType == CMD_UPDATE ||
			root->rowMarks)
	{
		if(fpinfo_o->rd_locator_info->locatorType
				!= fpinfo_i->rd_locator_info->locatorType)
		{
			if((IsLocatorReplicated(fpinfo_o->rd_locator_info->locatorType)
						&& reloptinfo_contain_targetrel_inx(root->parse->resultRelation, outerrel))
					|| (IsLocatorReplicated(fpinfo_i->rd_locator_info->locatorType)
						&& reloptinfo_contain_targetrel_inx(root->parse->resultRelation, innerrel)))
				return false;
		}
	}

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Merge FDW options.  We might be tempted to do this after we have deemed
	 * the foreign join to be OK.  But we must do this beforehand so that we
	 * know which quals can be evaluated on the foreign server, which might
	 * depend on shippable_extensions.
	 */
	fpinfo->server = fpinfo_o->server;
	merge_fdw_options(fpinfo, fpinfo_o, fpinfo_i);

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	hashclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = is_foreign_expr(root, joinrel,
													   rinfo->clause);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);

			if (!rinfo->can_join ||
					rinfo->hashjoinoperator == InvalidOid)
				continue;

			if (bms_is_subset(rinfo->left_relids, outerrel->relids) &&
					bms_is_subset(rinfo->right_relids, innerrel->relids))
			{
				/* lefthand side is outer */
				rinfo->outer_is_left = true;
				hashclauses = lappend(hashclauses, rinfo);
			}
			else if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
					bms_is_subset(rinfo->right_relids, outerrel->relids))
			{
				/* righthand side is outer */
				rinfo->outer_is_left = false;
				hashclauses = lappend(hashclauses, rinfo);
			}
		}
		else
		{
			if (is_remote_clause)
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * deparseExplicitTargetList() isn't smart enough to handle anything other
	 * than a Var.  In particular, if there's some PlaceHolderVar that would
	 * need to be evaluated within this join tree (because there's an upper
	 * reference to a quantity that may go to NULL as a result of an outer
	 * join), then we can't try to push the join down because we'll fail when
	 * we get to deparseExplicitTargetList().  However, a PlaceHolderVar that
	 * needs to be evaluated *at the top* of this join tree is OK, because we
	 * can do that locally after fetching the results from the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;
	fpinfo->hashclauses = hashclauses;

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
	fpinfo->lower_subquery_rels = bms_union(fpinfo_o->lower_subquery_rels,
											fpinfo_i->lower_subquery_rels);

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
	 * the outer side are added to remote_conds since those can be evaluated
	 * after the join is evaluated. The clauses from inner side are added to
	 * the joinclauses, since they need to be evaluated while constructing the
	 * join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   list_copy(fpinfo_o->remote_conds));
			break;

		case JOIN_LEFT:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
											  list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   list_copy(fpinfo_o->remote_conds));
			break;

		case JOIN_RIGHT:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
											  list_copy(fpinfo_o->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   list_copy(fpinfo_i->remote_conds));
			break;

		case JOIN_FULL:

			/*
			 * In this case, if any of the input relations has conditions, we
			 * need to deparse that relation as a subquery so that the
			 * conditions can be evaluated before the join.  Remember it in
			 * the fpinfo of this relation so that the deparser can take
			 * appropriate action.  Also, save the relids of base relations
			 * covered by that relation for later use by the deparser.
			 */
			if (fpinfo_o->remote_conds)
			{
				fpinfo->make_outerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									outerrel->relids);
			}
			if (fpinfo_i->remote_conds)
			{
				fpinfo->make_innerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels,
									innerrel->relids);
			}
			break;

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, all restrictions can be treated alike. Treating the
	 * pushed down conditions as join conditions allows a top level full outer
	 * join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		Assert(!fpinfo->joinclauses);
		fpinfo->joinclauses = fpinfo->remote_conds;
		fpinfo->remote_conds = NIL;
	}

	if(!joinrelation_build_locator(joinrel, jointype, outerrel, innerrel))
	{
		return false;
	}
	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/* Get user mapping */
	if (fpinfo->use_remote_estimate)
	{
		if (fpinfo_o->use_remote_estimate)
			fpinfo->user = fpinfo_o->user;
		else
			fpinfo->user = fpinfo_i->user;
	}
	else
		fpinfo->user = NULL;

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs, during one (usually the
	 * first) of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "(%s) %s JOIN (%s)",
					 fpinfo_o->relation_name->data,
					 get_jointype_name(fpinfo->jointype),
					 fpinfo_i->relation_name->data);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fpinfo->relation_index == 0);	/* shouldn't be set yet */
	fpinfo->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								Path *epq_path)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;
	PgFdwRelationInfo *fpinfo =  (PgFdwRelationInfo *) rel->fdw_private;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;
		Distribution *distribution = palloc0(sizeof(Distribution));

		if(pathkeys_contain_const(useful_pathkeys, rel))
			continue;
		set_path_distribution(fpinfo->rd_locator_info, distribution);
		if(fpinfo->remote_conds)
		{
			ListCell *rmlc;

			foreach (rmlc, fpinfo->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(rmlc);
				restrict_distribution(root, ri, distribution);
			}
		}



		estimate_path_cost_size(root, rel, NIL, useful_pathkeys,
								&rows, &width, &startup_cost, &total_cost,
								distribution);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		add_path(rel, (Path *)
				 create_foreignscan_path(root, rel,
										 NULL,
										 rows,
										 startup_cost,
										 total_cost,
										 useful_pathkeys,
										 rel->lateral_relids,
										 sorted_epq_path,
										 list_make1(distribution)));
	}
}

/*
 * Parse options from foreign server and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_server_options(PgFdwRelationInfo *fpinfo)
{
	ListCell   *lc;

	foreach(lc, fpinfo->server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fdw_startup_cost") == 0)
			fpinfo->fdw_startup_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
			fpinfo->fdw_tuple_cost = strtod(defGetString(def), NULL);
		else if (strcmp(def->defname, "extensions") == 0)
			fpinfo->shippable_extensions =
				ExtractExtensionList(defGetString(def), false);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(PgFdwRelationInfo *fpinfo)
{
	ListCell   *lc;

	foreach(lc, fpinfo->table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			fpinfo->use_remote_estimate = defGetBoolean(def);
		else if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

/*
 * compute_modulo
 *    Computes modulo of two 64-bit unsigned values.
 */
static int
compute_modulo(uint64 numerator, uint64 denominator)
{
	Assert(denominator > 0);

	return numerator % denominator;
}

/*
 * Parse options from foreign table and foreign table apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
relation_build_locator(RelOptInfo *baserel, Oid foreigntableid)
{
	LocInfo	*relationLocInfo;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) baserel->fdw_private;
	int		j;

	relationLocInfo = (LocInfo *) palloc0(sizeof(LocInfo));
	fpinfo->rd_locator_info = relationLocInfo;

	relationLocInfo->locatorType = *(GetClusterTableOption(fpinfo->table->options, 
				TABLE_OPTION_LOCATOR_TYPE));
	if(IsLocatorDistributedByValue(relationLocInfo->locatorType))
	{
		char *dist_col_name = GetClusterTableOption(fpinfo->table->options, TABLE_OPTION_PART_ATTR_NAME);
		if(dist_col_name == NULL)
			elog(ERROR, "table: %u, dist_col_name is NULL", fpinfo->table->relid);
		
		relationLocInfo->partAttrNum = get_attnum(fpinfo->table->relid, dist_col_name);
		if(relationLocInfo->partAttrNum == 0)
			elog(ERROR, "table: %u, dist_col_name %s is not exist", fpinfo->table->relid, dist_col_name);
	}
	if(relationLocInfo->partAttrNum)
	{
		Var		*var = NULL;
		ListCell        *lc;

		/* Look if the Var is already in the target list */
		foreach (lc, baserel->reltarget->exprs)
		{
			var = (Var *) lfirst(lc);
			if (IsA(var, Var) && var->varno == baserel->relid &&
					var->varattno == relationLocInfo->partAttrNum)
				break;
		}
		/* If not found we should look up the attribute and make the Var */
		if (!lc)
		{
			Relation 	relation = heap_open(foreigntableid, NoLock);
			TupleDesc	tdesc = RelationGetDescr(relation);
			FormData_pg_attribute att_tup;

			att_tup = tdesc->attrs[relationLocInfo->partAttrNum - 1];
			var = makeVar(baserel->relid, relationLocInfo->partAttrNum,
					att_tup.atttypid, att_tup.atttypmod,
					att_tup.attcollation, 0);


			heap_close(relation, NoLock);
		}

		relationLocInfo->distributionExpr = (Node *) var;
	}
	relationLocInfo->relid = foreigntableid;
	relationLocInfo->partAttrName = get_attname(relationLocInfo->relid, relationLocInfo->partAttrNum, true);
	relationLocInfo->rl_nodeList = NIL;
	for (j = 0; j < NumDataNodes; j++)
		relationLocInfo->rl_nodeList = lappend_int(relationLocInfo->rl_nodeList, j);

	if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN
			|| relationLocInfo->locatorType == LOCATOR_TYPE_REPLICATED)
	{
		int offset;
		/*
		 * pick a random one to start with,
		 * since each process will do this independently
		 */
		offset = compute_modulo(abs(rand()), list_length(relationLocInfo->rl_nodeList));

		relationLocInfo->roundRobinNode = relationLocInfo->rl_nodeList->head; /* initialize */
		for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
			relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
	}
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(PgFdwRelationInfo *fpinfo,
				  const PgFdwRelationInfo *fpinfo_o,
				  const PgFdwRelationInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
			fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);
	}
}

/*
 * polarxGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
void
polarxGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	PgFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *epq_path;		/* Path to create plan to be executed when
								 * EvalPlanQual gets triggered. */
	Distribution *distribution = NULL;

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;

	/*
	 * Create unfinished PgFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (PgFdwRelationInfo *) palloc0(sizeof(PgFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	/*
	 * If there is a possibility that EvalPlanQual will be executed, we need
	 * to be able to reconstruct the row using scans of the base relations.
	 * GetExistingLocalJoinPath will find a suitable path for this purpose in
	 * the path list of the joinrel, if one exists.  We must be careful to
	 * call it before adding any ForeignPath, since the ForeignPath might
	 * dominate the only suitable local path available.  We also do it before
	 * calling foreign_join_ok(), since that function updates fpinfo and marks
	 * it as pushable if the join is found to be pushable.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE ||
		root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	distribution = palloc0(sizeof(Distribution));

	set_path_distribution(fpinfo->rd_locator_info, distribution);
	if(fpinfo->remote_conds)
	{
		ListCell *rmlc;	

		foreach (rmlc, fpinfo->remote_conds)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(rmlc);
			restrict_distribution(root, ri, distribution);
		}
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path. The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 * The local conditions are applied after the join has been computed on
	 * the remote side like quals in WHERE clause, so pass jointype as
	 * JOIN_INNER.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);
	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * If we are going to estimate costs locally, estimate the join clause
	 * selectivity here while we have special join info.
	 */
	if (!fpinfo->use_remote_estimate)
		fpinfo->joinclause_sel = clauselist_selectivity(root, fpinfo->joinclauses,
														0, fpinfo->jointype,
														extra->sjinfo);

	/* Estimate costs for bare join relation */
	estimate_path_cost_size(root, joinrel, NIL, NIL, &rows,
							&width, &startup_cost, &total_cost, distribution);
	/* Now update this information in the joinrel */
	joinrel->rows = rows;
	joinrel->reltarget->width = width;
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
	joinpath = create_foreignscan_path(root,
									   joinrel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   joinrel->lateral_relids,
									   epq_path,
									   list_make1(distribution));	/* no fdw_private */

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	/* Consider pathkeys for the join relation */
	add_paths_with_pathkeys_for_rel(root, joinrel, epq_path);

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * polarxGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
void
polarxGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra)
{
}

static HeapTuple
make_tuple_from_datarow(char *msg,
						   Relation rel,
						   AttInMetadata *attinmeta,
						   List *retrieved_attrs,
						   ForeignScanState *fsstate,
						   MemoryContext temp_context,
                           bool is_scan)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	ItemPointer ctid = NULL;
	Oid			oid = InvalidOid;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	ListCell   *lc;
	int         col_count;
	char       *cur = msg;
	StringInfo  buffer;
	uint16        n16;
	uint32        n32;
	PgFdwScanState *fdwsstate = (is_scan && fsstate != NULL) ? (PgFdwScanState *) fsstate->fdw_state : NULL;
	ForeignScan *fsplan = fsstate != NULL ? (ForeignScan *) fsstate->ss.ps.plan : NULL;

	memcpy(&n16, cur, 2);
	cur += 2;
	col_count = ntohs(n16);
	if (col_count != list_length(retrieved_attrs))
	{
		if(col_count == 1 && list_length(retrieved_attrs) == 0) 
		{
			int len;

			/* get size */
			memcpy(&n32, cur, 4);
			cur += 4;
			len = ntohl(n32);
			if(len != -1)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("Tuple does not match the retrieved_attrs")));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Tuple does not match the retrieved_attrs")));
	}
	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	if (rel)
		tupdesc = RelationGetDescr(rel);
	else
	{
		Assert(fsstate);
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
		if(fdwsstate && fdwsstate->have_wholerow > 0)
		{
			if(fdwsstate->have_wholerow > tupdesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("Tuple description does not match whole row")));
			if(fdwsstate->wholerow_first == true)
			{
				oldcontext = MemoryContextSwitchTo(fsstate->ss.ss_ScanTupleSlot->tts_mcxt);
				fdwsstate->tupdesc_wlr = ExecCleanTypeFromTL(fsplan->fdw_scan_tlist,
						fdwsstate->has_oid);
				fdwsstate->tupdesc_wlr = BlessTupleDesc(fdwsstate->tupdesc_wlr);
				fdwsstate->wholerow_first = false;
				MemoryContextSwitchTo(oldcontext);
			}
		}
	}

	oldcontext = MemoryContextSwitchTo(temp_context);
	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));
	buffer = makeStringInfo();

	/*
	 * Set up and install callback to report where conversion error occurs.
	 */
	errpos.rel = rel;
	errpos.cur_attno = 0;
	errpos.fsstate = fsstate;
	errcallback.callback = conversion_error_callback;
	errcallback.arg = (void *) &errpos;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * i indexes columns in the relation, j indexes columns in the PGresult.
	 */
	foreach(lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		int len;

		/* get size */
		memcpy(&n32, cur, 4);
		cur += 4;
		len = ntohl(n32);

		if (len == -1)
		{
			if(i > 0)
			{
				Assert(i <= tupdesc->natts);
				nulls[i - 1] = true;
				values[i - 1] = (Datum) 0;
				continue;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("system columns can not be NULL")));
		}
		else if(len == -2)
		{
			/* to do */	
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("composite type is not support")));
		}
		else
		{
			appendBinaryStringInfo(buffer, cur, len);
			cur += len;
		}

		/*
		 * convert value to internal representation
		 *
		 * Note: we ignore system columns other than ctid and oid in result
		 */
		errpos.cur_attno = i;
		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			nulls[i - 1] = false;
			/* Apply the input function even to nulls, to support domains */
			values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
					buffer->data,
					attinmeta->attioparams[i - 1],
					attinmeta->atttypmods[i - 1]);
			resetStringInfo(buffer);
		}
		else if (i == SelfItemPointerAttributeNumber)
		{
			/* ctid */
			if (strlen(buffer->data) != 0)
			{
				Datum		datum;

				datum = DirectFunctionCall1(tidin, CStringGetDatum(buffer->data));
				ctid = (ItemPointer) DatumGetPointer(datum);
				resetStringInfo(buffer);
			}
		}
		else if (i == ObjectIdAttributeNumber)
		{
			/* oid */
			if (strlen(buffer->data) != 0)
			{
				Datum		datum;

				datum = DirectFunctionCall1(oidin, CStringGetDatum(buffer->data));
				oid = DatumGetObjectId(datum);
				resetStringInfo(buffer);
			}
		}
		errpos.cur_attno = 0;
	}

	/* Uninstall error context callback. */
	error_context_stack = errcallback.previous;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	if(fdwsstate && fdwsstate->have_wholerow > 0 && fdwsstate->wholerow_first == false)
	{
		tuple = toast_build_flattened_tuple(fdwsstate->tupdesc_wlr,
				values,
				nulls);
		values[fdwsstate->have_wholerow -1] =  PointerGetDatum(tuple->t_data);
		nulls[fdwsstate->have_wholerow -1] = false;
		HeapTupleHeaderSetTypeId(tuple->t_data, fdwsstate->tupdesc_wlr->tdtypeid);
		HeapTupleHeaderSetTypMod(tuple->t_data, fdwsstate->tupdesc_wlr->tdtypmod);
	}
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/*
	 * If we have an OID to return, install it.
	 */
	if (OidIsValid(oid))
		HeapTupleSetOid(tuple, oid);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}
/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 */
static void
conversion_error_callback(void *arg)
{
	const char *attname = NULL;
	const char *relname = NULL;
	bool		is_wholerow = false;
	ConversionLocation *errpos = (ConversionLocation *) arg;

	if (errpos->rel)
	{
		/* error occurred in a scan against a foreign table */
		TupleDesc	tupdesc = RelationGetDescr(errpos->rel);
		Form_pg_attribute attr = TupleDescAttr(tupdesc, errpos->cur_attno - 1);

		if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
			attname = NameStr(attr->attname);
		else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
			attname = "ctid";
		else if (errpos->cur_attno == ObjectIdAttributeNumber)
			attname = "oid";

		relname = RelationGetRelationName(errpos->rel);
	}
	else
	{
		/* error occurred in a scan against a foreign join */
		ForeignScanState *fsstate = errpos->fsstate;
		ForeignScan *fsplan = castNode(ForeignScan, fsstate->ss.ps.plan);
		EState	   *estate = fsstate->ss.ps.state;
		TargetEntry *tle;

		tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
							errpos->cur_attno - 1);

		/*
		 * Target list can have Vars and expressions.  For Vars, we can get
		 * its relation, however for expressions we can't.  Thus for
		 * expressions, just show generic context message.
		 */
		if (IsA(tle->expr, Var))
		{
			RangeTblEntry *rte;
			Var		   *var = (Var *) tle->expr;

			rte = rt_fetch(var->varno, estate->es_range_table);

			if (var->varattno == 0)
				is_wholerow = true;
			else
				attname = get_attname(rte->relid, var->varattno, false);

			relname = get_rel_name(rte->relid);
		}
		else
			errcontext("processing expression at position %d in select list",
					   errpos->cur_attno);
	}

	if (relname)
	{
		if (is_wholerow)
			errcontext("whole-row reference to foreign table \"%s\"", relname);
		else if (attname)
			errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
	}
}


static void
set_path_distribution(LocInfo *locator, Distribution *distribution)
{
	ListCell	*lc;

	foreach(lc, locator->rl_nodeList)
		distribution->nodes = bms_add_member(distribution->nodes,
				lfirst_int(lc));
	distribution->restrictNodes = NULL;
	distribution->distributionExpr = NULL;
	distribution->distributionType = locator->locatorType;
	if(locator->distributionExpr)
	{
		distribution->distributionExpr = locator->distributionExpr;
	}
}

static bool
joinrelation_build_locator(RelOptInfo *joinrel, JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel)
{
	LocInfo *rli_join;
	LocInfo *rli_inner;
	LocInfo *rli_outer;
	ListCell *lc;
	List *joinclauses;
	Bitmapset *inner = NULL;
	Bitmapset *outer = NULL;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) joinrel->fdw_private;

	rli_outer = ((PgFdwRelationInfo *) outerrel->fdw_private)->rd_locator_info;
	rli_inner = ((PgFdwRelationInfo *) innerrel->fdw_private)->rd_locator_info;

	if (rli_outer == NULL || rli_inner == NULL)
		return false;

	foreach(lc, rli_inner->rl_nodeList)
		inner = bms_add_member(inner, lfirst_int(lc));
	foreach(lc, rli_outer->rl_nodeList)
		outer = bms_add_member(outer, lfirst_int(lc));

	if ((rli_outer && IsLocatorReplicated(rli_outer->locatorType)) &&
			(rli_inner && IsLocatorReplicated(rli_inner->locatorType)))
	{
		/* Determine common nodes */
		Bitmapset *join;
		int nodenum;

		join = bms_intersect(inner, outer);
		if (bms_is_empty(join))
			return false;

		rli_join = (LocInfo *) palloc0(sizeof(LocInfo));

		rli_join->locatorType = LOCATOR_TYPE_REPLICATED;
		rli_join->distributionExpr = NULL;
		while ((nodenum = bms_first_member(join)) >= 0)
			rli_join->rl_nodeList = lappend_int(rli_join->rl_nodeList,
					nodenum);
		fpinfo->rd_locator_info = rli_join;
		return true;

	}

	if ((rli_inner && IsLocatorReplicated(rli_inner->locatorType)) && (jointype == JOIN_INNER ||
				jointype == JOIN_LEFT))
	{
		/* Determine common nodes */
		int nodenum;

		if (!bms_is_subset(outer, inner))
			return false;

		rli_join = (LocInfo *) palloc0(sizeof(LocInfo));

		rli_join->locatorType = rli_outer->locatorType;
		rli_join->distributionExpr = rli_outer->distributionExpr;
		while ((nodenum = bms_first_member(outer)) >= 0)
			rli_join->rl_nodeList = lappend_int(rli_join->rl_nodeList,
					nodenum);
		fpinfo->rd_locator_info = rli_join;
		return true;
	}

	if ((rli_outer && IsLocatorReplicated(rli_outer->locatorType)) && (jointype == JOIN_INNER ||
				jointype == JOIN_RIGHT))
	{
		/* Determine common nodes */
		int nodenum;

		if (!bms_is_subset(inner, outer))
			return false;

		rli_join = (LocInfo *) palloc0(sizeof(LocInfo));

		rli_join->locatorType = rli_inner->locatorType;
		rli_join->distributionExpr = rli_inner->distributionExpr;
		while ((nodenum = bms_first_member(inner)) >= 0)
			rli_join->rl_nodeList = lappend_int(rli_join->rl_nodeList,
					nodenum);
		fpinfo->rd_locator_info = rli_join;
		return true;
	}

	joinclauses = list_copy(fpinfo->joinclauses);

	if (rli_inner->locatorType == rli_outer->locatorType &&
			rli_inner->distributionExpr &&
			rli_outer->distributionExpr &&
			bms_equal(inner, outer))
	{
		ListCell	*lc;

		foreach(lc, joinclauses)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			ListCell *emc;
			bool found_outer, found_inner;


			if (ri->left_ec == NULL || ri->right_ec == NULL)
				continue;
			if (ri->orclause)
				continue;

			if (!OidIsValid(ri->hashjoinoperator) && (!check_hashjoinable(ri->clause)))
				continue;

			found_outer = false;
			found_inner = false;

			if (ri->left_ec == ri->right_ec)
			{
				foreach(emc, ri->left_ec->ec_members)
				{
					EquivalenceMember *em = (EquivalenceMember *) lfirst(emc);
					Expr	   *var = (Expr *)em->em_expr;
					if (IsA(var, RelabelType))
						var = ((RelabelType *) var)->arg;
					if (!found_outer)
						found_outer = equal(var, rli_outer->distributionExpr);

					if (!found_inner)
						found_inner = equal(var, rli_inner->distributionExpr);
				}
				if (found_outer && found_inner)
				{
					ListCell *tlc, *emc;
					int nodenum;

					rli_join = (LocInfo *) palloc0(sizeof(LocInfo));

					rli_join->locatorType = rli_inner->locatorType;
					rli_join->distributionExpr = NULL;
					while ((nodenum = bms_first_member(inner)) >= 0)
						rli_join->rl_nodeList = lappend_int(rli_join->rl_nodeList,
								nodenum);
					fpinfo->rd_locator_info = rli_join;	

					/*
					 * Each member of the equivalence class may be a
					 * distribution expression, but we prefer some from the
					 * target list.
					 */
					foreach(tlc, joinrel->reltarget->exprs)
					{
						Expr *var = (Expr *) lfirst(tlc);
						foreach(emc, ri->left_ec->ec_members)
						{
							EquivalenceMember *em;
							Expr *emvar;

							em = (EquivalenceMember *) lfirst(emc);
							emvar = (Expr *)em->em_expr;
							if (IsA(emvar, RelabelType))
								emvar = ((RelabelType *) emvar)->arg;
							if (equal(var, emvar))
							{
								rli_join->distributionExpr = (Node *) var;
								return true;
							}
						}
					}
					/* Not found, take any */
					rli_join->distributionExpr = rli_inner->distributionExpr;
					return true;
				}
			}
			/*
			 * Check clause, if both arguments are distribution keys and
			 * operator is an equality operator
			 */
			else
			{
				OpExpr *op_exp;
				Expr   *arg1,
				       *arg2;

				op_exp = (OpExpr *) ri->clause;
				if (!IsA(op_exp, OpExpr) || list_length(op_exp->args) != 2)
					continue;

				arg1 = (Expr *) linitial(op_exp->args);
				arg2 = (Expr *) lsecond(op_exp->args);

				found_outer = equal(arg1, rli_outer->distributionExpr) || equal(arg2, rli_outer->distributionExpr);
				found_inner = equal(arg1, rli_inner->distributionExpr) || equal(arg2, rli_inner->distributionExpr);

				if (found_outer && found_inner)
				{
					int nodenum;

					rli_join = (LocInfo *) palloc0(sizeof(LocInfo));

					rli_join->locatorType = rli_inner->locatorType;
					rli_join->distributionExpr = NULL;
					while ((nodenum = bms_first_member(inner)) >= 0)
						rli_join->rl_nodeList = lappend_int(rli_join->rl_nodeList,
								nodenum);
					fpinfo->rd_locator_info = rli_join;
					/*
					 * In case of outer join distribution key should not refer
					 * distribution key of nullable part.
					 */
					if (jointype == JOIN_FULL)
						/* both parts are nullable */
						rli_join->distributionExpr = NULL;
					else if (jointype == JOIN_RIGHT)
						rli_join->distributionExpr = rli_inner->distributionExpr;
					else
						rli_join->distributionExpr = rli_outer->distributionExpr;

					return true;
				}
			}
		}
	}
	return false;
}

static void
restrict_distribution(PlannerInfo *root, RestrictInfo *ri, Distribution *distribution)
{
	Oid	keytype;
	Const	*constExpr = NULL;
	bool	found_key = false;
	/*
	 * Can not restrict - not distributed or key is not defined
	 */
	if (distribution == NULL ||
			distribution->distributionExpr == NULL)
		return;
	/*
	 * We do not support OR'ed conditions yet
	 */
	if (ri->orclause)
		return;

	/*
	 * Check if the operator is hash joinable. Currently we only support hash
	 * joinable operator for arriving at restricted nodes. This allows us
	 * correctly deduce clauses which include a mix of int2/int4/int8 or
	 * float4/float8 or clauses which have same type arguments and have a hash
	 * joinable operator.
	 *
	 * Note: This stuff is mostly copied from check_hashjoinable
	 */
	{
		Expr	*clause = ri->clause;
		Oid	opno;
		Node	*leftarg;

		if (ri->pseudoconstant)
			return;
		if (!is_opclause(clause))
			return;
		if (list_length(((OpExpr *) clause)->args) != 2)
			return;

		opno = ((OpExpr *) clause)->opno;
		leftarg = linitial(((OpExpr *) clause)->args);

		if (!op_hashjoinable(opno, exprType(leftarg)) ||
				contain_volatile_functions((Node *) clause))
			return;
	}

	keytype = exprType(distribution->distributionExpr);
	if (ri->left_ec)
	{
		EquivalenceClass *ec = ri->left_ec;
		ListCell *lc;
		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
			if (equal(em->em_expr, distribution->distributionExpr))
				found_key = true;
			else if (bms_is_empty(em->em_relids))
			{
				Expr *cexpr = (Expr *) eval_const_expressions(root,
						(Node *) em->em_expr);
				if (IsA(cexpr, Const))
					constExpr = (Const *) cexpr;
			}
		}
	}
	if (ri->right_ec)
	{
		EquivalenceClass *ec = ri->right_ec;
		ListCell *lc;
		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
			if (equal(em->em_expr, distribution->distributionExpr))
				found_key = true;
			else if (bms_is_empty(em->em_relids))
			{
				Expr *cexpr = (Expr *) eval_const_expressions(root,
						(Node *) em->em_expr);
				if (IsA(cexpr, Const))
					constExpr = (Const *) cexpr;
			}
		}
	}
	if (IsA(ri->clause, OpExpr))
	{
		OpExpr *opexpr = (OpExpr *) ri->clause;
		if (opexpr->args->length == 2 &&
				op_mergejoinable(opexpr->opno, exprType(linitial(opexpr->args))))
		{
			Expr *arg1 = (Expr *) linitial(opexpr->args);
			Expr *arg2 = (Expr *) lsecond(opexpr->args);
			Expr *other = NULL;
			if (equal(arg1, distribution->distributionExpr))
				other = arg2;
			else if (equal(arg2, distribution->distributionExpr))
				other = arg1;
			if (other)
			{
				found_key = true;
				other = (Expr *) eval_const_expressions(root, (Node *) other);
				if (IsA(other, Const))
					constExpr = (Const *) other;
			}
		}
	}
	if (found_key && constExpr)
	{
		List		*nodeList = NIL;
		Bitmapset	*tmpset = bms_copy(distribution->nodes);
		Bitmapset	*restrictinfo = NULL;
		Locator		*locator;
		int		*nodenums;
		int		i, count;

		while((i = bms_first_member(tmpset)) >= 0)
			nodeList = lappend_int(nodeList, i);
		bms_free(tmpset);

		locator = createLocator(distribution->distributionType,
				RELATION_ACCESS_READ,
				keytype,
				LOCATOR_LIST_LIST,
				0,
				(void *) nodeList,
				(void **) &nodenums,
				false);
		count = GET_NODES(locator, constExpr->constvalue,
				constExpr->constisnull, NULL);

		for (i = 0; i < count; i++)
			restrictinfo = bms_add_member(restrictinfo, nodenums[i]);
		if (distribution->restrictNodes)
			distribution->restrictNodes = bms_intersect(distribution->restrictNodes,
					restrictinfo);
		else
			distribution->restrictNodes = restrictinfo;
		list_free(nodeList);
		freeLocator(locator);
	}
}

static RelationAccessType
get_relation_access_type(Query *query)
{
	CmdType command_type = query->commandType;
	bool for_update = query->rowMarks ? true : false;
	RelationAccessType rel_access;

	switch (command_type)
	{
		case CMD_SELECT:
			if (for_update)
				rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
			else
				rel_access = RELATION_ACCESS_READ;
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			rel_access = RELATION_ACCESS_UPDATE;
			break;

		case CMD_INSERT:
			rel_access = RELATION_ACCESS_INSERT;
			break;

		default:
			/* should not happen, but */
			elog(ERROR, "Unrecognised command type %d", command_type);
			break;
	}
	return rel_access;
}

static void
estimate_hash_bucket_stats_cluster(PlannerInfo *root, Node *hashkey, double nbuckets,
		Selectivity *mcv_freq,
		Selectivity *bucketsize_frac, int node_num)
{
	VariableStatData vardata;
	double		estfract,
			ndistinct,
			stanullfrac,
			avgfreq;
	bool		isdefault;
	AttStatsSlot sslot;

	Assert(node_num > 0);

	examine_variable(root, hashkey, 0, &vardata);

	/* Look up the frequency of the most common value, if available */
	*mcv_freq = 0.0;

	if (HeapTupleIsValid(vardata.statsTuple))
	{
		if (get_attstatsslot(&sslot, vardata.statsTuple,
					STATISTIC_KIND_MCV, InvalidOid,
					ATTSTATSSLOT_NUMBERS))
		{
			/*
			 * The first MCV stat is for the most common value.
			 */
			if (sslot.nnumbers > 0)
				*mcv_freq = sslot.numbers[0];
			free_attstatsslot(&sslot);
		}
	}

	/* Get number of distinct values */
	ndistinct = get_variable_numdistinct(&vardata, &isdefault);

	/*
	 * If ndistinct isn't real, punt.  We normally return 0.1, but if the
	 * mcv_freq is known to be even higher than that, use it instead.
	 */
	if (isdefault)
	{
		*bucketsize_frac = (Selectivity) Max(0.1, *mcv_freq);
		ReleaseVariableStats(vardata);
		return;
	}

	/* Get fraction that are null */
	if (HeapTupleIsValid(vardata.statsTuple))
	{
		Form_pg_statistic stats;

		stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
		stanullfrac = stats->stanullfrac;
	}
	else
		stanullfrac = 0.0;

	/* Compute avg freq of all distinct data values in raw relation */
	avgfreq = (1.0 - stanullfrac) / ndistinct;

	/*
	 * Adjust ndistinct to account for restriction clauses.  Observe we are
	 * assuming that the data distribution is affected uniformly by the
	 * restriction clauses!
	 *
	 * XXX Possibly better way, but much more expensive: multiply by
	 * selectivity of rel's restriction clauses that mention the target Var.
	 */
	if (vardata.rel && vardata.rel->tuples > 0)
	{
		ndistinct *= vardata.rel->rows / vardata.rel->tuples;
		ndistinct = clamp_row_est(ndistinct);
	}

	/*
	 * Initial estimate of bucketsize fraction is 1/nbuckets as long as the
	 * number of buckets is less than the expected number of distinct values;
	 * otherwise it is 1/ndistinct.
	 */
	if ((ndistinct / node_num) > nbuckets)
		estfract = 1.0 / nbuckets;
	else
		estfract = 1.0 / (ndistinct / node_num);

	/*
	 * Adjust estimated bucketsize upward to account for skewed distribution.
	 */
	if (avgfreq > 0.0 && *mcv_freq > avgfreq)
		estfract *= *mcv_freq / avgfreq;

	/*
	 * Clamp bucketsize to sane range (the above adjustment could easily
	 * produce an out-of-range result).  We set the lower bound a little above
	 * zero, since zero isn't a very sane result.
	 */
	if (estfract < 1.0e-6)
		estfract = 1.0e-6;
	else if (estfract > 1.0)
		estfract = 1.0;

	*bucketsize_frac = (Selectivity) estfract;

	ReleaseVariableStats(vardata);
}

static double
approx_tuple_count_cluster(PlannerInfo *root, PgFdwRelationInfo *fpinfo)
{
	double		tuples;
	PgFdwRelationInfo *fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
	PgFdwRelationInfo *fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
	int node_num_i  = IsLocatorReplicated(fpinfo_i->rd_locator_info->locatorType)
		? 1: list_length(fpinfo_i->rd_locator_info->rl_nodeList);
	int node_num_o = IsLocatorReplicated(fpinfo_o->rd_locator_info->locatorType)
		? 1: list_length(fpinfo_o->rd_locator_info->rl_nodeList);
	double		outer_tuples = fpinfo_i->rows/node_num_i;
	double		inner_tuples = fpinfo_o->rows/node_num_o;
	SpecialJoinInfo sjinfo;
	Selectivity selec = 1.0;
	ListCell   *l;

	/*
	 * Make up a SpecialJoinInfo for JOIN_INNER semantics.
	 */
	sjinfo.type = T_SpecialJoinInfo;
	sjinfo.min_lefthand = fpinfo->outerrel->relids;
	sjinfo.min_righthand = fpinfo->innerrel->relids;
	sjinfo.syn_lefthand = fpinfo->outerrel->relids;
	sjinfo.syn_righthand = fpinfo->innerrel->relids;
	sjinfo.jointype = JOIN_INNER;
	/* we don't bother trying to make the remaining fields valid */
	sjinfo.lhs_strict = false;
	sjinfo.delay_upper_joins = false;
	sjinfo.semi_can_btree = false;
	sjinfo.semi_can_hash = false;
	sjinfo.semi_operators = NIL;
	sjinfo.semi_rhs_exprs = NIL;

	/* Get the approximate selectivity */
	foreach(l, fpinfo->hashclauses)
	{
		Node	   *qual = (Node *) lfirst(l);

		/* Note that clause_selectivity will be able to cache its result */
		selec *= clause_selectivity(root, qual, 0, JOIN_INNER, &sjinfo);
	}

	/* Apply it to the input relation sizes */
	tuples = selec * outer_tuples * inner_tuples;

	return clamp_row_est(tuples);
}

static void
get_hash_join_cost(PlannerInfo *root, PgFdwRelationInfo *fpinfo,
			Cost *p_startup_cost, Cost *p_total_cost, int run_node_num, int retrieved_rows)
{
	int num_hashclauses = list_length(fpinfo->hashclauses);
	PgFdwRelationInfo *fpinfo_i = (PgFdwRelationInfo *) fpinfo->innerrel->fdw_private;
	PgFdwRelationInfo *fpinfo_o = (PgFdwRelationInfo *) fpinfo->outerrel->fdw_private;
	int node_num_i  = IsLocatorReplicated(fpinfo_i->rd_locator_info->locatorType)
		? 1: list_length(fpinfo_i->rd_locator_info->rl_nodeList);
	int node_num_o = IsLocatorReplicated(fpinfo_o->rd_locator_info->locatorType)
		? 1: list_length(fpinfo_o->rd_locator_info->rl_nodeList);
	Cost startup_cost_hash = 0.0;
	Cost run_cost_hash = 0.0;
	int inner_rows = fpinfo_i->rows/node_num_i;
	int outer_rows = fpinfo_o->rows/node_num_o;
	int numbuckets;
	int numbatches;
	int num_skew_mcvs;
	size_t space_allowed;
	int inner_width = fpinfo->innerrel->reltarget->width;
	int outer_width = fpinfo->outerrel->reltarget->width;
	Cost		cpu_per_tuple;
	double		hashjointuples;
	double		virtualbuckets;
	Selectivity innerbucketsize;
	Selectivity innermcvfreq;
	ListCell   *hcl;
	double retrieved_rows_hash;
	QualCost	hash_qual_cost;
	QualCost	qp_qual_cost;
	QualCost        join_cost;
	QualCost        remote_conds_cost;
	double		nrows;

	cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
	/* Calculate the cost of applying join clauses */
	cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

	startup_cost_hash += fpinfo_o->rel_startup_cost;
	run_cost_hash += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;;
	startup_cost_hash += fpinfo_i->rel_total_cost;

	startup_cost_hash += (cpu_operator_cost * num_hashclauses + cpu_tuple_cost) * inner_rows;
	run_cost_hash += cpu_operator_cost * num_hashclauses * outer_rows;

	ExecChooseHashTableSize(inner_rows,
			inner_width,
			true,		/* useskew */
			false,
			0,
			&space_allowed,
			&numbuckets,
			&numbatches,
			&num_skew_mcvs);
	if (numbatches > 1)
	{
		double outerpages = ceil((outer_rows * (MAXALIGN(outer_width) 
						+ MAXALIGN(sizeof(HeapTupleHeaderData)))) / BLCKSZ);
		double innerpages = ceil((inner_rows * (MAXALIGN(inner_width)
						+ MAXALIGN(sizeof(HeapTupleHeaderData)))) / BLCKSZ);

		startup_cost_hash += seq_page_cost * innerpages;
		run_cost_hash += seq_page_cost * (innerpages + 2 * outerpages);
	}
	if (!enable_hashjoin)
		startup_cost_hash += disable_cost;

	virtualbuckets = (double) numbuckets * (double) numbatches;

	innerbucketsize = 1.0;
	innermcvfreq = 1.0;
	foreach(hcl, fpinfo->hashclauses)
	{
		RestrictInfo *restrictinfo = lfirst_node(RestrictInfo, hcl);
		Selectivity thisbucketsize;
		Selectivity thismcvfreq;

		/*
		 * First we have to figure out which side of the hashjoin clause
		 * is the inner side.
		 *
		 * Since we tend to visit the same clauses over and over when
		 * planning a large query, we cache the bucket stats estimates in
		 * the RestrictInfo node to avoid repeated lookups of statistics.
		 */
		if (bms_is_subset(restrictinfo->right_relids,
					fpinfo->innerrel->relids))
		{
			/* righthand side is inner */
			thisbucketsize = restrictinfo->right_bucketsize;
			/* not cached yet */
			estimate_hash_bucket_stats_cluster(root,
					get_rightop(restrictinfo->clause),
					virtualbuckets,
					&restrictinfo->right_mcvfreq,
					&restrictinfo->right_bucketsize, node_num_i);
			thisbucketsize = restrictinfo->right_bucketsize;
			thismcvfreq = restrictinfo->right_mcvfreq;
		}
		else
		{
			Assert(bms_is_subset(restrictinfo->left_relids,
						fpinfo->innerrel->relids));
			/* lefthand side is inner */
			thisbucketsize = restrictinfo->left_bucketsize;
			/* not cached yet */
			estimate_hash_bucket_stats_cluster(root,
					get_leftop(restrictinfo->clause),
					virtualbuckets,
					&restrictinfo->left_mcvfreq,
					&restrictinfo->left_bucketsize, node_num_i);
			thisbucketsize = restrictinfo->left_bucketsize;
			thismcvfreq = restrictinfo->left_mcvfreq;
		}

		if (innerbucketsize > thisbucketsize)
			innerbucketsize = thisbucketsize;
		if (innermcvfreq > thismcvfreq)
			innermcvfreq = thismcvfreq;
	}
	if(((inner_rows * innermcvfreq) 
				* (MAXALIGN(inner_width) + MAXALIGN(sizeof(HeapTupleHeaderData)))) >
			(work_mem * 1024L))
		startup_cost_hash += disable_cost;
	cost_qual_eval(&hash_qual_cost, fpinfo->hashclauses, root);
	cost_qual_eval(&qp_qual_cost, fpinfo->joinclauses, root);
	qp_qual_cost.startup -= hash_qual_cost.startup;
	qp_qual_cost.per_tuple -= hash_qual_cost.per_tuple;

	startup_cost_hash += hash_qual_cost.startup;
	startup_cost_hash += qp_qual_cost.startup;
	startup_cost_hash += remote_conds_cost.startup;
	run_cost_hash += hash_qual_cost.per_tuple * outer_rows *
		clamp_row_est(inner_rows * innerbucketsize) * 0.5;
	hashjointuples = approx_tuple_count_cluster(root, fpinfo);
	nrows = inner_rows * outer_rows * run_node_num;
	cpu_per_tuple = cpu_tuple_cost + remote_conds_cost.per_tuple + qp_qual_cost.per_tuple;
	run_cost_hash += cpu_per_tuple * hashjointuples;
	retrieved_rows_hash = Min(retrieved_rows, nrows);
	run_cost_hash += fpinfo->local_conds_cost.per_tuple * retrieved_rows_hash;

	if(p_startup_cost)
		*p_startup_cost = startup_cost_hash;
	if(p_total_cost)
		*p_total_cost = startup_cost_hash + run_cost_hash;
}
static bool
polarx_start_command_on_connection(PGXCNodeHandle **connections, ForeignScanState *node, Snapshot snapshot)
{
	CommandId    cid;
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;	
	ResponseCombiner *combiner =  &(fsstate->combiner);
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int     numParams = fsstate->numParams;
	int	i;
	bool    send_desc = false;
	const char **values = fsstate->param_values;
	StringInfoData buf;
	MemoryContext oldcontext;
	uint16 n16;
    char *prep_name;

    GetPrepStmtNameAndSelfVersion(fsstate->query_hash, &prep_name);
	if (snapshot)
	{
		cid = snapshot->curcid;

		if (cid == InvalidCommandId)
		{
			elog(LOG, "commandId in snapshot is invalid.");
			cid = GetCurrentCommandId(false);
		}
	}
	else
	{
		cid = GetCurrentCommandId(false);
	}

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	process_query_params(econtext,
			fsstate->param_flinfo,
			fsstate->param_exprs,
			values);

	MemoryContextSwitchTo(oldcontext);

	initStringInfo(&buf);
	n16 = htons(numParams);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	for (i = 0; i < numParams; i++)
	{
		uint32 n32;
		if (values && values[i])
		{
			int	len;

			len  = strlen(values[i]);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, values[i], len);
		}
		else
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
	}

	if (fsstate->retrieved_attrs != NULL ||
			fsstate->rel_access_type == RELATION_ACCESS_READ ||
			fsstate->rel_access_type == RELATION_ACCESS_READ_FOR_UPDATE)
		send_desc = true;
	combiner->extended_query = true;

	for(i = 0; i < combiner->conn_count; i++)
	{
		connections[i]->recv_datarows = 0;

		elog(DEBUG5, "pgxc_start_command_on_connection - node %s, state %d",
				connections[i]->nodename, connections[i]->state);
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY && connections[i]->combiner != NULL
				&& connections[i]->combiner != combiner)
		{
			BufferConnection(connections[i]);
		}
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
		if (pgxc_node_send_cmd_id(connections[i], cid) < 0 )
			return false;
#endif
#ifdef POLARDB_X_UPGRADE

		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
			return false;
#endif

		if (pgxc_node_send_query_extended(connections[i],
					NULL,
					prep_name,
					combiner->cursor,
					numParams,
					fsstate->param_types,
					buf.len,
					buf.data,
					send_desc,
					1000) != 0)
			return false;
		connections[i]->combiner = combiner;
	}
	return  true;
}

static void
polarx_scan_begin(bool read_only, List *node_list, bool need_tran_block)
{
	int     conn_count = 0;
	PGXCNodeHandle **connections = NULL;
	PGXCNodeAllHandles *all_handles;

	all_handles = get_handles(node_list, NIL, false, true);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;



	if (pgxc_node_begin(conn_count, connections, 0, need_tran_block,
				read_only, PGXC_NODE_DATANODE))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not begin transaction on data node")));
}

static void
close_node_cursors(char *cursor, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle      **connections;
	ResponseCombiner    combiner;
	int                 conn_count;
	int                 i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false, true);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
		{
			PGXCNodeSetConnectionState(connections[i],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			PGXCNodeSetConnectionState(connections[i],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		}

		PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	memset(&combiner, 0, sizeof(ScanState));
	combiner.request_type = REQUEST_TYPE_QUERY;

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], &combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_READY ||
					connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				/* Unexpected response, ignore? */
			}
		}
	}

	ValidateAndCloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
}
static void
polarx_send_query_prepared(PgFdwModifyState *fmstate, List *nodelist,
		const char **values, TupleTableSlot *slot, int *nrows)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle      **connections;
	ResponseCombiner    combiner;
	int                 conn_count;
	int                 i;
	StringInfoData buf;
	uint16 n16;
	int     numParams = fmstate->p_nums;
	CommandId    cid = GetCurrentCommandId(false);
    char *prep_name;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

    GetPrepStmtNameAndSelfVersion(fmstate->p_name, &prep_name);
	/* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false, true);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	initStringInfo(&buf);
	n16 = htons(numParams);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	for (i = 0; i < numParams; i++)
	{
		uint32 n32;
		if (values && values[i])
		{
			int     len;

			len  = strlen(values[i]);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, values[i], len);
		}
		else {
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
	}

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
		if (pgxc_node_send_cmd_id(connections[i], cid) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command ID to Datanodes")));
#endif
		if (pgxc_node_send_query_extended(connections[i],
					NULL,
					prep_name,
					NULL,
					numParams,
					fmstate->param_types,
					buf.len,
					buf.data,
					false,
					0) != 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send prepared statement: %s", fmstate->p_name)));
		}
		connections[i]->combiner = NULL;	
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	memset(&combiner, 0, sizeof(ScanState));

	combiner.request_type = REQUEST_TYPE_QUERY;
	combiner.ss.ss_ScanTupleSlot = MakeTupleTableSlot(NULL);
	combiner.recv_node_count = conn_count;
	combiner.recv_tuples      = 0;
	combiner.recv_total_time = -1;
	combiner.recv_datarows = 0;
	combiner.node_count = conn_count;
	combiner.DML_processed = 0;
	combiner.extended_query = true;
	if(IsLocatorReplicated(fmstate->locator_type))
		combiner.combine_type = COMBINE_TYPE_SAME;
	else
		combiner.combine_type = COMBINE_TYPE_SUM;

	if (conn_count > 0)
	{
		combiner.connections = connections;
		combiner.conn_count = conn_count;
		combiner.current_conn = 0;
	}
	while(true)
	{
        RemoteDataRow datarow = FetchDatarow(&combiner);
		if (datarow == NULL)
			break;
		if(fmstate->has_returning)
		{
			store_returning_result_with_datarow(fmstate, slot, datarow->msg);
		}
        pfree(datarow);
	}

	if(nrows)
	{
		if(combiner.combine_type == COMBINE_TYPE_SAME)
			*nrows = combiner.DML_processed / conn_count;
		else
			*nrows = combiner.DML_processed;
	}
	if(combiner.ss.ss_ScanTupleSlot)
		ExecClearTuple(combiner.ss.ss_ScanTupleSlot);
	pfree(combiner.ss.ss_ScanTupleSlot);

	if (combiner.errorMessage)
	{
		char *tmp_errmsg = pstrdup(combiner.errorMessage);

		pgxc_connections_cleanup(&combiner);
		combiner.errorMessage = tmp_errmsg;
		pgxc_node_report_error(&combiner);
	}

	CloseCombiner(&combiner);	
	pfree_pgxc_all_handles(all_handles);
}
static void
store_returning_result_with_datarow(PgFdwModifyState *fmstate,
			TupleTableSlot *slot, char *res)
{
	HeapTuple	newtup;

	newtup = make_tuple_from_datarow(res,
			fmstate->rel,
			fmstate->attinmeta,
			fmstate->retrieved_attrs,
			NULL,
			fmstate->temp_cxt, false);
	/* tuple will be deleted when it is cleared from the slot */
	ExecStoreTuple(newtup, slot, InvalidBuffer, true);
}

static void
prepare_simple_sort_from_pathkeys(PlannerInfo *root, List **scan_tlist, List *pathkeys,
					RelOptInfo *foreignrel,
					const AttrNumber *reqColIdx,
					MergeAppend *simple_sort)
{
	ListCell   *i;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;
	Relids relids = foreignrel->relids;
	bool is_simple_rel = false;
	List *tlist = *scan_tlist;
	bool add_new_tle = false;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	if(tlist == NIL)
	{
		is_simple_rel = true;
		tlist =  build_tlist_to_deparse(foreignrel);		
	}
	foreach(i, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid			pk_datatype = InvalidOid;
		Oid			sortop;
		ListCell   *j;
		bool is_from_tlist = false;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0)	/* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else if (reqColIdx != NULL)
		{
			/*
			 * If we are given a sort column number to match, only consider
			 * the single TLE at that position.  It's possible that there is
			 * no such TLE, in which case fall through and generate a resjunk
			 * targetentry (we assume this must have happened in the parent
			 * plan as well).  If there is a TLE but it doesn't match the
			 * pathkey's EC, we do the same, which is probably the wrong thing
			 * but we'll leave it to caller to complain about the mismatch.
			 */
			tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
			if (tle)
			{
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr at right place in tlist */
					pk_datatype = em->em_datatype;
				}
				else
					tle = NULL;
			}
		}
		else
		{
			/*
			 * Otherwise, we can sort by any non-constant expression listed in
			 * the pathkey's EquivalenceClass.  For now, we take the first
			 * tlist item found in the EC. If there's no match, we'll generate
			 * a resjunk entry using the first EC member that is an expression
			 * in the input's vars.  (The non-const restriction only matters
			 * if the EC is below_outer_join; but if it isn't, it won't
			 * contain consts anyway, else we'd have discarded the pathkey as
			 * redundant.)
			 *
			 * XXX if we have a choice, is there any way of figuring out which
			 * might be cheapest to execute?  (For example, int4lt is likely
			 * much cheaper to execute than numericlt, but both might appear
			 * in the same equivalence class...)  Not clear that we ever will
			 * have an interesting choice in practice, so it may not matter.
			 */
			foreach(j, tlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					is_from_tlist = true;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
		{
			/*
			 * No matching tlist item; look for a computable expression. Note
			 * that we treat Aggrefs as if they were variables; this is
			 * necessary when attempting to sort the output from an Agg node
			 * for use in a WindowFunc (since grouping_planner will have
			 * treated the Aggrefs as variables, too).  Likewise, if we find a
			 * WindowFunc in a sort expression, treat it as a variable.
			 */
			Expr	   *sortexpr = NULL;

			foreach(j, ec->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
				List	   *exprvars;
				ListCell   *k;

				/*
				 * We shouldn't be trying to sort by an equivalence class that
				 * contains a constant, so no need to consider such cases any
				 * further.
				 */
				if (em->em_is_const)
					continue;

				/*
				 * Ignore child members unless they belong to the rel being
				 * sorted.
				 */
				if (em->em_is_child &&
						!bms_is_subset(em->em_relids, relids))
					continue;

				sortexpr = em->em_expr;
				exprvars = pull_var_clause((Node *) sortexpr,
						PVC_INCLUDE_AGGREGATES |
						PVC_INCLUDE_WINDOWFUNCS |
						PVC_INCLUDE_PLACEHOLDERS);
				foreach(k, exprvars)
				{
					if (!tlist_member_ignore_relabel(lfirst(k), tlist))
						break;
				}
				list_free(exprvars);
				if (!k)
				{
					pk_datatype = em->em_datatype;
					break;		/* found usable expression */
				}
			}
			if (!j)
				elog(ERROR, "could not find pathkey item to sort");

			/*
			 * Add resjunk entry to input's tlist
			 */
			tle = makeTargetEntry(sortexpr,
					list_length(tlist) + 1,
					NULL,
					true);
			tlist = lappend(tlist, tle);
			if(!add_new_tle)
				add_new_tle = true;
		}

		/*
		 * Look up the correct sort operator from the PathKey's slightly
		 * abstracted representation.
		 */
		sortop = get_opfamily_member(pathkey->pk_opfamily,
				pk_datatype,
				pk_datatype,
				pathkey->pk_strategy);
		if (!OidIsValid(sortop))	/* should not happen */
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
					pathkey->pk_strategy, pk_datatype, pk_datatype,
					pathkey->pk_opfamily);

		/* Add the column to the sort arrays */
		sortColIdx[numsortkeys] = (is_simple_rel && is_from_tlist) ? get_col_index(root, foreignrel, tle) : tle->resno;
		sortOperators[numsortkeys] = sortop;
		collations[numsortkeys] = ec->ec_collation;
		nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
		numsortkeys++;
	}
	if(is_simple_rel && add_new_tle)
		*scan_tlist = tlist;

	/* Return results */
	simple_sort->numCols = numsortkeys;
	simple_sort->sortColIdx = sortColIdx;
	simple_sort->sortOperators = sortOperators;
	simple_sort->collations = collations;
	simple_sort->nullsFirst = nullsFirst;
}
static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec,
				TargetEntry *tle,
				Relids relids)
{
	Expr	   *tlexpr;
	ListCell   *lc;

	/* We ignore binary-compatible relabeling on both ends */
	tlexpr = tle->expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr	   *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the rel being sorted.
		 */
		if (em->em_is_child &&
				!bms_is_subset(em->em_relids, relids))
			continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, tlexpr))
			return em;
	}

	return NULL;
}
static int
get_col_index(PlannerInfo *root, RelOptInfo *foreignrel, TargetEntry *tle)
{
	RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) foreignrel->fdw_private;
	Relation	rel = heap_open(rte->relid, NoLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int i;
	int col_inx = 0;
	int attrnum = 0;

	attrnum = tupdesc->natts;

	if(IsA(tle->expr, Var))
	{
		col_inx	= ((Var *)tle->expr)->varattno;
		if(col_inx == 0)
			elog(ERROR, "the pathkey var varattno is not valid");
	}
	else
		elog(ERROR, "the pathkey expr is not var in simple foreign rel");
	

	for (i = 1; i <= attrnum; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);	

		if(col_inx == i)
		{
			if (attr->attisdropped)
				continue;

			if(!bms_is_member(i - FirstLowInvalidHeapAttributeNumber, fpinfo->attrs_used))
			{
				bms_add_member(fpinfo->attrs_used, i);
			}
			break;
		}
	}
	if(col_inx < 0 && col_inx > FirstLowInvalidHeapAttributeNumber)
	{
		if(!bms_is_member(col_inx - FirstLowInvalidHeapAttributeNumber, fpinfo->attrs_used))
		{
			bms_add_member(fpinfo->attrs_used, col_inx);
		}
	}

	heap_close(rel, NoLock);
	if(i <= attrnum || (col_inx < 0 && col_inx > FirstLowInvalidHeapAttributeNumber))
	{
		return col_inx;
	}
	else
	{
		elog(ERROR, "the pathkey var attrnum  %d is not in foreign rel %d", col_inx, rte->relid);
		return 0;	
	}
}
static int32
fdw_heap_compare_slots(Datum a, Datum b, void *arg)
{
	PgFdwScanState *fsstate = (PgFdwScanState *) arg;
	int32		slot1 = DatumGetInt32(a);
	int32		slot2 = DatumGetInt32(b);

	HeapTuple s1 = fsstate->tuples[slot1];
	HeapTuple s2 = fsstate->tuples[slot2];
	int			nkey;

	Assert(s1 != NULL);
	Assert(s2 != NULL);

	for (nkey = 0; nkey < fsstate->ms_nkeys; nkey++)
	{
		SortSupport sortKey = fsstate->ms_sortkeys + nkey;
		AttrNumber	attno = sortKey->ssup_attno;
		Datum		datum1,
				datum2;
		bool		isNull1,
				isNull2;
		int			compare;

		if(attno == ObjectIdAttributeNumber)
		{
			datum1 = UInt32GetDatum(HeapTupleGetOid(s1));
			datum2 = UInt32GetDatum(HeapTupleGetOid(s2));
		}
		else if(attno == SelfItemPointerAttributeNumber)
		{
			datum1 = PointerGetDatum(&s1->t_self);
                        datum2 = PointerGetDatum(&s2->t_self);
		}
		else
		{
			datum1 = heap_getattr(s1, attno, fsstate->tupdesc, &isNull1);
			datum2 = heap_getattr(s2, attno, fsstate->tupdesc, &isNull2);
			
		}

		compare = ApplySortComparator(datum1, isNull1,
				datum2, isNull2,
				sortKey);
		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}
	return 0;
}
static TargetEntry *
get_node_index(Plan *plan, bool *is_exist)
{
	ListCell   *t;
	TargetEntry *res = NULL;

	if(plan == NULL)
		return NULL;

	if(IsA(plan, ForeignScan))
	{
		foreach(t, plan->targetlist)
		{
			TargetEntry *tle = lfirst(t);

			if (tle->resjunk && tle->resname &&
					(strcmp(tle->resname, "node_inx") == 0))
			{
				res = tle;
				if(is_exist)
					*is_exist = true;
				return res;
			}
		}
		foreach(t, ((ForeignScan *)plan)->fdw_scan_tlist)
		{
			TargetEntry *tle = lfirst(t);

			if (tle->resjunk && tle->resname &&
					(strcmp(tle->resname, "node_inx") == 0))
			{
				TargetEntry *new_tle = copyObject(tle);
				new_tle->resno = list_length(plan->targetlist) + 1;
				res = new_tle;
			
				lappend(plan->targetlist, new_tle);
				if(is_exist)
					*is_exist = true;
				return res;
			}
		}
	}
	if(res == NULL)
		res = get_node_index(plan->lefttree, NULL);
	if(res == NULL)
		res = get_node_index(plan->righttree, NULL);
	return res;
}
static void
change_wholerow(Plan *plan)
{
	ListCell   *t;
	TargetEntry *res = NULL;

	if(plan == NULL)
		return;

	if(IsA(plan, ForeignScan))
	{
		foreach(t, plan->targetlist)
		{
			TargetEntry *tle = lfirst(t);

			if (tle->resjunk && tle->resname &&
					(strcmp(tle->resname, "wholerow") == 0)
					&& ((Var *)tle->expr)->vartype != RECORDOID)
			{
				((Var *)tle->expr)->vartype = RECORDOID;
			}
		}
	}
	if(res == NULL)
		change_wholerow(plan->lefttree);
	if(res == NULL)
		change_wholerow(plan->righttree);
	return;
}
static bool
reloptinfo_contain_targetrel_inx(int targetrel_inx, RelOptInfo *rel)
{
	Bitmapset       *tmpset = bms_copy(rel->relids);
	bool is_target_rel = false;
	int i = -1;

	while((i = bms_first_member(tmpset)) >= 0)
	{
		if(targetrel_inx == i)
			is_target_rel = true;
	}
	if(tmpset)
		bms_free(tmpset);
	return is_target_rel;
}
static bool
pathkeys_contain_const(List *pathkeys, RelOptInfo *rel)
{
	ListCell   *lcell;
	PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;

	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		Expr	   *em_expr;

		em_expr = find_em_expr_for_rel(pathkey->pk_eclass,
				IS_UPPER_REL(rel) ? fpinfo->outerrel : rel);
		Assert(em_expr != NULL);

		if(IsA(em_expr, Const))
			return true;
	}
	return false;
}

bool
CheckForeginScanDistByParam(List *fdw_private)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return false;
    lc = list_nth_cell(fdw_private, FdwScanPrivateDistByParam);
    return ((DistributionForParam *)lfirst(lc))->paramId >= 0;
}

void
setParamsExternIntoForeginScan(List *fdw_private, ParamExternDataInfo *paramsExtern)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return;
    lc = list_nth_cell(fdw_private, FdwScanPrivateParamsExtern);
    lfirst(lc) = (void *)paramsExtern;
}

void
setFQSPlannedStmtIntoForeginScan(List *fdw_private, PlannedStmt *fqs_plannedstmt)
{
    ListCell *lc;

    if(fdw_private == NULL)
        return;
    if(fqs_plannedstmt != NULL)
    {
        lc = list_nth_cell(fdw_private, FdwScanPrivateFQSPlannedStmt);
        lfirst(lc) = fqs_plannedstmt;
    }
}

ParamExternDataInfo *
getParamsExternFromForeginScan(List *fdw_private)
{
    ListCell *lc;
    ParamExternDataInfo *res = NULL;

    if(fdw_private == NULL) 
        return NULL;
    lc = list_nth_cell(fdw_private, FdwScanPrivateParamsExtern);
    res = (ParamExternDataInfo *)lfirst(lc);
    lc = list_nth_cell(fdw_private, FdwScanPrivateFQSPlannedStmt);
    if(lfirst(lc) != NULL)
        res->fqs_plannedstmt = (PlannedStmt *)lfirst(lc);
    return res;
}

void
setParamsExternIntoForeginModify(List *fdw_private, ParamExternDataInfo *paramsExtern)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return;
    lc = list_nth_cell(fdw_private, FdwModifyPrivateParamsExtern);
    lfirst(lc) = (void *)paramsExtern;
}

ParamExternDataInfo *
getParamsExternFromForeginModify(List *fdw_private)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return NULL;
    lc = list_nth_cell(fdw_private, FdwModifyPrivateParamsExtern);
    return (ParamExternDataInfo *)lfirst(lc);
}

void
setParamsExternIntoForeginDirect(List *fdw_private, ParamExternDataInfo *paramsExtern)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return;
    lc = list_nth_cell(fdw_private, FdwDirectModifyPrivateParamsExtern);
    lfirst(lc) = (void *)paramsExtern;
}

ParamExternDataInfo *
getParamsExternFromForeginDirect(List *fdw_private)
{
    ListCell *lc;

    if(fdw_private == NULL) 
        return NULL;
    lc = list_nth_cell(fdw_private, FdwDirectModifyPrivateParamsExtern);
    return (ParamExternDataInfo *)lfirst(lc);
}

Oid
getTargetRelOidFromForeginDirect(List *fdw_private)
{
    if(fdw_private == NULL) 
        return InvalidOid;
    return intVal(list_nth(fdw_private, FdwDirectModifyPrivateTargetRelOid));
}

char * 
getPreparedStmtName(char *sql, List *reloids_list)
{
    char        *result = NULL;
    LOCKMODE    heap_lock =  AccessShareLock;
    Oid         sql_prepared_oid = InvalidOid;
    Oid         sql_index_oid = InvalidOid;
    Oid         sql_query_idx_oid = InvalidOid;
    Relation    sql_preapred_rel,
                sql_index_rel;
    Snapshot    snapshot;
    ScanKeyData     key;
    Datum       hash_val;
    bool        is_deleted = false;

    sql_prepared_oid = GetPolarxSqlPreparedOid();
    sql_index_oid = GetPolarxSqlPreparedIdxOid();
    sql_query_idx_oid = GetPolarxSqlQueryIdxOid();
    if(sql_prepared_oid == InvalidOid ||
            sql_index_oid == InvalidOid || 
            sql_query_idx_oid == InvalidOid )
        return NULL;
    sql_preapred_rel = heap_open(sql_prepared_oid, heap_lock);
    sql_index_rel = index_open(sql_index_oid, heap_lock);
    if(sql_preapred_rel == NULL ||
            sql_index_rel == NULL)
        return NULL;
    
    hash_val = hash_any((unsigned char *) sql, strlen(sql));
    ScanKeyInit(&key, Anum_sql_hash, BTEqualStrategyNumber, F_INT4EQ, hash_val);
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    result = get_preapred_name(snapshot, sql_preapred_rel,
                                sql_index_rel, &key, sql, NULL, &is_deleted);
    if(result == NULL)
    {
        int version = 0;

        UnregisterSnapshot(snapshot);
        index_close(sql_index_rel, heap_lock);
        heap_close(sql_preapred_rel, heap_lock);

        heap_lock = AccessExclusiveLock;
        sql_preapred_rel = heap_open(sql_prepared_oid, heap_lock);
        sql_index_rel = index_open(sql_index_oid, heap_lock);

        snapshot = RegisterSnapshot(GetLatestSnapshot());
        result = get_preapred_name(snapshot, sql_preapred_rel,
                                    sql_index_rel, &key, sql, &version, &is_deleted);
        if(result == NULL)
        {
            char        prep_name[NAMEDATALEN];
            Relation    sql_query_idx_rel;
            Relation    sql_reloids_idx_rel;
            int reloids_len = list_length(reloids_list);
            Datum       reloids = (Datum) 0;
            Oid         sql_reloids_oid = GetPolarxSqlOidsIdxOid();

            if (reloids_len)
            {
                int         pos;
                ListCell   *lc;
                ArrayType  *reloids_tmp;
                Datum      *reloids_arr = palloc(sizeof(Datum) * reloids_len);

                pos = 0;
                foreach(lc, reloids_list)
                {
                    reloids_arr[pos] = ObjectIdGetDatum(lfirst_oid(lc));
                    pos++;
                }
                reloids_tmp = construct_array(reloids_arr, reloids_len,
                                                OIDOID,
                                                sizeof(Oid), true, 'i');
                reloids = PointerGetDatum(reloids_tmp);

                pfree(reloids_arr);
            }
            sql_query_idx_rel = index_open(sql_query_idx_oid, heap_lock);
            sql_reloids_idx_rel = index_open(sql_reloids_oid, heap_lock);


            set_preapred_name(sql_preapred_rel, sql_index_rel,
                                sql_query_idx_rel,
                                sql_reloids_idx_rel,
                                hash_val, version +1,
                                reloids,
                                sql);
            index_close(sql_query_idx_rel, heap_lock);
            index_close(sql_reloids_idx_rel, heap_lock);
            snprintf(prep_name, sizeof(prep_name),
                    "%u_%d_%d", DatumGetUInt32(hash_val), version +1, 0);

            result = pstrdup(prep_name);
        }
    }
    if(is_deleted)
    {
        int reloids_len = list_length(reloids_list);
        Datum       reloids = (Datum) 0;

        if (reloids_len)
        {
            int         pos;
            ListCell   *lc;
            ArrayType  *reloids_tmp;
            Datum      *reloids_arr = palloc(sizeof(Datum) * reloids_len);

            pos = 0;
            foreach(lc, reloids_list)
            {
                reloids_arr[pos] = ObjectIdGetDatum(lfirst_oid(lc));
                pos++;
            }
            reloids_tmp = construct_array(reloids_arr, reloids_len,
                    OIDOID,
                    sizeof(Oid), true, 'i');
            reloids = PointerGetDatum(reloids_tmp);

            pfree(reloids_arr);
        }

        if(heap_lock == AccessShareLock)
        {
            UnregisterSnapshot(snapshot);
            index_close(sql_index_rel, heap_lock);
            heap_close(sql_preapred_rel, heap_lock);

            heap_lock = AccessExclusiveLock;
            sql_preapred_rel = heap_open(sql_prepared_oid, heap_lock);
            sql_index_rel = index_open(sql_index_oid, heap_lock);
        }

        snapshot = RegisterSnapshot(GetLatestSnapshot());
        search_and_update_reloids(snapshot, sql_preapred_rel,
                                    sql_index_rel, &key,
                                    sql, reloids);
    }
    UnregisterSnapshot(snapshot);
    index_close(sql_index_rel, heap_lock);
    heap_close(sql_preapred_rel, heap_lock);
    return result; 
}
static void
update_sql_prepared_reloids(HeapTuple old_tup,
                            Relation rel, Datum new_reloids)
{
    TupleDesc   rel_dsc;
    Datum       values[Anum_sql];
    bool        nulls[Anum_sql];
    bool        repl[Anum_sql];
    HeapTuple   new_tup;
    Relation    sql_reloids_idx_rel;
    Oid         sql_reloids_oid = GetPolarxSqlOidsIdxOid();

    sql_reloids_idx_rel = index_open(sql_reloids_oid, RowExclusiveLock);

    rel_dsc = RelationGetDescr(rel);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, false, sizeof(nulls));
    MemSet(repl, false, sizeof(repl));

    values[Anum_reloids -1] = new_reloids;
    repl[Anum_reloids-1] = true;

    new_tup = heap_modify_tuple(old_tup, rel_dsc, values, nulls, repl);
    simple_heap_update(rel, &old_tup->t_self, new_tup);

    index_insert_compat(sql_reloids_idx_rel,
            &values[Anum_reloids-1], &nulls[Anum_reloids-1],
            &(new_tup->t_self),
            rel,
            UNIQUE_CHECK_NO);
    heap_freetuple(new_tup);
    index_close(sql_reloids_idx_rel, RowExclusiveLock);
}

static void 
search_and_update_reloids(Snapshot snapshot, Relation sql_preapred_rel,
                     Relation sql_index_rel, ScanKey key, char *sql, Datum new_reloids)
{
    IndexScanDesc   sql_index_scan;
    HeapTuple       htup = NULL;
   
    sql_index_scan = index_beginscan(sql_preapred_rel, sql_index_rel, snapshot, 1, 0);
    index_rescan(sql_index_scan, key, 1, NULL, 0);

    while ((htup = index_getnext(sql_index_scan, ForwardScanDirection)) != NULL)
    {
        Datum       search_values[Anum_sql];
        bool        search_nulls[Anum_sql];

        heap_deform_tuple(htup, sql_preapred_rel->rd_att,
                search_values, search_nulls);

        if(memcmp(TextDatumGetCString(search_values[Anum_sql - 1]),
                    sql, strlen(sql)) == 0)
        {
            update_sql_prepared_reloids(htup, sql_preapred_rel, new_reloids);
            break;
        }
    }

    index_endscan(sql_index_scan);
}

static char * 
get_preapred_name(Snapshot snapshot, Relation sql_preapred_rel,
                     Relation sql_index_rel, ScanKey key,
                     char *sql, int *max_version, bool *is_deleted)
{
    char        prep_name[NAMEDATALEN];
    IndexScanDesc   sql_index_scan;
    HeapTuple       htup = NULL;
    bool            found = false;
    int             count = 0;
   
    sql_index_scan = index_beginscan(sql_preapred_rel, sql_index_rel, snapshot, 1, 0);
    index_rescan(sql_index_scan, key, 1, NULL, 0);

    while ((htup = index_getnext(sql_index_scan, ForwardScanDirection)) != NULL)
    {
        Datum       search_values[Anum_sql];
        bool        search_nulls[Anum_sql];

        heap_deform_tuple(htup, sql_preapred_rel->rd_att,
                search_values, search_nulls);

        if(search_values[Anum_version - 1] >= count)
            count = search_values[Anum_version - 1];
        if(memcmp(TextDatumGetCString(search_values[Anum_sql - 1]),
                    sql, strlen(sql)) == 0)
        {
            snprintf(prep_name, sizeof(prep_name),"%u_%d_%d",
                    DatumGetUInt32(search_values[Anum_sql_hash - 1]),
                    DatumGetUInt32(search_values[Anum_version - 1]),
                    DatumGetUInt32(search_values[Anum_self_vs - 1]));
            count = search_values[Anum_version - 1];
            if(is_deleted && search_nulls[Anum_reloids - 1])
               *is_deleted = true; 
            found = true;
            break;
        }
    }

    index_endscan(sql_index_scan);

    if(max_version)
        *max_version = count;
    if(!found)
        return NULL;
    return pstrdup(prep_name);
}

static void 
set_preapred_name(Relation sql_preapred_rel, Relation sql_index_rel,
                    Relation sql_query_idx_rel,
                    Relation sql_reloids_idx_rel,
                    Datum sql_hash, Datum version,
                    Datum reloids, char *sql)
{
    Datum       values[Anum_sql];
    bool        nulls[Anum_sql];
    HeapTuple       tuple;
    bool    unconflict = false;
    uint32      specToken;

    MemSet(nulls, 0, sizeof(nulls));

    values[Anum_sql_hash -1] = sql_hash;
    values[Anum_version -1] = version;
    values[Anum_self_vs -1] = 0;
    values[Anum_sql_md5 -1] = md5_text_local(cstring_to_text(sql));
    values[Anum_reloids -1] = reloids;
    values[Anum_sql -1] = CStringGetTextDatum(sql);

    tuple = heap_form_tuple(sql_preapred_rel->rd_att, values, nulls);
    specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());
    HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);

    heap_insert(sql_preapred_rel, tuple,
            GetCurrentCommandId(true), HEAP_INSERT_SPECULATIVE, NULL);
    unconflict = index_insert_compat(sql_query_idx_rel,
                &values[Anum_sql_md5-1], &nulls[Anum_sql_md5-1],
                &(tuple->t_self),
                sql_preapred_rel,
                UNIQUE_CHECK_PARTIAL);
        
    if(unconflict)
    {
        index_insert_compat(sql_index_rel,
            values, nulls,
            &(tuple->t_self),
            sql_preapred_rel,
            UNIQUE_CHECK_NO);
        index_insert_compat(sql_reloids_idx_rel,
            &values[Anum_reloids-1], &nulls[Anum_reloids-1],
            &(tuple->t_self),
            sql_preapred_rel,
            UNIQUE_CHECK_NO);
        heap_finish_speculative(sql_preapred_rel, tuple);
    }
    else
    {
        unconflict = index_insert_compat(sql_index_rel,
            values, nulls,
            &(tuple->t_self),
            sql_preapred_rel,
            UNIQUE_CHECK_PARTIAL);
        heap_abort_speculative(sql_preapred_rel, tuple); 
        if(unconflict)
        {
            elog(ERROR, "insert into sql_prepared with diffrent sql, \
                    but same hash value at same time is not supported.");
        }
    }
    SpeculativeInsertionLockRelease(GetCurrentTransactionId());

    CommandCounterIncrement();
}

static int
prepare_foreign_sql(const char *p_name, const char *query,
                        List *node_list,int *prep_conns)
{
	int		i = 0;
	int		j = 0;
	int		conn_num = list_length(node_list);
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle      **connections;
	PGXCNodeHandle      *res_conns[conn_num];
	ResponseCombiner    combiner;
	ListCell        *lc = NULL;
	int 		complete_num = 0;
    char *prep_name = NULL;
    int  self_version;

	Assert(conn_num > 0);
    Assert(p_name != NULL);

	all_handles = get_handles(node_list, NIL, false, true);
	connections = all_handles->datanode_handles;
    self_version = GetPrepStmtNameAndSelfVersion(p_name, &prep_name);
	foreach(lc, node_list)
	{
		int index = lfirst_int(lc);
        bool is_expired = false;
        bool is_prepared = false;
        
        is_prepared = IsStmtPrepared(prep_name,
                                        PGXCNodeGetNodeId(connections[j]->nodeoid, NULL),
                                        self_version,
                                        &is_expired);
        if(is_prepared && is_expired == false)
        {
                j++;
                continue;
        }

        if (connections[j]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[j]);
        if(is_expired)
        {
            pgxc_node_send_close(connections[j], true, prep_name);
        }
        if (pgxc_node_send_parse(connections[j], prep_name, query, 0, NULL) != 0)
		{
			PGXCNodeSetConnectionState(connections[j],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to prepare statement: %s, in node: %d", query, connections[j]->nodeoid)));
		}
		if (pgxc_node_send_sync(connections[j]) != 0)
		{
			PGXCNodeSetConnectionState(connections[j],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		}
		PGXCNodeSetConnectionState(connections[j], DN_CONNECTION_STATE_QUERY);
        if(prep_conns)
            prep_conns[i] = index;
        res_conns[i] = connections[j];
		i++;
        j++;
	}
	if(i == 0)
		return 0;
    conn_num = i;
    j = i;
	InitResponseCombiner(&combiner, conn_num, COMBINE_TYPE_NONE);
	memset(&combiner, 0, sizeof(ScanState));
	combiner.request_type = REQUEST_TYPE_QUERY;
    if(p_name)
    {
        combiner.prep_name = pstrdup(p_name);
    }

	while (conn_num > 0)
	{
		if (pgxc_node_receive(conn_num, res_conns, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send prepared in receive result")));
		i = 0;
		while (i < conn_num)
		{
			int res = handle_response(res_conns[i], &combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_READY || res_conns[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
			{
				if(res == RESPONSE_READY)
                {
                    complete_num++;
                }
				if (--conn_num > i)
				{
					res_conns[i] = res_conns[conn_num];
				}
			}
			else
			{
			}
		}
	}

	if(complete_num != j)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to send prepared due to not complete all")));
	}

	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
    return complete_num;
}

static bool 
check_hashjoinable(Expr *clause)
{
    Oid         opno;
    Node       *leftarg;

    if (!is_opclause(clause))
        return false;
    if (list_length(((OpExpr *) clause)->args) != 2)
        return false;

    opno = ((OpExpr *) clause)->opno;
    leftarg = linitial(((OpExpr *) clause)->args);

    if (op_hashjoinable(opno, exprType(leftarg)) &&
            !contain_volatile_functions((Node *) clause))
        return true;
    return false;
}
int
evalTargetNodeFromForeginScan(List *fdw_private, ParamExternDataInfo *paramsExtern, bool *is_replicate)
{
    ListCell *lc = NULL;
    DistributionForParam *dist_for_param = NULL;
    List *node_list;
    
    if(fdw_private == NULL)
        return -1;

    if(paramsExtern == NULL)
        return -1;
    lc = list_nth_cell(fdw_private, FdwScanPrivateDistByParam);
    dist_for_param = (DistributionForParam *)lfirst(lc);

    if(dist_for_param == NULL)
        return -1;
    if(dist_for_param->paramId == 0 && is_replicate != NULL)
    {
        *is_replicate = true;
    }

    node_list = GetRelationNodesWithDistAndParam(dist_for_param,  paramsExtern->param_list_info,
                                        (List *) list_nth(fdw_private, FdwScanPrivateNodeList));

    if(node_list == NULL || list_length(node_list) != 1)
        return -1;

    return linitial_int(node_list);
}

static Datum
md5_text_local(text *in_text)
{
    size_t      len;
    char        hexsum[32 + 1];

    /* Calculate the length of the buffer using varlena metadata */
    len = VARSIZE_ANY_EXHDR(in_text);

    /* get the hash result */
    if (pg_md5_hash(VARDATA_ANY(in_text), len, hexsum) == false)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory")));

    /* convert to text and return it */
    PG_RETURN_TEXT_P(cstring_to_text(hexsum));
}

static List * 
get_all_reloids(PlannerInfo *root, RelOptInfo *rel)
{
    int         relid = -1;
    PgFdwRelationInfo *fpinfo = (PgFdwRelationInfo *) rel->fdw_private;
    List *res = NIL;

    while ((relid = bms_next_member(rel->relids, relid)) >= 0)
    {
        RangeTblEntry *rte;
        if (bms_is_member(relid, fpinfo->lower_subquery_rels))
            continue;
        rte = planner_rt_fetch(relid, root); 
        if(rte->rtekind == RTE_RELATION)
        {
            if(res == NIL)
                res = list_make1_oid(rte->relid);
            else
                res = list_append_unique_oid(res, rte->relid);
        }
    }
    relid = -1;
    while ((relid = bms_next_member(fpinfo->lower_subquery_rels, relid)) >= 0)
    {
        RangeTblEntry *rte;

        rte = planner_rt_fetch(relid, root); 
        if(rte->rtekind == RTE_RELATION)
        {
            if(res == NIL)
                res = list_make1_oid(rte->relid);
            else
                res = list_append_unique_oid(res, rte->relid);
        }
    }

    return res;
}
