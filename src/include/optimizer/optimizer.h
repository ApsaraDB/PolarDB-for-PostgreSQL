/*-------------------------------------------------------------------------
 *
 * optimizer.h
 *	  External API for the Postgres planner.
 *
 * This header is meant to define everything that the core planner
 * exposes for use by non-planner modules.
 *
 * Note that there are files outside src/backend/optimizer/ that are
 * considered planner modules, because they're too much in bed with
 * planner operations to be treated otherwise.  FDW planning code is an
 * example.  For the most part, however, code outside the core planner
 * should not need to include any optimizer/ header except this one.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/optimizer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "nodes/parsenodes.h"

typedef struct ExplainState ExplainState;	/* defined in explain_state.h */

/*
 * We don't want to include nodes/pathnodes.h here, because non-planner
 * code should generally treat PlannerInfo as an opaque typedef.
 * But we'd like such code to use that typedef name, so define the
 * typedef both here and in pathnodes.h.
 */
typedef struct PlannerInfo PlannerInfo;
typedef struct IndexOptInfo IndexOptInfo;
typedef struct SpecialJoinInfo SpecialJoinInfo;

/* It also seems best not to include plannodes.h, params.h, or htup.h here */
typedef struct PlannedStmt PlannedStmt;
typedef struct ParamListInfoData *ParamListInfo;
typedef struct HeapTupleData *HeapTuple;


/* in path/clausesel.c: */

extern Selectivity clause_selectivity(PlannerInfo *root,
									  Node *clause,
									  int varRelid,
									  JoinType jointype,
									  SpecialJoinInfo *sjinfo);
extern Selectivity clause_selectivity_ext(PlannerInfo *root,
										  Node *clause,
										  int varRelid,
										  JoinType jointype,
										  SpecialJoinInfo *sjinfo,
										  bool use_extended_stats);
extern Selectivity clauselist_selectivity(PlannerInfo *root,
										  List *clauses,
										  int varRelid,
										  JoinType jointype,
										  SpecialJoinInfo *sjinfo);
extern Selectivity clauselist_selectivity_ext(PlannerInfo *root,
											  List *clauses,
											  int varRelid,
											  JoinType jointype,
											  SpecialJoinInfo *sjinfo,
											  bool use_extended_stats);

/* in path/costsize.c: */

/* widely used cost parameters */
extern PGDLLIMPORT double seq_page_cost;
extern PGDLLIMPORT double random_page_cost;
extern PGDLLIMPORT double cpu_tuple_cost;
extern PGDLLIMPORT double cpu_index_tuple_cost;
extern PGDLLIMPORT double cpu_operator_cost;
extern PGDLLIMPORT double parallel_tuple_cost;
extern PGDLLIMPORT double parallel_setup_cost;
extern PGDLLIMPORT double recursive_worktable_factor;
extern PGDLLIMPORT int effective_cache_size;

extern double clamp_row_est(double nrows);
extern int32 clamp_width_est(int64 tuple_width);
extern long clamp_cardinality_to_long(Cardinality x);

/* in path/indxpath.c: */

extern bool is_pseudo_constant_for_index(PlannerInfo *root, Node *expr,
										 IndexOptInfo *index);

/* in plan/planner.c: */

/* possible values for debug_parallel_query */
typedef enum
{
	DEBUG_PARALLEL_OFF,
	DEBUG_PARALLEL_ON,
	DEBUG_PARALLEL_REGRESS,
}			DebugParallelMode;

/* GUC parameters */
extern PGDLLIMPORT int debug_parallel_query;
extern PGDLLIMPORT bool parallel_leader_participation;
extern PGDLLIMPORT bool enable_distinct_reordering;

extern PlannedStmt *planner(Query *parse, const char *query_string,
							int cursorOptions,
							ParamListInfo boundParams,
							ExplainState *es);

extern Expr *expression_planner(Expr *expr);
extern Expr *expression_planner_with_deps(Expr *expr,
										  List **relationOids,
										  List **invalItems);

extern bool plan_cluster_use_sort(Oid tableOid, Oid indexOid);
extern int	plan_create_index_workers(Oid tableOid, Oid indexOid);

/* in plan/setrefs.c: */

extern void extract_query_dependencies(Node *query,
									   List **relationOids,
									   List **invalItems,
									   bool *hasRowSecurity);

/* in prep/prepqual.c: */

extern Node *negate_clause(Node *node);
extern Expr *canonicalize_qual(Expr *qual, bool is_check);

/* in util/clauses.c: */

extern bool contain_mutable_functions(Node *clause);
extern bool contain_mutable_functions_after_planning(Expr *expr);
extern bool contain_volatile_functions(Node *clause);
extern bool contain_volatile_functions_after_planning(Expr *expr);
extern bool contain_volatile_functions_not_nextval(Node *clause);

extern Node *eval_const_expressions(PlannerInfo *root, Node *node);

extern void convert_saop_to_hashed_saop(Node *node);

extern Node *estimate_expression_value(PlannerInfo *root, Node *node);

extern Expr *evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
						   Oid result_collation);

extern bool var_is_nonnullable(PlannerInfo *root, Var *var, bool use_rel_info);

extern List *expand_function_arguments(List *args, bool include_out_arguments,
									   Oid result_type,
									   HeapTuple func_tuple);

extern ScalarArrayOpExpr *make_SAOP_expr(Oid oper, Node *leftexpr,
										 Oid coltype, Oid arraycollid,
										 Oid inputcollid, List *exprs,
										 bool haveNonConst);

/* in util/predtest.c: */

extern bool predicate_implied_by(List *predicate_list, List *clause_list,
								 bool weak);
extern bool predicate_refuted_by(List *predicate_list, List *clause_list,
								 bool weak);

/* in util/tlist.c: */

extern int	count_nonjunk_tlist_entries(List *tlist);
extern TargetEntry *get_sortgroupref_tle(Index sortref,
										 List *targetList);
extern TargetEntry *get_sortgroupclause_tle(SortGroupClause *sgClause,
											List *targetList);
extern Node *get_sortgroupclause_expr(SortGroupClause *sgClause,
									  List *targetList);
extern List *get_sortgrouplist_exprs(List *sgClauses,
									 List *targetList);
extern SortGroupClause *get_sortgroupref_clause(Index sortref,
												List *clauses);
extern SortGroupClause *get_sortgroupref_clause_noerr(Index sortref,
													  List *clauses);

/* in util/var.c: */

/* Bits that can be OR'd into the flags argument of pull_var_clause() */
#define PVC_INCLUDE_AGGREGATES	0x0001	/* include Aggrefs in output list */
#define PVC_RECURSE_AGGREGATES	0x0002	/* recurse into Aggref arguments */
#define PVC_INCLUDE_WINDOWFUNCS 0x0004	/* include WindowFuncs in output list */
#define PVC_RECURSE_WINDOWFUNCS 0x0008	/* recurse into WindowFunc arguments */
#define PVC_INCLUDE_PLACEHOLDERS	0x0010	/* include PlaceHolderVars in
											 * output list */
#define PVC_RECURSE_PLACEHOLDERS	0x0020	/* recurse into PlaceHolderVar
											 * arguments */
#define PVC_INCLUDE_CONVERTROWTYPES	0x0040	/* include ConvertRowtypeExprs in
											 * output list */

extern Bitmapset *pull_varnos(PlannerInfo *root, Node *node);
extern Bitmapset *pull_varnos_of_level(PlannerInfo *root, Node *node, int levelsup);
extern void pull_varattnos(Node *node, Index varno, Bitmapset **varattnos);
extern List *pull_vars_of_level(Node *node, int levelsup);
extern bool contain_var_clause(Node *node);
extern bool contain_vars_of_level(Node *node, int levelsup);
extern bool contain_vars_returning_old_or_new(Node *node);
extern int	locate_var_of_level(Node *node, int levelsup);
extern List *pull_var_clause(Node *node, int flags);
extern Node *flatten_join_alias_vars(PlannerInfo *root, Query *query, Node *node);
extern Node *flatten_group_exprs(PlannerInfo *root, Query *query, Node *node);

#endif							/* OPTIMIZER_H */
