/*-------------------------------------------------------------------------
 *
 * joinpath.c
 *	  Routines to find all possible paths for processing a set of joins
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/joinpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

/* Hook for plugins to get control in add_paths_to_joinrel() */
set_join_pathlist_hook_type set_join_pathlist_hook = NULL;

/*
 * Paths parameterized by a parent rel can be considered to be parameterized
 * by any of its children, when we are performing partitionwise joins.  These
 * macros simplify checking for such cases.  Beware multiple eval of args.
 */
#define PATH_PARAM_BY_PARENT(path, rel)	\
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path),	\
									   (rel)->top_parent_relids))
#define PATH_PARAM_BY_REL_SELF(path, rel)  \
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

#define PATH_PARAM_BY_REL(path, rel)	\
	(PATH_PARAM_BY_REL_SELF(path, rel) || PATH_PARAM_BY_PARENT(path, rel))

static void try_partial_mergejoin_path(PlannerInfo *root,
									   RelOptInfo *joinrel,
									   Path *outer_path,
									   Path *inner_path,
									   List *pathkeys,
									   List *mergeclauses,
									   List *outersortkeys,
									   List *innersortkeys,
									   JoinType jointype,
									   JoinPathExtraData *extra);
static void sort_inner_and_outer(PlannerInfo *root, RelOptInfo *joinrel,
								 RelOptInfo *outerrel, RelOptInfo *innerrel,
								 JoinType jointype, JoinPathExtraData *extra);
static void match_unsorted_outer(PlannerInfo *root, RelOptInfo *joinrel,
								 RelOptInfo *outerrel, RelOptInfo *innerrel,
								 JoinType jointype, JoinPathExtraData *extra);
static void consider_parallel_nestloop(PlannerInfo *root,
									   RelOptInfo *joinrel,
									   RelOptInfo *outerrel,
									   RelOptInfo *innerrel,
									   JoinType jointype,
									   JoinPathExtraData *extra);
static void consider_parallel_mergejoin(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outerrel,
										RelOptInfo *innerrel,
										JoinType jointype,
										JoinPathExtraData *extra,
										Path *inner_cheapest_total);
static void hash_inner_and_outer(PlannerInfo *root, RelOptInfo *joinrel,
								 RelOptInfo *outerrel, RelOptInfo *innerrel,
								 JoinType jointype, JoinPathExtraData *extra);
static List *select_mergejoin_clauses(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  RelOptInfo *outerrel,
									  RelOptInfo *innerrel,
									  List *restrictlist,
									  JoinType jointype,
									  bool *mergejoin_allowed);
static void generate_mergejoin_paths(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *innerrel,
									 Path *outerpath,
									 JoinType jointype,
									 JoinPathExtraData *extra,
									 bool useallclauses,
									 Path *inner_cheapest_total,
									 List *merge_pathkeys,
									 bool is_partial);


/*
 * add_paths_to_joinrel
 *	  Given a join relation and two component rels from which it can be made,
 *	  consider all possible paths that use the two component rels as outer
 *	  and inner rel respectively.  Add these paths to the join rel's pathlist
 *	  if they survive comparison with other paths (and remove any existing
 *	  paths that are dominated by these paths).
 *
 * Modifies the pathlist field of the joinrel node to contain the best
 * paths found so far.
 *
 * jointype is not necessarily the same as sjinfo->jointype; it might be
 * "flipped around" if we are considering joining the rels in the opposite
 * direction from what's indicated in sjinfo.
 *
 * Also, this routine accepts the special JoinTypes JOIN_UNIQUE_OUTER and
 * JOIN_UNIQUE_INNER to indicate that the outer or inner relation has been
 * unique-ified and a regular inner join should then be applied.  These values
 * are not allowed to propagate outside this routine, however.  Path cost
 * estimation code, as well as match_unsorted_outer, may need to recognize that
 * it's dealing with such a case --- the combination of nominal jointype INNER
 * with sjinfo->jointype == JOIN_SEMI indicates that.
 */
void
add_paths_to_joinrel(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 SpecialJoinInfo *sjinfo,
					 List *restrictlist)
{
	JoinType	save_jointype = jointype;
	JoinPathExtraData extra;
	bool		mergejoin_allowed = true;
	ListCell   *lc;
	Relids		joinrelids;

	/*
	 * PlannerInfo doesn't contain the SpecialJoinInfos created for joins
	 * between child relations, even if there is a SpecialJoinInfo node for
	 * the join between the topmost parents. So, while calculating Relids set
	 * representing the restriction, consider relids of topmost parent of
	 * partitions.
	 */
	if (joinrel->reloptkind == RELOPT_OTHER_JOINREL)
		joinrelids = joinrel->top_parent_relids;
	else
		joinrelids = joinrel->relids;

	extra.restrictlist = restrictlist;
	extra.mergeclause_list = NIL;
	extra.sjinfo = sjinfo;
	extra.param_source_rels = NULL;

	/*
	 * See if the inner relation is provably unique for this outer rel.
	 *
	 * We have some special cases: for JOIN_SEMI, it doesn't matter since the
	 * executor can make the equivalent optimization anyway.  It also doesn't
	 * help enable use of Memoize, since a semijoin with a provably unique
	 * inner side should have been reduced to an inner join in that case.
	 * Therefore, we need not expend planner cycles on proofs.  (For
	 * JOIN_ANTI, although it doesn't help the executor for the same reason,
	 * it can benefit Memoize paths.)  For JOIN_UNIQUE_INNER, we must be
	 * considering a semijoin whose inner side is not provably unique (else
	 * reduce_unique_semijoins would've simplified it), so there's no point in
	 * calling innerrel_is_unique.  However, if the LHS covers all of the
	 * semijoin's min_lefthand, then it's appropriate to set inner_unique
	 * because the unique relation produced by create_unique_paths will be
	 * unique relative to the LHS.  (If we have an LHS that's only part of the
	 * min_lefthand, that is *not* true.)  For JOIN_UNIQUE_OUTER, pass
	 * JOIN_INNER to avoid letting that value escape this module.
	 */
	switch (jointype)
	{
		case JOIN_SEMI:
			extra.inner_unique = false; /* well, unproven */
			break;
		case JOIN_UNIQUE_INNER:
			extra.inner_unique = bms_is_subset(sjinfo->min_lefthand,
											   outerrel->relids);
			break;
		case JOIN_UNIQUE_OUTER:
			extra.inner_unique = innerrel_is_unique(root,
													joinrel->relids,
													outerrel->relids,
													innerrel,
													JOIN_INNER,
													restrictlist,
													false);
			break;
		default:
			extra.inner_unique = innerrel_is_unique(root,
													joinrel->relids,
													outerrel->relids,
													innerrel,
													jointype,
													restrictlist,
													false);
			break;
	}

	/*
	 * If the outer or inner relation has been unique-ified, handle as a plain
	 * inner join.
	 */
	if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
		jointype = JOIN_INNER;

	/*
	 * Find potential mergejoin clauses.  We can skip this if we are not
	 * interested in doing a mergejoin.  However, mergejoin may be our only
	 * way of implementing a full outer join, so override enable_mergejoin if
	 * it's a full join.
	 */
	if (enable_mergejoin || jointype == JOIN_FULL)
		extra.mergeclause_list = select_mergejoin_clauses(root,
														  joinrel,
														  outerrel,
														  innerrel,
														  restrictlist,
														  jointype,
														  &mergejoin_allowed);

	/*
	 * If it's SEMI, ANTI, or inner_unique join, compute correction factors
	 * for cost estimation.  These will be the same for all paths.
	 */
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique)
		compute_semi_anti_join_factors(root, joinrel, outerrel, innerrel,
									   jointype, sjinfo, restrictlist,
									   &extra.semifactors);

	/*
	 * Decide whether it's sensible to generate parameterized paths for this
	 * joinrel, and if so, which relations such paths should require.  There
	 * is usually no need to create a parameterized result path unless there
	 * is a join order restriction that prevents joining one of our input rels
	 * directly to the parameter source rel instead of joining to the other
	 * input rel.  (But see allow_star_schema_join().)	This restriction
	 * reduces the number of parameterized paths we have to deal with at
	 * higher join levels, without compromising the quality of the resulting
	 * plan.  We express the restriction as a Relids set that must overlap the
	 * parameterization of any proposed join path.  Note: param_source_rels
	 * should contain only baserels, not OJ relids, so starting from
	 * all_baserels not all_query_rels is correct.
	 */
	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo2 = (SpecialJoinInfo *) lfirst(lc);

		/*
		 * SJ is relevant to this join if we have some part of its RHS
		 * (possibly not all of it), and haven't yet joined to its LHS.  (This
		 * test is pretty simplistic, but should be sufficient considering the
		 * join has already been proven legal.)  If the SJ is relevant, it
		 * presents constraints for joining to anything not in its RHS.
		 */
		if (bms_overlap(joinrelids, sjinfo2->min_righthand) &&
			!bms_overlap(joinrelids, sjinfo2->min_lefthand))
			extra.param_source_rels = bms_join(extra.param_source_rels,
											   bms_difference(root->all_baserels,
															  sjinfo2->min_righthand));

		/* full joins constrain both sides symmetrically */
		if (sjinfo2->jointype == JOIN_FULL &&
			bms_overlap(joinrelids, sjinfo2->min_lefthand) &&
			!bms_overlap(joinrelids, sjinfo2->min_righthand))
			extra.param_source_rels = bms_join(extra.param_source_rels,
											   bms_difference(root->all_baserels,
															  sjinfo2->min_lefthand));
	}

	/*
	 * However, when a LATERAL subquery is involved, there will simply not be
	 * any paths for the joinrel that aren't parameterized by whatever the
	 * subquery is parameterized by, unless its parameterization is resolved
	 * within the joinrel.  So we might as well allow additional dependencies
	 * on whatever residual lateral dependencies the joinrel will have.
	 */
	extra.param_source_rels = bms_add_members(extra.param_source_rels,
											  joinrel->lateral_relids);

	/*
	 * 1. Consider mergejoin paths where both relations must be explicitly
	 * sorted.  Skip this if we can't mergejoin.
	 */
	if (mergejoin_allowed)
		sort_inner_and_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);

	/*
	 * 2. Consider paths where the outer relation need not be explicitly
	 * sorted. This includes both nestloops and mergejoins where the outer
	 * path is already ordered.  Again, skip this if we can't mergejoin.
	 * (That's okay because we know that nestloop can't handle
	 * right/right-anti/right-semi/full joins at all, so it wouldn't work in
	 * the prohibited cases either.)
	 */
	if (mergejoin_allowed)
		match_unsorted_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);

#ifdef NOT_USED

	/*
	 * 3. Consider paths where the inner relation need not be explicitly
	 * sorted.  This includes mergejoins only (nestloops were already built in
	 * match_unsorted_outer).
	 *
	 * Diked out as redundant 2/13/2000 -- tgl.  There isn't any really
	 * significant difference between the inner and outer side of a mergejoin,
	 * so match_unsorted_inner creates no paths that aren't equivalent to
	 * those made by match_unsorted_outer when add_paths_to_joinrel() is
	 * invoked with the two rels given in the other order.
	 */
	if (mergejoin_allowed)
		match_unsorted_inner(root, joinrel, outerrel, innerrel,
							 jointype, &extra);
#endif

	/*
	 * 4. Consider paths where both outer and inner relations must be hashed
	 * before being joined.  As above, disregard enable_hashjoin for full
	 * joins, because there may be no other alternative.
	 */
	if (enable_hashjoin || jointype == JOIN_FULL)
		hash_inner_and_outer(root, joinrel, outerrel, innerrel,
							 jointype, &extra);

	/*
	 * 5. If inner and outer relations are foreign tables (or joins) belonging
	 * to the same server and assigned to the same user to check access
	 * permissions as, give the FDW a chance to push down joins.
	 */
	if (joinrel->fdwroutine &&
		joinrel->fdwroutine->GetForeignJoinPaths)
		joinrel->fdwroutine->GetForeignJoinPaths(root, joinrel,
												 outerrel, innerrel,
												 save_jointype, &extra);

	/*
	 * 6. Finally, give extensions a chance to manipulate the path list.  They
	 * could add new paths (such as CustomPaths) by calling add_path(), or
	 * add_partial_path() if parallel aware.  They could also delete or modify
	 * paths added by the core code.
	 */
	if (set_join_pathlist_hook)
		set_join_pathlist_hook(root, joinrel, outerrel, innerrel,
							   save_jointype, &extra);
}

/*
 * We override the param_source_rels heuristic to accept nestloop paths in
 * which the outer rel satisfies some but not all of the inner path's
 * parameterization.  This is necessary to get good plans for star-schema
 * scenarios, in which a parameterized path for a large table may require
 * parameters from multiple small tables that will not get joined directly to
 * each other.  We can handle that by stacking nestloops that have the small
 * tables on the outside; but this breaks the rule the param_source_rels
 * heuristic is based on, namely that parameters should not be passed down
 * across joins unless there's a join-order-constraint-based reason to do so.
 * So we ignore the param_source_rels restriction when this case applies.
 *
 * allow_star_schema_join() returns true if the param_source_rels restriction
 * should be overridden, ie, it's okay to perform this join.
 */
static inline bool
allow_star_schema_join(PlannerInfo *root,
					   Relids outerrelids,
					   Relids inner_paramrels)
{
	/*
	 * It's a star-schema case if the outer rel provides some but not all of
	 * the inner rel's parameterization.
	 */
	return (bms_overlap(inner_paramrels, outerrelids) &&
			bms_nonempty_difference(inner_paramrels, outerrelids));
}

/*
 * If the parameterization is only partly satisfied by the outer rel,
 * the unsatisfied part can't include any outer-join relids that could
 * null rels of the satisfied part.  That would imply that we're trying
 * to use a clause involving a Var with nonempty varnullingrels at
 * a join level where that value isn't yet computable.
 *
 * In practice, this test never finds a problem because earlier join order
 * restrictions prevent us from attempting a join that would cause a problem.
 * (That's unsurprising, because the code worked before we ever added
 * outer-join relids to expression relids.)  It still seems worth checking
 * as a backstop, but we only do so in assert-enabled builds.
 */
#ifdef USE_ASSERT_CHECKING
static inline bool
have_unsafe_outer_join_ref(PlannerInfo *root,
						   Relids outerrelids,
						   Relids inner_paramrels)
{
	bool		result = false;
	Relids		unsatisfied = bms_difference(inner_paramrels, outerrelids);
	Relids		satisfied = bms_intersect(inner_paramrels, outerrelids);

	if (bms_overlap(unsatisfied, root->outer_join_rels))
	{
		ListCell   *lc;

		foreach(lc, root->join_info_list)
		{
			SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);

			if (!bms_is_member(sjinfo->ojrelid, unsatisfied))
				continue;		/* not relevant */
			if (bms_overlap(satisfied, sjinfo->min_righthand) ||
				(sjinfo->jointype == JOIN_FULL &&
				 bms_overlap(satisfied, sjinfo->min_lefthand)))
			{
				result = true;	/* doesn't work */
				break;
			}
		}
	}

	/* Waste no memory when we reject a path here */
	bms_free(unsatisfied);
	bms_free(satisfied);

	return result;
}
#endif							/* USE_ASSERT_CHECKING */

/*
 * paraminfo_get_equal_hashops
 *		Determine if the clauses in param_info and innerrel's lateral vars
 *		can be hashed.
 *		Returns true if hashing is possible, otherwise false.
 *
 * Additionally, on success we collect the outer expressions and the
 * appropriate equality operators for each hashable parameter to innerrel.
 * These are returned in parallel lists in *param_exprs and *operators.
 * We also set *binary_mode to indicate whether strict binary matching is
 * required.
 */
static bool
paraminfo_get_equal_hashops(PlannerInfo *root, ParamPathInfo *param_info,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *ph_lateral_vars, List **param_exprs,
							List **operators, bool *binary_mode)

{
	List	   *lateral_vars;
	ListCell   *lc;

	*param_exprs = NIL;
	*operators = NIL;
	*binary_mode = false;

	/* Add join clauses from param_info to the hash key */
	if (param_info != NULL)
	{
		List	   *clauses = param_info->ppi_clauses;

		foreach(lc, clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			OpExpr	   *opexpr;
			Node	   *expr;
			Oid			hasheqoperator;

			opexpr = (OpExpr *) rinfo->clause;

			/*
			 * Bail if the rinfo is not compatible.  We need a join OpExpr
			 * with 2 args.
			 */
			if (!IsA(opexpr, OpExpr) || list_length(opexpr->args) != 2 ||
				!clause_sides_match_join(rinfo, outerrel->relids,
										 innerrel->relids))
			{
				list_free(*operators);
				list_free(*param_exprs);
				return false;
			}

			if (rinfo->outer_is_left)
			{
				expr = (Node *) linitial(opexpr->args);
				hasheqoperator = rinfo->left_hasheqoperator;
			}
			else
			{
				expr = (Node *) lsecond(opexpr->args);
				hasheqoperator = rinfo->right_hasheqoperator;
			}

			/* can't do memoize if we can't hash the outer type */
			if (!OidIsValid(hasheqoperator))
			{
				list_free(*operators);
				list_free(*param_exprs);
				return false;
			}

			/*
			 * 'expr' may already exist as a parameter from a previous item in
			 * ppi_clauses.  No need to include it again, however we'd better
			 * ensure we do switch into binary mode if required.  See below.
			 */
			if (!list_member(*param_exprs, expr))
			{
				*operators = lappend_oid(*operators, hasheqoperator);
				*param_exprs = lappend(*param_exprs, expr);
			}

			/*
			 * When the join operator is not hashable then it's possible that
			 * the operator will be able to distinguish something that the
			 * hash equality operator could not. For example with floating
			 * point types -0.0 and +0.0 are classed as equal by the hash
			 * function and equality function, but some other operator may be
			 * able to tell those values apart.  This means that we must put
			 * memoize into binary comparison mode so that it does bit-by-bit
			 * comparisons rather than a "logical" comparison as it would
			 * using the hash equality operator.
			 */
			if (!OidIsValid(rinfo->hashjoinoperator))
				*binary_mode = true;
		}
	}

	/* Now add any lateral vars to the cache key too */
	lateral_vars = list_concat(ph_lateral_vars, innerrel->lateral_vars);
	foreach(lc, lateral_vars)
	{
		Node	   *expr = (Node *) lfirst(lc);
		TypeCacheEntry *typentry;

		/* Reject if there are any volatile functions in lateral vars */
		if (contain_volatile_functions(expr))
		{
			list_free(*operators);
			list_free(*param_exprs);
			return false;
		}

		typentry = lookup_type_cache(exprType(expr),
									 TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

		/* can't use memoize without a valid hash proc and equals operator */
		if (!OidIsValid(typentry->hash_proc) || !OidIsValid(typentry->eq_opr))
		{
			list_free(*operators);
			list_free(*param_exprs);
			return false;
		}

		/*
		 * 'expr' may already exist as a parameter from the ppi_clauses.  No
		 * need to include it again, however we'd better ensure we do switch
		 * into binary mode.
		 */
		if (!list_member(*param_exprs, expr))
		{
			*operators = lappend_oid(*operators, typentry->eq_opr);
			*param_exprs = lappend(*param_exprs, expr);
		}

		/*
		 * We must go into binary mode as we don't have too much of an idea of
		 * how these lateral Vars are being used.  See comment above when we
		 * set *binary_mode for the non-lateral Var case. This could be
		 * relaxed a bit if we had the RestrictInfos and knew the operators
		 * being used, however for cases like Vars that are arguments to
		 * functions we must operate in binary mode as we don't have
		 * visibility into what the function is doing with the Vars.
		 */
		*binary_mode = true;
	}

	/* We're okay to use memoize */
	return true;
}

/*
 * extract_lateral_vars_from_PHVs
 *	  Extract lateral references within PlaceHolderVars that are due to be
 *	  evaluated at 'innerrelids'.
 */
static List *
extract_lateral_vars_from_PHVs(PlannerInfo *root, Relids innerrelids)
{
	List	   *ph_lateral_vars = NIL;
	ListCell   *lc;

	/* Nothing would be found if the query contains no LATERAL RTEs */
	if (!root->hasLateralRTEs)
		return NIL;

	/*
	 * No need to consider PHVs that are due to be evaluated at joinrels,
	 * since we do not add Memoize nodes on top of joinrel paths.
	 */
	if (bms_membership(innerrelids) == BMS_MULTIPLE)
		return NIL;

	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);
		List	   *vars;
		ListCell   *cell;

		/* PHV is uninteresting if no lateral refs */
		if (phinfo->ph_lateral == NULL)
			continue;

		/* PHV is uninteresting if not due to be evaluated at innerrelids */
		if (!bms_equal(phinfo->ph_eval_at, innerrelids))
			continue;

		/*
		 * If the PHV does not reference any rels in innerrelids, use its
		 * contained expression as a cache key rather than extracting the
		 * Vars/PHVs from it and using those.  This can be beneficial in cases
		 * where the expression results in fewer distinct values to cache
		 * tuples for.
		 */
		if (!bms_overlap(pull_varnos(root, (Node *) phinfo->ph_var->phexpr),
						 innerrelids))
		{
			ph_lateral_vars = lappend(ph_lateral_vars, phinfo->ph_var->phexpr);
			continue;
		}

		/* Fetch Vars and PHVs of lateral references within PlaceHolderVars */
		vars = pull_vars_of_level((Node *) phinfo->ph_var->phexpr, 0);
		foreach(cell, vars)
		{
			Node	   *node = (Node *) lfirst(cell);

			if (IsA(node, Var))
			{
				Var		   *var = (Var *) node;

				Assert(var->varlevelsup == 0);

				if (bms_is_member(var->varno, phinfo->ph_lateral))
					ph_lateral_vars = lappend(ph_lateral_vars, node);
			}
			else if (IsA(node, PlaceHolderVar))
			{
				PlaceHolderVar *phv = (PlaceHolderVar *) node;

				Assert(phv->phlevelsup == 0);

				if (bms_is_subset(find_placeholder_info(root, phv)->ph_eval_at,
								  phinfo->ph_lateral))
					ph_lateral_vars = lappend(ph_lateral_vars, node);
			}
			else
				Assert(false);
		}

		list_free(vars);
	}

	return ph_lateral_vars;
}

/*
 * get_memoize_path
 *		If possible, make and return a Memoize path atop of 'inner_path'.
 *		Otherwise return NULL.
 *
 * Note that currently we do not add Memoize nodes on top of join relation
 * paths.  This is because the ParamPathInfos for join relation paths do not
 * maintain ppi_clauses, as the set of relevant clauses varies depending on how
 * the join is formed.  In addition, joinrels do not maintain lateral_vars.  So
 * we do not have a way to extract cache keys from joinrels.
 */
static Path *
get_memoize_path(PlannerInfo *root, RelOptInfo *innerrel,
				 RelOptInfo *outerrel, Path *inner_path,
				 Path *outer_path, JoinType jointype,
				 JoinPathExtraData *extra)
{
	List	   *param_exprs;
	List	   *hash_operators;
	ListCell   *lc;
	bool		binary_mode;
	List	   *ph_lateral_vars;

	/* Obviously not if it's disabled */
	if (!enable_memoize)
		return NULL;

	/*
	 * We can safely not bother with all this unless we expect to perform more
	 * than one inner scan.  The first scan is always going to be a cache
	 * miss.  This would likely fail later anyway based on costs, so this is
	 * really just to save some wasted effort.
	 */
	if (outer_path->parent->rows < 2)
		return NULL;

	/*
	 * Extract lateral Vars/PHVs within PlaceHolderVars that are due to be
	 * evaluated at innerrel.  These lateral Vars/PHVs could be used as
	 * memoize cache keys.
	 */
	ph_lateral_vars = extract_lateral_vars_from_PHVs(root, innerrel->relids);

	/*
	 * We can only have a memoize node when there's some kind of cache key,
	 * either parameterized path clauses or lateral Vars.  No cache key sounds
	 * more like something a Materialize node might be more useful for.
	 */
	if ((inner_path->param_info == NULL ||
		 inner_path->param_info->ppi_clauses == NIL) &&
		innerrel->lateral_vars == NIL &&
		ph_lateral_vars == NIL)
		return NULL;

	/*
	 * Currently we don't do this for SEMI and ANTI joins, because nested loop
	 * SEMI/ANTI joins don't scan the inner node to completion, which means
	 * memoize cannot mark the cache entry as complete.  Nor can we mark the
	 * cache entry as complete after fetching the first inner tuple, because
	 * if that tuple and the current outer tuple don't satisfy the join
	 * clauses, a second inner tuple that satisfies the parameters would find
	 * the cache entry already marked as complete.  The only exception is when
	 * the inner relation is provably unique, as in that case, there won't be
	 * a second matching tuple and we can safely mark the cache entry as
	 * complete after fetching the first inner tuple.  Note that in such
	 * cases, the SEMI join should have been reduced to an inner join by
	 * reduce_unique_semijoins.
	 */
	if ((jointype == JOIN_SEMI || jointype == JOIN_ANTI) &&
		!extra->inner_unique)
		return NULL;

	/*
	 * Memoize normally marks cache entries as complete when it runs out of
	 * tuples to read from its subplan.  However, with unique joins, Nested
	 * Loop will skip to the next outer tuple after finding the first matching
	 * inner tuple.  This means that we may not read the inner side of the
	 * join to completion which leaves no opportunity to mark the cache entry
	 * as complete.  To work around that, when the join is unique we
	 * automatically mark cache entries as complete after fetching the first
	 * tuple.  This works when the entire join condition is parameterized.
	 * Otherwise, when the parameterization is only a subset of the join
	 * condition, we can't be sure which part of it causes the join to be
	 * unique.  This means there are no guarantees that only 1 tuple will be
	 * read.  We cannot mark the cache entry as complete after reading the
	 * first tuple without that guarantee.  This means the scope of Memoize
	 * node's usefulness is limited to only outer rows that have no join
	 * partner as this is the only case where Nested Loop would exhaust the
	 * inner scan of a unique join.  Since the scope is limited to that, we
	 * just don't bother making a memoize path in this case.
	 *
	 * Lateral vars needn't be considered here as they're not considered when
	 * determining if the join is unique.
	 */
	if (extra->inner_unique)
	{
		Bitmapset  *ppi_serials;

		if (inner_path->param_info == NULL)
			return NULL;

		ppi_serials = inner_path->param_info->ppi_serials;

		foreach_node(RestrictInfo, rinfo, extra->restrictlist)
		{
			if (!bms_is_member(rinfo->rinfo_serial, ppi_serials))
				return NULL;
		}
	}

	/*
	 * We can't use a memoize node if there are volatile functions in the
	 * inner rel's target list or restrict list.  A cache hit could reduce the
	 * number of calls to these functions.
	 */
	if (contain_volatile_functions((Node *) innerrel->reltarget))
		return NULL;

	foreach(lc, innerrel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (contain_volatile_functions((Node *) rinfo))
			return NULL;
	}

	/*
	 * Also check the parameterized path restrictinfos for volatile functions.
	 * Indexed functions must be immutable so shouldn't have any volatile
	 * functions, however, with a lateral join the inner scan may not be an
	 * index scan.
	 */
	if (inner_path->param_info != NULL)
	{
		foreach(lc, inner_path->param_info->ppi_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (contain_volatile_functions((Node *) rinfo))
				return NULL;
		}
	}

	/* Check if we have hash ops for each parameter to the path */
	if (paraminfo_get_equal_hashops(root,
									inner_path->param_info,
									outerrel->top_parent ?
									outerrel->top_parent : outerrel,
									innerrel,
									ph_lateral_vars,
									&param_exprs,
									&hash_operators,
									&binary_mode))
	{
		return (Path *) create_memoize_path(root,
											innerrel,
											inner_path,
											param_exprs,
											hash_operators,
											extra->inner_unique,
											binary_mode,
											outer_path->rows);
	}

	return NULL;
}

/*
 * try_nestloop_path
 *	  Consider a nestloop join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_nestloop_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *pathkeys,
				  JoinType jointype,
				  JoinPathExtraData *extra)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;
	RelOptInfo *innerrel = inner_path->parent;
	RelOptInfo *outerrel = outer_path->parent;
	Relids		innerrelids;
	Relids		outerrelids;
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);

	/*
	 * If we are forming an outer join at this join, it's nonsensical to use
	 * an input path that uses the outer join as part of its parameterization.
	 * (This can happen despite our join order restrictions, since those apply
	 * to what is in an input relation not what its parameters are.)
	 */
	if (extra->sjinfo->ojrelid != 0 &&
		(bms_is_member(extra->sjinfo->ojrelid, inner_paramrels) ||
		 bms_is_member(extra->sjinfo->ojrelid, outer_paramrels)))
		return;

	/*
	 * Any parameterization of the input paths refers to topmost parents of
	 * the relevant relations, because reparameterize_path_by_child() hasn't
	 * been called yet.  So we must consider topmost parents of the relations
	 * being joined, too, while determining parameterization of the result and
	 * checking for disallowed parameterization cases.
	 */
	if (innerrel->top_parent_relids)
		innerrelids = innerrel->top_parent_relids;
	else
		innerrelids = innerrel->relids;

	if (outerrel->top_parent_relids)
		outerrelids = outerrel->top_parent_relids;
	else
		outerrelids = outerrel->relids;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible --- unless allow_star_schema_join
	 * says to allow it anyway.
	 */
	required_outer = calc_nestloop_required_outer(outerrelids, outer_paramrels,
												  innerrelids, inner_paramrels);
	if (required_outer &&
		!bms_overlap(required_outer, extra->param_source_rels) &&
		!allow_star_schema_join(root, outerrelids, inner_paramrels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/* If we got past that, we shouldn't have any unsafe outer-join refs */
	Assert(!have_unsafe_outer_join_ref(root, outerrelids, inner_paramrels));

	/*
	 * If the inner path is parameterized, it is parameterized by the topmost
	 * parent of the outer rel, not the outer rel itself.  We will need to
	 * translate the parameterization, if this path is chosen, during
	 * create_plan().  Here we just check whether we will be able to perform
	 * the translation, and if not avoid creating a nestloop path.
	 */
	if (PATH_PARAM_BY_PARENT(inner_path, outer_path->parent) &&
		!path_is_reparameterizable_by_child(inner_path, outer_path->parent))
	{
		bms_free(required_outer);
		return;
	}

	/*
	 * Do a precheck to quickly eliminate obviously-inferior paths.  We
	 * calculate a cheap lower bound on the path's cost and then use
	 * add_path_precheck() to see if the path is clearly going to be dominated
	 * by some existing path for the joinrel.  If not, do the full pushup with
	 * creating a fully valid path structure and submitting it to add_path().
	 * The latter two steps are expensive enough to make this two-phase
	 * methodology worthwhile.
	 */
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path, extra);

	if (add_path_precheck(joinrel, workspace.disabled_nodes,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
		add_path(joinrel, (Path *)
				 create_nestloop_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra,
									  outer_path,
									  inner_path,
									  extra->restrictlist,
									  pathkeys,
									  required_outer));
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_nestloop_path
 *	  Consider a partial nestloop join path; if it appears useful, push it into
 *	  the joinrel's partial_pathlist via add_partial_path().
 */
static void
try_partial_nestloop_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *pathkeys,
						  JoinType jointype,
						  JoinPathExtraData *extra)
{
	JoinCostWorkspace workspace;

	/*
	 * If the inner path is parameterized, the parameterization must be fully
	 * satisfied by the proposed outer path.  Parameterized partial paths are
	 * not supported.  The caller should already have verified that no lateral
	 * rels are required here.
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	Assert(bms_is_empty(PATH_REQ_OUTER(outer_path)));
	if (inner_path->param_info != NULL)
	{
		Relids		inner_paramrels = inner_path->param_info->ppi_req_outer;
		RelOptInfo *outerrel = outer_path->parent;
		Relids		outerrelids;

		/*
		 * The inner and outer paths are parameterized, if at all, by the top
		 * level parents, not the child relations, so we must use those relids
		 * for our parameterization tests.
		 */
		if (outerrel->top_parent_relids)
			outerrelids = outerrel->top_parent_relids;
		else
			outerrelids = outerrel->relids;

		if (!bms_is_subset(inner_paramrels, outerrelids))
			return;
	}

	/*
	 * If the inner path is parameterized, it is parameterized by the topmost
	 * parent of the outer rel, not the outer rel itself.  We will need to
	 * translate the parameterization, if this path is chosen, during
	 * create_plan().  Here we just check whether we will be able to perform
	 * the translation, and if not avoid creating a nestloop path.
	 */
	if (PATH_PARAM_BY_PARENT(inner_path, outer_path->parent) &&
		!path_is_reparameterizable_by_child(inner_path, outer_path->parent))
		return;

	/*
	 * Before creating a path, get a quick lower bound on what it is likely to
	 * cost.  Bail out right away if it looks terrible.
	 */
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path, extra);
	if (!add_partial_path_precheck(joinrel, workspace.disabled_nodes,
								   workspace.total_cost, pathkeys))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	add_partial_path(joinrel, (Path *)
					 create_nestloop_path(root,
										  joinrel,
										  jointype,
										  &workspace,
										  extra,
										  outer_path,
										  inner_path,
										  extra->restrictlist,
										  pathkeys,
										  NULL));
}

/*
 * try_mergejoin_path
 *	  Consider a merge join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_mergejoin_path(PlannerInfo *root,
				   RelOptInfo *joinrel,
				   Path *outer_path,
				   Path *inner_path,
				   List *pathkeys,
				   List *mergeclauses,
				   List *outersortkeys,
				   List *innersortkeys,
				   JoinType jointype,
				   JoinPathExtraData *extra,
				   bool is_partial)
{
	Relids		required_outer;
	int			outer_presorted_keys = 0;
	JoinCostWorkspace workspace;

	if (is_partial)
	{
		try_partial_mergejoin_path(root,
								   joinrel,
								   outer_path,
								   inner_path,
								   pathkeys,
								   mergeclauses,
								   outersortkeys,
								   innersortkeys,
								   jointype,
								   extra);
		return;
	}

	/*
	 * If we are forming an outer join at this join, it's nonsensical to use
	 * an input path that uses the outer join as part of its parameterization.
	 * (This can happen despite our join order restrictions, since those apply
	 * to what is in an input relation not what its parameters are.)
	 */
	if (extra->sjinfo->ojrelid != 0 &&
		(bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(inner_path)) ||
		 bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(outer_path))))
		return;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible.
	 */
	required_outer = calc_non_nestloop_required_outer(outer_path,
													  inner_path);
	if (required_outer &&
		!bms_overlap(required_outer, extra->param_source_rels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * If the given paths are already well enough ordered, we can skip doing
	 * an explicit sort.
	 *
	 * We need to determine the number of presorted keys of the outer path to
	 * decide whether explicit incremental sort can be applied when
	 * outersortkeys is not NIL.  We do not need to do the same for the inner
	 * path though, as incremental sort currently does not support
	 * mark/restore.
	 */
	if (outersortkeys &&
		pathkeys_count_contained_in(outersortkeys, outer_path->pathkeys,
									&outer_presorted_keys))
		outersortkeys = NIL;
	if (innersortkeys &&
		pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
		innersortkeys = NIL;

	/*
	 * See comments in try_nestloop_path().
	 */
	initial_cost_mergejoin(root, &workspace, jointype, mergeclauses,
						   outer_path, inner_path,
						   outersortkeys, innersortkeys,
						   outer_presorted_keys,
						   extra);

	if (add_path_precheck(joinrel, workspace.disabled_nodes,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
		add_path(joinrel, (Path *)
				 create_mergejoin_path(root,
									   joinrel,
									   jointype,
									   &workspace,
									   extra,
									   outer_path,
									   inner_path,
									   extra->restrictlist,
									   pathkeys,
									   required_outer,
									   mergeclauses,
									   outersortkeys,
									   innersortkeys,
									   outer_presorted_keys));
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_mergejoin_path
 *	  Consider a partial merge join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_partial_path().
 */
static void
try_partial_mergejoin_path(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   Path *outer_path,
						   Path *inner_path,
						   List *pathkeys,
						   List *mergeclauses,
						   List *outersortkeys,
						   List *innersortkeys,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	int			outer_presorted_keys = 0;
	JoinCostWorkspace workspace;

	/*
	 * See comments in try_partial_hashjoin_path().
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	Assert(bms_is_empty(PATH_REQ_OUTER(outer_path)));
	if (!bms_is_empty(PATH_REQ_OUTER(inner_path)))
		return;

	/*
	 * If the given paths are already well enough ordered, we can skip doing
	 * an explicit sort.
	 *
	 * We need to determine the number of presorted keys of the outer path to
	 * decide whether explicit incremental sort can be applied when
	 * outersortkeys is not NIL.  We do not need to do the same for the inner
	 * path though, as incremental sort currently does not support
	 * mark/restore.
	 */
	if (outersortkeys &&
		pathkeys_count_contained_in(outersortkeys, outer_path->pathkeys,
									&outer_presorted_keys))
		outersortkeys = NIL;
	if (innersortkeys &&
		pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
		innersortkeys = NIL;

	/*
	 * See comments in try_partial_nestloop_path().
	 */
	initial_cost_mergejoin(root, &workspace, jointype, mergeclauses,
						   outer_path, inner_path,
						   outersortkeys, innersortkeys,
						   outer_presorted_keys,
						   extra);

	if (!add_partial_path_precheck(joinrel, workspace.disabled_nodes,
								   workspace.total_cost, pathkeys))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	add_partial_path(joinrel, (Path *)
					 create_mergejoin_path(root,
										   joinrel,
										   jointype,
										   &workspace,
										   extra,
										   outer_path,
										   inner_path,
										   extra->restrictlist,
										   pathkeys,
										   NULL,
										   mergeclauses,
										   outersortkeys,
										   innersortkeys,
										   outer_presorted_keys));
}

/*
 * try_hashjoin_path
 *	  Consider a hash join path; if it appears useful, push it into
 *	  the joinrel's pathlist via add_path().
 */
static void
try_hashjoin_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *hashclauses,
				  JoinType jointype,
				  JoinPathExtraData *extra)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;

	/*
	 * If we are forming an outer join at this join, it's nonsensical to use
	 * an input path that uses the outer join as part of its parameterization.
	 * (This can happen despite our join order restrictions, since those apply
	 * to what is in an input relation not what its parameters are.)
	 */
	if (extra->sjinfo->ojrelid != 0 &&
		(bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(inner_path)) ||
		 bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(outer_path))))
		return;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible.
	 */
	required_outer = calc_non_nestloop_required_outer(outer_path,
													  inner_path);
	if (required_outer &&
		!bms_overlap(required_outer, extra->param_source_rels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * See comments in try_nestloop_path().  Also note that hashjoin paths
	 * never have any output pathkeys, per comments in create_hashjoin_path.
	 */
	initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
						  outer_path, inner_path, extra, false);

	if (add_path_precheck(joinrel, workspace.disabled_nodes,
						  workspace.startup_cost, workspace.total_cost,
						  NIL, required_outer))
	{
		add_path(joinrel, (Path *)
				 create_hashjoin_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra,
									  outer_path,
									  inner_path,
									  false,	/* parallel_hash */
									  extra->restrictlist,
									  required_outer,
									  hashclauses));
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_partial_hashjoin_path
 *	  Consider a partial hashjoin join path; if it appears useful, push it into
 *	  the joinrel's partial_pathlist via add_partial_path().
 *	  The outer side is partial.  If parallel_hash is true, then the inner path
 *	  must be partial and will be run in parallel to create one or more shared
 *	  hash tables; otherwise the inner path must be complete and a copy of it
 *	  is run in every process to create separate identical private hash tables.
 */
static void
try_partial_hashjoin_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *hashclauses,
						  JoinType jointype,
						  JoinPathExtraData *extra,
						  bool parallel_hash)
{
	JoinCostWorkspace workspace;

	/*
	 * If the inner path is parameterized, we can't use a partial hashjoin.
	 * Parameterized partial paths are not supported.  The caller should
	 * already have verified that no lateral rels are required here.
	 */
	Assert(bms_is_empty(joinrel->lateral_relids));
	Assert(bms_is_empty(PATH_REQ_OUTER(outer_path)));
	if (!bms_is_empty(PATH_REQ_OUTER(inner_path)))
		return;

	/*
	 * Before creating a path, get a quick lower bound on what it is likely to
	 * cost.  Bail out right away if it looks terrible.
	 */
	initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
						  outer_path, inner_path, extra, parallel_hash);
	if (!add_partial_path_precheck(joinrel, workspace.disabled_nodes,
								   workspace.total_cost, NIL))
		return;

	/* Might be good enough to be worth trying, so let's try it. */
	add_partial_path(joinrel, (Path *)
					 create_hashjoin_path(root,
										  joinrel,
										  jointype,
										  &workspace,
										  extra,
										  outer_path,
										  inner_path,
										  parallel_hash,
										  extra->restrictlist,
										  NULL,
										  hashclauses));
}

/*
 * sort_inner_and_outer
 *	  Create mergejoin join paths by explicitly sorting both the outer and
 *	  inner join relations on each available merge ordering.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
sort_inner_and_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	Path	   *outer_path;
	Path	   *inner_path;
	Path	   *cheapest_partial_outer = NULL;
	Path	   *cheapest_safe_inner = NULL;
	List	   *all_pathkeys;
	ListCell   *l;

	/* Nothing to do if there are no available mergejoin clauses */
	if (extra->mergeclause_list == NIL)
		return;

	/*
	 * We only consider the cheapest-total-cost input paths, since we are
	 * assuming here that a sort is required.  We will consider
	 * cheapest-startup-cost input paths later, and only if they don't need a
	 * sort.
	 *
	 * This function intentionally does not consider parameterized input
	 * paths, except when the cheapest-total is parameterized.  If we did so,
	 * we'd have a combinatorial explosion of mergejoin paths of dubious
	 * value.  This interacts with decisions elsewhere that also discriminate
	 * against mergejoins with parameterized inputs; see comments in
	 * src/backend/optimizer/README.
	 */
	outer_path = outerrel->cheapest_total_path;
	inner_path = innerrel->cheapest_total_path;

	/*
	 * If either cheapest-total path is parameterized by the other rel, we
	 * can't use a mergejoin.  (There's no use looking for alternative input
	 * paths, since these should already be the least-parameterized available
	 * paths.)
	 */
	if (PATH_PARAM_BY_REL(outer_path, innerrel) ||
		PATH_PARAM_BY_REL(inner_path, outerrel))
		return;

	/*
	 * If the joinrel is parallel-safe, we may be able to consider a partial
	 * merge join.  However, we can't handle JOIN_FULL, JOIN_RIGHT and
	 * JOIN_RIGHT_ANTI, because they can produce false null extended rows.
	 * Also, the resulting path must not be parameterized.
	 */
	if (joinrel->consider_parallel &&
		jointype != JOIN_FULL &&
		jointype != JOIN_RIGHT &&
		jointype != JOIN_RIGHT_ANTI &&
		outerrel->partial_pathlist != NIL &&
		bms_is_empty(joinrel->lateral_relids))
	{
		cheapest_partial_outer = (Path *) linitial(outerrel->partial_pathlist);

		if (inner_path->parallel_safe)
			cheapest_safe_inner = inner_path;
		else
			cheapest_safe_inner =
				get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
	}

	/*
	 * Each possible ordering of the available mergejoin clauses will generate
	 * a differently-sorted result path at essentially the same cost.  We have
	 * no basis for choosing one over another at this level of joining, but
	 * some sort orders may be more useful than others for higher-level
	 * mergejoins, so it's worth considering multiple orderings.
	 *
	 * Actually, it's not quite true that every mergeclause ordering will
	 * generate a different path order, because some of the clauses may be
	 * partially redundant (refer to the same EquivalenceClasses).  Therefore,
	 * what we do is convert the mergeclause list to a list of canonical
	 * pathkeys, and then consider different orderings of the pathkeys.
	 *
	 * Generating a path for *every* permutation of the pathkeys doesn't seem
	 * like a winning strategy; the cost in planning time is too high. For
	 * now, we generate one path for each pathkey, listing that pathkey first
	 * and the rest in random order.  This should allow at least a one-clause
	 * mergejoin without re-sorting against any other possible mergejoin
	 * partner path.  But if we've not guessed the right ordering of secondary
	 * keys, we may end up evaluating clauses as qpquals when they could have
	 * been done as mergeclauses.  (In practice, it's rare that there's more
	 * than two or three mergeclauses, so expending a huge amount of thought
	 * on that is probably not worth it.)
	 *
	 * The pathkey order returned by select_outer_pathkeys_for_merge() has
	 * some heuristics behind it (see that function), so be sure to try it
	 * exactly as-is as well as making variants.
	 */
	all_pathkeys = select_outer_pathkeys_for_merge(root,
												   extra->mergeclause_list,
												   joinrel);

	foreach(l, all_pathkeys)
	{
		PathKey    *front_pathkey = (PathKey *) lfirst(l);
		List	   *cur_mergeclauses;
		List	   *outerkeys;
		List	   *innerkeys;
		List	   *merge_pathkeys;

		/* Make a pathkey list with this guy first */
		if (l != list_head(all_pathkeys))
			outerkeys = lcons(front_pathkey,
							  list_delete_nth_cell(list_copy(all_pathkeys),
												   foreach_current_index(l)));
		else
			outerkeys = all_pathkeys;	/* no work at first one... */

		/* Sort the mergeclauses into the corresponding ordering */
		cur_mergeclauses =
			find_mergeclauses_for_outer_pathkeys(root,
												 outerkeys,
												 extra->mergeclause_list);

		/* Should have used them all... */
		Assert(list_length(cur_mergeclauses) == list_length(extra->mergeclause_list));

		/* Build sort pathkeys for the inner side */
		innerkeys = make_inner_pathkeys_for_merge(root,
												  cur_mergeclauses,
												  outerkeys);

		/* Build pathkeys representing output sort order */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerkeys);

		/*
		 * And now we can make the path.
		 *
		 * Note: it's possible that the cheapest paths will already be sorted
		 * properly.  try_mergejoin_path will detect that case and suppress an
		 * explicit sort step, so we needn't do so here.
		 */
		try_mergejoin_path(root,
						   joinrel,
						   outer_path,
						   inner_path,
						   merge_pathkeys,
						   cur_mergeclauses,
						   outerkeys,
						   innerkeys,
						   jointype,
						   extra,
						   false);

		/*
		 * If we have partial outer and parallel safe inner path then try
		 * partial mergejoin path.
		 */
		if (cheapest_partial_outer && cheapest_safe_inner)
			try_partial_mergejoin_path(root,
									   joinrel,
									   cheapest_partial_outer,
									   cheapest_safe_inner,
									   merge_pathkeys,
									   cur_mergeclauses,
									   outerkeys,
									   innerkeys,
									   jointype,
									   extra);
	}
}

/*
 * generate_mergejoin_paths
 *	Creates possible mergejoin paths for input outerpath.
 *
 * We generate mergejoins if mergejoin clauses are available.  We have
 * two ways to generate the inner path for a mergejoin: sort the cheapest
 * inner path, or use an inner path that is already suitably ordered for the
 * merge.  If we have several mergeclauses, it could be that there is no inner
 * path (or only a very expensive one) for the full list of mergeclauses, but
 * better paths exist if we truncate the mergeclause list (thereby discarding
 * some sort key requirements).  So, we consider truncations of the
 * mergeclause list as well as the full list.  (Ideally we'd consider all
 * subsets of the mergeclause list, but that seems way too expensive.)
 */
static void
generate_mergejoin_paths(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *innerrel,
						 Path *outerpath,
						 JoinType jointype,
						 JoinPathExtraData *extra,
						 bool useallclauses,
						 Path *inner_cheapest_total,
						 List *merge_pathkeys,
						 bool is_partial)
{
	List	   *mergeclauses;
	List	   *innersortkeys;
	List	   *trialsortkeys;
	Path	   *cheapest_startup_inner;
	Path	   *cheapest_total_inner;
	int			num_sortkeys;
	int			sortkeycnt;

	/* Look for useful mergeclauses (if any) */
	mergeclauses =
		find_mergeclauses_for_outer_pathkeys(root,
											 outerpath->pathkeys,
											 extra->mergeclause_list);

	/*
	 * Done with this outer path if no chance for a mergejoin.
	 *
	 * Special corner case: for "x FULL JOIN y ON true", there will be no join
	 * clauses at all.  Ordinarily we'd generate a clauseless nestloop path,
	 * but since mergejoin is our only join type that supports FULL JOIN
	 * without any join clauses, it's necessary to generate a clauseless
	 * mergejoin path instead.
	 */
	if (mergeclauses == NIL)
	{
		if (jointype == JOIN_FULL)
			 /* okay to try for mergejoin */ ;
		else
			return;
	}
	if (useallclauses &&
		list_length(mergeclauses) != list_length(extra->mergeclause_list))
		return;

	/* Compute the required ordering of the inner path */
	innersortkeys = make_inner_pathkeys_for_merge(root,
												  mergeclauses,
												  outerpath->pathkeys);

	/*
	 * Generate a mergejoin on the basis of sorting the cheapest inner. Since
	 * a sort will be needed, only cheapest total cost matters. (But
	 * try_mergejoin_path will do the right thing if inner_cheapest_total is
	 * already correctly sorted.)
	 */
	try_mergejoin_path(root,
					   joinrel,
					   outerpath,
					   inner_cheapest_total,
					   merge_pathkeys,
					   mergeclauses,
					   NIL,
					   innersortkeys,
					   jointype,
					   extra,
					   is_partial);

	/*
	 * Look for presorted inner paths that satisfy the innersortkey list ---
	 * or any truncation thereof, if we are allowed to build a mergejoin using
	 * a subset of the merge clauses.  Here, we consider both cheap startup
	 * cost and cheap total cost.
	 *
	 * Currently we do not consider parameterized inner paths here. This
	 * interacts with decisions elsewhere that also discriminate against
	 * mergejoins with parameterized inputs; see comments in
	 * src/backend/optimizer/README.
	 *
	 * As we shorten the sortkey list, we should consider only paths that are
	 * strictly cheaper than (in particular, not the same as) any path found
	 * in an earlier iteration.  Otherwise we'd be intentionally using fewer
	 * merge keys than a given path allows (treating the rest as plain
	 * joinquals), which is unlikely to be a good idea.  Also, eliminating
	 * paths here on the basis of compare_path_costs is a lot cheaper than
	 * building the mergejoin path only to throw it away.
	 *
	 * If inner_cheapest_total is well enough sorted to have not required a
	 * sort in the path made above, we shouldn't make a duplicate path with
	 * it, either.  We handle that case with the same logic that handles the
	 * previous consideration, by initializing the variables that track
	 * cheapest-so-far properly.  Note that we do NOT reject
	 * inner_cheapest_total if we find it matches some shorter set of
	 * pathkeys.  That case corresponds to using fewer mergekeys to avoid
	 * sorting inner_cheapest_total, whereas we did sort it above, so the
	 * plans being considered are different.
	 */
	if (pathkeys_contained_in(innersortkeys,
							  inner_cheapest_total->pathkeys))
	{
		/* inner_cheapest_total didn't require a sort */
		cheapest_startup_inner = inner_cheapest_total;
		cheapest_total_inner = inner_cheapest_total;
	}
	else
	{
		/* it did require a sort, at least for the full set of keys */
		cheapest_startup_inner = NULL;
		cheapest_total_inner = NULL;
	}
	num_sortkeys = list_length(innersortkeys);
	if (num_sortkeys > 1 && !useallclauses)
		trialsortkeys = list_copy(innersortkeys);	/* need modifiable copy */
	else
		trialsortkeys = innersortkeys;	/* won't really truncate */

	for (sortkeycnt = num_sortkeys; sortkeycnt > 0; sortkeycnt--)
	{
		Path	   *innerpath;
		List	   *newclauses = NIL;

		/*
		 * Look for an inner path ordered well enough for the first
		 * 'sortkeycnt' innersortkeys.  NB: trialsortkeys list is modified
		 * destructively, which is why we made a copy...
		 */
		trialsortkeys = list_truncate(trialsortkeys, sortkeycnt);
		innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
												   trialsortkeys,
												   NULL,
												   TOTAL_COST,
												   is_partial);
		if (innerpath != NULL &&
			(cheapest_total_inner == NULL ||
			 compare_path_costs(innerpath, cheapest_total_inner,
								TOTAL_COST) < 0))
		{
			/* Found a cheap (or even-cheaper) sorted path */
			/* Select the right mergeclauses, if we didn't already */
			if (sortkeycnt < num_sortkeys)
			{
				newclauses =
					trim_mergeclauses_for_inner_pathkeys(root,
														 mergeclauses,
														 trialsortkeys);
				Assert(newclauses != NIL);
			}
			else
				newclauses = mergeclauses;
			try_mergejoin_path(root,
							   joinrel,
							   outerpath,
							   innerpath,
							   merge_pathkeys,
							   newclauses,
							   NIL,
							   NIL,
							   jointype,
							   extra,
							   is_partial);
			cheapest_total_inner = innerpath;
		}
		/* Same on the basis of cheapest startup cost ... */
		innerpath = get_cheapest_path_for_pathkeys(innerrel->pathlist,
												   trialsortkeys,
												   NULL,
												   STARTUP_COST,
												   is_partial);
		if (innerpath != NULL &&
			(cheapest_startup_inner == NULL ||
			 compare_path_costs(innerpath, cheapest_startup_inner,
								STARTUP_COST) < 0))
		{
			/* Found a cheap (or even-cheaper) sorted path */
			if (innerpath != cheapest_total_inner)
			{
				/*
				 * Avoid rebuilding clause list if we already made one; saves
				 * memory in big join trees...
				 */
				if (newclauses == NIL)
				{
					if (sortkeycnt < num_sortkeys)
					{
						newclauses =
							trim_mergeclauses_for_inner_pathkeys(root,
																 mergeclauses,
																 trialsortkeys);
						Assert(newclauses != NIL);
					}
					else
						newclauses = mergeclauses;
				}
				try_mergejoin_path(root,
								   joinrel,
								   outerpath,
								   innerpath,
								   merge_pathkeys,
								   newclauses,
								   NIL,
								   NIL,
								   jointype,
								   extra,
								   is_partial);
			}
			cheapest_startup_inner = innerpath;
		}

		/*
		 * Don't consider truncated sortkeys if we need all clauses.
		 */
		if (useallclauses)
			break;
	}
}

/*
 * match_unsorted_outer
 *	  Creates possible join paths for processing a single join relation
 *	  'joinrel' by employing either iterative substitution or
 *	  mergejoining on each of its possible outer paths (considering
 *	  only outer paths that are already ordered well enough for merging).
 *
 * We always generate a nestloop path for each available outer path.
 * In fact we may generate as many as five: one on the cheapest-total-cost
 * inner path, one on the same with materialization, one on the
 * cheapest-startup-cost inner path (if different), one on the
 * cheapest-total inner-indexscan path (if any), and one on the
 * cheapest-startup inner-indexscan path (if different).
 *
 * We also consider mergejoins if mergejoin clauses are available.  See
 * detailed comments in generate_mergejoin_paths.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
match_unsorted_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	bool		nestjoinOK;
	bool		useallclauses;
	Path	   *inner_cheapest_total = innerrel->cheapest_total_path;
	Path	   *matpath = NULL;
	ListCell   *lc1;

	/*
	 * For now we do not support RIGHT_SEMI join in mergejoin or nestloop
	 * join.
	 */
	if (jointype == JOIN_RIGHT_SEMI)
		return;

	/*
	 * Nestloop only supports inner, left, semi, and anti joins.  Also, if we
	 * are doing a right, right-anti or full mergejoin, we must use *all* the
	 * mergeclauses as join clauses, else we will not have a valid plan.
	 * (Although these two flags are currently inverses, keep them separate
	 * for clarity and possible future changes.)
	 */
	switch (jointype)
	{
		case JOIN_INNER:
		case JOIN_LEFT:
		case JOIN_SEMI:
		case JOIN_ANTI:
			nestjoinOK = true;
			useallclauses = false;
			break;
		case JOIN_RIGHT:
		case JOIN_RIGHT_ANTI:
		case JOIN_FULL:
			nestjoinOK = false;
			useallclauses = true;
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) jointype);
			nestjoinOK = false; /* keep compiler quiet */
			useallclauses = false;
			break;
	}

	/*
	 * If inner_cheapest_total is parameterized by the outer rel, ignore it;
	 * we will consider it below as a member of cheapest_parameterized_paths,
	 * but the other possibilities considered in this routine aren't usable.
	 *
	 * Furthermore, if the inner side is a unique-ified relation, we cannot
	 * generate any valid paths here, because the inner rel's dependency on
	 * the outer rel makes unique-ification meaningless.
	 */
	if (PATH_PARAM_BY_REL(inner_cheapest_total, outerrel))
	{
		inner_cheapest_total = NULL;

		if (RELATION_WAS_MADE_UNIQUE(innerrel, extra->sjinfo, jointype))
			return;
	}

	if (nestjoinOK)
	{
		/*
		 * Consider materializing the cheapest inner path, unless
		 * enable_material is off or the path in question materializes its
		 * output anyway.
		 */
		if (enable_material && inner_cheapest_total != NULL &&
			!ExecMaterializesOutput(inner_cheapest_total->pathtype))
			matpath = (Path *)
				create_material_path(innerrel, inner_cheapest_total);
	}

	foreach(lc1, outerrel->pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *merge_pathkeys;

		/*
		 * We cannot use an outer path that is parameterized by the inner rel.
		 */
		if (PATH_PARAM_BY_REL(outerpath, innerrel))
			continue;

		/*
		 * The result will have this sort order (even if it is implemented as
		 * a nestloop, and even if some of the mergeclauses are implemented by
		 * qpquals rather than as true mergeclauses):
		 */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerpath->pathkeys);

		if (nestjoinOK)
		{
			/*
			 * Consider nestloop joins using this outer path and various
			 * available paths for the inner relation.  We consider the
			 * cheapest-total paths for each available parameterization of the
			 * inner relation, including the unparameterized case.
			 */
			ListCell   *lc2;

			foreach(lc2, innerrel->cheapest_parameterized_paths)
			{
				Path	   *innerpath = (Path *) lfirst(lc2);
				Path	   *mpath;

				try_nestloop_path(root,
								  joinrel,
								  outerpath,
								  innerpath,
								  merge_pathkeys,
								  jointype,
								  extra);

				/*
				 * Try generating a memoize path and see if that makes the
				 * nested loop any cheaper.
				 */
				mpath = get_memoize_path(root, innerrel, outerrel,
										 innerpath, outerpath, jointype,
										 extra);
				if (mpath != NULL)
					try_nestloop_path(root,
									  joinrel,
									  outerpath,
									  mpath,
									  merge_pathkeys,
									  jointype,
									  extra);
			}

			/* Also consider materialized form of the cheapest inner path */
			if (matpath != NULL)
				try_nestloop_path(root,
								  joinrel,
								  outerpath,
								  matpath,
								  merge_pathkeys,
								  jointype,
								  extra);
		}

		/* Can't do anything else if inner rel is parameterized by outer */
		if (inner_cheapest_total == NULL)
			continue;

		/* Generate merge join paths */
		generate_mergejoin_paths(root, joinrel, innerrel, outerpath,
								 jointype, extra, useallclauses,
								 inner_cheapest_total, merge_pathkeys,
								 false);
	}

	/*
	 * Consider partial nestloop and mergejoin plan if outerrel has any
	 * partial path and the joinrel is parallel-safe.  However, we can't
	 * handle joins needing lateral rels, since partial paths must not be
	 * parameterized.  Similarly, we can't handle JOIN_FULL, JOIN_RIGHT and
	 * JOIN_RIGHT_ANTI, because they can produce false null extended rows.
	 */
	if (joinrel->consider_parallel &&
		jointype != JOIN_FULL &&
		jointype != JOIN_RIGHT &&
		jointype != JOIN_RIGHT_ANTI &&
		outerrel->partial_pathlist != NIL &&
		bms_is_empty(joinrel->lateral_relids))
	{
		if (nestjoinOK)
			consider_parallel_nestloop(root, joinrel, outerrel, innerrel,
									   jointype, extra);

		/*
		 * If inner_cheapest_total is NULL or non parallel-safe then find the
		 * cheapest total parallel safe path.
		 */
		if (inner_cheapest_total == NULL ||
			!inner_cheapest_total->parallel_safe)
		{
			inner_cheapest_total =
				get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
		}

		if (inner_cheapest_total)
			consider_parallel_mergejoin(root, joinrel, outerrel, innerrel,
										jointype, extra,
										inner_cheapest_total);
	}
}

/*
 * consider_parallel_mergejoin
 *	  Try to build partial paths for a joinrel by joining a partial path
 *	  for the outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 * 'inner_cheapest_total' cheapest total path for innerrel
 */
static void
consider_parallel_mergejoin(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra,
							Path *inner_cheapest_total)
{
	ListCell   *lc1;

	/* generate merge join path for each partial outer path */
	foreach(lc1, outerrel->partial_pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *merge_pathkeys;

		/*
		 * Figure out what useful ordering any paths we create will have.
		 */
		merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
											 outerpath->pathkeys);

		generate_mergejoin_paths(root, joinrel, innerrel, outerpath, jointype,
								 extra, false, inner_cheapest_total,
								 merge_pathkeys, true);
	}
}

/*
 * consider_parallel_nestloop
 *	  Try to build partial paths for a joinrel by joining a partial path for the
 *	  outer relation to a complete path for the inner relation.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
consider_parallel_nestloop(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	Path	   *inner_cheapest_total = innerrel->cheapest_total_path;
	Path	   *matpath = NULL;
	ListCell   *lc1;

	/*
	 * Consider materializing the cheapest inner path, unless: 1)
	 * enable_material is off, 2) the cheapest inner path is not
	 * parallel-safe, 3) the cheapest inner path is parameterized by the outer
	 * rel, or 4) the cheapest inner path materializes its output anyway.
	 */
	if (enable_material && inner_cheapest_total->parallel_safe &&
		!PATH_PARAM_BY_REL(inner_cheapest_total, outerrel) &&
		!ExecMaterializesOutput(inner_cheapest_total->pathtype))
	{
		matpath = (Path *)
			create_material_path(innerrel, inner_cheapest_total);
		Assert(matpath->parallel_safe);
	}

	foreach(lc1, outerrel->partial_pathlist)
	{
		Path	   *outerpath = (Path *) lfirst(lc1);
		List	   *pathkeys;
		ListCell   *lc2;

		/* Figure out what useful ordering any paths we create will have. */
		pathkeys = build_join_pathkeys(root, joinrel, jointype,
									   outerpath->pathkeys);

		/*
		 * Try the cheapest parameterized paths; only those which will produce
		 * an unparameterized path when joined to this outerrel will survive
		 * try_partial_nestloop_path.  The cheapest unparameterized path is
		 * also in this list.
		 */
		foreach(lc2, innerrel->cheapest_parameterized_paths)
		{
			Path	   *innerpath = (Path *) lfirst(lc2);
			Path	   *mpath;

			/* Can't join to an inner path that is not parallel-safe */
			if (!innerpath->parallel_safe)
				continue;

			try_partial_nestloop_path(root, joinrel, outerpath, innerpath,
									  pathkeys, jointype, extra);

			/*
			 * Try generating a memoize path and see if that makes the nested
			 * loop any cheaper.
			 */
			mpath = get_memoize_path(root, innerrel, outerrel,
									 innerpath, outerpath, jointype,
									 extra);
			if (mpath != NULL)
				try_partial_nestloop_path(root, joinrel, outerpath, mpath,
										  pathkeys, jointype, extra);
		}

		/* Also consider materialized form of the cheapest inner path */
		if (matpath != NULL)
			try_partial_nestloop_path(root, joinrel, outerpath, matpath,
									  pathkeys, jointype, extra);
	}
}

/*
 * hash_inner_and_outer
 *	  Create hashjoin join paths by explicitly hashing both the outer and
 *	  inner keys of each available hash clause.
 *
 * 'joinrel' is the join relation
 * 'outerrel' is the outer join relation
 * 'innerrel' is the inner join relation
 * 'jointype' is the type of join to do
 * 'extra' contains additional input values
 */
static void
hash_inner_and_outer(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 JoinPathExtraData *extra)
{
	bool		isouterjoin = IS_OUTER_JOIN(jointype);
	List	   *hashclauses;
	ListCell   *l;

	/*
	 * We need to build only one hashclauses list for any given pair of outer
	 * and inner relations; all of the hashable clauses will be used as keys.
	 *
	 * Scan the join's restrictinfo list to find hashjoinable clauses that are
	 * usable with this pair of sub-relations.
	 */
	hashclauses = NIL;
	foreach(l, extra->restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		/*
		 * If processing an outer join, only use its own join clauses for
		 * hashing.  For inner joins we need not be so picky.
		 */
		if (isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		if (!restrictinfo->can_join ||
			restrictinfo->hashjoinoperator == InvalidOid)
			continue;			/* not hashjoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer".
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel->relids,
									 innerrel->relids))
			continue;			/* no good for these input relations */

		/*
		 * If clause has the form "inner op outer", check if its operator has
		 * valid commutator.  This is necessary because hashclauses in this
		 * form will get commuted in createplan.c to put the outer var on the
		 * left (see get_switched_clauses).  This probably shouldn't ever
		 * fail, since hashable operators ought to have commutators, but be
		 * paranoid.
		 *
		 * The clause being hashjoinable indicates that it's an OpExpr.
		 */
		if (!restrictinfo->outer_is_left &&
			!OidIsValid(get_commutator(castNode(OpExpr, restrictinfo->clause)->opno)))
			continue;

		hashclauses = lappend(hashclauses, restrictinfo);
	}

	/* If we found any usable hashclauses, make paths */
	if (hashclauses)
	{
		/*
		 * We consider both the cheapest-total-cost and cheapest-startup-cost
		 * outer paths.  There's no need to consider any but the
		 * cheapest-total-cost inner path, however.
		 */
		Path	   *cheapest_startup_outer = outerrel->cheapest_startup_path;
		Path	   *cheapest_total_outer = outerrel->cheapest_total_path;
		Path	   *cheapest_total_inner = innerrel->cheapest_total_path;
		ListCell   *lc1;
		ListCell   *lc2;

		/*
		 * If either cheapest-total path is parameterized by the other rel, we
		 * can't use a hashjoin.  (There's no use looking for alternative
		 * input paths, since these should already be the least-parameterized
		 * available paths.)
		 */
		if (PATH_PARAM_BY_REL(cheapest_total_outer, innerrel) ||
			PATH_PARAM_BY_REL(cheapest_total_inner, outerrel))
			return;

		/*
		 * Consider the cheapest startup outer together with the cheapest
		 * total inner, and then consider pairings of cheapest-total paths
		 * including parameterized ones.  There is no use in generating
		 * parameterized paths on the basis of possibly cheap startup cost, so
		 * this is sufficient.
		 */
		if (cheapest_startup_outer != NULL)
			try_hashjoin_path(root,
							  joinrel,
							  cheapest_startup_outer,
							  cheapest_total_inner,
							  hashclauses,
							  jointype,
							  extra);

		foreach(lc1, outerrel->cheapest_parameterized_paths)
		{
			Path	   *outerpath = (Path *) lfirst(lc1);

			/*
			 * We cannot use an outer path that is parameterized by the inner
			 * rel.
			 */
			if (PATH_PARAM_BY_REL(outerpath, innerrel))
				continue;

			foreach(lc2, innerrel->cheapest_parameterized_paths)
			{
				Path	   *innerpath = (Path *) lfirst(lc2);

				/*
				 * We cannot use an inner path that is parameterized by the
				 * outer rel, either.
				 */
				if (PATH_PARAM_BY_REL(innerpath, outerrel))
					continue;

				if (outerpath == cheapest_startup_outer &&
					innerpath == cheapest_total_inner)
					continue;	/* already tried it */

				try_hashjoin_path(root,
								  joinrel,
								  outerpath,
								  innerpath,
								  hashclauses,
								  jointype,
								  extra);
			}
		}

		/*
		 * If the joinrel is parallel-safe, we may be able to consider a
		 * partial hash join.
		 *
		 * However, we can't handle JOIN_RIGHT_SEMI, because the hash table is
		 * either a shared hash table or a private hash table per backend.  In
		 * the shared case, there is no concurrency protection for the match
		 * flags, so multiple workers could inspect and set the flags
		 * concurrently, potentially producing incorrect results.  In the
		 * private case, each worker has its own copy of the hash table, so no
		 * single process has all the match flags.
		 *
		 * Also, the resulting path must not be parameterized.
		 */
		if (joinrel->consider_parallel &&
			jointype != JOIN_RIGHT_SEMI &&
			outerrel->partial_pathlist != NIL &&
			bms_is_empty(joinrel->lateral_relids))
		{
			Path	   *cheapest_partial_outer;
			Path	   *cheapest_partial_inner = NULL;
			Path	   *cheapest_safe_inner = NULL;

			cheapest_partial_outer =
				(Path *) linitial(outerrel->partial_pathlist);

			/*
			 * Can we use a partial inner plan too, so that we can build a
			 * shared hash table in parallel?
			 */
			if (innerrel->partial_pathlist != NIL &&
				enable_parallel_hash)
			{
				cheapest_partial_inner =
					(Path *) linitial(innerrel->partial_pathlist);
				try_partial_hashjoin_path(root, joinrel,
										  cheapest_partial_outer,
										  cheapest_partial_inner,
										  hashclauses, jointype, extra,
										  true /* parallel_hash */ );
			}

			/*
			 * Normally, given that the joinrel is parallel-safe, the cheapest
			 * total inner path will also be parallel-safe, but if not, we'll
			 * have to search for the cheapest safe, unparameterized inner
			 * path.  If full, right, or right-anti join, we can't use
			 * parallelism (building the hash table in each backend) because
			 * no one process has all the match bits.
			 */
			if (jointype == JOIN_FULL ||
				jointype == JOIN_RIGHT ||
				jointype == JOIN_RIGHT_ANTI)
				cheapest_safe_inner = NULL;
			else if (cheapest_total_inner->parallel_safe)
				cheapest_safe_inner = cheapest_total_inner;
			else
				cheapest_safe_inner =
					get_cheapest_parallel_safe_total_inner(innerrel->pathlist);

			if (cheapest_safe_inner != NULL)
				try_partial_hashjoin_path(root, joinrel,
										  cheapest_partial_outer,
										  cheapest_safe_inner,
										  hashclauses, jointype, extra,
										  false /* parallel_hash */ );
		}
	}
}

/*
 * select_mergejoin_clauses
 *	  Select mergejoin clauses that are usable for a particular join.
 *	  Returns a list of RestrictInfo nodes for those clauses.
 *
 * *mergejoin_allowed is normally set to true, but it is set to false if
 * this is a right-semi join, or this is a right/right-anti/full join and
 * there are nonmergejoinable join clauses.  The executor's mergejoin
 * machinery cannot handle such cases, so we have to avoid generating a
 * mergejoin plan.  (Note that this flag does NOT consider whether there are
 * actually any mergejoinable clauses.  This is correct because in some
 * cases we need to build a clauseless mergejoin.  Simply returning NIL is
 * therefore not enough to distinguish safe from unsafe cases.)
 *
 * We also mark each selected RestrictInfo to show which side is currently
 * being considered as outer.  These are transient markings that are only
 * good for the duration of the current add_paths_to_joinrel() call!
 *
 * We examine each restrictinfo clause known for the join to see
 * if it is mergejoinable and involves vars from the two sub-relations
 * currently of interest.
 */
static List *
select_mergejoin_clauses(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *outerrel,
						 RelOptInfo *innerrel,
						 List *restrictlist,
						 JoinType jointype,
						 bool *mergejoin_allowed)
{
	List	   *result_list = NIL;
	bool		isouterjoin = IS_OUTER_JOIN(jointype);
	bool		have_nonmergeable_joinclause = false;
	ListCell   *l;

	/*
	 * For now we do not support RIGHT_SEMI join in mergejoin: the benefit of
	 * swapping inputs tends to be small here.
	 */
	if (jointype == JOIN_RIGHT_SEMI)
	{
		*mergejoin_allowed = false;
		return NIL;
	}

	foreach(l, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

		/*
		 * If processing an outer join, only use its own join clauses in the
		 * merge.  For inner joins we can use pushed-down clauses too. (Note:
		 * we don't set have_nonmergeable_joinclause here because pushed-down
		 * clauses will become otherquals not joinquals.)
		 */
		if (isouterjoin && RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		/* Check that clause is a mergeable operator clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
		{
			/*
			 * The executor can handle extra joinquals that are constants, but
			 * not anything else, when doing right/right-anti/full merge join.
			 * (The reason to support constants is so we can do FULL JOIN ON
			 * FALSE.)
			 */
			if (!restrictinfo->clause || !IsA(restrictinfo->clause, Const))
				have_nonmergeable_joinclause = true;
			continue;			/* not mergejoinable */
		}

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer".
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel->relids,
									 innerrel->relids))
		{
			have_nonmergeable_joinclause = true;
			continue;			/* no good for these input relations */
		}

		/*
		 * If clause has the form "inner op outer", check if its operator has
		 * valid commutator.  This is necessary because mergejoin clauses in
		 * this form will get commuted in createplan.c to put the outer var on
		 * the left (see get_switched_clauses).  This probably shouldn't ever
		 * fail, since mergejoinable operators ought to have commutators, but
		 * be paranoid.
		 *
		 * The clause being mergejoinable indicates that it's an OpExpr.
		 */
		if (!restrictinfo->outer_is_left &&
			!OidIsValid(get_commutator(castNode(OpExpr, restrictinfo->clause)->opno)))
		{
			have_nonmergeable_joinclause = true;
			continue;
		}

		/*
		 * Insist that each side have a non-redundant eclass.  This
		 * restriction is needed because various bits of the planner expect
		 * that each clause in a merge be associable with some pathkey in a
		 * canonical pathkey list, but redundant eclasses can't appear in
		 * canonical sort orderings.  (XXX it might be worth relaxing this,
		 * but not enough time to address it for 8.3.)
		 */
		update_mergeclause_eclasses(root, restrictinfo);

		if (EC_MUST_BE_REDUNDANT(restrictinfo->left_ec) ||
			EC_MUST_BE_REDUNDANT(restrictinfo->right_ec))
		{
			have_nonmergeable_joinclause = true;
			continue;			/* can't handle redundant eclasses */
		}

		result_list = lappend(result_list, restrictinfo);
	}

	/*
	 * Report whether mergejoin is allowed (see comment at top of function).
	 */
	switch (jointype)
	{
		case JOIN_RIGHT:
		case JOIN_RIGHT_ANTI:
		case JOIN_FULL:
			*mergejoin_allowed = !have_nonmergeable_joinclause;
			break;
		default:
			*mergejoin_allowed = true;
			break;
	}

	return result_list;
}
