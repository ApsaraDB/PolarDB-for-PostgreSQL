/*-------------------------------------------------------------------------
 *
 * planmain.c
 *	  Routines to plan a single query
 *
 * What's in a name, anyway?  The top-level entry point of the planner/
 * optimizer is over in planner.c, not here as you might think from the
 * file name.  But this is the main code for planning a basic join operation,
 * shorn of features like subselects, inheritance, aggregates, grouping,
 * and so on.  (Those are the things planner.c deals with.)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planmain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/clauses.h"
#include "optimizer/orclauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/cost.h"
#include "px/px_vars.h"

/* POLAR px */
#include "utils/guc.h"

/*
 * query_planner
 *	  Generate a path (that is, a simplified plan) for a basic query,
 *	  which may involve joins but not any fancier features.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * root describes the query to plan
 * tlist is the target list the query should produce
 *		(this is NOT necessarily root->parse->targetList!)
 * qp_callback is a function to compute query_pathkeys once it's safe to do so
 * qp_extra is optional extra data to pass to qp_callback
 *
 * Note: the PlannerInfo node also includes a query_pathkeys field, which
 * tells query_planner the sort order that is desired in the final output
 * plan.  This value is *not* available at call time, but is computed by
 * qp_callback once we have completed merging the query's equivalence classes.
 * (We cannot construct canonical pathkeys until that's done.)
 */
RelOptInfo *
query_planner(PlannerInfo *root, List *tlist,
			  query_pathkeys_callback qp_callback, void *qp_extra)
{
	Query	   *parse = root->parse;
	List	   *joinlist;
	RelOptInfo *final_rel;
	Index		rti;
	double		total_pages;

	/*
	 * If the query has an empty join tree, then it's something easy like
	 * "SELECT 2+2;" or "INSERT ... VALUES()".  Fall through quickly.
	 */
	if (parse->jointree->fromlist == NIL)
	{
		/* We need a dummy joinrel to describe the empty set of baserels */
		final_rel = build_empty_join_rel(root);

		/*
		 * If query allows parallelism in general, check whether the quals are
		 * parallel-restricted.  (We need not check final_rel->reltarget
		 * because it's empty at this point.  Anything parallel-restricted in
		 * the query tlist will be dealt with later.)
		 */
		if (root->glob->parallelModeOK)
			final_rel->consider_parallel =
				is_parallel_safe(root, parse->jointree->quals);

		/* The only path for it is a trivial Result path */
		add_path(final_rel, (Path *)
				 create_result_path(root, final_rel,
									final_rel->reltarget,
									(List *) parse->jointree->quals));

		/* Select cheapest path (pretty easy in this case...) */
		set_cheapest(final_rel);

		/*
		 * We still are required to call qp_callback, in case it's something
		 * like "SELECT 2+2 ORDER BY 1".
		 */
		root->canon_pathkeys = NIL;
		(*qp_callback) (root, qp_extra);

		return final_rel;
	}

	/*
	 * Init planner lists to empty.
	 *
	 * NOTE: append_rel_list was set up by subquery_planner, so do not touch
	 * here.
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;
	root->join_rel_level = NULL;
	root->join_cur_level = 0;
	root->canon_pathkeys = NIL;
	root->left_join_clauses = NIL;
	root->right_join_clauses = NIL;
	root->full_join_clauses = NIL;
	root->join_info_list = NIL;
	root->placeholder_list = NIL;
	root->fkey_list = NIL;
	root->initial_rels = NIL;

	/*
	 * Make a flattened version of the rangetable for faster access (this is
	 * OK because the rangetable won't change any more), and set up an empty
	 * array for indexing base relations.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * Populate append_rel_array with each AppendRelInfo to allow direct
	 * lookups by child relid.
	 */
	setup_append_rel_array(root);

	/*
	 * Construct RelOptInfo nodes for all base relations in query, and
	 * indirectly for all appendrel member relations ("other rels").  This
	 * will give us a RelOptInfo for every "simple" (non-join) rel involved in
	 * the query.
	 *
	 * Note: the reason we find the rels by searching the jointree and
	 * appendrel list, rather than just scanning the rangetable, is that the
	 * rangetable may contain RTEs for rels not actively part of the query,
	 * for example views.  We don't want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);

	/*
	 * Examine the targetlist and join tree, adding entries to baserel
	 * targetlists for all referenced Vars, and generating PlaceHolderInfo
	 * entries for all referenced PlaceHolderVars.  Restrict and join clauses
	 * are added to appropriate lists belonging to the mentioned relations. We
	 * also build EquivalenceClasses for provably equivalent expressions. The
	 * SpecialJoinInfo list is also built to hold information about join order
	 * restrictions.  Finally, we form a target joinlist for make_one_rel() to
	 * work from.
	 */
	build_base_rel_tlists(root, tlist);

	find_placeholders_in_jointree(root);

	find_lateral_references(root);

	joinlist = deconstruct_jointree(root);

	/*
	 * Reconsider any postponed outer-join quals now that we have built up
	 * equivalence classes.  (This could result in further additions or
	 * mergings of classes.)
	 */
	reconsider_outer_join_clauses(root);

	/*
	 * If we formed any equivalence classes, generate additional restriction
	 * clauses as appropriate.  (Implied join clauses are formed on-the-fly
	 * later.)
	 */
	generate_base_implied_equalities(root);

	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);

	/*
	 * Examine any "placeholder" expressions generated during subquery pullup.
	 * Make sure that the Vars they need are marked as needed at the relevant
	 * join level.  This must be done before join removal because it might
	 * cause Vars or placeholders to be needed above a join when they weren't
	 * so marked before.
	 */
	fix_placeholder_input_needed_levels(root);

	/*
	 * Remove any useless outer joins.  Ideally this would be done during
	 * jointree preprocessing, but the necessary information isn't available
	 * until we've built baserel data structures and classified qual clauses.
	 */
	joinlist = remove_useless_joins(root, joinlist);

	/*
	 * Also, reduce any semijoins with unique inner rels to plain inner joins.
	 * Likewise, this can't be done until now for lack of needed info.
	 */
	reduce_unique_semijoins(root);

	/*
	 * Now distribute "placeholders" to base rels as needed.  This has to be
	 * done after join removal because removal could change whether a
	 * placeholder is evaluable at a base rel.
	 */
	add_placeholders_to_base_rels(root);

	/*
	 * Construct the lateral reference sets now that we have finalized
	 * PlaceHolderVar eval levels.
	 */
	create_lateral_join_info(root);

	/*
	 * Match foreign keys to equivalence classes and join quals.  This must be
	 * done after finalizing equivalence classes, and it's useful to wait till
	 * after join removal so that we can skip processing foreign keys
	 * involving removed relations.
	 */
	match_foreign_keys_to_quals(root);

	/*
	 * Look for join OR clauses that we can extract single-relation
	 * restriction OR clauses from.
	 */
	extract_restriction_or_clauses(root);

	/*
	 * We should now have size estimates for every actual table involved in
	 * the query, and we also know which if any have been deleted from the
	 * query by join removal; so we can compute total_table_pages.
	 *
	 * Note that appendrels are not double-counted here, even though we don't
	 * bother to distinguish RelOptInfos for appendrel parents, because the
	 * parents will still have size zero.
	 *
	 * XXX if a table is self-joined, we will count it once per appearance,
	 * which perhaps is the wrong thing ... but that's not completely clear,
	 * and detecting self-joins here is difficult, so ignore it for now.
	 */
	total_pages = 0;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		if (IS_SIMPLE_REL(brel))
			total_pages += (double) brel->pages;
	}
	root->total_table_pages = total_pages;

	/*
	 * Ready to do the primary planning.
	 */
	final_rel = make_one_rel(root, joinlist);

	/* Check that we got at least one usable path */
	if (!final_rel || !final_rel->cheapest_total_path ||
		final_rel->cheapest_total_path->param_info != NULL)
		elog(ERROR, "failed to construct the join relation");

	return final_rel;
}

/* POLAR px*/
/*
 * Default configuration information
 */
PlannerConfig *DefaultPlannerConfig(void)
{
	PlannerConfig *c1 = (PlannerConfig *) palloc(sizeof(PlannerConfig));
	c1->enable_sort = enable_sort;
	c1->enable_hashagg = enable_hashagg;
	c1->enable_groupagg = enable_groupagg;
	c1->enable_nestloop = enable_nestloop;
	c1->enable_mergejoin = enable_mergejoin;
	c1->enable_hashjoin = enable_hashjoin;
	c1->px_enable_hashjoin_size_heuristic = px_enable_hashjoin_size_heuristic;
	c1->px_enable_predicate_propagation = px_enable_predicate_propagation;
	c1->constraint_exclusion = constraint_exclusion;

	c1->px_enable_minmax_optimization = px_enable_minmax_optimization;
	c1->px_enable_multiphase_agg = px_enable_multiphase_agg;
	c1->px_enable_preunique = px_enable_preunique;
	c1->px_eager_preunique = px_eager_preunique;
	c1->px_hashagg_streambottom = px_hashagg_streambottom;
	c1->px_enable_agg_distinct = px_enable_agg_distinct;
	c1->px_enable_dqa_pruning = px_enable_dqa_pruning;
	c1->px_eager_dqa_pruning = px_eager_dqa_pruning;
	c1->px_eager_one_phase_agg = px_eager_one_phase_agg;
	c1->px_eager_two_phase_agg = px_eager_two_phase_agg;
	c1->px_enable_sort_distinct = px_enable_sort_distinct;

	c1->px_enable_direct_dispatch = px_enable_direct_dispatch;

	c1->px_cte_sharing = px_cte_sharing;

	c1->honor_order_by = true;

	c1->is_under_subplan = false;

	return c1;
}
