/*-------------------------------------------------------------------------
 *
 * polar_or_to_union_all.c
 *	  Consider whether join OR clauses can be converted to UNION ALL queries.
 *
 * This module looks for OR clauses whose arms each reference a single
 * query relation (but not all the same rel), and tries to generate a path
 * representing conversion of such an OR clause into a UNION ALL operation.
 * For example,
 *		SELECT ... FROM a, b WHERE (cond-on-A OR cond-on-B) AND other-conds
 * can be implemented as
 *		SELECT ... FROM a, b WHERE cond-on-A AND other-conds
 *		UNION ALL
 *		SELECT ... FROM a, b WHERE cond-on-B AND other-conds
 *			AND (NOT (cond-on-A) OR (cond-on-A) IS NULL)
 * given a suitable definition of "UNION ALL" (one that won't merge rows that
 * would have been separate in the original query output).  Since this change
 * converts join clauses into restriction clauses, the modified query can be
 * much faster to run than the original, despite the duplication of effort
 * involved and the extra UNION ALL processing.  It's particularly useful for
 * star-schema queries where the OR arms reference different dimension tables;
 * each separated query may be able to remove joins to all but one dimension
 * table, and arrange that join to use an inner indexscan on the fact table.
 *
 * We must insist that the WHERE and JOIN/ON clauses contain no volatile
 * functions, because of the likelihood that qual clauses will be evaluated
 * more times than a naive programmer would expect.  We need not restrict
 * the SELECT's tlist, because that will be evaluated after the UNION ALL.
 *
 * Portions Copyright (c) 2020, Alibaba inc.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/polar_or_to_union_all.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"


static bool polar_is_suitable_join_or_clause(BoolExpr *orclause, List **relids);
static bool polar_is_query_safe_for_union_or_transform(PlannerInfo *root);
static List *polar_create_union_or_subpaths(PlannerInfo *root, BoolExpr *orclause,
						 int n, List *armrelids);
static void polar_union_or_qp_callback(PlannerInfo *root, void *extra);


/*
 * split_join_or_clauses - create paths based on splitting join OR clauses
 *
 * This should be called by grouping_planner() just before it's ready to call
 * query_planner(), because we generate simplified join paths by cloning the
 * planner's state and invoking query_planner() on a modified version of
 * the query parsetree.  Thus, all preprocessing needed before query_planner()
 * must already be done.  Note however that we repeat reduce_outer_joins()
 * because of the possibility that the simplified WHERE clause allows reduction
 * of an outer join to inner-join form.  That's okay for now, but maybe we
 * * should move the reduce_outer_joins() call into query_planner()?
 *
 * The result is a list (one entry per potential union OR path) of sublists of
 * best paths for the inputs to the UNION ALL step.  Adding the UNION ALL 
 * processing is retty mechanical, but we can't do it until we have a RelOptInfo 
 * for the top-level join rel.
 */
List *
polar_split_join_or_clauses(PlannerInfo *root)
{
	List	   *results = NIL;
	Query	   *parse = root->parse;
	bool		checked_query = false;
	ListCell   *lc;
	int			n;

	/*
	 * At least for now, we restrict this optimization to plain SELECTs.
	 */
	if (parse->commandType != CMD_SELECT ||
		parse->rowMarks ||
		parse->setOperations)
		return NIL;

	/*
	 * The query must reference multiple tables, else we certainly aren't
	 * going to find any suitable OR clauses.  Do a quick check that there's
	 * more than one RTE.
	 */
	if (list_length(parse->rtable) < 2)
		return NIL;

	/*
	 * Scan the top-level WHERE clause looking for suitable OR clauses, and
	 * for each one, generate paths for the UNION ALL input sub-queries. There
	 * might be more than one suitable OR clause, in which case we can try the
	 * transformation for each one of them separately and add that list of
	 * paths to the results.
	 *
	 * XXX we should also search the JOIN/ON clauses of any top-level inner
	 * JOIN nodes, since those are semantically equivalent to WHERE.  But it's
	 * hard to see how to do that without either copying the whole JOIN tree
	 * in advance or repeating the search after copying, and neither of those
	 * options seems attractive.
	 */
	n = 0;
	foreach(lc, (List *) parse->jointree->quals)
	{
		Node	   *qual = (Node *) lfirst(lc);
		List	   *armrelids;

		if (or_clause(qual) &&
			polar_is_suitable_join_or_clause((BoolExpr *) qual, &armrelids))
		{
			List	   *subpaths;

			/*
			 * Check that the query as a whole is safe for this optimization.
			 * We only need to do this once, but it's somewhat expensive, so
			 * don't do it till we find a candidate OR clause.
			 */
			if (!checked_query)
			{
				if (!polar_is_query_safe_for_union_or_transform(root))
					return NIL;
				checked_query = true;
			}
			/* OK, transform the query and create a list of sub-paths */
			subpaths = polar_create_union_or_subpaths(root, (BoolExpr *) qual,
												n, armrelids);
			results = lappend(results, subpaths);
		}
		n++;
	}

	return results;
}

/*
 * Finish constructing Paths representing the UNION ALL implementation of join
 * OR clause(s), and attach them to "joinrel", which is the final scan/join
 * relation returned by query_planner() for the conventional implementation of
 * the query.  "union_or_subpaths" is the output of split_join_or_clauses().
 */
void
polar_finish_union_or_paths(PlannerInfo *root, RelOptInfo *joinrel,
					  List *union_or_subpaths)
{
	ListCell   *lc;

	/* This loop iterates once per splittable OR clause */
	foreach(lc, union_or_subpaths)
	{
		List	   *subpaths = (List *) lfirst(lc);
		List	   *common_exprs;
		PathTarget *common_target;
		Path	   *appendpath;
		ListCell   *lc2;

		/*
		 * This coding assumes that the commonly available Vars will appear in
		 * the same order in each subpath target, which should be true but
		 * it's surely an implementation artifact.  If it stops being true, we
		 * could fall back on list_intersection(), but that'd be O(N^3).
		 */
		common_exprs = (List *)
			copyObject(((Path *) linitial(subpaths))->pathtarget->exprs);
		for_each_cell(lc2, lnext(list_head(subpaths)))
		{
			Path	   *subpath = (Path *) lfirst(lc2);
			ListCell   *lcs;
			ListCell   *lcc;
			ListCell   *prevc;

			lcs = list_head(subpath->pathtarget->exprs);
			prevc = NULL;
			lcc = list_head(common_exprs);
			while (lcc)
			{
				ListCell   *nextc = lnext(lcc);

				if (lcs && equal(lfirst(lcs), lfirst(lcc)))
				{
					lcs = lnext(lcs);
					prevc = lcc;
				}
				else
					/*no cover line*/
					common_exprs = list_delete_cell(common_exprs, lcc, prevc);
				lcc = nextc;
			}
		}
		common_target = create_empty_pathtarget();
		common_target->exprs = common_exprs;
		set_pathtarget_cost_width(root, common_target);
		/* Now forcibly apply this target to each subpath */
		foreach(lc2, subpaths)
		{
			Path	   *subpath = (Path *) lfirst(lc2);

			lfirst(lc2) = create_projection_path(root,
												 joinrel,
												 subpath,
												 common_target);
		}

		/*
		 * Generate Append path combining the sub-paths for this UNION.  The
		 * Append path's pathtarget has to match what is actually coming out
		 * of the subpaths, since Append can't project.
		 */
		appendpath = (Path *) create_append_path(root, joinrel, subpaths, NIL,
												 NULL, 0, false, NIL, -1);
		appendpath->pathtarget = common_target;

		/* Attach it to the joinrel */
		add_path(joinrel, (Path *) appendpath);
	}

	/* We need to refigure which is the cheapest path for the joinrel */
	set_cheapest(joinrel);
}

/*
 * Is this OR clause a suitable clause for splitting?
 *
 * Each of its arms must reference just one rel, and they must not all be
 * the same rel.
 * On success, pass back a list of the relids referenced by each OR arm,
 * so we don't have to repeat the pull_varnos() work later.
 */
static bool
polar_is_suitable_join_or_clause(BoolExpr *orclause, List **relids)
{
	bool		ok = false;
	List	   *relidlist = NIL;
	int			firstrelid = 0;
	ListCell   *lc;

	*relids = NIL;				/* prevent uninitialized-variable warnings */
	foreach(lc, orclause->args)
	{
		Node	   *qual = (Node *) lfirst(lc);
		Relids		varnos = pull_varnos(qual);
		int			relid;

		if (!bms_get_singleton_member(varnos, &relid))
			return false;		/* this arm fails the sine qua non */
		if (relidlist == NIL)
			firstrelid = relid;
		else if (firstrelid != relid)
			ok = true;			/* arms reference more than one relid */
		relidlist = lappend_int(relidlist, relid);
	}
	*relids = relidlist;
	return ok;
}

/*
 * Is query as a whole safe to apply union OR transformation to?
 * This checks relatively-expensive conditions that we don't want to
 * worry about until we've found a candidate OR clause.
 */
static bool
polar_is_query_safe_for_union_or_transform(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	Relids		allbaserels;
	ListCell   *lc;
	int			relid;

	/*
	 * Must not have any volatile functions in FROM or WHERE (see notes at
	 * head of file).
	 */
	if (contain_volatile_functions((Node *) parse->jointree))
		return false;

	allbaserels = get_relids_in_jointree((Node *) parse->jointree, false);
	relid = 0;
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		relid++;

		/* ignore RTEs that aren't referenced baserels */
		if (!bms_is_member(relid, allbaserels))
			continue;

		/* disallow TABLESAMPLE (might be okay if repeatable?) */
		if (rte->tablesample)
			return false;

		/* check for volatiles in security barrier quals */
		if (contain_volatile_functions((Node *) rte->securityQuals))
			return false;
	}

	/* OK to proceed */
	return true;
}

/*
 * Split the query and the given OR clause into one UNION ALL arm per relation
 * mentioned in the OR clause, and make a list of best paths for the UNION ALL
 * arms.  (Since the UNION ALL step will lose any ordering or fast-start
 * properties of the paths, there's no need to consider any but the
 * cheapest-total path for each arm; hence it's okay to return just one path
 * per arm.)
 *
 * "n" is the OR clause's index in the query's WHERE list.
 * "armrelids" is the OR-arm-to-referenced-rel mapping.
 */
static List *
polar_create_union_or_subpaths(PlannerInfo *root, BoolExpr *orclause,
						 int n, List *armrelids)
{
	List	   *subpaths = NIL;
	Relids		orrels;
	int			relid;
	ListCell   *lc;
	ListCell   *lc2;
	List	   *dedup_quals = NIL;

	/*
	 * There might be multiple OR arms referring to the same rel, which we
	 * should combine into a restriction OR clause.  So first identify the set
	 * of rels used in the OR.
	 */
	orrels = NULL;
	foreach(lc, armrelids)
		orrels = bms_add_member(orrels, lfirst_int(lc));

	/* Now, for each such rel, generate a path for a UNION ALL arm */
	while ((relid = bms_first_member(orrels)) >= 0)
	{
		List	   *orarms;
		PlannerInfo *subroot;
		Query	   *parse;
		List	   *subquery_quals;
		bool		hasOuterJoins;
		RelOptInfo *final_rel;
		Path	   *subpath;
		int			k;
		ListCell   *prev;
		Node	   *not_qual = NULL;
		NullTest   *isnull_qual = NULL;
		Node	   *dedup_qual = NULL;

		/* Extract the OR arms for this rel, making copies for safety */
		orarms = NIL;
		forboth(lc, orclause->args, lc2, armrelids)
		{
			Node	   *qual = (Node *) lfirst(lc);
			int			qualrelid = lfirst_int(lc2);

			if (qualrelid == relid)
				orarms = lappend(orarms, copyObject(qual));
		}
		Assert(orarms != NIL);
		if (list_length(orarms) == 1)
		{
			/*
			 * When there's just a single arm for this rel (the typical case),
			 * it goes directly into the subquery's WHERE list.  But it might
			 * be a sub-AND, in which case we must flatten it into a qual list
			 * to preserve AND/OR flatness.
			 */
			orarms = make_ands_implicit((Expr *) linitial(orarms));
		}
		else
		{
			/*
			 * When there's more than one arm, convert back to an OR clause.
			 * No flatness worries here; the arms were already valid OR-list
			 * elements.
			 */
			orarms = list_make1(make_orclause(orarms));
		}

		/* Clone the planner's state */
		subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
		memcpy(subroot, root, sizeof(PlannerInfo));
		subroot->parse = parse = (Query *) copyObject(root->parse);
		/* Making copies of these might be overkill, but be safe */
		subroot->processed_tlist = (List *) copyObject(root->processed_tlist);
		subroot->append_rel_list = (List *) copyObject(root->append_rel_list);
		/* Tell query_planner to expect full retrieval of UNION input */
		subroot->tuple_fraction = 1.0;
		subroot->limit_tuples = -1.0;

		/*
		 * Remove the subquery's copy of the original OR clause, which we
		 * identify by its index in the WHERE clause list.
		 */
		subquery_quals = (List *) parse->jointree->quals;
		k = 0;
		prev = NULL;
		foreach(lc, subquery_quals)
		{
			if (k == n)
			{
				subquery_quals = list_delete_cell(subquery_quals, lc, prev);
				break;
			}
			k++;
			prev = lc;
		}

		/* make NOT qual, it must before list_concat() modifiy orarms */
		not_qual = (Node *) makeBoolExpr(NOT_EXPR, copyObject(orarms), -1);

		/* And instead add the qual or quals we extracted from the OR clause */
		subquery_quals = list_concat(subquery_quals, orarms);
		subquery_quals = list_concat(subquery_quals, copyObject(dedup_quals));

		/* make IS NULL qual */
		isnull_qual = (NullTest *) makeNode(NullTest);
		isnull_qual->arg = (Expr *) copyObject(linitial(orarms));
		isnull_qual->nulltesttype = IS_NULL;
		isnull_qual->location = -1;

		dedup_qual = (Node *) makeBoolExpr(OR_EXPR, 
										   list_make2(not_qual, isnull_qual), -1);

		dedup_quals = lappend(dedup_quals, dedup_qual);

		parse->jointree->quals = (Node *) subquery_quals;

		/* Re-apply reduce_outer_joins() in case we can now reduce some */
		/* (XXX would be better if this just got moved into query_planner) */
		hasOuterJoins = false;
		foreach(lc, parse->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

			if (rte->rtekind == RTE_JOIN)
			{
				if (IS_OUTER_JOIN(rte->jointype))
				{
					hasOuterJoins = true;
					break;
				}
			}
		}
		if (hasOuterJoins)
			reduce_outer_joins(subroot);

		/* Plan the modified query */
		final_rel = query_planner(subroot, subroot->processed_tlist,
								  polar_union_or_qp_callback, NULL);

		/*
		 * Get the cheapest-total path for the subquery; there's little value
		 * in considering any others.
		 */
		subpath = final_rel->cheapest_total_path;
		Assert(subpath);

		/* Add cheapest-total path to subpaths list */
		subpaths = lappend(subpaths, subpath);
	}

	return subpaths;
}

/*
 * Compute query_pathkeys and other pathkeys during plan generation
 */
static void
polar_union_or_qp_callback(PlannerInfo *root, void *extra)
{
	/*
	 * Since the output of the subquery is going to go through a UNION ALL step
	 * that destroys ordering, there's little need to worry about what its
	 * output order is.  Hence, don't bother telling it about pathkeys that
	 * might apply to these later execution steps.
	 */
	root->group_pathkeys = NIL;
	root->window_pathkeys = NIL;
	root->distinct_pathkeys = NIL;
	root->sort_pathkeys = NIL;
	root->query_pathkeys = NIL;
}
