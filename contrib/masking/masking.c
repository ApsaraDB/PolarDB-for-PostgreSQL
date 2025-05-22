/*-------------------------------------------------------------------------
*
* masking_label.c
*
* IDENTIFICATION
*    contrib/masking/masking_label.c
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"
#include "masking.h"

#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

bool		masking_enabled = false;
static const Oid masking_func_arg1_array[] = {TEXTOID};
static const Oid masking_func_arg2_array[] = {TEXTOID, BPCHAROID};

static polar_masking_hook_type next_polar_masking_hook = NULL;
static void polar_next_masking_process_hook(List *query_list);

static inline Node *create_string_node(Datum dat);
static inline Node *create_integer_node(int value);
static inline Node *create_relabeltype_node(Node *node, int resulttype);
static Node *make_func_node(int funcid, Oid rettype, Node *arg);
static Oid	get_function_id(int arglen, const Oid argarr[], const char *funcname);
static Oid	get_masking_funcid(int masking_op);
static Node *create_masking_func_node(Var *var, int masking_op);
static Node *create_masking_var_node(MaskingInfo * maskinfo, Var *var);
static void get_relid_colid_from_var(RangeTblEntry *rte, Var *var, Oid *relid, AttrNumber *colid);
static void get_relid_colid_from_joinvar(List *rtable, const RangeTblEntry *rte, const Var *var, Oid *relid, AttrNumber *colid);
static void get_masking_relid_colid(List *rtable, Var *var, Oid *relid, AttrNumber *colid);
static Node *masking_process_var_node(Node *src_var, List *rtable);
static Node *masking_vars_walker(Node *node, void *rtable);
static void masking_process_targetlist(List **targetList, List *rtable);
static void masking_process_select_query(Query *query);
static bool masking_process_union(Node *union_node, Query *query);
static void masking_process_selectcmd(Query *query);
static void masking_process_after_rewrite(Query *query);


static inline Node *
create_string_node(Datum dat)
{
	Const	   *const_node = makeConst(TEXTOID,
									   -1,	/* typmod -1 is OK for all cases */
									   InvalidOid,	/* all cases are
													 * uncollatable types */
									   -2,
									   dat,
									   false,
									   false);

	return (Node *) const_node;
}

static inline Node *
create_integer_node(int value)
{
	Const	   *const_node = makeConst(INT4OID,
									   -1,	/* typmod -1 is OK for all cases */
									   InvalidOid,	/* all cases are
													 * uncollatable types */
									   sizeof(int32),
									   Int32GetDatum(value),
									   false,
									   true);

	return (Node *) const_node;
}


static inline Node *
create_relabeltype_node(Node *node, int resulttype)
{
	CoercionForm relabelformat;
	RelabelType *typeexpr;

	if (node == NULL)
	{
		return NULL;
	}
	relabelformat = COERCE_EXPLICIT_CAST;
	typeexpr = makeNode(RelabelType);
	if (!typeexpr)
		return NULL;
	typeexpr->arg = (Expr *) node;
	typeexpr->resulttype = resulttype;
	typeexpr->resulttypmod = -1;
	typeexpr->resultcollid = get_typcollation(resulttype);	/* OID of collation */
	typeexpr->relabelformat = relabelformat;
	return (Node *) typeexpr;
}

static Node *
make_func_node(int funcid, Oid rettype, Node *arg)
{
	FuncExpr   *funcexpr = makeNode(FuncExpr);

	funcexpr->funcid = funcid;
	funcexpr->funcresulttype = rettype;
	funcexpr->funcretset = false;
	funcexpr->funcvariadic = false;
	funcexpr->funcformat = COERCE_EXPLICIT_CALL;

	if (arg != NULL)
	{
		funcexpr->args = lappend(funcexpr->args, arg);
	}
	funcexpr->funccollid = get_typcollation(rettype);
	funcexpr->inputcollid = 100;

	return (Node *) funcexpr;
}


/*
 * Get funcition's oid
 */
static Oid
get_function_id(int arglen, const Oid argarr[], const char *funcname)
{
	CatCList   *catlist = NULL;
	Oid			funcid = InvalidOid;

	catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));

	if (catlist != NULL)
	{
		for (int i = 0; i < catlist->n_members; ++i)
		{
			HeapTuple	proctup = &catlist->members[i]->tuple;
			Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
			bool		samearg = true;;
			if (procform)
			{
				if (procform->pronamespace != POLAR_MASKING_NAMESPACE)
					continue;

				if (arglen != procform->pronargs)
					continue;

				for (int i = 0; i < arglen; i++)
				{
					if (argarr[i] != procform->proargtypes.values[i])
					{
						samearg = false;
						break;
					}
				}
				if (samearg)
				{
					funcid = procform->oid;
					break;
				}
			}
		}
	}

	ReleaseSysCacheList(catlist);
	return funcid;
}

/*
 * Get masking funcition's oid
 */
static Oid
get_masking_funcid(int masking_op)
{
	Oid			funcid = InvalidOid;

	Assert(masking_op != MASKING_UNKNOWN);

	switch (masking_op)
	{
		case MASKING_CREDITCARD:
			funcid = get_function_id(2, masking_func_arg2_array, "creditcardmasking");
			break;
		case MASKING_BASICEMAIL:
			funcid = get_function_id(2, masking_func_arg2_array, "basicemailmasking");
			break;
		case MASKING_FULLEMAIL:
			funcid = get_function_id(2, masking_func_arg2_array, "fullemailmasking");
			break;
		case MASKING_ALLDIGITS:
			funcid = get_function_id(2, masking_func_arg2_array, "alldigitsmasking");
			break;
		case MASKING_SHUFFLE:
			funcid = get_function_id(1, masking_func_arg1_array, "shufflemasking");
			break;
		case MASKING_RANDOM:
			funcid = get_function_id(1, masking_func_arg1_array, "randommasking");
			break;
		case MASKING_ALL:
			funcid = get_function_id(2, masking_func_arg2_array, "maskall");
			break;
		default:
			elog(ERROR, "unknown masking operator: %d", masking_op);
			break;
	}
	return funcid;
}

static Node *
create_masking_func_node(Var *var, int masking_op)
{
	Node	   *masking_func_node = NULL;
	Oid			funcid = get_masking_funcid(masking_op);

	if (funcid == InvalidOid)
		return masking_func_node;

	switch (var->vartype)
	{
		case TEXTOID:
			{
				masking_func_node = make_func_node(funcid, TEXTOID, (Node *) var);
			}
			break;
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
			{
				masking_func_node = make_func_node(funcid, TEXTOID, (Node *) var);
				if (masking_func_node != NULL)
				{
					Node	   *cast_node = create_relabeltype_node(masking_func_node, var->vartype);

					if (cast_node != NULL)
					{			/* success */
						masking_func_node = cast_node;
					}
				}
			}
			break;
		case NAMEOID:
			{
				Node	   *text_node = make_func_node(MASKING_NAMETOTEXT_FUNCID, TEXTOID, (Node *) var);

				if (text_node != NULL)
					masking_func_node = make_func_node(funcid, TEXTOID, text_node);
				if (masking_func_node != NULL)
				{
					Node	   *cast_func = make_func_node(MASKING_TEXTTONAME_FUNCID, NAMEOID, masking_func_node);

					if (cast_func != NULL)
					{			/* success */
						masking_func_node = cast_func;
					}
				}
			}
			break;
		default:
			break;
	}
	return masking_func_node;
}

/*
 * Create masked var node
 */
static Node *
create_masking_var_node(MaskingInfo * maskinfo, Var *var)
{
	Node	   *masked_node = NULL;

	masked_node = create_masking_func_node(var, maskinfo->masking_op);

	if (masked_node == NULL)
	{
		masked_node = (Node *) var;
	}
	return masked_node;
}

static void
get_relid_colid_from_var(RangeTblEntry *rte, Var *var, Oid *relid, AttrNumber *colid)
{
	if (rte && rte->relid > 0)
	{
		Form_pg_class reltup;
		HeapTuple	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));

		reltup = (Form_pg_class) GETSTRUCT(tp);
		if (reltup && reltup->relkind == RELKIND_RELATION)
		{
			*relid = rte->relid;
			*colid = var->varattno;
		}
		else
		{
			*relid = InvalidOid;
			*colid = InvalidAttrNumber;
		}
		ReleaseSysCache(tp);
	}
}

static void
get_relid_colid_from_joinvar(List *rtable, const RangeTblEntry *rte, const Var *var, Oid *relid, AttrNumber *colid)
{
	ListCell   *cell = NULL;
	int			joinpos = 1;
	Var		   *joinvar;

	foreach(cell, rte->joinaliasvars)
	{
		if (joinpos != (int) var->varattno)
		{
			++joinpos;
			continue;
		}
		joinvar = (Var *) lfirst(cell);

		get_masking_relid_colid(rtable, joinvar, relid, colid);
		break;
	}
}

/*
 * Get relid and colid of var node
 */
static void
get_masking_relid_colid(List *rtable, Var *var, Oid *relid, AttrNumber *colid)
{
	ListCell   *cell = NULL;
	int			pos = 1;
	RangeTblEntry *rte;

	foreach(cell, rtable)
	{
		if (pos != (int) var->varno)
		{
			++pos;
			continue;
		}

		rte = (RangeTblEntry *) lfirst(cell);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				if (OidIsValid(rte->relid))
				{
					get_relid_colid_from_var(rte, var, relid, colid);
				}
				break;
			case RTE_JOIN:
				get_relid_colid_from_joinvar(rtable, rte, var, relid, colid);
				break;
			default:
				break;
		}
		break;
	}
}

static Node *
masking_process_var_node(Node *src_var, List *rtable)
{
	Var		   *var;
	MaskingInfo maskinfo;

	MemSet(&maskinfo, 0, sizeof(MaskingInfo));

	var = (Var *) src_var;

	get_masking_relid_colid(rtable, var, &maskinfo.relid, &maskinfo.attnum);

	if (check_masking_for_var(&maskinfo))
	{
		return create_masking_var_node(&maskinfo, var);
	}
	return src_var;
}

/*
 * Recursively process var
 */
static Node *
masking_vars_walker(Node *node, void *rtable)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Node	   *newnode = expression_tree_mutator(
													  node, masking_vars_walker, (void *) rtable);

		return masking_process_var_node(newnode, (List *) rtable);
	}
	if (IsA(node, Query))
	{
		masking_process_after_rewrite((Query *) node);
		return node;
	}
	return expression_tree_mutator(
								   node, masking_vars_walker, (void *) rtable);
}

static void
masking_process_targetlist(List **targetList, List *rtable)
{
	List	   *target = *targetList;
	Node	   *res;

	if (target == NIL || rtable == NIL)
	{
		return;
	}

	res = masking_vars_walker((Node *) target, (void *) rtable);
	list_free_deep(target);
	*targetList = (List *) res;
}

static void
masking_process_select_query(Query *query)
{
	Assert(query != NULL);
	/* we only process vars in targetlist */
	if (query->targetList != NIL)
	{
		masking_process_targetlist(&(query->targetList), query->rtable);
	}
}

static bool
masking_process_union(Node *union_node, Query *query)
{
	if (union_node == NULL)
	{
		return false;
	}
	switch (nodeTag(union_node))
	{
			/*
			 * For union, recursively proecess masking
			 */
		case T_SetOperationStmt:
			{
				SetOperationStmt *stmt = (SetOperationStmt *) union_node;

				if (stmt->op != SETOP_UNION)
				{
					return false;
				}
				masking_process_union((Node *) (stmt->larg), query);
				masking_process_union((Node *) (stmt->rarg), query);
			}
			break;
		case T_RangeTblRef:
			{
				RangeTblRef *ref = (RangeTblRef *) union_node;
				Query	   *mostQuery;

				if (ref->rtindex <= 0 || ref->rtindex > list_length(query->rtable))
				{
					return false;
				}
				mostQuery = rt_fetch(ref->rtindex, query->rtable)->subquery;
				masking_process_select_query(mostQuery);
			}
			break;
		default:
			break;
	}
	return true;
}

static void
masking_process_selectcmd(Query *query)
{
	if (query == NULL)
	{
		return;
	}

	/* process set-operation tree */
	if (!masking_process_union(query->setOperations, query))
	{
		ListCell   *lc = NULL;

		/* process query in cte */
		if (query->cteList != NIL)
		{
			foreach(lc, query->cteList)
			{
				CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
				Query	   *cte_query = (Query *) cte->ctequery;

				masking_process_selectcmd(cte_query);
			}
		}
		/* process each subquery */
		if (query->rtable != NULL)
		{
			foreach(lc, query->rtable)
			{
				RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
				Query	   *subquery = (Query *) rte->subquery;

				masking_process_selectcmd(subquery);
			}
		}
		masking_process_select_query(query);
	}
}


static void
masking_process_after_rewrite(Query *query)
{
	switch (query->commandType)
	{
		case CMD_SELECT:
			{
				masking_process_selectcmd(query);
				break;
			}
		case CMD_UPDATE:
		case CMD_DELETE:
		case CMD_INSERT:
			{
				if (query->rtable != NIL)
				{
					ListCell   *lc = NULL;

					/* process INSERT/UPDATE/DELETE with RETURNING clause */
					if (query->returningList != NIL)
					{
						masking_process_targetlist(&(query->returningList), query->rtable);
					}

					if (query->targetList != NIL)
					{
						masking_process_targetlist(&(query->targetList), query->rtable);
					}

					foreach(lc, query->rtable)
					{
						RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

						if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
						{
							masking_process_targetlist(&(rte->subquery->targetList), rte->subquery->rtable);
						}
					}
				}
				break;
			}
		default:
			break;
	}

}


static void
polar_next_masking_process_hook(List *query_list)
{
	ListCell   *lc;
	Node	   *node;

	if (!masking_enabled || query_list == NIL)
	{
		return;
	}

	foreach(lc, query_list)
	{
		node = (Node *) lfirst(lc);
		if (nodeTag(node) == T_Query)
			masking_process_after_rewrite((Query *) node);
	}

	if (next_polar_masking_hook)
	{
		next_polar_masking_hook(query_list);
	}
}

void
_PG_init(void)
{
	next_polar_masking_hook = polar_masking_hook;
	polar_masking_hook = polar_next_masking_process_hook;

	DefineCustomBoolVariable(
							 "masking.masking_enabled",
							 "if masking is enabled",
							 NULL,
							 &masking_enabled,
							 false,
							 PGC_SIGHUP,
							 GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);
}

/*
 * Module unload function
 */
void
_PG_fini(void)
{
	/* uninstall hook */
	polar_masking_hook = next_polar_masking_hook;

}
