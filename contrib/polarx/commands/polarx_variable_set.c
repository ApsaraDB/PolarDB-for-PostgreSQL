/*-------------------------------------------------------------------------
 * polarx_variable_set.c
 *    support for propagation of SET including SET/SET LOCAL/SET in fuction
 *    for polarx 
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_variable_set.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgxc/connpool.h"
#include "executor/execRemoteQuery.h"
#include "commands/polarx_variable_set.h"
#include "utils/builtins.h"
#include "parser/parse_type.h"
#include "catalog/pg_type.h"
#include "distribute_transaction/txn.h"
#include "nodes/polarx_node.h"


static void strreplace_all(char *str, char *needle, char *replacement);
static char *flatten_set_variable_args(const char *name, List *args);
static void set_remote_config_option(const char *name, const char *value, bool local);
/*
 * Return a quoted GUC value, when necessary
 */
const char *
quote_guc_value(const char *value, int flags)
{
    char *new_value;

    if (value == NULL)
        return value;

    /*
     * An empty string is what gets created when someone fires SET var TO ''.
     * We must send it in its original form to the remote node.
     */
    if (!value[0])
        return "''";

    /*
     * A special case for empty string list members which may get replaced by
     * "\"\"" when flatten_set_variable_args gets called. So replace that back
     * to ''.
     */
    new_value = pstrdup(value);
    strreplace_all(new_value, "\"\"", "''");
    value = new_value;

    /* Finally don't quote empty string */
    if (strcmp(value, "''") == 0)
        return value;

    /*
     * If the GUC rceives list input, then the individual elements in the list
     * must be already quoted correctly by flatten_set_variable_args(). We must
     * not quote the entire value again.
     *
     * We deal with empty strings which may have already been replaced with ""
     * by flatten_set_variable_args. Unquote them so that remote side can
     * handle it.
     */
    if (flags & GUC_LIST_INPUT)
        return value;

    /*
     * Otherwise quote the value. quote_identifier() takes care of correctly
     * quoting the value when needed, including GUC_UNIT_MEMORY and
     * GUC_UNIT_TIME values.
     */
    return quote_identifier(value);
}

/*
 * Replace all occurrences of "needle" with "replacement". We do in-place
 * replacement, so "replacement" must be smaller or equal to "needle"
 */
static void
strreplace_all(char *str, char *needle, char *replacement)
{
    char       *s;

    s = strstr(str, needle);
    while (s != NULL)
    {
        int            replacementlen = strlen(replacement);
        char       *rest = s + strlen(needle);

        memcpy(s, replacement, replacementlen);
        memmove(s + replacementlen, rest, strlen(rest) + 1);
        s = strstr(str, needle);
    }
}

static void
set_remote_config_option(const char *name, const char *value, bool local)
{
    RemoteQuery    *step;
    StringInfoData  poolcmd;
    int flags;

    initStringInfo(&poolcmd);

    /*
     * Save new parameter value with the node manager.
     * XXX here we may check: if value equals to configuration default
     * just reset parameter instead. Minus one table entry, shorter SET
     * command sent downn... Sounds like optimization.
     */

    if (local)
    {
        flags = GetConfigOptionFlags(name, false);
        if (IsTransactionBlock())
            PGXCNodeSetParam(true, name, value, flags);
        value = quote_guc_value(value, flags);
        appendStringInfo(&poolcmd, "SET LOCAL %s TO %s", name,
                (value ? value : "DEFAULT"));
    }
    else if(strcmp(name, "RESET ALL") == 0)
    {
        PGXCNodeResetParams(false);
        appendStringInfo(&poolcmd, "RESET ALL");
    }
    else
    {
        flags = GetConfigOptionFlags(name, false);
        PGXCNodeSetParam(false, name, value, flags);
        value = quote_guc_value(value, flags);
        appendStringInfo(&poolcmd, "SET %s TO %s", name,
                (value ? value : "DEFAULT"));
    }

    /*
     * Send new value down to remote nodes if any is connected
     * XXX here we are creatig a node and invoke a function that is trying
     * to send some. That introduces some overhead, which may seem to be
     * significant if application sets a bunch of parameters before doing
     * anything useful - waste work for for each set statement.
     * We may want to avoid that, by resetting the remote parameters and
     * flagging that parameters needs to be updated before sending down next
     * statement.
     * On the other hand if session runs with a number of customized
     * parameters and switching one, that would cause all values are resent.
     * So let's go with "send immediately" approach: parameters are not set
     * too often to care about overhead here.
     */
    step = polarxMakeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->exec_nodes = NULL;
    step->sql_statement = poolcmd.data;
    /* force_autocommit is actually does not start transaction on nodes */
    step->force_autocommit = IsTransactionBlock() ? false : true;
    step->exec_type = EXEC_ON_CURRENT;
	g_in_set_config_option = true;
    ExecRemoteUtility(step);
	g_in_set_config_option = false;
    pfree(step);
    pfree(poolcmd.data);
}
void
ExecSetVariableStmtPre(VariableSetStmt *stmt)
{
    switch (stmt->kind)
    {
        case VAR_SET_VALUE:
        case VAR_SET_CURRENT:
            break;
        case VAR_SET_MULTI:

            if (strcmp(stmt->name, "TRANSACTION") == 0)
                /* SET TRANSACTION assumes "local" */
                    stmt->is_local = true;
            else if (strcmp(stmt->name, "SESSION CHARACTERISTICS") == 0)
                /* SET SESSION CHARACTERISTICS assumes "session" */
                    stmt->is_local = false;
            break;
        case VAR_SET_DEFAULT:
        case VAR_RESET:
        case VAR_RESET_ALL:
            break;
    }
}
void ExecSetVariableStmtPost(VariableSetStmt *stmt)
{
    bool is_local = stmt->is_local;
    char *name = NULL;
    char *value = NULL;
    if(IS_PGXC_LOCAL_COORDINATOR)
    {
        switch (stmt->kind)
        {
            case VAR_SET_VALUE:
            case VAR_SET_CURRENT:
                name = stmt->name;
                value = ExtractSetVariableArgs(stmt);
                set_remote_config_option(name, value, is_local);
                break;
            case VAR_SET_MULTI:
                {
                    ListCell   *head;

                    foreach(head, stmt->args)
                    {
                        DefElem    *item = (DefElem *) lfirst(head);

                        name = item->defname;
                        value = flatten_set_variable_args(name, list_make1(item->arg));
                        set_remote_config_option(name, value, is_local);
                    }
                }
                break;
            case VAR_SET_DEFAULT:
            case VAR_RESET:
                name = stmt->name;
                set_remote_config_option(name, value, is_local);
                break;
            case VAR_RESET_ALL:
                set_remote_config_option("RESET ALL", NULL, false);
                break;
        }
    }
}
static char *
flatten_set_variable_args(const char *name, List *args)
{
    StringInfoData buf;
    ListCell   *l;
    int flags = GetConfigOptionFlags(name, true);

    /* Fast path if just DEFAULT */
    if (args == NIL)
        return NULL;

    /* Complain if list input and non-list variable */
    if ((flags & GUC_LIST_INPUT) == 0 &&
            list_length(args) != 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("SET %s takes only one argument", name)));

    initStringInfo(&buf);

    /*
     * Each list member may be a plain A_Const node, or an A_Const within a
     * TypeCast; the latter case is supported only for ConstInterval arguments
     * (for SET TIME ZONE).
     */
    foreach(l, args)
    {
        Node       *arg = (Node *) lfirst(l);
        char       *val;
        TypeName   *typeName = NULL;
        A_Const    *con;

        if (l != list_head(args))
            appendStringInfoString(&buf, ", ");

        if (IsA(arg, TypeCast))
        {
            TypeCast   *tc = (TypeCast *) arg;

            arg = tc->arg;
            typeName = tc->typeName;
        }

        if (!IsA(arg, A_Const))
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(arg));
        con = (A_Const *) arg;

        switch (nodeTag(&con->val))
        {
            case T_Integer:
                appendStringInfo(&buf, "%d", intVal(&con->val));
                break;
            case T_Float:
                /* represented as a string, so just copy it */
                appendStringInfoString(&buf, strVal(&con->val));
                break;
            case T_String:
                val = strVal(&con->val);
                if (typeName != NULL)
                {
                    /*
                     * Must be a ConstInterval argument for TIME ZONE. Coerce
                     * to interval and back to normalize the value and account
                     * for any typmod.
                     */
                    Oid         typoid;
                    int32       typmod;
                    Datum       interval;
                    char       *intervalout;

                    typenameTypeIdAndMod(NULL, typeName, &typoid, &typmod);
                    Assert(typoid == INTERVALOID);

                    interval =
                        DirectFunctionCall3(interval_in,
                                CStringGetDatum(val),
                                ObjectIdGetDatum(InvalidOid),
                                Int32GetDatum(typmod));

                    intervalout =
                        DatumGetCString(DirectFunctionCall1(interval_out,
                                    interval));
                    appendStringInfo(&buf, "INTERVAL '%s'", intervalout);
                }
                else
                {
                    /*
                     * Plain string literal or identifier.  For quote mode,
                     * quote it if it's not a vanilla identifier.
                     */
                    if (flags & GUC_LIST_QUOTE)
                        appendStringInfoString(&buf, quote_identifier(val));
                    else
                        appendStringInfoString(&buf, val);
                }
                break;
            default:
                elog(ERROR, "unrecognized node type: %d",
                        (int) nodeTag(&con->val));
                break;
        }
    }

    return buf.data;
}

void
PolarxSetPGVariable(const char *name, List *args, bool is_local)
{
    char       *argstring = flatten_set_variable_args(name, args);

    if(!IS_PGXC_LOCAL_COORDINATOR)
        return;
    set_remote_config_option(name, argstring, is_local);
}

void PolarxNodeSetParam(bool local, const char *name, const char *value)
{
    int flags = GetConfigOptionFlags(name, false);    

    if(!IS_PGXC_LOCAL_COORDINATOR)
        return;
    if (local)
    {
        if (IsTransactionBlock())
            PGXCNodeSetParam(true, name, value, flags);
    }
    else
    {
        PGXCNodeSetParam(false, name, value, flags);
    }
}
void
PolarxProcessGUCArray(ArrayType *array,
         GucSource source, GucAction action)
{
    int         i;

    if(source != PGC_S_SESSION || !IS_PGXC_LOCAL_COORDINATOR)
        return;
    Assert(array != NULL);
    Assert(ARR_ELEMTYPE(array) == TEXTOID);
    Assert(ARR_NDIM(array) == 1);
    Assert(ARR_LBOUND(array)[0] == 1);

    for (i = 1; i <= ARR_DIMS(array)[0]; i++)
    {
        Datum       d;
        bool        isnull;
        char       *s;
        char       *name;
        char       *value;

        d = array_ref(array, 1, &i,
                -1 /* varlenarray */ ,
                -1 /* TEXT's typlen */ ,
                false /* TEXT's typbyval */ ,
                'i' /* TEXT's typalign */ ,
                &isnull);

        if (isnull)
            continue;

        s = TextDatumGetCString(d);

        ParseLongOption(s, &name, &value);
        if (!value)
        {
            ereport(WARNING,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("could not parse setting for parameter \"%s\"",
                         name)));
            free(name);
            continue;
        }

        set_remote_config_option(name, value, (action == GUC_ACTION_LOCAL));

        free(name);
        if (value)
            free(value);
        pfree(s);
    }
}
void
PolarxSetBackGUCArray(ArrayType *array,
         GucSource source, GucAction action)
{
    int         i;

    if(source != PGC_S_SESSION || !IS_PGXC_LOCAL_COORDINATOR)
        return;
    Assert(array != NULL);
    Assert(ARR_ELEMTYPE(array) == TEXTOID);
    Assert(ARR_NDIM(array) == 1);
    Assert(ARR_LBOUND(array)[0] == 1);

    for (i = 1; i <= ARR_DIMS(array)[0]; i++)
    {
        Datum       d;
        bool        isnull;
        char       *s;
        char       *name;
        char       *value;

        d = array_ref(array, 1, &i,
                -1 /* varlenarray */ ,
                -1 /* TEXT's typlen */ ,
                false /* TEXT's typbyval */ ,
                'i' /* TEXT's typalign */ ,
                &isnull);

        if (isnull)
            continue;

        s = TextDatumGetCString(d);

        ParseLongOption(s, &name, &value);
        value = GetConfigOptionByName(name, NULL, true);

        set_remote_config_option(name, value, (action == GUC_ACTION_LOCAL));

        free(name);
        if (value)
            free(value);
        pfree(s);
    }
}
