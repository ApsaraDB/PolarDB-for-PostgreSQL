/*-------------------------------------------------------------------------
 * polarx_dbcommands.c
 *   utitily functions in polarx for Database management commands
 *   (create/drop database).
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_dbcommands.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/polarx_dbcommands.h"

bool
IsSetTableSpace(AlterDatabaseStmt *stmt)
{
    ListCell   *option;
    /* Handle the SET TABLESPACE option separately */
    foreach(option, stmt->options)
    {
        DefElem    *defel = (DefElem *) lfirst(option);
        if (strcmp(defel->defname, "tablespace") == 0)
            return true;
    }
    return false;
}
