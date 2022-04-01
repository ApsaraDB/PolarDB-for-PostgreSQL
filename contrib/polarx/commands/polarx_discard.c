/*-------------------------------------------------------------------------
 * polarx_discard.c
 *   The implementation of the DISCARD command in polarx
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_discard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/polarx_discard.h"
#include "commands/polarx_variable_set.h"

void
DiscardCommandPre(DiscardStmt *stmt)
{
}
void
DiscardCommandPost(DiscardStmt *stmt)
{
    switch (stmt->target)
    {
        case DISCARD_ALL:
            PolarxNodeSetParam(false, "session_authorization", NULL);
            break;
        case DISCARD_PLANS:
        case DISCARD_SEQUENCES:
        case DISCARD_TEMP:
        default:
            break;
    }
}
