/*-------------------------------------------------------------------------
 *
 * polarx_discard.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_discard.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_DISCARD_H
#define POLARX_DISCARD_H
#include "nodes/parsenodes.h"
extern void DiscardCommandPre(DiscardStmt *stmt);
extern void DiscardCommandPost(DiscardStmt *stmt);
#endif /* POLARX_DISCARD_H */
