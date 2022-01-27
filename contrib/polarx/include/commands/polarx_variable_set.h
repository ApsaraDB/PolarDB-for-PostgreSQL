/*-------------------------------------------------------------------------
 *
 * polarx_variable_set.h
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * DENTIFICATION
 *        contrib/polarx/include/commands/polarx_variable_set.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_VARIABLE_SET_H
#define POLARX_VARIABLE_SET_H
#include "utils/array.h"
#include "utils/guc.h"
#include "nodes/parsenodes.h"

extern const char *quote_guc_value(const char *value, int flags);
extern void ExecSetVariableStmtPre(VariableSetStmt *stmt);
extern void ExecSetVariableStmtPost(VariableSetStmt *stmt);
extern void PolarxSetPGVariable(const char *name, List *args, bool is_local);
extern void PolarxNodeSetParam(bool local, const char *name, const char *value); 
extern void PolarxProcessGUCArray(ArrayType *array,
                                    GucSource source, GucAction action);
extern void PolarxSetBackGUCArray(ArrayType *array,
                                    GucSource source, GucAction action);
#endif /* POLARX_VARIABLE_SET_H */
