/*-------------------------------------------------------------------------
 *
 * nodeMotion_px.h
 *
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2004, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeMotion_px.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMOTION_H
#define NODEMOTION_H

#include "nodes/execnodes.h"

extern MotionState *ExecInitMotion(Motion *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMotion(MotionState *node);
extern void ExecEndMotion(MotionState *node);
extern void ExecReScanMotion(MotionState *node);
extern void ExecSquelchMotion(MotionState *node);

#endif   /* NODEMOTION_H */
