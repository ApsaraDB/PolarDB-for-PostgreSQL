/*-------------------------------------------------------------------------
 *
 * px_srlz.h
 *	  definitions for plan serialization utilities
 *
 * Portions Copyright (c) 2004-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_srlz.h
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXSRLZ_H
#define PXSRLZ_H

#include "nodes/nodes.h"

extern char *serializeNode(Node *node, int *size, int *uncompressed_size);
extern Node *deserializeNode(const char *strNode, int size);

#endif							/* PXSRLZ_H */
