/*-------------------------------------------------------------------------
 *
 * polarx_option.h
 *
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * IDENTIFICATION
 *		  contrib/polarx/include/polarx/polarx_option.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_OPTION_H
#define POLARX_OPTION_H 

#include "pgxc/locator.h"

typedef enum
{
    SERVER_OPTION_HOST,
    SERVER_OPTION_PORT
} ServerOption;

extern int ExtractConnectionOptions(List *defelems,
						 const char **keywords,
						 const char **values);
extern List *ExtractExtensionList(const char *extensionsString,
					 bool warnOnMissing);

extern char *GetClusterServerOption(List *options, int nodeIndex, ServerOption type);
extern char *GetClusterTableOption(List *options, TableOption type);
extern List *GetSingleNodeLibpgOption(List *options, int nodeIndex);
extern int GetNodesNum(List *options);

extern void RegisterFdwTxnCallback(void);
#endif							/* POLARX_OPTION_H */
