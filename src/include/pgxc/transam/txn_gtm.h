/*-------------------------------------------------------------------------
 *
 * txn_gtm.h
 *
 *      GTM access
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/transam/txn_gtm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARDBX_TXN_GTM_H
#define POLARDBX_TXN_GTM_H

#include "postgres.h"

#include "access/gtm.h"
#include "pgxc/pgxc.h"

extern bool log_gtm_stats;

GTM_Timestamp GetGlobalTimestampGTM(void);

#endif /* POLARDBX_TXN_GTM_H */
