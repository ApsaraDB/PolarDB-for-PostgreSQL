/*-------------------------------------------------------------------------
 *
 * polarx_connection.h
 *
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 * Copyright (c) 2020, Apache License Version 2.0
 *
 * IDENTIFICATION
 *		  contrib/polarx/include/polarx/polarx_connection.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARX_CONNECTION_H
#define POLARX_CONNECTION_H 

#include "foreign/foreign.h"
#include "libpq-fe.h"


extern PGconn *GetConnection(UserMapping *user, bool will_prep_stmt);
extern PGconn **GetConnections(UserMapping *user, bool will_prep_stmt, List *node_list, int *conn_num);
extern void ReleaseConnection(PGconn *conn);
extern void ReleaseConnectionCluster(PGconn **conns);
extern unsigned int GetCursorNumber(PGconn **conns);
extern unsigned int GetPrepStmtNumber(PGconn *conn);
extern PGresult *pgfdw_get_result(PGconn *conn, const char *query);
extern PGresult *pgfdw_exec_query(PGconn *conn, const char *query);
extern void pgfdw_report_error(int elevel, PGresult *res, PGconn *conn,
				   bool clear, const char *sql);
extern void pgfdw_async_exec_query_single(PGconn *conn, const char *query);
extern void pgfdw_exec_query_cluster(PGconn **conns, const char *query, int conn_num);
extern void pgfdw_exec_query_cluster_custom(PGconn **conns, const char *query, int conn_num, bool *conn_finish);
extern void RegisterFdwTxnCallback(void);
#endif							/* POSTGRES_FDW_H */
