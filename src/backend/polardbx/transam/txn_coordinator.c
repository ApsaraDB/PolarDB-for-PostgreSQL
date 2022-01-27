/*-------------------------------------------------------------------------
 *
 * execRemoteTrans.c
 *
 *      Distributed transaction coordination
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 * src/backend/polardbx/transam/txn_coordinator.c
 *
 *-------------------------------------------------------------------------
 */

#include "pgxc/transam/txn_coordinator.h"

#include <time.h>
#include <unistd.h>

#include "access/gtm.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "commands/tablecmds.h"
#include "gtm/gtm_c.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "pgxc/connpool.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/recvRemote.h"
#include "pgxc/transam/txn_gtm.h"
#include "distributed_txn/txn_timestamp.h"
#include "pgxc/transam/txn_util.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/tqual.h"
#include "pgxc/execRemoteQuery.h"
#include "pgxc/nodemgr.h"

LocalTwoPhaseState g_twophase_state;

bool g_in_plpgsql_exec_fun = false;
bool PlpgsqlDebugPrint = false;
bool force_2pc = true;
#ifdef POLARDB_X
bool g_in_set_config_option = false;
#endif

MemoryContext CommitContext = NULL;
TxnCoordinationType txn_coordination;

static bool temp_object_included = false;

static bool XactReadLocalNode;
static bool XactWriteLocalNode;
static HTAB *txn_datanode_queries = NULL;

typedef struct
{
	/* dynahash.c requires key to be first field */
	char stmt_name[NAMEDATALEN];
	int number_of_nodes;	 /* number of nodes where statement is active */
	int dns_node_indices[0]; /* node ids where statement is active */
} TxnDatanodeStatement;
#ifdef POLARDB_X
static long twophase_exception_pending_time = 10000000L; //10s
const char *error_request_sql = "select error_request();";
bool enable_twophase_recover_debug_print = false;
#endif

/* POLARX_TODO forward declaration */
extern bool PoolManagerCancelQuery(int dn_count, int *dn_list, int co_count, int *co_list, int signal);

static void ForgetTransactionLocalNode(void);
static bool print_twophase_state(StringInfo errormsg, bool isprint);
static bool IsTwoPhaseCommitRequired(bool localWrite);
static void get_partnodes(PGXCNodeAllHandles *handles, StringInfo participants);
static char *GetTransStateString(TwoPhaseTransState state);
static char *GetConnStateString(ConnState state);

static void pgxc_node_remote_cleanup_all(void);
static bool pgxc_node_remote_commit_prepared(char *prepareGID, bool commit, char *nodestring);
static char *pgxc_node_remote_prepare(const char *prepareGID, bool localNode, bool implicit);
static void pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle);
static void pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle);

static void InterceptBeforeCommitTimestamp(void);
static void InterceptAfterCommitTimestamp(void);
static void ResolveNodeString(char *nodestring, List **nodelist, List **coordlist, bool *prepared_local);
static void TxnCleanUpHandles(PGXCNodeAllHandles *handles, bool release_handle);
static bool IsNeedReleaseHandle();

static bool AbortRunningQuery(TranscationType txn_type, bool need_release_handle);
static int RemoteTransactionPrepareReadonly(PGXCNodeHandle *conn,
											const char *prepareGID,
											GlobalTimestamp global_prepare_ts,
											const char *commit_cmd,
											bool implicit);

static void InitTxnQueryHashTable(void);
static TxnDatanodeStatement *FetchTxnDatanodeStatement(const char *stmt_name, bool throwError);
static bool HaveActiveTxnDatanodeStatements(void);
static void DropAllTxnDatanodeStatement(void);
#ifdef POLARDB_X
static bool isXactWriteLocalNode(void);
static bool FinishGlobalXacts(char *prepareGID, char *nodestring);
#endif
static bool
IsNeedReleaseHandle()
{
	return !temp_object_included && !PersistentConnections;
}

/**
 * Clean up connections after transaction:
 * 1. clean up remote connections
 * 2. return handles back to the pooler
 * 3. free memory of handles manager
 */
static void
TxnCleanUpHandles(PGXCNodeAllHandles *handles, bool release_handle)
{
	DropAllTxnDatanodeStatement();
	if (IsNeedReleaseHandle())
	{
		pgxc_node_remote_cleanup_all();
		if (release_handle)
		{
			release_handles(false);
		}
	}
	clear_handles();
	pfree_pgxc_all_handles(handles);
}

static void
InterceptBeforeCommitTimestamp(void)
{
	if (delay_before_acquire_committs)
	{
		pg_usleep(delay_before_acquire_committs);
	}
}

static void
InterceptAfterCommitTimestamp(void)
{
	if (delay_after_acquire_committs)
	{
		pg_usleep(delay_after_acquire_committs);
	}
}

static void
ResolveNodeString(char *nodestring, List **nodelist, List **coordlist, bool *prepared_local)
{
	bool onlyone = true;
	char *nodename = strtok(nodestring, ",");
	while (nodename != NULL || onlyone)
	{
		int nodeIndex;
		char nodetype;

		if (onlyone)
		{
			nodename = nodestring;
		}
		onlyone = false;

		/* Get node type and index */
		nodetype = PGXC_NODE_NONE;
		nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
		if (nodetype == PGXC_NODE_NONE)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined", nodename)));

		/* Check if node is requested is the self-node or not */
		if (nodetype == PGXC_NODE_COORDINATOR)
		{
			if (nodeIndex == PGXCNodeId - 1)
				*prepared_local = true;
			else
				*coordlist = lappend_int(*coordlist, nodeIndex);
		}
		else
			*nodelist = lappend_int(*nodelist, nodeIndex);

		nodename = strtok(NULL, ",");
	}
}

static void
TwoPhaseStateAppendNode(bool datanode, int twophase_index, int handle_index,
						ConnState conn_state, TwoPhaseTransState trans_state)
{
	ConnTransState *node_state = datanode ? &g_twophase_state.datanode_state[twophase_index] : &g_twophase_state.coord_state[twophase_index];

	node_state->is_participant = true;
	node_state->handle_idx = handle_index;
	node_state->state = g_twophase_state.state;
	node_state->conn_state = conn_state;
	node_state->state = trans_state;
	if (datanode)
	{
		g_twophase_state.datanode_index++;
	}
	else
	{
		g_twophase_state.coord_index++;
	}
}

static void
TwoPhaseStateAppendConnection(bool is_datanode, int twophase_index)
{
	char node_type = is_datanode ? PGXC_NODE_DATANODE : PGXC_NODE_COORDINATOR;
	AllConnNodeInfo *conn = &g_twophase_state.connections[twophase_index];
	conn->node_type = node_type;
	conn->conn_trans_state_index = g_twophase_state.coord_index;
	g_twophase_state.connections_num++;
}

static int
RemoteTransactionPrepareReadonly(PGXCNodeHandle *conn,
								 const char *prepareGID,
								 GlobalTimestamp global_prepare_ts,
								 const char *commit_cmd,
								 bool implicit)
{
	if (implicit)
	{
		if (enable_distri_print)
		{
			elog(LOG,
				 "send prepare timestamp for xid %d gid %s prepare ts " INT64_FORMAT,
				 GetTopTransactionIdIfAny(),
				 prepareGID,
				 global_prepare_ts);
		}
	}
	/* Send down prepare command */
	if (pgxc_node_send_query(conn, commit_cmd))
	{
		/*
		 * not a big deal, it was read only, the connection will be
		 * abandoned later.
		 */
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to send COMMIT command to "
						"the node %u",
						conn->nodeoid)));
		return 1;
	}
	return 0;
}

void BeginTxnIfNecessary(PGXCNodeHandle *conn)
{
	Oid nodeoid = conn->nodeoid;

	if (NeedBeginTxn() && !NodeHasBeginTxn(nodeoid))
	{
		conn->plpgsql_need_begin_txn = true;
		SetNodeBeginTxn(nodeoid);
	}
	if (NeedBeginSubTxn() && !NodeHasBeginSubTxn(nodeoid))
	{
		conn->plpgsql_need_begin_sub_txn = true;
		SetNodeBeginSubTxn(nodeoid);
		if (PlpgsqlDebugPrint)
		{
			elog(LOG,
				 "[PLPGSQL] ExecRemoteQuery conn nodename:%s backendpid:%d sock:%d nodeoid:%u "
				 "need_begin_sub_txn",
				 conn->nodename,
				 conn->backend_pid,
				 conn->sock,
				 conn->nodeoid);
		}
	}
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
/* POLARX_TODO: remove gxid */
int pgxc_node_begin(int conn_count,
					PGXCNodeHandle **connections,
					GlobalTransactionId gxid,
					bool need_tran_block,
					bool readOnly,
					char node_type)
{
	const char *begin_cmd = "BEGIN";
	const char *begin_subtxn_cmd = "BEGIN_SUBTXN";
	const char *begin_both_cmd = "BEGIN;BEGIN_SUBTXN";
	int i;
	struct timeval *timeout = NULL;
	ResponseCombiner combiner;
	GlobalTimestamp start_ts = (GlobalTimestamp)TxnGetStartTs();
	PGXCNodeHandle *new_connections[conn_count];
	int new_count = 0;
	const char *cmd = begin_cmd;
	bool need_send_begin = false;

	/*
	 * If no remote connections, we don't have anything to do
	 */
	if (conn_count == 0)
		return 0;

	for (i = 0; i < conn_count; i++)
	{
		if (!readOnly && !IsConnFromDatanode())
			connections[i]->read_only = false;
		/*
		 * PGXC TODO - A connection should not be in DN_CONNECTION_STATE_QUERY
		 * state when we are about to send a BEGIN TRANSACTION command to the
		 * node. We should consider changing the following to an assert and fix
		 * any bugs reported
		 */
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);

#ifndef POLARX_TODO
		/* Send GXID and check for errors */
		if (pgxc_node_send_gxid(connections[i], gxid))
		{
			elog(WARNING, "pgxc_node_begin gxid is invalid.");
			return EOF;
		}
#endif /*POLARX_TODO*/
		/* Send timestamp and check for errors */

		if (txn_coordination != TXN_COORDINATION_NONE &&
			pgxc_node_send_timestamp(connections[i], start_ts))
		{
			return EOF;
		}

		if (IS_PGXC_DATANODE && GlobalTransactionIdIsValid(gxid))
		{
			need_tran_block = true;
		}
		else if (IS_PGXC_REMOTE_COORDINATOR)
		{
			need_tran_block = false;
		}

		if (need_tran_block && 'I' == connections[i]->transaction_status)
		{
			need_send_begin = true;
		}

		if (connections[i]->plpgsql_need_begin_txn && connections[i]->plpgsql_need_begin_sub_txn &&
			'I' == connections[i]->transaction_status)
		{
			need_send_begin = true;
			cmd = begin_both_cmd;
			connections[i]->plpgsql_need_begin_txn = false;
			connections[i]->plpgsql_need_begin_sub_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG,
					 "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_txn was true, and "
					 "conn->plpgsql_need_begin_sub_txn was true. in_plpgsql_exec_fun:%d",
					 cmd,
					 g_in_plpgsql_exec_fun);
			}
		}
		else if (connections[i]->plpgsql_need_begin_txn &&
				 'I' == connections[i]->transaction_status)
		{
			need_send_begin = true;
			connections[i]->plpgsql_need_begin_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG,
					 "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_txn was true, "
					 "g_in_plpgsql_exec_fun:%d, conn->plpgsql_need_begin_sub_txn:%d",
					 cmd,
					 g_in_plpgsql_exec_fun,
					 connections[i]->plpgsql_need_begin_sub_txn);
			}
		}
		else if (connections[i]->plpgsql_need_begin_sub_txn)
		{
			need_send_begin = true;
			cmd = begin_subtxn_cmd;
			connections[i]->plpgsql_need_begin_sub_txn = false;
			if (PlpgsqlDebugPrint)
			{
				elog(LOG,
					 "[PLPGSQL] pgxc_node_begin cmd:%s conn->plpgsql_need_begin_sub_txn was true, "
					 "g_in_plpgsql_exec_fun:%d, conn->plpgsql_need_begin_txn:%d",
					 cmd,
					 g_in_plpgsql_exec_fun,
					 connections[i]->plpgsql_need_begin_txn);
			}
			if ('T' != connections[i]->transaction_status)
			{
				elog(PANIC,
					 "[PLPGSQL] pgxc_node_begin need_begin_sub_txn wrong transaction_status");
			}
		}

		/* If exec savepoint command, we make sure begin should send(NB:can be sent only once)
		 * before send savepoint  */
		if ('I' == connections[i]->transaction_status && SavepointDefined())
		{
			need_send_begin = true;
		}

		if (enable_timestamp_debug_print)
		{
			elog(LOG,
				 "[PLPGSQL] pgxc_node_begin need_tran_block %d, connections[%d]->transaction_status %c "
				 "need_send_begin:%d",
				 need_tran_block,
				 i,
				 connections[i]->transaction_status,
				 need_send_begin);
		}

		/* Send BEGIN if not already in transaction */
		if (need_send_begin || force_2pc)
		{
			/* Send the BEGIN TRANSACTION command and check for errors */
			if (pgxc_node_send_query(connections[i], cmd))
			{
				return EOF;
			}

			elog(DEBUG5,
				 "pgxc_node_begin send %s to node %s, pid:%d",
				 cmd,
				 connections[i]->nodename,
				 connections[i]->backend_pid);
			new_connections[new_count++] = connections[i];
		}
	}

	/*
	 * If we did not send a BEGIN command to any node, we are done. Otherwise,
	 * we need to check for any errors and report them
	 */
	if (new_count == 0)
	{
		if (enable_timestamp_debug_print)
		{
			elog(LOG, "[pgxc_node_begin] new_count is 0.");
		}
		return 0;
	}

	InitResponseCombiner(&combiner, new_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	/* Receive responses */
	if (pgxc_node_receive_responses(new_count, new_connections, timeout, &combiner))
	{
		elog(WARNING, "pgxc_node_begin receive response fails.");
		return EOF;
	}
	/* Verify status */
	if (!ValidateAndCloseCombiner(&combiner))
	{
		elog(LOG, "pgxc_node_begin validating response fails.");
		return EOF;
	}
	/* Send virtualXID to the remote nodes using SET command */

	/* after transactions are started send down local set commands */
	char *init_str = PGXCNodeGetTransactionParamStr();

	if (init_str)
	{
		for (i = 0; i < new_count; i++)
		{
			pgxc_node_set_query(new_connections[i], init_str);
		}
	}

	/* No problem, let's get going */
	return 0;
}

/*
 * Prepare nodes which ran write operations during the transaction.
 * Read only remote transactions are committed and connections are released
 * back to the pool.
 * Function returns the list of nodes where transaction is prepared, including
 * local node, if requested, in format expected by the GTM server.
 * If something went wrong the function tries to abort prepared transactions on
 * the nodes where it succeeded and throws error. A warning is emitted if abort
 * prepared fails.
 * After completion remote connection handles are released.
 */
static char *
pgxc_node_remote_prepare(const char *prepareGID, bool localNode, bool implicit)
{
	bool isOK = true;
	int i;
	int conn_count = 0;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	PGXCNodeAllHandles *handles = get_current_handles();
	StringInfoData nodestr;
	GlobalTimestamp global_prepare_ts = InvalidGlobalTimestamp;
	char *commit_cmd = "COMMIT TRANSACTION";
	char *prepare_cmd = (char *)palloc(64 + strlen(prepareGID));
	char *abort_cmd = NULL;
	/* conn_state_index record index in g_twophase_state.conn_state or
	 * g_twophase_state.datanode_state */
	int conn_state_index = 0;
	int twophase_index = 0;
	StringInfoData partnodes;
#ifdef POLARDB_X
	int last_step_ret = 0;
#endif

	connections = (PGXCNodeHandle **)palloc(
		sizeof(PGXCNodeHandle *) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER));
	if (connections == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory for connections")));
	}

	initStringInfo(&nodestr);
	if (localNode)
		appendStringInfoString(&nodestr, PGXCNodeName);

	sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", prepareGID);

	if (!implicit)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("explicit prepare transaction is not supported")));
	}
	if (enable_distri_print)
	{
		elog(LOG,
			 "prepare remote transaction xid %d gid %s",
			 GetTopTransactionIdIfAny(),
			 prepareGID);
	}
	if (txn_coordination == TXN_COORDINATION_GTM)
	{
		global_prepare_ts = (LogicalTime)GetGlobalTimestampGTM();
#ifdef POLARDBX_TWO_PHASE_TESTS
		if (ERROR_GET_PREPARE_TIMESTAMP_FAIL == twophase_exception_case)
		{
			if (enable_twophase_recover_debug_print)
				elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, failed to get prepare timestamp from TSO.",
					 twophase_exception_case);
			elog(ERROR, "Fault injection. Failed to get prepare timestamp from TSO.");
		}
#endif

		if (!GlobalTimestampIsValid(global_prepare_ts))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to get global timestamp for PREPARED command")));
		}
		if (enable_distri_print)
		{
			elog(LOG,
				 "prepare phase get global prepare timestamp gid %s, time " INT64_FORMAT,
				 prepareGID,
				 global_prepare_ts);
		}
		SetGlobalPrepareTimestamp(global_prepare_ts);
	}

	ClearLocalTwoPhaseState();
	get_partnodes(handles, &partnodes);

	/*
	 *if conn->readonly == true, that is no update in this trans, do not record in g_twophase_state
	 */
	if ('\0' != partnodes.data[0])
	{
		/* strlen of prepareGID is checked in MarkAsPreparing, it satisfy strlen(gid) >= GIDSIZE  */
		strncpy(g_twophase_state.gid, prepareGID, strlen(prepareGID) + 1);
		g_twophase_state.state = TWO_PHASE_PREPARING;
		SetLocalTwoPhaseStateHandles(handles);
		strncpy(g_twophase_state.participants, partnodes.data, partnodes.len + 1);
		if (enable_twophase_recover_debug_print)
		{
			elog(DEBUG_2PC,
				 "pgxc_node_remote_prepare: set g_twophase_state.state to TWO_PHASE_PREPARING, "
				 "gid:%s, participants:%s",
				 g_twophase_state.gid,
				 g_twophase_state.participants);
		}
	}
	ereport(DEBUG1, (errmsg("2pc preparing, gid=%s, participants=%s",
							prepareGID, partnodes.data)));

	for (i = 0; i < handles->dn_conn_count + handles->co_conn_count; i++)
	{
		bool is_datanode = i < handles->dn_conn_count;
		int handle_index = is_datanode ? i : i - handles->dn_conn_count;
		int twophase_index = is_datanode ? g_twophase_state.datanode_index : g_twophase_state.coord_index;
		PGXCNodeHandle *conn = is_datanode ? handles->datanode_handles[handle_index] : handles->coord_handles[handle_index];
		ConnState conn_state = TWO_PHASE_HEALTHY;
		TwoPhaseTransState txn_state = g_twophase_state.state;

		/*
		 * If something went wrong already we have nothing to do here. The error
		 * will be reported at the end of the function, and we will rollback
		 * remotes as part of the error handling.
		 * Just skip to clean up section and check if we have already prepared
		 * somewhere, we should abort that prepared transaction.
		 */
		if (!isOK)
			goto prepare_err;

		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (conn->transaction_status == 'T')
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
				BufferConnection(conn);

			if (conn->read_only)
			{
				if (enable_timestamp_debug_print)
				{
					elog(LOG, "handles->dn_conn_count:%d, handles->co_conn_count:%d conn is readonly, remote backend_pid:%d, nodehost:%s, nodename:%s, global_prepare_ts:" INT64_FORMAT,
						 handles->dn_conn_count, handles->co_conn_count, conn->backend_pid, conn->nodehost, conn->nodename, global_prepare_ts);
				}
				int res = RemoteTransactionPrepareReadonly(
					conn, prepareGID, global_prepare_ts, commit_cmd, implicit);
				if (!res)
				{
					/* Read responses from these */
					connections[conn_count++] = conn;
				}
			}
			else
			{
				/*
				 *only record connections that satisfy !conn->readonly
				 */
				if (enable_timestamp_debug_print)
				{
					elog(LOG, "handles->dn_conn_count:%d, handles->co_conn_count:%d, conn is NOT readonly, remote backend_pid:%d, nodehost:%s, nodename:%s, global_prepare_ts:" INT64_FORMAT,
						 handles->dn_conn_count, handles->co_conn_count, conn->backend_pid, conn->nodehost, conn->nodename, global_prepare_ts);
				}
#ifdef POLARDB_X
				last_step_ret = 0;
				// send prepare timestamp when using tso
				if (txn_coordination == TXN_COORDINATION_GTM && implicit)
				{
#ifdef POLARDBX_TWO_PHASE_TESTS
					// condition: i != 0; let first connection be ok, so all test case can be part failure.
					if ((ERROR_SEND_PREPARE_TIMESTAMP_FAIL == twophase_exception_case) && (i != 0))
					{
						last_step_ret = -1;
						conn_state = TWO_PHASE_SEND_TIMESTAMP_ERROR;
						if (enable_twophase_recover_debug_print)
							elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, set conn_state to %d. simulate case: failed to send prepare tiemstamp. prepare_cmd:%s, global_prepare_ts:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
								 twophase_exception_case, conn_state, prepare_cmd, global_prepare_ts, conn->nodename, conn->backend_pid);
					}
					else
#endif
					{
						last_step_ret = pgxc_node_send_prepare_timestamp(conn, global_prepare_ts);
					}
				}

				if (0 == last_step_ret) // last step is success
				{
#ifdef POLARDBX_TWO_PHASE_TESTS
					if ((ERROR_SEND_PARTICIPATE_NODE_FAIL == twophase_exception_case) && (i != 0))
					{
						last_step_ret = -1;
						conn_state = TWO_PHASE_SEND_PARTICIPANTS_ERROR;
						if (enable_twophase_recover_debug_print)
							elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, set conn_state to %d, simulate case: failed to send participate node:%s. prepare_cmd:%s, global_prepare_ts:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
								 twophase_exception_case, conn_state, partnodes.data, prepare_cmd, global_prepare_ts, conn->nodename, conn->backend_pid);
					}
					else
#endif
					{
						last_step_ret = pgxc_node_send_partnodes(conn, partnodes.data);
					}
				}

				if (0 == last_step_ret) // last step is success
				{
#ifdef POLARDBX_TWO_PHASE_TESTS
					if ((ERROR_SEND_PREPARE_CMD_FAIL == twophase_exception_case) && (i != 0))
					{
						last_step_ret = -1;
						conn_state = TWO_PHASE_SEND_QUERY_ERROR;
						if (enable_twophase_recover_debug_print)
							elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, set conn_state to %d, simulate case: failed to send prepare cmd:%s. global_prepare_ts:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
								 twophase_exception_case, conn_state, prepare_cmd, global_prepare_ts, conn->nodename, conn->backend_pid);
					}
					// srcatch on prepare cmd to simulate case: get error response from remote backend. ERROR_RECV_PREPARE_CMD_RESPONSE_FAIL
					else if ((ERROR_RECV_PREPARE_CMD_RESPONSE_FAIL == twophase_exception_case) && (i != 0))
					{
						last_step_ret = pgxc_node_send_query(conn, error_request_sql);
						if (enable_twophase_recover_debug_print)
							elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, simulate case: recv error response from remote node. error_request_sql:%s. global_prepare_ts:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
								 twophase_exception_case, error_request_sql, global_prepare_ts, conn->nodename, conn->backend_pid);
					}
					else
#endif
					{
						last_step_ret = pgxc_node_send_query(conn, prepare_cmd);
					}
				}
#else
				if (txn_coordination == TXN_COORDINATION_GTM && implicit &&
					pgxc_node_send_prepare_timestamp(conn, global_prepare_ts))
					conn_state = TWO_PHASE_SEND_TIMESTAMP_ERROR;
				else if (pgxc_node_send_query(conn, prepare_cmd))
					conn_state = TWO_PHASE_SEND_QUERY_ERROR;
#endif

				if (conn_state != TWO_PHASE_HEALTHY)
				{
#ifdef POLARDB_X
					if (enable_twophase_recover_debug_print)
						elog(DEBUG_2PC, "conn_state:%d not healthy. i:%d, conn->nodename:%s, conn->backend_pid:%d", conn_state, i, conn->nodename, conn->backend_pid);
#endif
					isOK = false;
					txn_state = TWO_PHASE_PREPARE_ERROR;
				}
				else
				{
					char *nodename = get_pgxc_nodename(conn->nodeoid);
					if (nodestr.len > 0)
						appendStringInfoChar(&nodestr, ',');
					appendStringInfoString(&nodestr, nodename);

					conn->ck_resp_rollback = true;
					connections[conn_count++] = conn;
					TwoPhaseStateAppendConnection(is_datanode, twophase_index);
				}
				TwoPhaseStateAppendNode(is_datanode, twophase_index, handle_index, conn_state, txn_state);
			}
		}
		else if (conn->transaction_status == 'E')
		{
			/*
			 * Probably can not happen, if there was a error the engine would
			 * abort anyway, even in case of explicit PREPARE.
			 * Anyway, just in case...
			 */
#ifdef POLARDB_X
			if (enable_twophase_recover_debug_print)
				elog(DEBUG_2PC, "conn->transaction_status:'E'. i:%d, conn->nodename:%s, conn->backend_pid:%d", i, conn->nodename, conn->backend_pid);
#endif
			isOK = false;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("remote node %u is in error state", conn->nodeoid)));
		}
	}

	SetSendCommandId(false);

	if (!isOK)
		goto prepare_err;

	/* exit if nothing has been prepared */
	if (conn_count > 0)
	{
		int result;
		/*
		 * Receive and check for any errors. In case of errors, we don't bail out
		 * just yet. We first go through the list of connections and look for
		 * errors on each connection. This is important to ensure that we run
		 * an appropriate ROLLBACK command later on (prepared transactions must be
		 * rolled back with ROLLBACK PREPARED commands).
		 *
		 * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
		 * individual connections. The transaction_status field doesn't get set
		 * every time there is an error on the connection. The combiner mechanism is
		 * good for parallel proessing, but I think we should have a leak-proof
		 * mechanism to track connection status
		 */
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		g_twophase_state.response_operation = REMOTE_PREPARE;
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
#ifdef POLARDBX_TWO_PHASE_TESTS
		if (ERROR_RECV_PREPARE_CMD_RESPONSE_PENDING == twophase_exception_case)
		{
			if (NODE_EXCEPTION_NORMAL == twophase_exception_node_exception)
			{
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, sleep sometime after send prepare_cmd:%s, global_prepare_ts:" UINT64_FORMAT,
						 twophase_exception_case, prepare_cmd, global_prepare_ts);
				// sleep for several mins to simulate recv pending.
				pg_usleep(twophase_exception_pending_time);
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. sleep done.");
			}
			else if (NODE_EXCEPTION_CRASH == twophase_exception_node_exception)
			{
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, simulate node crash when send prepare command and waiting for remote node response.",
						 twophase_exception_case);
				// STOP
				elog(STOP, "exit backend when simulate node crash when send prepare command and waiting for remote node response.");
			}
			else
			{
				elog(ERROR, "twophase_exception_case:%d, unsupported twophase_exception_node_exception:%d", twophase_exception_case, twophase_exception_node_exception);
			}
		}
#endif
		if (result || !validate_combiner(&combiner))
			goto prepare_err;
		else
			CloseCombiner(&combiner);

		if (txn_coordination == TXN_COORDINATION_HLC)
		{
			LogicalTime coordinatedTs = 0;
			for (i = 0; i < conn_count; i++)
			{
				if (enable_timestamp_debug_print)
				{
					elog(LOG, "conn_count:%d, i:%d get receivedTimestamp:" INT64_FORMAT " from conn, nodename:%s, host:%s, backend_pid:%d, conn read only:%d, coordinatedTs:" INT64_FORMAT,
						 conn_count, i, connections[i]->receivedTimestamp, connections[i]->nodename, connections[i]->nodehost, connections[i]->backend_pid, connections[i]->read_only, coordinatedTs);
				}
				if (!connections[i]->read_only)
				{
					/* bugfix: if read only transaction, no necessary to update coordinatedTs, since datanode will not return valid timestamp. */
					coordinatedTs = Max(coordinatedTs, connections[i]->receivedTimestamp);
					if (coordinatedTs == 0)
					{
						elog(ERROR, "invalid prepare_ts from datanode");
					}
				}
			}
			if (enable_timestamp_debug_print)
			{
				elog(LOG, "At last, get coordinatedTs:" INT64_FORMAT, coordinatedTs);
			}
			if (coordinatedTs != 0)
			{
				/* bugfix: update coorinator committs only when coordinatedTs is valid. */
				TxnSetCoordinatedCommitTs(coordinatedTs);
			}
		}

		/* Before exit clean the flag, to avoid unnecessary checks */
		for (i = 0; i < conn_count; i++)
			connections[i]->ck_resp_rollback = false;

		clear_handles();
		pfree_pgxc_all_handles(handles);
	}

	pfree(prepare_cmd);
	if (partnodes.maxlen)
	{
		resetStringInfo(&partnodes);
		pfree(partnodes.data);
	}
	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}
	g_twophase_state.state = TWO_PHASE_PREPARED;
	ereport(DEBUG1, (errmsg("2pc prepared, gid=%s, nodestring=%s", prepareGID, nodestr.data)));
	return nodestr.data;

prepare_err:
	if (partnodes.maxlen)
	{
		resetStringInfo(&partnodes);
		pfree(partnodes.data);
	}

	/* read ReadyForQuery from connections which sent commit/commit prepared */
	if (!isOK)
	{
		if (conn_count > 0)
		{
			ResponseCombiner combiner3;
			InitResponseCombiner(&combiner3, conn_count, COMBINE_TYPE_NONE);
			g_twophase_state.response_operation = REMOTE_PREPARE_ERROR;
			/* Receive responses */
			pgxc_node_receive_responses(conn_count, connections, NULL, &combiner3);
			CloseCombiner(&combiner3);
		}
	}

	abort_cmd = (char *)palloc(64 + strlen(prepareGID));
	sprintf(abort_cmd, "ROLLBACK PREPARED '%s'", prepareGID);
	conn_count = 0;
	g_twophase_state.connections_num = 0;
	conn_state_index = 0;
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/*
		 * PREPARE succeeded on that node, roll it back there
		 */
		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Error while PREPARING transaction %s on "
								"node %s. Administrative action may be required "
								"to abort this transaction on the node",
								prepareGID,
								conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
			/* update datanode_state = TWO_PHASE_ABORTTING in prepare_err */
			while (g_twophase_state.datanode_index >= conn_state_index &&
				   g_twophase_state.datanode_state[conn_state_index].handle_idx != i)
			{
				conn_state_index++;
			}
			if (g_twophase_state.datanode_index < conn_state_index)
			{
				elog(ERROR,
					 "in pgxc_node_remote_prepare can not find twophase_state for node %s",
					 conn->nodename);
			}
			g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORTTING;
#ifdef POLARDBX_TWO_PHASE_TESTS
			if ((NODE_EXCEPTION_CRASH == twophase_exception_node_exception)) // fail in first node, which already do sth.
			{
				elog(LOG, "Fault injection. simulate node crash when going to send rollback cmd:%s.", abort_cmd);
				// STOP
				elog(STOP, "exit backend when simulate node crash when send rollback prepared cmd.");
			}
			else
			{
#endif
				/* Send down abort prepared command */
				if (pgxc_node_send_query(conn, abort_cmd))
				{
					g_twophase_state.datanode_state[conn_state_index].conn_state =
						TWO_PHASE_SEND_QUERY_ERROR;
					g_twophase_state.datanode_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
					/*
					 * Prepared transaction is left on the node, but we can not
					 * do anything with that except warn the user.
					 */
					ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send ABORT PREPARED command to "
									"the node %u",
									conn->nodeoid)));
				}
				else
				{
					/* Read responses from these */
					connections[conn_count++] = conn;
					g_twophase_state.datanode_state[conn_state_index].conn_state = TWO_PHASE_HEALTHY;
					TwoPhaseStateAppendConnection(true, twophase_index);
					conn_state_index++;
				}
#ifdef POLARDBX_TWO_PHASE_TESTS
			}
#endif
		}
	}
	conn_state_index = 0;
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Error while PREPARING transaction %s on "
								"node %s. Administrative action may be required "
								"to abort this transaction on the node",
								prepareGID,
								conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
			while (g_twophase_state.coord_index >= conn_state_index &&
				   g_twophase_state.coord_state[conn_state_index].handle_idx != i)
			{
				conn_state_index++;
			}
			if (g_twophase_state.coord_index < conn_state_index)
			{
				elog(ERROR,
					 "in pgxc_node_remote_prepare can not find twophase_state for node %s",
					 conn->nodename);
			}
			g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORTTING;
			/* Send down abort prepared command */
			if (pgxc_node_send_query(conn, abort_cmd))
			{
				g_twophase_state.coord_state[conn_state_index].conn_state =
					TWO_PHASE_SEND_QUERY_ERROR;
				g_twophase_state.coord_state[conn_state_index].state = TWO_PHASE_ABORT_ERROR;
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send ABORT PREPARED command to "
								"the node %u",
								conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
				g_twophase_state.coord_state[conn_state_index].conn_state = TWO_PHASE_HEALTHY;
				conn_state_index++;
				TwoPhaseStateAppendConnection(false, twophase_index);
			}
		}
	}
	if (conn_count > 0)
	{
		/* Just read out responses, throw error from the first combiner */
		ResponseCombiner combiner2;
		InitResponseCombiner(&combiner2, conn_count, COMBINE_TYPE_NONE);
		g_twophase_state.response_operation = REMOTE_PREPARE_ABORT;
		/* Receive responses */
		pgxc_node_receive_responses(conn_count, connections, NULL, &combiner2);
		CloseCombiner(&combiner2);
	}

	/*
	 * If the flag is set we are here because combiner carries error message
	 */
	if (isOK)
		pgxc_node_report_error(&combiner);
	else
		elog(ERROR, "failed to PREPARE transaction on one or more nodes");

	TxnCleanUpHandles(handles, true);

	pfree(abort_cmd);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	return NULL;
}

/*
 * Commit transactions on remote nodes.
 * If barrier lock is set wait while it is released.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_commit(TranscationType txn_type, bool need_release_handle)
{
	int result = 0;
	char *commitCmd = NULL;
	int i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int conn_count = 0;
	PGXCNodeAllHandles *handles = get_current_handles();

	switch (txn_type)
	{
	case TXN_TYPE_CommitTxn:
		commitCmd = "COMMIT TRANSACTION";
		break;
	case TXN_TYPE_CommitSubTxn:
		commitCmd = "COMMIT_SUBTXN";
		break;
	default:
		elog(PANIC, "pgxc_node_remote_commit invalid TranscationType:%d", txn_type);
		break;
	}

	/* palloc will FATAL when out of memory */
	connections = (PGXCNodeHandle **)palloc(
		sizeof(PGXCNodeHandle *) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER));

	SetSendCommandId(false);

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
#ifndef POLARX_TODO
	LWLockAcquire(BarrierLock, LW_SHARED);
#endif

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		if ((conn->transaction_status != 'I' && TXN_TYPE_CommitTxn == txn_type) ||
			(conn->transaction_status != 'I' && TXN_TYPE_CommitSubTxn == txn_type &&
			 NodeHasBeginSubTxn(conn->nodeoid)))
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				BufferConnection(conn);
			}

			if (pgxc_node_send_query(conn, commitCmd))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("pgxc_node_remote_commit failed to send COMMIT command to the node "
								"%s, pid:%d, for %s",
								conn->nodename,
								conn->backend_pid,
								strerror(errno))));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		// if (conn->transaction_status != 'I' && (!is_subtxn || (is_subtxn &&
		// NodeHasBeginSubTxn(conn->nodeoid))))
		if ((conn->transaction_status != 'I' && TXN_TYPE_CommitTxn == txn_type) ||
			(conn->transaction_status != 'I' && TXN_TYPE_CommitSubTxn == txn_type &&
			 NodeHasBeginSubTxn(conn->nodeoid)))
		{
			if (pgxc_node_send_query(conn, commitCmd))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("pgxc_node_remote_commit failed to send COMMIT command to the node "
								"%s, pid:%d, for %s",
								conn->nodename,
								conn->backend_pid,
								strerror(errno))));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	/*
	 * Release the BarrierLock.
	 */
#ifndef POLARX_TODO
	LWLockRelease(BarrierLock);
#endif

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);

		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result)
		{
			elog(LOG, "pgxc_node_remote_commit pgxc_node_receive_responses of COMMIT failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
			elog(LOG, "pgxc_node_remote_commit validate_combiner responese of COMMIT failed");
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				pgxc_node_report_error(&combiner);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifndef POLARX_TODO
	stat_transaction(conn_count);
#endif

	TxnCleanUpHandles(handles, need_release_handle);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}
}

void InitLocalTwoPhaseState(void)
{
	int participants_capacity;
	g_twophase_state.is_readonly = false;
	g_twophase_state.gid = (char *)MemoryContextAllocZero(TopMemoryContext, GIDSIZE);
	g_twophase_state.state = TWO_PHASE_INITIALTRANS;
	g_twophase_state.coord_index = g_twophase_state.datanode_index = 0;
	g_twophase_state.handles = NULL;
	g_twophase_state.connections_num = 0;
	g_twophase_state.response_operation = OTHER_OPERATIONS;

	g_twophase_state.coord_state = (ConnTransState *)MemoryContextAllocZero(
		TopMemoryContext, POLARX_MAX_COORDINATOR_NUMBER * sizeof(ConnTransState));
	g_twophase_state.datanode_state = (ConnTransState *)MemoryContextAllocZero(
		TopMemoryContext, POLARX_MAX_DATANODE_NUMBER * sizeof(ConnTransState));
	/* since participates conclude nodename and  ","*/
	participants_capacity =
		(NAMEDATALEN + 1) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER);
	g_twophase_state.participants =
		(char *)MemoryContextAllocZero(TopMemoryContext, participants_capacity);
	g_twophase_state.connections = (AllConnNodeInfo *)MemoryContextAllocZero(
		TopMemoryContext,
		(POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER) * sizeof(AllConnNodeInfo));
}

void SetLocalTwoPhaseStateHandles(PGXCNodeAllHandles *handles)
{
	g_twophase_state.handles = handles;
	g_twophase_state.connections_num = 0;
	g_twophase_state.coord_index = 0;
	g_twophase_state.datanode_index = 0;
}

void UpdateLocalTwoPhaseState(int result, PGXCNodeHandle *response_handle, int conn_index, char *errmsg)
{
	int index = 0;
	int twophase_index = 0;
	TwoPhaseTransState state = TWO_PHASE_INITIALTRANS;

	if (RESPONSE_READY != result && RESPONSE_ERROR != result)
	{
		return;
	}

	if (g_twophase_state.response_operation == OTHER_OPERATIONS || !IsTransactionState() ||
		g_twophase_state.state == TWO_PHASE_INITIALTRANS)
	{
		return;
	}
	Assert(NULL != g_twophase_state.handles);
	if (RESPONSE_ERROR == result)
	{
		switch (g_twophase_state.state)
		{
		case TWO_PHASE_PREPARING:
			/* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
			if (REMOTE_PREPARE == g_twophase_state.response_operation ||
				REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_PREPARE_ERROR;
			}
			else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_ABORT_ERROR;
			}
		case TWO_PHASE_PREPARED:
			break;

		case TWO_PHASE_COMMITTING:
			if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_COMMIT_ERROR;
			}
		case TWO_PHASE_COMMITTED:
			break;

		case TWO_PHASE_ABORTTING:
			if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
				REMOTE_ABORT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_ABORT_ERROR;
			}
		case TWO_PHASE_ABORTTED:
			break;
		default:
			Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
			return;
		}

		if (TWO_PHASE_INITIALTRANS != state)
		{
			/* update coord_state or datanode_state */
			twophase_index = g_twophase_state.connections[conn_index].conn_trans_state_index;
			if (PGXC_NODE_COORDINATOR == g_twophase_state.connections[conn_index].node_type)
			{
				g_twophase_state.coord_state[twophase_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.coord_state[twophase_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, "
						 "g_twophase_state.coord_state[%d] imply node: %s",
						 conn_index,
						 response_handle->nodename,
						 twophase_index,
						 g_twophase_state.handles->coord_handles[index]->nodename);
				}
			}
			else
			{
				g_twophase_state.datanode_state[twophase_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.datanode_state[twophase_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, "
						 "g_twophase_state.datanode_state[%d] imply node: %s",
						 conn_index,
						 response_handle->nodename,
						 twophase_index,
						 g_twophase_state.handles->datanode_handles[index]->nodename);
				}
			}
		}
		return;
	}
	else if (RESPONSE_READY == result && NULL == errmsg)
	{
		switch (g_twophase_state.state)
		{
		case TWO_PHASE_PREPARING:
			/* receive response in pgxc_node_remote_prepare or at the begining of prepare_err */
			if (REMOTE_PREPARE == g_twophase_state.response_operation ||
				REMOTE_PREPARE_ERROR == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_PREPARED;
			}
			else if (REMOTE_PREPARE_ABORT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_ABORTTED;
			}
		case TWO_PHASE_PREPARED:
			break;

		case TWO_PHASE_COMMITTING:
			if (REMOTE_FINISH_COMMIT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_COMMITTED;
			}
		case TWO_PHASE_COMMITTED:
			break;
		case TWO_PHASE_ABORTTING:
			if (REMOTE_FINISH_ABORT == g_twophase_state.response_operation ||
				REMOTE_ABORT == g_twophase_state.response_operation)
			{
				state = TWO_PHASE_ABORTTED;
			}
		case TWO_PHASE_ABORTTED:
			break;
		default:
			Assert((result < TWO_PHASE_INITIALTRANS) || (result > TWO_PHASE_ABORT_ERROR));
			return;
		}

		if (TWO_PHASE_INITIALTRANS != state)
		{
			twophase_index = g_twophase_state.connections[conn_index].conn_trans_state_index;
			if (PGXC_NODE_COORDINATOR == g_twophase_state.connections[conn_index].node_type)
			{
				g_twophase_state.coord_state[twophase_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.coord_state[twophase_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, "
						 "g_twophase_state.coord_state[%d] imply node: %s",
						 conn_index,
						 response_handle->nodename,
						 twophase_index,
						 g_twophase_state.handles->coord_handles[index]->nodename);
				}
			}
			else
			{
				g_twophase_state.datanode_state[twophase_index].state = state;
				if (enable_distri_print)
				{
					index = g_twophase_state.datanode_state[twophase_index].handle_idx;
					elog(LOG,
						 "In UpdateLocalTwoPhaseState connections[%d] imply node: %s, "
						 "g_twophase_state.datanode_state[%d] imply node: %s",
						 conn_index,
						 response_handle->nodename,
						 twophase_index,
						 g_twophase_state.handles->datanode_handles[index]->nodename);
				}
			}
		}
		return;
	}
}

void ClearLocalTwoPhaseState(void)
{
	if (enable_distri_print)
	{
		if (TWO_PHASE_PREPARED == g_twophase_state.state && IsXidImplicit(g_twophase_state.gid))
		{
			elog(LOG,
				 "clear g_twophase_state of transaction '%s' in state '%s'",
				 g_twophase_state.gid,
				 GetTransStateString(g_twophase_state.state));
		}
	}
	g_twophase_state.is_readonly = false;
	g_twophase_state.gid[0] = '\0';
	g_twophase_state.state = TWO_PHASE_INITIALTRANS;
	g_twophase_state.coord_index = 0;
	g_twophase_state.datanode_index = 0;
	g_twophase_state.handles = NULL;
	g_twophase_state.participants[0] = '\0';
	g_twophase_state.connections_num = 0;
	g_twophase_state.response_operation = OTHER_OPERATIONS;
}

static char *
GetTransStateString(TwoPhaseTransState state)
{
	switch (state)
	{
	case TWO_PHASE_INITIALTRANS:
		return "TWO_PHASE_INITIALTRANS";
	case TWO_PHASE_PREPARING:
		return "TWO_PHASE_PREPARING";
	case TWO_PHASE_PREPARED:
		return "TWO_PHASE_PREPARED";
	case TWO_PHASE_PREPARE_ERROR:
		return "TWO_PHASE_PREPARE_ERROR";
	case TWO_PHASE_COMMITTING:
		return "TWO_PHASE_COMMITTING";
	case TWO_PHASE_COMMITTED:
		return "TWO_PHASE_COMMITTED";
	case TWO_PHASE_COMMIT_ERROR:
		return "TWO_PHASE_COMMIT_ERROR";
	case TWO_PHASE_ABORTTING:
		return "TWO_PHASE_ABORTTING";
	case TWO_PHASE_ABORTTED:
		return "TWO_PHASE_ABORTTED";
	case TWO_PHASE_ABORT_ERROR:
		return "TWO_PHASE_ABORT_ERROR";
	case TWO_PHASE_UNKNOW_STATUS:
		return "TWO_PHASE_UNKNOW_STATUS";
	default:
		return NULL;
	}
	return NULL;
}

static char *
GetConnStateString(ConnState state)
{
	switch (state)
	{
	case TWO_PHASE_HEALTHY:
		return "TWO_PHASE_HEALTHY";
	case TWO_PHASE_SEND_GXID_ERROR:
		return "TWO_PHASE_SEND_GXID_ERROR";
	case TWO_PHASE_SEND_TIMESTAMP_ERROR:
		return "TWO_PHASE_SEND_TIMESTAMP_ERROR";
	case TWO_PHASE_SEND_STARTER_ERROR:
		return "TWO_PHASE_SEND_STARTER_ERROR";
	case TWO_PHASE_SEND_STARTXID_ERROR:
		return "TWO_PHASE_SEND_STARTXID_ERROR";
	case TWO_PHASE_SEND_PARTICIPANTS_ERROR:
		return "TWO_PHASE_SEND_PARTICIPANTS_ERROR";
	case TWO_PHASE_SEND_QUERY_ERROR:
		return "TWO_PHASE_SEND_QUERY_ERROR";
	default:
		return NULL;
	}
	return NULL;
}

static void
get_partnodes(PGXCNodeAllHandles *handles, StringInfo participants)
{
	int i;
	PGXCNodeHandle *conn;
	bool is_readonly = true;
	const char *gid = GetPrepareGID();

	initStringInfo(participants);

	/* start node participate the twophase transaction */
	if (IS_PGXC_LOCAL_COORDINATOR && (isXactWriteLocalNode() || !IsXidImplicit(gid)))
	{
		appendStringInfo(participants, "%s,", PGXCNodeName);
	}

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		conn = handles->datanode_handles[i];
		ereport(DEBUG1, (errmsg("get_partnodes: %s, transaction=%c, read_only=%d",
								conn->nodename,
								conn->transaction_status,
								conn->read_only)));
		if (conn->sock == NO_SOCKET)
		{
			continue;
		}
		else if (conn->transaction_status == 'T')
		{
			if (!conn->read_only)
			{
				is_readonly = false;
				appendStringInfo(participants, "%s,", conn->nodename);
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		conn = handles->coord_handles[i];
		ereport(DEBUG1, (errmsg("get_partnodes: %s, transaction=%c, read_only=%d",
								conn->nodename,
								conn->transaction_status,
								conn->read_only)));
		if (conn->sock == NO_SOCKET)
		{
			continue;
		}
		else if (conn->transaction_status == 'T')
		{
			if (!conn->read_only)
			{
				is_readonly = false;
				appendStringInfo(participants, "%s,", conn->nodename);
			}
		}
	}
	if (is_readonly && !IsXidImplicit(gid))
	{
		g_twophase_state.is_readonly = true;
	}
}

/*
 * Rollback transactions on remote nodes.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_abort(TranscationType txn_type, bool need_release_handle)
{
#define ROLLBACK_PREPARED_CMD_LEN 256
	bool force_release_handle = false;
	int ret = -1;
	int result = 0;
	char *rollbackCmd = NULL;
	int i;
	ResponseCombiner combiner;
	PGXCNodeHandle **connections = NULL;
	int conn_count = 0;

	PGXCNodeHandle **sync_connections = NULL;
	int sync_conn_count = 0;
	PGXCNodeAllHandles *handles = NULL;
	bool rollback_implict_txn = false;

	handles = get_current_handles();
	if (handles->co_conn_count + handles->dn_conn_count == 0)
	{
		return;
	}

	switch (txn_type)
	{
	case TXN_TYPE_RollbackTxn:
		if ('\0' != g_twophase_state.gid[0]) // NULL != GetPrepareGID())
		{
			rollbackCmd = palloc0(ROLLBACK_PREPARED_CMD_LEN);
			snprintf(rollbackCmd,
					 ROLLBACK_PREPARED_CMD_LEN,
					 "rollback prepared '%s'",
					 g_twophase_state.gid); // GetPrepareGID());
			rollback_implict_txn = true;
		}
		else
		{
			rollbackCmd = "ROLLBACK TRANSACTION";
		}
		break;
	case TXN_TYPE_RollbackSubTxn:
		rollbackCmd = "ROLLBACK_SUBTXN";
		break;
	case TXN_TYPE_CleanConnection:
		return;
	default:
		elog(PANIC, "pgxc_node_remote_abort invalid TranscationType:%d", txn_type);
		break;
	}

	/* palloc will FATAL when out of memory .*/
	connections = (PGXCNodeHandle **)palloc(
		sizeof(PGXCNodeHandle *) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER));
	sync_connections = (PGXCNodeHandle **)palloc(
		sizeof(PGXCNodeHandle *) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER));

	SetSendCommandId(false);

	elog(DEBUG5,
		 "pgxc_node_remote_abort - dn_conn_count %d, co_conn_count %d",
		 handles->dn_conn_count,
		 handles->co_conn_count);

	/* Send Sync if needed. */
	for (i = 0; i < handles->dn_conn_count + handles->co_conn_count; i++)
	{
		bool is_datanode = i < handles->dn_conn_count;
		int handle_index = is_datanode ? i : i - handles->dn_conn_count;
		PGXCNodeHandle *conn = is_datanode ? handles->datanode_handles[handle_index] : handles->coord_handles[handle_index];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
		{
			continue;
		}

		if (conn->transaction_status != 'I')
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				BufferConnection(conn);
			}

			/*
			 * If the remote session was running extended query protocol when
			 * it failed, it will expect a SYNC message before it accepts any
			 * other command
			 */
			if (conn->needSync)
			{
				ret = pgxc_node_send_sync(conn);
				if (ret)
				{
					add_error_message(conn, "Failed to send SYNC command");
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send SYNC command nodename:%s, pid:%d",
									conn->nodename,
									conn->backend_pid)));
				}
				else
				{
					/* Read responses from these */
					sync_connections[sync_conn_count++] = conn;
					result = EOF;
				}
			}
		}
	}

	if (sync_conn_count)
	{
		InitResponseCombiner(&combiner, sync_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(sync_conn_count, sync_connections, NULL, &combiner);
		if (result)
		{
			elog(LOG, "pgxc_node_remote_abort pgxc_node_receive_responses of SYNC failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
			elog(LOG, "pgxc_node_remote_abort validate_combiner responese of SYNC failed");
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send SYNC to on one or more nodes errmsg:%s",
								combiner.errorMessage)));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send SYNC to on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifdef _PG_REGRESS_
	{
		int ii = 0;
		for (ii = 0; ii < sync_conn_count; ii++)
		{
			if (pgxc_node_is_data_enqueued(sync_connections[ii]))
			{
				elog(PANIC,
					 "pgxc_node_remote_abort data left over in fd:%d, remote backendpid:%d",
					 sync_connections[ii]->sock,
					 sync_connections[ii]->backend_pid);
			}
		}
	}
#endif

	if (TWO_PHASE_ABORTTING == g_twophase_state.state && rollback_implict_txn)
	{
		SetLocalTwoPhaseStateHandles(handles);
	}

	for (i = 0; i < handles->dn_conn_count + handles->co_conn_count; i++)
	{
		bool is_datanode = i < handles->dn_conn_count;
		int handle_index = is_datanode ? i : i - handles->dn_conn_count;
		PGXCNodeHandle *conn = is_datanode ? handles->datanode_handles[handle_index] : handles->coord_handles[handle_index];
		int twophase_index = is_datanode ? g_twophase_state.datanode_index : g_twophase_state.coord_index;
		ConnState conn_state = TWO_PHASE_HEALTHY;
		TwoPhaseTransState txn_state = g_twophase_state.state = TWO_PHASE_ABORTTING;

		if (conn->sock == NO_SOCKET)
			continue;

		if ((conn->transaction_status != 'I' && TXN_TYPE_RollbackTxn == txn_type) ||
			(rollback_implict_txn && conn->ck_resp_rollback && TXN_TYPE_RollbackTxn == txn_type) ||
			(conn->transaction_status != 'I' && TXN_TYPE_RollbackSubTxn == txn_type &&
			 NodeHasBeginSubTxn(conn->nodeoid)))
		{

			if (pgxc_node_send_rollback(conn, rollbackCmd))
			{
				/* rollback failed */
				txn_state = TWO_PHASE_ABORT_ERROR;
				conn_state = TWO_PHASE_SEND_QUERY_ERROR;
				result = EOF;
				add_error_message(conn, "failed to send ROLLBACK TRANSACTION command");
			}
			else
			{
				/* rollback succeed */
				connections[conn_count++] = conn;
				conn->ck_resp_rollback = false;
			}

			if (rollback_implict_txn)
			{
				TwoPhaseStateAppendConnection(is_datanode, twophase_index);
				TwoPhaseStateAppendNode(is_datanode, twophase_index, handle_index, conn_state, txn_state);
			}
		}
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		if (rollback_implict_txn)
		{
			g_twophase_state.response_operation = REMOTE_ABORT;
		}
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result)
		{
			elog(LOG, "pgxc_node_remote_abort pgxc_node_receive_responses of ROLLBACK failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
#ifdef _PG_REGRESS_
			elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#else
			elog(LOG, "pgxc_node_remote_abort validate_combiner responese of ROLLBACK failed");
#endif
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send SYNC to on one or more nodes errmsg:%s",
								combiner.errorMessage)));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send SYNC to on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

#ifndef POLARX_TODO
	stat_transaction(conn_count);
#endif

	force_release_handle = validate_handles();
	if (force_release_handle)
	{
		elog(LOG, "found bad remote node connections, force release handles now");
		DropAllTxnDatanodeStatement();
		release_handles(true);
	}

	TxnCleanUpHandles(handles, need_release_handle);

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	if (sync_connections)
	{
		pfree(sync_connections);
		sync_connections = NULL;
	}
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 *
 *   SPECIAL WARNGING:
 *   ONLY LOG LEVEL ELOG CALL allowed here, else will cause coredump or resource leak in some rare
 * condition.
 */

static bool
AbortRunningQuery(TranscationType txn_type, bool need_release_handle)
{
	/*
	 * We are about to abort current transaction, and there could be an
	 * unexpected error leaving the node connection in some state requiring
	 * clean up, like COPY or pending query results.
	 * If we are running copy we should send down CopyFail message and read
	 * all possible incoming messages, there could be copy rows (if running
	 * COPY TO) ErrorResponse, ReadyForQuery.
	 * If there are pending results (connection state is DN_CONNECTION_STATE_QUERY)
	 * we just need to read them in and discard, all necessary commands are
	 * already sent. The end of input could be CommandComplete or
	 * PortalSuspended, in either case subsequent ROLLBACK closes the portal.
	 */
	bool cancel_ret = false;
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle **clean_nodes = NULL;
	int node_count = 0;
	int cancel_dn_count = 0, cancel_co_count = 0;
	int *cancel_dn_list = NULL;
	int *cancel_co_list = NULL;
	int i;

	clean_nodes = (PGXCNodeHandle **)palloc(sizeof(PGXCNodeHandle *) * (NumCoords + NumDataNodes));
	cancel_dn_list = (int *)palloc(sizeof(int) * NumDataNodes);
	cancel_co_list = (int *)palloc(sizeof(int) * NumCoords);

	all_handles = get_current_handles();
	/*
	 * Find "dirty" coordinator connections.
	 * COPY is never running on a coordinator connections, we just check for
	 * pending data.
	 */
	for (i = 0; i < all_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = all_handles->coord_handles[i];
		if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
		{
			if ((handle->state != DN_CONNECTION_STATE_IDLE) || !node_ready_for_query(handle))
			{
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				clean_nodes[node_count++] = handle;
				cancel_co_list[cancel_co_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);

#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename,
								handle->backend_pid,
								handle->state)));
#endif
				if (handle->in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename,
										handle->backend_pid)));
					}

#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename,
									handle->backend_pid)));
#endif
				}
			}
			else
			{
				if (handle->needSync)
				{
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Invalid node:%s pid:%d needSync flag",
									handle->nodename,
									handle->backend_pid)));
				}

#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.",
								handle->nodename,
								handle->backend_pid,
								handle->state)));
#endif
			}
		}
	}

	/*
	 * The same for data nodes, but cancel COPY if it is running.
	 */
	for (i = 0; i < all_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = all_handles->datanode_handles[i];
		if (handle->sock != NO_SOCKET && handle->sock < FD_SETSIZE)
		{
			if (handle->state == DN_CONNECTION_STATE_COPY_IN ||
				handle->state == DN_CONNECTION_STATE_COPY_OUT || !node_ready_for_query(handle))
			{
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename,
								handle->backend_pid,
								handle->state)));
#endif
				if (handle->in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename,
										handle->backend_pid)));
					}
#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename,
									handle->backend_pid)));
#endif
				}

#ifndef POLARX_TODO
				DataNodeCopyEnd(handle, true);
#endif
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				/*
				 * if datanode report error, there is no need to send cancel to it,
				 * and would not wait this datanode reponse.
				 */
				if ('E' != handle->transaction_status)
				{
					clean_nodes[node_count++] = handle;
					cancel_dn_list[cancel_dn_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
				}
			}
			else if (handle->state != DN_CONNECTION_STATE_IDLE)
			{
				/*
				 * Forget previous combiner if any since input will be handled by
				 * different one.
				 */
				handle->combiner = NULL;
				clean_nodes[node_count++] = handle;
				cancel_dn_list[cancel_dn_count++] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d need clean.",
								handle->nodename,
								handle->backend_pid,
								handle->state)));
#endif

				if (handle->in_extended_query)
				{
					if (pgxc_node_send_sync(handle))
					{
						ereport(LOG,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Failed to sync msg to node:%s pid:%d when abort",
										handle->nodename,
										handle->backend_pid)));
					}
#ifdef _PG_REGRESS_
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Succeed to sync msg to node:%s pid:%d when abort",
									handle->nodename,
									handle->backend_pid)));
#endif
				}
			}
			else
			{
				if (handle->needSync)
				{
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Invalid node:%s pid:%d needSync flag",
									handle->nodename,
									handle->backend_pid)));
				}
#ifdef _PG_REGRESS_
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("PreAbort_Remote node:%s pid:%d status:%d no need clean.",
								handle->nodename,
								handle->backend_pid,
								handle->state)));
#endif
			}
		}
	}

	if (cancel_co_count || cancel_dn_count)
	{
		/*
		 * Cancel running queries on the datanodes and the coordinators.
		 */
		cancel_ret = PoolManagerCancelQuery(
			cancel_dn_count, cancel_dn_list, cancel_co_count, cancel_co_list, SIGNAL_SIGINT);
		if (!cancel_ret)
		{
			elog(LOG, "PreAbort_Remote cancel query failed");
		}
	}

#if PGXC_CANCEL_DELAY > 0
	pg_usleep(PGXC_CANCEL_DELAY * 1000);
#endif

	/*
	 * Now read and discard any data from the connections found "dirty"
	 */
	if (node_count > 0)
	{
		ResponseCombiner combiner;

		InitResponseCombiner(&combiner, node_count, COMBINE_TYPE_NONE);
		combiner.extended_query = clean_nodes[0]->in_extended_query;
		/*
		 * Make sure there are zeroes in unused fields
		 */
		memset(&combiner, 0, sizeof(ScanState));
		combiner.connections = clean_nodes;
		combiner.conn_count = node_count;
		combiner.request_type = REQUEST_TYPE_ERROR;
		combiner.is_abort = true;

		pgxc_connections_cleanup(&combiner);

		/* prevent pfree'ing local variable */
		combiner.connections = NULL;

		CloseCombiner(&combiner);
	}

	pgxc_abort_connections(all_handles);

#ifdef _PG_REGRESS_
	{
		int nbytes = 0;
		int ii = 0;
		for (ii = 0; ii < node_count; ii++)
		{
			nbytes = pgxc_node_is_data_enqueued(clean_nodes[ii]);
			if (nbytes)
			{
				elog(PANIC,
					 "PreAbort_Remote %d bytes data left over in fd:%d remote backendpid:%d "
					 "nodename:%s",
					 nbytes,
					 clean_nodes[ii]->sock,
					 clean_nodes[ii]->backend_pid,
					 clean_nodes[ii]->nodename);
			}
		}
	}
#endif

	if (clean_nodes)
	{
		pfree(clean_nodes);
		clean_nodes = NULL;
	}

	if (cancel_dn_list)
	{
		pfree(cancel_dn_list);
		cancel_dn_list = NULL;
	}

	if (cancel_co_list)
	{
		pfree(cancel_co_list);
		cancel_co_list = NULL;
	}

	return true;
}

/*
 * Returns true if 2PC is required for consistent commit: if there was write
 * activity on two or more nodes within current transaction.
 */
static bool
IsTwoPhaseCommitRequired(bool localWrite)
{
	PGXCNodeAllHandles *handles;
	bool found = localWrite;
	int i;

	/* Never run 2PC on Datanode-to-Datanode connection */
	if (IS_PGXC_DATANODE)
		return false;

	if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL)
	{
		elog(DEBUG1,
			 "Transaction accessed temporary objects - "
			 "2PC will not be used and that can lead to data inconsistencies "
			 "in case of failures");
		return false;
	}

	/*
	 * If no XID assigned, no need to run 2PC since neither coordinator nor any
	 * remote nodes did write operation
	 */
#ifndef POLARX_TODO
	if (!TransactionIdIsValid(GetTopTransactionIdIfAny()))
		return false;
#endif

	handles = get_current_handles();
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];
		if (conn->sock != NO_SOCKET && !conn->read_only && conn->transaction_status == 'T')
		{
			if (found)
			{
				pfree_pgxc_all_handles(handles);
				return true; /* second found */
			}
			else
			{
				found = true; /* first found */
			}
		}
	}
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];
		if (conn->sock != NO_SOCKET && !conn->read_only && conn->transaction_status == 'T')
		{
			if (found)
			{
				pfree_pgxc_all_handles(handles);
				return true; /* second found */
			}
			else
			{
				found = true; /* first found */
			}
		}
	}
	pfree_pgxc_all_handles(handles);
	if (force_2pc)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "IsTwoPhaseCommitRequired: force_2pc.");
		return true;
	}
	return false;
}

/*
 * Complete previously prepared transactions on remote nodes.
 * Release remote connection after completion.
 */
static bool
pgxc_node_remote_commit_prepared(char *prepareGID, bool commit, char *nodestring)
{
	char *finish_cmd;
	PGXCNodeHandle **connections = NULL;
	int conn_count = 0;
	ResponseCombiner combiner;
	PGXCNodeAllHandles *pgxc_handles;
	bool prepared_local = false;
	List *nodelist = NIL;
	List *coordlist = NIL;
	int i;
	GlobalTimestamp global_committs;
	/*
	 *any send error in twophase trans will set all_conn_healthy to false
	 *all transaction call pgxc_node_remote_commit_prepared is twophase trans
	 *only called by starter: set g_twophase_state just before send msg to
	 *remote nodes
	 */
	bool all_conn_healthy = true;

	connections = (PGXCNodeHandle **)palloc(
		sizeof(PGXCNodeHandle *) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER));
	if (connections == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory for connections")));
	}

	/*
	 * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
	 * remote nodes and also finish the transaction locally is required
	 */
	elog(DEBUG8, "pgxc_node_remote_commit_prepared nodestring %s gid %s", nodestring, prepareGID);
	InterceptBeforeCommitTimestamp();
	global_committs = TxnDecideCoordinatedCommitTs();
#ifdef POLARDBX_TWO_PHASE_TESTS
	if (ERROR_GET_COMMIT_TIMESTAMP_FAIL == twophase_exception_case)
	{
		if (NODE_EXCEPTION_NORMAL == twophase_exception_node_exception)
		{
			if (enable_twophase_recover_debug_print)
				elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, simulate case failed to get global_committs.", twophase_exception_case);
			elog(ERROR, "Fault injection. Failed to get commit timestamp from TSO.");
		}
		else if (NODE_EXCEPTION_CRASH == twophase_exception_node_exception)
		{
			elog(LOG, "Fault injection. twophase_exception_case:%d, simulate node crash when failed to get commit timestamp.",
				 twophase_exception_case);
			// STOP
			elog(STOP, "exit backend when simulate node crash when failed to get commit timestamp.");
		}
		else
		{
			elog(ERROR, "twophase_exception_case:%d, unsupported twophase_exception_node_exception:%d", twophase_exception_case, twophase_exception_node_exception);
		}
	}
#endif

	SetGlobalCommitTimestamp(global_committs); /* Save for local commit */
	InterceptAfterCommitTimestamp();

	if (nodestring == NULL || nodestring[0] == '\0')
	{
		ereport(ERROR, (errmsg("2pc participants nodestring is empty")));
	}
#ifdef POLARDB_X
	/* should not scratch on nodestring, since we will use it later. */
	char *copyNodeString = strdup(nodestring);
	ResolveNodeString(copyNodeString, &nodelist, &coordlist, &prepared_local);
	free(copyNodeString);
#else
	ResolveNodeString(nodestring, &nodelist, &coordlist, &prepared_local);
#endif

	if (nodelist == NIL && coordlist == NIL)
		return prepared_local;

	pgxc_handles = get_handles(nodelist, coordlist, false, true);
	SetLocalTwoPhaseStateHandles(pgxc_handles);

	finish_cmd = (char *)palloc(128 + strlen(prepareGID));

#ifdef POLARDB_X
	if (commit)
		sprintf(finish_cmd, "/*internal*/ COMMIT PREPARED '%s'", prepareGID);
	else
		sprintf(finish_cmd, "/*internal*/ ROLLBACK PREPARED '%s'", prepareGID);
#else
	if (commit)
		sprintf(finish_cmd, "COMMIT PREPARED '%s'", prepareGID);
	else
		sprintf(finish_cmd, "ROLLBACK PREPARED '%s'", prepareGID);
#endif

	if (enable_distri_debug)
	{
		int node_count = pgxc_handles->dn_conn_count + pgxc_handles->co_conn_count;

		if (prepared_local)
		{
			node_count++;
		}
		is_distri_report = true;
	}

	g_twophase_state.state = commit ? TWO_PHASE_COMMITTING : TWO_PHASE_ABORTTING;

	for (i = 0; i < pgxc_handles->dn_conn_count + pgxc_handles->co_conn_count; i++)
	{
		bool is_datanode = i < pgxc_handles->dn_conn_count;
		int handle_index = is_datanode ? i : i - pgxc_handles->dn_conn_count;
		int twophase_index = is_datanode ? g_twophase_state.datanode_index : g_twophase_state.coord_index;
		PGXCNodeHandle *conn = is_datanode ? pgxc_handles->datanode_handles[handle_index] : pgxc_handles->coord_handles[handle_index];
		ConnState conn_state = TWO_PHASE_HEALTHY;
		TwoPhaseTransState txn_state = g_twophase_state.state;

#ifdef POLARDBX_TWO_PHASE_TESTS
		if ((ERROR_SEND_COMMIT_PREPARED_FAIL == twophase_exception_case) && (i != 0))
		{
			if (NODE_EXCEPTION_NORMAL == twophase_exception_node_exception)
			{
				conn_state = TWO_PHASE_SEND_QUERY_ERROR;
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, set conn_state to %d. simulate case: failed to send commit prepared command. command:%s, commit timestamp:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
						 twophase_exception_case, conn_state, finish_cmd, global_committs, conn->nodename, conn->backend_pid);
			}
			else if (NODE_EXCEPTION_CRASH == twophase_exception_node_exception)
			{
				elog(LOG, "Fault injection. twophase_exception_case:%d, simulate node crash when sending commit prepared command.",
					 twophase_exception_case);
				// STOP
				elog(STOP, "exit backend when simulate node crash when sending commit prepared command.");
			}
			else
			{
				elog(ERROR, "twophase_exception_case:%d, unsupported twophase_exception_node_exception:%d", twophase_exception_case, twophase_exception_node_exception);
			}
		}
		else if ((ERROR_RECV_COMMIT_PREPARED_RESPONSE_FAIL == twophase_exception_case) && (i != 0))
		{
			pgxc_node_send_query(conn, error_request_sql);
			if (enable_twophase_recover_debug_print)
				elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, simulate case: recv error when commit prepare command from remote node. error_request_sql:%s. commit timestamp:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
					 twophase_exception_case, error_request_sql, global_committs, conn->nodename, conn->backend_pid);
		}
		else
		{
			if (pgxc_node_send_gxid(conn, prepareGID) == 0 &&
				pgxc_node_send_global_timestamp(conn, global_committs) == 0 &&
				pgxc_node_send_query(conn, finish_cmd))
			{
				conn_state = TWO_PHASE_SEND_QUERY_ERROR;
				elog(LOG, "Failed to send commit prepared command. command:%s, commit timestamp:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
					 finish_cmd, global_committs, conn->nodename, conn->backend_pid);
			}
			else
			{
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Send finish_cmd:%s, global_committs:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
						 finish_cmd, global_committs, conn->nodename, conn->backend_pid);
			}
		}
#elif defined POLARDB_X
		if (pgxc_node_send_gxid(conn, prepareGID) == 0 &&
			pgxc_node_send_global_timestamp(conn, global_committs) == 0 &&
			pgxc_node_send_query(conn, finish_cmd))
		{
			conn_state = TWO_PHASE_SEND_QUERY_ERROR;
			elog(LOG, "Failed to send commit prepared command. command:%s, commit timestamp:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
				 finish_cmd, global_committs, conn->nodename, conn->backend_pid);
		}
		else
		{
			elog(DEBUG_2PC, "Send finish_cmd:%s, global_committs:" UINT64_FORMAT " to node:%s, remote backendpid:%d. ",
				 finish_cmd, global_committs, conn->nodename, conn->backend_pid);
		}
#else
		if (txn_coordination != TXN_COORDINATION_NONE &&
			pgxc_node_send_global_timestamp(conn, global_committs))
			conn_state = TWO_PHASE_SEND_TIMESTAMP_ERROR;
		else if (pgxc_node_send_query(conn, finish_cmd))
			conn_state = TWO_PHASE_SEND_QUERY_ERROR;
#endif

		if (conn_state != TWO_PHASE_HEALTHY)
		{
			all_conn_healthy = false;
			txn_state = commit ? TWO_PHASE_COMMIT_ERROR : TWO_PHASE_ABORT_ERROR;
		}
		else
		{
			connections[conn_count++] = conn;
			TwoPhaseStateAppendConnection(true, twophase_index);
		}
		TwoPhaseStateAppendNode(is_datanode, twophase_index, handle_index, conn_state, txn_state);
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		g_twophase_state.response_operation =
			(commit == true) ? REMOTE_FINISH_COMMIT : REMOTE_FINISH_ABORT;
#ifdef POLARDBX_TWO_PHASE_TESTS
		if (ERROR_RECV_COMMIT_PREPARED_RESPONSE_PENDING == twophase_exception_case)
		{
			if (NODE_EXCEPTION_NORMAL == twophase_exception_node_exception)
			{
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, sleep sometime after send finish_cmd:%s. nodestring:%s",
						 twophase_exception_case, finish_cmd, nodestring);
				// sleep several mins to simulate recv commit prepared cmd pending.
				pg_usleep(twophase_exception_pending_time);
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. sleep done.");
			}
			else if (NODE_EXCEPTION_CRASH == twophase_exception_node_exception)
			{
				if (enable_twophase_recover_debug_print)
					elog(DEBUG_2PC, "Fault injection. twophase_exception_case:%d, simulate node crash after send commit prepared :%s and waiting for remote node response.",
						 twophase_exception_case, finish_cmd);
				// STOP
				elog(STOP, "exit backend when simulate node crash when send commit prepared command:%s and waiting for remote node response.", finish_cmd);
			}
			else
			{
				elog(ERROR, "twophase_exception_case:%d, unsupported twophase_exception_node_exception:%d", twophase_exception_case, twophase_exception_node_exception);
			}
		}
#endif
		/* Receive responses */
		if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
			!validate_combiner(&combiner))
		{
			if (combiner.errorMessage)
				pgxc_node_report_error(&combiner);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or "
								"more nodes")));
#ifdef POLARDBX_TWO_PHASE_TESTS
			// should not reach here? since report error + commit prepared fail = backend exit.
			Assert(false);
#endif
		}
		else
			CloseCombiner(&combiner);
	}

	if (all_conn_healthy == false)
	{
		elog(ERROR, "Failed to send %s '%s' to one or more nodes", finish_cmd, prepareGID);
	}

#ifdef POLARDB_X
	if (XactLocalNodePrepared)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "Local node participated. So keep connections for now.");
		clear_handles();
		pfree_pgxc_all_handles(pgxc_handles);
	}
	else
	{
		FinishGlobalXacts(prepareGID, nodestring);
		TxnCleanUpHandles(pgxc_handles, true);
	}
#else
	TxnCleanUpHandles(pgxc_handles, true);
#endif

	pfree(finish_cmd);

	g_twophase_state.state = (commit == true) ? TWO_PHASE_COMMITTED : TWO_PHASE_ABORTTED;
	if (!prepared_local)
	{
		ClearLocalTwoPhaseState();
	}

	if (connections)
	{
		pfree(connections);
		connections = NULL;
	}

	return prepared_local;
}

/*
 * Execute DISCARD ALL command on all allocated nodes to remove all session
 * specific stuff before releasing them to pool for reuse by other sessions.
 */
static void
pgxc_node_remote_cleanup_all(void)
{
	PGXCNodeAllHandles *handles = get_current_handles();
	PGXCNodeHandle *new_connections[handles->co_conn_count + handles->dn_conn_count];
	int new_conn_count = 0;
	int i;
	char *resetcmd = "RESET ALL;"
					 "RESET SESSION AUTHORIZATION;"
					 "RESET transaction_isolation;";

	elog(DEBUG5,
		 "pgxc_node_remote_cleanup_all - handles->co_conn_count %d,"
		 "handles->dn_conn_count %d",
		 handles->co_conn_count,
		 handles->dn_conn_count);
	/*
	 * We must handle reader and writer connections both since even a read-only
	 * needs to be cleaned up.
	 */
	if (handles->co_conn_count + handles->dn_conn_count == 0)
	{
		pfree_pgxc_all_handles(handles);
		return;
	}

	/*
	 * Send down snapshot followed by DISCARD ALL command.
	 */
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->coord_handles[i];

		/* At this point connection should be in IDLE state */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
		{
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}

		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to clean up data nodes")));
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->datanode_handles[i];

		/* At this point connection should be in IDLE state */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
		{
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}

		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to clean up data nodes")));
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}

	if (new_conn_count)
	{
		ResponseCombiner combiner;
		InitResponseCombiner(&combiner, new_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		pgxc_node_receive_responses(new_conn_count, new_connections, NULL, &combiner);
		CloseCombiner(&combiner);
	}
	pfree_pgxc_all_handles(handles);
}

void InitTxnManagement(void)
{
	RegisterXactCallback(XactCallbackCoordinator, NULL);
	/*
	RegisterSubXactCallback(SubXactCallbackCoordinator, NULL);
	*/
	CommitContext = AllocSetContextCreateExtended(
		TopMemoryContext, "CommitContext", 8 * 1024, 8 * 1024, 8 * 1024);
}

void XactCallbackCoordinator(XactEvent event, void *args)
{
	switch (event)
	{
	case XACT_EVENT_PRE_COMMIT:
	{
		XactCallbackPreCommit();

		/* TODO:
			 * PostCommit perform the 2PC commit-prepared work, which should be
			 * put at after EVENT_COMMIT. But during commit-prepared, the SysCache
			 * will be used, which required the transaction status is INPROGRESS
			 */
		XactCallbackPostCommit();
		break;
	}
	case XACT_EVENT_COMMIT:
	{
		/* TODO */
		/* XactCallbackPostCommit(); */
		break;
	}
	case XACT_EVENT_ABORT:
	{
		XactCallbackPostAbort();
		break;
	}
	case XACT_EVENT_PRE_PREPARE:
	case XACT_EVENT_PREPARE:
	{
		break;
	}
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_ABORT:
	case XACT_EVENT_PARALLEL_COMMIT:
	{
		if (!IS_PGXC_SINGLE_NODE)
			ereport(WARNING,
					(errmsg("XactCallbackCoordinator unhandled event %d",
							event)));
		return;
	}
	}
}

void SubXactCallbackCoordinator(SubXactEvent event, void *args)
{
}

/**
 * Invoked before xact commit
 * Prepare remote transactions if 2pc is required
 */
void XactCallbackPreCommit(void)
{
	if (!IS_PGXC_LOCAL_COORDINATOR)
		return;

	bool isImplicit = !(IsTransactionStatePrepared());

	XactLocalNodePrepared = false;
	if (savePrepareGID)
	{
		pfree(savePrepareGID);
		savePrepareGID = NULL;
	}
	if (saveNodeString)
	{
		pfree(saveNodeString);
		saveNodeString = NULL;
	}
	/*
	 * Made node connections persistent if we are committing transaction
	 * that touched temporary tables. We never drop that flag, so after some
	 * transaction has created a temp table the session's remote connections
	 * become persistent.
	 * We do not need to set that flag if transaction that has created a temp
	 * table finally aborts - remote connections are not holding temporary
	 * objects in this case.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR && (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL))
		temp_object_included = true;

	bool xactWriteLocalNode = isXactWriteLocalNode();
	if (IsOnCommitActions() || !IsTwoPhaseCommitRequired(xactWriteLocalNode))
	{
#ifdef POLARDB_X
		if (enable_twophase_recover_debug_print)
		{
			elog(DEBUG_2PC, "XactCallbackPreCommit: no need to use 2pc, return.");
		}
#endif
		return;
	}

#ifdef POLARDB_X
	if (enable_twophase_recover_debug_print)
	{
		elog(DEBUG_2PC, "XactCallbackPreCommit: use 2pc, xactWriteLocalNode:%d.", xactWriteLocalNode);
	}
#endif

	const char *prepareGID = GetImplicit2PCGID(implicit2PC_head, xactWriteLocalNode);
#ifdef POLARDB_X
	if (enable_twophase_recover_debug_print)
		elog(DEBUG_2PC, "[xidtrace]XactCallbackPreCommit get prepareGID:%s", prepareGID);
#endif
	StorePrepareGID(prepareGID);
	savePrepareGID = MemoryContextStrdup(CommitContext, prepareGID);

#ifdef POLARDB_X
	char *nodestring = pgxc_node_remote_prepare(prepareGID, xactWriteLocalNode, isImplicit);
#else
	char *nodestring = pgxc_node_remote_prepare(prepareGID, false, true);
#endif
	if (nodestring)
	{
		g_twophase_state.state = TWO_PHASE_PREPARED;
		saveNodeString = MemoryContextStrdup(CommitContext, nodestring);
	}

	if (enable_timestamp_debug_print)
	{
		ereport(LOG, (errmsg("XactCallbackPreCommit nodestring=%s prepareGID=%s",
							 saveNodeString, prepareGID)));
	}

#ifdef POLARDB_X
	if (xactWriteLocalNode)
	{
		/*
		 * If local coordinator participate in, then prepare self after remote nodes prepare success.
		 * Also will start new transaction.
		 */
		PrepareStartNode();
		return;
	}
#endif
}

void XactCallbackPostCommit(void)
{
	if (IS_PGXC_COORDINATOR)
	{
		if (g_twophase_state.state == TWO_PHASE_PREPARED)
		{
			/* 2pc prepared, but no participants */
			if (saveNodeString != NULL && saveNodeString[0] != '\0')
			{
				pgxc_node_remote_commit_prepared(savePrepareGID, true, saveNodeString);
			}
		}
		else
		{
			/* TODO commit 1pc before local transaction commit */
			pgxc_node_remote_commit(TXN_TYPE_CommitTxn, true);
		}

#ifdef POLARDB_X
		if (XactLocalNodePrepared)
		{
			Assert(XactWriteLocalNode);
			XactLocalNodePrepared = false;
			PreventInTransactionBlock(true, "COMMIT IMPLICIT PREPARED");
			FinishPreparedTransaction(savePrepareGID, true);

			/* 2pc is all ok, so cleanup 2pc files and in-memory twophasedata in startnode. */
			CleanUpTwoPhaseFile(savePrepareGID);
			/* Finish remote node's global transaction, cleanup 2pc files and in-memory twophasedata */
			FinishGlobalXacts(savePrepareGID, saveNodeString);
		}
#else
		if (XactLocalNodePrepared)
			XactLocalNodePrepared = false;
#endif
	}

	ForgetTransactionLocalNode();
	AtEOXact_Twophase();
	ClearLocalTwoPhaseState();

	/*
	 * Set the command ID of Coordinator to be sent to the remote nodes
	 * as the 1st one.
	 * For remote nodes, enforce the command ID sending flag to false to avoid
	 * sending any command ID by default as now transaction is done.
	 */
	if (!IS_PGXC_SINGLE_NODE)
	{
		if (IS_PGXC_LOCAL_COORDINATOR)
			SetReceivedCommandId(FirstCommandId);
		else
			SetSendCommandId(false);
	}
}

void XactCallbackPostAbort(void)
{
	StringInfoData errormsg;

	/* print prepare err in pgxc_node_remote_prepare */
	if (TWO_PHASE_PREPARING == g_twophase_state.state)
	{
		print_twophase_state(&errormsg, true);
	}

	/*
	 * Cleanup the files created during database/tablespace operations.
	 * This must happen before we release locks, because we want to hold the
	 * locks acquired initially while we cleanup the files.
	 */
#ifndef POLARX_TODO
	AtEOXact_DBCleanup(false);

	SqueueProducerExit();
#endif

	/*
	 * Handle remote abort first.
	 */
	if (TWO_PHASE_ABORTTING != g_twophase_state.state)
	{
		bool need_release_handle = IsNeedReleaseHandle();
		g_twophase_state.state = TWO_PHASE_ABORTTING;
		AbortRunningQuery(TXN_TYPE_RollbackTxn, need_release_handle);
		pgxc_node_remote_abort(TXN_TYPE_RollbackTxn, need_release_handle);
	}

	if (XactLocalNodePrepared)
	{
		g_twophase_state.state = TWO_PHASE_ABORTTED;
		PreventInTransactionBlock(true, "ROLLBACK IMPLICIT PREPARED");
		FinishPreparedTransaction(savePrepareGID, false);
		XactLocalNodePrepared = false;
	}
	else
	{
		g_twophase_state.state = TWO_PHASE_ABORTTED;
		ClearLocalTwoPhaseState();
	}

	ForgetTransactionLocalNode();
	SetExitPlpgsqlFunc();
	// SetExitCreateExtension();
	SetCurrentHandlesReadonly();
	AtEOXact_Global();
	StorePrepareGID(NULL);
}

/**
 * Invoked by FinishPreparedTransaction when COMMIT PREPARED or ROLLBACK PREPARED
 * TODO implement explicit prepared transaction on participants
 * currently, explicit prepared transcation does not prepare on data nodes, but which should be done.
 */
void XactCallbackFinishPrepared(bool isCommit)
{
	if (IS_PGXC_DATANODE)
		return;
	if (!isCommit)
	{
		XactCallbackPostAbort();
	}
	else
	{
		XactCallbackPostCommit();
	}
}

#ifdef POLARDB_X
/*
 * Remember that the local node has done some write activity
 */
void RegisterTransactionLocalNode(bool write)
{
	if (write)
	{
		XactWriteLocalNode = true;
		XactReadLocalNode = false;
	}
	else
		XactReadLocalNode = true;
}
#endif

void ForgetTransactionLocalNode(void)
{
	XactReadLocalNode = XactWriteLocalNode = false;
}

void AtEOXact_Remote(void)
{
	PGXCNodeResetParams(true);
}

/* record and print errormsg in AbortTransaction, only print when isprint==true */
static bool
print_twophase_state(StringInfo errormsg, bool isprint)
{
	int i;
	int index;

	initStringInfo(errormsg);
	appendStringInfo(errormsg,
					 "Twophase Transaction '%s', partnodes '%s', failed in "
					 "Global State %s\n",
					 g_twophase_state.gid,
					 g_twophase_state.participants,
					 GetTransStateString(g_twophase_state.state));
	if (0 < (g_twophase_state.datanode_index + g_twophase_state.coord_index))
	{
		appendStringInfo(
			errormsg, "\t state of 2pc transaction '%s' on each node:\n", g_twophase_state.gid);
	}
	for (i = 0; i < g_twophase_state.datanode_index; i++)
	{
		if (g_twophase_state.datanode_state[i].is_participant == false)
			continue;
		index = g_twophase_state.datanode_state[i].handle_idx;
		appendStringInfo(errormsg,
						 "\t\t 2pc twophase state on datanode: %s, gid: %s, "
						 "trans state: %s, conn state: %s, receivedTimestamp: " LOGICALTIME_FORMAT
						 "\n",
						 g_twophase_state.handles->datanode_handles[index]->nodename,
						 g_twophase_state.gid,
						 GetTransStateString(g_twophase_state.datanode_state[i].state),
						 GetConnStateString(g_twophase_state.datanode_state[i].conn_state),
						 LOGICALTIME_STRING(g_twophase_state.datanode_state[i].receivedTs));
	}

	for (i = 0; i < g_twophase_state.coord_index; i++)
	{
		if (g_twophase_state.coord_state[i].is_participant == false)
			continue;
		index = g_twophase_state.coord_state[i].handle_idx;
		appendStringInfo(errormsg,
						 "\t\t 2pc twophase state on coordnode: %s, gid: %s, "
						 "state: %s, conn state: %s, receivedTimestamp: " LOGICALTIME_FORMAT "\n",
						 g_twophase_state.handles->coord_handles[index]->nodename,
						 g_twophase_state.gid,
						 GetTransStateString(g_twophase_state.coord_state[i].state),
						 GetConnStateString(g_twophase_state.coord_state[i].conn_state),
						 LOGICALTIME_STRING(g_twophase_state.coord_state[i].receivedTs));
	}
	if (0 < (g_twophase_state.datanode_index + g_twophase_state.coord_index))
	{
		appendStringInfo(errormsg,
						 "\t response msg of 2pc transaction '%s' on each node:\n",
						 g_twophase_state.gid);
	}
	for (i = 0; i < g_twophase_state.datanode_index; i++)
	{
		if (g_twophase_state.datanode_state[i].is_participant == false)
			continue;
		index = g_twophase_state.datanode_state[i].handle_idx;
		if (strlen(g_twophase_state.handles->datanode_handles[index]->error))
			appendStringInfo(errormsg,
							 "\t\t 2pc twophase state on datanode: %s, gid: %s, errmsg: %s\n",
							 g_twophase_state.handles->datanode_handles[index]->nodename,
							 g_twophase_state.gid,
							 g_twophase_state.handles->datanode_handles[index]->error);
	}

	for (i = 0; i < g_twophase_state.coord_index; i++)
	{
		if (g_twophase_state.coord_state[i].is_participant == false)
			continue;
		index = g_twophase_state.coord_state[i].handle_idx;
		if (strlen(g_twophase_state.handles->coord_handles[index]->error))
			appendStringInfo(errormsg,
							 "\t\t 2pc twophase state on coordnode: %s, gid: %s, errmsg: %s\n",
							 g_twophase_state.handles->coord_handles[index]->nodename,
							 g_twophase_state.gid,
							 g_twophase_state.handles->coord_handles[index]->error);
	}
	if (isprint)
	{
		elog(LOG, "%s", errormsg->data);
		resetStringInfo(errormsg);
	}
	return true;
}

static void
InitTxnQueryHashTable(void)
{
	HASHCTL hash_ctl;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));

	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(TxnDatanodeStatement) + NumDataNodes * sizeof(int);
	hash_ctl.hcxt = CacheMemoryContext;

	txn_datanode_queries = hash_create("Transaction Datanode Queries",
									   64,
									   &hash_ctl,
									   HASH_ELEM);
}

static TxnDatanodeStatement *
FetchTxnDatanodeStatement(const char *stmt_name, bool throwError)
{
	TxnDatanodeStatement *entry;

	/*
	 * If the hash table hasn't been initialized, it can't be storing
	 * anything, therefore it couldn't possibly store our plan.
	 */
	if (txn_datanode_queries)
		entry = (TxnDatanodeStatement *)hash_search(txn_datanode_queries, stmt_name, HASH_FIND, NULL);
	else
		entry = NULL;

	/* Report error if entry is not found */
	if (!entry && throwError)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PSTATEMENT),
				 errmsg("transaction datanode statement \"%s\" does not exist",
						stmt_name)));

	return entry;
}

static bool
HaveActiveTxnDatanodeStatements(void)
{
	HASH_SEQ_STATUS seq;
	TxnDatanodeStatement *entry;

	/* nothing cached */
	if (!txn_datanode_queries)
		return false;

	/* walk over cache */
	hash_seq_init(&seq, txn_datanode_queries);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		/* Stop walking and return true */
		if (entry->number_of_nodes > 0)
		{
			hash_seq_term(&seq);
			return true;
		}
	}
	/* nothing found */
	return false;
}

void DropTxnDatanodeStatement(const char *stmt_name)
{
	TxnDatanodeStatement *entry;

	entry = FetchTxnDatanodeStatement(stmt_name, false);
	if (entry)
	{
		int i;
		List *nodelist = NIL;

		/* make a List of integers from node numbers */
		for (i = 0; i < entry->number_of_nodes; i++)
			nodelist = lappend_int(nodelist, entry->dns_node_indices[i]);
		entry->number_of_nodes = 0;

		ExecCloseRemoteStatement(stmt_name, nodelist);

		hash_search(txn_datanode_queries, entry->stmt_name, HASH_REMOVE, NULL);
	}
}

bool PrepareTxnDatanodeStatement(char *stmt)
{
	bool exists;
	TxnDatanodeStatement *entry;
	bool first_call = false;

	/* Initialize the hash table, if necessary */
	if (!txn_datanode_queries)
	{
		InitTxnQueryHashTable();
		first_call = true;
	}

	hash_search(txn_datanode_queries, stmt, HASH_FIND, &exists);

	if (!exists)
	{
		entry = (TxnDatanodeStatement *)hash_search(txn_datanode_queries,
													stmt,
													HASH_ENTER,
													NULL);
		entry->number_of_nodes = 0;
	}
	return first_call;
}

bool ActivateTxnDatanodeStatementOnNode(const char *stmt_name, int noid)
{
	TxnDatanodeStatement *entry;
	int i;

	/* find the statement in cache */
	entry = FetchTxnDatanodeStatement(stmt_name, true);

	/* see if statement already active on the node */
	for (i = 0; i < entry->number_of_nodes; i++)
		if (entry->dns_node_indices[i] == noid)
			return true;

	/* statement is not active on the specified node append item to the list */
	entry->dns_node_indices[entry->number_of_nodes++] = noid;
	return false;
}

static void
DropAllTxnDatanodeStatement(void)
{
	HASH_SEQ_STATUS seq;
	TxnDatanodeStatement *entry;

	/* nothing cached */
	if (!txn_datanode_queries)
		return;

	/* walk over cache */
	hash_seq_init(&seq, txn_datanode_queries);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		int i;
		List *nodelist = NIL;

		/* make a List of integers from node numbers */
		for (i = 0; i < entry->number_of_nodes; i++)
			nodelist = lappend_int(nodelist, entry->dns_node_indices[i]);
		entry->number_of_nodes = 0;

		ExecCloseRemoteStatement(entry->stmt_name, nodelist);

		hash_search(txn_datanode_queries, entry->stmt_name, HASH_REMOVE, NULL);
	}
}

#ifdef POLARDB_X
static bool isXactWriteLocalNode(void)
{
	return XactWriteLocalNode;
}

static bool
FinishGlobalXacts(char *prepareGID, char *nodestring)
{
	// TODO: For now, clear pg_twophase file after all remote nodes compelete commit prepared cmd success.
	int i;
	List *nodelist = NIL;
	List *coordlist = NIL;
	bool prepared_local = false;
	PGXCNodeAllHandles *pgxc_handles;
	char *finish_global_txn_cmd = (char *)palloc(64 + strlen(prepareGID));
	sprintf(finish_global_txn_cmd, "select polardbx_finish_global_transation(%s) ", quote_literal_cstr(prepareGID));

	char *copyNodeString = strdup(nodestring);
	ResolveNodeString(copyNodeString, &nodelist, &coordlist, &prepared_local);
	free(copyNodeString);

	if (nodelist == NIL && coordlist == NIL)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "nodestring:%s only contain startnode itself.", nodestring);
		return prepared_local;
	}

	pgxc_handles = get_handles(nodelist, coordlist, false, true);

	int conn_count = pgxc_handles->dn_conn_count + pgxc_handles->co_conn_count;
	PGXCNodeHandle **connections = (PGXCNodeHandle **)palloc(sizeof(PGXCNodeHandle *) * conn_count);

	for (i = 0; i < conn_count; i++)
	{
		bool is_datanode = i < pgxc_handles->dn_conn_count;
		int handle_index = is_datanode ? i : i - pgxc_handles->dn_conn_count;
		PGXCNodeHandle *conn = is_datanode ? pgxc_handles->datanode_handles[handle_index] : pgxc_handles->coord_handles[handle_index];
		ConnState conn_state = TWO_PHASE_HEALTHY;

		if (txn_coordination != TXN_COORDINATION_NONE &&
			pgxc_node_send_query(conn, finish_global_txn_cmd))
		{
			conn_state = TWO_PHASE_SEND_QUERY_ERROR;
			elog(WARNING, "Failed to send finish_global_txn_cmd:%s to node:%s, backend_pid:%d, conn_state:%d",
				 finish_global_txn_cmd, conn->nodename, conn->backend_pid, conn_state);
		}
		connections[i] = conn;
	}
	pfree(finish_global_txn_cmd);

	ResponseCombiner combiner;
	if (conn_count > 0)
	{
		int result;
		/*
		 * Receive and check for any errors. In case of errors, we don't bail out
		 * just yet. We first go through the list of connections and look for
		 * errors on each connection. This is important to ensure that we run
		 * an appropriate ROLLBACK command later on (prepared transactions must be
		 * rolled back with ROLLBACK PREPARED commands).
		 *
		 * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
		 * individual connections. The transaction_status field doesn't get set
		 * every time there is an error on the connection. The combiner mechanism is
		 * good for parallel proessing, but I think we should have a leak-proof
		 * mechanism to track connection status
		 */
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		g_twophase_state.response_operation = REMOTE_PREPARE;
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);

		if (result)
		{
			elog(LOG, "FinishGlobalXacts pgxc_node_receive_responses of ROLLBACK failed");
			result = EOF;
		}
		else if (!validate_combiner(&combiner))
		{
			elog(LOG, "FinishGlobalXacts validate_combiner responese of ROLLBACK failed");
			result = EOF;
		}

		if (result)
		{
			if (combiner.errorMessage)
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send finish global transaction to on one or more nodes errmsg:%s",
								combiner.errorMessage)));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send finish global transaction to on one or more nodes")));
			}
		}
		CloseCombiner(&combiner);
	}

	TxnCleanUpHandles(pgxc_handles, true);
	return true;
}
#endif
