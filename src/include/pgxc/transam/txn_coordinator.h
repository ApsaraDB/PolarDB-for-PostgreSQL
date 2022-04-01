/*-------------------------------------------------------------------------
 *
 * txn_coordinator.h
 *
 *          Distributed transaction coordination
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/transam/txn_coordinator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARDBX_TXN_COORDINATOR_H
#define POLARDBX_TXN_COORDINATOR_H

#include "postgres.h"

#include "access/xact.h"
#include "pg_config_manual.h"
#include "pgxc/transam/txn_util.h"
#include "pgxc/transam/txn_util.h"
#include "distributed_txn/logical_clock.h"


typedef enum
{
	TWO_PHASE_HEALTHY				  = 0,	/* send cmd succeed */
	TWO_PHASE_SEND_GXID_ERROR		  = -1, /* send gxid failed */
	TWO_PHASE_SEND_TIMESTAMP_ERROR	  = -2, /* send timestamp fail */
	TWO_PHASE_SEND_STARTER_ERROR	  = -3, /* send startnode fail */
	TWO_PHASE_SEND_STARTXID_ERROR	  = -4, /* send xid in startnode fail */
	TWO_PHASE_SEND_PARTICIPANTS_ERROR = -5, /* send participants fail */
	TWO_PHASE_SEND_QUERY_ERROR		  = -6	/* send cmd fail */
} ConnState;

typedef enum
{
	TWO_PHASE_INITIALTRANS = 0, /* initial state */
	
	TWO_PHASE_PREPARING,		/* start to prepare */
	TWO_PHASE_PREPARED,			/* finish prepare */
	TWO_PHASE_PREPARE_ERROR,	/* fail to prepare */
	
	TWO_PHASE_COMMITTING,		/* start to commit */
	TWO_PHASE_COMMITTED,		/* finish commit */
	TWO_PHASE_COMMIT_ERROR,		/* send fail or response fail during 'commit
								   prepared' */
	TWO_PHASE_ABORTTING,		/* start to commit */
	TWO_PHASE_ABORTTED,			/* finish abort */
	TWO_PHASE_ABORT_ERROR,		/* send fail or response fail during 'rollback
								   prepared'*/
	TWO_PHASE_UNKNOW_STATUS		/* explicit twophase trans can not GetGTMGID */
} TwoPhaseTransState;

typedef enum
{
	OTHER_OPERATIONS = 0, /* we do not update g_twophase_state in
							 receive_response for  OTHER_OPERATIONS*/
	REMOTE_PREPARE,		  /* from pgxc_node_remote_prepare */
	REMOTE_PREPARE_ERROR, /* from prepare_err in pgxc_node_remote_prepare */
	REMOTE_PREPARE_ABORT, /* from abort in prepare_err */
	REMOTE_FINISH_COMMIT, /* from pgxc_node_remote_finish_prepared(commit) */
	REMOTE_FINISH_ABORT,  /* from pgxc_node_remote_finish_prepared(abort) */
	REMOTE_ABORT		  /* from pgxc_node_remote_abort */
} CurrentOperation;		  /* record twophase trans operation before receive responses
						   */

typedef struct ConnTransState /* record twophase trasaction state of each
								 connection*/
{
	bool			   is_participant;
	ConnState		   conn_state; /* record state of each connection in twophase trans */
	TwoPhaseTransState state;	   /* state of twophase trans in each connection */
	int				   handle_idx; /* index of dn_handles or cn_handles */
	LogicalTime		   receivedTs; /* received timestamp from datanode */
} ConnTransState;

typedef struct AllConnNodeInfo
{
	char node_type;				 /* 'C' or 'D'*/
	int	 conn_trans_state_index; /*index in g_twophase_state.coord_state or
									g_twophase_state.datanode_state*/
} AllConnNodeInfo;

typedef struct LocalTwoPhaseState
{
	bool is_readonly;			 /* since explicit transaction can be readonly, need to
									record readonly in 2pc file */
	ConnTransState *coord_state; /* each coord participants state */
	int				coord_index; /* index of coord_state */
	ConnTransState *datanode_state;
	int				datanode_index; /* index of datanode_state */

	TwoPhaseTransState state;						 /* global twophase state */
	char *			   gid;							 /* gid of twophase transaction*/
	char *			   participants;

	AllConnNodeInfo *	connections; /* map to coord_state or datanode_state in
										pgxc_node_receive_response */
	int				 connections_num;
	CurrentOperation response_operation;
} LocalTwoPhaseState;
extern LocalTwoPhaseState g_twophase_state;

#ifdef POLARDB_X
extern bool 			   enable_twophase_recover_debug_print;
#endif


extern void InitLocalTwoPhaseState(void);
#endif /*POLARDBX_TXN_COORDINATOR_H*/
