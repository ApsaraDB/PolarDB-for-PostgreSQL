/*-------------------------------------------------------------------------
 *
 * txn.h
 *		Declarations for txn.c
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/include/distribute_transaction/txn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TXN_H
#define TXN_H

#include "polarx.h"
#include "pg_config_manual.h"
#include "pgxc/pgxcnode.h"
#include "access/xact.h"
#define implicit2PC_head "_$XC$"
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
#include "distributed_txn/logical_clock.h"
#endif

#ifdef POLARDBX_TWO_PHASE_TESTS
extern int twophase_exception_case;
extern int twophase_exception_node_exception;

typedef enum
{
    ERROR_GET_PREPARE_TIMESTAMP_FAIL = 1, // only tso.
    ERROR_SEND_PREPARE_TIMESTAMP_FAIL, // only tso.
    ERROR_SEND_PARTICIPATE_NODE_FAIL,
    ERROR_SEND_PREPARE_CMD_FAIL,
    ERROR_RECV_PREPARE_CMD_RESPONSE_FAIL, // 5
    ERROR_RECV_PREPARE_CMD_RESPONSE_PENDING,
    ERROR_GET_COMMIT_TIMESTAMP_FAIL,
    ERROR_SEND_COMMIT_PREPARED_FAIL, // 8
    ERROR_RECV_COMMIT_PREPARED_RESPONSE_FAIL,
    ERROR_RECV_COMMIT_PREPARED_RESPONSE_PENDING, //10
    ERROR_SEND_ROLLBACK_PREPARED_TXN_FAIL, // 11
    ERROR_Butty
}TwophaseTestCase;

typedef enum
{
    NODE_EXCEPTION_NORMAL = 0, // node is normal. not in exception
    NODE_EXCEPTION_CRASH = 1
}TwophaseTestNodeException;
#endif

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
    OTHER_OPERATIONS = 0, /* we do not update g_coord_twophase_state in
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
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION    
    LogicalTime		   receivedTs; /* received timestamp from datanode */
#endif
} ConnTransState;

typedef struct AllConnNodeInfo
{
    char node_type;				 /* 'C' or 'D'*/
    int	 conn_trans_state_index; /*index in g_coord_twophase_state.coord_state or
									g_coord_twophase_state.datanode_state*/
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

    PGXCNodeAllHandles *handles;	 /* handles in each phase in twophase trans */
    AllConnNodeInfo *	connections; /* map to coord_state or datanode_state in
										pgxc_node_receive_response */
    int				 connections_num;
    CurrentOperation response_operation;
#ifdef POLARDBX_SHARDING
	bool local_coord_participate; /* Normally it is false, 
                                    * but when update shardmap meta, we want local coord 
                                    * act as a remote cn, thus local coord can exec prepare txn
                                    * sql instead of PrepareStartNode function. */
	bool force_2pc;               /* Force use 2pc. Currently used in update shardmap metadata, 
                                    * prepared and commit even if only one coordinator participated.  */
#endif
} LocalTwoPhaseState;
extern LocalTwoPhaseState g_coord_twophase_state;

typedef enum
{
    TXN_TYPE_CommitTxn,
    TXN_TYPE_CommitSubTxn,
    TXN_TYPE_RollbackTxn,
    TXN_TYPE_RollbackSubTxn,
    TXN_TYPE_CleanConnection,

    TXN_TYPE_Butt
} TranscationType;



typedef struct
{
    /* dynahash.c requires key to be first field */
    char stmt_name[NAMEDATALEN];
    int number_of_nodes;	 /* number of nodes where statement is active */
    int dns_node_indices[0]; /* node ids where statement is active */
} TxnDatanodeStatement;

extern bool EnableHLCTransaction;
extern bool EnableTransactionDebugPrint;

extern char *savePrepareGID;
extern char *saveNodeString;
extern bool	XactLocalNodePrepared;
extern bool g_in_set_config_option;

extern void RegisterDistributeTxnCallback(void);
extern void RegisterTransactionLocalNode(bool write);
extern void XactCallbackCoordinator(XactEvent event, void *args);

extern void XactCallbackPreCommit(void);
extern void XactCallbackPostCommit(void);
extern void XactCallbackPostAbort(void);

extern void UpdateLocalCoordTwoPhaseState(int result, PGXCNodeHandle *response_handle, int conn_index, char *errmsg);
extern int	 pgxc_node_begin(int				 conn_count,
                               PGXCNodeHandle **	 connections,
                               GlobalTransactionId gxid,
                               bool				 need_tran_block,
                               bool				 readOnly,
                               char				 node_type);

extern bool isXactWriteLocalNode(void);
extern char* GetImplicit2PCGID(const char *head, bool localWrite);
extern void StorePrepareGID(const char *gid);
extern MemoryContext CommitContext;
extern bool IsTwoPhaseCommitRequired(bool localWrite);
#endif							/* TXN_H */
