/*-------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *      Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxcnode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H
#include "postgres.h"

#include <unistd.h>

#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "pgxc/planner.h"
#include "pgxc/transam/txn_util.h"
#include "distributed_txn/logical_clock.h"
#include "tcop/dest.h"
#include "utils/snapshot.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#define CLEAR_BIT(data, bit) data = (~(1 << (bit)) & (data))
#define SET_BIT(data, bit)   data = ((1 << (bit)) | (data))
#define BIT_CLEAR(data, bit) (0 == ((1 << (bit)) & (data)))
#define BIT_SET(data, bit)   ((1 << (bit)) & (data))

#define UINT32_BITS_NUM 32
#define WORD_NUMBER_FOR_NODES (MAX_NODES_NUMBER / UINT32_BITS_NUM)

#define PGXC_NODENAME_LENGTH 64
#define POLARX_MAX_COORDINATOR_NUMBER 256
#define POLARX_MAX_DATANODE_NUMBER 256

#define NO_SOCKET -1

/* Helper structure to access Datanode from Session */
typedef enum
{
    DN_CONNECTION_STATE_IDLE,            /* idle, ready for query */
    DN_CONNECTION_STATE_QUERY,            /* query is sent, response expected */
    DN_CONNECTION_STATE_CLOSE,            /* close is sent, confirmation expected */
    DN_CONNECTION_STATE_ERROR_FATAL,    /* fatal error */
    DN_CONNECTION_STATE_COPY_IN,
    DN_CONNECTION_STATE_COPY_OUT
}    DNConnectionState;

typedef enum
{
    HANDLE_IDLE,
    HANDLE_ERROR,
    HANDLE_DEFAULT
}    PGXCNode_HandleRequested;


typedef enum
{
    DNStatus_OK      = 0,
    DNStatus_ERR     = 1,
    DNStatus_EXPIRED = 2,
    DNStatus_BUTTY
}DNStateEnum;

typedef enum
{
    SendSetQuery_OK                    = 0,
    SendSetQuery_EXPIRED            = 1,
    SendSetQuery_SendQuery_ERROR    = 2,
    SendSetQuery_Set_ERROR            = 3,
    SendSetQuery_BUTTY
}SendSetQueryStatus;

#define     MAX_ERROR_MSG_LENGTH          1024



#define DN_CONNECTION_STATE_ERROR(dnconn) \
        ((dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL \
            || (dnconn)->transaction_status == 'E')

#define HAS_MESSAGE_BUFFERED(conn) \
        ((conn)->inCursor + 4 < (conn)->inEnd \
            && (conn)->inCursor + ntohl(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

struct pgxc_node_handle
{
    Oid            nodeoid;
    int            nodeid;
    char        nodename[NAMEDATALEN];
    char        nodehost[NAMEDATALEN];
    int            nodeport;

    /* fd of the connection */
    int        sock;
    /* pid of the remote backend process */
    int        backend_pid;

    /* Connection state */
    char        transaction_status;
    DNConnectionState state;
    bool        read_only;
    struct ResponseCombiner *combiner;
#ifdef DN_CONNECTION_DEBUG
    bool        have_row_desc;
#endif
    uint64        sendGxidVersion;
    char        error[MAX_ERROR_MSG_LENGTH];
    /* Output buffer */
    char        *outBuffer;
    size_t        outSize;
    size_t        outEnd;
    /* Input buffer */
    char        *inBuffer;
    size_t        inSize;
    size_t        inStart;
    size_t        inEnd;
    size_t        inCursor;
    /*
     * Have a variable to enable/disable response checking and
     * if enable then read the result of response checking
     *
     * For details see comments of RESP_ROLLBACK
     */
    bool        ck_resp_rollback;

    bool        in_extended_query;
    bool        needSync; /* set when error and extend query. */
    char        last_command; /*last command we processed. */
    long        recv_datarows;
    bool         plpgsql_need_begin_sub_txn;
    bool         plpgsql_need_begin_txn;
	LogicalTime receivedTimestamp;
};
typedef struct pgxc_node_handle PGXCNodeHandle;

/* Structure used to get all the handles involved in a transaction */
typedef struct
{
    PGXCNodeHandle       *primary_handle;    /* Primary connection to PGXC node */
    int                    dn_conn_count;    /* number of Datanode Handles including primary handle */
    PGXCNodeHandle      **datanode_handles;    /* an array of Datanode handles */
    int                    co_conn_count;    /* number of Coordinator handles */
    PGXCNodeHandle      **coord_handles;    /* an array of Coordinator handles */
} PGXCNodeAllHandles;


extern int NumDataNodes;
extern int NumCoords;
extern int NumSlaveDataNodes;
extern int DataRowBufferSize;
extern bool enable_log_remote_query;


extern void InitMultinodeExecutor(bool is_force);
extern Oid get_nodeoid_from_nodeid(int nodeid, char node_type);


/* Open/close connection routines (invoked from Pool Manager) */
extern void PGXCNodeCleanAndRelease(int code, Datum arg);

extern PGXCNodeHandle *get_any_handle(List *datanodelist);
/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char *node_type);
extern Oid PGXCGetLocalNodeOid(Oid nodeoid);
extern Oid PGXCGetMainNodeOid(Oid nodeoid);
extern int PGXCNodeGetNodeIdFromName(char *node_name, char *node_type);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);

extern PGXCNodeAllHandles *get_handles(List *datanodelist, List *coordlist, bool is_query_coord_only, bool is_global_session);

extern PGXCNodeAllHandles *get_current_handles(void);
extern void pfree_pgxc_all_handles(PGXCNodeAllHandles *handles);

extern void release_handles(bool force);

extern void clear_handles(void);

extern int    ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);
extern int    ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);

extern int pgxc_node_send_query(PGXCNodeHandle *handle, const char *query);
extern int pgxc_node_send_rollback(PGXCNodeHandle *handle, const char *query);
extern int pgxc_node_send_describe(PGXCNodeHandle *handle, bool is_statement, const char *name);
extern int pgxc_node_send_execute(PGXCNodeHandle *handle, const char *portal, int fetch);
extern int pgxc_node_send_close(PGXCNodeHandle *handle, bool is_statement, const char *name);
extern int pgxc_node_send_sync(PGXCNodeHandle *handle);
extern int pgxc_node_send_disconnect(PGXCNodeHandle *handle, char *cursor, int cons);
extern int pgxc_node_send_bind(
	PGXCNodeHandle *handle, const char *portal, const char *statement, int paramlen, char *params);
extern int pgxc_node_send_parse(PGXCNodeHandle *handle,
								const char *	statement,
								const char *	query,
								short			num_params,
								Oid *			param_types);
extern int pgxc_node_send_flush(PGXCNodeHandle *handle);
extern int pgxc_node_send_query_extended(PGXCNodeHandle *handle,
										 const char *	 query,
										 const char *	 statement,
										 const char *	 portal,
										 int			 num_params,
										 Oid *			 param_types,
										 int			 paramlen,
										 char *			 params,
										 bool			 send_describe,
										 int			 fetch_size);
extern int pgxc_node_send_plan(PGXCNodeHandle *handle,
							   const char *	   statement,
							   const char *	   query,
							   const char *	   planstr,
							   short		   num_params,
							   Oid *		   param_types);
extern int pgxc_node_send_gid(PGXCNodeHandle *handle, char *gid);

/* distributed transaction */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
extern int pgxc_node_send_starter(PGXCNodeHandle *handle, char *startnode);
extern int pgxc_node_send_startxid(PGXCNodeHandle *handle, GlobalTransactionId transactionid);
extern int pgxc_node_send_partnodes(PGXCNodeHandle *handle, char *partnodes);
extern int pgxc_node_send_clean(PGXCNodeHandle *handle);
extern int pgxc_node_send_readonly(PGXCNodeHandle *handle);
extern int pgxc_node_send_after_prepare(PGXCNodeHandle *handle);
extern int pgxc_node_send_gxid(PGXCNodeHandle *handle, char* gxid);
extern int pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid);
extern int pgxc_node_send_snapshot(PGXCNodeHandle *handle, Snapshot snapshot);
extern int pgxc_node_send_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int pgxc_node_send_prepare_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int pgxc_node_send_prefinish_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int pgxc_node_send_global_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
#endif /*ENABLE_DISTRIBUTED_TRANSACTION*/

extern int    pgxc_node_receive(const int conn_count,
                  PGXCNodeHandle ** connections, struct timeval * timeout);
extern bool node_ready_for_query(PGXCNodeHandle *conn);
extern bool validate_handles(void);
extern int    pgxc_node_read_data(PGXCNodeHandle * conn, bool close_if_error);
extern int    pgxc_node_is_data_enqueued(PGXCNodeHandle *conn);
extern void pgxc_abort_connections(PGXCNodeAllHandles *all_handles);
extern void BufferConnection(PGXCNodeHandle *conn);

extern int    send_some(PGXCNodeHandle * handle, int len);
extern int    pgxc_node_flush(PGXCNodeHandle *handle);
extern void    pgxc_node_flush_read(PGXCNodeHandle *handle);

extern char get_message(PGXCNodeHandle *conn, int *len, char **msg);

extern void add_error_message(PGXCNodeHandle * handle, const char *message);

extern void PGXCNodeSetParam(bool local, const char *name, const char *value,
                int flags);
extern void PGXCNodeResetParams(bool only_local);
extern char *PGXCNodeGetSessionParamStr(void);
extern char *PGXCNodeGetTransactionParamStr(void);
extern void pgxc_node_set_query(PGXCNodeHandle *handle, const char *set_query);
extern void RequestInvalidateRemoteHandles(void);
extern void RequestRefreshRemoteHandles(void);
extern bool PoolerMessagesPending(void);
extern void PGXCNodeSetConnectionState(PGXCNodeHandle *handle,
        DNConnectionState new_state);
extern bool PgxcNodeDiffBackendHandles(List **nodes_alter,
               List **nodes_delete, List **nodes_add);
extern void PgxcNodeRefreshBackendHandlesShmem(List *nodes_alter);
extern void HandlePoolerMessages(void);
extern void pgxc_print_pending_data(PGXCNodeHandle *handle, bool reset);
extern void SetCurrentHandlesReadonly(void);


#ifdef POLARDB_X
extern int pgxc_node_send_cleanup_2pc_file_command(PGXCNodeHandle *handle, char* gid);
#endif

#endif /* PGXCNODE_H */

