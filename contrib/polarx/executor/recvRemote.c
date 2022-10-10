/*-------------------------------------------------------------------------
 *
 * recvRemote.c
 *
 *      Functions to receiver data from datanodes for fast ship query 
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx.h"

#include <time.h>

#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "optimizer/var.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "executor/execRemoteQuery.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/datarowstore.h"
#include "executor/recvRemote.h"
#include "distribute_transaction/txn.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "pool/poolnodes.h"


static void
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn);

static bool
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len);

static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, Oid node);

static void
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn);

static void
HandleDatanodeCommandId(ResponseCombiner *combiner, char *msg_body, size_t len);

static
void add_error_message_from_combiner(PGXCNodeHandle *handle, void *combiner_input);


static int
parse_row_count(const char *message, size_t len, uint64 *rowcount);
/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
void
pgxc_node_report_error(ResponseCombiner *combiner)
{// #lizard forgives
    /* If no combiner, nothing to do */
    if (!combiner)
        return;
    if (combiner->errorMessage)
    {
        char *code = combiner->errorCode;
#ifndef _PG_REGRESS_
        if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage)));
        else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail),
                    errhint("%s", combiner->errorHint)));
        else if (combiner->errorDetail != NULL)
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail)));
        else
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("node:%s, backend_pid:%d, %s", combiner->errorNode, combiner->backend_pid, combiner->errorMessage),
                    errhint("%s", combiner->errorHint)));
#else
        if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage)));
        else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail),
                    errhint("%s", combiner->errorHint)));
        else if (combiner->errorDetail != NULL)
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errdetail("%s", combiner->errorDetail)));
        else
            ereport(ERROR,
                    (errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
                    errmsg("%s", combiner->errorMessage),
                    errhint("%s", combiner->errorHint)));
#endif
    }
}

/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
void
InitResponseCombiner(ResponseCombiner *combiner, int node_count,
                       CombineType combine_type)
{
    combiner->node_count = node_count;
    combiner->connections = NULL;
    combiner->conn_count = 0;
    combiner->current_conn = 0;
    combiner->combine_type = combine_type;
    combiner->current_conn_rows_consumed = 0;
    combiner->command_complete_count = 0;
    combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
    combiner->description_count = 0;
    combiner->copy_in_count = 0;
    combiner->copy_out_count = 0;
    combiner->copy_file = NULL;
    combiner->errorMessage = NULL;
    combiner->errorDetail = NULL;
    combiner->errorHint = NULL;
    combiner->tuple_desc = NULL;
    combiner->probing_primary = false;
    combiner->returning_node = InvalidOid;
    combiner->currentRow = NULL;
    combiner->rowBuffer = NIL;
    combiner->tapenodes = NULL;
    combiner->merge_sort = false;
    combiner->extended_query = false;
    combiner->tapemarks = NULL;
    combiner->tuplesortstate = NULL;
    combiner->cursor = NULL;
    combiner->prep_name = NULL;
    combiner->update_cursor = NULL;
    combiner->cursor_count = 0;
    combiner->cursor_connections = NULL;
    combiner->remoteCopyType = REMOTE_COPY_NONE;
    combiner->dataRowBuffer  = NULL;
    combiner->dataRowMemSize = NULL;
    combiner->nDataRows      = NULL;
    combiner->tmpslot        = NULL;
    combiner->recv_datarows  = 0;
    combiner->prerowBuffers  = NULL;
    combiner->is_abort = false;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
    int            digits = 0;
    int            pos;

    *rowcount = 0;
    /* skip \0 string terminator */
    for (pos = 0; pos < len - 1; pos++)
    {
        if (message[pos] >= '0' && message[pos] <= '9')
        {
            *rowcount = *rowcount * 10 + message[pos] - '0';
            digits++;
        }
        else
        {
            *rowcount = 0;
            digits = 0;
        }
    }
    return digits;
}

#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
static void
HandleTimestamp(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	uint64 tmp;
	
	Assert(len == 8);
	memcpy(&tmp, msg_body, 8);
	conn->receivedTimestamp = (LogicalTime)pg_ntoh64(tmp);
	LogicalClockUpdate(conn->receivedTimestamp);
	if (enable_log_remote_query)
	{
		elog(DEBUG1, "receive timestamp "LOGICALTIME_FORMAT" from server %s",
				LOGICALTIME_STRING(conn->receivedTimestamp), conn->nodename);
	}
}
#endif/*ENABLE_DISTRIBUTED_TRANSACTION*/

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{// #lizard forgives
    int             digits = 0;
    EState           *estate = combiner->ss.ps.state;

    /*
     * If we did not receive description we are having rowcount or OK response
     */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COMMAND;
    /* Extract rowcount */
    if (combiner->combine_type != COMBINE_TYPE_NONE)
    {
        uint64    rowcount;
        digits = parse_row_count(msg_body, len, &rowcount);
        if (digits > 0)
	{
		if(estate)
		{
			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					/*
					 * Replicated command may succeed on on node and fail on
					 * another. The example is if distributed table referenced
					 * by a foreign key constraint defined on a partitioned
					 * table. If command deletes rows from the replicated table
					 * they may be referenced on one Datanode but not on other.
					 * So, replicated command on each Datanode either affects
					 * proper number of rows, or returns error. Here if
					 * combiner got an error already, we allow to report it,
					 * not the scaring data corruption message.
					 */
					if (combiner->errorMessage == NULL && rowcount != estate->es_processed)
					{
						/*
						 * In extend query protocol, need to set connection to idle
						 */
						if (combiner->extended_query &&
								conn->state == DN_CONNECTION_STATE_QUERY)
						{
							PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
#ifdef     _PG_REGRESS_
							elog(DEBUG1, "RESPONSE_COMPLETE_2 set node %s, remote pid %d DN_CONNECTION_STATE_IDLE", conn->nodename, conn->backend_pid);
#endif
						}
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned different results from the Datanodes")));
					}
				}
				else
					/* first result */
					estate->es_processed = rowcount;
			}
			else
				estate->es_processed += rowcount;
		}
		combiner->DML_processed += rowcount;
	}
        else
            combiner->combine_type = COMBINE_TYPE_NONE;
    }

    /* If response checking is enable only then do further processing */
    if (conn->ck_resp_rollback)
    {
        if (strcmp(msg_body, "ROLLBACK") == 0)
        {
            /*
             * Subsequent clean up routine will be checking this flag
             * to determine nodes where to send ROLLBACK PREPARED.
             * On current node PREPARE has failed and the two-phase record
             * does not exist, so clean this flag as if PREPARE was not sent
             * to that node and avoid erroneous command.
             */
            conn->ck_resp_rollback = false;
            /*
             * Set the error, if none, to force throwing.
             * If there is error already, it will be thrown anyway, do not add
             * this potentially confusing message
             */
            if (combiner->errorMessage == NULL)
            {
                MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
                combiner->errorMessage =
                                pstrdup("unexpected ROLLBACK from remote node");
                MemoryContextSwitchTo(oldcontext);
                /*
                 * ERRMSG_PRODUCER_ERROR
                 * Messages with this code are replaced by others, if they are
                 * received, so if node will send relevant error message that
                 * one will be replaced.
                 */
                combiner->errorCode[0] = 'X';
                combiner->errorCode[1] = 'X';
                combiner->errorCode[2] = '0';
                combiner->errorCode[3] = '1';
                combiner->errorCode[4] = '0';
            }
        }
    }
    combiner->command_complete_count++;
}

/*
 * consume describe response
 */
void
ignore_describe_response(char *msg_body, size_t len)
{
    int         i, nattr;
    uint16        n16;

    /* get number of attributes */
    memcpy(&n16, msg_body, 2);
    nattr = ntohs(n16);
    msg_body += 2;

    /* decode attributes */
    for (i = 1; i <= nattr; i++)
    {
        char        *attname;

        /* attribute name */
        attname = msg_body;
        msg_body += strlen(attname) + 1;

        /* table OID, ignored */
        msg_body += 4;

        /* column no, ignored */
        msg_body += 2;

        /* data type OID, ignored */
        msg_body += 4;

        /* type len, ignored */
        msg_body += 2;

        /* type mod */
        msg_body += 4;

        /* PGXCTODO text/binary flag? */
        msg_body += 2;

    }
}
/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len)
{
    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return false;
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_QUERY;
    if (combiner->request_type != REQUEST_TYPE_QUERY)
    {
        /* Inconsistent responses */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d", combiner->request_type)));
    }
    /* Increment counter and check if it was first */
    if (combiner->description_count == 0)
    {
        ignore_describe_response(msg_body, len);
        combiner->description_count++;
        return true;
    }
    combiner->description_count++;
    return false;
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if data row is accepted and successfully stored
 * within the combiner.
 */
static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, Oid node)
{// #lizard forgives
    /* We expect previous message is consumed */
    Assert(combiner->currentRow == NULL);

    if (combiner->request_type == REQUEST_TYPE_ERROR)
        return false;

    if (combiner->request_type != REQUEST_TYPE_QUERY)
    {
        /* Inconsistent responses */
        char data_buf[4096];

        snprintf(data_buf, len, "%s", msg_body);
        if(len > 4095)
            len = 4095;
        data_buf[len] = 0;
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Unexpected response from the data nodes for 'D' message, current request type %d, data %s",
                     combiner->request_type, data_buf)));
    }

    /*
     * If we got an error already ignore incoming data rows from other nodes
     * Still we want to continue reading until get CommandComplete
     */
    if (combiner->errorMessage)
        return false;

    /*
     * Replicated INSERT/UPDATE/DELETE with RETURNING: receive only tuples
     * from one node, skip others as duplicates
     */
    if (combiner->combine_type == COMBINE_TYPE_SAME)
    {
        /* Do not return rows when probing primary, instead return when doing
         * first normal node. Just save some CPU and traffic in case if
         * probing fails.
         */
        if (combiner->probing_primary)
            return false;
        if (OidIsValid(combiner->returning_node))
        {
            if (combiner->returning_node != node)
                return false;
        }
        else
            combiner->returning_node = node;
    }

    /*
     * We are copying message because it points into connection buffer, and
     * will be overwritten on next socket read
     */
    combiner->currentRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + len);
    memcpy(combiner->currentRow->msg, msg_body, len);
    combiner->currentRow->msglen = len;
    combiner->currentRow->msgnode = node;

    return true;
}


/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{// #lizard forgives
#define APPEND_LENGTH 128
    char *message_trans = NULL;
    /* parse error message */
    char *code = NULL;
    char *message = NULL;
    char *detail = NULL;
    char *hint = NULL;
    int   offset = 0;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len)
    {
        /* pointer to the field message */
        char *str = msg_body + offset + 1;

        switch (msg_body[offset])
        {
            case 'C':    /* code */
                code = str;
                break;
            case 'M':    /* message */
                message = str;
                break;
            case 'D':    /* details */
                detail = str;
                break;

            case 'H':    /* hint */
                hint = str;
                break;

            /* Fields not yet in use */
            case 'S':    /* severity */
            case 'R':    /* routine */
            case 'P':    /* position string */
            case 'p':    /* position int */
            case 'q':    /* int query */
            case 'W':    /* where */
            case 'F':    /* file */
            case 'L':    /* line */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    /*
     * We may have special handling for some errors, default handling is to
     * throw out error with the same message. We can not ereport immediately
     * because we should read from this and other connections until
     * ReadyForQuery is received, so we just store the error message.
     * If multiple connections return errors only first one is reported.
     *
     * The producer error may be hiding primary error, so if previously received
     * error is a producer error allow it to be overwritten.
     */
    if (combiner->errorMessage == NULL)
    {
        MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
#ifdef     _PG_REGRESS_
        (void)message_trans;
        combiner->errorMessage = pstrdup(message);
#else
        /* output more details */
        if (message)
        {
            message_trans = palloc(strlen(message)+APPEND_LENGTH);
            snprintf(message_trans, strlen(message)+APPEND_LENGTH,
                "nodename:%s,backend_pid:%d,message:%s", conn->nodename,conn->backend_pid,message);
        }

        combiner->errorMessage = message_trans;
#endif
        /* Error Code is exactly 5 significant bytes */
        if (code)
            memcpy(combiner->errorCode, code, 5);
        if (detail)
            combiner->errorDetail = pstrdup(detail);
        if (hint)
            combiner->errorHint = pstrdup(hint);
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * If the PREPARE TRANSACTION command fails for whatever reason, we don't
     * want to send down ROLLBACK PREPARED to this node. Otherwise, it may end
     * up rolling back an unrelated prepared transaction with the same GID as
     * used by this transaction
     */
    if (conn->ck_resp_rollback)
        conn->ck_resp_rollback = false;

    /*
     * If Datanode have sent ErrorResponse it will never send CommandComplete.
     * Increment the counter to prevent endless waiting for it.
     */
    combiner->command_complete_count++;
}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(ResponseCombiner *combiner, char *msg_body, size_t len)
{
    uint32        n32;
    CommandId    cid;

    Assert(msg_body != NULL);
    Assert(len >= 2);

    /* Get the command Id */
    memcpy(&n32, &msg_body[0], 4);
    cid = ntohl(n32);
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
    /* If received command Id is higher than current one, set it to a new value */
    if (cid > GetReceivedCommandId())
        SetReceivedCommandId(cid);
#endif
}

static
void add_error_message_from_combiner(PGXCNodeHandle *handle, void *combiner_input)
{// #lizard forgives
    ResponseCombiner *combiner;

    combiner = (ResponseCombiner*)combiner_input;

    elog(DEBUG1, "Remote node \"%s\", running with pid %d returned an error: %s",
            handle->nodename, handle->backend_pid, combiner->errorMessage);

    handle->transaction_status = 'E';
    if (handle->error[0] && combiner->errorMessage)
    {
        int32 offset = 0;

        offset = strnlen(handle->error, MAX_ERROR_MSG_LENGTH);

        if (combiner->errorMessage && combiner->errorDetail && combiner->errorHint)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s:%s:%s",
                combiner->errorMessage, combiner->errorDetail, combiner->errorHint);
        }
        else if (combiner->errorMessage && combiner->errorDetail)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s:%s",
                combiner->errorMessage, combiner->errorDetail);
        }
        else if (combiner->errorMessage)
        {
            snprintf(handle->error + offset, MAX_ERROR_MSG_LENGTH - offset, "%s",
                combiner->errorMessage);
        }

#ifdef _PG_REGRESS_
        elog(DEBUG1, "add_error_message node:%s, running with pid %d non first time error: %s, error ptr:%lx",
                handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error);
#endif

    }
    else if (combiner->errorMessage)
    {
        if (combiner->errorMessage && combiner->errorDetail && combiner->errorHint)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s:%s:%s",
                combiner->errorMessage, combiner->errorDetail, combiner->errorHint);
        }
        else if (combiner->errorMessage && combiner->errorDetail)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s:%s",
                combiner->errorMessage, combiner->errorDetail);
        }
        else if (combiner->errorMessage)
        {
            snprintf(handle->error, MAX_ERROR_MSG_LENGTH, "%s",
                combiner->errorMessage);
        }

#ifdef _PG_REGRESS_
        elog(DEBUG1, "add_error_message node:%s, running with pid %d first time error: %s, ptr:%lx",
                handle->nodename, handle->backend_pid, handle->error, (uint64)handle->error);
#endif
    }

    return;
}
/*
 * Read next message from the connection and update the combiner
 * and connection state accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * It returns if states need to be handled
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_READY - got ReadyForQuery
 * RESPONSE_COMPLETE - done with the connection, but not yet ready for query.
 * Also this result is output in case of error
 * RESPONSE_SUSPENDED - got PortalSuspended
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner)
{// #lizard forgives
    char       *msg;
    int            msg_len;
    char        msg_type;

    for (;;)
    {
        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         */
        if (proc_exit_inprogress)
        {
            PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
        }

        /*
         * Don't read from from the connection if there is a fatal error.
         * We still return RESPONSE_COMPLETE, not RESPONSE_ERROR, since
         * Handling of RESPONSE_ERROR assumes sending SYNC message, but
         * State DN_CONNECTION_STATE_ERROR_FATAL indicates connection is
         * not usable.
         */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
        {
#ifdef     _PG_REGRESS_
            elog(DEBUG1, "RESPONSE_COMPLETE_1 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
            return RESPONSE_COMPLETE;
        }

        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn))
            return RESPONSE_EOF;

        Assert(conn->combiner == combiner || conn->combiner == NULL);

        /* TODO handle other possible responses */
        msg_type = get_message(conn, &msg_len, &msg);
        elog(DEBUG5, "handle_response - received message %c, node %s, "
                "current_state %d", msg_type, conn->nodename, conn->state);

        conn->last_command = msg_type;

        /*
         * Add some protection code when receiving a messy message,
         * close the connection, and throw error
         */
        if (msg_len < 0)
        {
            PGXCNodeSetConnectionState(conn,
                    DN_CONNECTION_STATE_ERROR_FATAL);
            closesocket(conn->sock);
            conn->sock = NO_SOCKET;

            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Received messy message from node:%s host:%s port:%d pid:%d, "
                            "inBuffer:%p inSize:%lu inStart:%lu inEnd:%lu inCursor:%lu msg_len:%d, "
                            "This probably means the remote node terminated abnormally "
                            "before or while processing the request. ",
                            conn->nodename, conn->nodehost, conn->nodeport, conn->backend_pid,
                            conn->inBuffer, conn->inSize, conn->inStart, conn->inEnd, conn->inCursor, msg_len)));
        }

        switch (msg_type)
        {
            case '\0':            /* Not enough data in the buffer */
                return RESPONSE_EOF;
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
			case 'L':
				HandleTimestamp(combiner, msg, msg_len, conn);
				break;
#endif/* ENABLE_DISTRIBUTED_TRANSACTION*/

            case 'C':            /* CommandComplete */
                HandleCommandComplete(combiner, msg, msg_len, conn);
                conn->combiner = NULL;
                /*
                 * In case of simple query protocol, wait for the ReadyForQuery
                 * before marking connection as Idle
                 */
                if (combiner->extended_query &&
                    conn->state == DN_CONNECTION_STATE_QUERY)
                {
                    PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
#ifdef     _PG_REGRESS_
                    elog(DEBUG1, "RESPONSE_COMPLETE_2 set node %s, remote pid %d DN_CONNECTION_STATE_IDLE", conn->nodename, conn->backend_pid);
#endif
                }
#ifdef     _PG_REGRESS_
                elog(DEBUG1, "RESPONSE_COMPLETE_2 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_COMPLETE;

            case 'T':            /* RowDescription */
#ifdef DN_CONNECTION_DEBUG
                Assert(!conn->have_row_desc);
                conn->have_row_desc = true;
#endif
                if (HandleRowDescription(combiner, msg, msg_len))
                    return RESPONSE_TUPDESC;
                break;

            case 'D':            /* DataRow */
#ifdef DN_CONNECTION_DEBUG
                Assert(conn->have_row_desc);
#endif

                /* Do not return if data row has not been actually handled */
                if (HandleDataRow(combiner, msg, msg_len, conn->nodeoid))
                {
                    elog(DEBUG1, "HandleDataRow from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                    return RESPONSE_DATAROW;
                }
                break;
           case 's':            /* PortalSuspended */
                /* No activity is expected on the connection until next query */
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                return RESPONSE_SUSPENDED;
            case '1': /* ParseComplete */
                if(combiner->prep_name) 
                {
                    if(!SetPrepStmtIntoBEHash(combiner->prep_name, PGXCNodeGetNodeId(conn->nodeoid, NULL), true))
                        elog(ERROR, "save preapre stmt info into BE prep hash failed");
                }
                break;
            case '2': /* BindComplete */
                break;
            case '3': /* CloseComplete */
                if(combiner->prep_name)
                {
                    if(!SetPrepStmtIntoBEHash(combiner->prep_name, PGXCNodeGetNodeId(conn->nodeoid, NULL), false))
                        elog(ERROR, "save preapre stmt info into BE prep hash failed");
                }
                break;
            case 'n': /* NoData */
                /* simple notifications, continue reading */
                break;

            case 'E':            /* ErrorResponse */
                HandleError(combiner, msg, msg_len, conn);
                add_error_message_from_combiner(conn, combiner);
                /*
                 * In case the remote node was running an extended query
                 * protocol and reported an error, it will keep ignoring all
                 * subsequent commands until it sees a SYNC message. So make
                 * sure that we send down SYNC even before sending a ROLLBACK
                 * command
                 */


                combiner->errorNode   = conn->nodename;
                combiner->backend_pid = conn->backend_pid;

                if (conn->in_extended_query)
                {
                    conn->needSync = true;
                }
#ifdef     _PG_REGRESS_
                elog(DEBUG1, "HandleError from node %s, remote pid %d, errorMessage:%s",
                        conn->nodename, conn->backend_pid, combiner->errorMessage);
#endif
                return RESPONSE_ERROR;

            case 'A':            /* NotificationResponse */
            case 'N':            /* NoticeResponse */
            case 'S':            /* SetCommandComplete */
                /*
                 * Ignore these to prevent multiple messages, one from each
                 * node. Coordinator will send one for DDL anyway
                 */
                break;

            case 'Z':            /* ReadyForQuery */
            {
                /*
                 * Return result depends on previous connection state.
                 * If it was PORTAL_SUSPENDED Coordinator want to send down
                 * another EXECUTE to fetch more rows, otherwise it is done
                 * with the connection
                 */
                conn->transaction_status = msg[0];
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
                conn->have_row_desc = false;
#endif

#ifdef     _PG_REGRESS_
                elog(DEBUG1, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_READY;
            }

            case 'Y':            /* ReadyForCommit */
            {
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
                conn->have_row_desc = false;
#endif
                elog(DEBUG1, "ReadyForQuery from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                return RESPONSE_READY;
            }
            case 'M':            /* Command Id */
                HandleDatanodeCommandId(combiner, msg, msg_len);
                break;

            case 'b':
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
                return RESPONSE_BARRIER_OK;

            case 'I':            /* EmptyQuery */
#ifdef     _PG_REGRESS_
                elog(DEBUG1, "RESPONSE_COMPLETE_3 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
#endif
                return RESPONSE_COMPLETE;

            default:
                /* sync lost? */
                elog(WARNING, "Received unsupported message type: %c", msg_type);
                PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
                /* stop reading */
                elog(DEBUG1, "RESPONSE_COMPLETE_4 from node %s, remote pid %d", conn->nodename, conn->backend_pid);
                return RESPONSE_COMPLETE;
        }
    }
    /* never happen, but keep compiler quiet */
    return RESPONSE_EOF;
}


/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
bool
validate_combiner(ResponseCombiner *combiner)
{// #lizard forgives
    /* There was error message while combining */
    if (combiner->errorMessage)
    {
        elog(DEBUG1, "validate_combiner there is errorMessage in combiner");
        return false;
    }
    /* Check if state is defined */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
    {
        elog(DEBUG1, "validate_combiner request_type is REQUEST_TYPE_NOT_DEFINED");
        return false;
    }

    /* Check all nodes completed */
    if ((combiner->request_type == REQUEST_TYPE_COMMAND
            || combiner->request_type == REQUEST_TYPE_QUERY)
            && combiner->command_complete_count != combiner->node_count)
    {
        elog(DEBUG1, "validate_combiner request_type is %d, command_complete_count:%d not equal node_count:%d", combiner->request_type, combiner->command_complete_count, combiner->node_count);
        return false;
    }

    /* Check count of description responses */
    if (combiner->request_type == REQUEST_TYPE_QUERY && combiner->description_count != combiner->node_count)
    {
        elog(DEBUG1, "validate_combiner request_type is REQUEST_TYPE_QUERY, description_count:%d not equal node_count:%d", combiner->description_count, combiner->node_count);
        return false;
    }

    /* Check count of copy-in responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_IN
            && combiner->copy_in_count != combiner->node_count)
    {
        elog(DEBUG1, "validate_combiner request_type is REQUEST_TYPE_COPY_IN, copy_in_count:%d not equal node_count:%d", combiner->copy_in_count, combiner->node_count);
        return false;
    }

    /* Check count of copy-out responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_OUT
            && combiner->copy_out_count != combiner->node_count)
    {
        elog(DEBUG1, "validate_combiner request_type is REQUEST_TYPE_COPY_OUT, copy_out_count:%d not equal node_count:%d", combiner->copy_out_count, combiner->node_count);
        return false;
    }

    /* Add other checks here as needed */

    /* All is good if we are here */
    return true;
}
/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(ResponseCombiner *combiner)
{// #lizard forgives

    if (combiner->tuple_desc)
    {
        FreeTupleDesc(combiner->tuple_desc);
        combiner->tuple_desc = NULL;
    }
    if (combiner->errorMessage)
    {
        pfree(combiner->errorMessage);
        combiner->errorMessage = NULL;
    }
    if (combiner->errorDetail)
    {
        pfree(combiner->errorDetail);
        combiner->errorDetail = NULL;
    }
    if (combiner->errorHint)
    {
        pfree(combiner->errorHint);
        combiner->errorHint = NULL;
    }
    if (combiner->cursor_connections)
    {
        pfree(combiner->cursor_connections);
        combiner->cursor_connections = NULL;
    }
    if (combiner->tapenodes)
    {
        pfree(combiner->tapenodes);
        combiner->tapenodes = NULL;
    }
    if (combiner->tapemarks)
    {
        pfree(combiner->tapemarks);
        combiner->tapemarks = NULL;
    }
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
bool
ValidateAndCloseCombiner(ResponseCombiner *combiner)
{
    bool        valid = validate_combiner(combiner);

    CloseCombiner(combiner);

    return valid;
}

/*
 * We do not want it too long, when query is terminating abnormally we just
 * want to read in already available data, if datanode connection will reach a
 * consistent state after that, we will go normal clean up procedure: send down
 * ABORT etc., if data node is not responding we will signal pooler to drop
 * the connection.
 * It is better to drop and recreate datanode connection then wait for several
 * seconds while it being cleaned up when, for example, cancelling query.
 */
#define END_QUERY_TIMEOUT    1000
/*
 * Clean up and discard any data on the data node connections that might not
 * handled yet, including pending on the remote connection.
 */
void
pgxc_connections_cleanup(ResponseCombiner *combiner)
{// #lizard forgives
    int32    ret     = 0;
    struct timeval timeout;
    int  i = 0;
    timeout.tv_sec  = END_QUERY_TIMEOUT / 1000;
    timeout.tv_usec = (END_QUERY_TIMEOUT % 1000) * 1000;

    if (combiner->dataRowBuffer)
    {
	if(combiner->merge_sort)
	{
	    for (i = 0; i < combiner->conn_count; i++)
	    {
		    if (combiner->dataRowBuffer[i])
		    {
			    datarowstore_end(combiner->dataRowBuffer[i]);
			    combiner->dataRowBuffer[i] = NULL;
		    }
	    }
	}
	else if(combiner->dataRowBuffer[0])
	{
		datarowstore_end(combiner->dataRowBuffer[0]);
		combiner->dataRowBuffer[0] = NULL;
	}
        pfree(combiner->dataRowBuffer);
        combiner->dataRowBuffer = NULL;
    }

    /*
     * Read in and discard remaining data from the connections, if any
     */
    combiner->current_conn = 0;
    /*
     * Read in and discard remaining data from the connections, if any
     */
    for(i = 0; i< combiner->conn_count; i++)
    {
	    if(combiner->connections[i] && combiner->connections[i]->needSync)
	    {
		    if (pgxc_node_send_sync(combiner->connections[i]))
		    {
			    ereport(LOG,
					    (errcode(ERRCODE_INTERNAL_ERROR),
					     errmsg("Failed to sync msg to node:%s pid:%d when clean", combiner->connections[i]->nodename, combiner->connections[i]->backend_pid)));
		    }
	    }
    }
    while (combiner->conn_count > 0)
    {
        int res;
        PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

        /*
         * Possible if we are doing merge sort.
         * We can do usual procedure and move connections around since we are
         * cleaning up and do not care what connection at what position
         */
        if (conn == NULL)
        {
            REMOVE_CURR_CONN(combiner);
            continue;
        }

        /*
         * Connection owner is different, so no our data pending at
         * the connection, nothing to read in.
         */
        if (conn->combiner && conn->combiner != combiner)
        {
            elog(DEBUG1, "pgxc_connections_cleanup is different, remove connection:%s", conn->nodename);
            REMOVE_CURR_CONN(combiner);
            continue;
        }

        /* throw away current message that may be in the buffer */
        if (combiner->currentRow)
        {
            pfree(combiner->currentRow);
            combiner->currentRow = NULL;
        }

        ret = pgxc_node_receive(1, &conn, &timeout);
        if (DNStatus_ERR == ret)
        {
            elog(DEBUG1, "Failed to read response from data node:%s pid:%d when ending query for ERROR, node status:%d",
                         conn->nodename, conn->backend_pid, conn->state);
        }

        res = handle_response(conn, combiner);
        if (RESPONSE_EOF == res)
        {
            /* Continue to consume the data, until all connections are abosulately done. */
            if (pgxc_node_is_data_enqueued(conn) && !proc_exit_inprogress)
            {
                DNConnectionState state = DN_CONNECTION_STATE_IDLE;
                state       = conn->state;
                conn->state = DN_CONNECTION_STATE_QUERY;

                ret = pgxc_node_receive(1, &conn, &timeout);
                if (DNStatus_ERR == ret)
                {
                    elog(DEBUG1, "Failed to read response from data node:%u when ending query for ERROR, state:%d",
                                    conn->nodeoid, conn->state);
                }

                if (DN_CONNECTION_STATE_IDLE == state)
                {
                    conn->state = state;
                }
                continue;
            }
            else if (!proc_exit_inprogress)
            {
                if (conn->state == DN_CONNECTION_STATE_QUERY)
                {
                    combiner->current_conn = (combiner->current_conn + 1) % combiner->conn_count;
                    continue;
                }
            }
        }

        if (RESPONSE_ERROR == res)
        {
            if (conn->needSync)
            {
                if (pgxc_node_send_sync(conn))
                {
                    ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));
                }

#ifdef _PG_REGRESS_
                ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Succeed to sync msg to node:%s pid:%d when clean", conn->nodename, conn->backend_pid)));
#endif
                conn->needSync = false;

            }
            continue;
        }

        /* no data is expected */
        if (conn->state == DN_CONNECTION_STATE_IDLE || conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
        {

#ifdef _PG_REGRESS_
            int32 nbytes = pgxc_node_is_data_enqueued(conn);
            if (nbytes && conn->state == DN_CONNECTION_STATE_IDLE)
            {

                ereport(LOG,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("node:%s pid:%d status:%d %d bytes dataleft over.", conn->nodename, conn->backend_pid, conn->state, nbytes)));

            }
#endif
            REMOVE_CURR_CONN(combiner);
            continue;
        }
       
    }


}


void
BufferConnection(PGXCNodeHandle *conn)
{
    ResponseCombiner *combiner = conn->combiner;
    int node_index = 0;
    int node_cnt = 1;
    int res;
    MemoryContext oldcontext;

    if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
    {
        return;
    }

    elog(DEBUG2, "Buffer connection %u to step %s", conn->nodeoid, combiner->cursor);
    /*
     * When BufferConnection is invoked CurrentContext is related to other
     * portal, which is trying to control the connection.
     * TODO See if we can find better context to switch to
     */
    oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);

    /* Verify the connection is in use by the combiner */
    combiner->current_conn = 0;
    while (combiner->current_conn < combiner->conn_count)
    {
        if (combiner->connections[combiner->current_conn] == conn)
            break;
        combiner->current_conn++;
    }
    Assert(combiner->current_conn < combiner->conn_count);
    if(combiner->merge_sort)
    {
	    node_index = combiner->current_conn;
	    node_cnt = combiner->conn_count;
    }

    if (!combiner->dataRowBuffer)
    {
        combiner->dataRowBuffer = (Datarowstorestate **)palloc0(sizeof(Datarowstorestate *) * node_cnt);
    }

    if (!combiner->nDataRows)
    {
        combiner->nDataRows = (int *)palloc0(sizeof(int) * node_cnt);
    }

    if (!combiner->dataRowBuffer[node_index])
    {
        combiner->dataRowBuffer[node_index] = datarowstore_begin_datarow(false, work_mem);
    }

    if (!combiner->tmpslot)
    {
        TupleDesc desc = CreateTemplateTupleDesc(1, false);
        combiner->tmpslot = MakeSingleTupleTableSlot(desc);
    }

    while (true)
    {
        res = handle_response(conn, combiner);
        /*
         * If response message is a DataRow it will be handled on the next
         * iteration.
         * PortalSuspended will cause connection state change and break the loop
         * The same is for CommandComplete, but we need additional handling -
         * remove connection from the list of active connections.
         * We may need to add handling error response
         */

        /* Most often result check first */
        if (res == RESPONSE_DATAROW)
        {
            datarowstore_putdatarow(combiner->dataRowBuffer[node_index],
                                        combiner->currentRow);
            pfree(combiner->currentRow);
            combiner->currentRow = NULL;
            combiner->nDataRows[node_index]++;
            continue;
        }

        /* incomplete message, read more */
        if (res == RESPONSE_EOF)
        {
            if (pgxc_node_receive(1, &conn, NULL))
            {
                PGXCNodeSetConnectionState(conn,
                        DN_CONNECTION_STATE_ERROR_FATAL);
                add_error_message(conn, "Failed to fetch from data node");
            }
        }

        /*
         * End of result set is reached, so either set the pointer to the
         * connection to NULL (combiner with sort) or remove it from the list
         * (combiner without sort)
         */
        else if (res == RESPONSE_COMPLETE)
        {

            /*
             * If combiner is doing merge sort we should set reference to the
             * current connection to NULL in the array, indicating the end
             * of the tape is reached. FetchTuple will try to access the buffer
             * first anyway.
             * Since we remove that reference we can not determine what node
             * number was this connection, but we need this info to find proper
             * tuple in the buffer if we are doing merge sort. So store node
             * number in special array.
             * NB: We can not test if combiner->tuplesortstate is set here:
             * connection may require buffering inside tuplesort_begin_merge
             * - while pre-read rows from the tapes, one of the tapes may be
             * the local connection with RemoteSubplan in the tree. The
             * combiner->tuplesortstate is set only after tuplesort_begin_merge
             * returns.
	     */
		if(combiner->merge_sort)
		{
			combiner->connections[combiner->current_conn] = NULL;
		}
		else
			REMOVE_CURR_CONN(combiner);

            /*
             * If combiner runs Simple Query Protocol we need to read in
             * ReadyForQuery. In case of Extended Query Protocol it is not
             * sent and we should quit.
             */
            if (combiner->extended_query)
                break;
            
            if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
                             conn->nodename, conn->backend_pid)));
            }
        }
        else if (res == RESPONSE_ERROR)
        {
            if (combiner->extended_query)
            {
                /*
                 * Need to sync connection to enable receiving commands
                 * by the datanode
                 */
                if (pgxc_node_send_sync(conn) != 0)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Failed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));
                }
#ifdef _PG_REGRESS_
                ereport(LOG,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Succeed to sync msg to node %s backend_pid:%d", conn->nodename, conn->backend_pid)));                    
#endif

            }
        }
        else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
        {
            /* Now it is OK to quit */
            break;
        }

    }
    Assert(conn->state != DN_CONNECTION_STATE_QUERY);
    MemoryContextSwitchTo(oldcontext);
    conn->combiner = NULL;
}

static bool FetchFromTupleStore(ResponseCombiner *combiner)
{
    int node_index = 0;

    if (combiner->merge_sort)
        node_index = combiner->current_conn;

    if (!combiner->dataRowBuffer || 
        !combiner->dataRowBuffer[node_index])
    {
        return false;
    }
  
  
    if (datarowstore_getdatarow(combiner->dataRowBuffer[node_index], 
                                true, true, &combiner->currentRow))
    {
        combiner->nDataRows[node_index]--;
    }
    else
    {
        /* sanity check */
        if (combiner->nDataRows[node_index] != 0)
        {
            elog(ERROR, "connection %d has %d datarows left in tuplestore.", 
                            node_index, combiner->nDataRows[node_index]);
        }

        /* 
            * datarows fetched from tuplestore in memory will be freed by caller, 
            * we do not need to free them in datarowstore_end, 
            * avoid to free memtuples in datarowstore_end.
            */
        datarowstore_end(combiner->dataRowBuffer[node_index]);
        combiner->dataRowBuffer[node_index] = NULL;
        combiner->currentRow = NULL;
    }
    
    return true;
}
/*
 *FetchDatarow 
 *
        Get next tuple from one of the datanode connections.
 * The connections should be in combiner->connections, if "local" dummy
 * connection presents it should be the last active connection in the array.
 *      If combiner is set up to perform merge sort function returns tuple from
 * connection defined by combiner->current_conn, or NULL slot if no more tuple
 * are available from the connection. Otherwise it returns tuple from any
 * connection or NULL slot if no more available connections.
 *         Function looks into combiner->rowBuffer before accessing connection
 * and return a tuple from there if found.
 *         Function may wait while more data arrive from the data nodes. If there
 * is a locally executed subplan function advance it and buffer resulting rows
 * instead of waiting.
 */
RemoteDataRow
FetchDatarow(ResponseCombiner *combiner)
{// #lizard forgives
    PGXCNodeHandle *conn;

READ_NEXT_ROW:
    if (combiner->conn_count > combiner->current_conn)
        conn = combiner->connections[combiner->current_conn];
    else
        conn = NULL;

    /*
     * We first check the tuple store 
     */
    if (FetchFromTupleStore(combiner))
    {
        if (combiner->currentRow)
        {
            RemoteDataRow res_row = combiner->currentRow;
            combiner->currentRow = NULL;
            return res_row;
        }
    } 

    while (conn)
    {
        int res;

	/* Going to use a connection, buffer it if needed */
	if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL
			&& conn->combiner != combiner)
	{
		BufferConnection(conn);
		conn->combiner = combiner;
	}
        /*
         * If current connection is idle it means portal on the data node is
         * suspended. Request more and try to get it
         */
        if (combiner->extended_query &&
                conn->state == DN_CONNECTION_STATE_IDLE)
        {
            if (pgxc_node_send_execute(conn, combiner->cursor, 1000) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
            }

            if (pgxc_node_send_flush(conn) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
            }

            if (pgxc_node_receive(1, &conn, NULL))
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed receive data from node %u cursor '%s'", conn->nodeoid, combiner->cursor)));
            }
        }

        /* read messages */
        res = handle_response(conn, combiner);

        switch (res)
        {
            case RESPONSE_DATAROW:
                {
                    RemoteDataRow res_row = combiner->currentRow;
                    combiner->currentRow = NULL;
                    return res_row;
                }
            case RESPONSE_EOF:
                {
                    /* incomplete message, read more */
                    if (pgxc_node_receive(1, &conn, NULL))
                        ereport(ERROR,
                                (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
                    break;
                }
            case RESPONSE_COMPLETE:
                {
                    if (combiner->extended_query)
                    {
                        if(combiner->merge_sort)
                        {
                            combiner->connections[combiner->current_conn] = NULL;
                            conn = NULL;
                            if(combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn])
                                goto READ_NEXT_ROW;
                        }
                        else
                        {

                            REMOVE_CURR_CONN(combiner);
                            if (combiner->conn_count > 0)
                                conn = combiner->connections[combiner->current_conn];
                            else
                                goto READ_NEXT_ROW;
                        }
                        
                    }
                    break;
                }
            case RESPONSE_READY:
                {
                    if(combiner->merge_sort)
                    {
                        combiner->connections[combiner->current_conn] = NULL;
                        conn = NULL;
                        if(combiner->dataRowBuffer && combiner->dataRowBuffer[combiner->current_conn])
                            goto READ_NEXT_ROW;
                    }
                    else
                    {
                        REMOVE_CURR_CONN(combiner);
                        if (combiner->conn_count > 0)
                            conn = combiner->connections[combiner->current_conn];
                        else
                            goto READ_NEXT_ROW;
                    }

                    break;
                }
            case RESPONSE_SUSPENDED:
                {
                    break;
                }
            case RESPONSE_TUPDESC:
                    break;
            case RESPONSE_ERROR:
                {
                    /* print error message outside */
                    return NULL;
                }
            default:
                elog(ERROR, "unexpected result %d", res);
        }

    }

    return NULL;
}

/*
 * Handle responses from the Datanode connections
 */
int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
                         struct timeval * timeout, ResponseCombiner *combiner)
{// #lizard forgives
    bool        has_errmsg = false;
    int         connection_index;
    int            ret   = 0;
    int            count = conn_count;
    PGXCNodeHandle *to_receive[conn_count];
    int         func_ret = 0;
    int         last_fatal_conn_count = 0;
    int         receive_to_connections[conn_count];

    /* make a copy of the pointers to the connections */
    memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/* reset received timestamp */
	{
		int i = 0;
		for (; i < conn_count; i++) 
		{
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
			connections[i]->receivedTimestamp = 0;
#endif
		}
	}
    /*
     * Read results.
     * Note we try and read from Datanode connections even if there is an error on one,
     * so as to avoid reading incorrect results on the next statement.
     * Other safegaurds exist to avoid this, however.
     */
    while (count > 0)
    {
        int i = 0;
        ret = pgxc_node_receive(count, to_receive, timeout);
        if (DNStatus_ERR == ret)
        {
            int conn_loop;
            int rece_idx = 0;
            int fatal_conn_inner;
            has_errmsg = true;
            elog(DEBUG1, "pgxc_node_receive_responses pgxc_node_receive data from node number:%d failed", count);
            /* mark has error */
            func_ret = -1;
            memset((char*)to_receive, 0, conn_count*sizeof(PGXCNodeHandle *));

            fatal_conn_inner = 0;
            for(conn_loop = 0; conn_loop < conn_count;conn_loop++)
            {
                receive_to_connections[conn_loop] = conn_loop;
                if (connections[conn_loop]->state != DN_CONNECTION_STATE_ERROR_FATAL)
                {
                    to_receive[rece_idx] = connections[conn_loop];
                    receive_to_connections[rece_idx] = conn_loop;
                    rece_idx++;
                }
                else
                {
                    fatal_conn_inner++;
                }
            }

            if (last_fatal_conn_count == fatal_conn_inner)
            {
                /*
                 *if exit abnormally reset response_operation for next call
                 */
                g_coord_twophase_state.response_operation = OTHER_OPERATIONS;
                return EOF;
            }
            last_fatal_conn_count = fatal_conn_inner;

            count = rece_idx;
        }

        while (i < count)
        {
            int32 nbytes = 0;
            int result =  handle_response(to_receive[i], combiner);
#ifdef     _PG_REGRESS_
            elog(DEBUG1, "Received response %d on connection to node %s",
                    result, to_receive[i]->nodename);
#else
            elog(DEBUG5, "Received response %d on connection to node %s",
                    result, to_receive[i]->nodename);
#endif
            switch (result)
            {
                case RESPONSE_EOF: /* have something to read, keep receiving */
                    i++;
                    break;
                case RESPONSE_COMPLETE:
                    if (to_receive[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
                    {
                        /* Continue read until ReadyForQuery */
                        break;
                    }
                    else
                    {
                        /* error occurred, set buffer logically empty */
                        to_receive[i]->inStart = 0;
                        to_receive[i]->inCursor = 0;
                        to_receive[i]->inEnd = 0;
                    }
                    /* fallthru */
                case RESPONSE_READY:
                    /* fallthru */
                    if (!has_errmsg)
                    {
                        connection_index = i;
                    }
                    else
                    {
                        connection_index = receive_to_connections[i];
                    }
                    UpdateLocalCoordTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
                case RESPONSE_COPY:
                    /* try to read every byte from peer. */
                    nbytes = pgxc_node_is_data_enqueued(to_receive[i]);
                    if (nbytes)
                    {
                        int32               ret    = 0;
                        DNConnectionState estate =  DN_CONNECTION_STATE_IDLE;
                        /* Have data in buffer, try to receive and retry. */
                        elog(DEBUG1, "Pending response %d bytes on connection to node %s, pid %d try to read again. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid);
                        estate                 = to_receive[i]->state;
                        to_receive[i]->state = DN_CONNECTION_STATE_QUERY;
                        ret = pgxc_node_receive(1, &to_receive[i], NULL);
                        if (ret)
                        {
                            switch (ret)
                            {
                                case DNStatus_ERR:
                                    {
                                        elog(DEBUG1, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for ERROR:%s. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid, strerror(errno));
                                        break;
                                    }

                                case DNStatus_EXPIRED:
                                    {
                                        elog(DEBUG1, "pgxc_node_receive Pending data %d bytes from node:%s pid:%d failed for EXPIRED. ", nbytes, to_receive[i]->nodename, to_receive[i]->backend_pid);
                                        break;
                                    }
                                default:
                                    {
                                        /* Can not be here.*/
                                        break;
                                    }
                            }
                        }
                        to_receive[i]->state = estate;
                        i++;
                        break;
                    }

                    /* Handling is done, do not track this connection */
                    count--;
                    /* Move last connection in place */
                    if (i < count)
                        to_receive[i] = to_receive[count];
                    break;
                case RESPONSE_ERROR:
                    /* no handling needed, just wait for ReadyForQuery */
                    if (!has_errmsg)
                    {
                        connection_index = i;
                    }
                    else
                    {
                        connection_index = receive_to_connections[i];
                    }
                    UpdateLocalCoordTwoPhaseState(result, to_receive[i], connection_index, combiner->errorMessage);
                    break;

                case RESPONSE_WAITXIDS:
                case RESPONSE_ASSIGN_GXID:
                case RESPONSE_TUPDESC:
                    break;

                case RESPONSE_DATAROW:
                    combiner->currentRow = NULL;
                    break;

                default:
                    /* Inconsistent responses */
                    add_error_message(to_receive[i], "Unexpected response from the Datanodes");
                    elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
                    /* Stop tracking and move last connection in place */
                    count--;
                    if (i < count)
                        to_receive[i] = to_receive[count];
            }
        }
    }

    g_coord_twophase_state.response_operation = OTHER_OPERATIONS;
    return func_ret;
}
