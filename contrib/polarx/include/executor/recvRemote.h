/*-------------------------------------------------------------------------
 *
 * recvRemote.h
 *
 *      Utility functions to communicate to polarx Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/include/executor/recvRemote.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECVREMOTE_H
#define RECVREMOTE_H
#include "postgres.h"

#include <unistd.h>

#include "utils/timestamp.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include "utils/tuplestore.h"
#include "tcop/dest.h"
#include "nodes/execnodes.h"
#include "tcop/dest.h"
#include "pgxc/planner.h"
#include "pgxc/pgxcnode.h"

typedef enum
{
    REQUEST_TYPE_NOT_DEFINED,    /* not determined yet */
    REQUEST_TYPE_COMMAND,        /* OK or row count response */
    REQUEST_TYPE_QUERY,            /* Row description response */
    REQUEST_TYPE_COPY_IN,        /* Copy In response */
    REQUEST_TYPE_COPY_OUT,        /* Copy Out response */
    REQUEST_TYPE_ERROR            /* Error, ignore responses */
} RequestType;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum
{
    REMOTE_COPY_NONE,        /* Not defined yet */
    REMOTE_COPY_STDOUT,        /* Send back to client */
    REMOTE_COPY_FILE,        /* Write in file */
    REMOTE_COPY_TUPLESTORE    /* Store data in tuplestore */
} RemoteCopyType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
    CmdType cmdType;                        /* DML command type */
    char    data[COMPLETION_TAG_BUFSIZE];    /* execution result combination data */
} CombineTag;


/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5
#define RESPONSE_ERROR 6
#define RESPONSE_READY 10
#define RESPONSE_WAITXIDS 11
#define RESPONSE_ASSIGN_GXID 12
#define RESPONSE_TIMESTAMP 13
/*
 * Common part for all plan state nodes needed to access remote datanodes
 * ResponseCombiner must be the first field of the plan state node so we can
 * typecast
 */
typedef struct ResponseCombiner
{
    ScanState    ss;
    int            node_count;                /* total count of participating nodes */
    PGXCNodeHandle **connections;        /* Datanode connections being combined */
    int            conn_count;                /* count of active connections */
    int            current_conn;            /* used to balance load when reading from connections */
    long        current_conn_rows_consumed;
    CombineType combine_type;            /* see CombineType enum */
    int            command_complete_count; /* count of received CommandComplete messages */
    RequestType request_type;            /* see RequestType enum */
    TupleDesc    tuple_desc;                /* tuple descriptor to be referenced by emitted tuples */
    int            description_count;        /* count of received RowDescription messages */
    int            copy_in_count;            /* count of received CopyIn messages */
    int            copy_out_count;            /* count of received CopyOut messages */
    FILE       *copy_file;              /* used if copy_dest == COPY_FILE */
    uint64        processed;                /* count of data rows handled */
    char        errorCode[5];            /* error code to send back to client */
    char       *errorMessage;            /* error message to send back to client */
    char       *errorDetail;            /* error detail to send back to client */
    char       *errorHint;                /* error hint to send back to client */
    Oid            returning_node;            /* returning replicated node */
    RemoteDataRow currentRow;            /* next data ro to be wrapped into a tuple */
    /* TODO use a tuplestore as a rowbuffer */
    List        *rowBuffer;                /* buffer where rows are stored when connection
                                         * should be cleaned for reuse by other RemoteQuery */
    /*
     * To handle special case - if there is a simple sort and sort connection
     * is buffered. If EOF is reached on a connection it should be removed from
     * the array, but we need to know node number of the connection to find
     * messages in the buffer. So we store nodenum to that array if reach EOF
     * when buffering
     */
    Oid        *tapenodes;
    /*
     * If some tape (connection) is buffered, contains a reference on the cell
     * right before first row buffered from this tape, needed to speed up
     * access to the data
     */
    ListCell  **tapemarks;
    List            **prerowBuffers;    /*
                                          *used for each connection in prefetch with merge_sort,
                                          * put datarows in each rowbuffer in order
                                                                            */
    Tuplestorestate **dataRowBuffer;    /* used for prefetch */
    long             *dataRowMemSize;    /* size of datarow in memory */
    int             *nDataRows;         /* number of datarows in tuplestore */
    TupleTableSlot  *tmpslot;
    char*            errorNode;            /* node Oid, who raise an error, set when handle_response */
    int              backend_pid;        /* backend_pid, who raise an error, set when handle_response */
    bool             is_abort;
    bool        merge_sort;             /* perform mergesort of node tuples */
    bool        extended_query;         /* running extended query protocol */
    bool        probing_primary;        /* trying replicated on primary node */
    void       *tuplesortstate;            /* for merge sort */
    /* COPY support */
    RemoteCopyType remoteCopyType;
    Tuplestorestate *tuplestorestate;
    /* cursor support */
    char       *cursor;                    /* cursor name */
    char       *update_cursor;            /* throw this cursor current tuple can be updated */
    int            cursor_count;            /* total count of participating nodes */
    PGXCNodeHandle **cursor_connections;/* data node connections being combined */
    /* statistic information for debug */
    int         recv_node_count;       /* number of recv nodes */
    uint64      recv_tuples;           /* number of recv tuples */
    TimestampTz recv_total_time;       /* total time to recv tuples */
    /* used for remoteDML */
    int32      DML_processed;         /* count of DML data rows handled on remote nodes */
    PGXCNodeHandle **conns;
    int                 ccount;
    uint64     recv_datarows;
}    ResponseCombiner;

#define REMOVE_CURR_CONN(combiner) \
    if ((combiner)->current_conn < --((combiner)->conn_count)) \
    { \
        (combiner)->connections[(combiner)->current_conn] = \
                (combiner)->connections[(combiner)->conn_count]; \
    } \
    else \
        (combiner)->current_conn = 0

extern int handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner);

extern TupleDesc create_tuple_desc(char *msg_body, size_t len);
extern void InitResponseCombiner(ResponseCombiner *combiner, int node_count,
                       CombineType combine_type);
extern void CloseCombiner(ResponseCombiner *combiner);

extern void pgxc_node_report_error(ResponseCombiner *combiner);

extern bool validate_combiner(ResponseCombiner *combiner);

extern bool ValidateAndCloseCombiner(ResponseCombiner *combiner);

extern void pgxc_connections_cleanup(ResponseCombiner *combiner);

extern TupleTableSlot *FetchTuple(ResponseCombiner *combiner);

extern int pgxc_node_receive_responses(const int		 conn_count,
									   PGXCNodeHandle ** connections,
									   struct timeval *	 timeout,
									   ResponseCombiner *combiner);
#endif /* RECVREMOTE_H */
