/*-------------------------------------------------------------------------
 *
 * txninfo.h
 *	  Prepared transaction info
 *
 * Portions Copyright (c) 2012 Postgres-XC Development Group
 *
 * $Postgres-XC$
 *
 *-------------------------------------------------------------------------
 */

#ifndef TXNINFO_H
#define TXNINFO_H

#include <pthread.h>
#include "c.h"
#include "datatype/timestamp.h"

#define INVALID_INT_VALUE (-1)
#define ROLLBACK_ALL_PREPARED_TXN 0
#define COMMIT_ALL_PREPARED_TXN 1

typedef enum TXN_STATUS
{
	TXN_STATUS_INITIAL = 0,	/* Initial */
	TXN_STATUS_UNKNOWN,		/* Unknown: Frozen, running, or not started */
	TXN_STATUS_PREPARED,
	TXN_STATUS_COMMITTED,
	TXN_STATUS_ABORTED,
	TXN_STATUS_INPROGRESS,
	TXN_STATUS_FAILED		/* Error detected while interacting with the node */
} TXN_STATUS;

typedef enum NODE_TYPE
{
	NODE_TYPE_COORD = 1,
	NODE_TYPE_DATANODE,
	NODE_TYPE_GTM
} NODE_TYPE;


typedef struct node_info
{
	char	*node_name;
	int		port;
	char	*host;
	NODE_TYPE type;
	int		nodeid;
} node_info;

extern node_info *pgxc_clean_node_info;
extern int pgxc_clean_node_count;

typedef struct txn_info
{
	struct txn_info *next;
	char            *gid;       /* gid string used in prepare */
    TransactionId	*xid;       /* local xid, Array for each nodes */
	char			*owner;
	char			*origcoord;	/* Original coordinator who initiated the txn */
	bool			isorigcoord_part;	/* Is original coordinator a
										   participant? */
	int				num_dnparts;		/* Number of participant datanodes */
	int				num_coordparts;		/* Number of participant coordinators */
	int				*dnparts;	/* Whether a node was participant in the txn */
	int				*coordparts;
	TXN_STATUS		*txn_stat;	/* Array for each nodes */
	char			*msg;		/* Notice message for this txn. */
	char            *participate_nodes;
	int 			*nodeparts; /* Whether a node was participant in the txn */
    int		 prepare_timestamp_elapse; // current time - prepared time. unit: s
    TimestampTz		commit_timestamp;
} txn_info;

typedef struct database_info
{
	struct database_info *next;
	char *database_name;
	txn_info *head_txn_info;
	txn_info *last_txn_info;
} database_info;

extern database_info *head_database_info;
extern database_info *last_database_info;

/* Functions */
#if 0
extern txn_info *init_txn_info(char *database_name, TransactionId gxid);
#endif
extern int add_txn_info(char *database, char *node, char* gid, TransactionId xid, char *owner, char* participate_nodes,
								   int	prepared_time_elapse, TXN_STATUS status);
extern txn_info *find_txn_info(TransactionId gxid);

extern database_info *find_database_info(char *database_name);
extern database_info *add_database_info(char *database_name);
extern node_info *find_node_info(char *node_name);
extern node_info *find_node_info_by_nodeid(int nodeid);
extern int find_node_index(char *node_name);
extern int find_node_index_by_nodeid(int nodeid);
extern int set_node_info(char *node_name, int port, char *host, NODE_TYPE type,
		int nodeid, int index);
extern TXN_STATUS check_txn_global_status(txn_info *txn);
extern bool check2PCExists(void);
extern char *str_txn_stat(TXN_STATUS status);

#endif   /* TXNINFO_H */
