#include "txninfo.h"
/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 2011-2012 Postgres-XC Development Group
 *
 */
static txn_info *find_txn(char* gid);
static txn_info *make_txn_info(char *dbname, char* gid, TransactionId xid, char *owner, char* participate_nodes);

#define XIDPREFIX "_$XC$"

database_info *find_database_info(char *database_name)
{
	database_info *cur_database_info = head_database_info;

	for (;cur_database_info; cur_database_info = cur_database_info->next)
	{
		if(strcmp(cur_database_info->database_name, database_name) == 0)
			return(cur_database_info);
	}
	return(NULL);
}

database_info *add_database_info(char *database_name)
{
	database_info *rv;

	if ((rv = find_database_info(database_name)) != NULL)
		return rv;		/* Already in the list */
	rv = malloc(sizeof(database_info));
	if (rv == NULL)
		return NULL;
	rv->next = NULL;
	rv->database_name = strdup(database_name);
	if (rv->database_name == NULL)
	{
		free(rv);
		return NULL;
	}
	rv->head_txn_info = NULL;
	rv->last_txn_info = NULL;
	if (head_database_info == NULL)
	{
		head_database_info = last_database_info = rv;
		return rv;
	}
	else
	{
		last_database_info->next = rv;
		last_database_info = rv;
		return rv;
	}
}

int set_node_info(char *node_name, int port, char *host, NODE_TYPE type,
		int nodeid, int index)
{
	node_info *cur_node_info;

	if (index >= pgxc_clean_node_count)
		return -1;
	cur_node_info = &pgxc_clean_node_info[index];
	if (cur_node_info->node_name)
		free(cur_node_info->node_name);
	if (cur_node_info->host)
		free(cur_node_info->host);
	cur_node_info->node_name = strdup(node_name);
	if (cur_node_info->node_name == NULL)
		return -1;
	cur_node_info->port = port;
	cur_node_info->host = strdup(host);
	if (cur_node_info->host == NULL)
		return -1;
	cur_node_info->type = type;
	cur_node_info->nodeid = nodeid;
	return 0;
}

node_info *find_node_info(char *node_name)
{
	int i;
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (pgxc_clean_node_info[i].node_name == NULL)
			continue;
		if (strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0)
			return &pgxc_clean_node_info[i];
	}
	return(NULL);
}

node_info *find_node_info_by_nodeid(int nodeid)
{
	int i;
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (pgxc_clean_node_info[i].nodeid == nodeid)
			return &pgxc_clean_node_info[i];
	}
	return(NULL);
}

int find_node_index(char *node_name)
{
	int i;
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (pgxc_clean_node_info[i].node_name == NULL)
			continue;
		if (strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0)
			return i;
	}
	return  -1;
}

int find_node_index_by_nodeid(int nodeid)
{
	int i;
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (pgxc_clean_node_info[i].nodeid == nodeid)
			return i;
	}
	return  -1;
}

int add_txn_info(char *database, char *node, char* gid, TransactionId xid, char *owner, char* participate_nodes,
					 int   prepared_time_elapse, TXN_STATUS status)
{
	txn_info *txn;
	int	nodeidx;

	if ((txn = find_txn(gid)) == NULL)
	{
		txn = make_txn_info(database, gid, xid, owner, participate_nodes);
		if (txn == NULL)
		{
			fprintf(stderr, "No more memory.\n");
			exit(1);
		}
	}
	nodeidx = find_node_index(node);
	txn->txn_stat[nodeidx] = status;
	txn->xid[nodeidx] = xid;
	txn->nodeparts[nodeidx] = 1;

	// cross check for participate_nodes
	if (participate_nodes && strcmp(txn->participate_nodes, participate_nodes) != 0)
    {
		fprintf(stderr, "ERROR: database:%s, node:%s, gid:%s, xid:%d, get unmatch txn->participate_nodes:%s to participate_nodes:%s",
			database, node, gid, xid, txn->participate_nodes, participate_nodes);
		exit(1);
	}
	if (participate_nodes && strstr(participate_nodes, node) == NULL)
    {
        fprintf(stderr, "ERROR: database:%s, node:%s, gid:%s, xid:%d, get unmatch txn->participate_nodes:%s",
                database, node, gid, xid, txn->participate_nodes);
		exit(1);
	}

	// use the min elapse as result.
	if (txn->prepare_timestamp_elapse > 0) // if nowtime is before preparedtime a little bit, txn->prepare_timestamp_elapse will be -1
	{
		txn->prepare_timestamp_elapse = Min(txn->prepare_timestamp_elapse, prepared_time_elapse);
	}
	else
	{
		txn->prepare_timestamp_elapse = prepared_time_elapse;
	}

	fprintf(stdout, "database:%s node:%s gid:%s xid:%d prepare_timestamp_elapse:%d, commit_timestamp:" INT64_FORMAT"\n",
			database, node, gid, xid, txn->prepare_timestamp_elapse, txn->commit_timestamp);

	return 1;
}


static txn_info *
make_txn_info(char *dbname, char* gid, TransactionId xid, char *owner, char* participate_nodes)
{
	database_info *dbinfo;
	txn_info *txn;

	if ((dbinfo = find_database_info(dbname)) == NULL)
		dbinfo = add_database_info(dbname);
	txn = (txn_info *)malloc(sizeof(txn_info));
	if (txn == NULL)
		return NULL;
	memset(txn, 0, sizeof(txn_info));
	txn->gid = strdup(gid);
	txn->owner = strdup(owner);
	if (participate_nodes)
	    txn->participate_nodes = strdup(participate_nodes);
	txn->commit_timestamp = InvalidGlobalTimestamp;

	txn->xid = (TransactionId *)malloc(sizeof(TransactionId) * pgxc_clean_node_count);
	txn->txn_stat = (TXN_STATUS *)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->nodeparts = (int*) malloc (sizeof(int) * pgxc_clean_node_count);


	if (txn->gid == NULL || txn->owner == NULL || txn->participate_nodes == NULL ||
		txn->txn_stat == NULL || txn->xid == NULL || txn->txn_stat == NULL || txn->nodeparts == NULL)
	{
		if (txn->gid)
			free(txn->gid);
		if (txn->owner)
			free(txn->owner);
		if (txn->participate_nodes)
			free(txn->participate_nodes);
		if (txn->xid)
			free(txn->xid);
		if (txn->txn_stat)
			free(txn->txn_stat);
		if (txn->nodeparts)
			free(txn->txn_stat);

		free(txn);
		return(NULL);
	}

	memset(txn->xid, 0, sizeof(TransactionId) * pgxc_clean_node_count);
	memset(txn->txn_stat, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);
	memset(txn->nodeparts, 0, sizeof(int) * pgxc_clean_node_count);

	if (dbinfo->head_txn_info == NULL)
	{
		dbinfo->head_txn_info = dbinfo->last_txn_info = txn;
	}
	else
	{
		dbinfo->last_txn_info->next = txn;
		dbinfo->last_txn_info = txn;
	}

	return txn;
}

#if 0
/* Ugly ---> Remove this */
txn_info *init_txn_info(char *database_name, TransactionId gxid)
{
	database_info *database;
	txn_info *cur_txn_info;

	if ((database = find_database_info(database_name)) == NULL)
		return NULL;

	if (database->head_txn_info == NULL)
	{
		database->head_txn_info = database->last_txn_info = (txn_info *)malloc(sizeof(txn_info));
		if (database->head_txn_info == NULL)
			return NULL;
		memset(database->head_txn_info, 0, sizeof(txn_info));
		return database->head_txn_info;
	}
	for(cur_txn_info = database->head_txn_info; cur_txn_info; cur_txn_info = cur_txn_info->next)
	{
		if (cur_txn_info->gid == gxid)
			return(cur_txn_info);
	}
	cur_txn_info->next = database->last_txn_info = (txn_info *)malloc(sizeof(txn_info));
	if (cur_txn_info->next == NULL)
		return(NULL);
	memset(cur_txn_info->next, 0, sizeof(txn_info));
	if ((cur_txn_info->next->txn_stat = (TXN_STATUS *)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count)) == NULL)
		return(NULL);
	memset(cur_txn_info->next->txn_stat, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);
	return cur_txn_info->next;
}
#endif

static txn_info *find_txn(char* gid)
{
	database_info *cur_db;
	txn_info *cur_txn;

	for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
	{
		for (cur_txn = cur_db->head_txn_info; cur_txn; cur_txn = cur_txn->next)
		{
			if (strcmp(cur_txn->gid, gid) == 0)
				return cur_txn;
		}
	}
	return NULL;
}

TXN_STATUS check_txn_global_status(txn_info *txn)
{
#define TXN_PREPARED 	0x0001
#define TXN_COMMITTED 	0x0002
#define TXN_ABORTED		0x0004
#define TXN_INPROGRESS	0x0008

	int check_flag = 0;
	int nodeindx;

	if (txn == NULL)
		return TXN_STATUS_INITIAL;

	for (nodeindx = 0; nodeindx < pgxc_clean_node_count; nodeindx++)
    {
		if (txn->nodeparts[nodeindx] == 0)
		{
			fprintf(stdout, "Txn gid:%s, node:%s is not participated.\n", txn->gid, pgxc_clean_node_info[nodeindx].node_name);
			continue;
		}

        if (txn->txn_stat[nodeindx] == TXN_STATUS_INITIAL ||
            txn->txn_stat[nodeindx] == TXN_STATUS_UNKNOWN)
		{
			fprintf(stderr, "Txn gid:%s has invalid txn_stat:%s on node:%s\n", txn->gid, str_txn_stat(txn->txn_stat[nodeindx]), pgxc_clean_node_info[nodeindx].node_name);
			exit(-1);
			check_flag |= TXN_ABORTED;
		}
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_PREPARED)
            check_flag |= TXN_PREPARED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_COMMITTED)
            check_flag |= TXN_COMMITTED;
        else if (txn->txn_stat[nodeindx] == TXN_STATUS_ABORTED)
            check_flag |= TXN_ABORTED;
		else if (txn->txn_stat[nodeindx] == TXN_STATUS_INPROGRESS)
			check_flag |= TXN_INPROGRESS;
        else
		{
			fprintf(stderr, "Txn gid:%s has invalid txn_stat:%s on node:%s\n", txn->gid, str_txn_stat(txn->txn_stat[nodeindx]), pgxc_clean_node_info[nodeindx].node_name);
			exit(-1);
			return TXN_STATUS_FAILED;
		}

	}

    if ((check_flag & TXN_PREPARED) == 0)
	{
		fprintf(stderr, "FATAL: Txn gid:%s has no prepared txn on all node, at least on node is expected to prepared.\n", txn->gid);
		exit(-1);
		/* Should be at least one "prepared statement" in nodes */
		return TXN_STATUS_FAILED;
	}

    if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
	{
		fprintf(stderr, "FATAL: Txn gid:%s has both committed and aborted txn on all node.\n", txn->gid);
		exit(-1);
		/* Mix of committed and aborted. This should not happen. */
		return TXN_STATUS_FAILED;
	}

    if ((check_flag & TXN_COMMITTED))
    {
		// commit timestamp mustn't be null.
		if (InvalidGlobalTimestamp == txn->commit_timestamp)
        {
			fprintf(stderr, "Txn gid:%s doesn't have valid commit timestamp.\n", txn->gid);
			exit(1);
		}
        /* Some 2PC transactions are committed.  Need to commit others. */
        return TXN_STATUS_COMMITTED;
	}
    if (check_flag & TXN_ABORTED)
    {
        /* Some 2PC transactions are aborted.  Need to abort others. */
        return TXN_STATUS_ABORTED;
    }

	if (check_flag & TXN_INPROGRESS)
	{
		fprintf(stdout, "Txn gid:%s doesn't have committed or aborted xact on all nodes. Can't decide next action.\n", txn->gid);
		return TXN_STATUS_INPROGRESS;
	}

	if ((check_flag & (~TXN_PREPARED)) == 0x0)
	{
		/* All 2PC transactions are prepared. Abort all. */
		fprintf(stdout, "Txn gid:%s has prepared xacts on all participate nodes, we will abort. commit timestamp:" INT64_FORMAT"\n",
				txn->gid, txn->commit_timestamp);
		return TXN_STATUS_ABORTED;
	}

	fprintf(stderr, "Txn gid:%s can't decide which action.\n", txn->gid);
	exit(-1);
	return TXN_STATUS_UNKNOWN;

}

bool check2PCExists(void)
{
	database_info *cur_db;

	for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
	{
		txn_info *cur_txn;

		for (cur_txn = cur_db->head_txn_info; cur_txn; cur_txn = cur_txn->next)
		{
			return (true);
		}
	}
	return (false);
}

char *str_txn_stat(TXN_STATUS status)
{
	switch(status)
	{
		case TXN_STATUS_INITIAL:
			return("initial");
		case TXN_STATUS_UNKNOWN:
			return("unknown");
		case TXN_STATUS_PREPARED:
			return("prepared");
		case TXN_STATUS_COMMITTED:
			return("committed");
		case TXN_STATUS_ABORTED:
			return("aborted");
		case TXN_STATUS_FAILED:
			return("failed");
        case TXN_STATUS_INPROGRESS:
            return("inprogress");
		default:
			return("undefined status");
	}
	return("undefined status");
}
