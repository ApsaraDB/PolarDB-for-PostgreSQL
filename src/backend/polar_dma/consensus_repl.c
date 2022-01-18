/*-------------------------------------------------------------------------
 *
 * polar_dma.c
 *		Consensus Service for XLOG replication
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 * 	src/backend/polar_dma/consensus_repl.c
 *
 *-------------------------------------------------------------------------
 */

#include "easy_log.h"

#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <time.h>
#include <pthread.h>

#include "polar_consensus_c.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "common/md5.h"
#include "polar_datamax/polar_datamax.h"
#include "polar_dma/polar_dma.h"
#include "polar_dma/consensus_log.h"
#include "polar_dma/consensus_repl.h"
#include "libpq/pqsignal.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "funcapi.h"

#define CONSENSUS_TRANSFER_LEADER 	1
#define CONSENSUS_ADD_FOLLOWER    	2
#define CONSENSUS_DROP_FOLLOWER    	3
#define CONSENSUS_REQUEST_VOTE			4
#define CONSENSUS_ADD_LEARNER 			5
#define CONSENSUS_DROP_LEARNER 			6
#define CONSENSUS_CHANGE_LEARNER_TO_FOLLOWER 7
#define CONSENSUS_CHANGE_FOLLOWER_TO_LEARNER 8
#define CONSENSUS_CHANGE_WEIGHT_CONFIG 9
#define CONSENSUS_FORCE_SIGNLE_MODE 10
#define CONSENSUS_CHANGE_MATCH_INDEX 11
#define CONSENSUS_PURGE_LOGS 12
#define CONSENSUS_FORCE_PURGE_LOGS 13
#define CONSENSUS_CHANGE_CLUSTER_ID 14


#define CONSNESUS_TRANSIT_CHECK_INTERVAL 10000
#define HISTORIC_COMMIT_ADVANCE_INTERVAL 10000
#define XLOG_STREAMING_CHECK_INTERVAL 1000
#define CONSENSUS_COMMIT_TIMEOUT 5000
#define CONSENSUS_SYNC_TIMEOUT 5000
#define CONSENSUS_COMMAND_TIMEOUT 5000
#define CONSENSUS_STAT_FETCH_TIMEOUT 5000
#define CONSENSUS_CHECK_SHUTDOWN_INTERVAL 5000
#define CONSENSUS_ARCHIVE_ADVANCE_INTERVAL 5000

extern uint64 ConsensusGetCommitIndex(void);

/* GUC variables */
bool		polar_enable_dma = false;
bool		polar_dma_async_commit = false;

bool		polar_dma_force_change_meta = false;
bool		polar_dma_init_meta = false;
char		*polar_dma_members_info_string = NULL;
char		*polar_dma_learners_info_string = NULL;
char		*polar_dma_start_point_string = NULL;
int			polar_dma_cluster_id = 0;
uint64		polar_dma_current_term = 0;

char 		*polar_dma_repl_slot_name = NULL;
char 		*polar_dma_repl_app_name = NULL;
char 		*polar_dma_repl_user = "polardb";
char 		*polar_dma_repl_password = NULL;

int			polar_dma_port_deviation = 10000;

int			polar_dma_election_timeout = 5 * 1000;	/* 5s */
int			polar_dma_purge_timeout = 30 * 1000;	/* 30s */
int			polar_dma_io_thread_count = 4;
int			polar_dma_hb_thread_count = 0;
int 		polar_dma_worker_thread_count = 4;
int 		polar_dma_log_slots = 8192; /* 64M */
int 		polar_dma_log_keep_size_mb = 128; /* 128M */

bool		polar_dma_auto_leader_transfer = false;
int 		polar_dma_send_timeout = 0;
int 		polar_dma_new_follower_threshold = 10000;
bool		polar_dma_disable_election = false;
bool		polar_dma_auto_purge = true;
bool		polar_dma_delay_election = false;
int			polar_dma_delay_electionTimeout = 30 * 60 * 1000;	/* 30 mins */
int			polar_dma_xlog_check_timeout = 3;	/* 5 ms */
int 		polar_dma_max_packet_size = 128; /* kB */
int			polar_dma_pipeline_timeout = 3; /* 3 ms */
int			polar_dma_config_change_timeout = 60 * 1000;	/* 60 secs */
int 		polar_dma_max_delay_index = 50000;
int 		polar_dma_min_delay_index = 5000;
bool    polar_dma_time_statistics = true;

/* Consensus control data structure in the shared memory */
typedef struct ConsensusCtlData
{
	slock_t			mutex;
	pid_t				consensusPid;

	/*
	 * All subprocesses can wait on this queue.  Their waiting LSN
	 * (PGPROC->consensusInfo.waitLSN) are in increasing order. Protected by ConsensusLock.
	 */
	SHM_QUEUE		waitQueue;

	bool syncSuspending;
	XLogRecPtr	syncRqstLSN;
	pthread_mutex_t sync_cond_mutex;
	pthread_cond_t	sync_cond;

	/*
	 * The LSN that has been committed on the consensus log. checked while
	 * follower replaying WAL record and leader flushing WAL, Protected by
	 * cs_info_lck
	 */
	XLogRecPtr	syncedLSN;
	TimeLineID	syncedTLI;
	/*
	 * The LSN that can be purged on the consensus log. checked while
	 * deleting old xlog files, Protected by cs_info_lock
	 */
	XLogRecPtr	purgeLSN;
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	uint64 		commitIndex;
#else
	pg_atomic_uint64 commitIndex;
#endif
	pthread_rwlock_t cs_info_lck;

	/*
	 * current term of XLOG.
	 * Modified while Postmaster/StartupProcess changing state
	 * Protected by xlog_term_lck.
	 */
	uint64		xlogTerm;
	pthread_rwlock_t xlog_term_lck;

	/*
	 * local flushed LSN. modified by XLogFlush or Walreciever
	 * Protected by xlog_flush_lck.
	 */
	XLogRecPtr	flushedLSN;
	TimeLineID	flushedTLI;
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	uint64 			flushWaitTimeout;
#else
	pg_atomic_uint64 flushWaitTimeout;
#endif
	pthread_rwlock_t xlog_flush_lck;

	/*
	 * For views && commands
	 */
	pthread_mutex_t 	rqst_lck;
	pthread_mutex_t		rqstInstage;
	int		rqstType;
	char	nodeInfo[NI_MAXHOST+NI_MAXSERV];
	uint64	option;
	int		responce;
	pthread_mutex_t 	*wait_cond_mutex;
	pthread_cond_t		*wait_cond;
	bool	*finished;
	pthread_mutex_t 	rqst_cond_mutex;
	pthread_cond_t		rqst_cond;

	pthread_mutex_t stat_lck;
	SHM_QUEUE			statRqstQueue;
	uint64				statRqstFlag;
	pthread_rwlock_t stat_info_lck;
	ConsensusMemberInfo	 	memberInfo;
	ConsensusClusterInfo	clusterInfo[POLAR_DMA_MAX_NODES_NUM];
	ConsensusStatsInfo	 	statsInfo;
	int					numCluster;
	pthread_mutex_t 	stat_cond_mutex;
	pthread_cond_t		stat_cond;

	/*
	 * Acquire current_status_lock write lock by Consensus Process,
	 * and acquire read lock for status view by backend process
	 */
	uint64 currentStateTerm;
	uint64 currentLeaderId;
	uint64 currentLogTerm;
	bool inSmartShutdown;
	pthread_rwlock_t current_status_lock;

#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	/*
	 * Protect by current_status_lock
	 */
	XLogRecPtr currentAppendedLSN;
	ConsensusState  currentState;
#else
	pg_atomic_uint64 currentAppendedLSN;
	pg_atomic_uint32 currentState;
#endif

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	struct
	{
		pg_atomic_uint64 transit_waits;
		pg_atomic_uint64 xlog_transit_waits;
		pg_atomic_uint64 xlog_flush_waits;
		pg_atomic_uint64 consensus_appends;
		pg_atomic_uint64 consensus_wakeups;
		pg_atomic_uint64 consensus_backend_wakeups;
		pg_atomic_uint64 consensus_commit_waits;
		pg_atomic_uint64 consensus_commit_wait_time;
	} stats;
#endif
} ConsensusCtlData;

static ConsensusCtlData *ConsensusCtl = NULL;

static MemoryContext ConsensusContext = NULL;

static volatile sig_atomic_t shutdown_requested = false;

static CONSENSUS_HANDLE consensusHandle = INVALID_CONSENSUS_HANDLE;

static void ConsensusSigUsr1Handler(SIGNAL_ARGS);
static void ConsensusSigUsr2Handler(SIGNAL_ARGS);
static void ConsensusProcShutdownHandler(SIGNAL_ARGS);
static void ConsensusSigIntHandler(SIGNAL_ARGS);

static void consensus_startup(bool is_learner, bool is_logger, uint64_t mock_start_index);
static void consensus_mainloop(void);
static void consensus_quickdie(SIGNAL_ARGS);
static void consensus_on_exit(int code, Datum arg);
static bool consensus_try_synchronizing_flush_lsn(void);
static bool consensus_wait_for_next_entry(void);
static bool consensus_maybe_transit_state(char *leader_addr,
		char *leader_host, int *leader_port);
static void consensus_wakeup_backends(bool all);
static void* consensus_stat_thread(void *parm);
static void* consensus_command_thread(void *parm);
static void* consensus_append_log_thread(void *parm);
static void* consensus_advance_thread(void *parm);

static void consensus_set_flush_wait_timeout(uint64 wait_time);
static uint64 consensus_get_flush_wait_timeout(void);

static void ConsensusLockXLogTermShared(void);
static void ConsensusUnlockXLogTerm(void);

static void consensus_set_in_smart_shutdown(void);
static bool consensus_get_in_smart_shutdown(void);
static void consensus_set_state(uint32 state);
static int consensus_get_state(void);
static void consensus_set_appended_lsn(XLogRecPtr append_lsn);
static XLogRecPtr consensus_get_appended_lsn(void);

static bool consensus_wait_for_command(int cmd_type, void *args1, void *args2);

static int consensus_get_stat_collect_queue_length(void);
static int consensus_get_commit_wait_queue_length(void);
static void consensus_get_current_status(uint64 *term, uint64 *leader_id, uint64 *log_term);

static void
consensus_covert_start_pos(char *start_point_str,
		uint64 *start_index,
		XLogRecPtr *start_lsn,
		TimeLineID *start_tli)
{
	char	  *start_index_start;
	char	  *start_index_end;
	char	  *start_tli_start;
	char	  *start_tli_end;
	char	  *start_lsn_start;

	if (start_point_str == NULL || *start_point_str == '\0')
		return;

	/* parse start index */
	start_index_start = start_point_str;
	*start_index = pg_strtouint64(start_index_start, &start_index_end, 10);
	if (start_index_end == NULL || *start_index_end != ':')
		ereport(ERROR,
				(errmsg("invalid start index specified in start point address: %s",
						start_index_start)));

	/* parse start timeline */
	start_tli_start = ++start_index_end;
	*start_tli = strtol(start_tli_start, &start_tli_end, 10);
	if (start_tli_end == NULL || *start_tli_end != ':')
		ereport(ERROR,
				(errmsg("invalid start timeline specified in start point address: %s",
						start_index_start)));

	/* parse start lsn */
	start_lsn_start = ++start_tli_end;
	*start_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, 
				CStringGetDatum(start_lsn_start)));

	ereport(LOG,
			(errmsg("Consensus log start position: %s, start index: %lu, "
						"start lsn: %X/%X timeline %u",
					start_point_str, *start_index, 
					(uint32) ((*start_lsn) >> 32), (uint32) (*start_lsn),
					*start_tli)));
}

static void
consensus_covert_server_address(char *server_address_str, char *pg_ip, int *pg_port)
{
	char	  *colon = server_address_str;
	char	  *source_ip_start;
	char	  *source_ip_end;
	char	  *source_port_start;
	int			source_port;

	/* parse ip */
	source_ip_start = server_address_str;
	while (*colon != ':' && *colon != '\0')
		++colon;
	Assert (*colon != '\0');
	source_ip_end = colon++;

	Assert(source_ip_end - source_ip_start < NI_MAXHOST - 1);

	/* parse source port */
	source_port_start = colon;
	source_port = strtol(source_port_start, NULL, 10);
	Assert(source_port > polar_dma_port_deviation && source_port < 65535);

	memcpy(pg_ip, source_ip_start, source_ip_end - source_ip_start);
	pg_ip[source_ip_end - source_ip_start] = '\0';
	*pg_port = source_port - polar_dma_port_deviation;
}

/* process-safe, called by main thread or PG backends */
static void
consensus_check_and_convert_member_info(char *member_info,
																			  char **consensus_node_info)
{
	char	  *colon = member_info;
	char	  *ip_start;
	char	  *ip_end;
	char	  *pg_port_start;
	char	  *pg_port_end;
	int			pg_port;
	int			consensus_port;

	/* parse ip */
	ip_start = member_info;
	while (*colon != ':' && *colon != '\0')
		++colon;
	if (*colon == '\0')
	{
		ereport(ERROR,
				(errmsg("invalid server address string specified in server address: %s",
						member_info)));
	}
	ip_end = colon++;

	/* parse pg port */
	pg_port_start = colon;
	pg_port = strtol(pg_port_start, &pg_port_end, 10);

	consensus_port = pg_port + polar_dma_port_deviation;
	if (pg_port <= 0 || pg_port > 65535 || consensus_port > 65535)
	{
		ereport(ERROR,
				(errmsg("invalid port number specified in server address: %d, plus the "
								"client port obtained by adding %d to the consensus port.",
						pg_port, polar_dma_port_deviation)));
	}

	*ip_end = '\0';
	if (*pg_port_end == '\0')
	{
		*consensus_node_info = psprintf("%s:%d", ip_start, consensus_port);
	}
	else
	{
		*consensus_node_info = psprintf("%s:%d%s", ip_start, consensus_port, pg_port_end);
	}
	*ip_end = ':';
}

static char*
consensus_check_cluster_info(char *cluster_info)
{
	int 	num_servers = 0;
	char	*member_info;
	List	*consensus_nodes_list = NIL;
	char 	*consensus_cluster_info = NULL;
	char	*consensus_node_info;
	char 	*consensus_node_info_p;
	int		consensus_cluster_info_len = 0;
	char	*consensus_node_p = NULL;
	ListCell	*temp;

	Assert(CurrentMemoryContext == ConsensusContext);

	/* "127.0.0.1:10001#9F;0;127.0.0.1:10003#5N@1" */
	member_info = pstrdup(cluster_info);
	for (;;)
	{
		char		save_p;
		char		*p;

		p = member_info;
		while (*p != ';' && *p != '\0' && *p != '@')
			++p;
		save_p = *p;
		*p = '\0';

		/* null server */
		if (*member_info == '0' && strlen(member_info) == 1)
		{
			consensus_node_info = pstrdup(member_info);
			consensus_cluster_info_len += 1;
		}
		else
		{
			consensus_check_and_convert_member_info(member_info, &consensus_node_info);
		}

		consensus_nodes_list = lappend(consensus_nodes_list,
																		 (void *) consensus_node_info);
		++num_servers;

		if (save_p == '\0')
		{
			consensus_cluster_info_len += strlen(consensus_node_info);
			break;
		}
		else if (save_p == '@')
		{
			consensus_cluster_info_len += strlen(consensus_node_info);

			if (*(p+1) != '\0')
			{
				consensus_node_p = p + 1;
				consensus_cluster_info_len += 1 + strlen(p + 1);
			}

			break;
		}
		else
		{
			consensus_cluster_info_len += strlen(consensus_node_info) + 1;
			member_info = p + 1;
		}
	}

	consensus_cluster_info = palloc(consensus_cluster_info_len + 1);
	consensus_node_info_p = consensus_cluster_info;
	foreach(temp, consensus_nodes_list)
	{
		consensus_node_info = (char *)lfirst(temp);

		Assert(consensus_node_info_p - consensus_cluster_info <=
								consensus_cluster_info_len - strlen(consensus_node_info));

		strcpy(consensus_node_info_p, consensus_node_info);
		consensus_node_info_p += strlen(consensus_node_info);

		if (temp != list_tail(consensus_nodes_list))
		{
			Assert(consensus_node_info_p - consensus_cluster_info <=
												consensus_cluster_info_len - 1);

			*consensus_node_info_p = ';';
			consensus_node_info_p++;
		}
	}

	if (consensus_node_p != NULL)
	{
		Assert(consensus_node_info_p - consensus_cluster_info <=
								consensus_cluster_info_len - 1);

		*consensus_node_info_p = '@';
		consensus_node_info_p++;

		Assert(consensus_node_info_p - consensus_cluster_info <=
								consensus_cluster_info_len - strlen(consensus_node_p));

		strcpy(consensus_node_info_p, consensus_node_p);
		consensus_node_info_p += strlen(consensus_node_p);
	}
	*consensus_node_info_p = '\0';

	Assert(consensus_node_info_p - consensus_cluster_info == consensus_cluster_info_len);

	ereport(LOG,
			(errmsg("cluster_info: %s, node_info: %s",
									cluster_info, consensus_cluster_info)));

	return consensus_cluster_info;
}

static bool
consensus_get_leader_info(uint64 leader_id, char *leader_host, int *leader_port)
{
  char  *members_info;
  char  *member_info;
  int   len;
  int   server_id = 0;

  len = ConsensusMetaGetMemberInfo(&members_info);
	if (len == 0 || strrchr(members_info, '@') == NULL)
	{
		if (members_info)
			free(members_info);
		return false;
	}

  member_info = members_info;
  for (;;)
  {
    char    *p;

    p = member_info;
    while (*p != '\0' && *p != ';' && *p != '@')
      ++p;

    server_id++;
    if (server_id == leader_id)
    {
      char  save_p = *p;
      *p = '\0';
      consensus_covert_server_address(member_info, leader_host, leader_port);
      *p = save_p;
      break;
    }

    if (*p == '\0' || *p == '@')
      break;

    member_info = ++p;
  }

  free(members_info);

  return true;
}

void
ConsensusGetStatus(ConsensusStatus *consensus_status)
{
	consensus_status->current_state = (uint32)consensus_get_state();
	consensus_status->xlog_term = ConsensusGetXLogTerm();

	consensus_get_current_status(&consensus_status->term,
			&consensus_status->leader_id, &consensus_status->log_term);

	pthread_mutex_lock(&ConsensusCtl->sync_cond_mutex);
	consensus_status->sync_rqst_lsn = ConsensusCtl->syncRqstLSN ;
	pthread_mutex_unlock(&ConsensusCtl->sync_cond_mutex);

	consensus_status->appended_lsn = consensus_get_appended_lsn();
	ConsensusGetSyncedLSNAndTLI(&consensus_status->synced_lsn, 
			&consensus_status->synced_tli);
	consensus_status->purge_lsn = ConsensusGetPurgeLSN();
	consensus_status->commit_index = ConsensusGetCommitIndex();
	ConsensusGetXLogFlushedLSN(&consensus_status->flushed_lsn,
			&consensus_status->flushed_tli);

	consensus_status->commit_queue_length =
		  consensus_get_commit_wait_queue_length();

	consensus_status->stats_queue_length =
		  consensus_get_stat_collect_queue_length();
}

void
ConsensusGetStats(ConsensusStats *consensus_stats)
{
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	consensus_stats->transit_waits =
		pg_atomic_read_u64(&ConsensusCtl->stats.transit_waits);
	consensus_stats->xlog_transit_waits =
		pg_atomic_read_u64(&ConsensusCtl->stats.xlog_transit_waits);
	consensus_stats->xlog_flush_waits =
		pg_atomic_read_u64(&ConsensusCtl->stats.xlog_flush_waits);
	consensus_stats->consensus_appends =
		pg_atomic_read_u64(&ConsensusCtl->stats.consensus_appends);
	consensus_stats->consensus_wakeups =
		pg_atomic_read_u64(&ConsensusCtl->stats.consensus_wakeups);
	consensus_stats->consensus_backend_wakeups =
		pg_atomic_read_u64(&ConsensusCtl->stats.consensus_backend_wakeups);
	consensus_stats->consensus_commit_waits =
		pg_atomic_read_u64(&ConsensusCtl->stats.consensus_commit_waits);
	consensus_stats->consensus_commit_wait_time =
		pg_atomic_read_u64(&ConsensusCtl->stats.consensus_commit_wait_time);
#else
	memset(consensus_stats, 0, sizeof(ConsensusStats));
#endif
}

static void
consensus_stats_init(void)
{
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_init_u64(&ConsensusCtl->stats.transit_waits, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.xlog_transit_waits, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.xlog_flush_waits, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.consensus_appends, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.consensus_wakeups, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.consensus_backend_wakeups, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.consensus_commit_waits, 0);
	pg_atomic_init_u64(&ConsensusCtl->stats.consensus_commit_wait_time, 0);
#endif
}

static bool
consensus_meta_init(void)
{
	char	*consensus_meta_cluster_info = NULL;
	char	*consensus_cluster_info = NULL;
	char	*consensus_learner_info = NULL;
	uint64 start_index = 0;
	TimeLineID start_tli = 0;
	XLogRecPtr start_lsn = InvalidXLogRecPtr;
	bool 	is_learner = false;

	if (polar_dma_members_info_string)
	{
		if (*polar_dma_members_info_string != '\0')
			consensus_cluster_info = consensus_check_cluster_info(polar_dma_members_info_string);
		else
			consensus_cluster_info = pstrdup(polar_dma_members_info_string);
	}
	else if (polar_dma_init_meta)
	{
		consensus_cluster_info = pstrdup("");
	}

	if (polar_dma_learners_info_string)
	{
		if (*polar_dma_learners_info_string != '\0')
			consensus_learner_info = consensus_check_cluster_info(polar_dma_learners_info_string);
		else
			consensus_learner_info = pstrdup(polar_dma_learners_info_string);
	}
	else if (polar_dma_init_meta)
	{
		consensus_learner_info = pstrdup("");
	}

	if (polar_dma_start_point_string)
	{
		consensus_covert_start_pos(polar_dma_start_point_string,
				&start_index, &start_lsn, &start_tli);
	}

	if (polar_dma_cluster_id > 0)
		ConsensusMetaSetInt64(ClusterIdMetaKey, polar_dma_cluster_id, false);

	if ((consensus_cluster_info && strrchr(consensus_cluster_info, '@') == NULL) ||
			ConsensusMetaGetMemberInfo(&consensus_meta_cluster_info) == 0 ||
			strrchr(consensus_meta_cluster_info, '@') == NULL)
	{
		is_learner = true;
	}

	if (polar_dma_force_change_meta || polar_dma_init_meta)
	{
		ConsensusMetaForceChange(polar_dma_current_term,
				consensus_cluster_info,
				(consensus_cluster_info == NULL || *consensus_cluster_info == '\0') ?
					0 : strlen(consensus_cluster_info) + 1,
				consensus_learner_info,
				(consensus_learner_info == NULL || *consensus_learner_info == '\0') ?
					0 : strlen(consensus_learner_info) + 1,
				start_index, start_tli, start_lsn,
				is_learner, polar_dma_init_meta);
	}
	else
	{
		if (consensus_cluster_info)
		{
			ConsensusMetaSetMemberInfo(consensus_cluster_info,
					*consensus_cluster_info == '\0' ?  0 : strlen(consensus_cluster_info) + 1, false);
		}

		if (consensus_learner_info)
		{
			ConsensusMetaSetLearnerInfo(consensus_learner_info,
					*consensus_learner_info == '\0' ?  0 : strlen(consensus_learner_info) + 1, false);
		}
	}

	if (consensus_meta_cluster_info)
		free(consensus_meta_cluster_info);

	if (consensus_cluster_info)
		pfree(consensus_cluster_info);
	if (consensus_learner_info)
		pfree(consensus_learner_info);

	return is_learner;
}

/*
 * Consensus main entry point
 *
 * no return
 */
void
ConsensusMain(void)
{
	pthread_t 	appendpid, statpid, cmdpid, advancepid;
	uint64		start_commit_index, mock_start_index;
	TimeLineID 	timeline;
	XLogRecPtr 	lsn;
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	pthread_rwlockattr_t lock_attr;
#endif
	bool 		is_learner = false;

	ConsensusContext = AllocSetContextCreate(TopMemoryContext,
										 "PolarDMA",
										 ALLOCSET_DEFAULT_SIZES);
	MemoryContextAllowInCriticalSection(ConsensusContext, true);
	MemoryContextSwitchTo(ConsensusContext);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	pqsignal(SIGHUP, PostgresSigHupHandler);
	pqsignal(SIGINT, ConsensusSigIntHandler);
	pqsignal(SIGTERM, ConsensusProcShutdownHandler);		/* request shutdown */
	pqsignal(SIGQUIT, consensus_quickdie);	/* hard crash time */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, ConsensusSigUsr1Handler);
	pqsignal(SIGUSR2, ConsensusSigUsr2Handler);

	SpinLockAcquire(&ConsensusCtl->mutex);
	ConsensusCtl->consensusPid = MyProcPid;
	SpinLockRelease(&ConsensusCtl->mutex);
	on_proc_exit(consensus_on_exit, 0);

	consensus_stats_init();

 	if (!ConsensusMetaStartup())
	{
		ereport(FATAL, (errmsg("polar dma startup faild")));
	}

	is_learner = consensus_meta_init();

	if (polar_dma_force_change_meta || polar_dma_init_meta)
	{
		ereport(LOG,
				(errmsg("consensus process normal shutdown after force change or intialize meta")));
		kill(PostmasterPid, SIGQUIT);
		proc_exit(0);
	}

	if (!ConsensusLOGStartup())
	{
		ereport(ERROR,  (errmsg("consensus log startup faild")));
	}
	ConsensusLOGSetKeepSize(polar_dma_log_keep_size_mb);

	ConsensusLOGGetLastLSN(&lsn, &timeline);
	ConsensusSetXLogFlushedLSN(lsn, timeline, false);
	consensus_set_flush_wait_timeout(polar_dma_xlog_check_timeout);

#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	ConsensusCtl->currentState = CONSENSUS_STATE_DOWN;
	ConsensusCtl->currentAppendedLSN = lsn;
#else
	pg_atomic_init_u32(&ConsensusCtl->currentState, CONSENSUS_STATE_DOWN);
	pg_atomic_init_u64(&ConsensusCtl->currentAppendedLSN, lsn);
#endif
	ConsensusCtl->syncRqstLSN = lsn;

	ConsensusMetaGetInt64(CommitIndexMetaKey, &start_commit_index);
	if (consensus_log_get_entry_lsn(start_commit_index, &lsn, &timeline))
	{
		if (lsn > ConsensusCtl->syncedLSN)
		{
			ConsensusCtl->syncedLSN = lsn;
			ConsensusCtl->syncedTLI = timeline;
		}
	}

	Assert(ConsensusGetCommitIndex() == 0);
	ConsensusSetCommitIndex(start_commit_index);

	/* start up consensus */
	ConsensusMetaGetInt64(MockStartIndex, &mock_start_index);
	consensus_startup(is_learner, polar_is_dma_logger_node(), mock_start_index);

	if (pthread_create(&cmdpid, NULL, consensus_command_thread, NULL))
	{
		ereport(FATAL,  (errmsg("consensus create pthread faild")));
	}

	if (pthread_create(&statpid, NULL, consensus_stat_thread, NULL))
	{
		ereport(FATAL,  (errmsg("consensus create pthread faild")));
	}

	if (pthread_create(&appendpid, NULL, consensus_append_log_thread, NULL))
	{
		ereport(FATAL,  (errmsg("consensus create pthread faild")));
	}

	if (pthread_create(&advancepid, NULL, consensus_advance_thread, NULL))
	{
		ereport(FATAL,  (errmsg("consensus create pthread faild")));
	}

	consensus_mainloop();

	if (consensusHandle)
	{
		consensus_shutdown(consensusHandle);
	}

	pthread_join(advancepid, NULL);
	pthread_join(appendpid, NULL);
	pthread_join(cmdpid, NULL);
	pthread_join(statpid, NULL);

  proc_exit(shutdown_requested ? 0 : 1);
}

static int
convert_log_messages_to_easy_level(int min_messages)
{
	if (min_messages >= FATAL)
		return EASY_LOG_FATAL;
	else if (min_messages >= ERROR)
		return EASY_LOG_ERROR;
	else if (min_messages >= WARNING)
		return EASY_LOG_WARN;
	else if (min_messages >= LOG)
		return EASY_LOG_INFO;
	else if (min_messages == DEBUG1)
		return EASY_LOG_DEBUG;
	else if (min_messages < DEBUG1 && log_min_messages >= DEBUG5)
		return EASY_LOG_TRACE;
	else
		return EASY_LOG_WARN;
}

static void
consensus_startup(bool is_learner, bool is_logger, uint64_t mock_start_index)
{
	int			ret;
	int 		consensus_log_level = convert_log_messages_to_easy_level(log_min_messages);

	consensusHandle = consensus_create_instance(
							is_logger ? mock_start_index : 0,
							polar_dma_election_timeout,
							polar_dma_purge_timeout,
							polar_dma_max_packet_size * 1024,
							polar_dma_pipeline_timeout,
							polar_dma_config_change_timeout,
							consensus_log_level);

	if (consensusHandle == INVALID_CONSENSUS_HANDLE)
	{
		ereport(ERROR, 
				(errmsg("Unable to initialize consensus service.")));
	}

	if (!is_learner)
		ret = consensus_init_as_peer(consensusHandle,
										is_logger,
										polar_dma_io_thread_count,
										polar_dma_hb_thread_count,
										polar_dma_worker_thread_count,
										is_logger ? true : polar_dma_auto_leader_transfer,
										polar_dma_auto_purge,
										polar_dma_delay_election,
										polar_dma_delay_electionTimeout);
	else
		ret = consensus_init_as_learner(consensusHandle,
										is_logger,
										polar_dma_io_thread_count,
										polar_dma_hb_thread_count,
										polar_dma_worker_thread_count,
										is_logger ? true : polar_dma_auto_leader_transfer,
										polar_dma_auto_purge,
										polar_dma_delay_election,
										polar_dma_delay_electionTimeout);

	if (ret)
	{
		ereport(ERROR,
				(errmsg("Unable to initialize consensus service. return: %d", ret)));
	}
}


static void
consensus_mainloop(void)
{
	XLogSegNo old_segno = 0;
	TimeLineID old_committed_tli = 0;
	int timeout = CONSENSUS_CHECK_SHUTDOWN_INTERVAL;

	for (;;)
	{
		ResetLatch(MyLatch);

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!PostmasterIsAlive())
		{
			consensus_quickdie(SIGQUIT);
			break;
		}

		if (shutdown_requested)
		{
			break;
		}

		if (XLogArchivingAlways() ||
				GetRecoveryState() != RECOVERY_STATE_ARCHIVE)
		{
			XLogSegNo segno;
			TimeLineID tli;
			XLogRecPtr recptr;
			char xlog[MAXFNAMELEN];

			ConsensusGetSyncedLSNAndTLI(&recptr, &tli);

			XLByteToSeg(recptr, segno, wal_segment_size);

			if (old_committed_tli == 0 || old_segno < segno || old_committed_tli < tli)
			{
				XLogFileName(xlog, tli, segno, wal_segment_size);
				polar_is_datamax_mode = polar_is_datamax();		/* POLAR: Enter datamax Mode. */
				polar_dma_xlog_archive_notify(xlog, false);
				polar_is_datamax_mode = false;				/* POLAR: Leave datamax mode. */
				old_segno = segno;
				old_committed_tli = tli;
			}
			timeout = CONSENSUS_ARCHIVE_ADVANCE_INTERVAL;
		}

		WaitLatch(MyLatch,
								  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								  timeout, WAIT_EVENT_CONSENSUS_MAIN);
	}

}

static void*
consensus_advance_thread(void *parm)
{
	char current_leader_addr[NI_MAXHOST + NI_MAXSERV];
	char current_leader_host[NI_MAXHOST];
	int	current_leader_port = 0;

	/* main loop */
	while (!shutdown_requested)
	{
		/* check if we need transit our state based on our current state and XPaxos state. */
		if (consensus_maybe_transit_state(current_leader_addr, current_leader_host, &current_leader_port))
		{
			/* transit failed, sleep 1ms and retry */
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
			pg_atomic_fetch_add_u64(&ConsensusCtl->stats.transit_waits, 1);
#endif
			pg_usleep(CONSNESUS_TRANSIT_CHECK_INTERVAL);
			continue;
		}

		if (consensus_wait_for_next_entry())
		{
			consensus_wakeup_backends(false);
		}
	}
	return NULL;
}

/* SIGUSR1: let latch facility handle the signal */
static void
ConsensusSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

static void
ConsensusSigUsr2Handler(SIGNAL_ARGS)
{
	(void)ConsensusMetaSetInt64(CommitIndexMetaKey, ConsensusGetCommitIndex(), true);
}

static void
ConsensusSigIntHandler(SIGNAL_ARGS)
{
	consensus_set_in_smart_shutdown();
}

/* SIGTERM */
static void
ConsensusProcShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	(void)ConsensusMetaSetInt64(CommitIndexMetaKey, ConsensusGetCommitIndex(), true);

	shutdown_requested = true;

	pg_write_barrier();

	SetLatch(MyLatch);

	errno = save_errno;
}

static void
consensus_quickdie(SIGNAL_ARGS)
{
	/*
	 * Quickly exit without cleaning up anything.
	 */
	_exit(2);
}

Size
ConsensusShmemSize(void)
{
	Size sz;

	sz = ConsensusLOGShmemSize();
	sz = add_size(sz, sizeof(ConsensusCtlData));

	return sz;
}

void
ConsensusShmemInit(void)
{
	bool		found;

	ConsensusCtl = (ConsensusCtlData *)
		ShmemInitStruct("Consensus Ctl", sizeof(ConsensusCtlData), &found);

	if (!IsUnderPostmaster)
	{
		pthread_rwlockattr_t rwlock_attr;
		pthread_mutexattr_t mutex_attr;
		pthread_condattr_t cond_attr;

		pthread_rwlockattr_init(&rwlock_attr);
		pthread_rwlockattr_setpshared(&rwlock_attr,PTHREAD_PROCESS_SHARED);
		pthread_mutexattr_init(&mutex_attr);
		pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_NORMAL);
		pthread_mutexattr_setpshared(&mutex_attr,PTHREAD_PROCESS_SHARED);
		pthread_condattr_init(&cond_attr);
		pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED);

		/* initialize the control structure */
		MemSet(ConsensusCtl, 0, sizeof(ConsensusCtlData));

		SHMQueueInit(&ConsensusCtl->waitQueue);

		pthread_mutex_init(&ConsensusCtl->sync_cond_mutex, &mutex_attr);
		pthread_cond_init(&ConsensusCtl->sync_cond, &cond_attr);

		pthread_rwlock_init(&ConsensusCtl->cs_info_lck, &rwlock_attr);
		pthread_rwlock_init(&ConsensusCtl->xlog_term_lck, &rwlock_attr);
		pthread_rwlock_init(&ConsensusCtl->xlog_flush_lck, &rwlock_attr);

		pthread_mutex_init(&ConsensusCtl->rqst_lck, &mutex_attr);
		pthread_mutex_init(&ConsensusCtl->rqstInstage, &mutex_attr);
		pthread_mutex_init(&ConsensusCtl->rqst_cond_mutex, &mutex_attr);
		pthread_cond_init(&ConsensusCtl->rqst_cond, &cond_attr);

		SHMQueueInit(&ConsensusCtl->statRqstQueue);
		pthread_rwlock_init(&ConsensusCtl->stat_info_lck, &rwlock_attr);
		pthread_mutex_init(&ConsensusCtl->stat_lck, &mutex_attr);
		pthread_mutex_init(&ConsensusCtl->stat_cond_mutex, &mutex_attr);
		pthread_cond_init(&ConsensusCtl->stat_cond, &cond_attr);

		pthread_rwlock_init(&ConsensusCtl->current_status_lock, &rwlock_attr);

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_init_u64(&ConsensusCtl->commitIndex, 0);
		pg_atomic_init_u64(&ConsensusCtl->flushWaitTimeout, 0);
#endif
	}
	else
		Assert(found);

	ConsensusLOGShmemInit();
}

void
ConsensusProcInit(ConsensusProcInfo *procInfo)
{
	pthread_mutexattr_t lock_attr;
	pthread_condattr_t cond_attr;

	pthread_condattr_init(&cond_attr);
	pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED);
	pthread_cond_init(&procInfo->wait_cond, &cond_attr);

	pthread_mutexattr_init(&lock_attr);
	pthread_mutexattr_settype(&lock_attr, PTHREAD_MUTEX_NORMAL);
	pthread_mutexattr_setpshared(&lock_attr,PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(&procInfo->wait_cond_mutex, &lock_attr);

	procInfo->waitLSN = 0;
	procInfo->finished = true;
	SHMQueueElemInit(&procInfo->waitQueueElem);
}

static void
consensus_on_exit(int code, Datum arg)
{
	SpinLockAcquire(&ConsensusCtl->mutex);
	ConsensusCtl->consensusPid = 0;
	SpinLockRelease(&ConsensusCtl->mutex);
	consensus_set_state(CONSENSUS_STATE_DOWN);
}

/*
 * Transit the state of consensus if needed and bring down or up other components
 * (Startup, WALSender, WALReceivers) accordingly.
 *
 * Returns true if anything requires immediate attention without waiting.
 *
 */
static bool
consensus_maybe_transit_state(char *leader_addr, char *leader_host, int *leader_port)
{
	uint64 term,
				 leader_id,
				 local_id;
	uint64 current_append_term = 0;
	uint32 last_write_timeline = 0;
	XLogRecPtr last_write_lsn = 0;
	bool state_change = false;

	if (polar_check_pm_in_state_change())
	{
		easy_warn_log("PostMaster is in a state of changing. signale PostMaster and "
			 "check the consensus state next time");
		return true;
	}

	/* get current term and leader */
	consensus_get_leader(consensusHandle, &local_id, &leader_id, &term,
			leader_addr, NI_MAXHOST + NI_MAXSERV - 1);

	if ((ConsensusCtl->currentStateTerm != term ||
			 ConsensusCtl->currentLeaderId != leader_id))
	{
		if (leader_id > 0)
		{
			easy_warn_log("Consensus state change, current term: %llu, current leader: %lu,"
					"current leader addr: %s, old term: %llu, old leader: %lu",
					term, leader_id, leader_addr,
					ConsensusCtl->currentStateTerm, ConsensusCtl->currentLeaderId);
		}

		if (leader_id == local_id)
		{
			/*
			 * After became leader, reset consensus appended LSN align to consensus log
			 * and advance consensus log term. Disable consensus log to truncate
			 * backward, avoid shutdown while transfer back and forth between leader
			 * and follower
			 */
			ConsensusLOGGetLastLSN(&last_write_lsn, &last_write_timeline);

			consensus_set_appended_lsn(last_write_lsn);
			/* Logger's WAL and consensus log could be purged after downgrade from leader. 
			 * Because the data caches don't need to cleanup */
			if (polar_is_dma_data_node())
					ConsensusLOGSetDisablePurge(true);
			ConsensusLOGSetLogTerm(term);

			current_append_term = term;
			consensus_set_state(CONSENSUS_STATE_LEADER);
			state_change = true;
		}
		else
		{
			if (leader_id > 0 && leader_addr[0] != '\0')
			{
				consensus_covert_server_address(leader_addr,
						leader_host, leader_port);
				state_change = true;
			}
			else if (leader_id > 0 &&
					consensus_get_leader_info(leader_id, leader_host, leader_port))
			{
				state_change = true;
			}
			else if (consensus_get_state() == CONSENSUS_STATE_LEADER &&
					consensus_get_in_smart_shutdown())
			{
				state_change = true;
			}

			if (state_change)
			{
				consensus_set_state(CONSENSUS_STATE_FOLLOWER);
				current_append_term = ConsensusLOGGetLogTerm();
				ConsensusLOGGetLastLSN(&last_write_lsn, &last_write_timeline);
			}
		}
	}

	if (!state_change && leader_id > 0 && leader_addr[0] != '\0' &&
			consensus_get_state() == CONSENSUS_STATE_FOLLOWER)
	{
		current_append_term = ConsensusLOGGetLogTerm();
		if (ConsensusCtl->currentLogTerm != current_append_term)
		{
			easy_warn_log("Consensus log term changed, current log term: %llu,"
					" old log term: %llu", current_append_term, ConsensusCtl->currentLogTerm);
			consensus_covert_server_address(leader_addr, leader_host, leader_port);
			ConsensusLOGGetLastLSN(&last_write_lsn, &last_write_timeline);
			state_change = true;
		}
	}

	if (state_change)
	{
#ifdef USE_ASSERT_CHECKING
		XLogRecPtr	xlog_flushed_lsn;
		TimeLineID	xlog_flushed_tli;
#endif
		XLogRecPtr	committed_lsn;
		TimeLineID	committed_tli;

		pg_write_barrier();

		easy_warn_log("Consensus try signal Postmaster, term: %llu,"
				" leader: %lu, logTerm: %llu, logUpto: %X/%X, logTLI: %u",
				term, leader_id, current_append_term,
				(uint32) (last_write_lsn >> 32), (uint32) last_write_lsn, last_write_timeline);

#ifdef USE_ASSERT_CHECKING
		ConsensusGetXLogFlushedLSN(&xlog_flushed_lsn, &xlog_flushed_tli);
		Assert(xlog_flushed_tli >= last_write_timeline && xlog_flushed_lsn >= last_write_lsn);
#endif

		ConsensusGetSyncedLSNAndTLI(&committed_lsn, &committed_tli);
		/* if synced point beyond last entry point, advance switch point */
		if (committed_tli > last_write_timeline || committed_lsn > last_write_lsn)
		{
			last_write_timeline = committed_tli;
			last_write_lsn = committed_lsn;
			easy_warn_log("Consensus advance logUpto, term: %llu,"
					" leader: %lu, logTerm: %llu, logUpto: %X/%X, logTLI: %u",
					term, leader_id, current_append_term,
					(uint32) (last_write_lsn >> 32), (uint32) last_write_lsn, last_write_timeline);
		}

		polar_signal_pm_state_change(consensus_get_state(),
				leader_host, *leader_port, term, current_append_term,
				last_write_timeline, last_write_lsn);

		pthread_rwlock_wrlock(&ConsensusCtl->current_status_lock);
		ConsensusCtl->currentStateTerm = term;
		ConsensusCtl->currentLeaderId = leader_id;
		ConsensusCtl->currentLogTerm = current_append_term;
		pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
	}

	return !(leader_id > 0);
}

static bool
consensus_try_synchronizing_flush_lsn(void)
{
	XLogRecPtr	xlog_flushed_lsn,
							last_append_lsn;
	TimeLineID	xlog_flushed_tli;
	uint64			index,
							term,
							xlog_term,
							consensus_term;
	int					ret;
	ConsensusLogPayload consensus_log_payload;

	/*
	 * lock xlog term shared and check XLog state change finished.
	 * set consensus log term, then append the flushed point to x-paxos
	 */
	ConsensusLockXLogTermShared();
	if (consensus_get_state() != CONSENSUS_STATE_LEADER)
	{
		ConsensusUnlockXLogTerm();
		return true;
	}

	xlog_term = ConsensusGetXLogTerm();
	consensus_term = ConsensusLOGGetTerm();
	if (xlog_term < consensus_term)
	{
		ConsensusUnlockXLogTerm();
		easy_debug_log("consensus_try_synchronizing_flush_lsn, Append log entry "
				"ignore, consensus term: %llu, xlog_term: %llu", consensus_term, xlog_term);
		return true;
	}

	last_append_lsn = consensus_get_appended_lsn();
	ConsensusGetXLogFlushedLSN(&xlog_flushed_lsn, &xlog_flushed_tli);
	if (xlog_flushed_lsn <= last_append_lsn)
	{
		ConsensusUnlockXLogTerm();
		easy_debug_log("consensus_try_synchronizing_flush_lsn, Append log entry "
				"ignore, xlog flushed lsn: %X/%X timeline %u, last_append_lsn: %X/%X",
				(uint32) (xlog_flushed_lsn >> 32), (uint32) xlog_flushed_lsn, xlog_flushed_tli,
				(uint32) (last_append_lsn >> 32), (uint32) last_append_lsn);
		return true;
	}

	easy_info_log("begin to append entry, append_lsn: %X/%X, append_tli: %u",
			(uint32) (xlog_flushed_lsn >> 32), (uint32) xlog_flushed_lsn,
			xlog_flushed_tli);

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusCtl->stats.consensus_appends, 1);
#endif

	ConsensusLOGNormalPayloadInit(&consensus_log_payload,
			xlog_flushed_lsn, xlog_flushed_tli);
	if ((ret = consensus_append_normal_entry(consensusHandle, &index, &term,
					(const char *)&consensus_log_payload, sizeof(consensus_log_payload))))
	{
		ConsensusUnlockXLogTerm();
		if (ret == CONSENSUS_NOT_LEADER)
		{
			easy_info_log("consensus_try_synchronizing_flush_lsn, Append log entry "
					"ignore, this server may be not leader now!");
			/* requested votes since last state check. to append in the next loop. */
			return true;
		}
		/* Or we have a problem. Report and crash. */
		easy_fatal_log("Unexpected error in consensus library");
		abort();
	}
	ConsensusUnlockXLogTerm();
	consensus_set_appended_lsn(xlog_flushed_lsn);

	easy_info_log("consensus_try_synchronizing_flush_lsn, Append log entry "
			"term: %llu, index: %llu, lsn: %X/%X, timeline: %u", term, index,
			(uint32) (xlog_flushed_lsn >> 32), (uint32) xlog_flushed_lsn,
			xlog_flushed_tli);

	return false;
}

uint64
ConsensusGetCommitIndex(void)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	uint64 commit_index;
	pthread_rwlock_rdlock(&ConsensusCtl->cs_info_lck);
	commit_index = ConsensusCtl->commitIndex;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);
	return commit_index;
#else
	return pg_atomic_read_u64(&ConsensusCtl->commitIndex);
#endif
}

void
ConsensusSetCommitIndex(uint64 commit_index)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	pthread_rwlock_wrlock(&ConsensusCtl->cs_info_lck);
	ConsensusCtl->commitIndex = commit_index;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);
#else
	pg_atomic_write_u64(&ConsensusCtl->commitIndex, commit_index);
#endif
}

static bool
consensus_wait_for_next_entry(void)
{
	uint64 index;
	XLogRecPtr commit_lsn;
	TimeLineID commit_timeline;
	uint64 last_committed_index = ConsensusGetCommitIndex();

	easy_debug_log("Consensus begin wait for commit, last commit index %llu, current state term %llu",
			last_committed_index, ConsensusCtl->currentStateTerm);

	if (ConsensusCtl->currentStateTerm != 0 && ConsensusGetXLogTerm() == ConsensusCtl->currentStateTerm)
	{
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusCtl->stats.consensus_commit_waits, 1);
		pg_atomic_fetch_add_u64(&ConsensusCtl->stats.consensus_commit_wait_time, 1);
#endif
		if (!(index = consensus_wait_for_commit(consensusHandle,
						last_committed_index + 1,
						ConsensusCtl->currentStateTerm)))
		{
			easy_warn_log("Consensus term change when wait for commit, index %llu", index);
			/* Term changed, let our caller perform state transition */
			return false;
		}
	}
	else
	{
		pg_usleep(HISTORIC_COMMIT_ADVANCE_INTERVAL);
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusCtl->stats.xlog_transit_waits, 1);
#endif
		index = consensus_get_commit_index(consensusHandle);
	}

	easy_debug_log("Consensus finish waitting for commit, current commit index %llu, "
			"last commit index %llu, current state term %llu",
			index, last_committed_index, ConsensusCtl->currentStateTerm);

	/* Now we now a new batch of committed indexes, find and update flush lsn. */
	if (index > last_committed_index)
	{
		XLogRecPtr old_commit_lsn = ConsensusGetSyncedLSN();
		consensus_log_get_entry_lsn(index, &commit_lsn, &commit_timeline);
		easy_warn_log("Advance committed lsn: %X/%X timeline: %u, old committed lsn: %X/%X",
				(uint32) (commit_lsn >> 32), (uint32) commit_lsn, commit_timeline,
				(uint32) (old_commit_lsn >> 32), (uint32) old_commit_lsn);

		if (!ConsensusMetaSetInt64(CommitIndexMetaKey, index, true))
			abort();

		if (commit_lsn != InvalidXLogRecPtr && commit_timeline != 0)
			ConsensusSetSyncedLSN(commit_lsn, commit_timeline);

		ConsensusSetCommitIndex(index);

		return true;
	}

	return false;
}

/*
 * Locked consensus log term before call this function.
 */
bool
consensus_check_physical_flush_lsn(uint64 term,
		XLogRecPtr lsn, TimeLineID tli, bool wait)
{
	XLogRecPtr	physical_flush_lsn;
	TimeLineID physical_flush_timeline;
	uint64 physical_flush_term;
	int			num_retries = 0;
	int			total_retries = consensus_get_flush_wait_timeout();

	physical_flush_term = ConsensusGetXLogTerm();
	ConsensusGetXLogFlushedLSN(&physical_flush_lsn, &physical_flush_timeline);

	easy_debug_log("consensus_check_physical_flush_lsn, physical_flush_term: %llu, "
			"physical_flush_lsn: %X/%X timeline %u, term: %llu, lsn: %X/%X",
			physical_flush_term,
			(uint32) (physical_flush_lsn >> 32), (uint32) physical_flush_lsn, physical_flush_timeline,
			term, (uint32) (lsn >> 32), (uint32) lsn);

	if (physical_flush_term >= term &&
			physical_flush_lsn >= lsn &&
			physical_flush_timeline >= tli)
	{
		easy_debug_log("consensus_check_physical_flush_lsn success. "
				"physical_flush_term: %llu, physical_flush_lsn: %X/%X, physical_flush_timeline: %u"
				"check_term: %llu, check_lsn: %X/%X, check_timeline: %u",
				physical_flush_term,
				(uint32) (physical_flush_lsn >> 32), (uint32) physical_flush_lsn, physical_flush_timeline,
				term, (uint32) (lsn >> 32), (uint32) lsn, tli);
		return true;
	}

	if (!wait)
	{
		easy_warn_log("consensus_check_physical_flush_lsn failed without retry. "
				"physical_flush_term: %llu, physical_flush_lsn: %X/%X timeline %u, "
				"check_term: %llu, check_lsn: %X/%X, check_timeline: %u",
				physical_flush_term,
				(uint32) (physical_flush_lsn >> 32), (uint32) physical_flush_lsn, physical_flush_timeline,
				term, (uint32) (lsn >> 32), (uint32) lsn, tli);
		return false;
	}

	do
	{
		pg_usleep(XLOG_STREAMING_CHECK_INTERVAL);
		num_retries++;
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusCtl->stats.xlog_flush_waits, 1);
#endif
		ConsensusGetXLogFlushedLSN(&physical_flush_lsn, &physical_flush_timeline);
		physical_flush_term = ConsensusGetXLogTerm();
	}
	while (num_retries < total_retries &&
			(physical_flush_term < term || physical_flush_lsn < lsn || physical_flush_timeline < tli));

	if (num_retries >= total_retries)
	{
		easy_info_log("consensus_check_physical_flush_lsn wait timeout after %d retries, "
				"physical_flush_term: %llu, physical_flush_lsn: %X/%X timeline: %u, "
				"check_term: %llu, check_lsn: %X/%X, check_timeline: %u",
				num_retries, physical_flush_term,
				(uint32) (physical_flush_lsn >> 32), (uint32) physical_flush_lsn, physical_flush_timeline,
				term, (uint32) (lsn >> 32), (uint32) lsn, tli);
		return false;
	}
	else
	{
		easy_debug_log("consensus_check_physical_flush_lsn success after %d retries. "
				"physical_flush_term: %llu, physical_flush_lsn: %X/%X timeline %u, "
				"check_term: %llu, check_lsn: %X/%X, check_timeline: %u",
				num_retries, physical_flush_term,
				(uint32) (physical_flush_lsn >> 32), (uint32) physical_flush_lsn, physical_flush_timeline,
				term, (uint32) (lsn >> 32), (uint32) lsn, tli);
		return true;
	}
}

static int
consensus_get_commit_wait_queue_length(void)
{
	int length = 0;
	PGPROC	   *proc = NULL;

	LWLockAcquire(ConsensusLock, LW_SHARED);

	proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->waitQueue),
			&(ConsensusCtl->waitQueue),
			offsetof(PGPROC, consensusInfo.waitQueueElem));

	while(proc != NULL)
	{
		length++;
		proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->waitQueue),
										  &(proc->consensusInfo.waitQueueElem),
										  offsetof(PGPROC, consensusInfo.waitQueueElem));
	}

	LWLockRelease(ConsensusLock);

	return length;
}

static void
consensus_wakeup_backends(bool all)
{
	PGPROC	   *proc = NULL;
	PGPROC	   *thisproc = NULL;
	XLogRecPtr	flush = ConsensusGetSyncedLSN();

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
	pg_atomic_fetch_add_u64(&ConsensusCtl->stats.consensus_wakeups, 1);
#endif

	LWLockAcquire(ConsensusLock, LW_EXCLUSIVE);
	proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->waitQueue),
			&(ConsensusCtl->waitQueue),
			offsetof(PGPROC, consensusInfo.waitQueueElem));
	while (proc)
	{
		if (!all && flush < proc->consensusInfo.waitLSN)
		{
			break;
		}

		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->waitQueue),
				&(proc->consensusInfo.waitQueueElem),
				offsetof(PGPROC, consensusInfo.waitQueueElem));

		if (!SHMQueueIsDetached(&(thisproc->consensusInfo.waitQueueElem)))
			SHMQueueDelete(&(thisproc->consensusInfo.waitQueueElem));

#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
		pg_atomic_fetch_add_u64(&ConsensusCtl->stats.consensus_backend_wakeups, 1);
#endif

		/* Notify the process. */
		pthread_mutex_lock(&thisproc->consensusInfo.wait_cond_mutex);
		pthread_cond_signal(&thisproc->consensusInfo.wait_cond);
		pthread_mutex_unlock(&thisproc->consensusInfo.wait_cond_mutex);
	}

	LWLockRelease(ConsensusLock);
}

static void
consensus_get_wait_tv(struct timespec *abstime, int wait_us)
{
	struct timeval now;
	gettimeofday(&now, NULL);
	abstime->tv_nsec = ((now.tv_usec + wait_us) % (1000 * 1000)) * 1000;
	abstime->tv_sec = now.tv_sec + (now.tv_usec + wait_us) / (1000 * 1000);
}

static void
consensus_cancel_wait(void)
{
	LWLockAcquire(ConsensusLock, LW_EXCLUSIVE);
	if (!SHMQueueIsDetached(&(MyProc->consensusInfo.waitQueueElem)))
		SHMQueueDelete(&MyProc->consensusInfo.waitQueueElem);
	LWLockRelease(ConsensusLock);
}

/*
 * Wait for the flush lsn to be raised above the given lsn argument.
 * return true if committed success or wait timeout
 * Note: Only if the database shutdown or restart, this function return false
 */
bool
ConsensusWaitForLSN(XLogRecPtr lsn, bool flush)
{
	PGPROC	*proc;
	XLogRecPtr commit_lsn;
	struct timespec abstime;

	if (ConsensusGetSyncedLSN() >= lsn)
		return true;

	if (flush)
		ConsensusWakeupCommit(lsn);

	LWLockAcquire(ConsensusLock, LW_EXCLUSIVE);

	if (ConsensusGetSyncedLSN() >= lsn)
	{
		LWLockRelease(ConsensusLock);
		return true;
	}

	MyProc->consensusInfo.waitLSN = lsn;
	proc = (PGPROC *) SHMQueuePrev(&(ConsensusCtl->waitQueue),
			&(ConsensusCtl->waitQueue),
			offsetof(PGPROC, consensusInfo.waitQueueElem));

	while (proc)
	{
		if (proc->consensusInfo.waitLSN < MyProc->consensusInfo.waitLSN)
			break;

		proc = (PGPROC *) SHMQueuePrev(&(ConsensusCtl->waitQueue),
				&(proc->consensusInfo.waitQueueElem),
				offsetof(PGPROC, consensusInfo.waitQueueElem));
	}

	if (proc)
		SHMQueueInsertAfter(&(proc->consensusInfo.waitQueueElem), &(MyProc->consensusInfo.waitQueueElem));
	else
		SHMQueueInsertAfter(&(ConsensusCtl->waitQueue), &(MyProc->consensusInfo.waitQueueElem));

	LWLockRelease(ConsensusLock);

	commit_lsn = ConsensusGetSyncedLSN();

	pgstat_report_wait_start(WAIT_EVENT_CONSENSUS_COMMIT);

	while(commit_lsn < lsn)
	{
		consensus_get_wait_tv(&abstime, CONSENSUS_COMMIT_TIMEOUT);
		pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);
		pthread_cond_timedwait(&MyProc->consensusInfo.wait_cond, &MyProc->consensusInfo.wait_cond_mutex, &abstime);
		pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);

		commit_lsn = ConsensusGetSyncedLSN();
		if (commit_lsn >= lsn || !flush)
		{
			break;
		}

		if (!PostmasterIsAlive())
		{
			ProcDiePending = true;
			break;
		}

		if (!flush && ProcDiePending)
			break;

		commit_lsn = ConsensusGetSyncedLSN();
	}

	consensus_cancel_wait();
	MyProc->consensusInfo.waitLSN = 0;

	pgstat_report_wait_end();

	return commit_lsn >= lsn;
}


void
ConsensusWakeupCommit(XLogRecPtr rqstLSN)
{
	SpinLockAcquire(&ConsensusCtl->mutex);

	if (ConsensusCtl->consensusPid > 0)
	{
		pthread_mutex_lock(&ConsensusCtl->sync_cond_mutex);
		if (rqstLSN > ConsensusCtl->syncRqstLSN)
		{
			ConsensusCtl->syncRqstLSN = rqstLSN;
			if (ConsensusCtl->syncSuspending)
			{
				pthread_cond_signal(&ConsensusCtl->sync_cond);
			}
		}
		pthread_mutex_unlock(&ConsensusCtl->sync_cond_mutex);
	}

	SpinLockRelease(&ConsensusCtl->mutex);
}

static void
ConsensusLockXLogTermShared(void)
{
	pthread_rwlock_rdlock(&ConsensusCtl->xlog_term_lck);
}

static void
ConsensusUnlockXLogTerm(void)
{
	pthread_rwlock_unlock(&ConsensusCtl->xlog_term_lck);
}

uint64
ConsensusGetXLogTerm(void)
{
	uint64 term;

	pthread_rwlock_rdlock(&ConsensusCtl->xlog_term_lck);
	term = ConsensusCtl->xlogTerm;
	pthread_rwlock_unlock(&ConsensusCtl->xlog_term_lck);

	return term;
}

void
ConsensusSetXLogTerm(uint64 term)
{
	pthread_rwlock_wrlock(&ConsensusCtl->xlog_term_lck);
	ConsensusCtl->xlogTerm = term;
	pthread_rwlock_unlock(&ConsensusCtl->xlog_term_lck);

	ereport(LOG, (errmsg("XLog term changed, term: %lu", term)));
}

/* Only called by postgres backend or consensus main thread */
void
ConsensusSetXLogFlushedLSN(XLogRecPtr flush_lsn, TimeLineID flush_timeline, bool force)
{
	bool update = false;
	pthread_rwlock_wrlock(&ConsensusCtl->xlog_flush_lck);
	if (force ||
			flush_lsn > ConsensusCtl->flushedLSN)
	{
		ConsensusCtl->flushedLSN = flush_lsn;
		ConsensusCtl->flushedTLI = flush_timeline;
		update = true;
	}
	pthread_rwlock_unlock(&ConsensusCtl->xlog_flush_lck);

	if (update)
	{
		elog(DEBUG1, "XLog flushed point advanced, timeline %u, LSN: %X/%X",
						flush_timeline, (uint32) (flush_lsn >> 32),
						(uint32) flush_lsn);
	}
}

void
ConsensusGetXLogFlushedLSN(XLogRecPtr *flush_lsn, TimeLineID *flush_timeline)
{
	pthread_rwlock_rdlock(&ConsensusCtl->xlog_flush_lck);
	*flush_lsn = ConsensusCtl->flushedLSN;
	*flush_timeline = ConsensusCtl->flushedTLI;
	pthread_rwlock_unlock(&ConsensusCtl->xlog_flush_lck);
}

static void
consensus_set_flush_wait_timeout(uint64 wait_time)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	pthread_rwlock_wrlock(&ConsensusCtl->xlog_flush_lck);
	ConsensusCtl->flushWaitTimeout = wait_time;
	pthread_rwlock_unlock(&ConsensusCtl->xlog_flush_lck);
#else
	pg_atomic_write_u64(&ConsensusCtl->flushWaitTimeout, wait_time);
#endif
}

static uint64
consensus_get_flush_wait_timeout(void)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	uint64 wait_time;
	pthread_rwlock_rdlock(&ConsensusCtl->xlog_flush_lck);
	wait_time = ConsensusCtl->flushWaitTimeout;
	pthread_rwlock_unlock(&ConsensusCtl->xlog_flush_lck);
	return wait_time;
#else
	return pg_atomic_read_u64(&ConsensusCtl->flushWaitTimeout);
#endif
}

static void
consensus_set_appended_lsn(XLogRecPtr append_lsn)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	pthread_rwlock_wrlock(&ConsensusCtl->current_status_lock);
	ConsensusCtl->currentAppendedLSN = append_lsn;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
#else
	pg_atomic_write_u64(&ConsensusCtl->currentAppendedLSN, append_lsn);
#endif
}

static XLogRecPtr
consensus_get_appended_lsn(void)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	XLogRecPtr	appendedLSN;
	pthread_rwlock_rdlock(&ConsensusCtl->current_status_lock);
	appendedLSN = ConsensusCtl->currentAppendedLSN;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
	return appendedLSN;
#else
	return pg_atomic_read_u64(&ConsensusCtl->currentAppendedLSN);
#endif
}

static void
consensus_set_state(uint32 state)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	pthread_rwlock_wrlock(&ConsensusCtl->current_status_lock);
	ConsensusCtl->currentState = state;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
#else
	pg_atomic_write_u32(&ConsensusCtl->currentState, state);
#endif
}

static int
consensus_get_state(void)
{
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
	int state;
	pthread_rwlock_rdlock(&ConsensusCtl->current_status_lock);
	state = ConsensusCtl->currentState;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
	return state;
#else
	return pg_atomic_read_u32(&ConsensusCtl->currentState);
#endif
}

static void
consensus_set_in_smart_shutdown(void)
{
	pthread_rwlock_wrlock(&ConsensusCtl->current_status_lock);
	ConsensusCtl->inSmartShutdown = true;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
}

static bool
consensus_get_in_smart_shutdown(void)
{
	bool ret;
	pthread_rwlock_rdlock(&ConsensusCtl->current_status_lock);
	ret = ConsensusCtl->inSmartShutdown;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
	return ret;
}

static void
consensus_get_current_status(uint64 *term, uint64 *leader_id, uint64 *log_term)
{
	pthread_rwlock_rdlock(&ConsensusCtl->current_status_lock);
	*term = ConsensusCtl->currentStateTerm;
	*leader_id = ConsensusCtl->currentLeaderId;
	*log_term = ConsensusCtl->currentLogTerm;
	pthread_rwlock_unlock(&ConsensusCtl->current_status_lock);
}

void
ConsensusSetSyncedLSN(XLogRecPtr lsn, TimeLineID tli)
{
	pthread_rwlock_wrlock(&ConsensusCtl->cs_info_lck);
	if (lsn > ConsensusCtl->syncedLSN)
	{
		ConsensusCtl->syncedLSN = lsn;
		ConsensusCtl->syncedTLI = tli;
	}
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);
}

void
ConsensusGetSyncedLSNAndTLI(XLogRecPtr *lsn, TimeLineID *tli)
{
	pthread_rwlock_rdlock(&ConsensusCtl->cs_info_lck);
	*lsn = ConsensusCtl->syncedLSN;
	*tli = ConsensusCtl->syncedTLI;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);
}

XLogRecPtr
ConsensusGetSyncedLSN(void)
{
	XLogRecPtr syncedLSN;

	pthread_rwlock_rdlock(&ConsensusCtl->cs_info_lck);
	syncedLSN = ConsensusCtl->syncedLSN;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);

	return syncedLSN;
}

void
ConsensusSetPurgeLSN(XLogRecPtr lsn)
{
	pthread_rwlock_wrlock(&ConsensusCtl->cs_info_lck);
	if (lsn > ConsensusCtl->purgeLSN)
		ConsensusCtl->purgeLSN = lsn;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);

	easy_info_log("consensus_set_purge_lsn, lsn: %X/%X",
			(uint32) (lsn >> 32), (uint32) lsn);
}

XLogRecPtr
ConsensusGetPurgeLSN(void)
{
	XLogRecPtr purgeLSN;
	pthread_rwlock_rdlock(&ConsensusCtl->cs_info_lck);
	purgeLSN = ConsensusCtl->purgeLSN;
	pthread_rwlock_unlock(&ConsensusCtl->cs_info_lck);

	return purgeLSN;
}

bool
ConsensusCheckpoint(void)
{
	pid_t pid;

	SpinLockAcquire(&ConsensusCtl->mutex);
	pid = ConsensusCtl->consensusPid;
	SpinLockRelease(&ConsensusCtl->mutex);

	if (pid > 0)
		kill(pid, SIGUSR2);

	return true;
}

void
ConsensusGetMemberInfo(ConsensusMemberInfo *member_info)
 {
	 pthread_rwlock_rdlock(&ConsensusCtl->stat_info_lck);
	 *member_info = ConsensusCtl->memberInfo;
	 pthread_rwlock_unlock(&ConsensusCtl->stat_info_lck);
}

int
ConsensusGetClusterInfo(ConsensusClusterInfo *cluster_info)
{
	int numCluster;

	pthread_rwlock_rdlock(&ConsensusCtl->stat_info_lck);
	numCluster = ConsensusCtl->numCluster;
	memcpy(cluster_info, ConsensusCtl->clusterInfo,
			numCluster * sizeof(ConsensusClusterInfo));
	pthread_rwlock_unlock(&ConsensusCtl->stat_info_lck);

	return numCluster;
}

void
ConsensusGetStatsInfo(ConsensusStatsInfo *stats_info)
{
	pthread_rwlock_rdlock(&ConsensusCtl->stat_info_lck);
	*stats_info = ConsensusCtl->statsInfo;
	pthread_rwlock_unlock(&ConsensusCtl->stat_info_lck);
}

static int
consensus_get_stat_collect_queue_length(void)
{
	int 	length = 0;
	PGPROC 	*proc = NULL;

	pthread_mutex_lock(&ConsensusCtl->stat_lck);

	proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->statRqstQueue),
			&(ConsensusCtl->statRqstQueue),
			offsetof(PGPROC, consensusInfo.waitQueueElem));

	while (proc != NULL)
	{
		length++;
		proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->statRqstQueue),
				&(proc->consensusInfo.waitQueueElem),
				offsetof(PGPROC, consensusInfo.waitQueueElem));
	}

	pthread_mutex_unlock(&ConsensusCtl->stat_lck);

	return length;
}

static void
consesus_cancel_stat_collect(void)
{
	pthread_mutex_lock(&ConsensusCtl->stat_lck);
	if (!SHMQueueIsDetached(&(MyProc->consensusInfo.waitQueueElem)))
		SHMQueueDelete(&MyProc->consensusInfo.waitQueueElem);
	pthread_mutex_unlock(&ConsensusCtl->stat_lck);
}

bool
ConsensusWaitForStatCollect(int info_flags)
{
	struct timespec abstime;

	pthread_mutex_lock(&ConsensusCtl->stat_lck);

	ConsensusCtl->statRqstFlag |= info_flags;

	pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);
	MyProc->consensusInfo.finished = false;
	pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);

	SHMQueueInsertAfter(&(ConsensusCtl->statRqstQueue), &(MyProc->consensusInfo.waitQueueElem));

	pthread_mutex_unlock(&ConsensusCtl->stat_lck);

	pthread_mutex_lock(&ConsensusCtl->stat_cond_mutex);
	pthread_cond_signal(&ConsensusCtl->stat_cond);
	pthread_mutex_unlock(&ConsensusCtl->stat_cond_mutex);

	pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);

	while(!MyProc->consensusInfo.finished)
	{
		pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);
		if (ProcDiePending)
		{
			consesus_cancel_stat_collect();

			ProcDiePending = true;
			ereport(ERROR,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for consensus stat fetch and "
						 "terminating connection due to administrator command")));
		}

		if (QueryCancelPending)
		{
			consesus_cancel_stat_collect();

			QueryCancelPending = false;
			ereport(ERROR,
					(errmsg("canceling wait for consensus stat fetch due to user request")));
		}

		if (!PostmasterIsAlive())
		{
			consesus_cancel_stat_collect();
			ProcDiePending = true;
			return false;
		}

		consensus_get_wait_tv(&abstime, CONSENSUS_STAT_FETCH_TIMEOUT);
		pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);
		if (!MyProc->consensusInfo.finished)
		{
			pthread_cond_timedwait(&MyProc->consensusInfo.wait_cond,
					&MyProc->consensusInfo.wait_cond_mutex, &abstime);
		}
	}
	pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);

	consesus_cancel_stat_collect();

	return MyProc->consensusInfo.finished;
}

static int
consesus_cancel_consensus_command(void)
{
	int rc;

	pthread_mutex_lock(&ConsensusCtl->rqst_lck);

	rc = ConsensusCtl->responce;
	ConsensusCtl->responce = 0;
	ConsensusCtl->rqstType = 0;

	pthread_mutex_unlock(&ConsensusCtl->rqst_lck);

	return rc;
}

static bool
consensus_wait_for_command(int cmd_type, void *args1, void *args2)
{
	int rc;
	struct timespec abstime;

	pthread_mutex_lock(&ConsensusCtl->rqstInstage);

	pthread_mutex_lock(&ConsensusCtl->rqst_lck);

	ConsensusCtl->rqstType = cmd_type;
	ConsensusCtl->responce = 0;
	if (args1 != NULL)
	{
		strncpy(ConsensusCtl->nodeInfo, (char *)args1, sizeof(ConsensusCtl->nodeInfo) - 1);
	}
	if (cmd_type == CONSENSUS_CHANGE_WEIGHT_CONFIG)
	{
		Assert(args2 != NULL);
		ConsensusCtl->option = *(int *)args2;
	}
	else if (cmd_type == CONSENSUS_CHANGE_MATCH_INDEX)
	{
		Assert(args2 != NULL);
		ConsensusCtl->option = *(uint64 *)args2;
	}

	ConsensusCtl->wait_cond_mutex = &MyProc->consensusInfo.wait_cond_mutex;
	ConsensusCtl->wait_cond = &MyProc->consensusInfo.wait_cond;
	ConsensusCtl->finished = &MyProc->consensusInfo.finished;

	pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);
	MyProc->consensusInfo.finished = false;
	pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);

	pthread_mutex_unlock(&ConsensusCtl->rqst_lck);

	/* signal consensus cmd request handler */
	pthread_mutex_lock(&ConsensusCtl->rqst_cond_mutex);
	pthread_cond_signal(&ConsensusCtl->rqst_cond);
	pthread_mutex_unlock(&ConsensusCtl->rqst_cond_mutex);

	pthread_mutex_lock(&MyProc->consensusInfo.wait_cond_mutex);
	while(!MyProc->consensusInfo.finished)
	{
		if (ProcDiePending)
		{
			MyProc->consensusInfo.finished = true;
			pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);
			(void) consesus_cancel_consensus_command();
			pthread_mutex_unlock(&ConsensusCtl->rqstInstage);

			ereport(ERROR,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for consensus command and "
						 "terminating connection due to administrator command")));
		}

		if (QueryCancelPending)
		{
			MyProc->consensusInfo.finished = true;
			pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);
			(void) consesus_cancel_consensus_command();
			pthread_mutex_unlock(&ConsensusCtl->rqstInstage);

			QueryCancelPending = false;
			ereport(ERROR,
					(errmsg("canceling wait for consesus command due to user request")));
		}

		if (!PostmasterIsAlive())
		{
			ProcDiePending = true;
			break;
		}

		consensus_get_wait_tv(&abstime, CONSENSUS_COMMAND_TIMEOUT);
		pthread_cond_timedwait(&MyProc->consensusInfo.wait_cond, &MyProc->consensusInfo.wait_cond_mutex, &abstime);
	}
	pthread_mutex_unlock(&MyProc->consensusInfo.wait_cond_mutex);

	rc = consesus_cancel_consensus_command();

	pthread_mutex_unlock(&ConsensusCtl->rqstInstage);

	if (MyProc->consensusInfo.finished && rc != 0)
	{
		ereport(ERROR,
				(errmsg("Failed to execute consensus command: %s",
				 consensus_get_error_message(rc))));
	}

	return MyProc->consensusInfo.finished;
}


static void
consensus_handle_stat_request(void)
{
	PGPROC *proc = NULL;
	PGPROC *thisproc = NULL;

	pthread_mutex_lock(&ConsensusCtl->stat_lck);
	if (SHMQueueEmpty(&(ConsensusCtl->statRqstQueue)))
	{
		Assert(ConsensusCtl->statRqstFlag == 0);
		pthread_mutex_unlock(&ConsensusCtl->stat_lck);
		return;
	}

	pthread_rwlock_wrlock(&ConsensusCtl->stat_info_lck);
	Assert(ConsensusCtl->statRqstFlag > 0);
	if (ConsensusCtl->statRqstFlag & POLAR_DMA_STAT_MEMBER_INFO)
	{
		consensus_get_member_info(consensusHandle, &ConsensusCtl->memberInfo);
	}

	if (ConsensusCtl->statRqstFlag & POLAR_DMA_STAT_CLUSTER_INFO)
	{
		ConsensusCtl->numCluster = consensus_get_cluster_info(consensusHandle,
														  ConsensusCtl->clusterInfo,
														  sizeof(ConsensusCtl->clusterInfo));
	}

	if (ConsensusCtl->statRqstFlag & POLAR_DMA_STATS_INFO)
	{
		consensus_get_stats_info(consensusHandle, &ConsensusCtl->statsInfo);
	}
	pthread_rwlock_unlock(&ConsensusCtl->stat_info_lck);

	proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->statRqstQueue),
								   &(ConsensusCtl->statRqstQueue),
								   offsetof(PGPROC, consensusInfo.waitQueueElem));
	while (proc)
	{
		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(ConsensusCtl->statRqstQueue),
									   &(proc->consensusInfo.waitQueueElem),
									   offsetof(PGPROC, consensusInfo.waitQueueElem));

		if (!SHMQueueIsDetached(&(thisproc->consensusInfo.waitQueueElem)))
			SHMQueueDelete(&(thisproc->consensusInfo.waitQueueElem));

		/* Notify the process. */
		pthread_mutex_lock(&thisproc->consensusInfo.wait_cond_mutex);
		if (!thisproc->consensusInfo.finished)
		{
			thisproc->consensusInfo.finished = true;
			pthread_cond_signal(&thisproc->consensusInfo.wait_cond);
		}
		pthread_mutex_unlock(&thisproc->consensusInfo.wait_cond_mutex);
	}

	ConsensusCtl->statRqstFlag = 0;
	pthread_mutex_unlock(&ConsensusCtl->stat_lck);
}

static void
consensus_handle_cmd_request(void)
{
	pthread_mutex_lock(&ConsensusCtl->rqst_lck);
	if (ConsensusCtl->rqstType == 0)
	{
		pthread_mutex_unlock(&ConsensusCtl->rqst_lck);
		return;
	}

	if (ConsensusCtl->rqstType == CONSENSUS_TRANSFER_LEADER)
	{
		easy_warn_log("consensus_transfer_leader: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce =
			consensus_transfer_leader(consensusHandle, ConsensusCtl->nodeInfo);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_ADD_FOLLOWER)
	{
		easy_warn_log("consensus_add_follower: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_change_member(consensusHandle,
				ConsensusCtl->nodeInfo, CONFIGURE_MEMBER_ADD_FOLLOWER);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_DROP_FOLLOWER)
	{
		easy_warn_log("consensus_drop_follower: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_change_member(consensusHandle,
				ConsensusCtl->nodeInfo, CONFIGURE_MEMBER_DROP_FOLLOWER);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_ADD_LEARNER)
	{
		easy_warn_log("consensus_add_learner: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_change_learner(consensusHandle,
				ConsensusCtl->nodeInfo, CONFIGURE_LEARNER_ADD_LEARNER);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_DROP_LEARNER)
	{
		easy_warn_log("consensus_drop_learner: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_change_learner(consensusHandle,
				ConsensusCtl->nodeInfo, CONFIGURE_LEARNER_DROP_LEARNER);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_CHANGE_LEARNER_TO_FOLLOWER)
	{
		easy_warn_log("consensus_change_learner_to_follower: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_change_member(consensusHandle,
				ConsensusCtl->nodeInfo, CONFIGURE_MEMBER_CHANGE_LEARNER_TO_FOLLOWER);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_CHANGE_FOLLOWER_TO_LEARNER)
	{
		easy_warn_log("consensus_change_follower_to_learner: %s", ConsensusCtl->nodeInfo);
		ConsensusCtl->responce = consensus_downgrade_member(consensusHandle,
				ConsensusCtl->nodeInfo);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_CHANGE_WEIGHT_CONFIG)
	{
		easy_warn_log("consensus_change_election_weigth: %s, %d",
				ConsensusCtl->nodeInfo, ConsensusCtl->option);
		ConsensusCtl->responce = consensus_configure_member(consensusHandle,
				ConsensusCtl->nodeInfo, false, (int)ConsensusCtl->option);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_REQUEST_VOTE)
	{
		easy_warn_log("consensus_request_vote");
		ConsensusCtl->responce = consensus_request_vote(consensusHandle, true);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_FORCE_SIGNLE_MODE)
	{
		easy_warn_log("consensus_force_single_mode");
		ConsensusCtl->responce = consensus_change_to_single_mode(consensusHandle);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_CHANGE_MATCH_INDEX)
	{
		easy_warn_log("consensus_change_match_index");
		ConsensusCtl->responce = consensus_change_match_index(consensusHandle,
				ConsensusCtl->nodeInfo, ConsensusCtl->option);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_PURGE_LOGS) {
		easy_warn_log("consensus_purge_logs");
		ConsensusCtl->responce = consensus_purge_logs(consensusHandle,
				false, ConsensusCtl->option);
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_FORCE_PURGE_LOGS) {
		easy_warn_log("consensus_force_purge_logs");
		ConsensusLOGTruncateForward(ConsensusCtl->option, true);
		ConsensusCtl->responce = 0;
	}
	else if (ConsensusCtl->rqstType == CONSENSUS_CHANGE_CLUSTER_ID)
	{
		easy_warn_log("consensus_change_cluster_id");
		ConsensusCtl->responce = consensus_change_cluster_id(consensusHandle,
				ConsensusCtl->option);
	}
	else
	{
		pthread_mutex_unlock(&ConsensusCtl->rqst_lck);
		return;
	}

	/* Notify the process. */
	pthread_mutex_lock(ConsensusCtl->wait_cond_mutex);
	if (!(*ConsensusCtl->finished))
	{
		*ConsensusCtl->finished = true;
		pthread_cond_signal(ConsensusCtl->wait_cond);
	}
	pthread_mutex_unlock(ConsensusCtl->wait_cond_mutex);

	ConsensusCtl->rqstType = 0;
	pthread_mutex_unlock(&ConsensusCtl->rqst_lck);
}

static void*
consensus_command_thread(void *parm)
{
	struct timespec abstime;

	while (!shutdown_requested)
	{
		consensus_get_wait_tv(&abstime, CONSENSUS_CHECK_SHUTDOWN_INTERVAL);
		pthread_mutex_lock(&ConsensusCtl->rqst_cond_mutex);
		pthread_cond_timedwait(&ConsensusCtl->rqst_cond,
													 &ConsensusCtl->rqst_cond_mutex, &abstime);
		pthread_mutex_unlock(&ConsensusCtl->rqst_cond_mutex);

		consensus_handle_cmd_request();
	}

	return NULL;
}

static void*
consensus_stat_thread(void *parm)
{
	struct timespec abstime;

	while (!shutdown_requested)
	{
		consensus_get_wait_tv(&abstime, CONSENSUS_CHECK_SHUTDOWN_INTERVAL);
		pthread_mutex_lock(&ConsensusCtl->stat_cond_mutex);
		pthread_cond_timedwait(&ConsensusCtl->stat_cond,
													 &ConsensusCtl->stat_cond_mutex, &abstime);
		pthread_mutex_unlock(&ConsensusCtl->stat_cond_mutex);

		consensus_handle_stat_request();
	}

	return NULL;
}

static void* consensus_append_log_thread(void *parm)
{
	bool do_wait = false;
	struct timespec abstime;

	while (!shutdown_requested)
	{
		do_wait = true;

		if (consensus_get_state() == CONSENSUS_STATE_LEADER)
		{
			do_wait = consensus_try_synchronizing_flush_lsn();
		}

		if (do_wait)
		{
			consensus_get_wait_tv(&abstime, CONSENSUS_SYNC_TIMEOUT);
			pthread_mutex_lock(&ConsensusCtl->sync_cond_mutex);
			if (ConsensusCtl->syncRqstLSN <= consensus_get_appended_lsn())
			{
				ConsensusCtl->syncSuspending = true;
				pthread_cond_timedwait(&ConsensusCtl->sync_cond,
						&ConsensusCtl->sync_cond_mutex, &abstime);
				ConsensusCtl->syncSuspending = false;
			}
			pthread_mutex_unlock(&ConsensusCtl->sync_cond_mutex);
		}
	}

	return NULL;
}

void
assign_dma_xlog_check_timeout(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_set_flush_wait_timeout(newval);
}

void
assign_dma_send_timeout(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_SEND_TIMEOUT, newval);
}

void
assign_dma_pipeline_timeout(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_PIPELINE_TIMEOUT, newval);
}

void
assign_dma_config_change_timeout(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_CONFIG_CHANGE_TIMEOUT, newval);
}

void
assign_dma_delay_election_timeout(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_DELAY_ELECTION_TIMEOUT, newval);
}

void
assign_dma_max_packet_size(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_MAX_PACKET_SIZE, newval);
}

void
assign_dma_new_follower_threshold(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_NEW_FOLLOWER_THRESHOLD, newval);
}

void
assign_dma_max_delay_index(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_MAX_DELAY_INDEX, newval);
}

void
assign_dma_min_delay_index(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_MIN_DELAY_INDEX, newval);
}

void
assign_dma_log_level(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
	{
		consensus_assign_conf_int(consensusHandle, ASSIGN_INT_LOG_LEVEL,
				convert_log_messages_to_easy_level(newval));
	}
}

void
assign_dma_log_keep_size(int newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
	{
		ConsensusLOGSetKeepSize(newval);
	}
}

void
assign_dma_auto_leader_transfer(bool newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_bool(consensusHandle, ASSIGN_BOOL_AUTO_LEADER_TRANSFER, newval);
}

void
assign_dma_disable_election(bool newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_bool(consensusHandle, ASSIGN_BOOL_DISABLE_ELECTION, newval);
}

void
assign_dma_delay_election(bool newval, void *extra)
{
	if (consensusHandle != INVALID_CONSENSUS_HANDLE)
		consensus_assign_conf_bool(consensusHandle, ASSIGN_BOOL_DELAY_ELECTION, newval);
}

const char *
show_polar_dma_repl_password(void)
{
	static char	crypt_pwd[MD5_PASSWD_LEN + 1];

	if (!pg_md5_encrypt(polar_dma_repl_user, polar_dma_repl_password,
				strlen(polar_dma_repl_password), crypt_pwd))
		return "unavailable";

	return crypt_pwd;
}


static bool
consensus_command_disabled_on_logger(DMACommandKind kind)
{
	return (kind == CC_ADD_FOLLOWER) ||
		(kind == CC_DROP_FOLLOWER) ||
		(kind == CC_ADD_LEARNER) ||
		(kind == CC_DROP_LEARNER) ||
		// CC_REQUEST_VOTE
		(kind == CC_CHANGE_LEARNER_TO_FOLLOWER) ||
		(kind == CC_CHANGE_FOLLOWER_TO_LEARNER) ||
		(kind == CC_CHANGE_WEIGHT_CONFIG) ||
		(kind == CC_FORCE_SIGNLE_MODE);
}

bool
PolarDMAUtility(PolarDMACommandStmt *dma_stmt)
{
	char *consensus_node_info;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to execute ALTER SYSTEM command"))));

	if (polar_is_dma_logger_node() &&
			consensus_command_disabled_on_logger(dma_stmt->kind))
	{
		ereport(ERROR, (errmsg("cannot execute this DMA command on logger node")));
	}

	if (dma_stmt->kind == CC_TRANSFER_LEADER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_TRANSFER_LEADER, consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_ADD_FOLLOWER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_ADD_FOLLOWER, consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_ADD_LEARNER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_ADD_LEARNER, consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_DROP_FOLLOWER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_DROP_FOLLOWER, consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_DROP_LEARNER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_DROP_LEARNER, consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_CHANGE_LEARNER_TO_FOLLOWER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_CHANGE_LEARNER_TO_FOLLOWER,
				consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_CHANGE_FOLLOWER_TO_LEARNER)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_CHANGE_FOLLOWER_TO_LEARNER,
				consensus_node_info, NULL);
	}
	else if (dma_stmt->kind == CC_FORCE_SIGNLE_MODE)
	{
		return consensus_wait_for_command(CONSENSUS_FORCE_SIGNLE_MODE, NULL, NULL);
	}
	else if (dma_stmt->kind == CC_CHANGE_WEIGHT_CONFIG)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_CHANGE_WEIGHT_CONFIG, consensus_node_info,
				&dma_stmt->weight);
	}
	else if (dma_stmt->kind == CC_CHANGE_MATCH_INDEX)
	{
		consensus_check_and_convert_member_info(dma_stmt->node, &consensus_node_info);
		return consensus_wait_for_command(CONSENSUS_CHANGE_MATCH_INDEX, consensus_node_info,
				&dma_stmt->matchindex);
	}
	else if (dma_stmt->kind == CC_REQUEST_VOTE)
	{
		return consensus_wait_for_command(CONSENSUS_REQUEST_VOTE, NULL, NULL);
	}
	else if (dma_stmt->kind == CC_PURGE_LOGS)
	{
		return consensus_wait_for_command(CONSENSUS_PURGE_LOGS, NULL, &dma_stmt->purgeindex);
	}
	else if (dma_stmt->kind == CC_FORCE_PURGE_LOGS)
	{
		return consensus_wait_for_command(CONSENSUS_FORCE_PURGE_LOGS, NULL, &dma_stmt->purgeindex);
	}
	else if (dma_stmt->kind == CC_CHANGE_CLUSTER_ID)
	{
		return consensus_wait_for_command(CONSENSUS_CHANGE_CLUSTER_ID, NULL, &dma_stmt->clusterid);
	}

	return false;
}


