/*-------------------------------------------------------------------------
 *
 * polar_dma.h
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
 *      src/include/polar_dma/polar_dma.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _POLAR_DMA_MAIN_H
#define _POLAR_DMA_MAIN_H

#include "postgres.h"
#include "access/xlog.h"
#include "nodes/parsenodes.h"
#include "storage/shmem.h"

#include "polar_dma/polar_consensus_stats.h"

typedef enum ConsensusState {
    CONSENSUS_STATE_DOWN = 0, /* the polar_dma process is yet to be set up.*/
    CONSENSUS_STATE_LEADER, /* we are a leader */
    CONSENSUS_STATE_FOLLOWER, /* we are a follower */

    NUM_CONSENSUS_STATE
} ConsensusState;

typedef struct ConsensusProcInfo {
	pthread_cond_t wait_cond;
	pthread_mutex_t wait_cond_mutex;
	bool finished;
	SHM_QUEUE waitQueueElem;
	XLogRecPtr waitLSN;
} ConsensusProcInfo;

typedef struct ConsensusStatus {
	uint32  current_state;
	uint64  term;
	uint64  log_term;
	uint64  xlog_term;
	uint64  leader_id;
	XLogRecPtr sync_rqst_lsn;
	uint32  synced_tli;
	XLogRecPtr synced_lsn;
	XLogRecPtr purge_lsn;
	uint64  commit_index;
	XLogRecPtr flushed_lsn;
	uint32  flushed_tli;
	XLogRecPtr appended_lsn;
	uint32  commit_queue_length;
	uint32  stats_queue_length;
} ConsensusStatus;

typedef struct ConsensusLogStatus {
	uint64  current_term;
	uint64  current_index;
	uint64  sync_index;
	XLogRecPtr last_write_lsn;
	uint32  last_write_timeline;
	uint64  last_append_term;
	uint64  next_append_term;
	uint64  variable_length_log_next_offset;
	bool    disable_purge;
} ConsensusLogStatus;

typedef struct ConsensusMetaStatus {
	uint64  current_term;
	uint64  vote_for;
	uint64  last_leader_term;
	uint64  last_leader_log_index;
	uint64  scan_index;
	uint64  cluster_id;
	uint64  commit_index;
	uint64  purge_index;
} ConsensusMetaStatus;

typedef struct ConsensusStats {
	uint64 transit_waits;
	uint64 xlog_transit_waits;
	uint64 xlog_flush_waits;
	uint64 consensus_appends;
	uint64 consensus_wakeups;
	uint64 consensus_backend_wakeups;
	uint64 consensus_commit_waits;
	uint64 consensus_commit_wait_time;
} ConsensusStats;

typedef struct ConsensusLogStats {
	uint64 log_reads;
	uint64 variable_log_reads;
	uint64 log_appends;
	uint64 variable_log_appends;
	uint64 log_flushes;
	uint64 meta_flushes;
	uint64 xlog_flush_tries;
	uint64 xlog_flush_failed_tries;
	uint64 xlog_transit_waits;
} ConsensusLogStats;

/* GUC settings */
extern bool polar_enable_dma;
extern bool polar_dma_async_commit;
extern bool	polar_dma_force_change_meta;
extern bool polar_dma_init_meta;
extern char	*polar_dma_members_info_string;
extern char	*polar_dma_learners_info_string;
extern char *polar_dma_start_point_string;
extern int	polar_dma_cluster_id;
extern uint64 polar_dma_current_term;
extern int polar_dma_port_deviation;	

extern char *polar_dma_repl_app_name;
extern char *polar_dma_repl_slot_name;
extern char *polar_dma_repl_user;
extern char *polar_dma_repl_password;

extern bool polar_dma_auto_leader_transfer;
extern int 	polar_dma_send_timeout;
extern int 	polar_dma_new_follower_threshold;
extern bool	polar_dma_auto_purge;
extern bool polar_dma_disable_election;
extern bool polar_dma_delay_election;
extern int 	polar_dma_delay_electionTimeout;
extern int 	polar_dma_max_delay_index;
extern int  polar_dma_min_delay_index;
extern int	polar_dma_xlog_check_timeout;
extern int	polar_dma_election_timeout;
extern int 	polar_dma_max_packet_size;
extern int	polar_dma_pipeline_timeout;
extern int	polar_dma_config_change_timeout;
extern int	polar_dma_purge_timeout;
extern int	polar_dma_io_thread_count;	
extern int	polar_dma_hb_thread_count;
extern int 	polar_dma_worker_thread_count;
extern int 	polar_dma_log_slots;
extern int	polar_dma_log_keep_size_mb;

#define POLAR_DMA_MAX_NODES_NUM 32

#define POLAR_DMA_STAT_MEMBER_INFO		(1 << 0)
#define POLAR_DMA_STAT_CLUSTER_INFO 	(1 << 1)
#define POLAR_DMA_STATS_INFO					(1 << 2)

#ifdef USE_DMA
#define POLAR_ENABLE_DMA() polar_enable_dma
#define POLAR_DMA_ASYNC_COMMIT() polar_dma_async_commit
#define POLAR_DMA_REPL_SLOT_NAME() polar_dma_repl_slot_name
#else
#define POLAR_ENABLE_DMA() false
#define POLAR_DMA_ASYNC_COMMIT() false
#define POLAR_DMA_REPL_SLOT_NAME() false
static inline void ConsensusMain(void) { Assert(false); }
static inline bool ConsensusWaitForLSN(XLogRecPtr lsn, bool flush) { Assert(false); return false; }
static inline bool ConsensusCheckpoint(void) { Assert(false); return false; }
static inline void ConsensusWakeupCommit(XLogRecPtr rqstLSN) { Assert(false); }

static inline void ConsensusSetXLogTerm(uint64 term) { Assert(false); }
static inline void ConsensusSetXLogFlushedLSN(XLogRecPtr flush_lsn, TimeLineID flush_timeline, bool force) { Assert(false); }
static inline void ConsensusGetXLogFlushedLSN(XLogRecPtr *flush_lsn, TimeLineID *flush_timeline) { Assert(false); }
static inline XLogRecPtr ConsensusGetPurgeLSN(void) { Assert(false); return InvalidXLogRecPtr; }
static inline XLogRecPtr ConsensusGetSyncedLSN(void) { Assert(false); return InvalidXLogRecPtr; }
static inline void ConsensusGetSyncedLSNAndTLI(XLogRecPtr *lsn, TimeLineID *tli) { Assert(false); }
static inline void ConsensusSetSyncedLSN(XLogRecPtr lsn, TimeLineID tli) { Assert(false); }
#endif

extern Size ConsensusShmemSize(void);
extern void ConsensusShmemInit(void);
extern void ConsensusProcInit(ConsensusProcInfo *procInfo);
extern void ConsensusMain(void);
extern bool ConsensusWaitForLSN(XLogRecPtr lsn, bool flush);
extern void ConsensusWakeupCommit(XLogRecPtr rqstLSN);
extern bool ConsensusCheckpoint(void);

extern uint64 ConsensusGetXLogTerm(void);
extern void ConsensusSetXLogTerm(uint64 term);
extern void ConsensusSetXLogFlushedLSN(XLogRecPtr flush_lsn, TimeLineID flush_timeline, bool force);
extern void ConsensusGetXLogFlushedLSN(XLogRecPtr *flush_lsn, TimeLineID *flush_timeline);
extern XLogRecPtr ConsensusGetPurgeLSN(void);
extern void ConsensusSetPurgeLSN(XLogRecPtr lsn);
extern XLogRecPtr ConsensusGetSyncedLSN(void);
extern void ConsensusGetSyncedLSNAndTLI(XLogRecPtr *lsn, TimeLineID *tli);
extern void ConsensusSetSyncedLSN(XLogRecPtr lsn, TimeLineID tli);

extern bool ConsensusWaitForStatCollect(int info_flags);
extern void ConsensusGetMemberInfo(ConsensusMemberInfo *member_info);
extern int ConsensusGetClusterInfo(ConsensusClusterInfo *cluster_info);
extern void ConsensusGetStatsInfo(ConsensusStatsInfo *stats_info);
extern void ConsensusGetStatus(ConsensusStatus *consensus_status);
extern void ConsensusGetStats(ConsensusStats *consensus_stats);
extern void ConsensusLogGetStats(ConsensusLogStats *log_stats);
extern void ConsensusLogGetStatus(ConsensusLogStatus *log_status);
extern void ConsensusMetaGetStatus(ConsensusMetaStatus *meta_status);

extern bool	PolarDMAUtility(PolarDMACommandStmt *dma_stmt);
extern void assign_dma_xlog_check_timeout(int newval, void *extra);
extern void assign_dma_send_timeout(int newval, void *extra);
extern void assign_dma_pipeline_timeout(int newval, void *extra);
extern void assign_dma_config_change_timeout(int newval, void *extra);
extern void assign_dma_delay_election_timeout(int newval, void *extra);
extern void assign_dma_max_packet_size(int newval, void *extra);
extern void assign_dma_new_follower_threshold(int newval, void *extra);
extern void assign_dma_max_delay_index(int newval, void *extra);
extern void assign_dma_min_delay_index(int newval, void *extra);
extern void assign_dma_log_level(int newval, void *extra);
extern void assign_dma_log_keep_size(int newval, void *extra);
extern void assign_dma_auto_leader_transfer(bool newval, void *extra);
extern void assign_dma_disable_election(bool newval, void *extra);
extern void assign_dma_delay_election(bool newval, void *extra);
extern const char * show_polar_dma_repl_password(void);

#endif /* _POLAR_DMA_MAIN_H */
