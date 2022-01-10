/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *      src/backend/polar_dma/include/polar_consensus_c.h
 *-------------------------------------------------------------------------
 */

#ifndef CONSENSUS_C_H
#define CONSENSUS_C_H

#include <stdint.h>
#include <sys/types.h>

#include "polar_consensus_stats.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * CONSENSUS_HANDLE is the opaque reference to an instance in the consensus protocol. 
 */
typedef uintptr_t CONSENSUS_HANDLE;

#define INVALID_CONSENSUS_HANDLE ((CONSENSUS_HANDLE) 0)

#define CONFIGURE_MEMBER_ADD_FOLLOWER 1
#define CONFIGURE_MEMBER_DROP_FOLLOWER 2
#define CONFIGURE_MEMBER_CHANGE_LEARNER_TO_FOLLOWER 3

#define CONFIGURE_LEARNER_ADD_LEARNER 1
#define CONFIGURE_LEARNER_DROP_LEARNER 2

#define ASSIGN_BOOL_DISABLE_ELECTION 1
#define ASSIGN_BOOL_DELAY_ELECTION 2
#define ASSIGN_BOOL_AUTO_LEADER_TRANSFER 3

#define ASSIGN_INT_SEND_TIMEOUT 1
#define ASSIGN_INT_PIPELINE_TIMEOUT 2
#define ASSIGN_INT_CONFIG_CHANGE_TIMEOUT 3
#define ASSIGN_INT_DELAY_ELECTION_TIMEOUT 4
#define ASSIGN_INT_MAX_PACKET_SIZE 5 
#define ASSIGN_INT_NEW_FOLLOWER_THRESHOLD 5
#define ASSIGN_INT_MAX_DELAY_INDEX 7
#define ASSIGN_INT_MIN_DELAY_INDEX 8
#define ASSIGN_INT_LOG_LEVEL 9

/*
 * Create an instance in the consensus protocol. After the call, it is not yet
 * configured in any cluster. Timeouts are in milliseconds.
 *
 * default value of electionTimeout = 5 (s)
 *
 * default value of purgeTimeout = 10 (s).
 *
 * @return A handle to the instance or INVALID_CONSENSUS_HANDLE on error.
 *
 */
extern CONSENSUS_HANDLE consensus_create_instance(
		uint64_t mockStartIndex,
		uint64_t electionTimeout,
		uint64_t purgeTimeout,
		uint64_t maxPacketSize, 
		uint64_t pipelineTimeout, 
		uint64_t configureChangeTimeout, 
		uint32_t logLevel);

/*
 * Initialize the instance as a peer. If serverPortList is not NULL, it is
 * assumed to be the current cluster configuration. Otherwise, the persisted
 * one will be loaded. It is an error if both cannot be found.
 * 
 * @return 0 on success, or other values on error
 */
extern int consensus_init_as_peer(CONSENSUS_HANDLE handle,
		bool isLogger,
		unsigned IOThreadCount,
		unsigned heartbeatThreadCount,
		unsigned workThreadCount,
		bool autoLeaderTransfer,
		bool autoPurge,
		bool delayElection,
		uint64_t delayElectiionTimeout);

/*
 * Initialize the instance as a learner.
 *
 * @return 0 on success, or other values on error
 */
extern int consensus_init_as_learner(CONSENSUS_HANDLE handle,
		bool isLogger,
		unsigned IOThreadCount,
		unsigned heartbeatThreadCount,
		unsigned workThreadCount,
		bool autoLeaderTransfer,
		bool autoPurge,
		bool delayElection,
		uint64_t delayElectiionTimeout);

extern int consensus_assign_conf_int(CONSENSUS_HANDLE handle, int param_key, int value);
extern void consensus_assign_conf_bool(CONSENSUS_HANDLE handle, int param_key, bool value);

extern int consensus_request_vote(CONSENSUS_HANDLE handle, bool force);
extern int consensus_transfer_leader(CONSENSUS_HANDLE handle, const char* nodeInfo);
extern int consensus_reset_election_delay(CONSENSUS_HANDLE handle);
extern int consensus_downgrade_member(CONSENSUS_HANDLE handle, const char* nodeInfo);
extern int consensus_change_member(CONSENSUS_HANDLE handle, const char* nodeInfo, int opType);
extern int consensus_change_learner(CONSENSUS_HANDLE handle, const char* nodeInfo, int opType);
extern int consensus_configure_member(CONSENSUS_HANDLE handle, const char* nodeInfo, 
		bool forceSync, int electionWeight);
extern int consensus_change_to_single_mode(CONSENSUS_HANDLE handle);
extern int consensus_change_match_index(CONSENSUS_HANDLE handle, const char* nodeInfo,
		uint64_t matchIndex);
extern int consensus_purge_logs(CONSENSUS_HANDLE handle, bool local, uint64_t purgeIndex);
extern int consensus_change_cluster_id(CONSENSUS_HANDLE handle, uint64_t clusterID);

/*
 * Try appending a log entry. If it is successful, index and term of the
 * proposed log entry will be stored in *index and *term if they are not NULL.
 *
 * Possible error code:
 * CONSENSUS_NOT_A_LEADER
 *
 * @return 0 on success, or error codes
 */
extern int consensus_append_normal_entry(
		CONSENSUS_HANDLE handle,
		uint64_t *index,
		uint64_t *term,
		const char *value,
		size_t len);

/*
 * Wait for the commit index to catch up to at least index.
 *
 * @return 0 if failed, or the latest commit index
 */
extern uint64_t consensus_wait_for_commit(CONSENSUS_HANDLE handle,
		uint64_t index,
		uint64_t term);

/*
 * Atomically get the commit index.
 */
extern uint64_t consensus_get_commit_index(CONSENSUS_HANDLE handle);

extern uint64_t consensus_get_server_id(CONSENSUS_HANDLE handle);

/*
 * Get the ID of the current leader whom we believe to be.
 */
extern void consensus_get_leader(CONSENSUS_HANDLE handle, 
										uint64_t *local_id, 
										uint64_t *leader_id, 
										uint64_t *term,
										char *leader_addr,
										int leader_addr_len);

extern int consensus_shutdown(CONSENSUS_HANDLE handle);

extern const char* consensus_get_error_message(int error);

extern void consensus_get_member_info(CONSENSUS_HANDLE handle, 
									 ConsensusMemberInfo *info);
extern void consensus_get_stats_info(CONSENSUS_HANDLE handle, 
									 ConsensusStatsInfo *sinfo);
extern int consensus_get_cluster_info(CONSENSUS_HANDLE handle, 
									  ConsensusClusterInfo *cinfo,
									  int maxCluster);

/*
 * Public error codes that the callers might be concerned about.
 */
enum ConsensusErrorCode {
    CONSENSUS_NO_ERROR = 0,             /* No Error, must be 0*/
    CONSENSUS_INIT_ERROR,               /* not a leader */
    CONSENSUS_NO_VOTE_REQUESTED,        /* unable to request vote */
    CONSENSUS_NOT_LEADER,               /* not a leader */

    CONSENSUS_NUM_ERROR_CODES           /* number of error codes; MUST BE LAST */
};

#ifdef __cplusplus
} // extern "C"
#endif

#endif  /* CONSENSUS_C_H */
