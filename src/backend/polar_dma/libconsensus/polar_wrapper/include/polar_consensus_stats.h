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
 *      src/include/polar_dma/polar_consensus_stats.h
 *-------------------------------------------------------------------------
 */

#ifndef CONSENSUS_STATS_H
#define CONSENSUS_STATS_H

#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif
typedef struct ConsensusMemberInfo {
	uint64_t serverId;
	uint64_t currentTerm;
	uint64_t currentLeader;
	uint64_t commitIndex;
	uint64_t lastLogTerm;
	uint64_t lastLogIndex;
	int      role;
	uint64_t votedFor;
	uint64_t lastAppliedIndex;
} ConsensusMemberInfo;

typedef struct ConsensusClusterInfo {
	uint64_t serverId;
	char     ipPort[256];
	uint64_t matchIndex;
	uint64_t nextIndex;
	int      role;
	uint64_t hasVoted;
	bool     forceSync;
	uint     electionWeight;
	uint64_t learnerSource;
	uint64_t appliedIndex;
	bool     pipelining;
	bool     useApplied;
} ConsensusClusterInfo;

typedef struct ConsensusStatsInfo {
	uint64_t serverId;
	uint64_t countMsgAppendLog;
	uint64_t countMsgRequestVote;
	uint64_t countHeartbeat;
	uint64_t countOnMsgAppendLog;
	uint64_t countOnMsgRequestVote;
	uint64_t countOnHeartbeat;
	uint64_t countReplicateLog;
} ConsensusStatsInfo;

#ifdef __cplusplus
} // extern "C"
#endif

#endif  /* CONSENSUS_STATS_H */
