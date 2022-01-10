/*-------------------------------------------------------------------------
 *
 * consensus_log.h
 *		consensus logfiles
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
 * 	src/include/polar_dma/consensus_log.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONSENSUS_LOG_H
#define CONSENSUS_LOG_H

#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include "c.h"
#include "access/xlogdefs.h"
#include "port/pg_crc32c.h"

typedef struct ConsensusMetaHeader
{
	uint32  version;
	uint64	current_term;
	uint64	vote_for;
	uint64	last_leader_term;
	uint64	last_leader_log_index;
	uint64 	scan_index;
	uint64	cluster_id;
	uint64 	commit_index;
	uint64  purge_index;
	uint64  mock_start_index;
	uint64  mock_start_tli;
	uint64  mock_start_lsn;
	uint32	member_info_size;
	uint32	learner_info_size;
	pg_crc32c	crc;
} ConsensusMetaHeader;

typedef struct ConsensusLogEntryHeader
{
	uint64 log_term;
	uint64 log_index;
	uint32 op_type;
} ConsensusLogEntryHeader;

typedef struct ConsensusLogEntry
{
	ConsensusLogEntryHeader header;
	uint64 log_lsn; 
	uint32 log_timeline;
	uint32 variable_length;
	uint64 variable_offset;
	uint64 reserved;
	pg_crc32c log_crc;
} ConsensusLogEntry;

typedef union ConsensusLogPayload
{
	struct {
		uint32 log_timeline;
		uint64 log_lsn;
	} fixed_value;
	struct {
		uint32 buffer_size;
		char *buffer;
	} variable_value;
} ConsensusLogPayload;

typedef struct FixedLengthLogPageTrailer
{
	uint64  last_term;     
	TimeLineID start_timeline;     
	XLogRecPtr start_lsn; 
	uint64	start_offset;
	uint64  end_offset; 
	uint64  end_entry; 
} FixedLengthLogPageTrailer;


#define DMA_META_VERSION 1 

#define ConsensusEntryNormalType 0
#define ConsensusEntryNopType 1 
#define ConsensusEntryConfigChangeType 2 

#define CurrentTermMetaKey 0
#define VoteForMetaKey 1 
#define LastLeaderTermMetaKey 2 
#define LastLeaderIndexMetaKey 3 
#define ScanIndexMetaKey 4 
#define ClusterIdMetaKey 5 
#define CommitIndexMetaKey 6 
#define PurgeIndexMetaKey 7
#define MockStartIndex 8
#define MockStartTLI 9
#define MockStartLSN 10

#define CONSENSUS_ENTRY_IS_FIXED(entry_type) \
 			((entry_type) == ConsensusEntryNormalType ||  \
			 (entry_type) == ConsensusEntryNopType)

#define CONSENSUS_LOG_PAGE_SIZE 8192
#define CONSENSUS_CC_LOG_PAGE_SIZE 8192 

#define FIXED_LENGTH_LOGS_PER_PAGE  \
	((CONSENSUS_LOG_PAGE_SIZE - sizeof(FixedLengthLogPageTrailer)) / sizeof(ConsensusLogEntry))

#define MEMBER_INFO_MAX_LENGTH 1024

extern Size ConsensusLOGShmemSize(void);
extern void ConsensusLOGShmemInit(void);

extern bool ConsensusLOGStartup(void);

extern void ConsensusLOGNormalPayloadInit(ConsensusLogPayload *log_payload, 
							uint64 log_lsn, uint32 log_timeline);

extern bool ConsensusLOGAppend(ConsensusLogEntryHeader *log_entry_header, 
							ConsensusLogPayload *log_payload, 
							uint64* consensus_index, bool flush, bool check_lsn);
extern bool ConsensusLOGRead(uint64_t log_index, 
							ConsensusLogEntryHeader *log_entry_header, 
							ConsensusLogPayload *log_payload,
							bool fixed_payload);
extern bool ConsensusLOGFlush(uint64 pageno);

extern void ConsensusLOGTruncateForward(uint64 log_index, bool force);
extern bool ConsensusLOGTruncateBackward(uint64 log_index);

extern bool consensus_log_get_entry_lsn(uint64 log_index, 
							XLogRecPtr *lsn, TimeLineID *tli);

extern uint64 ConsensusLOGGetLength(void);

extern void ConsensusLOGSetDisablePurge(bool disable);
extern uint64 ConsensusLOGGetLastIndex(void);
extern void ConsensusLOGSetLogTerm(uint64 term);
extern uint64 ConsensusLOGGetTerm(void);
extern void ConsensusLOGSetTerm(uint64 term);
extern uint64 ConsensusLOGGetLogTerm(void);
extern void ConsensusLOGGetLastLSN(XLogRecPtr *last_write_lsn, 
							TimeLineID *last_write_timeline);
extern uint32 ConsensusLOGGetKeepSize(void);
extern void ConsensusLOGSetKeepSize(uint32 log_keep_size);

extern bool ConsensusMetaStartup(void);

extern bool ConsensusMetaForceChange(int current_term,
							char *cluster_info,
							int cluster_info_size,
							char *learner_info,
							int learner_info_size,
							uint64 mock_start_index,
							TimeLineID mock_start_tli,
							XLogRecPtr mock_start_lsn,
							bool is_learner,
							bool reset);

extern bool ConsensusMetaGetInt64(int key, uint64 *value);
extern bool ConsensusMetaSetInt64(int key, uint64 value, bool flush);
extern void ConsensusMetaSetMemberInfo(char *member_info, int member_info_size, bool flush);
extern int ConsensusMetaGetMemberInfo(char **member_info);
extern int ConsensusMetaGetMemberInfoFromArray(char **member_info);
extern void ConsensusMetaSetLearnerInfo(char *learner_info, int learner_info_size, bool flush);
extern int ConsensusMetaGetLearnerInfo(char **learner_info);
extern int ConsensusMetaGetLearnerInfoFromArray(char **learner_info);

extern bool ConsensusMetaFlush(void);


#endif //CONSENSUS_LOG_H
