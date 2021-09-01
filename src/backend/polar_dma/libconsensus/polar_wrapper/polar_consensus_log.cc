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
 * @file polar_consensus_log.cc
 * @brief Implementation of Persisted File Log.
 */

#include <unistd.h>
#include "easy_log.h"
#include "paxos.h"
#include "polar_consensus_log.h"

extern "C" {
#include "polar_dma/consensus_log.h"
} // extern "C"

using namespace alisql;

PolarConsensusLog::PolarConsensusLog(uint64_t mock_start_index) :
	PaxosLog()
{
	m_mock_start_index = mock_start_index;
}

PolarConsensusLog::~PolarConsensusLog()
{
}

int
PolarConsensusLog::convert_log_operation_to_polar_type(int logOp)
{
	if (logOp == kNormal)
	{
		return ConsensusEntryNormalType; 
	}
	else if (logOp == kNop)
	{
		return ConsensusEntryNopType; 
	}
	else if (logOp == kConfigureChange)
	{
		return ConsensusEntryConfigChangeType; 
	}
	else
	{
		easy_info_log("unexpected consensus log operation.");
		abort();
	}
	return ConsensusEntryNormalType; 
}

LogOperation
PolarConsensusLog::convert_polar_log_type_to_operation(int opType)
{
	if (opType == ConsensusEntryNormalType)
	{
		return kNormal;
	}
	if (opType == ConsensusEntryNopType)
	{
		return kNop;
	}
	if (opType == ConsensusEntryConfigChangeType)
	{
		return kConfigureChange; 
	}
	else
	{
		easy_info_log("unexpected consensus log type.");
		abort();
	}
	return kNormal;
}

void PolarConsensusLog::setTerm(uint64_t term)
{
	ConsensusLOGSetTerm(term);
}

int 
PolarConsensusLog::getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail)
{
	ConsensusLogEntryHeader log_entry_header;
	ConsensusLogPayload log_payload;
	LogOperation log_op;

	if (logIndex < m_mock_start_index)
	{
		/* log entry before mock start index */
		entry.set_term(0);
		entry.set_index(logIndex);
		entry.set_ikey("");
		entry.set_optype(kMock);
		entry.set_value("");
		return 0;
	}
	
	if (!ConsensusLOGRead(logIndex, &log_entry_header, &log_payload, false))
	{
		/* log not exists or have been truncated */
		entry.set_optype(kMock);
		return -1;
	}

	log_op = convert_polar_log_type_to_operation(log_entry_header.op_type);

	entry.set_term(log_entry_header.log_term);
	entry.set_index(logIndex);
	entry.set_ikey("");
	entry.set_optype(log_op);

	if (CONSENSUS_ENTRY_IS_FIXED(log_entry_header.op_type))
	{
		entry.set_value(&log_payload.fixed_value, sizeof(log_payload.fixed_value));
	}
	else
	{
		entry.set_value(log_payload.variable_value.buffer, 
										log_payload.variable_value.buffer_size);
		free(log_payload.variable_value.buffer);
	}

	return 0;
}

uint64_t PolarConsensusLog::getLeftSize(uint64_t startLogIndex)
{
	uint64_t lastLogIndex = getLastLogIndex();

	if (lastLogIndex > startLogIndex)
		return (lastLogIndex - startLogIndex) * 
			(sizeof(ConsensusLogEntryHeader) + sizeof(ConsensusLogPayload));
	else
		return 0;
}

bool PolarConsensusLog::getLeftSize(uint64_t startLogIndex, uint64_t maxPacketSize)
{
	uint64_t lastLogIndex = getLastLogIndex();
	uint64_t size = 0;

	if (lastLogIndex > startLogIndex)
	{
		size = (lastLogIndex - startLogIndex) * 
			(sizeof(ConsensusLogEntryHeader) + sizeof(ConsensusLogPayload));
	}

	if (size >= maxPacketSize)
		return true;
	else
		return false;
}

int
PolarConsensusLog::getEmptyEntry(LogEntry &entry)
{
	ConsensusLogPayload log_payload;

	memset(&log_payload, 0, sizeof(log_payload));

	entry.set_term(0);
	entry.set_index(0);
	entry.set_optype(kNop);
	entry.set_ikey("");
	entry.set_value(&log_payload.fixed_value, sizeof(log_payload.fixed_value));
	return 0;
}

uint64_t
PolarConsensusLog::getLastLogIndex()
{
	return ConsensusLOGGetLastIndex();
}

uint64_t
PolarConsensusLog::append(const LogEntry &entry)
{
	return append(entry, true);
}

uint64_t
PolarConsensusLog::append(const LogEntry &entry, bool check_lsn)
{
	ConsensusLogEntryHeader log_entry_header;
	ConsensusLogPayload 		log_payload; 
	uint64 	log_index = 0;	
	int			log_type;	

	log_type = convert_log_operation_to_polar_type(entry.optype());
	log_entry_header.log_term = entry.term();
	log_entry_header.log_index = entry.index();
	log_entry_header.op_type = log_type;

	if (CONSENSUS_ENTRY_IS_FIXED(log_type))
	{
		const ConsensusLogPayload *log_payload_ptr = 
			(const ConsensusLogPayload *) entry.value().data();
		assert(entry.value().size() == sizeof(ConsensusLogPayload));
		log_payload = *log_payload_ptr; 
	}
	else
	{
		log_payload.variable_value.buffer = const_cast<char *>(entry.value().data());
		log_payload.variable_value.buffer_size = entry.value().size();
	}	

	(void) ConsensusLOGAppend(&log_entry_header, &log_payload, &log_index, 
														true, check_lsn);

	return log_index;
}

uint64_t
PolarConsensusLog::append(const ::google::protobuf::RepeatedPtrField<LogEntry> &entries)
{
	ConsensusLogEntryHeader log_entry_header;
	ConsensusLogPayload 		log_payload; 
	uint64 	log_index = 0;	
	int			log_type;	

	for (auto it= entries.begin(); it != entries.end(); ++it)
	{
		const LogEntry &entry = *it;
		log_type = convert_log_operation_to_polar_type(entry.optype());
		log_entry_header.log_term = entry.term();
		log_entry_header.log_index = entry.index();
		log_entry_header.op_type = log_type;

		if (CONSENSUS_ENTRY_IS_FIXED(log_type))
		{
			const ConsensusLogPayload *log_payload_ptr = 
				(const ConsensusLogPayload *) entry.value().data();
			assert(entry.value().size() == sizeof(ConsensusLogPayload));
			log_payload = *log_payload_ptr; 
		}
		else
		{
			log_payload.variable_value.buffer = const_cast<char *>(entry.value().data());
			log_payload.variable_value.buffer_size = entry.value().size();
		}	

		/* terminate if append error */
		if (!ConsensusLOGAppend(&log_entry_header, &log_payload, &log_index, false, true))
		{
			break;
		}
	}

	if (!ConsensusLOGFlush(0))
	{
		log_index = 0;
		abort();
	}
	
	return log_index;
}

uint64_t
PolarConsensusLog::appendWithCheck(const LogEntry &entry)
{
	return append(entry, false);
}

uint64_t
PolarConsensusLog::getLength()
{
	return ConsensusLOGGetLength();
}

void
PolarConsensusLog::truncateBackward(uint64_t firstIndex)
{
	(void) ConsensusLOGTruncateBackward(firstIndex);
}

void
PolarConsensusLog::truncateForward(uint64_t lastIndex)
{
	(void) ConsensusLOGTruncateForward(lastIndex, false);
}

int
PolarConsensusLog::getMetaData(const std::string &key, uint64_t *value)
{
	bool ok;

	if (key == Paxos::keyCurrentTerm)
	{
		ok = ConsensusMetaGetInt64(CurrentTermMetaKey, value);
	}
	else if (key == Paxos::keyVoteFor)
	{
		ok = ConsensusMetaGetInt64(VoteForMetaKey, value);
	}
	else if (key == Paxos::keyLastLeaderTerm)
	{
		ok = ConsensusMetaGetInt64(LastLeaderTermMetaKey, value);
	}
	else if (key == Paxos::keyLastLeaderLogIndex)
	{
		ok = ConsensusMetaGetInt64(LastLeaderIndexMetaKey, value);
	}
	else if (key == Paxos::keyScanIndex)
	{
		ok = ConsensusMetaGetInt64(ScanIndexMetaKey, value);
	}
	else if (key == Paxos::keyClusterId)
	{
		ok = ConsensusMetaGetInt64(ClusterIdMetaKey, value);
	}
	else
	{
		assert(false);
		ok = false;
	}

	return ok ? 0 : -1;
}

int
PolarConsensusLog::setMetaData(const std::string &key, const uint64_t value)
{
	bool ok;

	if (key == Paxos::keyCurrentTerm)
	{
		ok = ConsensusMetaSetInt64(CurrentTermMetaKey, value, true);
	}
	else if (key == Paxos::keyVoteFor)
	{
		ok = ConsensusMetaSetInt64(VoteForMetaKey, value, true);
	}
	else if (key == Paxos::keyLastLeaderTerm)
	{
		ok = ConsensusMetaSetInt64(LastLeaderTermMetaKey, value, true);
	}
	else if (key == Paxos::keyLastLeaderLogIndex)
	{
		ok = ConsensusMetaSetInt64(LastLeaderIndexMetaKey, value, true);
	}
	else if (key == Paxos::keyScanIndex)
	{
		ok = ConsensusMetaSetInt64(ScanIndexMetaKey, value, true);
	}
	else if (key == Paxos::keyClusterId)
	{
		ok = ConsensusMetaSetInt64(ClusterIdMetaKey, value, true);
	}
	else
	{
		assert(false);
		ok = false;
	}

	return ok ? 0: -1;
}

int
PolarConsensusLog::getMetaData(const std::string &key, std::string &value)
{
	int 	ret = 0;
	char 	*value_ptr;
	int		value_size = 0;

	if (key == Paxos::keyMemberConfigure)
	{
		value_size = ConsensusMetaGetMemberInfo(&value_ptr);
	}
	else if (key == Paxos::keyLearnerConfigure)
	{
		value_size = ConsensusMetaGetLearnerInfo(&value_ptr);
	}
	else
	{
		assert(false);
		ret = -1;
	}

	if (value_size > 0)
	{
		assert(value_size > 1);
		value.assign(value_ptr, value_size-1);
		free(value_ptr);
	}
	else
	{
		value.assign("");
	}

	return ret;
}

int
PolarConsensusLog::setMetaData(const std::string &key, const std::string &value)
{
	int 	ret = 0;

	if (key == Paxos::keyMemberConfigure)
	{
		ConsensusMetaSetMemberInfo(const_cast<char *>(value.c_str()), 
										value == "" ? 0 : value.size() + 1, true);
	}
	else if (key == Paxos::keyLearnerConfigure)
	{
		ConsensusMetaSetLearnerInfo(const_cast<char *>(value.c_str()), 
										value == "" ? 0 : value.size() + 1, true);
		ret = -1;
	}
	else
	{
		assert(false);
		ret = -1;
	}

	return ret;
}

