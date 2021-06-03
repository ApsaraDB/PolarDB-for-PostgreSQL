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
 * @file consensus_log.h
 * @brief PostgreSQL consensus log manager
 */

#ifndef POLAR_CONSENSUS_LOG_INC
#define POLAR_CONSENSUS_LOG_INC

#include "paxos_log.h"
#include "paxos.pb.h"

using namespace alisql;


/**
 * A persisted file log implementation
 *
 * We refrain from using currentTerm_ inherited from PaxosLog if possible,
 * in case that we switch to true virtual interface of PaxosLog.
 **/
class PolarConsensusLog: public PaxosLog {
public:
	PolarConsensusLog(uint64_t mock_start_index);
	virtual ~PolarConsensusLog();

	int convert_log_operation_to_polar_type(int logOp);
	LogOperation convert_polar_log_type_to_operation(int opType);

	virtual void setTerm(uint64_t term);

	virtual int getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail = false);
	virtual int getEmptyEntry(LogEntry &entry);

	virtual uint64_t getLeftSize(uint64_t startLogIndex);
	virtual bool getLeftSize(uint64_t startLogIndex, uint64_t maxPacketSize);

	virtual uint64_t getLength();

	virtual uint64_t getLastLogIndex();

	virtual uint64_t append(const LogEntry &entry);
	virtual uint64_t append(const ::google::protobuf::RepeatedPtrField<LogEntry> &entries);
	virtual uint64_t appendWithCheck(const LogEntry &entry);
	virtual uint64_t append(const LogEntry &entry, bool check_lsn);

	virtual void truncateBackward(uint64_t firstIndex);
	virtual void truncateForward(uint64_t lastIndex);

	virtual int getMetaData(const std::string &key, uint64_t *value);
	virtual int setMetaData(const std::string &key, const uint64_t value);
	virtual int getMetaData(const std::string &key, std::string &value);
	virtual int setMetaData(const std::string &key, const std::string &value);

private:
	uint64_t m_mock_start_index;
};

#endif // POLAR_CONSENSUS_LOG_INC 
