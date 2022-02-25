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
 * @file polar_consensus_c.cc
 * @brief Implementation of concensus C wrapper with XPaxos
 */

#include <unistd.h>
#include "easy_log.h"
#include "paxos.h"
#include "paxos_server.h"
#include "polar_consensus_c.h"
#include "polar_consensus_log.h"

class ConsensusContext {
	public:
		ConsensusContext();
		~ConsensusContext();

		int init(uint64_t mockStartIndex, 
				uint64_t electionTimeout,
				uint64_t purgeTimeout,
				uint64_t maxPacketSize, 
				uint64_t pipelineTimeout, 
				uint64_t configureChangeTimeout, 
				uint32_t logLevel);

		int start_as_peer(bool isLogger,
				unsigned IOThreadCount,
				unsigned heartbeatThreadCount,
				unsigned workThreadCount,
				bool autoLeaderTransfer,
				bool autoPurge,
				bool delayElection,
				uint64_t delayElectiionTimeout,
				bool learner);

		void assign_conf_bool(int param_key, bool value);
		int assign_conf_int(int param_key, int new_value);

		int shutdown();

		int append_normal_entry(
				uint64_t *index,
				uint64_t *term,
				const char *value,
				size_t len);

		uint64_t wait_for_commit(uint64_t index, uint64_t term);

		uint64_t get_commit_index();
		uint64_t get_current_term();
		void get_current_leader(uint64_t *local_id, uint64_t *leader_id, uint64_t *term, 
				char *leader_addr, int leader_addr_len);
		uint64_t get_current_server_id();

		void get_member_info(ConsensusMemberInfo *info);
		int get_cluster_info(ConsensusClusterInfo cinfo[10], int maxCluster);
		void get_stats_info(ConsensusStatsInfo *sinfo);

		int transfer_leader(const char* node_info);
		int downgrade_member(const char* node_info);
		int change_member(const char* node_info, int op_type);
		int change_learner(const char* node_info, int op_type);
		int configure_member(const char* node_info, bool force_sync, int weight);
		int request_vote(bool force);
		int change_to_single_mode();
		int change_match_index(const char* node_info, uint64_t match_index);
		int purge_logs(bool local, uint64_t purge_index);
		int change_cluster_id(uint64_t cluster_id);

		int reset_election_delay();

	private:
		bool                m_initialized;
		PolarConsensusLog		*m_log;
		alisql::Paxos       *m_paxos;
		std::shared_ptr<alisql::AliSQLServer> m_local_server;
};


ConsensusContext::ConsensusContext()
	: m_initialized(false),
	m_log(nullptr),
	m_paxos(nullptr),
	m_local_server(nullptr)
{
}

ConsensusContext::~ConsensusContext()
{
	if (m_log)
		delete m_log;
	if (m_paxos)
		delete m_paxos;
	if (m_local_server)
		m_local_server.reset();
}

int
ConsensusContext::init(uint64_t mockStartIndex,
		uint64_t electionTimeout, uint64_t purgeTimeout,
		uint64_t maxPacketSize, uint64_t pipelineTimeout, 
		uint64_t configureChangeTimeout, uint32_t logLevel)
{
	assert(!m_initialized);

	m_log = new PolarConsensusLog(mockStartIndex);
	if (!m_log)
	{
		return CONSENSUS_INIT_ERROR;
	}

	m_local_server = std::shared_ptr<alisql::AliSQLServer>(new alisql::AliSQLServer(0));
	if (m_local_server == nullptr)
	{
		delete m_log;
		m_log = nullptr;
		return CONSENSUS_INIT_ERROR;
	}

	m_paxos = new alisql::Paxos(electionTimeout,
			std::shared_ptr<alisql::PaxosLog>(m_log, [](void*){}),
			purgeTimeout);
	if (!m_paxos) 
	{
		delete m_log;
		m_log = nullptr;
		m_local_server.reset();
		m_local_server = nullptr;
		return CONSENSUS_INIT_ERROR;
	}

	m_paxos->setMaxPacketSize(maxPacketSize);
	m_paxos->setPipeliningTimeout(pipelineTimeout);
	m_paxos->setAlertLogLevel(alisql::Paxos::AlertLogLevel(logLevel));
	m_paxos->setConfigureChangeTimeout(configureChangeTimeout);

	m_initialized = true;
	return 0;
}

int
ConsensusContext::start_as_peer(bool isLogger, unsigned IOThreadCount, 
		unsigned heartbeatThreadCount, unsigned workThreadCount,
		bool autoLeaderTransfer, bool autoPurge, bool delayElection, 
		uint64_t delayElectionTimeout, bool learner)
{
	std::vector<std::string> servers;
	std::string server;
	int ret;

	/* 
	 * Set up Paxos as a peer to other servers.
	 */
	if (!learner)
		ret = m_paxos->init(servers, 0, NULL, IOThreadCount, workThreadCount, 
				delayElection, delayElectionTimeout, m_local_server, 
				false, heartbeatThreadCount);
	else 
		ret = m_paxos->initAsLearner(server, NULL, IOThreadCount, workThreadCount, 
				delayElection, delayElectionTimeout, m_local_server, 
				false, heartbeatThreadCount);
	if (ret)
	{
		easy_error_log("Polar consensus initialize x-paxos failed, return: %d", ret);
		return CONSENSUS_INIT_ERROR;
	}

	m_paxos->initAutoPurgeLog(autoPurge, false, NULL);
	m_paxos->setAsLogType(isLogger);
	m_paxos->setEnableAutoLeaderTransfer(autoLeaderTransfer);

	return 0;
}

void
ConsensusContext::assign_conf_bool(int param_key, bool value)
{
	if (param_key == ASSIGN_BOOL_DISABLE_ELECTION)
	{
		m_paxos->debugDisableElection = value;
		m_paxos->debugDisableStepDown = value;
	}
	else if (param_key == ASSIGN_BOOL_DELAY_ELECTION)
	{
		m_paxos->setEnableDelayElection(value); 
	}
	else if (param_key == ASSIGN_BOOL_AUTO_LEADER_TRANSFER)
	{
		m_paxos->setEnableAutoLeaderTransfer(value); 
	}
}

int
ConsensusContext::assign_conf_int(int param_key, int value)
{
	if (param_key == ASSIGN_INT_SEND_TIMEOUT)
	{
		m_paxos->setSendPacketTimeout(value); 
	}
	else if (param_key == ASSIGN_INT_PIPELINE_TIMEOUT)
	{
		m_paxos->setPipeliningTimeout(value); 
	}
	else if (param_key == ASSIGN_INT_CONFIG_CHANGE_TIMEOUT)
	{
		m_paxos->setConfigureChangeTimeout(value); 
	}
	else if (param_key == ASSIGN_INT_DELAY_ELECTION_TIMEOUT)
	{
		m_paxos->setDelayElectionTimeout(value); 
	}
	else if (param_key == ASSIGN_INT_MAX_PACKET_SIZE)
	{
		m_paxos->setMaxPacketSize(value); 
	}
	else if (param_key == ASSIGN_INT_NEW_FOLLOWER_THRESHOLD)
	{
		m_paxos->setMaxDelayIndex4NewMember(value); 
	}
	else if (param_key == ASSIGN_INT_MAX_DELAY_INDEX)
	{
		m_paxos->setMaxDelayIndex(value); 
	}
	else if (param_key == ASSIGN_INT_MIN_DELAY_INDEX)
	{
		m_paxos->setMinDelayIndex(value); 
	}
	else if (param_key == ASSIGN_INT_LOG_LEVEL)
	{
		m_paxos->setAlertLogLevel(alisql::Paxos::AlertLogLevel(value)); 
	}

	return 0;
}

int
ConsensusContext::shutdown()
{
	m_paxos->shutdown();
	return 0;
}

int
ConsensusContext::request_vote(bool force)
{
	return m_paxos->requestVote(force);
}

int
ConsensusContext::append_normal_entry(
		uint64_t *index,
		uint64_t *term,
		const char *value,
		size_t len)
{
	uint64_t index_t;

	alisql::LogEntry entry;
	entry.set_index(0);
	/* We do look at the op type to distinguish between user content and empty logs. */
	entry.set_optype(alisql::kNormal);
	entry.set_value(value, len);

	index_t = m_paxos->replicateLog(entry);
	if (!index_t)
	{
		easy_error_log("Polar consensus x-paxos replicate log failed");
		return CONSENSUS_NOT_LEADER;
	}

	if (index)
	{
		*index = index_t;
	}
	if (term)
	{
		*term = entry.term();
	}

	return 0;
}

uint64_t
ConsensusContext::wait_for_commit(uint64_t index, uint64_t term)
{
	return m_paxos->waitCommitIndexUpdate(index - 1, term);
}

uint64_t
ConsensusContext::get_commit_index()
{
	return m_paxos->getCommitIndex();
}

void
ConsensusContext::get_current_leader(uint64_t *local_id, uint64_t *leader_id, 
		uint64_t *term, char *leader_addr, int leader_addr_len)
{
	std::string leader_addr_str;
	m_paxos->getCurrentLeaderWithTerm(local_id, leader_id, term, leader_addr_str);
	strncpy(leader_addr, leader_addr_str.c_str(), leader_addr_len);
}

uint64_t
ConsensusContext::get_current_server_id()
{
	return m_local_server->serverId;
}

uint64_t
ConsensusContext::get_current_term()
{
	/* Internally it loads the variable with seq_cst. */
	return m_paxos->getTerm();
}

int
ConsensusContext::reset_election_delay()
{
	m_paxos->resetDelayElection();
	return 0;
}

int
ConsensusContext::transfer_leader(const char* node_info)
{
	return m_paxos->leaderTransfer(std::string(node_info));
}

int
ConsensusContext::downgrade_member(const char* node_info)
{
	return m_paxos->downgradeMember(std::string(node_info));
}

int
ConsensusContext::change_member(const char* node_info, int op_type)
{
	std::string node_str = node_info;

	if (op_type == CONFIGURE_MEMBER_ADD_FOLLOWER)
		return m_paxos->changeMember(
				alisql::Paxos::CCOpType::CCAddLearnerAutoChange, node_str);
	else if (op_type == CONFIGURE_MEMBER_DROP_FOLLOWER)
		return m_paxos->changeMember(
				alisql::Paxos::CCOpType::CCDelNode, node_str);
	else if (op_type == CONFIGURE_MEMBER_CHANGE_LEARNER_TO_FOLLOWER)
		return m_paxos->changeMember(
				alisql::Paxos::CCOpType::CCAddNode, node_str);

	assert(false);
	return 0;
}

int
ConsensusContext::change_learner(const char* node_info, int op_type)
{
	std::vector<std::string> node_vector;
	node_vector.push_back(std::string(node_info));

	if (op_type == CONFIGURE_LEARNER_ADD_LEARNER)
		return m_paxos->changeLearners(alisql::Paxos::CCOpType::CCAddNode, node_vector);
	else if (op_type == CONFIGURE_LEARNER_DROP_LEARNER)
		return m_paxos->changeLearners(alisql::Paxos::CCOpType::CCDelNode, node_vector);

	assert(false);
	return 0;
}

int
ConsensusContext::configure_member(const char* node_info, 
		bool force_sync, int weight)
{
	std::string node_str = node_info;
	return m_paxos->configureMember(node_str, force_sync, weight);
}

int
ConsensusContext::change_match_index(const char* node_info, uint64_t match_index)
{
	m_paxos->forceFixMatchIndex(node_info, match_index);
	return alisql::PaxosErrorCode::PE_NONE;
}

int
ConsensusContext::purge_logs(bool local, uint64_t purge_index)
{
	int ret;

	if (purge_index > 0)
		ret = m_paxos->forcePurgeLog(local, purge_index);
	else
		ret = m_paxos->forcePurgeLog(local);

	return ret;
}

int
ConsensusContext::change_cluster_id(uint64_t cluster_id)
{
	int ret = m_paxos->setClusterId(cluster_id);
	return (ret == 0) ? 
		alisql::PaxosErrorCode::PE_NONE : alisql::PaxosErrorCode::PE_DEFAULT;
}

int
ConsensusContext::change_to_single_mode()
{
	return m_paxos->forceSingleLeader();
}

void
ConsensusContext::get_member_info(ConsensusMemberInfo *info)
{
	alisql::Paxos::MemberInfoType mi;
	m_paxos->getMemberInfo(&mi);

	info->serverId         = mi.serverId;
	info->currentTerm      = mi.currentTerm;
	info->currentLeader    = mi.currentLeader;
	info->commitIndex      = mi.commitIndex;
	info->lastLogTerm      = mi.lastLogTerm;
	info->lastLogIndex     = mi.lastLogIndex;
	info->role             = int(mi.role);
	info->votedFor         = mi.votedFor;
	info->lastAppliedIndex = mi.lastAppliedIndex;
}

int
ConsensusContext::get_cluster_info(ConsensusClusterInfo *cinfo, int maxCluster)
{
	std::vector<alisql::Paxos::ClusterInfoType> cis;
	m_paxos->getClusterInfo(cis);
	int   numCluster = 0;

	for (size_t counter = 0; counter < cis.size() && counter < (size_t)maxCluster; ++counter)
	{

		auto &ciIn = cis[counter];
		ConsensusClusterInfo *ciOut = &cinfo[counter];

		size_t len = ciIn.ipPort.length();
		if (len >  sizeof(ciOut->ipPort) - 1) {
			len = sizeof(ciOut->ipPort) - 1;
		}
		strncpy(ciOut->ipPort, ciIn.ipPort.c_str(), len);
		ciOut->ipPort[len] = 0;

		ciOut->serverId       =   ciIn.serverId;
		ciOut->matchIndex     =   ciIn.matchIndex;
		ciOut->nextIndex      =   ciIn.nextIndex;
		ciOut->role           =   int(ciIn.role);
		ciOut->hasVoted       =   ciIn.hasVoted;
		ciOut->forceSync      =   ciIn.forceSync;
		ciOut->electionWeight =   ciIn.electionWeight;
		ciOut->learnerSource  =   ciIn.learnerSource;
		ciOut->appliedIndex   =   ciIn.appliedIndex;
		ciOut->pipelining     =   ciIn.pipelining;
		ciOut->useApplied     =   ciIn.useApplied;
		numCluster++;
	}
	return numCluster;
}

void
ConsensusContext::get_stats_info(ConsensusStatsInfo *sinfo)
{
	const alisql::Paxos::StatsType &si = m_paxos->getStats();
	alisql::Paxos::MemberInfoType mi;
	m_paxos->getMemberInfo(&mi);

	sinfo->serverId               = mi.serverId;
	sinfo->countMsgAppendLog      = si.countMsgAppendLog;
	sinfo->countMsgRequestVote    = si.countMsgRequestVote;
	sinfo->countHeartbeat         = si.countHeartbeat;
	sinfo->countOnMsgAppendLog    = si.countOnMsgAppendLog;
	sinfo->countOnMsgRequestVote  = si.countOnMsgRequestVote;
	sinfo->countOnHeartbeat       = si.countOnHeartbeat;
	sinfo->countReplicateLog      = si.countReplicateLog;
}

/* c interface impl. */
extern "C" {

CONSENSUS_HANDLE ConsensusContext2Handle(ConsensusContext *ctx) {
    return reinterpret_cast<CONSENSUS_HANDLE>(ctx);
}

ConsensusContext *Handle2ConsensusContext(CONSENSUS_HANDLE handle) {
    return reinterpret_cast<ConsensusContext*>(handle);
}

CONSENSUS_HANDLE
consensus_create_instance(
		uint64_t mockStartIndex,
		uint64_t electionTimeout,
		uint64_t purgeTimeout,
		uint64_t maxPacketSize, 
		uint64_t pipelineTimeout, 
		uint64_t configureChangeTimeout, 
		uint32_t logLevel)
{
	ConsensusContext *ctx = reinterpret_cast<ConsensusContext*>(
			malloc(sizeof(ConsensusContext)));
	memset(ctx, 0, sizeof(ConsensusContext));

	if (ctx->init(mockStartIndex, electionTimeout, purgeTimeout, maxPacketSize, 
			pipelineTimeout, configureChangeTimeout, logLevel))
	{
		free(ctx);
		return INVALID_CONSENSUS_HANDLE;
	}

	return ConsensusContext2Handle(ctx);
}

int
consensus_init_as_peer(CONSENSUS_HANDLE handle,
		bool isLogger,
		unsigned IOThreadCount,
		unsigned heartbeatThreadCount,
		unsigned workThreadCount,
		bool autoLeaderTransfer,
		bool autoPurge,
		bool delayElection,
		uint64_t delayElectiionTimeout)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->start_as_peer(isLogger, IOThreadCount, heartbeatThreadCount, workThreadCount, 
			autoLeaderTransfer, autoPurge, delayElection, delayElectiionTimeout, false);
}

int
consensus_init_as_learner(CONSENSUS_HANDLE handle,
		bool isLogger,
		unsigned IOThreadCount,
		unsigned heartbeatThreadCount,
		unsigned workThreadCount,
		bool autoLeaderTransfer,
		bool autoPurge,
		bool delayElection,
		uint64_t delayElectiionTimeout)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->start_as_peer(isLogger, IOThreadCount, heartbeatThreadCount, workThreadCount, 
			autoLeaderTransfer, autoPurge, delayElection, delayElectiionTimeout, true);
}

int
consensus_assign_conf_int(CONSENSUS_HANDLE handle, int param_key, int value)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->assign_conf_int(param_key, value);
}

void
consensus_assign_conf_bool(CONSENSUS_HANDLE handle, int param_key, bool value)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->assign_conf_bool(param_key, value);
}

int
consensus_request_vote(CONSENSUS_HANDLE handle, bool force)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	ctx->request_vote(force);

	return alisql::PaxosErrorCode::PE_NONE;
}

int
consensus_shutdown(CONSENSUS_HANDLE handle)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);
	return ctx->shutdown();
}

int
consensus_append_normal_entry(CONSENSUS_HANDLE handle,
		uint64_t *index,
		uint64_t *term,
		const char *value,
		size_t len)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->append_normal_entry(index, term, value, len);
}

uint64_t
consensus_wait_for_commit(CONSENSUS_HANDLE handle,
		uint64_t index,
		uint64_t term)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->wait_for_commit(index, term);
}

uint64_t
consensus_get_commit_index(CONSENSUS_HANDLE handle)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_commit_index();
}

uint64_t
consensus_get_server_id(CONSENSUS_HANDLE handle)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_current_server_id();
}

void
consensus_get_leader(CONSENSUS_HANDLE handle, uint64_t *local_id, 
		uint64_t *leader_id, uint64_t *term, char *leader_addr, int leader_addr_len)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_current_leader(local_id, leader_id, term, leader_addr, leader_addr_len);
}

void
consensus_get_member_info(CONSENSUS_HANDLE handle, ConsensusMemberInfo *info)
{
	assert(info != NULL);
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_member_info(info);
}

int
consensus_get_cluster_info(CONSENSUS_HANDLE handle, ConsensusClusterInfo *cinfo, int maxCluster)
{
	assert(cinfo != NULL);
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_cluster_info(cinfo, maxCluster);
}

void
consensus_get_stats_info(CONSENSUS_HANDLE handle, ConsensusStatsInfo *sinfo)
{
	assert(sinfo != NULL);
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->get_stats_info(sinfo);
}

const char*
consensus_get_error_message(int error)
{
	return alisql::pxserror(error);
}

int
consensus_transfer_leader(CONSENSUS_HANDLE handle, const char* nodeInfo)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->transfer_leader(nodeInfo);
}

int
consensus_reset_election_delay(CONSENSUS_HANDLE handle)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->reset_election_delay();
}

int
consensus_downgrade_member(CONSENSUS_HANDLE handle, const char* nodeInfo)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->downgrade_member(nodeInfo);
}

int
consensus_change_member(CONSENSUS_HANDLE handle, const char* nodeInfo, int opType)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->change_member(nodeInfo, opType);
}

int
consensus_change_learner(CONSENSUS_HANDLE handle, const char* nodeInfo, int opType)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->change_learner(nodeInfo, opType);
}

int
consensus_configure_member(CONSENSUS_HANDLE handle, const char* nodeInfo, 
		bool forceSync, int electionWeight)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->configure_member(nodeInfo, forceSync, electionWeight);
}

int
consensus_change_to_single_mode(CONSENSUS_HANDLE handle)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->change_to_single_mode();
}

int
consensus_change_match_index(CONSENSUS_HANDLE handle, const char* nodeInfo,
		uint64_t matchIndex)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->change_match_index(nodeInfo, matchIndex);
}

int
consensus_purge_logs(CONSENSUS_HANDLE handle, bool local, uint64_t purgeIndex)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->purge_logs(local, purgeIndex);
}

int
consensus_change_cluster_id(CONSENSUS_HANDLE handle, uint64_t clusterID)
{
	ConsensusContext *ctx = Handle2ConsensusContext(handle);

	return ctx->change_cluster_id(clusterID);
}

} // extern "C"

