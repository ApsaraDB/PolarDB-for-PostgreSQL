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
 * @file consensus.h
 * @brief the interface class of Consensus
 */

#ifndef  cluster_consensus_INC
#define  cluster_consensus_INC

#include "paxos.pb.h"
#include "service.h"

namespace alisql {


/**
 * @class Consensus
 *
 * @brief interface of consensus algorithm module
 *
 **/
class Consensus {
  public:
    enum MsgType {
        RequestVote= 0,
        RequestVoteResponce,
        AppendLog,
        AppendLogResponce,
        LeaderCommand,
        LeaderCommandResponce,
        ClusterIdNotMatch,
        OptimisticHeartbeat,
    };

    typedef enum CCOpType {
      CCNoOp= 0,
      CCMemberOp= 1,
      CCLearnerOp= 2,
      CCAddNode= 3,
      CCDelNode= 4,
      CCConfigureNode= 5,
      CCDowngradeNode= 6,
      CCSyncLearnerAll= 7,
      CCAddLearnerAutoChange= 8,
      CCLeaderTransfer= 9
    } CCOpTypeT;

    Consensus () {};
    virtual ~Consensus () {};

    virtual int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) = 0;
    virtual int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) = 0;
    virtual int onRequestVoteResponce(PaxosMsg *msg) = 0;
    virtual int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId) = 0;
    virtual int onAppendLogResponce(PaxosMsg *msg) = 0;
    virtual int requestVote(bool force) = 0;
    virtual uint64_t replicateLog(LogEntry &entry) = 0;
    virtual int appendLog(const bool needLock) = 0;
    virtual int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) = 0;
    virtual int onLeaderCommandResponce(PaxosMsg *msg) = 0;
    virtual int onClusterIdNotMatch(PaxosMsg *msg) = 0;
    virtual uint64_t getClusterId() = 0;
    virtual int setClusterId(uint64_t ci) = 0;
    virtual bool isShutdown() = 0;

  protected:

  private:
    Consensus ( const Consensus &other );   // copy constructor
    const Consensus& operator = ( const Consensus &other ); // assignment operator

};/* end of class Consensus */



} //namespace alisql

#endif     //#ifndef cluster_consensus_INC 
