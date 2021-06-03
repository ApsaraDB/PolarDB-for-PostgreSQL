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
 * @file paxos_server.h
 * @brief 
 */

#ifndef  cluster_paxos_server_INC
#define  cluster_paxos_server_INC

#include <stdint.h>
#include <memory>
#include <easy_io.h>
#include <service.h>
#include "paxos.pb.h"
#include "easyNet.h"
#include "thread_timer.h"
#include "single_process_queue.h"
#include "msg_compress.h"

namespace alisql {
class Paxos;
typedef std::chrono::steady_clock::time_point TimePoint;
typedef std::chrono::microseconds MSType;

/**
 * @class Server
 *
 * @brief 
 *
 **/
//class Server : public std::enable_shared_from_this<Server>{
class Server : public NetServer{
  public:
    Server (uint64_t serverIdArg) : serverId(serverIdArg), paxos(NULL),forceSync(false),
        electionWeight(5), learnerSource(0), sendByAppliedIndex(false), flowControl(0) {};
    virtual ~Server () {};

    virtual void beginRequestVote(void *) = 0;
    virtual void beginLeadership(void *) = 0;
    virtual void stepDown(void *) = 0;
    virtual void stop(void *) = 0;
    virtual void sendMsg(void *) = 0;
    virtual void connect(void *) = 0;
    virtual void disconnect(void *) = 0;
    virtual void fillInfo(void *) = 0;
    virtual void fillFollowerMeta(void *) = 0;
    virtual uint64_t getLastAckEpoch() const = 0;
    virtual void setLastAckEpoch(uint64_t epoch) = 0;
    virtual uint64_t getMatchIndex() const = 0;
    virtual void resetMatchIndex(uint64_t /* matchIndex */) { };
    virtual uint64_t getAppliedIndex() const = 0;
    virtual bool haveVote() const = 0;
    virtual void setMsgCompressOption(void *) = 0;
    /* TODO:
    virtual bool isCaughtUp() const = 0;
    */
    virtual uint64_t getLastLogIndex();
    virtual uint64_t getLastCachedLogIndex();

    //const uint64_t serverId;
    uint64_t serverId;
    //std::string strAddr;
    Paxos *paxos;
    bool forceSync;
    uint electionWeight;
    uint64_t learnerSource;
    bool sendByAppliedIndex; // default false
    int64_t flowControl;

    /*
  private:
    Server ( const Server &other );   // copy constructor
    const Server& operator = ( const Server &other ); // assignment operator
    */

};/* end of class Server */


/**
 * @class LocalServer
 *
 * @brief 
 *
 **/
class LocalServer : public Server{
  public:
    LocalServer (uint64_t serverId);
    virtual ~LocalServer () {};

    virtual void beginRequestVote(void *) {};
    virtual void beginLeadership(void *);
    virtual void stepDown(void *) {};
    virtual void stop(void *) {};
    virtual void sendMsg(void *);
    virtual void connect(void *) {};
    virtual void disconnect(void *) {};
    virtual void fillInfo(void *);
    virtual void fillFollowerMeta(void *);
    virtual uint64_t getLastAckEpoch() const;
    virtual void setLastAckEpoch(uint64_t) {};
    virtual uint64_t getMatchIndex() const {return lastSyncedIndex.load();};
    virtual uint64_t getAppliedIndex() const;
    virtual bool haveVote() const;
    virtual void setMsgCompressOption(void *) {};
    /* TODO:
    virtual bool isCaughtUp() const;
    */

    virtual uint64_t appendLog(LogEntry &entry);
    virtual uint64_t writeLog(LogEntry &entry);
    virtual uint64_t writeLogDone(uint64_t logIndex);
    virtual void writeCacheLogDone();
    uint64_t writeLogDoneInternal(uint64_t logIndex, bool forceSend= false);

    std::atomic<uint64_t> lastSyncedIndex;
    /* for logType node, try more times for electionWeightAction */
    bool logType;
    /* timeout for long rtt learner (default 0) */
    uint64_t learnerConnTimeout;
    uint64_t cidx;

};/* end of class LocalServer */


/**
 * @class AliSQLServer
 *
 * @brief 
 *
 **/
class AliSQLServer : public LocalServer{
  public:
    AliSQLServer (uint64_t serverId)
      :LocalServer(serverId)
    {}
    virtual ~AliSQLServer () {};
    virtual uint64_t writeLogDone(uint64_t logIndex);
    void setLastNonCommitDepIndex(uint64_t logIndex);

};/* end of class LocalServer */


class SendMsgTask;

/**
 * @class RemoteServer
 *
 * @brief 
 *
 **/
class RemoteServer : public Server{
  public:
    RemoteServer (uint64_t serverId);
    virtual ~RemoteServer ();

    virtual void beginRequestVote(void *);
    virtual void beginLeadership(void *);
    virtual void stepDown(void *);
    virtual void stop(void *);
    virtual void sendMsg(void *);
    virtual void connect(void *);
    virtual void disconnect(void *);
    virtual void fillInfo(void *);
    virtual void fillFollowerMeta(void *);
    virtual uint64_t getLastAckEpoch() const {return lastAckEpoch.load();}
    virtual void setLastAckEpoch(uint64_t epoch) {lastAckEpoch.store(epoch);lostConnect.store(false);};
    virtual uint64_t getMatchIndex() const {return matchIndex;}
    virtual uint64_t getAppliedIndex() const {return appliedIndex.load();};
    virtual void resetMatchIndex(uint64_t matchIndex_) { matchIndex.store(matchIndex_); hasMatched = false; }
    virtual bool haveVote() const {return hasVote;}
    virtual void setMsgCompressOption(void *);
    /* TODO:
    virtual bool isCaughtUp() const;
    */
    void sendMsgFunc(bool lockless, bool force, void *);
    static void sendMsgFuncAsync(SendMsgTask *task);
    void sendMsgFuncInternal(bool lockless, bool force, void *ptr, bool async);
    void setAddr(easy_addr_t addrArg) {addr= addrArg;}
    virtual void onConnectCb();
    void resetNextIndex();
    static TimePoint now() {
      return std::chrono::steady_clock::now();
    }
    static uint64_t diffMS(TimePoint tp) {
      TimePoint ntp= now();
      return std::chrono::duration_cast<std::chrono::microseconds>(ntp - tp).count();
    }
    std::shared_ptr<RemoteServer> getSharedThis() {return std::dynamic_pointer_cast<RemoteServer>(shared_from_this());}

    /* sendMsgQueue may be freed in async thread. */
    std::shared_ptr<SingleProcessQueue<SendMsgTask>> sendMsgQueue;
    std::atomic<uint64_t> nextIndex;
    std::atomic<uint64_t> matchIndex;
    std::atomic<uint64_t> lastAckEpoch;
    bool hasVote;
    bool isLeader;
    bool isLearner;
    easy_addr_t addr;
    std::shared_ptr<Service> srv;
    bool hasMatched;
    bool needAddr;
    std::atomic<bool> disablePipelining;
    std::atomic<bool> lostConnect;
    std::atomic<bool> appendError;
    std::atomic<bool> netError;
    std::atomic<bool> isStop;
    std::unique_ptr<ThreadTimer> heartbeatTimer;
    std::atomic<uint64_t> waitForReply;
    /*
      A msg should not reset waitForReply if msgId is less than guardId.
      This variable is set if disablePipelining.
     */
    std::atomic<uint64_t> guardId;
    std::atomic<uint64_t> msgId;
    std::atomic<uint64_t> appliedIndex;
    TimePoint lastSendTP;
    TimePoint lastMergeTP;
    MsgCompressOption msgCompressOption;

  protected:
    uint64_t getConnTimeout();
};/* end of class RemoteServer */


class SendMsgTask {
  public:
    PaxosMsg *msg;
    std::shared_ptr<RemoteServer> server;
    bool force;
    SendMsgTask(PaxosMsg *m, std::shared_ptr<RemoteServer> s, bool f) :msg(m), server(s), force(f) {};
    ~SendMsgTask() {
      delete msg;
      msg= NULL;
    }
    bool merge(const SendMsgTask *task) {
      assert(server == task->server);
      //assert(msg->msgtype() == Paxos::AppendLog);
      //assert(task->msg->msgtype() == Paxos::AppendLog);
      if (msg->term() == task->msg->term() && !force && !task->force)
      {
        if (task->msg->commitindex() > msg->commitindex())
          msg->set_commitindex(task->msg->commitindex());
        return true;
      }
      return false;
    }
    void printMergeInfo(uint64_t mergedNum) {
      easy_warn_log("SendMsgTask: %llu tasks merged for server %llu", mergedNum, server->serverId);
    }
};




} //namespace alisql

#endif     //#ifndef cluster_paxos_server_INC 
