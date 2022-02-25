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
 * @file service.h
 * @brief 
 */

#ifndef  cluster_service_INC
#define  cluster_service_INC

#include "ev.h"
#include "easyNet.h"
#include "paxos.pb.h"
#include "client_service.h"
#include "thread_timer.h"
#include <google/protobuf/io/coded_stream.h>

namespace alisql {

class Consensus;

bool MyParseFromArray(google::protobuf::Message &msg, const void* data, int size);

/**
 * @class Service
 *
 * @brief interface class for Service
 *
 **/
class Service {
  public:
    Service(Consensus *cons);
    virtual ~Service() {};

    virtual int init(uint64_t ioThreadCnt= 4, uint64_t workThreadCnt= 4, uint64_t ConnectTimeout= 300, bool memory_usage_count= false, uint64_t heartbeatThreadCnt= 0);
    virtual int start(int port);
    virtual void closeThreadPool();
    virtual int shutdown();
    virtual int stop();
    virtual void setSendPacketTimeout(uint64_t t);
    virtual int sendPacket(easy_addr_t addr, const std::string& buf, uint64_t id= 0);
    virtual int resendPacket(easy_addr_t addr, void *ptr, uint64_t id= 0);
    virtual easy_addr_t createConnection(const std::string &addr, NetServerRef server, uint64_t timeout, uint64_t index= 0) {return net_->createConnection(addr, server, timeout, index);}
    virtual void disableConnnection(easy_addr_t addr) { net_->disableConnection(addr); }
    // --- async stuff---
    struct CallbackBase;
    typedef std::shared_ptr<CallbackBase> CallbackType;
    class ServiceEvent {
      public:
        Consensus *cons;
        ulong type;
        void *arg;
        void *arg1;
        CallbackType cb;
    };/* end of class ServiceEvent */
    struct CallbackBase {
      virtual void run() = 0;
    };
    template<typename Callable>
      struct Callback : public CallbackBase{
        Callable cb;
        Callback(Callable&& f) : cb(std::forward<Callable>(f)) {}
        virtual void run() {cb();}
      };
    int pushAsyncEvent(CallbackType cb);
    template<typename Callable, typename... Args>
      int sendAsyncEvent(Callable&& f, Args&&... args)
      {
        CallbackType callBackPtr;
#if (__GNUC__ >= 7 || __clang__)
        callBackPtr= makeCallback(std::bind(std::forward<Callable>(f), std::forward<Args>(args)...));
#else
        callBackPtr= makeCallback(std::__bind_simple(std::forward<Callable>(f), std::forward<Args>(args)...));
#endif
        pushAsyncEvent(callBackPtr);
        return 0;
      }
    template<typename Callable>
      std::shared_ptr<Callback<Callable>>
      makeCallback(Callable&& f)
      {
        return std::make_shared<Callback<Callable>>(std::forward<Callable>(f));
      }
    //virtual int sendAsyncEvent(ulong type, void *arg= NULL, void *arg1= NULL);
    static int onAsyncEvent(Service::CallbackType cb);

    static int process(easy_request_t *r, void *args);

    std::shared_ptr<EasyNet> getEasyNet() {return net_;}
    Consensus *getConsensus() {return cons_;}
    void setConsensus(Consensus *cons) {cons_= cons;}
    easy_thread_pool_t *getWorkPool() {return workPool_;}
    easy_thread_pool_t *getHeartbeatPool() {return heartbeatPool_;}
    ThreadTimerService *getThreadTimerService() {return tts_.get();}
    bool workPoolIsRunning() { return !shutdown_stage_1.load(); }


    static std::atomic<uint64_t> running;
    static uint64_t workThreadCnt;
    ClientService *cs;
      
  protected:
    easy_io_t *pool_eio_;
    std::atomic<bool> shutdown_stage_1;
    easy_thread_pool_t *workPool_;
    easy_thread_pool_t *heartbeatPool_;
    std::shared_ptr<EasyNet> net_;
    std::shared_ptr<ThreadTimerService> tts_;
    Consensus *cons_;

};/* end of class Service */

};/* end of namespace alisql */

#endif     //#ifndef cluster_service_INC 
