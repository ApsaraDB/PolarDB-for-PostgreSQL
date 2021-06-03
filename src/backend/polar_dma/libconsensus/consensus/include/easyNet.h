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
 * @file easyNet.h
 * @brief 
 */

#ifndef  cluster_easynet_INC
#define  cluster_easynet_INC

#include <map>
#include <memory>
#include <mutex>
#include <easy_io.h>
#include <easy_atomic.h>
#include "net.h"

namespace alisql {

typedef struct NetPacket {
    uint type;
    uint64_t packetId;
    void *msg;
    int len;
    char *data;
    char buffer[0];
} NetPacket;

const uint NetPacketTypeNet= 0;
const uint NetPacketTypeAsync= 1;

const uint64_t NetPacketHeaderSize= sizeof(uint64_t);

// global variable to count easy pool memory usage
extern easy_atomic_t easy_pool_alloc_byte;

/**
 * @class EasyNet
 *
 * @brief 
 *
 **/
class EasyNet : public Net{
  public:
    EasyNet (uint64_t num= 2, const uint64_t sessionTimeout= 300, bool memory_usage_count= false);
    virtual ~EasyNet () {};

    virtual int init(void *ptr= NULL);
    virtual int start(int port);
    virtual int shutdown();
    virtual int stop();

    /* TODO here we should use a general handler. */
    virtual easy_addr_t createConnection(const std::string &addr, NetServerRef server, uint64_t timeout= 1000, uint64_t index= 0);
    virtual void disableConnection(easy_addr_t addr);
    virtual int sendPacket(easy_addr_t addr, const char *buf, uint64_t len, uint64_t id= 0);
    virtual int sendPacket(easy_addr_t addr, const std::string& buf, uint64_t id= 0);
    virtual int resendPacket(easy_addr_t addr, void *ptr, uint64_t id= 0);
    virtual int setRecvPacketCallback(void *handler);

    void setWorkPool(easy_thread_pool_t* tp) {std::lock_guard<std::mutex> lg(lock_); workPool_= tp;}
    void incRecived() {__sync_fetch_and_add(&reciveCnt_, 1);}
    uint64_t getReciveCnt() {return reciveCnt_;}
    // bool isShutDown() {return isShutdown_;} /* not used now. */

    uint64_t getAddrKey(easy_addr_t addr);
    NetServerRef getConnData(easy_addr_t addr, bool locked = false);
    void setConnData(easy_addr_t addr, NetServerRef server);
    void delConnDataById(uint64_t id);
    NetServerRef getConnDataAndSetFail(easy_connection_t *c, bool isFail);
    uint64_t getConnCnt() {return connStatus_.size();}
    void setSessionTimeout(uint64_t t) { sessionTimeout_= t; }

    static void tryFreeMsg(NetPacket *np);

    /* Handler functions. */
    static int reciveProcess(easy_request_t *r);
    static void *paxosDecode(easy_message_t *m);
    static int paxosEncode(easy_request_t *r, void *data);
    static int onConnected(easy_connection_t *c);
    static int onDisconnected(easy_connection_t *c);
    static int onClientCleanup(easy_request_t *r, void *apacket);
    static uint64_t getPacketId(easy_connection_t *c, void *data);

  protected:
    /* libeasy member. */
    easy_io_t *eio_;
    easy_io_handler_pt clientHandler_;
    easy_io_handler_pt serverHandler_;
    easy_thread_pool_t *workPool_;

    /*TODO we shoud use shared_ptr here. */
    std::map<uint64_t, NetServerRef> connStatus_;
    std::mutex lock_;

    uint64_t reciveCnt_;
    bool isShutdown_;
    uint64_t sessionTimeout_;

  private:
    EasyNet ( const EasyNet &other );   // copy constructor
    const EasyNet& operator = ( const EasyNet &other ); // assignment operator

};/* end of class EasyNet */



} //namespace alisql
#endif     //#ifndef cluster_easynet_INC 
