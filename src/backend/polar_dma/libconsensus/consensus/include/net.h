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
 * @file net.h
 * @brief 
 */

#ifndef  cluster_net_INC
#define  cluster_net_INC

#include <string>
#include <easy_io.h>

namespace alisql {

class NetServer : public std::enable_shared_from_this<NetServer>{
  public:
    NetServer ():c(nullptr) {};
    virtual ~NetServer () {};
    std::string strAddr;
    void *c;
};
typedef std::shared_ptr<NetServer> NetServerRef;

/**
 * @class Net
 *
 * @brief interface of net module
 *
 **/
class Net {
  public:
    Net () {};
    virtual ~Net () {};

    /* TODO here we should use a general handler. */
    virtual easy_addr_t createConnection(const std::string &addr, NetServerRef server, uint64_t timeout, uint64_t index) = 0;
    virtual void disableConnection(easy_addr_t addr) = 0;
    virtual int sendPacket(easy_addr_t addr, const char *buf, uint64_t len, uint64_t id) = 0;
    virtual int sendPacket(easy_addr_t addr, const std::string& buf, uint64_t id) = 0;
    virtual int setRecvPacketCallback(void *handler) = 0;

    virtual int init(void *ptr) = 0;
    virtual int start(int portArg) = 0;
    virtual int shutdown() = 0;

  protected:

  private:
    Net ( const Net &other );   // copy constructor
    const Net& operator = ( const Net &other ); // assignment operator

};/* end of class Net */


} //namespace alisql

#endif     //#ifndef cluster_net_INC 
