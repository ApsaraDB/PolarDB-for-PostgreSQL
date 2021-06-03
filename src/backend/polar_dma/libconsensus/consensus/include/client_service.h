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
 * @file client_service.h
 * @brief 
 */

#ifndef  client_service_INC
#define  client_service_INC

#include <cstring>
#include <string>
#include <map>
#include "easyNet.h"

namespace alisql {

class Paxos;

/**
 * @class ClientService
 *
 * @brief 
 *
 **/
class ClientService {
  public:
    ClientService () {};
    virtual ~ClientService () {};

    const std::string &get(const std::string &key) {return map_[key];}
    void set(const std::string &key, const std::string &val);
    const std::string set(const char *strKeyVal, uint64_t len);
    int serviceProcess(easy_request_t *r, void *args);

  protected:
    std::map<const std::string, const std::string> map_;


  private:
    ClientService ( const ClientService &other );   // copy constructor
    const ClientService& operator = ( const ClientService &other ); // assignment operator

};/* end of class ClientService */



} //namespace alisql

#endif     //#ifndef client_service_INC 
