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
 * @file paxos_option.h
 * @brief
 */

#ifndef  paxos_option_INC
#define  paxos_option_INC

#include <atomic>
#include <string>
#include <memory>

namespace alisql {

class Paxos;
class RemoteServer;

/* store extra information from upper module */
class ExtraStore {
public:
  ExtraStore() {};
  virtual ~ExtraStore() {};
  /* remote info received from leader */
  virtual std::string getRemote() = 0;
  virtual void setRemote(const std::string &) = 0;
  /* local info to send to others */
  virtual std::string getLocal() = 0;
};

class DefaultExtraStore: public ExtraStore {
  virtual std::string getRemote() { return ""; }
  virtual void setRemote(const std::string &) {}
  /* local info to send to others */
  virtual std::string getLocal() { return ""; }
};

class PaxosOption {
friend class Paxos;
friend class RemoteServer;
public:
  PaxosOption()
    :enableLearnerHeartbeat_(false)
    ,enableAutoLeaderTransfer_(false)
    ,autoLeaderTransferCheckSeconds_(60)
  {
    extraStore = std::make_shared<DefaultExtraStore>();
  }
  ~PaxosOption() {}
private:
  std::atomic<bool> enableLearnerHeartbeat_;
  std::atomic<bool> enableAutoLeaderTransfer_;
  std::atomic<uint64_t> autoLeaderTransferCheckSeconds_;
  std::shared_ptr<ExtraStore> extraStore;
};

} //namespace alisql

#endif //#ifndef paxos_option_INC
