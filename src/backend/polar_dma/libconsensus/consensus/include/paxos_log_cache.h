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
 * @file paxos_log_cache.h
 * @brief 
 */

#ifndef  paxos_log_cache_INC
#define  paxos_log_cache_INC

#include <deque>
#include "paxos.pb.h"

namespace alisql {

class PaxosLogCacheNode {
  public:
    uint64_t beginIndex;
    uint64_t endIndex;
    ::google::protobuf::RepeatedPtrField<LogEntry> entries;

    PaxosLogCacheNode(uint64_t bi, uint64_t ei, ::google::protobuf::RepeatedPtrField<LogEntry>& ets)
      :beginIndex(bi)
       ,endIndex(ei)
    {
      entries.CopyFrom(ets);
    }

    int getByteSize()
    {
      int ret = 0;
#ifdef DEBUG_LOG_CACHE
      for (auto it=entries.begin(); it !=entries.end(); ++it)
      {
        ret += it->ByteSize();
      }
#endif
      return ret;
    }
};

/**
 * @class PaxosLogCache
 *
 * @brief 
 *
 **/
class PaxosLogCache {
  public:
    PaxosLogCache () { byteSize_ = 0; }
    virtual ~PaxosLogCache () {}

    /* May be merge node to exist one. */
    void put(uint64_t beginIndex, uint64_t endIndex, ::google::protobuf::RepeatedPtrField<LogEntry>& entries);
    void put(PaxosLogCacheNode *newNode);
    PaxosLogCacheNode *get(uint64_t beginIndex);
    void clear();
    int getByteSize() { return byteSize_; }
    void setCommitIndex(uint64_t index) {commitIndex = index > commitIndex? index: commitIndex; }
    uint64_t getCommitIndex() { return commitIndex; }

    PaxosLogCacheNode *debugGet(uint64_t i);
    int debugGetByteSize();

  protected:
    std::deque<PaxosLogCacheNode *> logCache_;
    int byteSize_;
    /*
     * max msg->commitIndex in cache
     * used for configureChange
     * (TODO) follower commitIndex_ is a bit smaller
     */
    uint64_t commitIndex;

  private:
    PaxosLogCache ( const PaxosLogCache &other );   // copy constructor
    const PaxosLogCache& operator = ( const PaxosLogCache &other ); // assignment operator

};/* end of class PaxosLogCache */

} //namespace alisql

#endif     //#ifndef paxos_log_cache_INC 
