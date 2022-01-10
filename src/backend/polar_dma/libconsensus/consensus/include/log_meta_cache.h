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
 * @file log_meta_cache.h
 * @brief paxos log meta cache
 */

#ifndef  log_meta_cache_INC
#define  log_meta_cache_INC

#include <cstdint>
#include <cstddef>
#include <vector>

namespace alisql {

class LogMetaCache {
  public:
    LogMetaCache();

    void init();

    void reset();

    // return false if `index` is not continuous
    bool putLogMeta(uint64_t index, uint64_t term, uint64_t optype, uint64_t info);

    // return false if `index` is not included in the cache
    bool getLogMeta(uint64_t index, uint64_t *term, uint64_t *optype, uint64_t *info);

  private:

    struct LogMetaEntry {
      uint64_t index;
      uint64_t term;
      uint64_t optype;
      uint64_t info;
    };

    static const size_t MaxEntrySize = 8192;

    uint64_t count_;
    uint64_t left_;
    uint64_t right_;
    uint64_t maxIndex_;
    std::vector<LogMetaEntry> array_;
};

} // namespace alisql

#endif /* log_meta_cache_INC */
