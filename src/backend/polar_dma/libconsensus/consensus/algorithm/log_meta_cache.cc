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
 * @file log_meta_cache.cc
 * @brief paxos log meta cache
 *
 */

#include "log_meta_cache.h"

#include <cassert>

namespace alisql {

LogMetaCache::LogMetaCache() { reset(); }

void LogMetaCache::init()
{
  array_.resize(MaxEntrySize);
}

void LogMetaCache::reset()
{
  count_ = 0;
  left_  = 0;
  right_ = 0;
  maxIndex_ = 0;
}

bool LogMetaCache::putLogMeta(uint64_t index, uint64_t term, uint64_t optype, uint64_t info)
{
  assert(index);
  if (count_ != 0 && index < maxIndex_ + 1)
    return false;
  if (count_ != 0 && index > maxIndex_ + 1)
    reset();

  if (count_ >= MaxEntrySize) {
    left_ = (left_ + 1) % MaxEntrySize;
    count_ = count_ - 1;
  }

  array_[right_] = LogMetaEntry{index, term, optype, info};
  right_ = (right_ + 1) % MaxEntrySize;
  ++count_;
  maxIndex_ = index;
  return true;
}

bool LogMetaCache::getLogMeta(uint64_t index, uint64_t *term, uint64_t *optype, uint64_t *info)
{
  if (count_ == 0 || index < array_[left_].index || index > maxIndex_)
    return false;

  uint64_t pos = (left_ + index - array_[left_].index) % MaxEntrySize;
  assert(index == array_[pos].index);
  *term = array_[pos].term;
  *optype = array_[pos].optype;
  *info = array_[pos].info;
  return true;
}

} // namespace alisql

