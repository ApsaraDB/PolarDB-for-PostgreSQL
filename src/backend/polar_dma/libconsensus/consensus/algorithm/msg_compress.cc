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
 * @file msg_compress.cc
 * @brief paxos msg compression module
 */

#include <string>
#include <cstdint>

#include "msg_compress.h"
#include "crc.h"

#ifdef LZ4
#include "lz4.h"
#endif

#ifdef ZSTD
#include "zstd.h"
#endif

namespace alisql {

// return 0 on failure
static size_t compress(MsgCompressionType type, const std::string &src, std::string &dst)
{
  size_t ret = 0;
  switch (type) {
    case LZ4:
#ifdef LZ4
      int bound = LZ4_compressBound(src.size());
      if (bound == 0) return 0;
      dst.resize(bound);
      ret = LZ4_compress_default(src.data(), (char *)dst.data(), src.size(), dst.size());
      if (ret == 0) return 0;
#else
      return 0;
#endif
      break;
    case ZSTD:
#ifdef ZSTD
      size_t bound = ZSTD_compressBound(src.size());
      if (bound == 0) return 0;
      dst.resize(bound);
      ret = ZSTD_compress((void *)dst.data(), dst.size(), src.data(), src.size(), 1 /* level */);
      if (ZSTD_isError(ret)) return 0;
#else
      return 0;
#endif
      break;
    default:
      return 0;
  }
  dst.resize(ret);
  return ret;
}

// return false on failure
static bool decompress(MsgCompressionType type, const std::string &src, std::string &dst)
{
  switch (type) {
    case LZ4:
#ifdef LZ4
      int ret = LZ4_decompress_safe(src.data(), (char *)dst.data(), src.size(), dst.size());
      if (ret < 0) return false;
#else
      return false;
#endif
      break;
    case ZSTD:
#ifdef ZSTD
      size_t ret = ZSTD_decompress((void *)dst.data(), dst.size(), src.data(), src.size());
      if (ZSTD_isError(ret)) return false;

#else
      return false;
#endif
      break;
    default:
     return false;
  }
  return true;
}

size_t msgCompress(const MsgCompressOption &option, PaxosMsg &msg, size_t sizeHint)
{
  if (option.type() == None)
    return 0;

  msg.clear_compressedentries();

  if (sizeHint < option.sizeThreshold())
    return 0;

  std::string rawBytes, compressedBytes;
  rawBytes.reserve(sizeof(int) + sizeof(size_t) * msg.entries_size() + sizeHint);
  int total = msg.entries_size();
  rawBytes.append((const char *)&total, sizeof(int));
  for (auto &entry : msg.entries()) {
    size_t size = entry.ByteSize();
    rawBytes.append((const char *)&size, sizeof(size_t));
    if (entry.AppendToString(&rawBytes) == false)
      return 0;
  }

  size_t ret = compress(option.type(), rawBytes, compressedBytes);
  if (ret == 0) return 0;

  // compress succeed
  CompressedLogEntries *ce = new CompressedLogEntries();
  if (option.checksum()) {
    uint32_t checksum = calculateCRC32(rawBytes.data(), rawBytes.size());
    ce->set_checksum(checksum);
  }
  ce->set_type(option.type());
  ce->set_rawsize(rawBytes.size());
  ce->set_data(compressedBytes);
  msg.set_allocated_compressedentries(ce);
  msg.mutable_entries()->Clear();
  return rawBytes.size() - compressedBytes.size();
}

bool msgDecompress(PaxosMsg &msg)
{
  if (msg.has_compressedentries() == false)
    return true;

  msg.mutable_entries()->Clear();

  CompressedLogEntries ce = msg.compressedentries();

  size_t rawSize = ce.rawsize();
  std::string rawBytes(rawSize, 0);
  bool ret = decompress((MsgCompressionType)ce.type(), ce.data(), rawBytes);
  if (ret == false)
    return false;

  if (ce.has_checksum()) {
    uint32_t checksum = calculateCRC32(rawBytes.data(), rawBytes.size());
    if (ce.checksum() != checksum)
      return false;
  }

  size_t ptr = 0;
  char *data = (char *)rawBytes.data();
  if (ptr + sizeof(int) > rawSize)
    return false; // data corruption!
  int total = *((int *)(data + ptr));
  ptr += sizeof(int);
  for (int i = 0; i < total; ++i) {
    if (ptr + sizeof(size_t) > rawSize)
      return false; // data corruption!
    size_t size = *((size_t *)(data + ptr));
    ptr += sizeof(size_t);
    if (ptr + size > rawSize)
      return false;
    LogEntry le;
    if (le.ParseFromArray(data + ptr, size) == false)
      return false; // data corruption
    ptr += size;
    *(msg.mutable_entries()->Add()) = le;
  }
  msg.clear_compressedentries();
  return true;
}

} // namespace alisql
