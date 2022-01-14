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
 * @file client_service.cc
 * @brief 
 */

#include "paxos.h"
#include "client_service.h"

namespace alisql {

void ClientService::set(const std::string &key, const std::string &val)
{
  auto it= map_.find(key);
  if (map_.find(key) != map_.end())
    map_.erase(it);
  map_.emplace(key, val);
}

const std::string ClientService::set(const char *strKeyVal, uint64_t len)
{
  const char *val= strstr(strKeyVal, " ");
  if (val == NULL)
    return "";
  const std::string key(strKeyVal, val-strKeyVal);
  const std::string value(val + 1, len - (val-strKeyVal) - 1);
  set(key, value);
  return value;
}

int ClientService::serviceProcess(easy_request_t *r, void *args)
{
  NetPacket *np= (NetPacket *)r->ipacket;
  Paxos *paxos= (Paxos *)args;

  if (np->len > 4)
  {
    const char *buf= (const char *)np->data;
    uint64_t len= np->len;
    if (strncmp(buf, "get", 3) == 0 || strncmp(buf, "GET", 3) == 0)
    {
      const char *rpos= strstr(buf + 4, "\r");
      const std::string &str= get(std::string(buf + 4, rpos - buf - 4));
      if ((np= (NetPacket *) easy_pool_alloc(r->ms->pool, sizeof(NetPacket) + str.length() + 2)) == NULL)
      {
        r->opacket= NULL;
        return EASY_OK;
      }

      std::string out= str + "\r\n";

      np->type= NetPacketTypeNet;
      np->len= out.length();
      np->data= &np->buffer[0];

      memcpy(np->data, out.c_str(), out.length());

      r->opacket= (void *)np;
      return EASY_OK;
    }
    else if (strncmp(buf, "set", 3) == 0 || strncmp(buf, "SET", 3) == 0)
    {
      const char *rpos= strstr(buf + 4, "\r");
      r->opacket= (void *)NULL;

      const char *strKeyVal= buf + 4;
      len= rpos - buf - 4;

      const char *val= strstr(strKeyVal, " ");
      if (val == NULL)
        return EASY_OK;
      const std::string key(strKeyVal, val-strKeyVal);
      const std::string value(val + 1, len - (val-strKeyVal) - 1);

      LogEntry le;
      le.set_index(0);
      le.set_optype(1);
      le.set_ikey(key);
      le.set_value(value);

      paxos->replicateLog(le);

      return EASY_OK;
    }
  }
  return EASY_AGAIN;
}
} //namespace alisql
