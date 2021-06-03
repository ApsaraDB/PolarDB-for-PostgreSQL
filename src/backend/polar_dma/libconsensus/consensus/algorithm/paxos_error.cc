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
 * @file paxos_error.cc
 * @brief
 */

#include "paxos_error.h"

namespace alisql {

static const char *paxos_error_msg[PE_TOTAL] = {
  "Success.",  // PE_NONE
  "Some error happens, please check the error log.",  // PE_DEFAULT
  "Current node is not a leader.", // PE_NOTLEADER
  "Target node not exists.", // PE_NOTFOUND
  "Target node already exists.", // PE_EXISTS
  "A concurrent command is running, please retry later.", // PE_CONFLICTS
  "This node delays too much.", // PE_DELAY
  "Invalid argument, please check the error log.", // PE_INVALIDARGUMENT
  "Timeout.", // PE_TIMEOUT
  "Replicate log fail.", // PE_REPLICATEFAIL
  "This node is already a learner.", // PE_DOWNGRADLEARNER
  "Downgrade a leader is not allowed.", // PE_DOWNGRADELEADER
  "Configure forcesync or weight to a learner is not allowed.", // PE_WEIGHTLEARNER
  "Leader transfer to a learner is not allowed.", // PE_NOTFOLLOWER
  "For test." // PE_TEST
};

const char* pxserror(int error_code)
{
  // TODO: refactor all return code to PaxosErrorCode
  if (error_code < 0)
    return paxos_error_msg[PE_DEFAULT];
  // Invalid PaxosErrorCode case
  if (error_code >= PE_TOTAL)
    return "Unknown error.";
  const char* s = paxos_error_msg[error_code];
  // nullptr case should return default errmsg
  return s? s: paxos_error_msg[PE_DEFAULT];
}

const char* pxserror()
{
  return pxserror(PE_DEFAULT);
}

}

