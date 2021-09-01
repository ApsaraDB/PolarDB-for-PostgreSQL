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
 * @file paxos_error.h
 * @brief
 */

#ifndef CONSENSUS_INCLUDE_PAXOS_ERROR_H_
#define CONSENSUS_INCLUDE_PAXOS_ERROR_H_

namespace alisql {

enum PaxosErrorCode: int {
  PE_NONE,
  PE_DEFAULT,
  PE_NOTLEADR,
  PE_NOTFOUND,
  PE_EXISTS,
  PE_CONFLICTS,
  PE_DELAY,
  PE_INVALIDARGUMENT,
  PE_TIMEOUT,
  PE_REPLICATEFAIL,
  PE_DOWNGRADLEARNER,
  PE_DOWNGRADELEADER,
  PE_WEIGHTLEARNER,
  PE_NOTFOLLOWER,
  PE_TEST,
  PE_TOTAL
};

const char* pxserror(int error_code);
const char* pxserror();

}

#endif /* CONSENSUS_INCLUDE_PAXOS_ERROR_H_ */
