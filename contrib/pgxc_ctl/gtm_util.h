/*-------------------------------------------------------------------------
 *
 * gtm_util.h
 *
 *    GTM utility module of Postgres-XC configuration and operation tool.
 *
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
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_UTIL_H
#define GTM_UTIL_H

#include "gtm/gtm_client.h"
#include "gtm/libpq-fe.h"

extern int unregisterFromGtm(char *line);
extern int process_unregister_command(GTM_PGXCNodeType type, char *nodename);
#define unregister_gtm_proxy(name) do{process_unregister_command(GTM_NODE_GTM_PROXY, name);}while(0)
#define unregister_coordinator(name) do{process_unregister_command(GTM_NODE_COORDINATOR, name);}while(0)
#define unregister_datanode(name) do{process_unregister_command(GTM_NODE_DATANODE, name);}while(0)

#endif /* GTM_UTIL_H */
