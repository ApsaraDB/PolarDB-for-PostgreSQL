/*-------------------------------------------------------------------------
 *
 * gtm_cmd.h
 *
 *    GTM command module of Postgres-XC configuration and operation tool.
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
#ifndef GTM_CMD_H
#define GTM_CMD_H

#include "gtm/gtm_c.h"
#include "utils.h"

extern int init_gtm_master(bool stop);
extern int init_gtm_slave(void);
extern int init_gtm_proxy(char **nodeList);
extern int init_gtm_proxy_all(void);
extern cmd_t *prepare_initGtmMaster(bool stop);
extern cmd_t *prepare_initGtmSlave(void);
extern cmd_t *prepare_initGtmProxy(char *nodeName);

extern int add_gtmMaster(char *name, char *host, int port, char *dir);
extern int add_gtmSlave(char *name, char *host, int port, char *dir);
extern int add_gtmProxy(char *name, char *host, int port, char *dir);
extern int remove_gtmMaster(bool clean_opt);
extern int remove_gtmSlave(bool clean_opt);
extern int remove_gtmProxy(char *name, bool clean_opt);

extern int kill_gtm_master(void);
extern int kill_gtm_slave(void);
extern int kill_gtm_proxy(char **nodeList);
extern int kill_gtm_proxy_all(void);
extern cmd_t *prepare_killGtmMaster(void);
extern cmd_t *prepare_killGtmSlave(void);
extern cmd_t *prepare_killGtmProxy(char *nodeName);

extern int show_config_gtmMaster(int flag, char *hostname);
extern int show_config_gtmSlave(int flag, char *hostname);
extern int show_config_gtmProxy(int flag, int idx, char *hostname);
extern int show_config_gtmProxies(char **nameList);

extern int start_gtm_master(void);
extern int start_gtm_slave(void);
extern int start_gtm_proxy(char **nodeList);
extern int start_gtm_proxy_all(void);
extern cmd_t *prepare_startGtmMaster(void);
extern cmd_t *prepare_startGtmSlave(void);
extern cmd_t *prepare_startGtmProxy(char *nodeName);

extern int stop_gtm_master(void);
extern int stop_gtm_slave(void);
extern int stop_gtm_proxy(char **nodeList);
extern int stop_gtm_proxy_all(void);
extern cmd_t *prepare_stopGtmMaster(void);
extern cmd_t *prepare_stopGtmSlave(void);
extern cmd_t *prepare_stopGtmProxy(char *nodeName);

extern int failover_gtm(void);
extern int reconnect_gtm_proxy(char **nodeList);
extern int reconnect_gtm_proxy_all(void);
extern cmd_t *prepare_reconnectGtmProxy(char *nodeName);

extern int clean_gtm_master(void);
extern int clean_gtm_slave(void);
extern cmd_t *prepare_cleanGtmMaster(void);
extern cmd_t *prepare_cleanGtmSlave(void);

extern int clean_gtm_proxy(char **nodeList);
extern int clean_gtm_proxy_all(void);
extern cmd_t *prepare_cleanGtmProxy(char *nodeName);

#endif /* GTM_CMD_H */
