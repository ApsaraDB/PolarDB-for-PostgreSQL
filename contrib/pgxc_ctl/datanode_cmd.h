/*-------------------------------------------------------------------------
 *
 * datanode_cmd.h
 *
 *    Datanode command module of Postgres-XC configuration and operation tool.
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
#ifndef DATANODE_CMD_H
#define DATANODE_CMD_H

#include "utils.h"

extern int init_datanode_master(char **nodeList);
extern int init_datanode_master_all(void);
extern int init_datanode_slave(char **nodeList);
extern int init_datanode_slave_all(void);
extern int init_datanode_learner_all(void);
extern int init_datanode_learner(char **nodeList);
extern cmd_t *prepare_initDatanodeMaster(char *nodeName);
extern cmd_t *prepare_initDatanodeSlave(char *nodeName);
extern cmd_t *prepare_initPaxosDNMaster(char *nodeName);
extern cmd_t *prepare_initPaxosDNSlave(char *nodeName);
extern cmd_t *prepare_initPaxosDNLearner(char *nodeName);
extern cmd_t *prepare_initPaxosDNSecondSlave(char *nodeName);

extern cmd_t *prepare_basebackup(char *nodeName);
extern cmd_t *prepare_basebackup_secondSlave(char *nodeName);

extern int start_datanode_master(char **nodeList);
extern int start_datanode_master_all(void);
extern int start_datanode_slave(char **nodeList);
extern int start_datanode_slave_all(void);
extern int start_datanode_learner(char **nodeList);
extern int start_datanode_learner_all(void);
extern cmd_t *prepare_startDatanodeMaster(char *nodeName);
extern cmd_t *prepare_startDatanodeSlave(char *nodeName);
extern cmd_t *prepare_startDatanodeLearner(char *nodeName);

extern int stop_datanode_master(char **nodeList, char *immediate);
extern int stop_datanode_master_all(char *immediate);
extern int stop_datanode_slave(char **nodeList, char *immediate);
extern int stop_datanode_slave_all(char *immediate);
extern int stop_datanode_learner(char **nodeList, char *immediate);
extern int stop_datanode_learner_all(char *immediate);
extern cmd_t *prepare_stopDatanodeSlave(char *nodeName, char *immediate);
extern cmd_t *prepare_stopDatanodeMaster(char *nodeName, char *immediate);
extern cmd_t *prepare_stopDatanodeLearner(char *nodeName, char *immediate);

extern int failover_datanode(char **nodeList);

extern int kill_datanode_master(char **nodeList);
extern int kill_datanode_master_all(void);
extern int kill_datanode_slave(char **nodeList);
extern int kill_datanode_slave_all(void);
extern cmd_t *prepare_killDatanodeMaster(char *nodeName);
extern cmd_t *prepare_killDatanodeSlave(char *nodeName);
extern int kill_datanode_learner(char **nodeList);
extern cmd_t *prepare_killDatanodeLearner(char *nodeName);

extern int clean_datanode_master(char **nodeList);
extern int clean_datanode_master_all(void);
extern int clean_datanode_slave(char **nodeList);
extern int clean_datanode_slave_all(void);
extern cmd_t *prepare_cleanDatanodeMaster(char *nodeName);
extern cmd_t *prepare_cleanDatanodeSlave(char *nodeName);
extern int clean_datanode_learner(char **nodeList);
extern int clean_datanode_learner_all(void);
extern cmd_t *prepare_cleanDatanodeLearner(char *nodeName);

//#ifdef XCP
extern int add_datanodeMaster(char *name, char *host, int port, int pooler,
        char *dir, char *walDir, char *extraConf, char *extraPgHbaConf);
//#else
//extern int add_datanodeMaster(char *name, char *host, int port, char *dir,
//        char *restore_dname, char *extraConf, char *extraPgHbaConf);
//#endif
extern int add_datanodeSlave(char *name, char *host, int port, int pooler,
        char *dir, char *walDir, char *archDir);

extern int add_datanodeMaster_paxos(char *name, char *host, int port, int pooler,
        char *dir, char *walDir, char *extraConf, char *extraPgHbaConf);
extern int add_datanodeSlave_paxos(char *name, char *host, int port, int pooler,
        char *dir, char *walDir, char *archDir);
extern int add_datanodeLearner_paxos(char *name, char *host, int port, int pooler, char *dir,
        char *walDir);
extern int remove_datanodeMaster(char *name, int clean_opt);
extern int remove_datanodeSlave(char *name, int clean_opt);
extern int remove_datanodeLearner(char *name, int clean_opt);

extern int show_config_datanodeMasterSlaveMulti(char **nodeList);
extern int show_config_datanodeMasterMulti(char **nodeList);
extern int show_config_datanodeSlaveMulti(char **nodeList);
extern int show_config_datanodeMaster(int flag, int idx, char *hostname);
extern int show_config_datanodeSlave(int flag, int idx, char *hostname);
extern int show_config_datanodeLearner(int flag, int idx, char *hostname);

extern int check_AllDatanodeRunning(void);

extern int failover_oneDatanode(int datanodeIdx);
extern int failover_oneDatanode_aa(int datanodeIdx);
extern int failover_oneDatanode_standalone(int datanodeIdx);


#endif /* DATANODE_CMD_H */
