/*-------------------------------------------------------------------------
 *
 * config.h
 *
 *    Configuration module of Postgres-XC configuration and operation tool.
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
#ifndef CONFIG_H
#define CONFIG_H

#include <stdio.h>
#include <string.h>

typedef enum NodeType {
    NodeType_UNDEF = 0, 
    NodeType_GTM, 
    NodeType_GTM_PROXY, 
    NodeType_COORDINATOR, 
    NodeType_DATANODE, 
    NodeType_SERVER} NodeType;

#define HAType_STREAMING "1"
#define HAType_AA "2"
#define HAType_PAXOS "3"

void read_vars(FILE *conf);
void check_configuration(int type);
void check_configuration_standalone(void);
int dump_configuration(void);
void filter_logical_node(void);
void read_selected_vars(FILE *conf, char *selectThis[]);
char *get_word(char *line, char **token);
int is_none(char *s);
int backup_configuration(void);
NodeType getNodeType(char *nodeName);
int checkSpecificResourceConflict(char *name, char *host, int port, char *dir, int is_gtm);
int checkNameConflict(char *name, int is_gtm);
int checkPortConflict(char *host, int port);
int checkDirConflict(char *host, char *dir);
void makeServerList(void);
void makeServerList_standalone(void);
int getDefaultWalSender(int isCoord);
int getRepNum(void);
int getLearnerType(void);


#define DEBUG() (strcasecmp(sval(VAR_debug), "y") == 0)
#define VERBOSE() (strcasecmp(sval(VAR_verbose), "y") == 0)
#define isVarYes(x) ((sval(x) != NULL) && (strcasecmp(sval(x), "y") == 0))
#define isPaxosEnv() ((sval(VAR_datanodeRepNum) != NULL) && strcasecmp(sval(VAR_datanodeRepNum), "2") == 0)
#define isStreamEnv() ((sval(VAR_datanodeRepNum) != NULL) && strcasecmp(sval(VAR_datanodeRepNum), "1") == 0)


void handle_no_slaves(void);
void handle_no_slaves_standalone(void);

#endif /* CONFIG_H */
