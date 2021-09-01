/*-------------------------------------------------------------------------
 *
 * pgx_ctl.h
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
#ifndef PGXC_CTL_H
#define PGXC_CTL_H

#include <stdio.h>

/* Common macros */
#define MAXPATH (512-1)
#define PGXC_CTL_HOME "PGXC_CTL_HOME"
#define HOME    "HOME"
#define PGXC_CTL_BASH "pgxc_ctl_bash"

#define MAXLINE (8192-1)
#define DEFAULT_CONF_FILE_NAME "pgxc_ctl.conf"

#define pgxc_ctl_home_def "pgxc_ctl"

#define MAXTOKEN (64-1)

#define MAXNODE (256-1)


#define true 1
#define false 0
#define TRUE 1
#define FALSE 0

/* Global variable definition */
extern char pgxc_ctl_home[];
extern char pgxc_bash_path[];
extern char pgxc_ctl_config_path[];
extern char progname[];

/* Important files */
extern FILE *inF;
extern FILE *outF;

/* pg_ctl stop option */
#define IMMEDIATE "immediate"
#define FAST "fast"
#define SMART "smart"

/* My nodename default --> used to ping */
#define DefaultName "pgxc_ctl"
extern char *myName;    /* pgxc_ctl name used to ping */
#define DefaultDatabase "postgres"
extern char *defaultDatabase;

extern void print_simple_node_info(char *nodeName, char *port, char *dir,
                                   char *extraConfig, char *specificExtraConfig);

#endif /* PGXC_CTL_H */
