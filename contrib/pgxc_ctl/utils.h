/*-------------------------------------------------------------------------
 *
 * utils.h
 *
 *    Utilty module of Postgres-XC configuration and operation tool.
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
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>

extern void *Malloc(size_t size);
extern void *Malloc0(size_t size);
extern void *Realloc(void *ptr, size_t size);
extern void Free(void *ptr);
extern int Chdir(char *path, int flag);
extern FILE *Fopen(char *path, char *mode);
extern char *Strdup(const char *s);
extern char **addToList(char **List, char *val);
extern void appendFiles(FILE *f, char **fileList);
extern FILE *prepareLocalStdin(char *buf, int len, char **fileList);
extern char *timeStampString(char *buf, int len);
extern char **makeActualNodeList(char **nodeList);
extern int coordIdx(char *coordName);
extern int datanodeIdx(char *datanodeName);
extern pid_t get_prog_pid(char *host, char *pidfile, char *dir);
extern int pingNode(char *host, char *port);
extern int pingNodeSlave(char *host, char *datadir);
extern void trimNl(char *s);
extern char *getChPidList(char *host, pid_t ppid);
extern char *getIpAddress(char *hostName);
extern int pgxc_check_dir(const char *dir);

#define get_postmaster_pid(host, dir) get_prog_pid(host, "postmaster", dir)
#define freeAndReset(x) do{Free(x);(x)=NULL;}while(0)
#define myWEXITSTATUS(rc) ((rc) & 0x000000FF)

/* Printout variable in bash format */
#define svalFormat "%s=%s\n"
#define expandSval(name) name, sval(name)
#define avalFormat "%s=( %s )\n"
#define expandAval(name) name, listValue(name)
#define fprintAval(f, name) do{ char * __t__ = listValue(name); fprintf(f, avalFormat, name, __t__); Free(__t__); }while(0)
#define fprintSval(f, name) do{fprintf(f, svalFormat, expandSval(name));}while(0)
