/*-------------------------------------------------------------------------
 *
 * do_command.h
 *
 *    Main command module of Postgres-XC configuration and operation tool.
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
#ifndef DO_COMMAND_H
#define DO_COMMAND_H

extern int forceInit;
extern void do_command(FILE *inf, FILE *outf);
extern int  do_singleLine(char *buf, char *wkline);
extern int get_any_available_coord(int except);
extern int get_any_available_datanode(int except);
extern void deploy_cm(void);

#endif /* DO_COMMAND_H */
