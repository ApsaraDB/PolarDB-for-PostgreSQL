/*-------------------------------------------------------------------------
 *
 * procstat.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
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
 * IDENTIFICATION
 *		external/polar_monitor_preload/polar_procstat.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_PROCSTAT_H
#define POLAR_PROCSTAT_H

#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <linux/limits.h>
#include <sys/times.h>

typedef enum PROCARGTYPE{
	PROCNUM = 0, 
	PROCSTR   =  1 ,
    PROCL = 2,
    PROCLL = 3,
    PROCULL = 4,
    PROCINT = 5
}PROCARGTYPE;

typedef unsigned long int num;

typedef struct polar_proc_stat
{
    num utime;                              /** user mode jiffies **/
    num stime;                             /** kernel mode jiffies **/
    num rss;                                /** RssAnon + RssFile + RssShare **/
    num share;                              /** RssFile + RssShare */
} polar_proc_stat;

extern int polar_get_proc_stat(int pid, polar_proc_stat *stat);
#endif							/* POLAR_PROCSTAT_H */
