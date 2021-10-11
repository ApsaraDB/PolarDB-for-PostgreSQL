/*-------------------------------------------------------------------------
 *
 * procstat.h
 *
 *	Copyright (c) 2020, Alibaba.inc
 *
 * IDENTIFICATION
 *		external/polar_monitor/procstat.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROCSTAT_H
#define PROCSTAT_H

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
#endif							/* PROCSTAT_H */
