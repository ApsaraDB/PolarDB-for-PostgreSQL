/*-------------------------------------------------------------------------
 *
 * procstat.c
 *
 *	Copyright (c) 2020, Alibaba.inc
 *
 *	IDENTIFICATION
 *		external/polar_monitor/procstat.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include "procstat.h"

#define MAXPATHLENGTH 80
#define MAXSTATLEN    1024

static const char * delim = " ";
static char path[MAXPATHLENGTH];
static char procbuf[MAXSTATLEN];

static char * readone(char* curstr, void *x, PROCARGTYPE argtype) ;
static int parse_proc_pid_stat(int pid, polar_proc_stat *prostat);
static int parse_proc_pid_statm(int pid, polar_proc_stat *stat);

static char * readone(char* curstr, void *x, PROCARGTYPE argtype) 
{
	char* token = NULL;
	char* savestr = NULL;

	token = strtok_r (curstr, delim, &savestr);
	if(token)
	{
		switch (argtype)
		{
		case PROCINT : 
			*(int *)x = (int)atoi(token);
			break;

		case PROCNUM : 
			*(num *)x = (num)atoll(token);
			break;

		case PROCL : 
			*(long *)x = (long)atol(token);
			break;

		case PROCLL :
			*(long long *)x = (long long)atoll(token);
			break;

		case PROCSTR :
			strncpy((char *)x, token, savestr-curstr);
			break;
		
		default:
			break;
		}
	}
	return savestr;
}

/*  
 *  POLAR:
 * 	Parse a proc/[pid]/stat file 
 *  if return 1 ,not find stat file
 *  return 0, successful!
 */
static int
parse_proc_pid_stat(int pid, polar_proc_stat *prostat)
{
	char * curstr = NULL;
	char tty_null[64];
	FILE *input;
	int 	i;

    input = NULL;
    memset(path, 0 ,sizeof(path));
    sprintf(path,"/proc/%d/stat",pid);

    input = fopen(path, "r");
    if(!input) {
      return 1;
    }

    if(!fread(procbuf, MAXSTATLEN, 1, input))
	{
		if(ferror(input))
		{
			fclose(input);
			return 1;
  		}
	}

    curstr = readone(procbuf, tty_null, PROCSTR);
    curstr = readone(curstr, tty_null, PROCSTR);

	/* We don't need extra here */
	for (i = 0; i < 11; i++)
		curstr = readone(curstr, tty_null, PROCSTR);

    curstr = readone(curstr, &prostat->utime, PROCNUM);
    curstr = readone(curstr, &prostat->stime, PROCNUM);

    return fclose(input);
}

/*  
 *  POLAR:
 * 	Parse a proc/[pid]/statm file 
 *  if return 1 ,not find stat file
 *  return 0, successful!
 */
static int
parse_proc_pid_statm(int pid, polar_proc_stat *stat)
{
	char * curstr = NULL;
	char tty_null[64];
	FILE *input;

    input = NULL;
    memset(path, 0 ,sizeof(path));
    sprintf(path,"/proc/%d/statm",pid);

    input = fopen(path, "r");
    if(!input) {
      return 1;
    }

    if(!fread(procbuf, MAXSTATLEN, 1, input))
	{
		if(ferror(input))
		{
			fclose(input);
			return 1;
  		}
	}

	curstr = readone(procbuf, tty_null, PROCSTR); 
	curstr = readone(curstr, &stat->rss, PROCNUM);
	curstr = readone(curstr, &stat->share, PROCNUM);
	
    return fclose(input);
}

int 
polar_get_proc_stat(int pid, polar_proc_stat *stat)
{
	int ret = parse_proc_pid_stat(pid, stat);
	if (ret)
		return ret;

	return parse_proc_pid_statm(pid, stat);
}