/*-------------------------------------------------------------------------
 *
 * remote_recovery.h
 *	
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * src/include/replication/remote_recovery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _REMOTE_RECOVERY_H
#define _REMOTE_RECOVERY_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "getaddrinfo.h"		/* for NI_MAXHOST */
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgtime.h"
#include "utils/tuplestore.h"


/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO		1024


/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
	REMOTE_RECOVERY_STOPPED,				/* stopped and mustn't start up again */
	REMOTE_RECOVERY_STARTING,			/* launched, but the process hasn't
								 * initialized yet */
	REMOTE_RECOVERY_STREAMING,			/* walreceiver is streaming */
	REMOTE_RECOVERY_WAITING,				/* stopped streaming, waiting for orders */
	REMOTE_RECOVERY_RESTARTING,			/* asked to restart streaming */
	REMOTE_RECOVERY_STOPPING				/* requested to stop, but still running */
} RemoteRcvState;

/* Memory area for management of REMOTE RECOVERY */
typedef struct
{
	RemoteRcvState 	remoteRcvState;

	uint32			remoteFetchCount;

	uint32			remoteMissCount;

	pg_time_t	startTime;

	/*
	 * connection string; initially set to connect to the primary, and later
	 * clobbered to hide security-sensitive fields.
	 */
	char		conninfo[MAXCONNINFO];

	/*
	 * Host name (this can be a host name, an IP address, or a directory path)
	 * and port number of the active replication connection.
	 */
	char		sender_host[NI_MAXHOST];
	int			sender_port;


	/* set true once conninfo is ready to display (obfuscated pwds etc) */
	bool		ready_to_display;
} RemoteRcvData;

extern RemoteRcvData *RemoteRcv;

extern void InitRemoteFetchPageStreaming(const char *conninfo);
extern int RemoteFetchPageStreaming(
						RelFileNode rnode, 
						ForkNumber forknum,
					   	BlockNumber blkno, 
						char **page);
extern void FinishRemoteFetchPageStreaming(void);


extern bool enable_remote_recovery_print;
#endif							/* _REMOTE_RECOVERY_H */
