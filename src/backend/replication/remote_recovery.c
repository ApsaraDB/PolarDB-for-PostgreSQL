/*-------------------------------------------------------------------------
 *
 * remote_recovery.c
 * Remote Fetching Recovery is used to fetch full page from mirror (standby or master)
 * during recovery to prevent page write torn in the absence of full page write.
 * Full page write is turned off for reduced IO and hence improved performance.
 * However, page torn may happen in case of OS crash or power loss.
 * In order to gurantee high availability while providing high performance,
 * Remote Recovery is designed to fetch full pages from the mirror node during recovery.
 *
 * @Author  , 2020.09.28
 *
 * This file contains the server-facing parts of remote_recovery. The libpq-
 * specific parts are in the libpqwalreceiver module. It's loaded
 * dynamically to avoid linking the server with libpq.
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
 *
 * IDENTIFICATION
 *	  src/backend/replication/remote_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "replication/remote_recovery.h"
#include "replication/walreceiver.h"
#include "pgstat.h"

bool		enable_remote_recovery_print = false;


static StringInfoData reply_message;
static StringInfoData incoming_message;


RemoteRcvData *RemoteRcv = NULL;

/* libpqwalreceiver connection */
static WalReceiverConn *wrconn = NULL;
static int	FetchPageRcvProcessMsg(unsigned char type, char *buf, int len);

/*
 * Init fetch page streaming to standby.
 */
void
InitRemoteFetchPageStreaming(const char *conninfo)
{
	RemoteRcvData *remotercv;
	char	   *err;
	WalRcvStreamOptions options;


	if (conninfo == NULL)
		elog(ERROR, "standby connection is null");

	RemoteRcv = palloc0(sizeof(RemoteRcvData));
	remotercv = RemoteRcv;

	strlcpy((char *) remotercv->conninfo, conninfo, MAXCONNINFO);
	remotercv->remoteRcvState = REMOTE_RECOVERY_STARTING;

	if (WalReceiverFunctions)
		elog(ERROR, "libpqwalreceiver cannot be used due to being having initialized");

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	wrconn = walrcv_connect(conninfo, false, "remote_recovery_receiver", &err);
	if (!wrconn)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));


	options.startpoint = checkpointRedo;
	options.proto.physical.startpointTLI = checkpointTLI;

	if (walrcv_startfetchpage(wrconn, &options))
	{
		initStringInfo(&reply_message);
		initStringInfo(&incoming_message);
	}
	else
		ereport(ERROR,
				(errmsg("standby server contains no page")));

	elog(LOG, "REMOTE RECOVERY successfully initializes fetch page streaming to standby %s %X/%X timeline %u",
		 conninfo,
		 (uint32) (checkpointRedo >> 32),
		 (uint32) (checkpointRedo),
		 checkpointTLI);
}

void
FinishRemoteFetchPageStreaming(void)
{
	walrcv_endfetchpage(wrconn);
	RemoteRcv->remoteRcvState = REMOTE_RECOVERY_STOPPED;
	elog(LOG, "REMOTE RECOVERY finishes fetch page streaming to standby: page fetched %d page missed %d",
		 RemoteRcv->remoteFetchCount, RemoteRcv->remoteMissCount);
}


/*
 * Return 0 if the remote page exists
 * otherwise, return 1
 */
static int
FetchPageRcvProcessMsg(unsigned char type, char *buf, int len)
{
	int			hdrlen;
	int			pagelen;

	resetStringInfo(&incoming_message);

	switch (type)
	{
		case 'w':				/* page records */
			{
				hdrlen = sizeof(int32);
				if (len < hdrlen)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid page message received from standby")));

				appendBinaryStringInfo(&incoming_message, buf, hdrlen);
				pagelen = pq_getmsgint(&incoming_message, 4);
				if (pagelen == 0)
				{
					elog(LOG, "no valid page pagelen %d len %d on standby",
						 pagelen, len);
					return 1;
				}
				if (pagelen != BLCKSZ)
					elog(PANIC, "invalid page size %d", pagelen);

				break;
			}

		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg_internal("invalid replication message type %d",
									 type)));
	}

	return 0;
}
#define NAPTIME_PER_CYCLE 100	/* max sleep time between cycles (100ms) */

/*
 * Return 0 if the remote page exists
 * otherwise, return 1
 */
int
RemoteFetchPageStreaming(RelFileNode rnode,
						 ForkNumber forknum,
						 BlockNumber blkno,
						 char **page)
{
	int			len;
	char	   *buf;
	int			metalen = 1 + sizeof(int32);

	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'f');

	pq_sendint32(&reply_message, rnode.spcNode);
	pq_sendint32(&reply_message, rnode.dbNode);
	pq_sendint32(&reply_message, rnode.relNode);
	pq_sendint32(&reply_message, forknum);
	pq_sendint32(&reply_message, blkno);
	walrcv_send(wrconn, reply_message.data, reply_message.len);

	if (enable_remote_recovery_print)
		elog(LOG, "REMOTE RECOVERY fetch page : spcnode %d dbnode %d relnode %d forknum %d blkno %d",
			 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);

	for (;;)
	{
		/* pgsocket	wait_fd = PGINVALID_SOCKET; */
		/* int			rc; */
		/* receive requested page from standby */
		len = walrcv_fetchpage(wrconn, &buf);
		if (len > 0)
		{
			if (FetchPageRcvProcessMsg(buf[0], &buf[1], len))
			{
				elog(LOG, "REMOTE RECOVERY no valid page len %d on standby: spcnode %d dbnode %d relnode %d forknum %d blkno %d",
					 len, rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
				RemoteRcv->remoteMissCount++;
				return 1;
			}

			buf += metalen;
			/* return the data portion pointer */
			*page = buf;

			if (enable_remote_recovery_print)
				elog(LOG, "REMOTE RECOVERY valid page len %d on standby: spcnode %d dbnode %d relnode %d forknum %d blkno %d",
					 len, rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
			RemoteRcv->remoteFetchCount++;
			break;
		}
		else if (len <= 0)
			elog(PANIC, "cannot receive data from standby: spcnode %d dbnode %d relnode %d forknum %d blkno %d",
				 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);

		/*
		 * rc = WaitLatchOrSocket(NULL, WL_SOCKET_READABLE | WL_TIMEOUT ,
		 * wait_fd, NAPTIME_PER_CYCLE, WAIT_EVENT_WAL_RECEIVER_MAIN);
		 */
	}

	return 0;
}
