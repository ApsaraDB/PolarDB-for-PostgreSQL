/*-------------------------------------------------------------------------
 *
 * px_pq.c
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  src/backend/px/dispatcher/px_pq.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "px/px_pq.h"

int
PQsendPxQuery_shared(PGconn *conn, char *shared_query, int query_len, bool nonblock)
{
	int			ret;

	if (!PQsendQueryStart(conn))
		return 0;

	if (!shared_query)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("shared mpp-query string is a null pointer\n"));
		return 0;
	}

	if (conn->outBuffer_shared)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("Cannot overwrite shared query string\n"));
		return 0;
	}

	/*
	 * Stash the original buffer and switch to the incoming pointer. We will
	 * restore the buffer after completing to send the shared query.
	 */
	conn->outBufferSaved = conn->outBuffer;

	conn->outBuffer = shared_query;
	conn->outBuffer_shared = true;

	conn->outMsgStart = 0;
	conn->outMsgEnd = query_len;
	conn->outCount = query_len;

	/* remember we are using simple query protocol */
	conn->queryclass = PGQUERY_SIMPLE;

	/*
	 * Give the data a push.  In nonblock mode, don't complain if we're unable
	 * to send it all; PQgetResult() will do any additional flushing needed.
	 */
	if (nonblock)
		ret = pqFlushNonBlocking(conn);
	else
		ret = pqFlush(conn);

	if (ret < 0)
	{
		pqHandleSendFailure(conn);
		return 0;
	}

	/* OK, it's launched! */
	conn->asyncStatus = PGASYNC_BUSY;
	return 1;
}
