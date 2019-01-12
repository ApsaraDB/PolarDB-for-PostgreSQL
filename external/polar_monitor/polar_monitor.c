/*-------------------------------------------------------------------------
 *
 * polar_monitor.c
 *  display some information of polardb
 *   
 * Copyright (c) 2018, Alibaba Group Holding Limited
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
 * limitations under the License.*
 * 
 * IDENTIFICATION
 *  external/polar_monitor/polar_monitor.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "funcapi.h"
#include "storage/polar_fd.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(polar_consistent_lsn);

Datum
polar_consistent_lsn(PG_FUNCTION_ARGS)
{
	XLogRecPtr cosistent_lsn;

	cosistent_lsn = polar_get_consistent_lsn();
	PG_RETURN_LSN(cosistent_lsn);
}

PG_FUNCTION_INFO_V1(polar_oldest_apply_lsn);

Datum
polar_oldest_apply_lsn(PG_FUNCTION_ARGS)
{
	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recovery is in progress"),
				    errhint("WAL control functions cannot be executed during recovery.")));

	PG_RETURN_LSN(polar_get_oldest_applied_lsn());
}

PG_FUNCTION_INFO_V1(polar_get_node_type);
Datum
polar_get_node_type(PG_FUNCTION_ARGS)
{
	char	*mode;
	PolarNodeType node_type = polar_node_type();

	switch (node_type)
	{
		case POLAR_MASTER:
			mode = "master";
			break;
		case POLAR_REPLICA:
			mode = "replica";
			break;
		case POLAR_STANDBY:
			mode = "standby";
			break;
		default:
			mode = "unknown";
	}

	PG_RETURN_TEXT_P(cstring_to_text(mode));
}