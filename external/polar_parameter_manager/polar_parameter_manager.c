/*-------------------------------------------------------------------------
 *
 * polar_parameter_manager.c
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
 *	  external/polar_parameter_manager/polar_parameter_manager.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "funcapi.h"
#include "miscadmin.h"

#define POLAR_DESC_INFO_LEN 4

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(polar_get_guc_info);

Datum
polar_get_guc_info(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	struct config_generic *conf;
	Datum		values[POLAR_DESC_INFO_LEN];
	bool		nulls[POLAR_DESC_INFO_LEN];
	bool		is_visible;
	bool		is_changable;

	InitMaterializedSRF(fcinfo, 0);

	conf = polar_parameter_check_name_internal(name);

	MemSet(values, 0, sizeof(values));
	if (conf == NULL)
	{
		MemSet(nulls, 1, sizeof(nulls));
		goto end_ret;
	}
	else
		MemSet(nulls, 0, sizeof(nulls));

	/* set is_visible, default is false */
	is_visible = false;
	if (conf->flags & POLAR_GUC_IS_VISIBLE)
		is_visible = true;
	else if (conf->flags & POLAR_GUC_IS_INVISIBLE)
		is_visible = false;
	/* Make the configuration of autovacuum available to users */
	else if (conf->group == AUTOVACUUM)
		is_visible = true;

	/* set is_changable, default is true */
	is_changable = true;
	if (conf->flags & POLAR_GUC_IS_CHANGEABLE)
		is_changable = true;
	else if (conf->flags & POLAR_GUC_IS_UNCHANGEABLE)
		is_changable = false;
	/* heuristic rules for unchangable */
	else if (conf->context == PGC_INTERNAL)
		is_changable = false;
	else if (
#ifdef POLAR_TODO
			 conf->group == POLAR_NODE_STATIC ||
#endif
			 conf->group == RESOURCES_BGWRITER ||
			 conf->group == REPLICATION_STANDBY ||
			 conf->group == REPLICATION_SENDING ||
			 conf->group == WAL_ARCHIVING ||
			 conf->group == WAL_CHECKPOINTS ||
			 conf->group == WAL_SETTINGS ||
			 conf->group == WAL_RECOVERY_TARGET ||
			 conf->group == CONN_AUTH_SETTINGS ||
			 conf->group == CONN_AUTH_SSL ||
			 conf->group == FILE_LOCATIONS ||
			 conf->group == LOCK_MANAGEMENT ||
			 conf->group == LOGGING_WHERE ||
			 conf->group == LOGGING_WHAT ||
			 conf->group == STATS_CUMULATIVE ||
			 conf->group == STATS_MONITORING)
		is_changable = false;
	else if (conf->flags & (GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_DISALLOW_IN_AUTO_FILE | GUC_CUSTOM_PLACEHOLDER))
		is_changable = false;

	/* set Datum */
	values[0] = BoolGetDatum(is_visible);
	values[1] = BoolGetDatum(is_changable);
	/* set optional */
	if (conf->optional != NULL)
		values[2] = CStringGetTextDatum(conf->optional);
	else
	{
		values[2] = CStringGetTextDatum("");
		nulls[2] = true;
	}
	/* set is list input */
	values[3] = BoolGetDatum(conf->flags & GUC_LIST_INPUT);

end_ret:
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum) 0;
}
