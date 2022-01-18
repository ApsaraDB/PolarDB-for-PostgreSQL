/*-------------------------------------------------------------------------
 *
 * polar_sql_time_stat.c
 *	  SQL life cycle time statistics.
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
 *	  src/backend/utils/misc/polar_sql_time_stat.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "utils/polar_sql_time_stat.h"

polar_sql_time_stat polar_sql_time_stat_local_summary;

static instr_time 	polar_start;
/*
 *  init sql time stat
 */
void
polar_init_sql_time_local_stats(void)
{
	memset(&polar_sql_time_stat_local_summary, 0, sizeof(polar_sql_time_stat_local_summary));
}

/*
 * sql set record time, exclude in the execute stage.
 */
void
polar_sql_stat_set_time(void)
{
	if (!polar_enable_track_sql_time_stat)
		return;

	INSTR_TIME_SET_CURRENT(polar_start);	
}	

/*
 * sql stat record time
 */
void
polar_sql_stat_record_time(sqlLifeStage sql_stage)
{
	instr_time end;
	uint64 diff;

	if (!polar_enable_track_sql_time_stat)
		return;

	if (INSTR_TIME_IS_ZERO(polar_start))
		return;

	INSTR_TIME_SET_CURRENT(end);
	diff = INSTR_TIME_GET_MICROSEC(end) - INSTR_TIME_GET_MICROSEC(polar_start);
	
	switch (sql_stage)
	{
	case SQL_PARSE:  
		polar_sql_time_stat_local_summary.parse_time = diff;
		break;
	
	case SQL_ANALYZE:
		polar_sql_time_stat_local_summary.analyze_time = diff;
		break;

	case QUERY_REWRITE:
		polar_sql_time_stat_local_summary.rewrite_time = diff;
		break;

	case SQL_PLAN:
		polar_sql_time_stat_local_summary.plan_time = diff;
		break;

	//case SQL_EXECUTE:
		//polar_sql_time_stat_local_summary.execute_time = diff;
		//break;

	default:
		break;
	}

}