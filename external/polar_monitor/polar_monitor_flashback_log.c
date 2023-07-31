/*-------------------------------------------------------------------------
 *
 * polar_monitor_flashback_log.c
 *    display some information of polardb datamax
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
 * IDENTIFICATION
 *    external/polar_monitor/polar_monitor_flashback_log.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_point.h"
#include "utils/pg_lsn.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"

/*
 * POLAR: return the flashback log write status.
 */
PG_FUNCTION_INFO_V1(polar_stat_flashback_log_write);
Datum
polar_stat_flashback_log_write(PG_FUNCTION_ARGS)
{
#define NUM_FLASHBACK_LOG_WRITE_INFO_ELEM 6
	TupleDesc	tupdesc;
	Datum		values[NUM_FLASHBACK_LOG_WRITE_INFO_ELEM];
	bool		nulls[NUM_FLASHBACK_LOG_WRITE_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int         i = 0;
	int         cols = 1;
	uint64       write_total_num;
	uint64       bg_worker_write_num;
	uint64       segs_added_total_num;
	uint64       max_seg_no;
	polar_flog_rec_ptr write_result;
	polar_flog_rec_ptr fbpoint_flog_start_ptr;

	if (!polar_is_flog_enabled(flog_instance))
		elog(ERROR, "The flashback log is unenable, please run the function in the"
			 " RW or standby and check GUC polar_enable_flashback_log=on"
			 "and polar_enable_lazy_checkpoint=off and polar_flashback_logindex_mem_size>0.");

	memset(nulls, 0, sizeof(nulls));
	memset(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_FLASHBACK_LOG_WRITE_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_total_num", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "bg_worker_write_num", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "segs_added_total_num", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "max_seg_no", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "write_result", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "fbpoint_flog_start_ptr", LSNOID, -1, 0);

	polar_get_flog_write_stat(flog_instance->buf_ctl, &write_total_num, &bg_worker_write_num, &segs_added_total_num);
	max_seg_no = polar_get_flog_max_seg_no(flog_instance->buf_ctl);
	write_result = polar_get_flog_write_result(flog_instance->buf_ctl);
	fbpoint_flog_start_ptr = polar_get_fbpoint_start_ptr(flog_instance->buf_ctl);

	values[i++] = Int64GetDatum((int64)write_total_num);
	values[i++] = Int64GetDatum((int64)bg_worker_write_num);
	values[i++] = Int64GetDatum((int64)segs_added_total_num);
	values[i++] = Int64GetDatum((int64)max_seg_no);
	values[i++] = LSNGetDatum(write_result);
	values[i] = LSNGetDatum(fbpoint_flog_start_ptr);

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}

/*
 * POLAR: return the flashback log buffer status.
 */
PG_FUNCTION_INFO_V1(polar_stat_flashback_log_buf);
Datum
polar_stat_flashback_log_buf(PG_FUNCTION_ARGS)
{
#define NUM_FLASHBACK_LOG_BUF_INFO_ELEM 5
	TupleDesc	tupdesc;
	Datum		values[NUM_FLASHBACK_LOG_BUF_INFO_ELEM];
	bool		nulls[NUM_FLASHBACK_LOG_BUF_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int			i = 0;
	int         cols = 1;
	polar_flog_rec_ptr prev_ptr;
	polar_flog_rec_ptr ptr;
	polar_flog_rec_ptr initalized_upto;
	XLogRecPtr  keep_wal_lsn = InvalidXLogRecPtr;   /* keep lsn of WAL */
	bool is_ready;

	if (!polar_is_flog_enabled(flog_instance))
		elog(ERROR, "The flashback log is unenable, please run the function in the"
			 " RW or standby and check GUC polar_enable_flashback_log=on"
			 "and polar_enable_lazy_checkpoint=off and polar_flashback_logindex_mem_size>0.");

	memset(nulls, 0, sizeof(nulls));
	memset(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_FLASHBACK_LOG_BUF_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "insert_curr_ptr", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "insert_prev_ptr", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "initalized_upto", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "keep_wal_lsn", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_ready", BOOLOID, -1, 0);

	ptr = polar_get_curr_flog_ptr(flog_instance->buf_ctl, &prev_ptr);
	initalized_upto = polar_get_flog_buf_initalized_upto(flog_instance->buf_ctl);
	keep_wal_lsn = flog_instance->buf_ctl->redo_lsn;
	is_ready = POLAR_IS_FLOG_BUF_READY(flog_instance->buf_ctl);

	values[i++] = LSNGetDatum(ptr);
	values[i++] = LSNGetDatum(prev_ptr);
	values[i++] = LSNGetDatum(initalized_upto);
	values[i++] = LSNGetDatum(keep_wal_lsn);
	values[i] = BoolGetDatum(is_ready);

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}

/*
 * POLAR: return the flashback log list status.
 */
PG_FUNCTION_INFO_V1(polar_stat_flashback_log_list);
Datum
polar_stat_flashback_log_list(PG_FUNCTION_ARGS)
{
#define NUM_FLASHBACK_LOG_LIST_INFO_ELEM 3
	TupleDesc	tupdesc;
	Datum		values[NUM_FLASHBACK_LOG_LIST_INFO_ELEM];
	bool		nulls[NUM_FLASHBACK_LOG_LIST_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int         i = 0;
	int         cols = 1;
	uint64       insert_total_num;
	uint64       remove_total_num;
	uint64       bg_remove_num;

	if (!polar_is_flog_enabled(flog_instance))
		elog(ERROR, "The flashback log is unenable, please run the function in the"
			 " RW or standby and check GUC polar_enable_flashback_log=on"
			 "and polar_enable_lazy_checkpoint=off and polar_flashback_logindex_mem_size>0.");

	memset(nulls, 0, sizeof(nulls));
	memset(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_FLASHBACK_LOG_LIST_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "insert_total_num", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "remove_total_num", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "bg_remove_num", INT8OID, -1, 0);

	polar_get_flog_list_stat(flog_instance->list_ctl, &insert_total_num, &remove_total_num, &bg_remove_num);

	values[i++] = Int64GetDatum((int64)insert_total_num);
	values[i++] = Int64GetDatum((int64)remove_total_num);
	values[i] = Int64GetDatum((int64)bg_remove_num);

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}

/*
 * POLAR: return the flashback logindex status.
 */
PG_FUNCTION_INFO_V1(polar_stat_flashback_logindex);
Datum
polar_stat_flashback_logindex(PG_FUNCTION_ARGS)
{
#define NUM_FLASHBACK_LOGINDEX_INFO_ELEM 3
	TupleDesc	tupdesc;
	Datum		values[NUM_FLASHBACK_LOGINDEX_INFO_ELEM];
	bool		nulls[NUM_FLASHBACK_LOGINDEX_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int			i = 0;
	int         cols = 1;
	polar_flog_rec_ptr max_ptr_in_mem;
	polar_flog_rec_ptr max_ptr_in_disk;
	bool is_ready;

	if (!polar_is_flog_enabled(flog_instance))
		elog(ERROR, "The flashback log is unenable, please run the function in the"
			 " RW or standby and check GUC polar_enable_flashback_log=on"
			 "and polar_enable_lazy_checkpoint=off and polar_flashback_logindex_mem_size>0.");

	memset(nulls, 0, sizeof(nulls));
	memset(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_FLASHBACK_LOGINDEX_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "max_ptr_in_mem", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "max_ptr_in_disk", LSNOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "is_ready", BOOLOID, -1, 0);

	max_ptr_in_mem = polar_get_flog_index_max_ptr(flog_instance->logindex_snapshot);
	max_ptr_in_disk = polar_get_flog_index_meta_max_ptr(flog_instance->logindex_snapshot);
	is_ready = polar_is_flog_ready(flog_instance);

	values[i++] = LSNGetDatum(max_ptr_in_mem);
	values[i++] = LSNGetDatum(max_ptr_in_disk);
	values[i] = BoolGetDatum(is_ready);

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}

/*
 * POLAR: return the flashback logindex queue status.
 */
PG_FUNCTION_INFO_V1(polar_stat_flashback_logindex_queue);
Datum
polar_stat_flashback_logindex_queue(PG_FUNCTION_ARGS)
{
#define NUM_FLASHBACK_LOGINDEX_QUEUE_INFO_ELEM 3
	TupleDesc	tupdesc;
	Datum		values[NUM_FLASHBACK_LOGINDEX_QUEUE_INFO_ELEM];
	bool		nulls[NUM_FLASHBACK_LOGINDEX_QUEUE_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	int			i = 0;
	int         cols = 1;
	uint64 free_up_times = 0;
	uint64 read_from_file_rec_nums = 0;
	uint64 read_from_queue_rec_nums = 0;

	if (!polar_is_flog_enabled(flog_instance))
		elog(ERROR, "The flashback log is unenable, please run the function in the"
			 " RW or standby and check GUC polar_enable_flashback_log=on"
			 "and polar_enable_lazy_checkpoint=off and polar_flashback_logindex_mem_size>0.");

	if (polar_flashback_logindex_queue_buffers < 0)
		elog(ERROR, "polar_flashback_logindex_queue_buffers is zero, so there no flashback logindex queue");

	memset(nulls, 0, sizeof(nulls));
	memset(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_FLASHBACK_LOGINDEX_QUEUE_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "free_up_total_times", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_from_file_rec_nums", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) cols++, "read_from_queue_nums", INT8OID, -1, 0);

	polar_get_flog_index_queue_stat(flog_instance->queue_ctl->queue_stat, &free_up_times, &read_from_file_rec_nums,
									&read_from_queue_rec_nums);
	values[i++] = UInt8GetDatum(free_up_times);
	values[i++] = UInt8GetDatum(read_from_file_rec_nums);
	values[i] = UInt8GetDatum(read_from_queue_rec_nums);

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(result);
}
