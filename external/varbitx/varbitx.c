/*-------------------------------------------------------------------------
 *
 * varbitx.c
 *
 * Copyright (c) 2024, Alibaba Group Holding Limited
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
 *	  external/varbitx/varbitx.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/varbit.h"
#include "miscadmin.h"


PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(bitsetbit_array);
Datum		bitsetbit_array(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitsetbit_array_rec);
Datum		bitsetbit_array_rec(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitcountbit);
Datum		bitcountbit(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitfillbit);
Datum		bitfillbit(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitpositebit);
Datum		bitpositebit(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitgetbit_array);
Datum		bitgetbit_array(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(get_bit_array);
Datum		get_bit_array(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bitcountarray);
Datum		bitcountarray(PG_FUNCTION_ARGS);


Datum
bitgetbit_array(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	VarBit	   *result;			/* The resulting bit string			  */
	bits8	   *r;
	bits8	   *pos;
	int			i = 0,
				count = 0,
				len = 0,
				begin = 0;
	int			bitlen = VARBITLEN(arg1);
	int			byteNo,
				bitNo;

	if (PG_NARGS() == 3)
	{
		begin = PG_GETARG_INT32(1);
		count = PG_GETARG_INT32(2);
	}
	/* POLAR: begin and count should be not less 0 */
	if (begin < 0 || count < 0 || begin >= bitlen)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid parameter value of start_location or count ")));

	if (begin + count > bitlen)
		count = bitlen - begin;

	len = VARBITTOTALLEN(count);

	/* set to 0 so that *r is always initialised and string is zero-padded */
	result = (VarBit *) palloc0(len);
	SET_VARSIZE(result, len);
	VARBITLEN(result) = count;

	r = VARBITS(result);
	pos = VARBITS(arg1);

	for (i = 0; i < count; i++)
	{
		int			byteNo1 = (begin + i) / BITS_PER_BYTE;
		int			bitNo1 = BITS_PER_BYTE - 1 - ((begin + i) % BITS_PER_BYTE);

		byteNo = i / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (i % BITS_PER_BYTE);

		/*
		 * Update the byte.
		 */
		if (!(pos[byteNo1] & (1 << bitNo1)))
			r[byteNo] &= (~(1 << bitNo));
		else
			r[byteNo] |= (1 << bitNo);
	}
	PG_RETURN_VARBIT_P(result);

}

Datum
bitfillbit(PG_FUNCTION_ARGS)
{
	int32		targetBit = PG_GETARG_INT32(0);
	int32		bitlen = PG_GETARG_INT32(1);
	int32		len = VARBITTOTALLEN(bitlen);
	VarBit	   *result;			/* The resulting bit string			  */
	bits8	   *r;
	int			i = 0;
	int			byteNo,
				bitNo;

	/*
	 * sanity check!
	 */
	if (targetBit != 0 && targetBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target bit must be 0 or 1")));
	/* set to 0 so that *r is always initialised and string is zero-padded */
	result = (VarBit *) palloc0(len);
	SET_VARSIZE(result, len);
	VARBITLEN(result) = bitlen;

	r = VARBITS(result);
	for (i = 0; i < bitlen; i++)
	{
		byteNo = i / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (i % BITS_PER_BYTE);

		/*
		 * Update the byte.
		 */
		if (!targetBit)
			r[byteNo] &= (~(1 << bitNo));
		else
			r[byteNo] |= (1 << bitNo);
	}
	PG_RETURN_VARBIT_P(result);

}

Datum
bitsetbit_array(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	int32		newBit = PG_GETARG_INT32(1);
	ArrayType  *array1 = NULL;
	int		   *dims1;
	int			nitems1;
	int		   *pos;
	int			force_set = 0;

	VarBit	   *result;
	int			bitlen,
				reslen;
	bits8	   *r,
			   *p;
	int			byteNo,
				bitNo;
	int			i = 0,
				j = 0,
				n = 0,
				max_pos = 0,
				count = -9999;

	if (PG_NARGS() == 4)
	{
		force_set = PG_GETARG_BOOL(2);
		array1 = PG_GETARG_ARRAYTYPE_P(3);
	}
	else if (PG_NARGS() == 5)
	{
		force_set = PG_GETARG_BOOL(2);
		array1 = PG_GETARG_ARRAYTYPE_P(3);
		count = PG_GETARG_INT32(4);
	}
	else
	{
		array1 = PG_GETARG_ARRAYTYPE_P(2);
	}
	dims1 = (int32 *) ARR_DATA_PTR(array1);
	nitems1 = ArrayGetNItems(ARR_NDIM(array1), ARR_DIMS(array1));
	pos = dims1;

	bitlen = VARBITLEN(arg1);

	if (PG_NARGS() == 4 && count < 0)
		count = nitems1;

	for (i = 0; i < nitems1; i++)
	{
		n = pos[i];
		if (n < 0)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("bit index %d out of valid range (0..%d)",
							n, bitlen - 1)));
	}

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new bit must be 0 or 1")));

	for (i = 0; i < nitems1; i++)
	{
		if (max_pos < pos[i])
			max_pos = pos[i];
	}

	if (max_pos < bitlen)
		max_pos = bitlen - 1;

	/*
	 * POLAR: length of result shoud be calculated by VARBITTOTALLEN(len). And
	 * result allocated by palloc0() is always zero-padded.
	 */
	reslen = VARBITTOTALLEN(max_pos + 1);
	result = (VarBit *) palloc0(reslen);
	SET_VARSIZE(result, reslen);
	VARBITLEN(result) = max_pos + 1;

	p = VARBITS(arg1);
	r = VARBITS(result);

	memcpy(r, p, VARBITBYTES(arg1));

	if (max_pos >= bitlen)
	{
		for (i = bitlen; i < max_pos + 1; i++)
		{
			byteNo = i / BITS_PER_BYTE;
			bitNo = BITS_PER_BYTE - 1 - (i % BITS_PER_BYTE);

			/*
			 * Update the byte.
			 */
			if (force_set == 0)
				r[byteNo] &= (~(1 << bitNo));
			else
				r[byteNo] |= (1 << bitNo);
		}
	}
	for (i = 0, j = 0; i < nitems1 && j < count; i++)
	{
		n = pos[i];
		byteNo = n / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

		/* bit will change, place it in array */
		if ((newBit == 0 && (r[byteNo] & (1 << bitNo))) ||
			(newBit && (r[byteNo] & (1 << bitNo)) == 0))
		{
			j++;
		}

		/*
		 * Update the byte.
		 */
		if (newBit == 0)
			r[byteNo] &= (~(1 << bitNo));
		else
			r[byteNo] |= (1 << bitNo);
	}
	PG_RETURN_VARBIT_P(result);
}

Datum
bitsetbit_array_rec(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	int32		newBit = PG_GETARG_INT32(1);
	ArrayType  *array1 = NULL;
	int		   *dims1;
	int			nitems1;
	int		   *pos;
	int			force_set = 0;

	VarBit	   *result;
	int			bitlen,
				reslen;
	bits8	   *r,
			   *p;
	int			byteNo,
				bitNo;
	int			i = 0,
				j = 0,
				n = 0,
				max_pos = 0,
				count = -9999;
	ArrayBuildState *astate = NULL;
	ArrayType  *result_array;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[2];
	bool		nulls[2];


	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* need to build tuplestore in query context */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	/* generate junk in short-term context */
	MemoryContextSwitchTo(oldcontext);

	if (PG_NARGS() == 4)
	{
		force_set = PG_GETARG_BOOL(2);
		array1 = PG_GETARG_ARRAYTYPE_P(3);
	}
	else if (PG_NARGS() == 5)
	{
		force_set = PG_GETARG_BOOL(2);
		array1 = PG_GETARG_ARRAYTYPE_P(3);
		count = PG_GETARG_INT32(4);
	}
	dims1 = (int32 *) ARR_DATA_PTR(array1);
	nitems1 = ArrayGetNItems(ARR_NDIM(array1), ARR_DIMS(array1));
	pos = dims1;

	bitlen = VARBITLEN(arg1);

	if (PG_NARGS() == 4 && count < 0)
		count = nitems1;

	for (i = 0; i < nitems1; i++)
	{
		n = pos[i];
		if (n < 0)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("bit index %d out of valid range (0..%d)",
							n, bitlen - 1)));
	}

	/*
	 * sanity check!
	 */
	if (newBit != 0 && newBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new bit must be 0 or 1")));

	for (i = 0; i < nitems1; i++)
	{
		if (max_pos < pos[i])
			max_pos = pos[i];
	}

	if (max_pos < bitlen)
		max_pos = bitlen - 1;

	/*
	 * POLAR: length of result shoud be calculated by VARBITTOTALLEN(len). And
	 * result allocated by palloc0() is always zero-padded.
	 */
	reslen = VARBITTOTALLEN(max_pos + 1);
	result = (VarBit *) palloc0(reslen);
	SET_VARSIZE(result, reslen);
	VARBITLEN(result) = max_pos + 1;

	p = VARBITS(arg1);
	r = VARBITS(result);

	memcpy(r, p, VARBITBYTES(arg1));

	if (max_pos >= bitlen)
	{
		for (i = bitlen; i < max_pos + 1; i++)
		{
			byteNo = i / BITS_PER_BYTE;
			bitNo = BITS_PER_BYTE - 1 - (i % BITS_PER_BYTE);

			/*
			 * Update the byte.
			 */
			if (force_set == 0)
				r[byteNo] &= (~(1 << bitNo));
			else
				r[byteNo] |= (1 << bitNo);
		}
	}
	for (i = 0, j = 0; i < nitems1 && j < count; i++)
	{
		n = pos[i];
		byteNo = n / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

		/* bit will change, place it in array */
		if ((newBit == 0 && (r[byteNo] & (1 << bitNo))) ||
			(newBit && (r[byteNo] & (1 << bitNo)) == 0))
		{
			astate =
				accumArrayResult(astate, Int32GetDatum(n), false,
								 INT4OID, CurrentMemoryContext);
			j++;
		}

		/*
		 * Update the byte.
		 */
		if (newBit == 0)
			r[byteNo] &= (~(1 << bitNo));
		else
			r[byteNo] |= (1 << bitNo);
	}
	if (astate == NULL)
		result_array = construct_empty_array(INT4OID);
	else
		result_array = (ArrayType *) makeArrayResult(astate, CurrentMemoryContext);

	/*
	 * build tupdesc for result tuples. This must match the definition of the
	 * pg_prepared_statements view in system_views.sql
	 */
	/* POLAR: tupdesc should be build in ecxt_per_query_memory. */
	MemoryContextSwitchTo(per_query_ctx);
	tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "varbit",
					   VARBITOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "array",
					   INT4ARRAYOID, -1, 0);
	tupstore =
		tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[0] = VarBitPGetDatum(result);
	values[1] = PointerGetDatum(result_array);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	return (Datum) 0;
}

Datum
bitcountarray(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	int32		targetBit = PG_GETARG_INT32(1);
	ArrayType  *array1 = NULL;
	int		   *dims1;
	int			nitems1;
	int			bitlen;
	bits8	   *p;
	int			i = 0,
				pos = 0;
	int			result = 0;
	int			byteNo,
				bitNo;

	if (PG_NARGS() == 3)
	{
		array1 = PG_GETARG_ARRAYTYPE_P(2);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argn must be 3")));

	dims1 = (int32 *) ARR_DATA_PTR(array1);
	nitems1 = ArrayGetNItems(ARR_NDIM(array1), ARR_DIMS(array1));
	bitlen = VARBITLEN(arg1);

	/*
	 * sanity check!
	 */
	if (targetBit != 0 && targetBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target bit must be 0 or 1")));
	p = VARBITS(arg1);

	for (i = 0; i < nitems1; i++)
	{
		pos = dims1[i];
		if (pos < 0)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("bit index %d out of valid, value should >= 0",
							pos)));
		if (pos >= bitlen)
			continue;
		byteNo = pos / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (pos % BITS_PER_BYTE);

		if ((p[byteNo] & (1 << bitNo)) && targetBit)
			result++;
		if (!(p[byteNo] & (1 << bitNo)) && !targetBit)
			result++;
	}
	PG_RETURN_INT32(result);
}


Datum
bitcountbit(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	int32		targetBit = PG_GETARG_INT32(1);

	int32		begin = 0;
	int32		count = VARBITLEN(arg1);
	int			bitlen;
	bits8	   *p;
	int			i = 0;
	int			result = 0;
	int			byteNo,
				bitNo;

	if (PG_NARGS() > 2 && PG_NARGS() == 4)
	{
		begin = PG_GETARG_INT32(2);
		count = PG_GETARG_INT32(3);
	}
	else if (PG_NARGS() != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argn must be 2 or 4")));

	bitlen = VARBITLEN(arg1);
	if (begin < 0 || begin >= bitlen || count < 0 || begin + count > bitlen)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("bit index begin:%d, count:%d out of valid range (0..%d)",
						begin, count, bitlen - 1)));

	/*
	 * sanity check!
	 */
	if (targetBit != 0 && targetBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target bit must be 0 or 1")));
	p = VARBITS(arg1);

	for (i = begin; i < Min(begin + count, bitlen); i++)
	{
		byteNo = i / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (i % BITS_PER_BYTE);

		if ((p[byteNo] & (1 << bitNo)) && targetBit)
			result++;
		if (!(p[byteNo] & (1 << bitNo)) && !targetBit)
			result++;
	}
	PG_RETURN_INT32(result);
}
Datum
bitpositebit(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	int32		targetBit = PG_GETARG_INT32(1);
	bool		inc = 0;
	int			bitlen;
	bits8	   *p;
	int			byteNo,
				bitNo;
	int			i = 0,
				j = 0;
	int			pos = 0,
				count = 0;
	ArrayBuildState *astate = NULL;

	bitlen = VARBITLEN(arg1);
	if (targetBit != 0 && targetBit != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target bit must be 0 or 1")));

	if (PG_NARGS() == 3)
	{
		count = bitlen;
		inc = PG_GETARG_BOOL(2);
	}
	else
	{
		count = PG_GETARG_INT32(2);
		inc = PG_GETARG_BOOL(3);
	}

	p = VARBITS(arg1);
	for (i = 0, j = 0; i < bitlen && j < count; i++)
	{
		if (inc)
			pos = i;
		else
			pos = bitlen - i - 1;
		byteNo = pos / BITS_PER_BYTE;
		bitNo = BITS_PER_BYTE - 1 - (pos % BITS_PER_BYTE);

		if ((p[byteNo] & (1 << bitNo)) && targetBit == 1)
		{
			astate =
				accumArrayResult(astate, Int32GetDatum(pos), false,
								 INT4OID, CurrentMemoryContext);
			j++;
		}
		else if (!(p[byteNo] & (1 << bitNo)) && targetBit == 0)
		{
			astate =
				accumArrayResult(astate, Int32GetDatum(pos), false,
								 INT4OID, CurrentMemoryContext);
			j++;
		}

	}
	if (astate == NULL)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT4OID));
	else
		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}
Datum
get_bit_array(PG_FUNCTION_ARGS)
{
	VarBit	   *arg1 = PG_GETARG_VARBIT_P(0);
	ArrayType  *array1 = NULL;
	int		   *dims1;
	int			nitems1;
	int32		targetBit = 0;
	int			bitlen = 0;
	bits8	   *p;
	int			byteNo,
				bitNo;
	int			i = 0,
				j = 0,
				begin = 0;
	int			pos = 0,
				count = 0;
	ArrayBuildState *astate = NULL;

	bitlen = VARBITLEN(arg1);

	if (PG_NARGS() == 4)
	{
		begin = PG_GETARG_INT32(1);
		count = PG_GETARG_INT32(2);
		targetBit = PG_GETARG_INT32(3);

		if (targetBit != 0 && targetBit != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("target bit must be 0 or 1")));

		/* POLAR: begin and count should be not less 0 */
		if (begin < 0 || count < 0 || begin >= bitlen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value of start_location or count ")));

		p = VARBITS(arg1);
		for (i = begin, j = 0; i < bitlen && j < count; i++)
		{
			pos = i;
			byteNo = pos / BITS_PER_BYTE;
			bitNo = BITS_PER_BYTE - 1 - (pos % BITS_PER_BYTE);

			if ((p[byteNo] & (1 << bitNo)) && targetBit == 1)
			{
				astate =
					accumArrayResult(astate, Int32GetDatum(pos), false,
									 INT4OID, CurrentMemoryContext);
			}
			else if (!(p[byteNo] & (1 << bitNo)) && targetBit == 0)
			{
				astate =
					accumArrayResult(astate, Int32GetDatum(pos), false,
									 INT4OID, CurrentMemoryContext);
			}
			j++;
		}
	}
	else if (PG_NARGS() == 3)
	{
		targetBit = PG_GETARG_INT32(1);
		array1 = PG_GETARG_ARRAYTYPE_P(2);
		dims1 = (int32 *) ARR_DATA_PTR(array1);
		nitems1 = ArrayGetNItems(ARR_NDIM(array1), ARR_DIMS(array1));
		p = VARBITS(arg1);
		if (targetBit != 0 && targetBit != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("target bit must be 0 or 1")));

		for (i = 0; i < nitems1; i++)
		{
			pos = dims1[i];
			if (pos < 0)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("bit index %d out of valid, value should >= 0",
								pos)));
			if (pos >= bitlen)
				continue;
			byteNo = pos / BITS_PER_BYTE;
			bitNo = BITS_PER_BYTE - 1 - (pos % BITS_PER_BYTE);

			if ((p[byteNo] & (1 << bitNo)) && targetBit == 1)
			{
				astate =
					accumArrayResult(astate, Int32GetDatum(pos), false,
									 INT4OID, CurrentMemoryContext);
			}
			else if (!(p[byteNo] & (1 << bitNo)) && targetBit == 0)
			{
				astate =
					accumArrayResult(astate, Int32GetDatum(pos), false,
									 INT4OID, CurrentMemoryContext);
			}
		}
	}

	if (astate == NULL)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT4OID));
	else
		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}
