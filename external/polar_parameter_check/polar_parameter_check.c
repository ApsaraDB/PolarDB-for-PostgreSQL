/*-------------------------------------------------------------------------
 *
 * polar_parameter_check.c
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
 *	  external/polar_parameter_check/polar_parameter_check.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;


PG_FUNCTION_INFO_V1(polar_parameter_name_check);
PG_FUNCTION_INFO_V1(polar_parameter_value_check);

Datum
polar_parameter_name_check(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(!PG_ARGISNULL(0) && polar_parameter_check_name_internal(
			text_to_cstring(PG_GETARG_TEXT_PP(0))) != NULL);
}

Datum
polar_parameter_value_check(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(!PG_ARGISNULL(0) && !PG_ARGISNULL(1) && 
		polar_parameter_check_value_internal(text_to_cstring(PG_GETARG_TEXT_PP(0)), 
			text_to_cstring(PG_GETARG_TEXT_PP(1))));
}

