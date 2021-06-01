/*
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
 */

#ifndef  EASY_SUMMARY_H
#define  EASY_SUMMARY_H

#include <easy_define.h>

#include "easy_io_struct.h"
#include "easy_log.h"

EASY_CPP_START
//////////////////////////////////////////////////////////////////////////////////
//接口函数
extern easy_summary_t          *easy_summary_create();
extern void                     easy_summary_destroy(easy_summary_t *sum);
extern easy_summary_node_t     *easy_summary_locate_node(int fd, easy_summary_t *sum, int hidden);
extern void                     easy_summary_destroy_node(int fd, easy_summary_t *sum);
extern void                     easy_summary_copy(easy_summary_t *src, easy_summary_t *dest);
extern easy_summary_t          *easy_summary_diff(easy_summary_t *ns, easy_summary_t *os);
extern void                     easy_summary_html_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, easy_summary_t *last);
extern void                     easy_summary_raw_output(easy_pool_t *pool,
        easy_list_t *bc, easy_summary_t *sum, const char *desc);

/////////////////////////////////////////////////////////////////////////////////

EASY_CPP_END

#endif
