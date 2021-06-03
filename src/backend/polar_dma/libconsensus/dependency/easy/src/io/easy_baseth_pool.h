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

#ifndef EASY_BASETH_POOL_H
#define EASY_BASETH_POOL_H

#include <easy_define.h>

/**
 * base pthread线程池
 */

EASY_CPP_START

#include "easy_io_struct.h"

#define easy_thread_pool_for_each(th, tp, offset)                   \
    for((th) = (typeof(*(th))*)&(tp)->data[offset];                 \
            (char*)(th) < (tp)->last;                               \
            th = (typeof(*th)*)(((char*)th) + (tp)->member_size))

// 第n个
static inline void *easy_thread_pool_index(easy_thread_pool_t *tp, int n)
{
    if (n < 0 || n >= tp->thread_count)
        return NULL;

    return &tp->data[n * tp->member_size];
}

static inline void *easy_thread_pool_hash(easy_thread_pool_t *tp, uint64_t hv)
{
    hv %= tp->thread_count;
    return &tp->data[hv * tp->member_size];
}

static inline void *easy_thread_pool_rr(easy_thread_pool_t *tp, int start)
{
    int                     n, t;

    if ((t = tp->thread_count - start) > 0) {
        n = easy_atomic32_add_return(&tp->last_number, 1);
        n %= t;
        n += start;
    } else {
        n = 0;
    }

    return &tp->data[n * tp->member_size];
}

// baseth
void *easy_baseth_on_start(void *args);
void easy_baseth_on_wakeup(void *args);
void easy_baseth_init(void *args, easy_thread_pool_t *tp,
                      easy_baseth_on_start_pt *start, easy_baseth_on_wakeup_pt *wakeup);
void easy_baseth_pool_on_wakeup(easy_thread_pool_t *tp);
easy_thread_pool_t *easy_baseth_pool_create(easy_io_t *eio, int thread_count, int member_size);
void easy_baseth_pool_destroy(easy_thread_pool_t *tp);

EASY_CPP_END

#endif
