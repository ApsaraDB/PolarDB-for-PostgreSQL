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

#ifndef EASY_MEM_SLAB_H_
#define EASY_MEM_SLAB_H_

#include <easy_define.h>
#include <easy_list.h>
#include <easy_atomic.h>
#include <easy_mem_page.h>

/**
 * 简单内存分配器
 */
EASY_CPP_START

#define EASY_MEM_SLAB_MIN   512
typedef struct easy_mem_slab_t easy_mem_slab_t;
typedef struct easy_mem_cache_t easy_mem_cache_t;
typedef struct easy_mem_mgr_t easy_mem_mgr_t;
typedef struct easy_mem_sizes_t easy_mem_sizes_t;

struct easy_mem_slab_t {
    easy_list_t             list;
    unsigned char           *mem;
    uint16_t                inuse;
    uint16_t                free;
    uint16_t                cache_idx;
    uint16_t                next_pos[0];
};

struct easy_mem_cache_t {
    easy_list_t             slabs_partial;
    easy_list_t             slabs_full;
    easy_list_t             slabs_free;
    easy_list_t             next;

    uint32_t                order;
    uint32_t                buffer_size;
    uint32_t                num;
    uint32_t                offset;
    uint32_t                free_objects;
    uint32_t                free_limit;
    int                     idx;
    easy_spin_t             lock;
};

struct easy_mem_mgr_t {
    int                     started;
    int                     cache_max_num;
    int                     cache_fix_num;
    int                     cache_num;
    int64_t                 max_size;

    easy_list_t             list;
    easy_spin_t             lock;
    easy_mem_cache_t        *caches;
    easy_mem_zone_t         *zone;
};

// 内存初始化
int easy_mem_slab_init(int start_size, int64_t max_size);
void easy_mem_slab_destroy();
// 内存分配
void *easy_mem_slab_realloc(void *ptr, size_t size);
// 分配
void *easy_mem_cache_alloc(easy_mem_cache_t *cache);
// 释放
void easy_mem_cache_free(easy_mem_cache_t *cache, void *obj);
// 创建一下mem_cache
easy_mem_cache_t *easy_mem_cache_create(int buffer_size);

EASY_CPP_END

#endif
