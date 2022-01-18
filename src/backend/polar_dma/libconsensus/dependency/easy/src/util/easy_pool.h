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

#ifndef EASY_POOL_H_
#define EASY_POOL_H_

/**
 * 简单的内存池
 */
#include <easy_define.h>
#include <easy_list.h>
#include <easy_atomic.h>

EASY_CPP_START

#ifdef EASY_DEBUG_MAGIC
#define EASY_DEBUG_MAGIC_POOL     0x4c4f4f5059534145
#define EASY_DEBUG_MAGIC_MESSAGE  0x4753454d59534145
#define EASY_DEBUG_MAGIC_SESSION  0x5353455359534145
#define EASY_DEBUG_MAGIC_CONNECT  0x4e4e4f4359534145
#define EASY_DEBUG_MAGIC_REQUEST  0x5551455259534145
#endif

#define EASY_POOL_ALIGNMENT         512
#define EASY_POOL_PAGE_SIZE         4096
#define easy_pool_alloc(pool, size)  easy_pool_alloc_ex(pool, size, sizeof(long))
#define easy_pool_nalloc(pool, size) easy_pool_alloc_ex(pool, size, 1)

typedef void *(*easy_pool_realloc_pt)(void *ptr, size_t size);
typedef struct easy_pool_large_t easy_pool_large_t;
typedef struct easy_pool_t easy_pool_t;
typedef void (easy_pool_cleanup_pt)(const void *data);
typedef struct easy_pool_cleanup_t easy_pool_cleanup_t;

struct easy_pool_large_t {
    easy_pool_large_t       *next;
    uint8_t                 *data;
};

struct easy_pool_cleanup_t {
    easy_pool_cleanup_pt    *handler;
    easy_pool_cleanup_t     *next;
    const void              *data;
};

struct easy_pool_t {
    uint8_t                 *last;
    uint8_t                 *end;
    easy_pool_t             *next;
    uint16_t                failed;
    uint16_t                flags;
    uint32_t                max;

    // pool header
    easy_pool_t             *current;
    easy_pool_large_t       *large;
    easy_atomic_t           ref;
    easy_spin_t             tlock;
    easy_pool_cleanup_t     *cleanup;
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
};

extern easy_pool_realloc_pt easy_pool_realloc;
extern void *easy_pool_default_realloc (void *ptr, size_t size);

extern easy_pool_t *easy_pool_create(uint32_t size);
extern void easy_pool_clear(easy_pool_t *pool);
extern void easy_pool_destroy(easy_pool_t *pool);
extern void *easy_pool_alloc_ex(easy_pool_t *pool, uint32_t size, int align);
extern void *easy_pool_calloc(easy_pool_t *pool, uint32_t size);
extern void easy_pool_set_allocator(easy_pool_realloc_pt alloc);
extern void easy_pool_set_lock(easy_pool_t *pool);
extern easy_pool_cleanup_t *easy_pool_cleanup_new(easy_pool_t *pool, const void *data, easy_pool_cleanup_pt *handler);
extern void easy_pool_cleanup_reg(easy_pool_t *pool, easy_pool_cleanup_t *cl);

extern char *easy_pool_strdup(easy_pool_t *pool, const char *str);

EASY_CPP_END
#endif
