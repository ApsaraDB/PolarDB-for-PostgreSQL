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

#include "easy_pool.h"
#include <assert.h>
#include <stdio.h>

/**
 * 简单的内存池
 */

static void *easy_pool_alloc_block(easy_pool_t *pool, uint32_t size);
static void *easy_pool_alloc_large(easy_pool_t *pool, easy_pool_large_t *large, uint32_t size);
easy_pool_realloc_pt    easy_pool_realloc = easy_pool_default_realloc;
#define EASY_POOL_LOCK(pool) int kcolt = pool->flags; if (unlikely(kcolt)) easy_spin_lock(&pool->tlock);
#define EASY_POOL_UNLOCK(pool) if (unlikely(kcolt)) easy_spin_unlock(&pool->tlock);

easy_pool_t *easy_pool_create(uint32_t size)
{
    easy_pool_t             *p;

    // 对齐
    size = easy_align(size + sizeof(easy_pool_t), EASY_POOL_ALIGNMENT);

    if ((p = (easy_pool_t *)easy_pool_realloc(NULL, size)) == NULL)
        return NULL;

    memset(p, 0, sizeof(easy_pool_t));
    p->tlock = EASY_SPIN_INITER;
    p->last = (uint8_t *) p + sizeof(easy_pool_t);
    p->end = (uint8_t *) p + size;
    p->max = size - sizeof(easy_pool_t);
    p->current = p;
#ifdef EASY_DEBUG_MAGIC
    p->magic = EASY_DEBUG_MAGIC_POOL;
#endif

    return p;
}

// clear
void easy_pool_clear(easy_pool_t *pool)
{
    easy_pool_t             *p, *n;
    easy_pool_large_t       *l;
    easy_pool_cleanup_t     *cl;

    // cleanup
    for (cl = pool->cleanup; cl; cl = cl->next) {
        if (cl->handler) (*cl->handler)(cl->data);
    }

    // large
    for(l = pool->large; l; l = l->next) {
        easy_pool_realloc(l->data, 0);
    }

    // other page
    for(p = pool->next; p; p = n) {
        n = p->next;
        easy_pool_realloc(p, 0);
    }

    pool->cleanup = NULL;
    pool->large = NULL;
    pool->next = NULL;
    pool->current = pool;
    pool->failed = 0;
    pool->last = (uint8_t *) pool + sizeof(easy_pool_t);
}

void easy_pool_destroy(easy_pool_t *pool)
{
    easy_pool_clear(pool);
    assert(pool->ref == 0);
#ifdef EASY_DEBUG_MAGIC
    pool->magic ++;
#endif
    easy_pool_realloc(pool, 0);
}

void *easy_pool_alloc_ex(easy_pool_t *pool, uint32_t size, int align)
{
    uint8_t                 *m;
    easy_pool_t             *p;
    int                     dsize;

    // init
    dsize = 0;

    if (size > pool->max) {
        dsize = size;
        size = sizeof(easy_pool_large_t);
    }

    EASY_POOL_LOCK(pool);

    p = pool->current;

    do {
        m = easy_align_ptr(p->last, align);

        if (m + size <= p->end) {
            p->last = m + size;
            break;
        }

        p = p->next;
    } while (p);

    // 重新分配一块出来
    if (p == NULL) {
        m = (uint8_t *)easy_pool_alloc_block(pool, size);
    }

    if (m && dsize) {
        m = (uint8_t *)easy_pool_alloc_large(pool, (easy_pool_large_t *)m, dsize);
    }

    EASY_POOL_UNLOCK(pool);

    return m;
}

void *easy_pool_calloc(easy_pool_t *pool, uint32_t size)
{
    void                    *p;

    if ((p = easy_pool_alloc_ex(pool, size, sizeof(long))) != NULL)
        memset(p, 0, size);

    return p;
}

// set lock
void easy_pool_set_lock(easy_pool_t *pool)
{
    pool->flags = 1;
}

// set realloc
void easy_pool_set_allocator(easy_pool_realloc_pt alloc)
{
    easy_pool_realloc = (alloc ? alloc : easy_pool_default_realloc);
}

void *easy_pool_default_realloc (void *ptr, size_t size)
{
    if (size) {
        return realloc (ptr, size);
    } else if (ptr) {
        free (ptr);
    }

    return 0;
}
///////////////////////////////////////////////////////////////////////////////////////////////////
// default realloc

static void *easy_pool_alloc_block(easy_pool_t *pool, uint32_t size)
{
    uint8_t                 *m;
    uint32_t                psize;
    easy_pool_t             *p, *newpool, *current;

    psize = (uint32_t) (pool->end - (uint8_t *) pool);

    if ((m = (uint8_t *)easy_pool_realloc(NULL, psize)) == NULL)
        return NULL;

    newpool = (easy_pool_t *) m;
    newpool->end = m + psize;
    newpool->next = NULL;
    newpool->failed = 0;

    m += offsetof(easy_pool_t, current);
    m = easy_align_ptr(m, sizeof(unsigned long));
    newpool->last = m + size;
    current = pool->current;

    for (p = current; p->next; p = p->next) {
        if (p->failed++ > 4) {
            current = p->next;
        }
    }

    p->next = newpool;
    pool->current = current ? current : newpool;

    return m;
}

static void *easy_pool_alloc_large(easy_pool_t *pool, easy_pool_large_t *large, uint32_t size)
{
    if ((large->data = (uint8_t *)easy_pool_realloc(NULL, size)) == NULL)
        return NULL;

    large->next = pool->large;
    pool->large = large;
    return large->data;
}

/**
 * strdup
 */
char *easy_pool_strdup(easy_pool_t *pool, const char *str)
{
    int                     sz;
    char                    *ptr;

    if (str == NULL)
        return NULL;

    sz = strlen(str) + 1;

    if ((ptr = (char *)easy_pool_alloc(pool, sz)) == NULL)
        return NULL;

    memcpy(ptr, str, sz);
    return ptr;
}

easy_pool_cleanup_t *easy_pool_cleanup_new(easy_pool_t *pool, const void *data, easy_pool_cleanup_pt *handler)
{
    easy_pool_cleanup_t *cl;
    cl = easy_pool_alloc(pool, sizeof(easy_pool_t));

    if (cl) {
        cl->handler = handler;
        cl->data = data;
    }

    return cl;
}

void easy_pool_cleanup_reg(easy_pool_t *pool, easy_pool_cleanup_t *cl)
{
    EASY_POOL_LOCK(pool);
    cl->next = pool->cleanup;
    pool->cleanup = cl;
    EASY_POOL_UNLOCK(pool);
}

