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

#ifndef EASY_ARRAY_H_
#define EASY_ARRAY_H_

/**
 * 固定长度数组分配
 */
#include "easy_pool.h"
#include "easy_list.h"

EASY_CPP_START

typedef struct easy_array_t {
    easy_pool_t             *pool;
    easy_list_t             list;
    int                     object_size;
    int                     count;
} easy_array_t;

easy_array_t *easy_array_create(int object_size);
void easy_array_destroy(easy_array_t *array);
void *easy_array_alloc(easy_array_t *array);
void easy_array_free(easy_array_t *array, void *ptr);

EASY_CPP_END

#endif
