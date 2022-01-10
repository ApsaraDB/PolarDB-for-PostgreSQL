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

#ifndef EASY_CLIENT_H_
#define EASY_CLIENT_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * 主动连接管理
 */

EASY_CPP_START

void *easy_client_list_find(easy_hash_t *table, easy_addr_t *addr);
int easy_client_list_add(easy_hash_t *table, easy_addr_t *addr, easy_hash_list_t *list);

EASY_CPP_END

#endif
