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

#ifndef EASY_REQUEST_H_
#define EASY_REQUEST_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * 一个request对象
 */

EASY_CPP_START

void easy_request_server_done(easy_request_t *r);
void easy_request_client_done(easy_request_t *r);
void easy_request_set_cleanup(easy_request_t *r, easy_list_t *output);

EASY_CPP_END

#endif

