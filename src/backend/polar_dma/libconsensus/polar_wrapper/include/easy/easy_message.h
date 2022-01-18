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

#ifndef EASY_MESSAGE_H_
#define EASY_MESSAGE_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * 接收message
 */

EASY_CPP_START

easy_message_t *easy_message_create(easy_connection_t *c);
easy_message_t *easy_message_create_nlist(easy_connection_t *c);
int easy_message_destroy(easy_message_t *m, int del);
int easy_session_process(easy_session_t *s, int stop);

EASY_CPP_END

#endif
