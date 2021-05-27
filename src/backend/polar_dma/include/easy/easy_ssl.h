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

#ifndef EASY_SSL_H_
#define EASY_SSL_H_

#include <easy_define.h>
#include "easy_io_struct.h"

/**
 * ssl支持模块
 */

EASY_CPP_START

#define EASY_SSL_SCACHE_BUILTIN 0
#define EASY_SSL_SCACHE_OFF     1
typedef struct easy_ssl_ctx_server_t {
    easy_hash_list_t        node;
    char                    *server_name;
    easy_ssl_ctx_t          *ss;
} easy_ssl_ctx_server_t;

typedef struct easy_ssl_pass_phrase_dialog_t {
    char                    *type;
    char                    *server_name;
} easy_ssl_pass_phrase_dialog_t;

#define easy_ssl_get_connection(s) SSL_get_ex_data((SSL*)s, easy_ssl_connection_index)
extern int              easy_ssl_connection_index;

// function
int easy_ssl_connection_create(easy_ssl_ctx_t *ssl, easy_connection_t *c);
int easy_ssl_connection_destroy(easy_connection_t *c);
void easy_ssl_connection_handshake(struct ev_loop *loop, ev_io *w, int revents);
int easy_ssl_client_do_handshake(easy_connection_t *c);
void easy_ssl_client_handshake(struct ev_loop *loop, ev_io *w, int revents);

EASY_CPP_END

#endif
