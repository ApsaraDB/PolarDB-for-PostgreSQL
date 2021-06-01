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

#ifndef EASY_SOCKET_H_
#define EASY_SOCKET_H_

#include <easy_define.h>
#include "easy_io_struct.h"
#include "easy_log.h"
#include <netinet/in.h>

/**
 * socket处理
 */

EASY_CPP_START

#define EASY_FLAGS_DEFERACCEPT 0x001
#define EASY_FLAGS_REUSEPORT   0x002
#define EASY_FLAGS_SREUSEPORT  0x004
#define EASY_FLAGS_NOLISTEN    0x008
#define SO_REUSEPORT 15
int easy_socket_listen(int udp, easy_addr_t *address, int *flags, int backlog);
int easy_socket_write(easy_connection_t *c, easy_list_t *l);
int easy_socket_read(easy_connection_t *c, char *buf, int size, int *pending);
int easy_socket_non_blocking(int fd);
int easy_socket_set_tcpopt(int fd, int option, int value);
int easy_socket_set_opt(int fd, int option, int value);
int easy_socket_support_ipv6();
int easy_socket_usend(easy_connection_t *c, easy_list_t *l);
int easy_socket_error(int fd);
int easy_socket_set_linger(int fd, int t);

int easy_socket_udpwrite(int fd, struct sockaddr *addr, easy_list_t *l);
int easy_socket_tcpwrite(int fd, easy_list_t *l);

EASY_CPP_END

#endif
