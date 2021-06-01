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

#ifndef EASY_INET_H_
#define EASY_INET_H_

/**
 * inet的通用函数
 */
#include "easy_define.h"

EASY_CPP_START

extern char *easy_inet_addr_to_str(easy_addr_t *addr, char *buffer, int len);
extern easy_addr_t easy_inet_str_to_addr(const char *host, int port);
extern int easy_inet_parse_host(easy_addr_t *address, const char *host, int port);
extern int easy_inet_is_ipaddr(const char *host);
extern int easy_inet_hostaddr(uint64_t *address, int size, int local);
extern easy_addr_t easy_inet_add_port(easy_addr_t *addr, int diff);
extern easy_addr_t easy_inet_getpeername(int s);
extern void easy_inet_atoe(void *a, easy_addr_t *e);
extern void easy_inet_etoa(easy_addr_t *e, void *a);
extern int easy_inet_myip(easy_addr_t *addr);

EASY_CPP_END

#endif
