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

#ifndef EASY_STRING_H_
#define EASY_STRING_H_

/**
 * inet的通用函数
 */
#include <stdarg.h>
#include "easy_define.h"
#include "easy_pool.h"

EASY_CPP_START

extern char *easy_strncpy(char *dst, const char *src, size_t n);
extern char *easy_string_tohex(const char *str, int n, char *result, int size);
extern char *easy_string_toupper(char *str);
extern char *easy_string_tolower(char *str);
extern char *easy_string_format_size(double bytes, char *buffer, int size);
extern char *easy_strcpy(char *dest, const char *src);
extern char *easy_num_to_str(char *dest, int len, uint64_t number);
extern char *easy_string_capitalize(char *str, int len);
extern int easy_vsnprintf(char *buf, size_t size, const char *fmt, va_list args);
extern int lnprintf(char *str, size_t size, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 3, 4)));

EASY_CPP_END

#endif
