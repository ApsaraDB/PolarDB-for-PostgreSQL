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

#ifndef EASY_DEFINE_H_
#define EASY_DEFINE_H_

/**
 * 定义一些编译参数
 */

#ifdef __cplusplus
# define EASY_CPP_START extern "C" {
# define EASY_CPP_END }
#else
# define EASY_CPP_START
# define EASY_CPP_END
#endif

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stddef.h>
#include <inttypes.h>
#include <unistd.h>
#include <execinfo.h>

///////////////////////////////////////////////////////////////////////////////////////////////////
// define
#define easy_free(ptr)              if(ptr) free(ptr)
#define easy_malloc(size)           malloc(size)
#define easy_realloc(ptr, size)     realloc(ptr, size)
#ifndef likely
#define likely(x)                   __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x)                 __builtin_expect(!!(x), 0)
#endif
#define easy_align_ptr(p, a)        (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))
#define easy_align(d, a)            (((d) + (a - 1)) & ~(a - 1))
#define easy_max(a,b)               (a > b ? a : b)
#define easy_min(a,b)               (a < b ? a : b)
#define easy_div(a,b)               ((b) ? ((a)/(b)) : 0)
#define easy_memcpy(dst, src, n)    (((char *) memcpy(dst, src, (n))) + (n))
#define easy_const_strcpy(b, s)     easy_memcpy(b, s, sizeof(s)-1)
#define easy_safe_close(fd)         {if((fd)>=0){close((fd));(fd)=-1;}}
#define easy_ignore(exp)            {int ignore __attribute__ ((unused)) = (exp);}

#define EASY_OK                     0
#define EASY_ERROR                  (-1)
#define EASY_ABORT                  (-2)
#define EASY_ASYNC                  (-3)
#define EASY_BREAK                  (-4)
#define EASY_AGAIN                  (-EAGAIN)

// DEBUG
//#define EASY_DEBUG_DOING            1
//#define EASY_DEBUG_MAGIC            1
///////////////////////////////////////////////////////////////////////////////////////////////////
// typedef
typedef struct easy_addr_t {
    uint16_t                  family;
    uint16_t                  port;
    union {
        uint32_t                addr;
        uint8_t                 addr6[16];
    } u;
    uint32_t                cidx;
} easy_addr_t;

#endif
