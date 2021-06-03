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

#include <easy_string.h>
#include <easy_log.h>
#include <easy_time.h>
#include <pthread.h>
#include <sys/syscall.h>
#include "easy_io.h"

easy_log_print_pt       easy_log_print = easy_log_print_default;
easy_log_format_pt      easy_log_format = easy_log_format_default;
easy_log_level_t        easy_log_level = EASY_LOG_WARN;

/**
 * 设置log的打印函数
 */
void  easy_log_set_print(easy_log_print_pt p)
{
    easy_log_print = p;
    ev_set_syserr_cb(easy_log_print);
}

/**
 * 设置log的格式函数
 */
void  easy_log_set_format(easy_log_format_pt p)
{
    easy_log_format = p;
}

/**
 * 加上日志
 */
void easy_log_format_default(int level, const char *file, int line, const char *function, const char *fmt, ...)
{
    static __thread ev_tstamp   oldtime = 0.0;
    static __thread char        time_str[35];
    ev_tstamp                   now;
    int                         len;
    char                        buffer[4096];

    // 从loop中取
    if (easy_baseth_self && easy_baseth_self->loop) {
        now = ev_now(easy_baseth_self->loop);
    } else {
        //now = time(NULL);
        now = ev_time();
    }

    if (oldtime != now) {
        time_t                  t;
        struct tm               tm;
        oldtime = now;
        t = (time_t) now;
        easy_localtime((const time_t *)&t, &tm);
        lnprintf(time_str, 35, "[%04d-%02d-%02d %02d:%02d:%02d.%06d]",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, (int)((now - t) * 1000000));
    }

    // log level
    char level_str[64];
    switch (level) {
    case EASY_LOG_FATAL:
      strcpy(level_str, "[ERROR]");
      break;
    case EASY_LOG_ERROR:
      strcpy(level_str, "[ERROR]");
      break;
    case EASY_LOG_WARN:
      strcpy(level_str, "[Warning]");
      break;
    case EASY_LOG_INFO:
      strcpy(level_str, "[Info]");
      break;
    case EASY_LOG_DEBUG:
      strcpy(level_str, "[Debug]");
      break;
    case EASY_LOG_TRACE:
      strcpy(level_str, "[Trace]");
      break;
    default:
      strcpy(level_str, "[Default]");
      break;
    }

		// thread id
    char tid_str[64];
	lnprintf(tid_str, 64, "%d(%d)", getpid(), (int)syscall(__NR_gettid));

    // print
    len = lnprintf(buffer, 192, "%s %s %s ", time_str, level_str, tid_str);
    va_list                 args;
    va_start(args, fmt);
    len += easy_vsnprintf(buffer + len, 4090 - len,  fmt, args);
    va_end(args);

    // 去掉最后'\n'
    while(buffer[len - 1] == '\n') len --;

    buffer[len++] = '\n';
    buffer[len] = '\0';

    easy_log_print(buffer);
}

/**
 * 打印出来
 */
void easy_log_print_default(const char *message)
{
    easy_ignore(write(2, message, strlen(message)));
}

void __attribute__((constructor)) easy_log_start_()
{
    char                    *p = getenv("easy_log_level");

    if (p) easy_log_level = (easy_log_level_t)atoi(p);
}

