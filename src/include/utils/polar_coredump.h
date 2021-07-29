/*-------------------------------------------------------------------------
 *
 * polar_coredump.h
 *		definitions for environment variable and structure when got_coredump.
 *
 *	Copyright (c) 2019, Alibaba.inc
 *
 *	IDENTIFICATION
 *		src/include/utils/polar_coredump.h
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dlfcn.h>

#define POLAR_MAX_STACK_FRAMES 64
#define POLAR_CORE_DUMP_FILE_SUFFIX "polar_core_stack_info"
#define POLAR_CORE_DUMP_PATTERN_FILE "/proc/sys/kernel/core_pattern"
#define POLAR_CORE_MAGIC_NUMBER (0xBA0BABEE)

typedef struct StackInfoOnDisk{
  int stack_size;
  void* stack_traces[POLAR_MAX_STACK_FRAMES];
  uint32 magic_number;	 /* for identity cross-check */
} StackInfoOnDisk;

extern int backtrace(void **buffer, int size);
extern bool polar_read_core_pattern(const char *core_pattern_path, char *buf);
extern void polar_program_error_handler(SIGNAL_ARGS);