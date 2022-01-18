/*-------------------------------------------------------------------------
 *
 * memdebug.h
 *	  Memory debugging support.
 *
 * Currently, this file either wraps <valgrind/memcheck.h> or substitutes
 * empty definitions for Valgrind client request macros we use.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/memdebug.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMDEBUG_H
#define MEMDEBUG_H

#ifdef USE_VALGRIND
#include <valgrind/memcheck.h>
#define MEMDEBUG_CHECK_MEM_IS_DEFINED(addr, size)           VALGRIND_CHECK_MEM_IS_DEFINED((addr), (size))
#define MEMDEBUG_CREATE_MEMPOOL(context, redzones, zeroed)  VALGRIND_CREATE_MEMPOOL((context), (redzones), (zeroed))
#define MEMDEBUG_DESTROY_MEMPOOL(context)                   VALGRIND_DESTROY_MEMPOOL((context))
#define MEMDEBUG_MAKE_MEM_DEFINED(addr, size)               VALGRIND_MAKE_MEM_DEFINED((addr), (size))
#define MEMDEBUG_MAKE_MEM_NOACCESS(addr, size)              VALGRIND_MAKE_MEM_NOACCESS((addr), (size))
#define MEMDEBUG_MAKE_MEM_UNDEFINED(addr, size)             VALGRIND_MAKE_MEM_UNDEFINED((addr), (size))
#define MEMDEBUG_MEMPOOL_ALLOC(context, addr, size)         VALGRIND_MEMPOOL_ALLOC((context), (addr), (size))
#define MEMDEBUG_MEMPOOL_FREE(context, addr)                VALGRIND_MEMPOOL_FREE((context), (addr))
#define MEMDEBUG_MEMPOOL_CHANGE(context, optr, nptr, size)  VALGRIND_MEMPOOL_CHANGE((context), (optr), (nptr), (size))
#elif defined(__SANITIZE_ADDRESS__)
extern void __asan_poison_memory_region(void const volatile *addr, size_t size);
extern void __asan_unpoison_memory_region(void const volatile *addr, size_t size);

#define ASAN_POISON_MEMORY_REGION(addr, size) \
	  __asan_poison_memory_region((addr), (size))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
	  __asan_unpoison_memory_region((addr), (size))

#define MEMDEBUG_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)
#define MEMDEBUG_CREATE_MEMPOOL(context, redzones, zeroed)	do {} while (0)
#define MEMDEBUG_DESTROY_MEMPOOL(context)					do {} while (0)
#define MEMDEBUG_MAKE_MEM_DEFINED(addr, size)               ASAN_UNPOISON_MEMORY_REGION((addr), (size))
#define MEMDEBUG_MAKE_MEM_NOACCESS(addr, size)              ASAN_POISON_MEMORY_REGION((addr), (size))
#define MEMDEBUG_MAKE_MEM_UNDEFINED(addr, size)             ASAN_UNPOISON_MEMORY_REGION((addr), (size))
#define MEMDEBUG_MEMPOOL_ALLOC(context, addr, size)         ASAN_UNPOISON_MEMORY_REGION((addr), (size))
#define MEMDEBUG_MEMPOOL_FREE(context, addr)				do {} while (0)
#define MEMDEBUG_MEMPOOL_CHANGE(context, optr, nptr, size)	do {} while (0)
#else
#define MEMDEBUG_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)
#define MEMDEBUG_CREATE_MEMPOOL(context, redzones, zeroed)	do {} while (0)
#define MEMDEBUG_DESTROY_MEMPOOL(context)					do {} while (0)
#define MEMDEBUG_MAKE_MEM_DEFINED(addr, size)				do {} while (0)
#define MEMDEBUG_MAKE_MEM_NOACCESS(addr, size)				do {} while (0)
#define MEMDEBUG_MAKE_MEM_UNDEFINED(addr, size)				do {} while (0)
#define MEMDEBUG_MEMPOOL_ALLOC(context, addr, size)			do {} while (0)
#define MEMDEBUG_MEMPOOL_FREE(context, addr)				do {} while (0)
#define MEMDEBUG_MEMPOOL_CHANGE(context, optr, nptr, size)	do {} while (0)
#endif


#ifdef CLOBBER_FREED_MEMORY

/* Wipe freed memory for debugging purposes */
static inline void
wipe_mem(void *ptr, size_t size)
{
	MEMDEBUG_MAKE_MEM_UNDEFINED(ptr, size);
	memset(ptr, 0x7F, size);
	MEMDEBUG_MAKE_MEM_NOACCESS(ptr, size);
}

#endif							/* CLOBBER_FREED_MEMORY */

#ifdef MEMORY_CONTEXT_CHECKING

static inline void
set_sentinel(void *base, Size offset)
{
	char	   *ptr = (char *) base + offset;

	MEMDEBUG_MAKE_MEM_UNDEFINED(ptr, 1);
	*ptr = 0x7E;
	MEMDEBUG_MAKE_MEM_NOACCESS(ptr, 1);
}

static inline bool
sentinel_ok(const void *base, Size offset)
{
	const char *ptr = (const char *) base + offset;
	bool		ret;

	MEMDEBUG_MAKE_MEM_DEFINED(ptr, 1);
	ret = *ptr == 0x7E;
	MEMDEBUG_MAKE_MEM_NOACCESS(ptr, 1);

	return ret;
}

#endif							/* MEMORY_CONTEXT_CHECKING */

#ifdef RANDOMIZE_ALLOCATED_MEMORY

void		randomize_mem(char *ptr, size_t size);

#endif							/* RANDOMIZE_ALLOCATED_MEMORY */

#endif							/* MEMDEBUG_H */
