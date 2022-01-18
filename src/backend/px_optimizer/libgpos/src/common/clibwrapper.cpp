//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		clibwrapper.cpp
//
//	@doc:
//		Wrapper for functions in C library
//
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"

#include <cxxabi.h>
#include <dlfcn.h>
#include <errno.h>
#include <fenv.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <wchar.h>

#include "gpos/assert.h"
#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/utils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		clib::USleep
//
//	@doc:
//		Sleep given number of microseconds
//
//---------------------------------------------------------------------------
void
gpos::clib::USleep(ULONG usecs)
{
	GPOS_ASSERT(1000000 >= usecs);

	// ignore return value
	(void) usleep(usecs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strcmp
//
//	@doc:
//		Compare two strings
//
//---------------------------------------------------------------------------
INT
gpos::clib::Strcmp(const CHAR *left, const CHAR *right)
{
	GPOS_ASSERT(nullptr != left);
	GPOS_ASSERT(nullptr != right);

	return strcmp(left, right);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strncmp
//
//	@doc:
//		Compare two strings up to a specified number of characters
//
//---------------------------------------------------------------------------
INT
gpos::clib::Strncmp(const CHAR *left, const CHAR *right, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != left);
	GPOS_ASSERT(nullptr != right);

	return strncmp(left, right, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Memcmp
//
//	@doc:
//		Compare a specified number of bytes of two regions of memory
//---------------------------------------------------------------------------
INT
gpos::clib::Memcmp(const void *left, const void *right, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != left);
	GPOS_ASSERT(nullptr != right);

	return memcmp(left, right, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Wcsncmp
//
//	@doc:
//		Compare two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
INT
gpos::clib::Wcsncmp(const WCHAR *left, const WCHAR *right, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != left);
	GPOS_ASSERT(nullptr != right);

	return wcsncmp(left, right, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::WcStrNCpy
//
//	@doc:
//		Copy two strings up to a specified number of wide characters
//
//---------------------------------------------------------------------------
WCHAR *
gpos::clib::WcStrNCpy(WCHAR *dest, const WCHAR *src, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != dest);
	GPOS_ASSERT(nullptr != src && num_bytes > 0);

	// check for overlap
	GPOS_ASSERT(((src + num_bytes) <= dest) || ((dest + num_bytes) <= src));

	return wcsncpy(dest, src, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Memcpy
//
//	@doc:
//		Copy a specified number of bytes between two memory areas
//
//---------------------------------------------------------------------------
void *
gpos::clib::Memcpy(void *dest, const void *src, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != dest);

	GPOS_ASSERT(nullptr != src && num_bytes > 0);

#ifdef GPOS_DEBUG
	const BYTE *src_addr = static_cast<const BYTE *>(src);
	const BYTE *dest_addr = static_cast<const BYTE *>(dest);
#endif	// GPOS_DEBUG

	// check for overlap
	GPOS_ASSERT(((src_addr + num_bytes) <= dest_addr) ||
				((dest_addr + num_bytes) <= src_addr));

	return memcpy(dest, src, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Wmemcpy
//
//	@doc:
//		Copy a specified number of wide characters
//
//---------------------------------------------------------------------------
WCHAR *
gpos::clib::Wmemcpy(WCHAR *dest, const WCHAR *src, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != dest);
	GPOS_ASSERT(nullptr != src && num_bytes > 0);

#ifdef GPOS_DEBUG
	const WCHAR *src_addr = static_cast<const WCHAR *>(src);
	const WCHAR *dest_addr = static_cast<WCHAR *>(dest);
#endif

	// check for overlap
	GPOS_ASSERT(((src_addr + num_bytes) <= dest_addr) ||
				((dest_addr + num_bytes) <= src_addr));

	return wmemcpy(dest, src, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strncpy
//
//	@doc:
//		Copy a specified number of characters
//
//---------------------------------------------------------------------------
CHAR *
gpos::clib::Strncpy(CHAR *dest, const CHAR *src, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != dest);
	GPOS_ASSERT(nullptr != src && num_bytes > 0);
	GPOS_ASSERT(((src + num_bytes) <= dest) || ((dest + num_bytes) <= src));

	return strncpy(dest, src, num_bytes);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strchr
//
//	@doc:
//		Find the first occurrence of the character c (converted to a char) in
//		the null-terminated string beginning at src. Returns a pointer to the
//		located character, or a null pointer if no match was found
//
//---------------------------------------------------------------------------
CHAR *
gpos::clib::Strchr(const CHAR *src, INT c)
{
	GPOS_ASSERT(nullptr != src);

	return (CHAR *) strchr(src, c);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Memset
//
//	@doc:
//		Set the bytes of a given memory block to a specific value
//
//---------------------------------------------------------------------------
void *
gpos::clib::Memset(void *dest, INT value, SIZE_T num_bytes)
{
	GPOS_ASSERT(nullptr != dest);
	GPOS_ASSERT_IFF(0 <= value, 255 >= value);

	return memset(dest, value, num_bytes);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Qsort
//
//	@doc:
//		Sort a specified number of elements
//
//---------------------------------------------------------------------------
void
gpos::clib::Qsort(void *dest, SIZE_T num_bytes, SIZE_T size,
				  Comparator comparator)
{
	GPOS_ASSERT(nullptr != dest);

	qsort(dest, num_bytes, size, comparator);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Getopt
//
//	@doc:
//		Parse the command-line arguments
//
//---------------------------------------------------------------------------
INT
gpos::clib::Getopt(INT argc, CHAR *const argv[], const CHAR *opt_string)
{
	return getopt(argc, argv, opt_string);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strtol
//
//	@doc:
//		Convert string to long integer
//
//---------------------------------------------------------------------------
LINT
gpos::clib::Strtol(const CHAR *val, CHAR **end, ULONG base)
{
	GPOS_ASSERT(nullptr != val);
	GPOS_ASSERT(0 == base || 2 == base || 10 == base || 16 == base);

	return strtol(val, end, base);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Strtoll
//
//	@doc:
//		Convert string to long long integer
//
//---------------------------------------------------------------------------
LINT
gpos::clib::Strtoll(const CHAR *val, CHAR **end, ULONG base)
{
	GPOS_ASSERT(nullptr != val);
	GPOS_ASSERT(0 == base || 2 == base || 10 == base || 16 == base);

	return strtoll(val, end, base);
}

//---------------------------------------------------------------------------
//	@function:
//		clib::Rand
//
//	@doc:
//		Return a pseudo-random integer between 0 and RAND_MAX
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::Rand(ULONG *seed)
{
	GPOS_ASSERT(nullptr != seed);

	INT res = rand_r(seed);

	GPOS_ASSERT(res >= 0 && res <= RAND_MAX);

	return static_cast<ULONG>(res);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Vswprintf
//
//	@doc:
//		Format wide character output conversion
//
//---------------------------------------------------------------------------
INT
gpos::clib::Vswprintf(WCHAR *wcstr, SIZE_T max_len, const WCHAR *format,
					  VA_LIST vaArgs)
{
	GPOS_ASSERT(nullptr != wcstr);
	GPOS_ASSERT(nullptr != format);

	INT res = vswprintf(wcstr, max_len, format, vaArgs);
	if (-1 == res && EILSEQ == errno)
	{
		// Invalid multibyte character encountered. This can happen if the byte sequence does not
		// match with the server encoding.
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIllegalByteSequence);
	}

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Vsnprintf
//
//	@doc:
//		Format string
//
//---------------------------------------------------------------------------
INT
gpos::clib::Vsnprintf(CHAR *src, SIZE_T size, const CHAR *format,
					  VA_LIST vaArgs)
{
	GPOS_ASSERT(nullptr != src);
	GPOS_ASSERT(nullptr != format);

	return vsnprintf(src, size, format, vaArgs);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strerror_r
//
//	@doc:
//		Return string describing error number
//
//---------------------------------------------------------------------------
void
gpos::clib::Strerror_r(INT errnum, CHAR *buf, SIZE_T buf_len)
{
	GPOS_ASSERT(nullptr != buf);

#ifdef _GNU_SOURCE
	// GNU-specific strerror_r() returns char*.
	CHAR *error_str = strerror_r(errnum, buf, buf_len);
	GPOS_ASSERT(nullptr != error_str);

	// GNU strerror_r() may return a pointer to a static error string.
	// Copy it into 'buf' if that is the case.
	if (error_str != buf)
	{
		strncpy(buf, error_str, buf_len);
		// Ensure null-terminated.
		buf[buf_len - 1] = '\0';
	}
#else  // !_GNU_SOURCE
	// POSIX.1-2001 standard strerror_r() returns int.
	INT str_err_code GPOS_ASSERTS_ONLY = strerror_r(errnum, buf, buf_len);
	GPOS_ASSERT(0 == str_err_code);

#endif
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Wcslen
//
//	@doc:
//		Calculate the length of a wide-character string
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::Wcslen(const WCHAR *dest)
{
	GPOS_ASSERT(nullptr != dest);

	return (ULONG) wcslen(dest);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Localtime_r
//
//	@doc:
//		Convert the calendar time time to broken-time representation;
//		Expressed relative to the user's specified time zone
//
//---------------------------------------------------------------------------
struct tm *
gpos::clib::Localtime_r(const TIME_T *time, TIME *result)
{
	GPOS_ASSERT(nullptr != time);

	localtime_r(time, result);

	GPOS_ASSERT(nullptr != result);

	return result;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Malloc
//
//	@doc:
//		Allocate dynamic memory
//
//---------------------------------------------------------------------------
void *
gpos::clib::Malloc(SIZE_T size)
{
	return malloc(size);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Free
//
//	@doc:
//		Free dynamic memory
//
//---------------------------------------------------------------------------
void
gpos::clib::Free(void *src)
{
	free(src);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strlen
//
//	@doc:
//		Calculate the length of a string
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::Strlen(const CHAR *buf)
{
	GPOS_ASSERT(nullptr != buf);

	return (ULONG) strlen(buf);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Wctomb
//
//	@doc:
//		Convert a wide character to a multibyte sequence
//
//---------------------------------------------------------------------------
INT
gpos::clib::Wctomb(CHAR *dest, WCHAR src)
{
	return wctomb(dest, src);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Mbstowcs
//
//	@doc:
//		Convert a multibyte sequence to wide character array
//
//---------------------------------------------------------------------------
ULONG
gpos::clib::Mbstowcs(WCHAR *dest, const CHAR *src, SIZE_T len)
{
	GPOS_ASSERT(nullptr != dest);
	GPOS_ASSERT(nullptr != src);

	return (ULONG) mbstowcs(dest, src, len);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Wcstombs
//
//	@doc:
//		Convert a wide-character string to a multi-byte string
//
//---------------------------------------------------------------------------
LINT
gpos::clib::Wcstombs(CHAR *dest, WCHAR *src, ULONG_PTR dest_size)
{
	return wcstombs(dest, src, dest_size);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Strtod
//
//	@doc:
//		Convert string to double;
//		if conversion fails, return 0.0
//
//---------------------------------------------------------------------------
DOUBLE
gpos::clib::Strtod(const CHAR *str)
{
	return strtod(str, nullptr);
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Demangle
//
//	@doc:
//		Return a pointer to the start of the NULL-terminated
//		symbol or NULL if demangling fails
//
//---------------------------------------------------------------------------
CHAR *
gpos::clib::Demangle(const CHAR *symbol, CHAR *buf, SIZE_T *len, INT *status)
{
	GPOS_ASSERT(nullptr != symbol);

	CHAR *res = abi::__cxa_demangle(symbol, buf, len, status);

	GPOS_ASSERT(-3 != *status && "One of the arguments is invalid.");

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		clib::Dladdr
//
//	@doc:
//		Resolve symbol information from its address
//
//---------------------------------------------------------------------------
void
gpos::clib::Dladdr(void *addr, DL_INFO *info)
{
	INT res GPOS_ASSERTS_ONLY = dladdr(addr, info);

	GPOS_ASSERT(0 != res);
}

// EOF
