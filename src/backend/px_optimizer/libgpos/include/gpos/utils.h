//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		utils.h
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_utils_H
#define GPOS_utils_H

#include "gpos/error/CException.h"
#include "gpos/io/COstreamBasic.h"
#include "gpos/types.h"

#define ALIGNED_16(x) \
	(((ULONG_PTR) x >> 1) << 1 == (ULONG_PTR) x)  // checks 16-bit alignment
#define ALIGNED_32(x) \
	(((ULONG_PTR) x >> 2) << 2 == (ULONG_PTR) x)  // checks 32-bit alignment
#define ALIGNED_64(x) \
	(((ULONG_PTR) x >> 3) << 3 == (ULONG_PTR) x)  // checks 64-bit alignment

#define MAX_ALIGNED(x) ALIGNED_64(x)

#define ALIGN_STORAGE __attribute__((aligned(8)))

#define GPOS_GET_FRAME_POINTER(x) ((x) = (ULONG_PTR) __builtin_frame_address(0))

#define GPOS_MSEC_IN_SEC ((ULLONG) 1000)
#define GPOS_USEC_IN_MSEC ((ULLONG) 1000)
#define GPOS_USEC_IN_SEC (((ULLONG) 1000) * 1000)
#define GPOS_NSEC_IN_SEC (((ULLONG) 1000) * 1000 * 1000)

namespace gpos
{
// print wide-character string to stdout
void Print(WCHAR *wsz);

// generic memory dumper routine
IOstream &HexDump(IOstream &os, const void *pv, ULLONG size);

// generic hash function for byte strings
ULONG HashByteArray(const BYTE *, const ULONG);

// generic hash function; by address
template <class T>
inline ULONG
HashValue(const T *pt)
{
	return HashByteArray((BYTE *) pt, GPOS_SIZEOF(T));
}

// generic hash function for pointer types -- use e.g. when address is ID of object
template <class T>
inline ULONG
HashPtr(const T *pt)
{
	return HashByteArray((BYTE *) &pt, GPOS_SIZEOF(void *));
}

// equality function on pointers
template <class T>
inline BOOL
EqualPtr(const T *pt1, const T *pt2)
{
	return pt1 == pt2;
}

// hash function for ULONG_PTR
inline ULONG
HashULongPtr(const ULONG_PTR &key)
{
	return (ULONG) key;
}

// combine ULONG hashes
ULONG CombineHashes(ULONG, ULONG);

// equality function, which uses the equality operator of the arguments type
template <class T>
inline BOOL
Equals(const T *pt1, const T *pt2)
{
	return *pt1 == *pt2;
}

// equality function for ULONG_PTR
inline BOOL
EqualULongPtr(const ULONG_PTR &key_left, const ULONG_PTR &key_right)
{
	return key_left == key_right;
}

// yield and sleep (time in muSec)
// note that in some platforms the minimum sleep interval is 1ms
void USleep(ULONG);

// add two unsigned long long values, throw an exception if overflow occurs
ULLONG Add(ULLONG first, ULLONG second);

// multiply two unsigned long long values, throw an exception if overflow occurs
ULLONG Multiply(ULLONG first, ULLONG second);

// extern definitions for standard streams; to be used during
// startup/shutdown when outside of task framework
extern COstreamBasic oswcerr;
extern COstreamBasic oswcout;

}  // namespace gpos


#endif	// !GPOS_utils_H

// EOF
