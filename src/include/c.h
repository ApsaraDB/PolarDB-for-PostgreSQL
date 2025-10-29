/*-------------------------------------------------------------------------
 *
 * c.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/c.h
 *
 *-------------------------------------------------------------------------
 */
/* IWYU pragma: always_keep */
/*
 *----------------------------------------------------------------
 *	 TABLE OF CONTENTS
 *
 *		When adding stuff to this file, please try to put stuff
 *		into the relevant section, or add new sections as appropriate.
 *
 *	  section	description
 *	  -------	------------------------------------------------
 *		0)		pg_config.h and standard system headers
 *		1)		compiler characteristics
 *		2)		bool, true, false
 *		3)		standard system types
 *		4)		IsValid macros for system types
 *		5)		lengthof, alignment
 *		6)		assertions
 *		7)		widely useful macros
 *		8)		random stuff
 *		9)		system-specific hacks
 *
 * NOTE: since this file is included by both frontend and backend modules,
 * it's usually wrong to put an "extern" declaration here, unless it's
 * ifdef'd so that it's seen in only one case or the other.
 * typedefs and macros are the kind of thing that might go here.
 *
 *----------------------------------------------------------------
 */
#ifndef C_H
#define C_H

/* IWYU pragma: begin_exports */

/*
 * These headers must be included before any system headers, because on some
 * platforms they affect the behavior of the system headers (for example, by
 * defining _FILE_OFFSET_BITS).
 */
#include "pg_config.h"
#include "pg_config_manual.h"	/* must be after pg_config.h */
#include "pg_config_os.h"		/* config from include/port/PORTNAME.h */

/* System header files that should be available everywhere in Postgres */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <stdint.h>
#include <sys/types.h>
#include <errno.h>
#if defined(WIN32) || defined(__CYGWIN__)
#include <fcntl.h>				/* ensure O_BINARY is available */
#endif
#include <locale.h>
#ifdef HAVE_XLOCALE_H
#include <xlocale.h>
#endif
#ifdef ENABLE_NLS
#include <libintl.h>
#endif

 /* Pull in fundamental symbols that we also expose to applications */
#include "postgres_ext.h"

/* Define before including zlib.h to add const decorations to zlib API. */
#ifdef HAVE_LIBZ
#define ZLIB_CONST
#endif


/* ----------------------------------------------------------------
 *				Section 1: compiler characteristics
 * ----------------------------------------------------------------
 */

/*
 * Disable "inline" if PG_FORCE_DISABLE_INLINE is defined.
 * This is used to work around compiler bugs and might also be useful for
 * investigatory purposes.
 */
#ifdef PG_FORCE_DISABLE_INLINE
#undef inline
#define inline
#endif

/*
 * Previously used PostgreSQL-specific spelling, for backward compatibility
 * for extensions.
 */
#define pg_restrict restrict

/*
 * Attribute macros
 *
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Type-Attributes.html
 * Clang: https://clang.llvm.org/docs/AttributeReference.html
 */

/*
 * For compilers which don't support __has_attribute, we just define
 * __has_attribute(x) to 0 so that we can define macros for various
 * __attribute__s more easily below.
 */
#ifndef __has_attribute
#define __has_attribute(attribute) 0
#endif

/* only GCC supports the unused attribute */
#ifdef __GNUC__
#define pg_attribute_unused() __attribute__((unused))
#else
#define pg_attribute_unused()
#endif

/*
 * pg_nodiscard means the compiler should warn if the result of a function
 * call is ignored.  The name "nodiscard" is chosen in alignment with the C23
 * standard attribute with the same name.  For maximum forward compatibility,
 * place it before the declaration.
 */
#ifdef __GNUC__
#define pg_nodiscard __attribute__((warn_unused_result))
#else
#define pg_nodiscard
#endif

/*
 * pg_noreturn corresponds to the C11 noreturn/_Noreturn function specifier.
 * We can't use the standard name "noreturn" because some third-party code
 * uses __attribute__((noreturn)) in headers, which would get confused if
 * "noreturn" is defined to "_Noreturn", as is done by <stdnoreturn.h>.
 *
 * In a declaration, function specifiers go before the function name.  The
 * common style is to put them before the return type.  (The MSVC fallback has
 * the same requirement.  The GCC fallback is more flexible.)
 */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define pg_noreturn _Noreturn
#elif defined(__GNUC__)
#define pg_noreturn __attribute__((noreturn))
#elif defined(_MSC_VER)
#define pg_noreturn __declspec(noreturn)
#else
#define pg_noreturn
#endif

/*
 * This macro will disable address safety instrumentation for a function
 * when running with "-fsanitize=address". Think twice before using this!
 */
#if defined(__clang__) || __GNUC__ >= 8
#define pg_attribute_no_sanitize_address() __attribute__((no_sanitize("address")))
#elif __has_attribute(no_sanitize_address)
/* This would work for clang, but it's deprecated. */
#define pg_attribute_no_sanitize_address() __attribute__((no_sanitize_address))
#else
#define pg_attribute_no_sanitize_address()
#endif

/*
 * Place this macro before functions that should be allowed to make misaligned
 * accesses.  Think twice before using it on non-x86-specific code!
 * Testing can be done with "-fsanitize=alignment -fsanitize-trap=alignment"
 * on clang, or "-fsanitize=alignment -fno-sanitize-recover=alignment" on gcc.
 */
#if __clang_major__ >= 7 || __GNUC__ >= 8
#define pg_attribute_no_sanitize_alignment() __attribute__((no_sanitize("alignment")))
#else
#define pg_attribute_no_sanitize_alignment()
#endif

/*
 * pg_attribute_nonnull means the compiler should warn if the function is
 * called with the listed arguments set to NULL.  If no arguments are
 * listed, the compiler should warn if any pointer arguments are set to NULL.
 */
#if __has_attribute (nonnull)
#define pg_attribute_nonnull(...) __attribute__((nonnull(__VA_ARGS__)))
#else
#define pg_attribute_nonnull(...)
#endif

/*
 * pg_attribute_target allows specifying different target options that the
 * function should be compiled with (e.g., for using special CPU instructions).
 * Note that there still needs to be a configure-time check to verify that a
 * specific target is understood by the compiler.
 */
#if __has_attribute (target)
#define pg_attribute_target(...) __attribute__((target(__VA_ARGS__)))
#else
#define pg_attribute_target(...)
#endif

/*
 * Append PG_USED_FOR_ASSERTS_ONLY to definitions of variables that are only
 * used in assert-enabled builds, to avoid compiler warnings about unused
 * variables in assert-disabled builds.
 */
#ifdef USE_ASSERT_CHECKING
#define PG_USED_FOR_ASSERTS_ONLY
#else
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif

/* GCC supports format attributes */
#if defined(__GNUC__)
#define pg_attribute_format_arg(a) __attribute__((format_arg(a)))
#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))
#else
#define pg_attribute_format_arg(a)
#define pg_attribute_printf(f,a)
#endif

/* GCC supports aligned and packed */
#if defined(__GNUC__)
#define pg_attribute_aligned(a) __attribute__((aligned(a)))
#define pg_attribute_packed() __attribute__((packed))
#elif defined(_MSC_VER)
/*
 * MSVC supports aligned.
 *
 * Packing is also possible but only by wrapping the entire struct definition
 * which doesn't fit into our current macro declarations.
 */
#define pg_attribute_aligned(a) __declspec(align(a))
#else
/*
 * NB: aligned and packed are not given default definitions because they
 * affect code functionality; they *must* be implemented by the compiler
 * if they are to be used.
 */
#endif

/*
 * Use "pg_attribute_always_inline" in place of "inline" for functions that
 * we wish to force inlining of, even when the compiler's heuristics would
 * choose not to.  But, if possible, don't force inlining in unoptimized
 * debug builds.
 */
#if defined(__GNUC__) && defined(__OPTIMIZE__)
/* GCC supports always_inline via __attribute__ */
#define pg_attribute_always_inline __attribute__((always_inline)) inline
#elif defined(_MSC_VER)
/* MSVC has a special keyword for this */
#define pg_attribute_always_inline __forceinline
#else
/* Otherwise, the best we can do is to say "inline" */
#define pg_attribute_always_inline inline
#endif

/*
 * Forcing a function not to be inlined can be useful if it's the slow path of
 * a performance-critical function, or should be visible in profiles to allow
 * for proper cost attribution.  Note that unlike the pg_attribute_XXX macros
 * above, this should be placed before the function's return type and name.
 */
/* GCC supports noinline via __attribute__ */
#if defined(__GNUC__)
#define pg_noinline __attribute__((noinline))
/* msvc via declspec */
#elif defined(_MSC_VER)
#define pg_noinline __declspec(noinline)
#else
#define pg_noinline
#endif

/*
 * For now, just define pg_attribute_cold and pg_attribute_hot to be empty
 * macros on minGW 8.1.  There appears to be a compiler bug that results in
 * compilation failure.  At this time, we still have at least one buildfarm
 * animal running that compiler, so this should make that green again. It's
 * likely this compiler is not popular enough to warrant keeping this code
 * around forever, so let's just remove it once the last buildfarm animal
 * upgrades.
 */
#if defined(__MINGW64__) && __GNUC__ == 8 && __GNUC_MINOR__ == 1

#define pg_attribute_cold
#define pg_attribute_hot

#else
/*
 * Marking certain functions as "hot" or "cold" can be useful to assist the
 * compiler in arranging the assembly code in a more efficient way.
 */
#if __has_attribute (cold)
#define pg_attribute_cold __attribute__((cold))
#else
#define pg_attribute_cold
#endif

#if __has_attribute (hot)
#define pg_attribute_hot __attribute__((hot))
#else
#define pg_attribute_hot
#endif

#endif							/* defined(__MINGW64__) && __GNUC__ == 8 &&
								 * __GNUC_MINOR__ == 1 */
/*
 * Mark a point as unreachable in a portable fashion.  This should preferably
 * be something that the compiler understands, to aid code generation.
 * In assert-enabled builds, we prefer abort() for debugging reasons.
 */
#if defined(HAVE__BUILTIN_UNREACHABLE) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __builtin_unreachable()
#elif defined(_MSC_VER) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __assume(0)
#else
#define pg_unreachable() abort()
#endif

/*
 * Define a compiler-independent macro for determining if an expression is a
 * compile-time integer const.  We don't define this macro to return 0 when
 * unsupported due to the risk of users of the macro misbehaving if we return
 * 0 when the expression *is* an integer constant.  Callers may check if this
 * macro is defined by checking if HAVE_PG_INTEGER_CONSTANT_P is defined.
 */
#if defined(HAVE__BUILTIN_CONSTANT_P)

/* When __builtin_constant_p() is available, use it. */
#define pg_integer_constant_p(x) __builtin_constant_p(x)
#define HAVE_PG_INTEGER_CONSTANT_P
#elif defined(_MSC_VER) && defined(__STDC_VERSION__)

/*
 * With MSVC we can use a trick with _Generic to make this work.  This has
 * been borrowed from:
 * https://stackoverflow.com/questions/49480442/detecting-integer-constant-expressions-in-macros
 * and only works with integer constants.  Compilation will fail if given a
 * constant or variable of any type other than an integer.
 */
#define pg_integer_constant_p(x) \
	_Generic((1 ? ((void *) ((x) * (uintptr_t) 0)) : &(int) {1}), int *: 1, void *: 0)
#define HAVE_PG_INTEGER_CONSTANT_P
#endif

/*
 * pg_assume(expr) states that we assume `expr` to evaluate to true. In assert
 * enabled builds pg_assume() is turned into an assertion, in optimized builds
 * we try to clue the compiler into the fact that `expr` is true.
 *
 * This is useful for two purposes:
 *
 * 1) Avoid compiler warnings by telling the compiler about assumptions the
 *	  code makes. This is particularly useful when building with optimizations
 *	  and w/o assertions.
 *
 * 2) Help the compiler to generate more efficient code
 *
 * It is unspecified whether `expr` is evaluated, therefore it better be
 * side-effect free.
 */
#if defined(USE_ASSERT_CHECKING)
#define pg_assume(expr) Assert(expr)
#elif defined(HAVE__BUILTIN_UNREACHABLE)
#define pg_assume(expr) \
	do { \
		if (!(expr)) \
			__builtin_unreachable(); \
	} while (0)
#elif defined(_MSC_VER)
#define pg_assume(expr) __assume(expr)
#else
#define pg_assume(expr) ((void) 0)
#endif

/*
 * Hints to the compiler about the likelihood of a branch. Both likely() and
 * unlikely() return the boolean value of the contained expression.
 *
 * These should only be used sparingly, in very hot code paths. It's very easy
 * to mis-estimate likelihoods.
 */
#ifdef __GNUC__
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)	((x) != 0)
#define unlikely(x) ((x) != 0)
#endif

/*
 * CppAsString
 *		Convert the argument to a string, using the C preprocessor.
 * CppAsString2
 *		Convert the argument to a string, after one round of macro expansion.
 * CppConcat
 *		Concatenate two arguments together, using the C preprocessor.
 *
 * Note: There used to be support here for pre-ANSI C compilers that didn't
 * support # and ##.  Nowadays, these macros are just for clarity and/or
 * backward compatibility with existing PostgreSQL code.
 */
#define CppAsString(identifier) #identifier
#define CppAsString2(x)			CppAsString(x)
#define CppConcat(x, y)			x##y

/*
 * VA_ARGS_NARGS
 *		Returns the number of macro arguments it is passed.
 *
 * An empty argument still counts as an argument, so effectively, this is
 * "one more than the number of commas in the argument list".
 *
 * This works for up to 63 arguments.  Internally, VA_ARGS_NARGS_() is passed
 * 64+N arguments, and the C99 standard only requires macros to allow up to
 * 127 arguments, so we can't portably go higher.  The implementation is
 * pretty trivial: VA_ARGS_NARGS_() returns its 64th argument, and we set up
 * the call so that that is the appropriate one of the list of constants.
 * This idea is due to Laurent Deniau.
 */
#define VA_ARGS_NARGS(...) \
	VA_ARGS_NARGS_(__VA_ARGS__, \
				   63,62,61,60,                   \
				   59,58,57,56,55,54,53,52,51,50, \
				   49,48,47,46,45,44,43,42,41,40, \
				   39,38,37,36,35,34,33,32,31,30, \
				   29,28,27,26,25,24,23,22,21,20, \
				   19,18,17,16,15,14,13,12,11,10, \
				   9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define VA_ARGS_NARGS_( \
	_01,_02,_03,_04,_05,_06,_07,_08,_09,_10, \
	_11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
	_21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
	_31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
	_41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
	_51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
	_61,_62,_63,  N, ...) \
	(N)

/*
 * Generic function pointer.  This can be used in the rare cases where it's
 * necessary to cast a function pointer to a seemingly incompatible function
 * pointer type while avoiding gcc's -Wcast-function-type warnings.
 */
typedef void (*pg_funcptr_t) (void);

/*
 * We require C99, hence the compiler should understand flexible array
 * members.  However, for documentation purposes we still consider it to be
 * project style to write "field[FLEXIBLE_ARRAY_MEMBER]" not just "field[]".
 * When computing the size of such an object, use "offsetof(struct s, f)"
 * for portability.  Don't use "offsetof(struct s, f[0])", as this doesn't
 * work with MSVC and with C++ compilers.
 */
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

/*
 * Does the compiler support #pragma GCC system_header? We optionally use it
 * to avoid warnings that we can't fix (e.g. in the perl headers).
 * See https://gcc.gnu.org/onlinedocs/cpp/System-Headers.html
 *
 * Headers for which we do not want to show compiler warnings can,
 * conditionally, use #pragma GCC system_header to avoid warnings. Obviously
 * this should only be used for external headers over which we do not have
 * control.
 *
 * Support for the pragma is tested here, instead of during configure, as gcc
 * also warns about the pragma being used in a .c file. It's surprisingly hard
 * to get autoconf to use .h as the file-ending. Looks like gcc has
 * implemented the pragma since the 2000, so this test should suffice.
 *
 *
 * Alternatively, we could add the include paths for problematic headers with
 * -isystem, but that is a larger hammer and is harder to search for.
 *
 * A more granular alternative would be to use #pragma GCC diagnostic
 * push/ignored/pop, but gcc warns about unknown warnings being ignored, so
 * every to-be-ignored-temporarily compiler warning would require its own
 * pg_config.h symbol and #ifdef.
 */
#ifdef __GNUC__
#define HAVE_PRAGMA_GCC_SYSTEM_HEADER	1
#endif


/* ----------------------------------------------------------------
 *				Section 2:	bool, true, false
 * ----------------------------------------------------------------
 */

/*
 * bool
 *		Boolean value, either true or false.
 *
 * PostgreSQL currently cannot deal with bool of size other than 1; there are
 * static assertions around the code to prevent that.
 */

#include <stdbool.h>


/* ----------------------------------------------------------------
 *				Section 3:	standard system types
 * ----------------------------------------------------------------
 */

/*
 * Pointer
 *		Variable holding address of any memory resident object.
 *
 *		XXX Pointer arithmetic is done with this, so it can't be void *
 *		under "true" ANSI compilers.
 */
typedef char *Pointer;

/* Historical names for types in <stdint.h>. */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

/*
 * bitsN
 *		Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;			/* >= 8 bits */
typedef uint16 bits16;			/* >= 16 bits */
typedef uint32 bits32;			/* >= 32 bits */

/*
 * 64-bit integers
 */
#define INT64CONST(x)  INT64_C(x)
#define UINT64CONST(x) UINT64_C(x)

/* snprintf format strings to use for 64-bit integers */
#define INT64_FORMAT "%" PRId64
#define UINT64_FORMAT "%" PRIu64

/*
 * 128-bit signed and unsigned integers
 *		There currently is only limited support for such types.
 *		E.g. 128bit literals and snprintf are not supported; but math is.
 *		Also, because we exclude such types when choosing MAXIMUM_ALIGNOF,
 *		it must be possible to coerce the compiler to allocate them on no
 *		more than MAXALIGN boundaries.
 */
#if defined(PG_INT128_TYPE)
#if defined(pg_attribute_aligned) || ALIGNOF_PG_INT128_TYPE <= MAXIMUM_ALIGNOF
#define HAVE_INT128 1

typedef PG_INT128_TYPE int128
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
		   ;

typedef unsigned PG_INT128_TYPE uint128
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
		   ;

#endif
#endif

/* Historical names for limits in <stdint.h>. */
#define PG_INT8_MIN		INT8_MIN
#define PG_INT8_MAX		INT8_MAX
#define PG_UINT8_MAX	UINT8_MAX
#define PG_INT16_MIN	INT16_MIN
#define PG_INT16_MAX	INT16_MAX
#define PG_UINT16_MAX	UINT16_MAX
#define PG_INT32_MIN	INT32_MIN
#define PG_INT32_MAX	INT32_MAX
#define PG_UINT32_MAX	UINT32_MAX
#define PG_INT64_MIN	INT64_MIN
#define PG_INT64_MAX	INT64_MAX
#define PG_UINT64_MAX	UINT64_MAX

/*
 * We now always use int64 timestamps, but keep this symbol defined for the
 * benefit of external code that might test it.
 */
#define HAVE_INT64_TIMESTAMP

/*
 * Size
 *		Size of any memory resident object, as returned by sizeof.
 */
typedef size_t Size;

/*
 * Index
 *		Index into any memory resident array.
 *
 * Note:
 *		Indices are non negative.
 */
typedef unsigned int Index;

/*
 * Offset
 *		Offset into any memory resident array.
 *
 * Note:
 *		This differs from an Index in that an Index is always
 *		non negative, whereas Offset may be negative.
 */
typedef signed int Offset;

/*
 * Common Postgres datatype names (as used in the catalogs)
 */
typedef float float4;
typedef double float8;

/*
 * float8, int8, and related datatypes are now always pass-by-value.
 * We keep this symbol to avoid breaking extension code that may use it.
 */
#define FLOAT8PASSBYVAL true

/*
 * Oid, RegProcedure, TransactionId, SubTransactionId, MultiXactId,
 * CommandId
 */

/* typedef Oid is in postgres_ext.h */

/*
 * regproc is the type name used in the include/catalog headers, but
 * RegProcedure is the preferred name in C code.
 */
typedef Oid regproc;
typedef regproc RegProcedure;

typedef uint32 TransactionId;

typedef uint32 LocalTransactionId;

typedef uint32 SubTransactionId;

#define InvalidSubTransactionId		((SubTransactionId) 0)
#define TopSubTransactionId			((SubTransactionId) 1)

/* MultiXactId must be equivalent to TransactionId, to fit in t_xmax */
typedef TransactionId MultiXactId;

typedef uint32 MultiXactOffset;

typedef uint32 CommandId;

#define FirstCommandId	((CommandId) 0)
#define InvalidCommandId	(~(CommandId)0)


/* ----------------
 *		Variable-length datatypes all share the 'struct varlena' header.
 *
 * NOTE: for TOASTable types, this is an oversimplification, since the value
 * may be compressed or moved out-of-line.  However datatype-specific routines
 * are mostly content to deal with de-TOASTed values only, and of course
 * client-side routines should never see a TOASTed value.  But even in a
 * de-TOASTed value, beware of touching vl_len_ directly, as its
 * representation is no longer convenient.  It's recommended that code always
 * use macros VARDATA_ANY, VARSIZE_ANY, VARSIZE_ANY_EXHDR, VARDATA, VARSIZE,
 * and SET_VARSIZE instead of relying on direct mentions of the struct fields.
 * See varatt.h for details of the TOASTed form.
 * ----------------
 */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE_ANY_EXHDR(ptr).
 */
typedef struct varlena bytea;
typedef struct varlena text;
typedef struct varlena BpChar;	/* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar; /* var-length char, ie SQL varchar(n) */

/*
 * Specialized array types.  These are physically laid out just the same
 * as regular arrays (so that the regular array subscripting code works
 * with them).  They exist as distinct types mostly for historical reasons:
 * they have nonstandard I/O behavior which we don't want to change for fear
 * of breaking applications that look at the system catalogs.  There is also
 * an implementation issue for oidvector: it's part of the primary key for
 * pg_proc, and we can't use the normal btree array support routines for that
 * without circularity.
 */
typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for int2vector */
	int32		dataoffset;		/* always 0 for int2vector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	int16		values[FLEXIBLE_ARRAY_MEMBER];
} int2vector;

typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for oidvector */
	int32		dataoffset;		/* always 0 for oidvector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	Oid			values[FLEXIBLE_ARRAY_MEMBER];
} oidvector;

/*
 * Representation of a Name: effectively just a C string, but null-padded to
 * exactly NAMEDATALEN bytes.  The use of a struct is historical.
 */
typedef struct nameData
{
	char		data[NAMEDATALEN];
} NameData;
typedef NameData *Name;

#define NameStr(name)	((name).data)


/* ----------------------------------------------------------------
 *				Section 4:	IsValid macros for system types
 * ----------------------------------------------------------------
 */
/*
 * BoolIsValid
 *		True iff bool is valid.
 */
#define BoolIsValid(boolean)	((boolean) == false || (boolean) == true)

/*
 * PointerIsAligned
 *		True iff pointer is properly aligned to point to the given type.
 */
#define PointerIsAligned(pointer, type) \
		(((uintptr_t)(pointer) % (sizeof (type))) == 0)

#define OffsetToPointer(base, offset) \
		((void *)((char *) base + offset))

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define RegProcedureIsValid(p)	OidIsValid(p)


/* ----------------------------------------------------------------
 *				Section 5:	lengthof, alignment
 * ----------------------------------------------------------------
 */
/*
 * lengthof
 *		Number of elements in an array.
 */
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

/* ----------------
 * Alignment macros: align a length or address appropriately for a given type.
 * The fooALIGN() macros round up to a multiple of the required alignment,
 * while the fooALIGN_DOWN() macros round down.  The latter are more useful
 * for problems like "how many X-sized structures will fit in a page?".
 *
 * NOTE: TYPEALIGN[_DOWN] will not work if ALIGNVAL is not a power of 2.
 * That case seems extremely unlikely to be needed in practice, however.
 *
 * NOTE: MAXIMUM_ALIGNOF, and hence MAXALIGN(), intentionally exclude any
 * larger-than-8-byte types the compiler might have.
 * ----------------
 */

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
/* MAXALIGN covers only built-in types, not buffers */
#define BUFFERALIGN(LEN)		TYPEALIGN(ALIGNOF_BUFFER, (LEN))
#define CACHELINEALIGN(LEN)		TYPEALIGN(PG_CACHE_LINE_SIZE, (LEN))

#define TYPEALIGN_DOWN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_SHORT, (LEN))
#define INTALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_INT, (LEN))
#define LONGALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN_DOWN(LEN)		TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))
#define BUFFERALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_BUFFER, (LEN))

/*
 * The above macros will not work with types wider than uintptr_t, like with
 * uint64 on 32-bit platforms.  That's not problem for the usual use where a
 * pointer or a length is aligned, but for the odd case that you need to
 * align something (potentially) wider, use TYPEALIGN64.
 */
#define TYPEALIGN64(ALIGNVAL,LEN)  \
	(((uint64) (LEN) + ((ALIGNVAL) - 1)) & ~((uint64) ((ALIGNVAL) - 1)))

/* we don't currently need wider versions of the other ALIGN macros */
#define MAXALIGN64(LEN)			TYPEALIGN64(MAXIMUM_ALIGNOF, (LEN))


/* ----------------------------------------------------------------
 *				Section 6:	assertions
 * ----------------------------------------------------------------
 */

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Assert() can be used in both frontend and backend code. In frontend code it
 * just calls the standard assert, if it's available. If use of assertions is
 * not configured, it does nothing.
 */
#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#define AssertMacro(p)	((void) assert(p))

#else							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Assert
 *		Generates a fatal exception if the given condition is false.
 */
#define Assert(condition) \
	do { \
		if (!(condition)) \
			ExceptionalCondition(#condition, __FILE__, __LINE__); \
	} while (0)

/*
 * AssertMacro is the same as Assert but it's suitable for use in
 * expression-like macros, for example:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 */
#define AssertMacro(condition) \
	((void) ((condition) || \
			 (ExceptionalCondition(#condition, __FILE__, __LINE__), 0)))

#endif							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Check that `ptr' is `bndr' aligned.
 */
#define AssertPointerAlignment(ptr, bndr) \
	Assert(TYPEALIGN(bndr, (uintptr_t)(ptr)) == (uintptr_t)(ptr))

/*
 * ExceptionalCondition is compiled into the backend whether or not
 * USE_ASSERT_CHECKING is defined, so as to support use of extensions
 * that are built with that #define with a backend that isn't.  Hence,
 * we should declare it as long as !FRONTEND.
 */
#ifndef FRONTEND
pg_noreturn extern void ExceptionalCondition(const char *conditionName,
											 const char *fileName, int lineNumber);
#endif

/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * throw a compile error using the "errmessage" (a string literal).
 *
 * C11 has _Static_assert(), and most C99 compilers already support that.  For
 * portability, we wrap it into StaticAssertDecl().  _Static_assert() is a
 * "declaration", and so it must be placed where for example a variable
 * declaration would be valid.  As long as we compile with
 * -Wno-declaration-after-statement, that also means it cannot be placed after
 * statements in a function.  Macros StaticAssertStmt() and StaticAssertExpr()
 * make it safe to use as a statement or in an expression, respectively.
 *
 * For compilers without _Static_assert(), we fall back on a kluge that
 * assumes the compiler will complain about a negative width for a struct
 * bit-field.  This will not include a helpful error message, but it beats not
 * getting an error at all.
 */
#ifndef __cplusplus
#ifdef HAVE__STATIC_ASSERT
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); true; }))
#else							/* !HAVE__STATIC_ASSERT */
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
	((void) sizeof(struct { int static_assert_failure : (condition) ? 1 : -1; }))
#define StaticAssertExpr(condition, errmessage) \
	StaticAssertStmt(condition, errmessage)
#endif							/* HAVE__STATIC_ASSERT */
#else							/* C++ */
#if defined(__cpp_static_assert) && __cpp_static_assert >= 200410
#define StaticAssertDecl(condition, errmessage) \
	static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	static_assert(condition, errmessage)
#define StaticAssertExpr(condition, errmessage) \
	({ static_assert(condition, errmessage); })
#else							/* !__cpp_static_assert */
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
	do { struct static_assert_struct { int static_assert_failure : (condition) ? 1 : -1; }; } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); }))
#endif							/* __cpp_static_assert */
#endif							/* C++ */


/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is intended for use in macros, eg
 *		#define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it provides at least some coverage.
 */
#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(__builtin_types_compatible_p(__typeof__(varname), typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof__(varname), typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#else							/* !HAVE__BUILTIN_TYPES_COMPATIBLE_P */
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(sizeof(varname) == sizeof(typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(sizeof(varname) == sizeof(typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#endif							/* HAVE__BUILTIN_TYPES_COMPATIBLE_P */


/* ----------------------------------------------------------------
 *				Section 7:	widely useful macros
 * ----------------------------------------------------------------
 */
/*
 * Max
 *		Return the maximum of two numbers.
 */
#define Max(x, y)		((x) > (y) ? (x) : (y))

/*
 * Min
 *		Return the minimum of two numbers.
 */
#define Min(x, y)		((x) < (y) ? (x) : (y))


/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)

/*
 * MemSet
 *	Exactly the same as standard library function memset(), but considerably
 *	faster for zeroing small word-aligned structures (such as parsetree nodes).
 *	This has to be a macro because the main point is to avoid function-call
 *	overhead.   However, we have also found that the loop is faster than
 *	native libc memset() on some platforms, even those with assembler
 *	memset() functions.  More research needs to be done, perhaps with
 *	MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len) \
	do \
	{ \
		/* must be void* because we don't know if it is integer aligned yet */ \
		void   *_vstart = (void *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
			(_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			/* \
			 *	If MEMSET_LOOP_LIMIT == 0, optimizer should find \
			 *	the whole "if" false at compile time. \
			 */ \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_start = (long *) _vstart; \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_vstart, _val, _len); \
	} while (0)

/*
 * MemSetAligned is the same as MemSet except it omits the test to see if
 * "start" is word-aligned.  This is okay to use if the caller knows a-priori
 * that the pointer is suitably aligned (typically, because he just got it
 * from palloc(), which always delivers a max-aligned pointer).
 */
#define MemSetAligned(start, val, len) \
	do \
	{ \
		long   *_start = (long *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_start, _val, _len); \
	} while (0)


/*
 * Macros for range-checking float values before converting to integer.
 * We must be careful here that the boundary values are expressed exactly
 * in the float domain.  PG_INTnn_MIN is an exact power of 2, so it will
 * be represented exactly; but PG_INTnn_MAX isn't, and might get rounded
 * off, so avoid using that.
 * The input must be rounded to an integer beforehand, typically with rint(),
 * else we might draw the wrong conclusion about close-to-the-limit values.
 * These macros will do the right thing for Inf, but not necessarily for NaN,
 * so check isnan(num) first if that's a possibility.
 */
#define FLOAT4_FITS_IN_INT16(num) \
	((num) >= (float4) PG_INT16_MIN && (num) < -((float4) PG_INT16_MIN))
#define FLOAT4_FITS_IN_INT32(num) \
	((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
#define FLOAT4_FITS_IN_INT64(num) \
	((num) >= (float4) PG_INT64_MIN && (num) < -((float4) PG_INT64_MIN))
#define FLOAT8_FITS_IN_INT16(num) \
	((num) >= (float8) PG_INT16_MIN && (num) < -((float8) PG_INT16_MIN))
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))


/* ----------------------------------------------------------------
 *				Section 8:	random stuff
 * ----------------------------------------------------------------
 */

/*
 * Invert the sign of a qsort-style comparison result, ie, exchange negative
 * and positive integer values, being careful not to get the wrong answer
 * for INT_MIN.  The argument should be an integral variable.
 */
#define INVERT_COMPARE_RESULT(var) \
	((var) = ((var) < 0) ? 1 : -(var))

/*
 * Use this, not "char buf[BLCKSZ]", to declare a field or local variable
 * holding a page buffer, if that page might be accessed as a page.  Otherwise
 * the variable might be under-aligned, causing problems on alignment-picky
 * hardware.  We include both "double" and "int64" in the union to ensure that
 * the compiler knows the value must be MAXALIGN'ed (cf. configure's
 * computation of MAXIMUM_ALIGNOF).
 */
typedef union PGAlignedBlock
{
	char		data[BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedBlock;

/*
 * Use this to declare a field or local variable holding a page buffer, if that
 * page might be accessed as a page or passed to an SMgr I/O function.  If
 * allocating using the MemoryContext API, the aligned allocation functions
 * should be used with PG_IO_ALIGN_SIZE.  This alignment may be more efficient
 * for I/O in general, but may be strictly required on some platforms when
 * using direct I/O.
 */
typedef union PGIOAlignedBlock
{
#ifdef pg_attribute_aligned
	pg_attribute_aligned(PG_IO_ALIGN_SIZE)
#endif
	char		data[BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGIOAlignedBlock;

/* Same, but for an XLOG_BLCKSZ-sized buffer */
typedef union PGAlignedXLogBlock
{
#ifdef pg_attribute_aligned
	pg_attribute_aligned(PG_IO_ALIGN_SIZE)
#endif
	char		data[XLOG_BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedXLogBlock;

/* msb for char */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/*
 * Support macros for escaping strings.  escape_backslash should be true
 * if generating a non-standard-conforming string.  Prefixing a string
 * with ESCAPE_STRING_SYNTAX guarantees it is non-standard-conforming.
 * Beware of multiple evaluation of the "ch" argument!
 */
#define SQL_STR_DOUBLE(ch, escape_backslash)	\
	((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))

#define ESCAPE_STRING_SYNTAX	'E'


#define STATUS_OK				(0)
#define STATUS_ERROR			(-1)
#define STATUS_EOF				(-2)

/*
 * gettext support
 */

#ifndef ENABLE_NLS
/* stuff we'd otherwise get from <libintl.h> */
#define gettext(x) (x)
#define dgettext(d,x) (x)
#define ngettext(s,p,n) ((n) == 1 ? (s) : (p))
#define dngettext(d,s,p,n) ((n) == 1 ? (s) : (p))
#endif

#define _(x) gettext(x)

/*
 *	Use this to mark string constants as needing translation at some later
 *	time, rather than immediately.  This is useful for cases where you need
 *	access to the original string and translated string, and for cases where
 *	immediate translation is not possible, like when initializing global
 *	variables.
 *
 *	https://www.gnu.org/software/gettext/manual/html_node/Special-cases.html
 */
#define gettext_noop(x) (x)

/*
 * To better support parallel installations of major PostgreSQL
 * versions as well as parallel installations of major library soname
 * versions, we mangle the gettext domain name by appending those
 * version numbers.  The coding rule ought to be that wherever the
 * domain name is mentioned as a literal, it must be wrapped into
 * PG_TEXTDOMAIN().  The macros below do not work on non-literals; but
 * that is somewhat intentional because it avoids having to worry
 * about multiple states of premangling and postmangling as the values
 * are being passed around.
 *
 * Make sure this matches the installation rules in nls-global.mk.
 */
#ifdef SO_MAJOR_VERSION
#define PG_TEXTDOMAIN(domain) (domain CppAsString2(SO_MAJOR_VERSION) "-" PG_MAJORVERSION)
#else
#define PG_TEXTDOMAIN(domain) (domain "-" PG_MAJORVERSION)
#endif

/*
 * Macro that allows to cast constness and volatile away from an expression, but doesn't
 * allow changing the underlying type.  Enforcement of the latter
 * currently only works for gcc like compilers.
 *
 * Please note IT IS NOT SAFE to cast constness away if the result will ever
 * be modified (it would be undefined behaviour). Doing so anyway can cause
 * compiler misoptimizations or runtime crashes (modifying readonly memory).
 * It is only safe to use when the result will not be modified, but API
 * design or language restrictions prevent you from declaring that
 * (e.g. because a function returns both const and non-const variables).
 *
 * Note that this only works in function scope, not for global variables (it'd
 * be nice, but not trivial, to improve that).
 */
#if defined(__cplusplus)
#define unconstify(underlying_type, expr) const_cast<underlying_type>(expr)
#define unvolatize(underlying_type, expr) const_cast<underlying_type>(expr)
#elif defined(HAVE__BUILTIN_TYPES_COMPATIBLE_P)
#define unconstify(underlying_type, expr) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), const underlying_type), \
					  "wrong cast"), \
	 (underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), volatile underlying_type), \
					  "wrong cast"), \
	 (underlying_type) (expr))
#else
#define unconstify(underlying_type, expr) \
	((underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
	((underlying_type) (expr))
#endif

/* ----------------------------------------------------------------
 *				Section 9: system-specific hacks
 *
 *		This should be limited to things that absolutely have to be
 *		included in every source file.  The port-specific header file
 *		is usually a better place for this sort of thing.
 * ----------------------------------------------------------------
 */

/*
 *	NOTE:  this is also used for opening text files.
 *	WIN32 treats Control-Z as EOF in files opened in text mode.
 *	Therefore, we open files in binary mode on Win32 so we can read
 *	literal control-Z.  The other affect is that we see CRLF, but
 *	that is OK because we can already handle those cleanly.
 */
#if defined(WIN32) || defined(__CYGWIN__)
#define PG_BINARY	O_BINARY
#define PG_BINARY_A "ab"
#define PG_BINARY_R "rb"
#define PG_BINARY_W "wb"
#else
#define PG_BINARY	0
#define PG_BINARY_A "a"
#define PG_BINARY_R "r"
#define PG_BINARY_W "w"
#endif

/*
 * Provide prototypes for routines not present in a particular machine's
 * standard C library.
 */

#if !HAVE_DECL_FDATASYNC
extern int	fdatasync(int fildes);
#endif

/*
 * Thin wrappers that convert strings to exactly 64-bit integers, matching our
 * definition of int64.  (For the naming, compare that POSIX has
 * strtoimax()/strtoumax() which return intmax_t/uintmax_t.)
 */
#if SIZEOF_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtol(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoul(str, endptr, base))
#elif SIZEOF_LONG_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoull(str, endptr, base))
#else
#error "cannot find integer type of the same size as int64_t"
#endif

/*
 * Similarly, wrappers around labs()/llabs() matching our int64.
 */
#if SIZEOF_LONG == 8
#define i64abs(i) ((int64) labs(i))
#elif SIZEOF_LONG_LONG == 8
#define i64abs(i) ((int64) llabs(i))
#else
#error "cannot find integer type of the same size as int64_t"
#endif

/*
 * Use "extern PGDLLIMPORT ..." to declare variables that are defined
 * in the core backend and need to be accessible by loadable modules.
 * No special marking is required on most ports.
 */
#ifndef PGDLLIMPORT
#define PGDLLIMPORT
#endif

/*
 * Use "extern PGDLLEXPORT ..." to declare functions that are defined in
 * loadable modules and need to be callable by the core backend or other
 * loadable modules.
 * If the compiler knows __attribute__((visibility("*"))), we use that,
 * unless we already have a platform-specific definition.  Otherwise,
 * no special marking is required.
 */
#ifndef PGDLLEXPORT
#ifdef HAVE_VISIBILITY_ATTRIBUTE
#define PGDLLEXPORT __attribute__((visibility("default")))
#else
#define PGDLLEXPORT
#endif
#endif

/*
 * The following is used as the arg list for signal handlers.  Any ports
 * that take something other than an int argument should override this in
 * their pg_config_os.h file.  Note that variable names are required
 * because it is used in both the prototypes as well as the definitions.
 * Note also the long name.  We expect that this won't collide with
 * other names causing compiler warnings.
 */

#ifndef SIGNAL_ARGS
#define SIGNAL_ARGS  int postgres_signal_arg
#endif

/*
 * When there is no sigsetjmp, its functionality is provided by plain
 * setjmp.  We now support the case only on Windows.  However, it seems
 * that MinGW-64 has some longstanding issues in its setjmp support,
 * so on that toolchain we cheat and use gcc's builtins.
 */
#ifdef WIN32
#ifdef __MINGW64__
typedef intptr_t sigjmp_buf[5];
#define sigsetjmp(x,y) __builtin_setjmp(x)
#define siglongjmp __builtin_longjmp
#else							/* !__MINGW64__ */
#define sigjmp_buf jmp_buf
#define sigsetjmp(x,y) setjmp(x)
#define siglongjmp longjmp
#endif							/* __MINGW64__ */
#endif							/* WIN32 */

/* /port compatibility functions */
#include "port.h"

/*
 * char16_t and char32_t
 *      Unicode code points.
 *
 * uchar.h should always be available in C11, but it's not available on
 * Mac. However, these types are keywords in C++11, so when using C++, we
 * can't redefine the types.
 *
 * XXX: when uchar.h is available everywhere, we can remove this check and
 * just include uchar.h unconditionally.
 *
 * XXX: this section is out of place because uchar.h needs to be included
 * after port.h, due to an interaction with win32_port.h in some cases.
 */
#ifdef HAVE_UCHAR_H
#include <uchar.h>
#else
#ifndef __cplusplus
typedef uint16_t char16_t;
typedef uint32_t char32_t;
#endif
#endif

/* IWYU pragma: end_exports */

#endif							/* C_H */
