/* ----------
 * pg_compress.h -
 *
 *	Definitions for the all kinds of compressor
 *
 * src/include/common/pg_compress.h
 * ----------
 */

#ifndef _PG_COMPRESS_H_
#define _PG_COMPRESS_H_

#include "common/pg_lzcompress.h"

#define NO_LZ4_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method lz4 not supported"), \
			 errdetail("This functionality requires the server to be built with lz4 support."), \
			 errhint("You need to rebuild PostgreSQL using %s.", "--with-lz4")))

#define NO_ZSTD_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method zstd not supported"), \
			 errdetail("This functionality requires the server to be built with zstd support."), \
			 errhint("You need to rebuild PostgreSQL using %s.", "--with-zstd")))

/*
 * Guess the maximum buffer size required to store a compressed version of
 * input size.
 */
#ifdef USE_LZ4
#include "lz4.h"
#define	LZ4_MAX_BUFSZ(inputsz)		LZ4_COMPRESSBOUND(inputsz)
#else
#define LZ4_MAX_BUFSZ(inputsz)		0
#endif

#ifdef USE_ZSTD
#include "zstd.h"
#define ZSTD_MAX_BUFSZ(inputsz)		ZSTD_COMPRESSBOUND(inputsz)
#else
#define ZSTD_MAX_BUFSZ(inputsz)		0
#endif

#define PGLZ_MAX_BUFSZ(inputsz)		PGLZ_MAX_OUTPUT(inputsz)

/* Buffer size required to store a compressed version of backup block image */
#define COMPRESS_BUFSIZE(inputsz)	Max(Max(PGLZ_MAX_BUFSZ(inputsz), LZ4_MAX_BUFSZ(inputsz)), ZSTD_MAX_BUFSZ(inputsz))

#endif							/* _PG_COMPRESS_H_ */
