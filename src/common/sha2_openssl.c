/*-------------------------------------------------------------------------
 *
 * sha2_openssl.c
 *	  Set of wrapper routines on top of OpenSSL to support SHA-224
 *	  SHA-256, SHA-384 and SHA-512 functions.
 *
 * This should only be used if code is compiled with OpenSSL support.
 *
 * Portions Copyright (c) 2025, Alibaba Group Holding Limited
 * Portions Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/common/sha2_openssl.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <openssl/sha.h>

#include "common/sha2.h"
#include "sha2_int.h"

/* Interface routines for SHA-512 */
void
pg_sha512_init(pg_sha512_ctx *ctx)
{
	SHA512_Init((SHA512_CTX *) ctx);
}

void
pg_sha512_update(pg_sha512_ctx *ctx, const uint8 *data, size_t len)
{
	SHA512_Update((SHA512_CTX *) ctx, data, len);
}

void
pg_sha512_final(pg_sha512_ctx *ctx, uint8 *dest)
{
	SHA512_Final(dest, (SHA512_CTX *) ctx);
}
