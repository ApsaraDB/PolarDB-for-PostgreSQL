/*-------------------------------------------------------------------------
 *
 * encode.c
 *	  Various data encoding/decoding things.
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/encode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "mb/pg_wchar.h"
#include "port/simd.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "varatt.h"


/*
 * Encoding conversion API.
 * encode_len() and decode_len() compute the amount of space needed, while
 * encode() and decode() perform the actual conversions.  It is okay for
 * the _len functions to return an overestimate, but not an underestimate.
 * (Having said that, large overestimates could cause unnecessary errors,
 * so it's better to get it right.)  The conversion routines write to the
 * buffer at *res and return the true length of their output.
 */
struct pg_encoding
{
	uint64		(*encode_len) (const char *data, size_t dlen);
	uint64		(*decode_len) (const char *data, size_t dlen);
	uint64		(*encode) (const char *data, size_t dlen, char *res);
	uint64		(*decode) (const char *data, size_t dlen, char *res);
};

static const struct pg_encoding *pg_find_encoding(const char *name);

/*
 * SQL functions.
 */

Datum
binary_encode(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_PP(0);
	Datum		name = PG_GETARG_DATUM(1);
	text	   *result;
	char	   *namebuf;
	char	   *dataptr;
	size_t		datalen;
	uint64		resultlen;
	uint64		res;
	const struct pg_encoding *enc;

	namebuf = TextDatumGetCString(name);

	enc = pg_find_encoding(namebuf);
	if (enc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized encoding: \"%s\"", namebuf)));

	dataptr = VARDATA_ANY(data);
	datalen = VARSIZE_ANY_EXHDR(data);

	resultlen = enc->encode_len(dataptr, datalen);

	/*
	 * resultlen possibly overflows uint32, therefore on 32-bit machines it's
	 * unsafe to rely on palloc's internal check.
	 */
	if (resultlen > MaxAllocSize - VARHDRSZ)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("result of encoding conversion is too large")));

	result = palloc(VARHDRSZ + resultlen);

	res = enc->encode(dataptr, datalen, VARDATA(result));

	/* Make this FATAL 'cause we've trodden on memory ... */
	if (res > resultlen)
		elog(FATAL, "overflow - encode estimate too small");

	SET_VARSIZE(result, VARHDRSZ + res);

	PG_RETURN_TEXT_P(result);
}

Datum
binary_decode(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_PP(0);
	Datum		name = PG_GETARG_DATUM(1);
	bytea	   *result;
	char	   *namebuf;
	char	   *dataptr;
	size_t		datalen;
	uint64		resultlen;
	uint64		res;
	const struct pg_encoding *enc;

	namebuf = TextDatumGetCString(name);

	enc = pg_find_encoding(namebuf);
	if (enc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized encoding: \"%s\"", namebuf)));

	dataptr = VARDATA_ANY(data);
	datalen = VARSIZE_ANY_EXHDR(data);

	resultlen = enc->decode_len(dataptr, datalen);

	/*
	 * resultlen possibly overflows uint32, therefore on 32-bit machines it's
	 * unsafe to rely on palloc's internal check.
	 */
	if (resultlen > MaxAllocSize - VARHDRSZ)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("result of decoding conversion is too large")));

	result = palloc(VARHDRSZ + resultlen);

	res = enc->decode(dataptr, datalen, VARDATA(result));

	/* Make this FATAL 'cause we've trodden on memory ... */
	if (res > resultlen)
		elog(FATAL, "overflow - decode estimate too small");

	SET_VARSIZE(result, VARHDRSZ + res);

	PG_RETURN_BYTEA_P(result);
}


/*
 * HEX
 */

/*
 * The hex expansion of each possible byte value (two chars per value).
 */
static const char hextbl[512] =
"000102030405060708090a0b0c0d0e0f"
"101112131415161718191a1b1c1d1e1f"
"202122232425262728292a2b2c2d2e2f"
"303132333435363738393a3b3c3d3e3f"
"404142434445464748494a4b4c4d4e4f"
"505152535455565758595a5b5c5d5e5f"
"606162636465666768696a6b6c6d6e6f"
"707172737475767778797a7b7c7d7e7f"
"808182838485868788898a8b8c8d8e8f"
"909192939495969798999a9b9c9d9e9f"
"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
"b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
"c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
"d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
"e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static inline uint64
hex_encode_scalar(const char *src, size_t len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		unsigned char usrc = *((const unsigned char *) src);

		memcpy(dst, &hextbl[2 * usrc], 2);
		src++;
		dst += 2;
	}
	return (uint64) len * 2;
}

uint64
hex_encode(const char *src, size_t len, char *dst)
{
#ifdef USE_NO_SIMD
	return hex_encode_scalar(src, len, dst);
#else
	const uint64 tail_idx = len & ~(sizeof(Vector8) - 1);
	uint64		i;

	/*
	 * This splits the high and low nibbles of each byte into separate
	 * vectors, adds the vectors to a mask that converts the nibbles to their
	 * equivalent ASCII bytes, and interleaves those bytes back together to
	 * form the final hex-encoded string.
	 */
	for (i = 0; i < tail_idx; i += sizeof(Vector8))
	{
		Vector8		srcv;
		Vector8		lo;
		Vector8		hi;
		Vector8		mask;

		vector8_load(&srcv, (const uint8 *) &src[i]);

		lo = vector8_and(srcv, vector8_broadcast(0x0f));
		mask = vector8_gt(lo, vector8_broadcast(0x9));
		mask = vector8_and(mask, vector8_broadcast('a' - '0' - 10));
		mask = vector8_add(mask, vector8_broadcast('0'));
		lo = vector8_add(lo, mask);

		hi = vector8_and(srcv, vector8_broadcast(0xf0));
		hi = vector8_shift_right(hi, 4);
		mask = vector8_gt(hi, vector8_broadcast(0x9));
		mask = vector8_and(mask, vector8_broadcast('a' - '0' - 10));
		mask = vector8_add(mask, vector8_broadcast('0'));
		hi = vector8_add(hi, mask);

		vector8_store((uint8 *) &dst[i * 2],
					  vector8_interleave_low(hi, lo));
		vector8_store((uint8 *) &dst[i * 2 + sizeof(Vector8)],
					  vector8_interleave_high(hi, lo));
	}

	(void) hex_encode_scalar(src + i, len - i, dst + i * 2);

	return (uint64) len * 2;
#endif
}

static inline bool
get_hex(const char *cp, char *out)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = hexlookup[c];

	*out = (char) res;

	return (res >= 0);
}

uint64
hex_decode(const char *src, size_t len, char *dst)
{
	return hex_decode_safe(src, len, dst, NULL);
}

static inline uint64
hex_decode_safe_scalar(const char *src, size_t len, char *dst, Node *escontext)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		if (!get_hex(s, &v1))
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid hexadecimal digit: \"%.*s\"",
							pg_mblen(s), s)));
		s++;
		if (s >= srcend)
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid hexadecimal data: odd number of digits")));
		if (!get_hex(s, &v2))
			ereturn(escontext, 0,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid hexadecimal digit: \"%.*s\"",
							pg_mblen(s), s)));
		s++;
		*p++ = (v1 << 4) | v2;
	}

	return p - dst;
}

/*
 * This helper converts each byte to its binary-equivalent nibble by
 * subtraction and combines them to form the return bytes (separated by zero
 * bytes).  Returns false if any input bytes are outside the expected ranges of
 * ASCII values.  Otherwise, returns true.
 */
#ifndef USE_NO_SIMD
static inline bool
hex_decode_simd_helper(const Vector8 src, Vector8 *dst)
{
	Vector8		sub;
	Vector8		mask_hi = vector8_interleave_low(vector8_broadcast(0), vector8_broadcast(0x0f));
	Vector8		mask_lo = vector8_interleave_low(vector8_broadcast(0x0f), vector8_broadcast(0));
	Vector8		tmp;
	bool		ret;

	tmp = vector8_gt(vector8_broadcast('9' + 1), src);
	sub = vector8_and(tmp, vector8_broadcast('0'));

	tmp = vector8_gt(src, vector8_broadcast('A' - 1));
	tmp = vector8_and(tmp, vector8_broadcast('A' - 10));
	sub = vector8_add(sub, tmp);

	tmp = vector8_gt(src, vector8_broadcast('a' - 1));
	tmp = vector8_and(tmp, vector8_broadcast('a' - 'A'));
	sub = vector8_add(sub, tmp);

	*dst = vector8_issub(src, sub);
	ret = !vector8_has_ge(*dst, 0x10);

	tmp = vector8_and(*dst, mask_hi);
	tmp = vector8_shift_right(tmp, 8);
	*dst = vector8_and(*dst, mask_lo);
	*dst = vector8_shift_left(*dst, 4);
	*dst = vector8_or(*dst, tmp);
	return ret;
}
#endif							/* ! USE_NO_SIMD */

uint64
hex_decode_safe(const char *src, size_t len, char *dst, Node *escontext)
{
#ifdef USE_NO_SIMD
	return hex_decode_safe_scalar(src, len, dst, escontext);
#else
	const uint64 tail_idx = len & ~(sizeof(Vector8) * 2 - 1);
	uint64		i;
	bool		success = true;

	/*
	 * We must process 2 vectors at a time since the output will be half the
	 * length of the input.
	 */
	for (i = 0; i < tail_idx; i += sizeof(Vector8) * 2)
	{
		Vector8		srcv;
		Vector8		dstv1;
		Vector8		dstv2;

		vector8_load(&srcv, (const uint8 *) &src[i]);
		success &= hex_decode_simd_helper(srcv, &dstv1);

		vector8_load(&srcv, (const uint8 *) &src[i + sizeof(Vector8)]);
		success &= hex_decode_simd_helper(srcv, &dstv2);

		vector8_store((uint8 *) &dst[i / 2], vector8_pack_16(dstv1, dstv2));
	}

	/*
	 * If something didn't look right in the vector path, try again in the
	 * scalar path so that we can handle it correctly.
	 */
	if (!success)
		i = 0;

	return i / 2 + hex_decode_safe_scalar(src + i, len - i, dst + i / 2, escontext);
#endif
}

static uint64
hex_enc_len(const char *src, size_t srclen)
{
	return (uint64) srclen << 1;
}

static uint64
hex_dec_len(const char *src, size_t srclen)
{
	return (uint64) srclen >> 1;
}

/*
 * BASE64 and BASE64URL
 */

static const char _base64[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const char _base64url[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static const int8 b64lookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
	-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
	-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

/*
 * pg_base64_encode_internal
 *
 * Helper for decoding base64 or base64url.  When url is passed as true the
 * input will be encoded using base64url.  len bytes in src is encoded into
 * dst.
 */
static uint64
pg_base64_encode_internal(const char *src, size_t len, char *dst, bool url)
{
	char	   *p,
			   *lend = dst + 76;
	const char *s,
			   *end = src + len;
	int			pos = 2;
	uint32		buf = 0;
	const char *alphabet = url ? _base64url : _base64;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= (unsigned char) *s << (pos << 3);
		pos--;
		s++;

		/* write it out */
		if (pos < 0)
		{
			*p++ = alphabet[(buf >> 18) & 0x3f];
			*p++ = alphabet[(buf >> 12) & 0x3f];
			*p++ = alphabet[(buf >> 6) & 0x3f];
			*p++ = alphabet[buf & 0x3f];

			pos = 2;
			buf = 0;

			if (!url && p >= lend)
			{
				*p++ = '\n';
				lend = p + 76;
			}
		}
	}

	/* Handle remaining bytes in buf */
	if (pos != 2)
	{
		*p++ = alphabet[(buf >> 18) & 0x3f];
		*p++ = alphabet[(buf >> 12) & 0x3f];

		if (pos == 0)
		{
			*p++ = alphabet[(buf >> 6) & 0x3f];
			if (!url)
				*p++ = '=';
		}
		else if (!url)
		{
			*p++ = '=';
			*p++ = '=';
		}
	}

	return p - dst;
}

static uint64
pg_base64_encode(const char *src, size_t len, char *dst)
{
	return pg_base64_encode_internal(src, len, dst, false);
}

static uint64
pg_base64url_encode(const char *src, size_t len, char *dst)
{
	return pg_base64_encode_internal(src, len, dst, true);
}

/*
 * pg_base64_decode_internal
 *
 * Helper for decoding base64 or base64url. When url is passed as true the
 * input will be assumed to be encoded using base64url.
 */
static uint64
pg_base64_decode_internal(const char *src, size_t len, char *dst, bool url)
{
	const char *srcend = src + len,
			   *s = src;
	char	   *p = dst;
	char		c;
	int			b = 0;
	uint32		buf = 0;
	int			pos = 0,
				end = 0;

	while (s < srcend)
	{
		c = *s++;

		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			continue;

		/* convert base64url to base64 */
		if (url)
		{
			if (c == '-')
				c = '+';
			else if (c == '_')
				c = '/';
		}

		if (c == '=')
		{
			/* end sequence */
			if (!end)
			{
				if (pos == 2)
					end = 1;
				else if (pos == 3)
					end = 2;
				else
				{
					/* translator: %s is the name of an encoding scheme */
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unexpected \"=\" while decoding %s sequence", url ? "base64url" : "base64")));
				}
			}
			b = 0;
		}
		else
		{
			b = -1;
			if (c > 0 && c < 127)
				b = b64lookup[(unsigned char) c];
			if (b < 0)
			{
				/* translator: %s is the name of an encoding scheme */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid symbol \"%.*s\" found while decoding %s sequence",
								pg_mblen(s - 1), s - 1,
								url ? "base64url" : "base64")));
			}
		}
		/* add it to buffer */
		buf = (buf << 6) + b;
		pos++;
		if (pos == 4)
		{
			*p++ = (buf >> 16) & 255;
			if (end == 0 || end > 1)
				*p++ = (buf >> 8) & 255;
			if (end == 0 || end > 2)
				*p++ = buf & 255;
			buf = 0;
			pos = 0;
		}
	}

	if (pos == 2)
	{
		buf <<= 12;
		*p++ = (buf >> 16) & 0xFF;
	}
	else if (pos == 3)
	{
		buf <<= 6;
		*p++ = (buf >> 16) & 0xFF;
		*p++ = (buf >> 8) & 0xFF;
	}
	else if (pos != 0)
	{
		/* translator: %s is the name of an encoding scheme */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid %s end sequence", url ? "base64url" : "base64"),
				 errhint("Input data is missing padding, is truncated, or is otherwise corrupted.")));
	}

	return p - dst;
}

static uint64
pg_base64_decode(const char *src, size_t len, char *dst)
{
	return pg_base64_decode_internal(src, len, dst, false);
}

static uint64
pg_base64url_decode(const char *src, size_t len, char *dst)
{
	return pg_base64_decode_internal(src, len, dst, true);
}

static uint64
pg_base64_enc_len(const char *src, size_t srclen)
{
	/* 3 bytes will be converted to 4, linefeed after 76 chars */
	return ((uint64) srclen + 2) / 3 * 4 + (uint64) srclen / (76 * 3 / 4);
}

static uint64
pg_base64_dec_len(const char *src, size_t srclen)
{
	return ((uint64) srclen * 3) >> 2;
}

static uint64
pg_base64url_enc_len(const char *src, size_t srclen)
{
	/*
	 * Unlike standard base64, base64url doesn't use padding characters when
	 * the input length is not divisible by 3
	 */
	return (srclen + 2) / 3 * 4;
}

static uint64
pg_base64url_dec_len(const char *src, size_t srclen)
{
	/*
	 * For base64, each 4 characters of input produce at most 3 bytes of
	 * output.  For base64url without padding, we need to round up to the
	 * nearest 4
	 */
	size_t		adjusted_len = srclen;

	if (srclen % 4 != 0)
		adjusted_len += 4 - (srclen % 4);

	return (adjusted_len * 3) / 4;
}

/*
 * Escape
 * Minimally escape bytea to text.
 * De-escape text to bytea.
 *
 * We must escape zero bytes and high-bit-set bytes to avoid generating
 * text that might be invalid in the current encoding, or that might
 * change to something else if passed through an encoding conversion
 * (leading to failing to de-escape to the original bytea value).
 * Also of course backslash itself has to be escaped.
 *
 * De-escaping processes \\ and any \### octal
 */

#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

static uint64
esc_encode(const char *src, size_t srclen, char *dst)
{
	const char *end = src + srclen;
	char	   *rp = dst;
	uint64		len = 0;

	while (src < end)
	{
		unsigned char c = (unsigned char) *src;

		if (c == '\0' || IS_HIGHBIT_SET(c))
		{
			rp[0] = '\\';
			rp[1] = DIG(c >> 6);
			rp[2] = DIG((c >> 3) & 7);
			rp[3] = DIG(c & 7);
			rp += 4;
			len += 4;
		}
		else if (c == '\\')
		{
			rp[0] = '\\';
			rp[1] = '\\';
			rp += 2;
			len += 2;
		}
		else
		{
			*rp++ = c;
			len++;
		}

		src++;
	}

	return len;
}

static uint64
esc_decode(const char *src, size_t srclen, char *dst)
{
	const char *end = src + srclen;
	char	   *rp = dst;
	uint64		len = 0;

	while (src < end)
	{
		if (src[0] != '\\')
			*rp++ = *src++;
		else if (src + 3 < end &&
				 (src[1] >= '0' && src[1] <= '3') &&
				 (src[2] >= '0' && src[2] <= '7') &&
				 (src[3] >= '0' && src[3] <= '7'))
		{
			int			val;

			val = VAL(src[1]);
			val <<= 3;
			val += VAL(src[2]);
			val <<= 3;
			*rp++ = val + VAL(src[3]);
			src += 4;
		}
		else if (src + 1 < end &&
				 (src[1] == '\\'))
		{
			*rp++ = '\\';
			src += 2;
		}
		else
		{
			/*
			 * One backslash, not followed by ### valid octal. Should never
			 * get here, since esc_dec_len does same check.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}

		len++;
	}

	return len;
}

static uint64
esc_enc_len(const char *src, size_t srclen)
{
	const char *end = src + srclen;
	uint64		len = 0;

	while (src < end)
	{
		if (*src == '\0' || IS_HIGHBIT_SET(*src))
			len += 4;
		else if (*src == '\\')
			len += 2;
		else
			len++;

		src++;
	}

	return len;
}

static uint64
esc_dec_len(const char *src, size_t srclen)
{
	const char *end = src + srclen;
	uint64		len = 0;

	while (src < end)
	{
		if (src[0] != '\\')
			src++;
		else if (src + 3 < end &&
				 (src[1] >= '0' && src[1] <= '3') &&
				 (src[2] >= '0' && src[2] <= '7') &&
				 (src[3] >= '0' && src[3] <= '7'))
		{
			/*
			 * backslash + valid octal
			 */
			src += 4;
		}
		else if (src + 1 < end &&
				 (src[1] == '\\'))
		{
			/*
			 * two backslashes = backslash
			 */
			src += 2;
		}
		else
		{
			/*
			 * one backslash, not followed by ### valid octal
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}

		len++;
	}
	return len;
}

/*
 * Common
 */

static const struct
{
	const char *name;
	struct pg_encoding enc;
}			enclist[] =

{
	{
		"hex",
		{
			hex_enc_len, hex_dec_len, hex_encode, hex_decode
		}
	},
	{
		"base64",
		{
			pg_base64_enc_len, pg_base64_dec_len, pg_base64_encode, pg_base64_decode
		}
	},
	{
		"base64url",
		{
			pg_base64url_enc_len, pg_base64url_dec_len, pg_base64url_encode, pg_base64url_decode
		}
	},
	{
		"escape",
		{
			esc_enc_len, esc_dec_len, esc_encode, esc_decode
		}
	},
	{
		NULL,
		{
			NULL, NULL, NULL, NULL
		}
	}
};

static const struct pg_encoding *
pg_find_encoding(const char *name)
{
	int			i;

	for (i = 0; enclist[i].name; i++)
		if (pg_strcasecmp(enclist[i].name, name) == 0)
			return &enclist[i].enc;

	return NULL;
}
