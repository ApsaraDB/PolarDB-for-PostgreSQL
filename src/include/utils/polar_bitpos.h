/*-------------------------------------------------------------------------
 *
 * polar_bitpos.h
 *   Macro to get bit pos from uin64
 *
 *
 * src/include/utils/polar_bitpos.h
 *
 * Copyright (c) 2019, Alibaba.inc
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_BITPOS_H
#define POLAR_BITPOS_H

/* Get least bit position which is set */
#ifdef __GNUC__
#define POLAR_BIT_LEAST_POS(v, p) ((p) = __builtin_ffsl(v))
#else
#define POLAR_BIT_LEAST_POS(val, p) \
	{\
		uint64_t v = (val); \
		(p) = 0; \
		if (((v) & 0xffffffff) == 0) { \
			(p) += 32; \
			(v) >>= 32; \
		} \
		if (((v) & 0xffff) == 0) { \
			(p) += 16; \
			(v) >>= 16; \
		} \
		if (((v) & 0xff) == 0) { \
			(p) += 8; \
			(v) >>= 8; \
		} \
		if (((v) & 0xf) == 0) { \
			(p) += 4; \
			(v) >>= 4; \
		} \
		if (((v) & 0x3) == 0) { \
			(p) += 2; \
			(v) >>= 2; \
		} \
		if (((v) & 0x1) == 0) { \
			(p) += 1; \
		} \
	}
#endif

#define POLAR_BIT_CLEAR_LEAST(x) ((x) &= ((x) - 1))

/* The occupied pos start from 1 */
#define POLAR_BIT_OCCUPY(x, i) ((x) |= (1L << (i-1)))
#define POLAR_BIT_IS_OCCUPIED(x, i) ((x) & (1L << (i-1)))
#define POLAR_BIT_RELEASE_OCCUPIED(x, i) ((x) &= (~(1L << (i-1))))

#endif
