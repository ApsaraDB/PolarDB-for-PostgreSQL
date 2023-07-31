/*-------------------------------------------------------------------------
 *
 * polar_tools.h
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *	  src/bin/polar_tools/polar_tools.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_TOOLS_H
#define POLAR_TOOLS_H

#include "postgres.h"

#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "polar_flashback/polar_flashback_log_internal.h"

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

/* Rotate a uint32 value left by k bits - note multiple evaluation! */
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

extern uint32 hash_any(register const unsigned char *k, register int keylen);
extern uint64 hash_any_extended(register const unsigned char *k, register int keylen, uint64 seed);
extern int block_header_dump_main(int argc, char **argv);
extern int control_data_change_main(int argc, char **argv);
extern int logindex_meta_main(int argc, char **argv);
extern int logindex_bloom_main(int argc, char **argv);
extern int logindex_table_main(int argc, char **argv);
extern int logindex_page_main(int argc, char **argv);
extern int dma_meta_main(int argc, char **argv);
extern int dma_log_main(int argc, char **argv);
extern int datamax_meta_main(int argc, char **argv);
extern int datamax_get_wal_main(int argc, char **argv);
extern int flashback_log_control_dump_main(int argc, char **argv);
extern int flashback_log_file_dump_main(int argc, char **argv);
extern int flashback_point_file_dump_main(int argc, char **argv);
extern int fra_control_dump_main(int argc, char **argv);
extern int flashback_snapshot_dump_main(int argc, char **argv);
extern void flashback_snapshot_dump(const char *dir, fbpoint_pos_t snapshot_pos);

#endif
