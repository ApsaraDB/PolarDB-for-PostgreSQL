/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/lib/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
								  uint64 seed);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
							  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
								size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);

extern bloom_filter *polar_bloom_init_struct(uint8 *bloom_buf, size_t bloom_buf_size,
											 int64 total_elems, uint64 seed);

#endif							/* BLOOMFILTER_H */
