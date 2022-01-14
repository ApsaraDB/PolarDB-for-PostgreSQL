/*-------------------------------------------------------------------------
 *
 * memtup_px.h
 *	  In Memory Tuple format
 *
 * Portions Copyright (c) 2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/memtup_px.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMTUP_H
#define MEMTUP_H

#include "access/tupdesc.h"

typedef enum MemTupleBindFlag
{
	MTB_ByVal_Native 	= 1,	/* Fixed len, native (returned as datum ) */
	MTB_ByVal_Ptr    	= 2,	/* Fixed len, convert to pointer for datum */
	MTB_ByRef			= 3,	/* var len */
	MTB_ByRef_CStr   	= 4,   /* varlen, CString type */
} MemTupleBindFlag;

typedef struct MemTupleAttrBinding
{
	int offset; 		/* offset of attr in memtuple */
	int len;				/* attribute length */
	int len_aligned;		/* attribute length, padded for aligning the physically following attribute */
	MemTupleBindFlag flag;	/* binding flag */
	int null_byte;		/* which byte holds the null flag for the attr */
	unsigned char null_mask;		/* null bit mask */
} MemTupleAttrBinding;

typedef struct MemTupleBindingCols
{
	uint32 var_start; 	/* varlen fields start */
	MemTupleAttrBinding *bindings; /* bindings for attrs (cols) */
	short *null_saves;				/* saved space from each attribute when null */
	short *null_saves_aligned;		/* saved space from each attribute when null - uses aligned length */
	bool has_null_saves_alignment_mismatch;		/* true if one or more attributes has mismatching alignment and length  */
	bool has_dropped_attr_alignment_mismatch;	/* true if one or more dropped attributes has mismatching alignment and length */
} MemTupleBindingCols;

typedef struct MemTupleBinding
{
	TupleDesc tupdesc;
	int column_align;
	int null_bitmap_extra_size;  /* extra bytes required by null bitmap */

	MemTupleBindingCols bind;  	/* 2 bytes offsets */
	MemTupleBindingCols large_bind; /* large tup, 4 bytes offsets */
} MemTupleBinding;

typedef struct MemTupleData
{
	uint32 PRIVATE_mt_len;
	unsigned char PRIVATE_mt_bits[1]; 	/* varlen */
} MemTupleData;

typedef MemTupleData *MemTuple;

#define MEMTUP_LEAD_BIT 0x80000000
#define MEMTUP_LEN_MASK 0x3FFFFFF8
#define MEMTUP_HASNULL   1
#define MEMTUP_LARGETUP  2
#define MEMTUP_HASEXTERNAL 	 4

#define MEMTUP_ALIGN(LEN) TYPEALIGN(8, (LEN)) 
#define MEMTUPLE_LEN_FITSHORT 0xFFF0

static inline bool mtbind_has_oid(MemTupleBinding *pbind)
{
	return pbind->tupdesc->tdhasoid;
}

static inline bool memtuple_lead_bit_set(MemTuple tup)
{
	return (tup->PRIVATE_mt_len & MEMTUP_LEAD_BIT) != 0;
}
static inline uint32 memtuple_size_from_uint32(uint32 len)
{
	Assert ((len & MEMTUP_LEAD_BIT) != 0);
	return len & MEMTUP_LEN_MASK;
}
static inline uint32 memtuple_get_size(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_LEN_MASK);
}
static inline void memtuple_set_mtlen(MemTuple mtup, uint32 mtlen)
{
	Assert((mtlen & MEMTUP_LEAD_BIT) != 0);
	mtup->PRIVATE_mt_len = mtlen;
}
static inline bool memtuple_get_hasnull(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_HASNULL) != 0;
}
static inline void memtuple_set_hasnull(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_HASNULL;
}
static inline bool memtuple_get_islarge(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_LARGETUP) != 0;
}
static inline void memtuple_set_islarge(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_LARGETUP; 
}
static inline bool memtuple_get_hasext(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_HASEXTERNAL) != 0;
}
static inline void memtuple_set_hasext(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_HASEXTERNAL;
}

extern void destroy_memtuple_binding(MemTupleBinding *pbind);
extern MemTupleBinding* create_memtuple_binding(TupleDesc tupdesc);

extern Datum memtuple_getattr(MemTuple mtup, MemTupleBinding *pbind, int attnum, bool *isnull);
extern bool memtuple_attisnull(MemTuple mtup, MemTupleBinding *pbind, int attnum);

extern MemTuple mem_copytuple(MemTuple mtup);

extern uint32 compute_memtuple_size(MemTupleBinding *pbind, Datum *values, bool *isnull, bool hasnull, uint32 *nullsaves);

extern MemTuple memtuple_form_to(MemTupleBinding *pbind, Datum *values, bool *isnull, MemTuple dest, uint32 *destlen, bool inline_toast);
extern void memtuple_deform(MemTuple mtup, MemTupleBinding *pbind, Datum *datum, bool *isnull);

extern void MemTupleSetOid(MemTuple mtup, MemTupleBinding *pbind, Oid oid);

extern Oid MemTupleGetOid(MemTuple mtup, MemTupleBinding *pbind);

#endif /* MEMTUP_H */
