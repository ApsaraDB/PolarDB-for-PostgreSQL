//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IDatum.h
//
//	@doc:
//		Base class for datum representation inside optimizer
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatum_H
#define GPNAUCRATES_IDatum_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/DbgPrintMixin.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;

class IDatum;

// hash map mapping ULONG -> Datum
typedef CHashMap<ULONG, IDatum, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupRelease<IDatum> >
	UlongToIDatumMap;

//---------------------------------------------------------------------------
//	@class:
//		IDatum
//
//	@doc:
//		Base abstract class for datum representation inside optimizer
//
//---------------------------------------------------------------------------
class IDatum : public CRefCount, public DbgPrintMixin<IDatum>
{
private:
public:
	IDatum(const IDatum &) = delete;

	// ctor
	IDatum() = default;

	// dtor
	~IDatum() override = default;

	// accessor for datum type
	virtual IMDType::ETypeInfo GetDatumType() = 0;

	// accessor of metadata id
	virtual IMDId *MDId() const = 0;

	virtual INT
	TypeModifier() const
	{
		return default_type_modifier;
	}

	// accessor of size
	virtual ULONG Size() const = 0;

	// is datum null?
	virtual BOOL IsNull() const = 0;

	// return string representation
	virtual const CWStringConst *GetStrRepr(CMemoryPool *mp) const = 0;

	// hash function
	virtual ULONG HashValue() const = 0;

	// Match function on datums
	virtual BOOL Matches(const IDatum *) const = 0;

	// create a copy of the datum
	virtual IDatum *MakeCopy(CMemoryPool *mp) const = 0;

	// stats greater than
	virtual BOOL
	StatsAreGreaterThan(const IDatum *datum) const
	{
		BOOL stats_are_comparable = datum->StatsAreComparable(this);
		GPOS_ASSERT(stats_are_comparable &&
					"Invalid invocation of StatsAreGreaterThan");
		return stats_are_comparable && datum->StatsAreLessThan(this);
	}

	// does the datum need to be padded before statistical derivation
	virtual BOOL NeedsPadding() const = 0;

	// return the padded datum
	virtual IDatum *MakePaddedDatum(CMemoryPool *mp, ULONG col_len) const = 0;

	// does datum support like predicate
	virtual BOOL SupportsLikePredicate() const = 0;

	// return the default scale factor of like predicate
	virtual CDouble GetLikePredicateScaleFactor() const = 0;

	// byte array for char/varchar columns
	virtual const BYTE *GetByteArrayValue() const = 0;

	// is datum mappable to a base type for statistics purposes
	virtual BOOL
	StatsMappable()
	{
		return this->StatsAreComparable(this);
	}

	// can datum be mapped to a double
	virtual BOOL IsDatumMappableToDouble() const = 0;

	// map to double for statistics computation
	virtual CDouble GetDoubleMapping() const = 0;

	// can datum be mapped to LINT
	virtual BOOL IsDatumMappableToLINT() const = 0;

	// map to LINT for statistics computation
	virtual LINT GetLINTMapping() const = 0;

	// equality based on mapping to LINT or CDouble
	virtual BOOL StatsAreEqual(const IDatum *datum) const;

	// stats less than
	virtual BOOL StatsAreLessThan(const IDatum *datum) const;

	// distance function
	virtual CDouble GetStatsDistanceFrom(const IDatum *datum) const;

	// return double representation of mapping value
	CDouble GetValAsDouble() const;

	// check if the given pair of datums are stats comparable
	virtual BOOL StatsAreComparable(const IDatum *datum) const;

	virtual gpos::IOstream &OsPrint(gpos::IOstream &os) const = 0;
};	// class IDatum

// array of idatums
typedef CDynamicPtrArray<IDatum, CleanupRelease> IDatumArray;
}  // namespace gpnaucrates


#endif	// !GPNAUCRATES_IDatum_H

// EOF
