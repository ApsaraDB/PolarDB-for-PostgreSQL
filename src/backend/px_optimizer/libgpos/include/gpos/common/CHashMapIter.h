//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHashMapIter.h
//
//	@doc:
//		Hash map iterator
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMapIter_H
#define GPOS_CHashMapIter_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CHashMapIter
//
//	@doc:
//		Hash map iterator
//
//---------------------------------------------------------------------------
template <class K, class T, ULONG (*HashFn)(const K *),
		  BOOL (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMapIter : public CStackObject
{
	// short hand for hashmap type
	typedef CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> TMap;

private:
	// map to iterate
	const TMap *m_map;

	// current hashchain
	ULONG m_chain_idx;

	// current key
	ULONG m_key_idx;

	// is initialized?
	BOOL m_is_initialized;

	// method to return the current element
	const typename TMap::CHashMapElem *
	Get() const
	{
		typename TMap::CHashMapElem *elem = nullptr;
		K *k = (*(m_map->m_keys))[m_key_idx - 1];
		elem = m_map->Lookup(k);

		return elem;
	}

public:
	CHashMapIter(const CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>
					 &) = delete;

	// ctor
	CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>(TMap *ptm)
		: m_map(ptm), m_chain_idx(0), m_key_idx(0)
	{
		GPOS_ASSERT(nullptr != ptm);
	}

	// dtor
	virtual ~CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>() =
		default;

	// advance iterator to next element
	BOOL
	Advance()
	{
		if (m_key_idx < m_map->m_keys->Size())
		{
			m_key_idx++;
			return true;
		}

		return false;
	}

	// current key
	const K *
	Key() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (nullptr != elem)
		{
			return elem->Key();
		}
		return nullptr;
	}

	// current value
	const T *
	Value() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (nullptr != elem)
		{
			return elem->Value();
		}
		return nullptr;
	}

};	// class CHashMapIter

}  // namespace gpos

#endif	// !GPOS_CHashMapIter_H

// EOF
