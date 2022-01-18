//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates
//
//	Hash set iterator

#ifndef GPOS_CHashSetIter_H
#define GPOS_CHashSetIter_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashSet.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{
// Hash set iterator
template <class T, ULONG (*HashFn)(const T *),
		  BOOL (*EqFn)(const T *, const T *), void (*CleanupFn)(T *)>
class CHashSetIter : public CStackObject
{
	// short hand for hashset type
	typedef CHashSet<T, HashFn, EqFn, CleanupFn> TSet;

private:
	// set to iterate
	const TSet *m_set;

	// current hashchain
	ULONG m_chain_idx;

	// current element
	ULONG m_elem_idx;

	// is initialized?
	BOOL m_is_initialized;

public:
	CHashSetIter(const CHashSetIter<T, HashFn, EqFn, CleanupFn> &) = delete;

	// ctor
	CHashSetIter<T, HashFn, EqFn, CleanupFn>(TSet *set)
		: m_set(set), m_chain_idx(0), m_elem_idx(0)
	{
		GPOS_ASSERT(nullptr != set);
	}

	// dtor
	virtual ~CHashSetIter<T, HashFn, EqFn, CleanupFn>() = default;

	// advance iterator to next element
	BOOL
	Advance()
	{
		if (m_elem_idx < m_set->m_elements->Size())
		{
			m_elem_idx++;
			return true;
		}

		return false;
	}

	// current element
	const T *
	Get() const
	{
		const typename TSet::CHashSetElem *elem = nullptr;
		T *t = (*(m_set->m_elements))[m_elem_idx - 1];
		elem = m_set->Lookup(t);
		if (nullptr != elem)
		{
			return elem->Value();
		}
		return nullptr;
	}

};	// class CHashSetIter

}  // namespace gpos

#endif	// !GPOS_CHashSetIter_H

// EOF
