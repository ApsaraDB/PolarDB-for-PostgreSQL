//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 VMware, Inc. or its affiliates.
//
//	@filename:
//		CWorkerPoolManager.cpp
//
//	@doc:
//		Central scheduler;
//		* maintains global worker-local-storage
//		* keeps track of all worker pools
//---------------------------------------------------------------------------

#include "gpos/memory/CMemoryPoolManager.h"

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTraceFlag.h"


using namespace gpos;
using namespace gpos::clib;


// global instance of memory pool manager
CMemoryPoolManager *CMemoryPoolManager::m_memory_pool_mgr = nullptr;

// ctor
CMemoryPoolManager::CMemoryPoolManager(CMemoryPool *internal,
									   EMemoryPoolType memory_pool_type)
	: m_internal_memory_pool(internal),
	  m_ht_all_pools(nullptr),
	  m_memory_pool_type(memory_pool_type)
{
	GPOS_ASSERT(nullptr != internal);
	GPOS_ASSERT(GPOS_OFFSET(CMemoryPool, m_link) ==
				GPOS_OFFSET(CMemoryPoolTracker, m_link));
}

// Set up CMemoryPoolManager's internals.
// This must be done here instead of the constructor because CMemoryPoolManager
// uses virtual methods for determining the correct CMemoryPool for
// allocations; and these virtual methods must not be called in the
// constructor, or else it will use the base class allocator instead of the one
// defined in derived-class virtual function.
void
CMemoryPoolManager::Setup()
{
	m_ht_all_pools = GPOS_NEW(m_internal_memory_pool)
		CSyncHashtable<CMemoryPool, ULONG_PTR>();
	m_ht_all_pools->Init(
		m_internal_memory_pool, GPOS_MEMORY_POOL_HT_SIZE,
		GPOS_OFFSET(CMemoryPool, m_link), GPOS_OFFSET(CMemoryPool, m_hash_key),
		&(CMemoryPool::m_invalid), HashULongPtr, EqualULongPtr);

	// create pool used in allocations made using global new operator
	m_global_memory_pool = CreateMemoryPool();
}

// Initialize global memory pool manager using CMemoryPoolTracker
GPOS_RESULT
CMemoryPoolManager::Init()
{
	if (nullptr == CMemoryPoolManager::m_memory_pool_mgr)
	{
		return SetupGlobalMemoryPoolManager<CMemoryPoolManager,
											CMemoryPoolTracker>();
	}

	return GPOS_OK;
}


CMemoryPool *
CMemoryPoolManager::CreateMemoryPool()
{
	CMemoryPool *mp = NewMemoryPool();

	// accessor scope
	{
		// HERE BE DRAGONS
		// See comment in CCache::InsertEntry
		const ULONG_PTR hashKey = mp->GetHashKey();
		MemoryPoolKeyAccessor acc(*m_ht_all_pools, hashKey);
		acc.Insert(mp);
	}

	return mp;
}


// Allocate a new NewMemoryPool
CMemoryPool *
CMemoryPoolManager::NewMemoryPool()
{
	return GPOS_NEW(m_internal_memory_pool) CMemoryPoolTracker();
}


// Release given memory pool
void
CMemoryPoolManager::Destroy(CMemoryPool *mp)
{
	GPOS_ASSERT(nullptr != mp);

	// accessor scope
	{
		// HERE BE DRAGONS
		// See comment in CCache::InsertEntry
		const ULONG_PTR hashKey = mp->GetHashKey();
		MemoryPoolKeyAccessor acc(*m_ht_all_pools, hashKey);
		acc.Remove(mp);
	}

	mp->TearDown();

	GPOS_DELETE(mp);
}


// Return total allocated size in bytes
ULLONG
CMemoryPoolManager::TotalAllocatedSize()
{
	ULLONG total_size = 0;
	MemoryPoolIter iter(*m_ht_all_pools);
	while (iter.Advance())
	{
		MemoryPoolIterAccessor acc(iter);
		CMemoryPool *mp = acc.Value();
		if (nullptr != mp)
		{
			total_size = total_size + mp->TotalAllocatedSize();
		}
	}

	return total_size;
}

// free memory allocation
void
CMemoryPoolManager::DeleteImpl(void *ptr, CMemoryPool::EAllocationType eat)
{
	CMemoryPoolTracker::DeleteImpl(ptr, eat);
}

// get user requested size of allocation
ULONG
CMemoryPoolManager::UserSizeOfAlloc(const void *ptr)
{
	return CMemoryPoolTracker::UserSizeOfAlloc(ptr);
}

#ifdef GPOS_DEBUG


// Print contents of all allocated memory pools
IOstream &
CMemoryPoolManager::OsPrint(IOstream &os)
{
	os << "Print memory pools: " << std::endl;

	MemoryPoolIter iter(*m_ht_all_pools);
	while (iter.Advance())
	{
		CMemoryPool *mp = nullptr;
		{
			MemoryPoolIterAccessor acc(iter);
			mp = acc.Value();
		}

		if (nullptr != mp)
		{
			os << *mp << std::endl;
		}
	}

	return os;
}


// Print memory pools with total allocated size above given threshold
void
CMemoryPoolManager::PrintOverSizedPools(
	CMemoryPool *trace,
	ULLONG size_threshold  // size threshold in bytes
)
{
	MemoryPoolIter iter(*m_ht_all_pools);
	while (iter.Advance())
	{
		MemoryPoolIterAccessor acc(iter);
		CMemoryPool *mp = acc.Value();

		if (nullptr != mp)
		{
			ULLONG size = mp->TotalAllocatedSize();
			if (size > size_threshold)
			{
				CAutoTrace at(trace);
				at.Os() << std::endl
						<< "OVERSIZED MEMORY POOL: " << size << " bytes "
						<< std::endl;
			}
		}
	}
}
#endif	// GPOS_DEBUG


// Destroy a memory pool at shutdown
void
CMemoryPoolManager::DestroyMemoryPoolAtShutdown(CMemoryPool *mp)
{
	GPOS_ASSERT(nullptr != mp);

#ifdef GPOS_DEBUG
	gpos::oswcerr << "Leaked " << *mp << std::endl;
#endif	// GPOS_DEBUG

	mp->TearDown();
	GPOS_DELETE(mp);
}


// Clean-up memory pools
// Counterpart to CMemoryPoolManager::Setup()
void
CMemoryPoolManager::Cleanup()
{
#ifdef GPOS_DEBUG
	if (0 < m_global_memory_pool->TotalAllocatedSize())
	{
		// allocations made by calling global new operator are not deleted
		gpos::oswcerr << "Memory leaks detected" << std::endl
					  << *m_global_memory_pool << std::endl;
	}
#endif	// GPOS_DEBUG

	GPOS_ASSERT(nullptr != m_global_memory_pool);
	Destroy(m_global_memory_pool);

	// cleanup left-over memory pools;
	// any such pool means that we have a leak
	m_ht_all_pools->DestroyEntries(DestroyMemoryPoolAtShutdown);
	GPOS_DELETE(m_ht_all_pools);
}


// Delete memory pools and release manager
void
CMemoryPoolManager::Shutdown()
{
	// cleanup remaining memory pools
	Cleanup();

	// save off pointers for explicit deletion
	CMemoryPool *internal = m_internal_memory_pool;

	::delete CMemoryPoolManager::m_memory_pool_mgr;
	CMemoryPoolManager::m_memory_pool_mgr = nullptr;

#ifdef GPOS_DEBUG
	internal->AssertEmpty(oswcerr);
#endif	// GPOS_DEBUG

	Free(internal);
}

// EOF
