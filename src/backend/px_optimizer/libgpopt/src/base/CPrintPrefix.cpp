//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPrintPrefix.cpp
//
//	@doc:
//		Implementation of print prefix class
//---------------------------------------------------------------------------

#include "gpopt/base/CPrintPrefix.h"

#include "gpos/base.h"
#include "gpos/task/IWorker.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPrintPrefix::CPrintPrefix
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPrintPrefix::CPrintPrefix(const CPrintPrefix *ppfx, const CHAR *sz)
	: m_ppfx(ppfx), m_sz(sz)
{
	GPOS_ASSERT(nullptr != sz);
}


//---------------------------------------------------------------------------
//	@function:
//		CPrintPrefix::OsPrint
//
//	@doc:
//		print function;
//		recursively traverse the linked list of prefixes and print them
//		in reverse order
//
//---------------------------------------------------------------------------
IOstream &
CPrintPrefix::OsPrint(IOstream &os) const
{
	GPOS_CHECK_STACK_SIZE;

	if (nullptr != m_ppfx)
	{
		(void) m_ppfx->OsPrint(os);
	}

	os << m_sz;
	return os;
}


// EOF
