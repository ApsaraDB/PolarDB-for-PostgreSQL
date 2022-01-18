//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumper.cpp
//
//	@doc:
//		Partial implementation of interface for minidump handler
//---------------------------------------------------------------------------

#include "gpos/error/CMiniDumper.h"

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::CMiniDumper
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumper::CMiniDumper() = default;


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::~CMiniDumper
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMiniDumper::~CMiniDumper()
{
	if (m_initialized)
	{
		CTask *task = CTask::Self();

		GPOS_ASSERT(nullptr != task);

		task->ConvertErrCtxt()->Unregister(
#ifdef GPOS_DEBUG
			this
#endif	// GPOS_DEBUG
		);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Init
//
//	@doc:
//		Initialize
//
//---------------------------------------------------------------------------
void
CMiniDumper::Init(COstream *oos)
{
	GPOS_ASSERT(!m_initialized);
	GPOS_ASSERT(!m_finalized);

	CTask *task = CTask::Self();

	GPOS_ASSERT(nullptr != task);

	m_oos = oos;

	task->ConvertErrCtxt()->Register(this);

	m_initialized = true;

	SerializeHeader();
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::Finalize
//
//	@doc:
//		Finalize
//
//---------------------------------------------------------------------------
void
CMiniDumper::Finalize()
{
	GPOS_ASSERT(m_initialized);
	GPOS_ASSERT(!m_finalized);

	SerializeFooter();

	m_finalized = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumper::GetOStream
//
//	@doc:
//		Get stream to serialize to
//
//---------------------------------------------------------------------------
COstream &
CMiniDumper::GetOStream()
{
	GPOS_ASSERT(m_initialized);

	return *m_oos;
}


// EOF
