//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryVisitorPrint.cpp
//
//	@doc:
//		Implementation of memory object visitor for printing debug information
//		for all allocated objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/memory/CMemoryVisitorPrint.h"

#include "gpos/assert.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/ITask.h"
#include "gpos/types.h"
#include "gpos/utils.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::CMemoryVisitorPrint
//
//	@doc:
//	  Ctor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::CMemoryVisitorPrint(IOstream &os) : m_visits(0), m_os(os)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::~CMemoryVisitorPrint
//
//	@doc:
//	  Dtor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::~CMemoryVisitorPrint() = default;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::FVisit
//
//	@doc:
//		Prints the live object information to the output stream.
//
//---------------------------------------------------------------------------
void
CMemoryVisitorPrint::Visit(void *user_addr, SIZE_T user_size, void *total_addr,
						   SIZE_T total_size, const CHAR *alloc_filename,
						   const ULONG alloc_line, ULLONG alloc_seq_number,
						   CStackDescriptor *stack_desc)
{
	m_os << COstream::EsmDec << "allocation sequence number "
		 << alloc_seq_number << ","
		 << " total size " << (ULONG) total_size << " bytes,"
		 << " base address " << total_addr << ","
		 << " user size " << (ULONG) user_size << " bytes,"
		 << " user address " << user_addr << ","
		 << " allocated by " << alloc_filename << ":" << alloc_line
		 << std::endl;

	ITask *task = ITask::Self();
	if (nullptr != task)
	{
		if (nullptr != stack_desc &&
			task->IsTraceSet(EtracePrintMemoryLeakStackTrace))
		{
			m_os << "Stack trace: " << std::endl;
			stack_desc->AppendTrace(m_os, 8 /*ulDepth*/);
		}

		if (task->IsTraceSet(EtracePrintMemoryLeakDump))
		{
			m_os << "Memory dump: " << std::endl;
			gpos::HexDump(m_os, total_addr, total_size);
		}
	}

	++m_visits;
}


// EOF
