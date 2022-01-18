//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CErrorContext.cpp
//
//	@doc:
//		Implements context container for error handling
//---------------------------------------------------------------------------

#include "gpos/error/CErrorContext.h"

#include "gpos/error/CMessageRepository.h"
#include "gpos/error/CMiniDumper.h"
#include "gpos/error/CSerializable.h"
#include "gpos/io/ioutils.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/utils.h"

using namespace gpos;


// logger buffer must be large enough to store error messages
GPOS_CPL_ASSERT(GPOS_ERROR_MESSAGE_BUFFER_SIZE <= GPOS_LOG_ENTRY_BUFFER_SIZE);


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::CErrorContext
//
//	@doc:
//
//---------------------------------------------------------------------------
CErrorContext::CErrorContext(CMiniDumper *mini_dumper_handle)
	: m_exception(CException::m_invalid_exception),

	  m_static_buffer(m_error_msg, GPOS_ARRAY_SIZE(m_error_msg)),
	  m_mini_dumper_handle(mini_dumper_handle)
{
	m_serializable_objects_list.Init(
		GPOS_OFFSET(CSerializable, m_err_ctxt_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::~CErrorContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CErrorContext::~CErrorContext()
{
	GPOS_ASSERT(!m_pending && "unhandled error pending");
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Reset
//
//	@doc:
//		Clean up error context
//
//---------------------------------------------------------------------------
void
CErrorContext::Reset()
{
	GPOS_ASSERT(m_pending);

	m_pending = false;
	m_rethrown = false;
	m_serializing = false;
	m_exception = CException::m_invalid_exception;
	m_static_buffer.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Record
//
//	@doc:
//		Grab error context and save it off and format error message
//
//---------------------------------------------------------------------------
void
CErrorContext::Record(CException &exc, VA_LIST vl)
{
	if (m_serializing)
		return;

#ifdef GPOS_DEBUG
	if (m_pending)
	{
		// reset pending flag so we can throw from here
		m_pending = false;

		GPOS_ASSERT(!"Pending error unhandled when raising new error");
	}
#endif	// GPOS_DEBUG

	m_pending = true;
	m_exception = exc;

	// store stack, skipping current frame
	m_stack_descriptor.BackTrace(1);

	ELocale locale = ITask::Self()->Locale();
	CMessage *msg =
		CMessageRepository::GetMessageRepository()->LookupMessage(exc, locale);
	msg->Format(&m_static_buffer, vl);

	m_severity = msg->GetSeverity();

	if (GPOS_FTRACE(EtracePrintExceptionOnRaise))
	{
		std::wcerr << GPOS_WSZ_LIT("Exception: ") << m_static_buffer.GetBuffer()
				   << std::endl;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::AppendErrno
//
//	@doc:
//		Print errno
//
//---------------------------------------------------------------------------
void
CErrorContext::AppendErrnoMsg()
{
	GPOS_ASSERT(m_pending);
	GPOS_ASSERT(GPOS_MATCH_EX(m_exception, CException::ExmaSystem,
							  CException::ExmiIOError));
	GPOS_ASSERT(0 < errno && "Errno has not been set");

	// get errno description
	clib::Strerror_r(errno, m_system_error_msg,
					 GPOS_ARRAY_SIZE(m_system_error_msg));

	m_static_buffer.AppendFormat(GPOS_WSZ_LIT(" (%s)"), m_system_error_msg);
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::CopyPropErrCtxt
//
//	@doc:
//		Copy necessary info for error propagation
//
//---------------------------------------------------------------------------
void
CErrorContext::CopyPropErrCtxt(const IErrorContext *err_ctxt)
{
	GPOS_ASSERT(!m_pending);

	m_pending = true;

	// copy exception
	m_exception = err_ctxt->GetException();

	// copy error message
	m_static_buffer.Reset();
	m_static_buffer.Append(
		&(dynamic_cast<const CErrorContext *>(err_ctxt)->m_static_buffer));

	// copy severity
	m_severity = err_ctxt->GetSeverity();
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Serialize
//
//	@doc:
//		Serialize registered objects
//
//---------------------------------------------------------------------------
void
CErrorContext::Serialize()
{
	if (m_serializing)
		return;

	if (nullptr == m_mini_dumper_handle ||
		m_serializable_objects_list.IsEmpty())
	{
		return;
	}

	m_serializing = true;

	// Abort might throw an error, so prevent aborting to
	// avoid recursion.
	CAutoSuspendAbort asa;
	// get mini-dumper's stream to serialize to
	COstream &oos = m_mini_dumper_handle->GetOStream();

	// serialize objects to reserved space
	m_mini_dumper_handle->SerializeEntryHeader();

	for (CSerializable *serializable_obj = m_serializable_objects_list.First();
		 nullptr != serializable_obj;
		 serializable_obj = m_serializable_objects_list.Next(serializable_obj))
	{
		serializable_obj->Serialize(oos);
	}

	m_mini_dumper_handle->SerializeEntryFooter();

	m_serializing = false;
}


// EOF
