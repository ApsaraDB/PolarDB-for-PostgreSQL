//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerSyslog.cpp
//
//	@doc:
//		Implementation of Syslog logging
//---------------------------------------------------------------------------

#include "gpos/error/CLoggerSyslog.h"

#include <syslog.h>

#include "gpos/common/syslibwrapper.h"
#include "gpos/string/CStringStatic.h"

using namespace gpos;

// initialization of static members
CLoggerSyslog CLoggerSyslog::m_alert_logger(nullptr /*szName*/,
											LOG_PERROR | LOG_CONS, LOG_ALERT);


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::CLoggerSyslog
//
//	@doc:
//		Ctor - set executable name, initialization flags and message priority
//
//---------------------------------------------------------------------------
CLoggerSyslog::CLoggerSyslog(const CHAR *proc_name, ULONG init_mask,
							 ULONG message_priority)
	: m_proc_name(proc_name),
	  m_init_mask(init_mask),
	  m_message_priority(message_priority)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::~CLoggerSyslog
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLoggerSyslog::~CLoggerSyslog() = default;

//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::Write
//
//	@doc:
//		Write string to syslog
//
//---------------------------------------------------------------------------
void
CLoggerSyslog::Write(const WCHAR *log_entry,
					 ULONG	// severity
)
{
	CHAR *buffer = CLogger::Msg();

	// create message
	CStringStatic str(buffer, GPOS_LOG_MESSAGE_BUFFER_SIZE);
	str.AppendConvert(log_entry);

	// send message to syslog
	syslib::OpenLog(m_proc_name, m_init_mask, LOG_USER);
	syslib::SysLog(m_message_priority, buffer);
	syslib::CloseLog();
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerSyslog::Write
//
//	@doc:
//		Write alert message to syslog - use ASCII characters only
//
//---------------------------------------------------------------------------
void
CLoggerSyslog::Alert(const WCHAR *msg)
{
	m_alert_logger.Write(msg, CException::ExsevError);
}

// EOF
