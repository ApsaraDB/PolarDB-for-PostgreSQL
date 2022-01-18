//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerStream.h
//
//	@doc:
//		Implementation of logging interface over stream
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerStream_H
#define GPOS_CLoggerStream_H

#include "gpos/error/CLogger.h"
#include "gpos/io/IOstream.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CLoggerStream
//
//	@doc:
//		Stream logging.
//
//---------------------------------------------------------------------------

class CLoggerStream : public CLogger
{
private:
	// log stream
	IOstream &m_os;

	// write string to stream
	void
	Write(const WCHAR *log_entry,
		  ULONG	 // severity
		  ) override
	{
		m_os = m_os << log_entry;
	}

public:
	CLoggerStream(const CLoggerStream &) = delete;

	// ctor
	CLoggerStream(IOstream &os);

	// dtor
	~CLoggerStream() override;

	// wrapper for stdout
	static CLoggerStream m_stdout_stream_logger;

	// wrapper for stderr
	static CLoggerStream m_stderr_stream_logger;

};	// class CLoggerStream
}  // namespace gpos

#endif	// !GPOS_CLoggerStream_H

// EOF
