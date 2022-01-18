//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileDescriptor.cpp
//
//	@doc:
//		File descriptor abstraction
//---------------------------------------------------------------------------


#include "gpos/io/CFileDescriptor.h"

#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/IWorker.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CFileDescriptor::CFileDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFileDescriptor::CFileDescriptor() = default;


//---------------------------------------------------------------------------
//	@function:
//		CFileDescriptor::OpenFile
//
//	@doc:
//		Open file descriptor
//
//---------------------------------------------------------------------------
void
CFileDescriptor::OpenFile(const CHAR *file_path, ULONG mode,
						  ULONG permission_bits)
{
	GPOS_ASSERT(!IsFileOpen());

	BOOL fOpened = false;

	while (!fOpened)
	{
		m_file_descriptor = GPOS_FILE_DESCR_INVALID;

		// create file with given mode and permissions.
		m_file_descriptor = ioutils::OpenFile(file_path, mode, permission_bits);

		// check for error
		if (GPOS_FILE_DESCR_INVALID == m_file_descriptor)
		{
			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				GPOS_CHECK_ABORT;

				continue;
			}

			GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
		}

		fOpened = true;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CFile::~CFile
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFileDescriptor::~CFileDescriptor()
{
	if (IsFileOpen())
	{
		CloseFile();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CFile::CloseFile
//
//	@doc:
//		Close file
//
//---------------------------------------------------------------------------
void
CFileDescriptor::CloseFile()
{
	GPOS_ASSERT(IsFileOpen());

	BOOL fClosed = false;

	while (!fClosed)
	{
		INT res = ioutils::CloseFile(m_file_descriptor);

		// check for error
		if (0 != res)
		{
			GPOS_ASSERT(EINTR == errno || EIO == errno);

			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				continue;
			}
		}

		fClosed = true;
	}

	m_file_descriptor = GPOS_FILE_DESCR_INVALID;
}

// EOF
