//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileDescriptor.h
//
//	@doc:
//		File descriptor abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_CFileDescriptor_H
#define GPOS_CFileDescriptor_H

#include "gpos/types.h"

#define GPOS_FILE_NAME_BUF_SIZE (1024)
#define GPOS_FILE_DESCR_INVALID (-1)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CFileDescriptor
//
//	@doc:
//		File handler abstraction;
//
//---------------------------------------------------------------------------

class CFileDescriptor
{
private:
	// file descriptor
	INT m_file_descriptor{GPOS_FILE_DESCR_INVALID};

protected:
	// ctor -- accessible through inheritance only
	CFileDescriptor();

	// dtor -- accessible through inheritance only
	virtual ~CFileDescriptor();

	// get file descriptor
	INT
	GetFileDescriptor() const
	{
		return m_file_descriptor;
	}

	// open file
	void OpenFile(const CHAR *file_path, ULONG mode, ULONG permission_bits);

	// close file
	void CloseFile();

public:
	CFileDescriptor(const CFileDescriptor &) = delete;

	// check if file is open
	BOOL
	IsFileOpen() const
	{
		return (GPOS_FILE_DESCR_INVALID != m_file_descriptor);
	}

};	// class CFile
}  // namespace gpos

#endif	// !GPOS_CFile_H

// EOF
