//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CFileWriter.h
//
//	@doc:
//		File writer
//---------------------------------------------------------------------------

#ifndef GPOS_CFileWriter_H
#define GPOS_CFileWriter_H

#include "gpos/io/CFileDescriptor.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CFileWriter
//
//	@doc:
//		Implementation of file handler for raw output;
//		does not provide thread-safety
//
//---------------------------------------------------------------------------
class CFileWriter : public CFileDescriptor
{
private:
	// file size
	ULLONG m_file_size{0};

public:
	CFileWriter(const CFileWriter &) = delete;

	// ctor
	CFileWriter();

	// dtor
	~CFileWriter() override = default;

	ULLONG
	FileSize() const
	{
		return m_file_size;
	}

	// open file for writing
	void Open(const CHAR *file_path, ULONG permission_bits);

	// close file
	void Close();

	// write bytes to file
	void Write(const BYTE *read_buffer, const ULONG_PTR write_size);

};	// class CFileWriter

}  // namespace gpos

#endif	// !GPOS_CFileWriter_H

// EOF
