//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWString.cpp
//
//	@doc:
//		Implementation of the wide character string class.
//---------------------------------------------------------------------------

#include "gpos/string/CWString.h"

#include "gpos/base.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CWString::CWString
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWString::CWString(ULONG length)
	: CWStringBase(length,
				   true	 // owns_memory
				   ),
	  m_w_str_buffer(nullptr)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::GetBuffer
//
//	@doc:
//		Returns the wide character buffer storing the string
//
//---------------------------------------------------------------------------
const WCHAR *
CWString::GetBuffer() const
{
	return m_w_str_buffer;
}


//---------------------------------------------------------------------------
//	@function:
//		CWString::Append
//
//	@doc:
//		Appends a string to the current string
//
//---------------------------------------------------------------------------
void
CWString::Append(const CWStringBase *str)
{
	GPOS_ASSERT(nullptr != str);
	if (0 < str->Length())
	{
		AppendBuffer(str->GetBuffer());
	}
	GPOS_ASSERT(IsValid());
}

// EOF
