//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CName.cpp
//
//	@doc:
//		Metadata name of objects
//		Encapsulates encoding etc. so optimizer logic does not have to
//		deal with it.
//		Only assumption, name string is NULL terminated;
//---------------------------------------------------------------------------

#include "gpopt/metadata/CName.h"

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CName);

//---------------------------------------------------------------------------
//	@function:
//		CName::CName
//
//	@doc:
//		ctor
//		deep copy of the provided string
//
//---------------------------------------------------------------------------
CName::CName(CMemoryPool *mp, const CWStringBase *str)
	: m_str_name(nullptr), m_fDeepCopy(true)
{
	m_str_name = GPOS_NEW(mp) CWStringConst(mp, str->GetBuffer());
}

//---------------------------------------------------------------------------
//	@function:
//		CName::CName
//
//	@doc:
//		ctor
//		the string object can become property of the CName object, or not, as
//		specified by the fOwnsMemory argument
//
//---------------------------------------------------------------------------
CName::CName(const CWStringConst *str, BOOL fOwnsMemory)
	: m_str_name(str), m_fDeepCopy(fOwnsMemory)
{
	GPOS_ASSERT(nullptr != m_str_name);
	GPOS_ASSERT(m_str_name->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CName::CName
//
//	@doc:
//		ctor
//		combine 2 names to one compound name; used to incrementally add
//		names for canonical multi-part names
//
//---------------------------------------------------------------------------
CName::CName(CMemoryPool *mp, const CName &nameFirst, const CName &nameSecond)
	: m_str_name(nullptr), m_fDeepCopy(false)
{
	CWStringDynamic *pstrTmp =
		GPOS_NEW(mp) CWStringDynamic(mp, (nameFirst.Pstr())->GetBuffer());
	pstrTmp->Append(nameSecond.Pstr());

	m_str_name = GPOS_NEW(mp) CWStringConst(mp, pstrTmp->GetBuffer());
	m_fDeepCopy = true;

	// release tmp string
	GPOS_DELETE(pstrTmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CName::CName
//
//	@doc:
//		ctor
//		shallow copy constructor
//
//---------------------------------------------------------------------------
CName::CName(const CName &name) : m_str_name(name.Pstr()), m_fDeepCopy(false)
{
	GPOS_ASSERT(nullptr != m_str_name->GetBuffer());
	GPOS_ASSERT(m_str_name->IsValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CName::CName
//
//	@doc:
//		ctor
//		deep copy constructor
//
//---------------------------------------------------------------------------
CName::CName(CMemoryPool *mp, const CName &name)
	: m_str_name(nullptr), m_fDeepCopy(false)
{
	DeepCopy(mp, name.Pstr());
}


//---------------------------------------------------------------------------
//	@function:
//		CName::~CName
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CName::~CName()
{
	GPOS_ASSERT(m_str_name->IsValid());

	if (m_fDeepCopy)
	{
		GPOS_DELETE(m_str_name);
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CName::DeepCopy
//
//	@doc:
//		Deep copying of string
//
//---------------------------------------------------------------------------
void
CName::DeepCopy(CMemoryPool *mp, const CWStringConst *str)
{
	m_str_name = GPOS_NEW(mp) CWStringConst(mp, str->GetBuffer());
	m_fDeepCopy = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CName::Equals
//
//	@doc:
//		comparison of names
//
//---------------------------------------------------------------------------
BOOL
CName::Equals(const CName &name) const
{
	return m_str_name->Equals((name.Pstr()));
}



//---------------------------------------------------------------------------
//	@function:
//		CName::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CName::OsPrint(IOstream &os) const
{
	os << GPOPT_NAME_QUOTE_BEGIN << m_str_name->GetBuffer()
	   << GPOPT_NAME_QUOTE_END;
	return os;
}

// EOF
