//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IOstream.h
//
//	@doc:
//		Output stream interface;
//---------------------------------------------------------------------------
#ifndef GPOS_IOstream_H
#define GPOS_IOstream_H

#include "gpos/types.h"

namespace gpos
{
// wide char ostream
typedef std::basic_ostream<WCHAR, std::char_traits<WCHAR> > WOSTREAM;

//---------------------------------------------------------------------------
//	@class:
//		IOstream
//
//	@doc:
//		Defines all available operator interfaces; avoids having to overload
//		system stream classes or their operators/member functions
//
//---------------------------------------------------------------------------
class IOstream
{
protected:
	// ctor
	IOstream() = default;

	BOOL m_fullPrecision{false};

public:
	enum EStreamManipulator
	{
		EsmDec,
		EsmHex
		// no sentinel to enforce strict switch-ing
	};

	// virtual dtor
	virtual ~IOstream() = default;

	// operator interface
	virtual IOstream &operator<<(const CHAR *) = 0;
	virtual IOstream &operator<<(const WCHAR) = 0;
	virtual IOstream &operator<<(const CHAR) = 0;
	virtual IOstream &operator<<(ULONG) = 0;
	virtual IOstream &operator<<(ULLONG) = 0;
	virtual IOstream &operator<<(INT) = 0;
	virtual IOstream &operator<<(LINT) = 0;
	virtual IOstream &operator<<(DOUBLE) = 0;
	virtual IOstream &operator<<(const void *) = 0;
	virtual IOstream &operator<<(WOSTREAM &(*) (WOSTREAM &) ) = 0;
	virtual IOstream &operator<<(EStreamManipulator) = 0;

	// needs to be implemented by subclass
	virtual IOstream &operator<<(const WCHAR *) = 0;

	void
	SetFullPrecision(BOOL fullPrecision)
	{
		m_fullPrecision = fullPrecision;
	}
};

}  // namespace gpos

#endif	// !GPOS_IOstream_H

// EOF
