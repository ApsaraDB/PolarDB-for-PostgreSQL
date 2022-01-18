//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableMDAccessor.h
//
//	@doc:
//		Wrapper for serializing MD accessor objects
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializableMDAccessor_H
#define GPOS_CSerializableMDAccessor_H

#include "gpos/base.h"
#include "gpos/error/CSerializable.h"

#include "gpopt/operators/CExpression.h"

using namespace gpos;
using namespace gpdxl;

namespace gpopt
{
// fwd decl
class CMDAccessor;

//---------------------------------------------------------------------------
//	@class:
//		CSerializableMDAccessor
//
//	@doc:
//		Wrapper for serializing MD objects in a minidump
//
//---------------------------------------------------------------------------
class CSerializableMDAccessor : public CSerializable
{
private:
	// MD accessor
	CMDAccessor *m_pmda;

	// serialize header
	void SerializeHeader(COstream &oos);

	// serialize footer
	static void SerializeFooter(COstream &oos);

public:
	CSerializableMDAccessor(const CSerializableMDAccessor &) = delete;

	// ctor
	explicit CSerializableMDAccessor(CMDAccessor *md_accessor);

	// dtor
	~CSerializableMDAccessor() override = default;

	// serialize object to passed stream
	void Serialize(COstream &oos) override;

};	// class CSerializableMDAccessor
}  // namespace gpopt

#endif	// !GPOS_CSerializableMDAccessor_H

// EOF
