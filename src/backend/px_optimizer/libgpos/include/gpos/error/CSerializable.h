//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializable.h
//
//	@doc:
//		Interface for serializable objects;
//		used to dump objects when an exception is raised;
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializable_H
#define GPOS_CSerializable_H

#include "gpos/base.h"
#include "gpos/common/CList.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSerializable
//
//	@doc:
//		Interface for serializable objects;
//
//---------------------------------------------------------------------------
class CSerializable : CStackObject
{
private:
public:
	CSerializable(const CSerializable &) = delete;

	// ctor
	CSerializable();

	// dtor
	virtual ~CSerializable();

	// serialize object to passed stream
	virtual void Serialize(COstream &oos) = 0;

	// link for list in error context
	SLink m_err_ctxt_link;

};	// class CSerializable
}  // namespace gpos

#endif	// !GPOS_CSerializable_H

// EOF
