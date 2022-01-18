//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderGeneric.h
//
//	@doc:
//		Provider of system-independent metadata objects.
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderGeneric_H
#define GPMD_CMDProviderGeneric_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

#define GPMD_DEFAULT_SYSID GPOS_WSZ_LIT("GPDB")

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDProviderGeneric
//
//	@doc:
//		Provider of system-independent metadata objects.
//
//---------------------------------------------------------------------------
class CMDProviderGeneric
{
private:
	// mdid of int2
	IMDId *m_mdid_int2;

	// mdid of int4
	IMDId *m_mdid_int4;

	// mdid of int8
	IMDId *m_mdid_int8;

	// mdid of bool
	IMDId *m_mdid_bool;

	// mdid of oid
	IMDId *m_mdid_oid;

public:
	CMDProviderGeneric(const CMDProviderGeneric &) = delete;

	// ctor/dtor
	CMDProviderGeneric(CMemoryPool *mp);

	// dtor
	~CMDProviderGeneric();

	// return the mdid for the requested type
	IMDId *MDId(IMDType::ETypeInfo type_info) const;

	// default system id
	static CSystemId SysidDefault();
};
}  // namespace gpmd



#endif	// !GPMD_CMDProviderGeneric_H

// EOF
