//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMDIndexInfo.h
//
//	@doc:
//		Implementation of indexinfo in relation metadata
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIndexInfo_H
#define GPMD_CMDIndexInfo_H

#include "gpos/base.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDInterface.h"

namespace gpmd
{
using namespace gpos;
using namespace gpdxl;

// class for indexinfo in relation metadata
class CMDIndexInfo : public IMDInterface
{
private:
	// index mdid
	IMDId *m_mdid;

	// is the index partial
	BOOL m_is_partial;

public:
	// ctor
	CMDIndexInfo(IMDId *mdid, BOOL is_partial);

	// dtor
	~CMDIndexInfo() override;

	// index mdid
	IMDId *MDId() const;

	// is the index partial
	BOOL IsPartial() const;

	// serialize indexinfo in DXL format given a serializer object
	virtual void Serialize(CXMLSerializer *) const;

#ifdef GPOS_DEBUG
	// debug print of the index info
	virtual void DebugPrint(IOstream &os) const;
#endif
};

}  // namespace gpmd

#endif	// !GPMD_CMDIndexInfo_H

// EOF
