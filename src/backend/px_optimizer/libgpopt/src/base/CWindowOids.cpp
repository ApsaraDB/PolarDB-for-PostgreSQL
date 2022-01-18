//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.

#include "gpopt/base/CWindowOids.h"

using namespace gpopt;

CWindowOids::CWindowOids(OID row_number_oid, OID rank_oid)
{
	m_oidRowNumber = row_number_oid;
	m_oidRank = rank_oid;
}

OID
CWindowOids::OidRowNumber() const
{
	return m_oidRowNumber;
}

OID
CWindowOids::OidRank() const
{
	return m_oidRank;
}

CWindowOids *
CWindowOids::GetWindowOids(CMemoryPool *mp)
{
	return GPOS_NEW(mp) CWindowOids(DUMMY_ROW_NUMBER_OID, DUMMY_WIN_RANK);
}
