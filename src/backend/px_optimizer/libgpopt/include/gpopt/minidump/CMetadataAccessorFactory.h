//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CMetadataAccessorFactory_H
#define GPOPT_CMetadataAccessorFactory_H

#include "gpos/common/CAutoP.h"
#include "gpos/memory/CMemoryPool.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/minidump/CDXLMinidump.h"

namespace gpopt
{
class CMetadataAccessorFactory
{
public:
	CMetadataAccessorFactory(CMemoryPool *mp, CDXLMinidump *pdxlmd,
							 const CHAR *file_name);

	CMDAccessor *Pmda();

private:
	CAutoP<CMDAccessor> m_apmda;
};

}  // namespace gpopt
#endif
