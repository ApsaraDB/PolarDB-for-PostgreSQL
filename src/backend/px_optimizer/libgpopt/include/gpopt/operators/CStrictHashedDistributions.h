//	Greenplum Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.

#ifndef GPOPT_CHashedDistributions_H
#define GPOPT_CHashedDistributions_H

#include "gpos/memory/CMemoryPool.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecStrictHashed.h"

namespace gpopt
{
// Build hashed distributions used in physical union all during
// distribution derivation. The class is an array of hashed
// distribution on input column of each child, and an output hashed
// distribution on UnionAll output columns
// If there exists no redistributable columns in the input list,
// it creates a random distribution.
class CStrictHashedDistributions : public CDistributionSpecArray
{
public:
	CStrictHashedDistributions(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
							   CColRef2dArray *pdrgpdrgpcrInput);
};
}  // namespace gpopt

#endif	//GPOPT_CHashedDistributions_H
