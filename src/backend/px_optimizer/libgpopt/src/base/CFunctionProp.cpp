//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CFunctionProp.cpp
//
//	@doc:
//		Implementation of function properties
//---------------------------------------------------------------------------

#include "gpopt/base/CFunctionProp.h"

#include "gpos/base.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CFunctionProp);

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::CFunctionProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionProp::CFunctionProp(IMDFunction::EFuncStbl func_stability,
							 BOOL fHasVolatileFunctionScan, BOOL fScan)
	: m_efs(func_stability),
	  m_fHasVolatileFunctionScan(fHasVolatileFunctionScan),
	  m_fScan(fScan),
	  m_isGlobalFunc(false)
{
	GPOS_ASSERT(IMDFunction::EfsSentinel > func_stability);
	GPOS_ASSERT_IMP(fScan && IMDFunction::EfsVolatile == func_stability,
					fHasVolatileFunctionScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::~CFunctionProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionProp::~CFunctionProp() = default;

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::SingletonExecution
//
//	@doc:
//		Check if must execute on a single host based on function properties
//
//---------------------------------------------------------------------------
BOOL
CFunctionProp::NeedsSingletonExecution() const
{
	// a function needs to execute on a single host if any of the following holds:
	// a) it reads or modifies SQL data
	// b) it is volatile and used as a scan operator (i.e. in the from clause)

	// TODO:  - Feb 10, 2014; enable the following line instead of the
	// current return statement once all function properties are fixed
	//return (IMDFunction::EfdaContainsSQL < m_efda || (m_fScan && IMDFunction::EfsVolatile == m_efs));

	return m_fScan && (IMDFunction::EfsVolatile == m_efs ||
					   IMDFunction::EfsStable == m_efs);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CFunctionProp::OsPrint(IOstream &os) const
{
	const CHAR *rgszStability[] = {"Immutable", "Stable", "Volatile"};

	os << rgszStability[m_efs];
	return os;
}

// EOF
