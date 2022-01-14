//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDProviderRelcache.h
//
//	@doc:
//		Relcache-based provider of metadata objects.
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderRelcache_H
#define GPMD_CMDProviderRelcache_H

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

// fwd decl
namespace gpopt
{
	class CMDAccessor;
}

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderRelcache
	//
	//	@doc:
	//		Relcache-based provider of metadata objects.
	//
	//---------------------------------------------------------------------------
	class CMDProviderRelcache : public IMDProvider
	{
		private:
			// memory pool
			CMemoryPool *m_mp;

			// private copy ctor
			CMDProviderRelcache(const CMDProviderRelcache&);

		public:
			// ctor/dtor
			explicit
			CMDProviderRelcache(CMemoryPool *mp);

			~CMDProviderRelcache() override
			{
			}

			// returns the DXL string of the requested metadata object
			CWStringBase *GetMDObjDXLStr(CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *md_id) const override;

			// return the mdid for the requested type
			IMDId *MDId
				(
				CMemoryPool *mp,
				CSystemId sysid,
				IMDType::ETypeInfo type_info
				)
				const  override
			{
				return GetGPDBTypeMdid(mp, sysid, type_info);
			}

			// return the requested metadata object
			IMDCacheObject *GetMDObj(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *mdid) const override;
	};
}



#endif // !GPMD_CMDProviderRelcache_H

// EOF
