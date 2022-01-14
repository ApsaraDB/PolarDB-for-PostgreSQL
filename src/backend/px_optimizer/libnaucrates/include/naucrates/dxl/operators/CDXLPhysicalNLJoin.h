//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalNLJoin.h
//
//	@doc:
//		Class for representing DXL nested loop join operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalNLJoin_H
#define GPDXL_CDXLPhysicalNLJoin_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLColRef.h"
#include "naucrates/dxl/operators/CDXLPhysicalJoin.h"

namespace gpdxl
{
// indices of nested loop join elements in the children array
enum Edxlnlj
{
	EdxlnljIndexProjList = 0,
	EdxlnljIndexFilter,
	EdxlnljIndexJoinFilter,
	EdxlnljIndexLeftChild,
	EdxlnljIndexRightChild,
	EdxlnljIndexSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalNLJoin
//
//	@doc:
//		Class for representing DXL nested loop join operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalNLJoin : public CDXLPhysicalJoin
{
private:
	// flag to indicate whether operator is an index nested loops,
	// i.e., inner side is an index scan that uses values from outer side
	BOOL m_is_index_nlj;

	// array holding nest params col references used for creating nestparam
	// node during translation
	CDXLColRefArray *m_nest_params_col_refs;

	// if nest params are required to be parsed
	BOOL m_nest_params_exists;

	void SerializeNestLoopParamsToDXL(CXMLSerializer *pxmlser) const;

public:
	CDXLPhysicalNLJoin(const CDXLPhysicalNLJoin &) = delete;

	// ctor/dtor
	CDXLPhysicalNLJoin(CMemoryPool *mp, EdxlJoinType join_type,
					   BOOL is_index_nlj, BOOL nest_params_exists);

	~CDXLPhysicalNLJoin() override;

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// is operator an index nested loops?
	BOOL
	IsIndexNLJ() const
	{
		return m_is_index_nlj;
	}

	// nest params exists for parsing
	BOOL NestParamsExists() const;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	void SetNestLoopParamsColRefs(CDXLColRefArray *nest_params_col_refs);

	CDXLColRefArray *GetNestLoopParamsColRefs() const;

	// conversion function
	static CDXLPhysicalNLJoin *
	PdxlConvert(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalNLJoin == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalNLJoin *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalNLJoin_H

// EOF
