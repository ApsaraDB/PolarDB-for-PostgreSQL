//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		ITimer.h
//
//	@doc:
//		A timer which records time between construction and the ElapsedMS call;
//---------------------------------------------------------------------------
#ifndef GPOS_ITimer_H
#define GPOS_ITimer_H

#include "gpos/types.h"
#include "gpos/utils.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		ITimer
//
//	@doc:
//		Timer interface;
//
//---------------------------------------------------------------------------
class ITimer
{
private:
public:
	ITimer(const ITimer &) = delete;

	// ctor
	ITimer() = default;

	// dtor
	virtual ~ITimer() = default;

	// retrieve elapsed time in micro-seconds
	virtual ULONG ElapsedUS() const = 0;

	// retrieve elapsed time in milli-seconds
	ULONG
	ElapsedMS() const
	{
		return ElapsedUS() / GPOS_USEC_IN_MSEC;
	}

	// restart timer
	virtual void Restart() = 0;

};	// class ITimer

}  // namespace gpos

#endif	// !GPOS_ITimer_H

// EOF
