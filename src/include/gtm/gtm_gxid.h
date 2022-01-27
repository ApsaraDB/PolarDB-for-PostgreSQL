/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
#ifndef _GTM_GXID_H
#define _GTM_GXID_H

/* ----------------
 *        Special transaction ID values
 *
 * BootstrapGlobalTransactionId is the XID for "bootstrap" operations, and
 * FrozenGlobalTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalGlobalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define BootstrapGlobalTransactionId        ((GlobalTransactionId) 1)
#define FrozenGlobalTransactionId            ((GlobalTransactionId) 2)
#define FirstNormalGlobalTransactionId    ((GlobalTransactionId) 3)
#define MaxGlobalTransactionId            ((GlobalTransactionId) 0xFFFFFFFF)
#define FirstGlobalTimestamp            ((GlobalTimestamp) 1000000000)

/* ----------------
 *        transaction ID manipulation macros
 * ----------------
 */
#define GlobalTransactionIdIsNormal(xid)        ((xid) >= FirstNormalGlobalTransactionId)
#define GlobalTransactionIdEquals(id1, id2)    ((id1) == (id2))
#define GlobalTransactionIdStore(xid, dest)    (*(dest) = (xid))
#define StoreInvalidGlobalTransactionId(dest) (*(dest) = InvalidGlobalTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdAdvance(dest)    \
    do { \
        (dest)++; \
        if ((dest) < FirstNormalGlobalTransactionId) \
            (dest) = FirstNormalGlobalTransactionId; \
    } while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define GlobalTransactionIdRetreat(dest)    \
    do { \
        (dest)--; \
    } while ((dest) < FirstNormalGlobalTransactionId)

extern bool GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2);
extern bool GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2);
#endif
