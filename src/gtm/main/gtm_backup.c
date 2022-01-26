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
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_backup.h"
#include "gtm/elog.h"

GTM_RWLock gtm_bkup_lock;
bool gtm_need_bkup;

#ifndef POLARDB_X
extern char GTMControlFile[];



void GTM_WriteRestorePoint(void)
{
    FILE *f = fopen(GTMControlFile, "w");

    if (f == NULL)
    {
        ereport(LOG, (errno,
                      errmsg("Cannot open control file"),
                      errhint("%s", strerror(errno))));
        return;
    }
    GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_WRITE);
    if (!gtm_need_bkup)
    {
        GTM_RWLockRelease(&gtm_bkup_lock);
        fclose(f);
        return;
    }
    gtm_need_bkup = FALSE;
    GTM_RWLockRelease(&gtm_bkup_lock);
    GTM_WriteRestorePointVersion(f);
    GTM_WriteRestorePointXid(f);
    GTM_WriteRestorePointSeq(f);
    fclose(f);
}
#endif

void GTM_WriteBarrierBackup(char *barrier_id)
{
#define MyMAXPATH 1023

    FILE  *f;
    char BarrierFilePath[MyMAXPATH+1];
    extern char *GTMDataDir;

    snprintf(BarrierFilePath, MyMAXPATH, "%s/GTM_%s.control", GTMDataDir, barrier_id);
    if ((f = fopen(BarrierFilePath, "w")) == NULL)
    {
        ereport(LOG, (errno,
                      errmsg("Cannot open control file"),
                      errhint("%s", strerror(errno))));
        return;
    }
    GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_WRITE);
    gtm_need_bkup = FALSE;
    GTM_RWLockRelease(&gtm_bkup_lock);
    GTM_WriteRestorePointVersion(f);
    GTM_WriteRestorePointXid(f);
    GTM_WriteRestorePointSeq(f);
    fclose(f);
}
    
void GTM_SetNeedBackup(void)
{
    GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_READ);
    gtm_need_bkup = TRUE;
    GTM_RWLockRelease(&gtm_bkup_lock);
}

bool GTM_NeedBackup(void)
{
    return gtm_need_bkup;
}
