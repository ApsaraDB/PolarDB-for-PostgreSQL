/*-------------------------------------------------------------------------
 * polar_priority_replication.h
 *	   Polar priority replication management.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *      src/include/replication/polar_priority_replication.h
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_PRIORITY_REPLICATION_H
#define POLAR_PRIORITY_REPLICATION_H

#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walsender_private.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/* POLAR */
#define POLAR_DEFAULT_HIGH_PRI_REP_STANDBY_NAME "standby1"

/* POLAR: used only while holding SyncRepLock */
#define POLAR_PRI_REP_ENABLE() \
	(WalSndCtl->priority_replication_mode > POLAR_PRI_REP_OFF)

#define POLAR_HIGH_PRI_REP_STANDBYS_DEFINED() \
	(polar_high_priority_replication_standby_names != NULL && \
	polar_high_priority_replication_standby_names[0] != '\0')

#define POLAR_LOW_PRI_REP_STANDBYS_DEFINED() \
	(polar_low_priority_replication_standby_names != NULL && \
	polar_low_priority_replication_standby_names[0] != '\0')

typedef enum
{
	POLAR_PRI_REP_OFF,		/* disable priority replication */
	POLAR_PRI_REP_ANY,	/* priority replication wait for any one standby on a higher level */
	POLAR_PRI_REP_ALL	/* priority replication wait for all standby on a higher level*/
} PriorityReplicationMode;

/* POLAR GUCs */
extern int		polar_priority_replication_mode;
extern bool		polar_priority_replication_force_wait;

extern char	   *polar_high_priority_replication_standby_names;
extern char	   *polar_low_priority_replication_standby_names;

/* POLAR: called by low priority replication walsender */
extern void polar_priority_replication_wait_for_lsn(XLogRecPtr lsn);
extern void polar_priority_replication_walsender_wait_for_lsn(int wait_priority, XLogRecPtr lsn);

/* POLAR: called by high priority replication walsender */
extern void polar_priority_replication_release_waiters(void);

/* POLAR: called by checkpointer */
extern void polar_priority_replication_update_priority_replication_force_wait(void);
extern void polar_priority_replication_update_high_priority_replication_standbys_defined(void);
extern void polar_priority_replication_update_low_priority_replication_standbys_defined(void);
extern void polar_priority_replication_update_priority_replication_mode(void);
extern void polar_priority_replication_update_walsender_type(void);

#endif							/* POLAR_PRIORITY_REPLICATION_H */