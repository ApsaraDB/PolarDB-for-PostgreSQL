/*-------------------------------------------------------------------------
 * slot.h
 *	   Replication slot management.
 *
 * Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOT_H
#define SLOT_H

#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Behaviour of replication slots, upon release or crash.
 *
 * Slots marked as PERSISTENT are crash-safe and will not be dropped when
 * released. Slots marked as EPHEMERAL will be dropped when released or after
 * restarts.  Slots marked TEMPORARY will be dropped at the end of a session
 * or on error.
 *
 * EPHEMERAL is used as a not-quite-ready state when creating persistent
 * slots.  EPHEMERAL slots can be made PERSISTENT by calling
 * ReplicationSlotPersist().  For a slot that goes away at the end of a
 * session, TEMPORARY is the appropriate choice.
 */
typedef enum ReplicationSlotPersistency
{
	RS_PERSISTENT,
	RS_EPHEMERAL,
	RS_TEMPORARY
} ReplicationSlotPersistency;

/* For ReplicationSlotAcquire, q.v. */
typedef enum SlotAcquireBehavior
{
	SAB_Error,
	SAB_Block,
	SAB_Inquire
} SlotAcquireBehavior;

/*
 * On-Disk data of a replication slot, preserved across restarts.
 * Warning!!!: modifying this structure will affect the compatibility. 
 * Please add the compatibility logic in the restoreslotfromdisk function.
 */
typedef struct ReplicationSlotPersistentData
{
	/* The slot's identifier */
	NameData	name;

	/* database the slot is active on */
	Oid			database;

	/*
	 * The slot's behaviour when being dropped (or restored after a crash).
	 */
	ReplicationSlotPersistency persistency;

	/*
	 * xmin horizon for data
	 *
	 * NB: This may represent a value that hasn't been written to disk yet;
	 * see notes for effective_xmin, below.
	 */
	TransactionId xmin;

	/*
	 * xmin horizon for catalog tuples
	 *
	 * NB: This may represent a value that hasn't been written to disk yet;
	 * see notes for effective_xmin, below.
	 */
	TransactionId catalog_xmin;

	/* oldest LSN that might be required by this replication slot */
	XLogRecPtr	restart_lsn;

	/*
	 * Oldest LSN that the client has acked receipt for.  This is used as the
	 * start_lsn point in case the client doesn't specify one, and also as a
	 * safety measure to jump forwards in case the client specifies a
	 * start_lsn that's further in the past than this value.
	 */
	XLogRecPtr	confirmed_flush;

	/* plugin name */
	NameData	plugin;

	/* POLAR: replica apply lsn */
	XLogRecPtr	polar_replica_apply_lsn;

	/* restart_lsn is copied here when the slot is invalidated */
	XLogRecPtr	polar_invalidated_at;
} ReplicationSlotPersistentData;

/*
 * Shared memory state of a single replication slot.
 *
 * The in-memory data of replication slots follows a locking model based
 * on two linked concepts:
 * - A replication slot's in_use flag is switched when added or discarded using
 * the LWLock ReplicationSlotControlLock, which needs to be hold in exclusive
 * mode when updating the flag by the backend owning the slot and doing the
 * operation, while readers (concurrent backends not owning the slot) need
 * to hold it in shared mode when looking at replication slot data.
 * - Individual fields are protected by mutex where only the backend owning
 * the slot is authorized to update the fields from its own slot.  The
 * backend owning the slot does not need to take this lock when reading its
 * own fields, while concurrent backends not owning this slot should take the
 * lock when reading this slot's data.
 */
typedef struct ReplicationSlot
{
	/* lock, on same cacheline as effective_xmin */
	slock_t		mutex;

	/* is this slot defined */
	bool		in_use;

	/* Who is streaming out changes for this slot? 0 in unused slots. */
	pid_t		active_pid;

	/* any outstanding modifications? */
	bool		just_dirtied;
	bool		dirty;

	/*
	 * For logical decoding, it's extremely important that we never remove any
	 * data that's still needed for decoding purposes, even after a crash;
	 * otherwise, decoding will produce wrong answers.  Ordinary streaming
	 * replication also needs to prevent old row versions from being removed
	 * too soon, but the worst consequence we might encounter there is
	 * unwanted query cancellations on the standby.  Thus, for logical
	 * decoding, this value represents the latest xmin that has actually been
	 * written to disk, whereas for streaming replication, it's just the same
	 * as the persistent value (data.xmin).
	 */
	TransactionId effective_xmin;
	TransactionId effective_catalog_xmin;

	/* data surviving shutdowns and crashes */
	ReplicationSlotPersistentData data;

	/* is somebody performing io on this slot? */
	LWLock		io_in_progress_lock;

	/* Condition variable signalled when active_pid changes */
	ConditionVariable active_cv;

	/* all the remaining data is only used for logical slots */

	/*
	 * When the client has confirmed flushes >= candidate_xmin_lsn we can
	 * advance the catalog xmin.  When restart_valid has been passed,
	 * restart_lsn can be increased.
	 */
	TransactionId candidate_catalog_xmin;
	XLogRecPtr	candidate_xmin_lsn;
	XLogRecPtr	candidate_restart_valid;
	XLogRecPtr	candidate_restart_lsn;

	/* POLAR: lsn to record replica lock redo state */
	XLogRecPtr	polar_replica_lock_lsn;

	/* POLAR: node infos */
	char		node_name[NAMEDATALEN];		/* dummy size, it's OK to truncate */
	char		node_host[NI_MAXHOST];
	int			node_port;
	char		node_release_date[20];
	char		node_version[20];
	char		node_slot_name[NAMEDATALEN];
	int			node_type;
	int			node_state;
	int			node_cpu;
	int			node_cpu_quota;
	int			node_memory;
	int			node_memory_quota;
	int			node_iops;
	int			node_iops_quota;
	int			node_connection;
	int			node_connection_quota;
	int			node_px_connection;
	int			node_px_connection_quota;
	int32		polar_sent_cluster_info_generation; /* The cluster info generation we have sent */
} ReplicationSlot;

#define SlotIsPhysical(slot) ((slot)->data.database == InvalidOid)
#define SlotIsLogical(slot) ((slot)->data.database != InvalidOid)

/* POLAR BEGIN */

/*
 * POLAR: polardb standby mounts a remote filesystem different from master's in read and write mode
 */
#define POLAR_IS_MASTER_IN_RECOVERY_OR_STANDBY \
	(RecoveryInProgress() && polar_enable_shared_storage_mode && !polar_in_replica_mode())

#define	POLAR_IS_MASTER_OR_STANDBY \
	(polar_enable_shared_storage_mode && !polar_in_replica_mode())
/*
 * POLAR: whether persist logical slot in polarstore
 */
#define POLAR_PERSIST_LOGICAL_SLOT(slot) \
	(polar_enable_persisted_logical_slot && \
	(POLAR_IS_MASTER_OR_STANDBY) && \
	!RecoveryInProgress() && \
	SlotIsLogical(slot))

/*
 * POLAR: whether restore persisted logical slot from polarstore
 */
#define POLAR_RESTORE_PERSISTED_LOGICAL_SLOT \
	(polar_enable_persisted_logical_slot && POLAR_IS_MASTER_IN_RECOVERY_OR_STANDBY)

/*
 * POLAR: whether persist spill file in polarstore
 */
#define POLAR_PERSIST_SPILL_FILE \
	(polar_enable_persisted_logical_slot && polar_enable_persisted_spill_file && \
	POLAR_IS_MASTER_OR_STANDBY)
/* POLAR END */

/*
 * POLAR: whether persisted physical slot in shared storage for online promote
 */
#define POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(slot) \
	(polar_enable_persisted_physical_slot && POLAR_IS_MASTER_OR_STANDBY && \
	!SlotIsLogical(slot))

/*
 * POLAR: whether restore persisted physical slot from polarstore for online promote
 */
#define POLAR_RESTORE_PERSISTED_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE \
	(polar_enable_persisted_physical_slot && POLAR_IS_MASTER_OR_STANDBY)

/*
 * Shared memory control area for all of replication slots.
 */
typedef struct ReplicationSlotCtlData
{
	/*
	 * This array should be declared [FLEXIBLE_ARRAY_MEMBER], but for some
	 * reason you can't do that in an otherwise-empty struct.
	 */
	ReplicationSlot replication_slots[1];
} ReplicationSlotCtlData;

/*
 * Pointers to shared memory
 */
extern PGDLLIMPORT ReplicationSlotCtlData *ReplicationSlotCtl;
extern PGDLLIMPORT ReplicationSlot *MyReplicationSlot;

/* GUCs */
extern PGDLLIMPORT int max_replication_slots;

/* shmem initialization functions */
extern Size ReplicationSlotsShmemSize(void);
extern void ReplicationSlotsShmemInit(void);

/* management of individual slots */
extern void ReplicationSlotCreate(const char *name, bool db_specific,
					  ReplicationSlotPersistency p);
extern void ReplicationSlotPersist(void);
extern void ReplicationSlotDrop(const char *name, bool nowait);

extern void ReplicationSlotAcquire(const char *name, bool nowait);
extern void ReplicationSlotRelease(void);
extern void ReplicationSlotCleanup(void);
extern void ReplicationSlotSave(void);
extern void ReplicationSlotMarkDirty(void);

/* misc stuff */
extern bool ReplicationSlotValidateName(const char *name, int elevel);
extern void ReplicationSlotReserveWal(void);
extern void ReplicationSlotsComputeRequiredXmin(bool already_locked);
extern void ReplicationSlotsComputeRequiredLSN(void);
extern XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void);
extern bool ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive);
extern void ReplicationSlotsDropDBSlots(Oid dboid);

extern void StartupReplicationSlots(void);
extern void CheckPointReplicationSlots(void);

extern void CheckSlotRequirements(void);

extern void InvalidateObsoleteReplicationSlots(XLogSegNo oldestSegno);
extern ReplicationSlot *SearchNamedReplicationSlot(const char *name);

/* POLAR */
extern void polar_compute_and_set_oldest_apply_lsn(void);
extern int polar_repl_slots_reserved_for_superuser;
extern void polar_reload_replication_slots_from_shared_storage(void);
extern XLogRecPtr polar_set_initial_datamax_restart_lsn(ReplicationSlot *slot);
/* POLAR: end */

#endif							/* SLOT_H */
