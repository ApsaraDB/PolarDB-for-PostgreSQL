/*-------------------------------------------------------------------------
 *
 * slot.c
 *	   Replication slot management.
 *
 *
 * Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/slot.c
 *
 * NOTES
 *
 * Replication slots are used to keep state about replication streams
 * originating from this cluster.  Their primary purpose is to prevent the
 * premature removal of WAL or of old tuple versions in a manner that would
 * interfere with replication; they are also useful for monitoring purposes.
 * Slots need to be permanent (to allow restarts), crash-safe, and allocatable
 * on standbys (to support cascading setups).  The requirement that slots be
 * usable on standbys precludes storing them in the system catalogs.
 *
 * Each replication slot gets its own directory inside the $PGDATA/pg_replslot
 * directory. Inside that directory the state file will contain the slot's
 * own data. Additional data can be stored alongside that file if required.
 * While the server is running, the state data is also cached in memory for
 * efficiency.
 *
 * ReplicationSlotAllocationLock must be taken in exclusive mode to allocate
 * or free a slot. ReplicationSlotControlLock must be taken in shared mode
 * to iterate over the slots, and in exclusive mode to change the in_use flag
 * of a slot.  The remaining data in each slot is protected by its mutex.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/transam.h"
#include "access/xlog_internal.h"
#include "common/string.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/slot.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"

/* POLAR */
#include "access/polar_logindex_redo.h"
#include "polar_datamax/polar_datamax.h"
#include "replication/polar_cluster_info.h"
#include "replication/syncrep.h"
#include "storage/polar_fd.h"
#include "utils/guc.h"
/* POLAR end */

/*
 * Replication slot on-disk data structure.
 */
typedef struct ReplicationSlotOnDisk
{
	/* first part of this struct needs to be version independent */

	/* data not covered by checksum */
	uint32		magic;
	pg_crc32c	checksum;

	/* data covered by checksum */
	uint32		version;
	uint32		length;

	/*
	 * The actual data in the slot that follows can differ based on the above
	 * 'version'.
	 */

	ReplicationSlotPersistentData slotdata;
} ReplicationSlotOnDisk;

/* size of version independent data */
#define ReplicationSlotOnDiskConstantSize \
	offsetof(ReplicationSlotOnDisk, slotdata)
/* size of the part of the slot not covered by the checksum */
#define SnapBuildOnDiskNotChecksummedSize \
	offsetof(ReplicationSlotOnDisk, version)
/* size of the part covered by the checksum */
#define SnapBuildOnDiskChecksummedSize \
	sizeof(ReplicationSlotOnDisk) - SnapBuildOnDiskNotChecksummedSize
/* size of the slot data that is version dependent */
#define ReplicationSlotOnDiskV2Size \
	sizeof(ReplicationSlotOnDisk) - ReplicationSlotOnDiskConstantSize

/* POLAR */
/* size of the part covered by the checksum */
#define SnapBuildOnDiskChecksummedVersion2Size \
	offsetof(ReplicationSlotOnDisk, slotdata) + offsetof(ReplicationSlotPersistentData, polar_replica_apply_lsn) - SnapBuildOnDiskNotChecksummedSize
/* size of the slot data that is version dependent */
#define ReplicationSlotOnDiskV2Version2Size \
	offsetof(ReplicationSlotOnDisk, slotdata) + offsetof(ReplicationSlotPersistentData, polar_replica_apply_lsn) - ReplicationSlotOnDiskConstantSize

#define ReplicationSlotPersistentDataVersion2Size \
	offsetof(ReplicationSlotPersistentData, polar_replica_apply_lsn)

/* size of the part covered by the checksum */
#define SnapBuildOnDiskChecksummedVersion3Size \
	offsetof(ReplicationSlotOnDisk, slotdata) + offsetof(ReplicationSlotPersistentData, polar_invalidated_at) - SnapBuildOnDiskNotChecksummedSize
/* size of the slot data that is version dependent */
#define ReplicationSlotOnDiskV2Version3Size \
	offsetof(ReplicationSlotOnDisk, slotdata) + offsetof(ReplicationSlotPersistentData, polar_invalidated_at) - ReplicationSlotOnDiskConstantSize

#define ReplicationSlotPersistentDataVersion3Size \
	offsetof(ReplicationSlotPersistentData, polar_invalidated_at)
/* POLAR end */

#define SLOT_MAGIC		0x1051CA1	/* format identifier */
#define SLOT_VERSION	2		/* version for new files */
/* POLAR */
#define POLAR_SLOT_VERSION_V2	10002	/* version for polar customized file */
#define POLAR_SLOT_VERSION_V3	10003
/* POLAR end */

/* Control array for replication slot management */
ReplicationSlotCtlData *ReplicationSlotCtl = NULL;

/* My backend's replication slot in the shared memory array */
ReplicationSlot *MyReplicationSlot = NULL;

/* GUCs */
int			max_replication_slots = 0;	/* the maximum number of replication
										 * slots */
/* POLAR */

static void ReplicationSlotDropAcquired(void);
static void ReplicationSlotDropPtr(ReplicationSlot *slot);

/* internal persistency functions */
static void RestoreSlotFromDisk(const char *name, bool polar_vfs);
static void CreateSlotOnDisk(ReplicationSlot *slot);
static void SaveSlotToPath(ReplicationSlot *slot, const char *path, int elevel);

/* POLAR */
static void polar_restore_all_slots_from_dir(char *replslot_dir, bool load_from_vfs);

/*
 * Report shared-memory space needed by ReplicationSlotShmemInit.
 */
Size
ReplicationSlotsShmemSize(void)
{
	Size		size = 0;

	if (max_replication_slots == 0)
		return size;

	size = offsetof(ReplicationSlotCtlData, replication_slots);
	size = add_size(size,
					mul_size(max_replication_slots, sizeof(ReplicationSlot)));

	return size;
}

/*
 * Allocate and initialize shared memory for replication slots.
 */
void
ReplicationSlotsShmemInit(void)
{
	bool		found;

	if (max_replication_slots == 0)
		return;

	ReplicationSlotCtl = (ReplicationSlotCtlData *)
		ShmemInitStruct("ReplicationSlot Ctl", ReplicationSlotsShmemSize(),
						&found);

	LWLockRegisterTranche(LWTRANCHE_REPLICATION_SLOT_IO_IN_PROGRESS,
						  "replication_slot_io");

	if (!found)
	{
		int			i;

		/* First time through, so initialize */
		MemSet(ReplicationSlotCtl, 0, ReplicationSlotsShmemSize());

		for (i = 0; i < max_replication_slots; i++)
		{
			ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

			/* everything else is zeroed by the memset above */
			SpinLockInit(&slot->mutex);
			LWLockInitialize(&slot->io_in_progress_lock, LWTRANCHE_REPLICATION_SLOT_IO_IN_PROGRESS);
			ConditionVariableInit(&slot->active_cv);
		}
	}
}

/*
 * Check whether the passed slot name is valid and report errors at elevel.
 *
 * Slot names may consist out of [a-z0-9_]{1,NAMEDATALEN-1} which should allow
 * the name to be used as a directory name on every supported OS.
 *
 * Returns whether the directory name is valid or not if elevel < ERROR.
 */
bool
ReplicationSlotValidateName(const char *name, int elevel)
{
	const char *cp;

	if (strlen(name) == 0)
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("replication slot name \"%s\" is too short",
						name)));
		return false;
	}

	if (strlen(name) >= NAMEDATALEN)
	{
		ereport(elevel,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("replication slot name \"%s\" is too long",
						name)));
		return false;
	}

	for (cp = name; *cp; cp++)
	{
		if (!((*cp >= 'a' && *cp <= 'z')
			  || (*cp >= '0' && *cp <= '9')
			  || (*cp == '_')))
		{
			ereport(elevel,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("replication slot name \"%s\" contains invalid character",
							name),
					 errhint("Replication slot names may only contain lower case letters, numbers, and the underscore character.")));
			return false;
		}
	}
	return true;
}

/*
 * Create a new replication slot and mark it as used by this backend.
 *
 * name: Name of the slot
 * db_specific: logical decoding is db specific; if the slot is going to
 *	   be used for that pass true, otherwise false.
 */
void
ReplicationSlotCreate(const char *name, bool db_specific,
					  ReplicationSlotPersistency persistency)
{
	ReplicationSlot *slot = NULL;
	int			i;
	/* POLAR: count the available slots */
	int polar_replication_slots_available = 0;
	/* POLAR: end */

	Assert(MyReplicationSlot == NULL);

	ReplicationSlotValidateName(name, ERROR);

	/*
	 * If some other backend ran this code concurrently with us, we'd likely
	 * both allocate the same slot, and that would be bad.  We'd also be at
	 * risk of missing a name collision.  Also, we don't want to try to create
	 * a new slot while somebody's busy cleaning up an old one, because we
	 * might both be monkeying with the same directory.
	 */
	LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

	/*
	 * Check for name collision, and identify an allocatable slot.  We need to
	 * hold ReplicationSlotControlLock in shared mode for this, so that nobody
	 * else can change the in_use flags while we're looking at them.
	 */
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("replication slot \"%s\" already exists", name)));
		if (!s->in_use && slot == NULL)
			slot = s;
		/* POLAR: count the used slots */
		if (!s->in_use)
			polar_replication_slots_available++;
		/* POLAR: end */
	}
	LWLockRelease(ReplicationSlotControlLock);

	/* If all slots are in use, we're out of luck. */
	if (slot == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("all replication slots are in use"),
				 errhint("Free one or increase max_replication_slots.")));

	/* POLAR: limit slots use of non_super users */
	if (polar_repl_slots_reserved_for_superuser >= 0 &&
			polar_replication_slots_available <= polar_repl_slots_reserved_for_superuser &&
			!MyProc->issuper)
	{
		slot = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("replication slots of non_super users reach upper limit, current polar_replication_slots_available: %d, polar_repl_slots_reserved_for_superuser %d", polar_replication_slots_available, polar_repl_slots_reserved_for_superuser)));
	}
	/* POLAR: end */

	/* POLAR: RO store replication slot on local disk, but couldn't check same slot name on RW which store on polarstore */
	if (polar_enable_persisted_physical_slot && !db_specific)
	{
		char		path[MAXPGPATH];
		struct stat st;
		char 		replslot_dir[MAXPGPATH] = {0};

		polar_make_file_path_level2(replslot_dir, "pg_replslot");
		snprintf(path, MAXPGPATH, "%s/%s", replslot_dir, name);

		if (polar_stat(path, &st) == 0 && S_ISDIR(st.st_mode))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("replication slot \"%s\" already exists on RW", name)));
		}
	}
	/* POLAR: end */

	/*
	 * Since this slot is not in use, nobody should be looking at any part of
	 * it other than the in_use field unless they're trying to allocate it.
	 * And since we hold ReplicationSlotAllocationLock, nobody except us can
	 * be doing that.  So it's safe to initialize the slot.
	 */
	Assert(!slot->in_use);
	Assert(slot->active_pid == 0);

	/* first initialize persistent data */
	memset(&slot->data, 0, sizeof(ReplicationSlotPersistentData));
	StrNCpy(NameStr(slot->data.name), name, NAMEDATALEN);
	slot->data.database = db_specific ? MyDatabaseId : InvalidOid;
	slot->data.persistency = persistency;

	/* and then data only present in shared memory */
	slot->just_dirtied = false;
	slot->dirty = false;
	slot->effective_xmin = InvalidTransactionId;
	slot->effective_catalog_xmin = InvalidTransactionId;
	slot->candidate_catalog_xmin = InvalidTransactionId;
	slot->candidate_xmin_lsn = InvalidXLogRecPtr;
	slot->candidate_restart_valid = InvalidXLogRecPtr;
	slot->candidate_restart_lsn = InvalidXLogRecPtr;

	slot->node_name[0] = '\0';
	slot->node_host[0] = '\0';
	slot->node_port = 0;
	slot->node_release_date[0] = '\0';
	slot->node_version[0] = '\0';
	slot->node_slot_name[0] = '\0';
	slot->node_type = POLAR_UNKNOWN;
	slot->node_state = POLAR_NODE_OFFLINE;
	/* POLAR: it means we don't want send cluster info to this slot, unless the slot report node info */
	slot->polar_sent_cluster_info_generation = POLAR_CLUSTER_INFO_INVALID_GENERATION;

	/*
	 * Create the slot on disk.  We haven't actually marked the slot allocated
	 * yet, so no special cleanup is required if this errors out.
	 */
	CreateSlotOnDisk(slot);

	/*
	 * We need to briefly prevent any other backend from iterating over the
	 * slots while we flip the in_use flag. We also need to set the active
	 * flag while holding the ControlLock as otherwise a concurrent
	 * SlotAcquire() could acquire the slot as well.
	 */
	LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);

	slot->in_use = true;

	/* We can now mark the slot active, and that makes it our slot. */
	SpinLockAcquire(&slot->mutex);
	Assert(slot->active_pid == 0);
	slot->active_pid = MyProcPid;
	SpinLockRelease(&slot->mutex);
	MyReplicationSlot = slot;

	/* POLAR */
	slot->polar_replica_lock_lsn = InvalidXLogRecPtr;
	/* POLAR end */

	LWLockRelease(ReplicationSlotControlLock);

	/*
	 * Now that the slot has been marked as in_use and active, it's safe to
	 * let somebody else try to allocate a slot.
	 */
	LWLockRelease(ReplicationSlotAllocationLock);

	/* Let everybody know we've modified this slot */
	ConditionVariableBroadcast(&slot->active_cv);
}

/*
 * Find a previously created slot and mark it as used by this backend.
 */
void
ReplicationSlotAcquire(const char *name, bool nowait)
{
	ReplicationSlot *slot;
	int			active_pid;
	int			i;

retry:
	Assert(MyReplicationSlot == NULL);

	/*
	 * Search for the named slot and mark it active if we find it.  If the
	 * slot is already active, we exit the loop with active_pid set to the PID
	 * of the backend that owns it.
	 */
	active_pid = 0;
	slot = NULL;
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0)
		{
			/*
			 * This is the slot we want; check if it's active under some other
			 * process.  In single user mode, we don't need this check.
			 */
			if (IsUnderPostmaster)
			{
				/*
				 * Get ready to sleep on it in case it is active.  (We may end
				 * up not sleeping, but we don't want to do this while holding
				 * the spinlock.)
				 */
				ConditionVariablePrepareToSleep(&s->active_cv);

				SpinLockAcquire(&s->mutex);

				active_pid = s->active_pid;
				if (active_pid == 0)
					active_pid = s->active_pid = MyProcPid;

				SpinLockRelease(&s->mutex);
			}
			else
				active_pid = MyProcPid;
			slot = s;

			break;
		}
	}
	LWLockRelease(ReplicationSlotControlLock);

	/* If we did not find the slot, error out. */
	if (slot == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("replication slot \"%s\" does not exist", name)));

	/*
	 * If we found the slot but it's already active in another backend, we
	 * either error out or retry after a short wait, as caller specified.
	 */
	if (active_pid != MyProcPid)
	{
		if (nowait)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("replication slot \"%s\" is active for PID %d",
							name, active_pid)));

		/* Wait here until we get signaled, and then restart */
		ConditionVariableSleep(&slot->active_cv,
							   WAIT_EVENT_REPLICATION_SLOT_DROP);
		ConditionVariableCancelSleep();
		goto retry;
	}
	else
		ConditionVariableCancelSleep(); /* no sleep needed after all */

	/* Let everybody know we've modified this slot */
	ConditionVariableBroadcast(&slot->active_cv);

	/* We made this slot active, so it's ours now. */
	MyReplicationSlot = slot;
}

/*
 * Release the replication slot that this backend considers to own.
 *
 * This or another backend can re-acquire the slot later.
 * Resources this slot requires will be preserved.
 */
void
ReplicationSlotRelease(void)
{
	ReplicationSlot *slot = MyReplicationSlot;
	bool	update_cluster_info;

	Assert(slot != NULL && slot->active_pid != 0);

	/* POLAR: slot data will be reset, save it */
	update_cluster_info = SlotIsPhysical(slot);

	if (slot->data.persistency == RS_EPHEMERAL)
	{
		/*
		 * Delete the slot. There is no !PANIC case where this is allowed to
		 * fail, all that may happen is an incomplete cleanup of the on-disk
		 * data.
		 */
		ReplicationSlotDropAcquired();
	}

	/*
	 * If slot needed to temporarily restrain both data and catalog xmin to
	 * create the catalog snapshot, remove that temporary constraint.
	 * Snapshots can only be exported while the initial snapshot is still
	 * acquired.
	 */
	if (!TransactionIdIsValid(slot->data.xmin) &&
		TransactionIdIsValid(slot->effective_xmin))
	{
		SpinLockAcquire(&slot->mutex);
		slot->effective_xmin = InvalidTransactionId;
		SpinLockRelease(&slot->mutex);
		ReplicationSlotsComputeRequiredXmin(false);
	}

	if (slot->data.persistency == RS_PERSISTENT)
	{
		/*
		 * Mark persistent slot inactive.  We're not freeing it, just
		 * disconnecting, but wake up others that may be waiting for it.
		 */
		SpinLockAcquire(&slot->mutex);
		slot->active_pid = 0;
		SpinLockRelease(&slot->mutex);
		ConditionVariableBroadcast(&slot->active_cv);
	}

	MyReplicationSlot = NULL;

	/* might not have been set when we've been a plain slot */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags &= ~PROC_IN_LOGICAL_DECODING;
	LWLockRelease(ProcArrayLock);

	if (update_cluster_info)
	{
		/* POLAR: slot is down, update cluster info generation */
		elog(LOG, "release slot");
		polar_update_cluster_info();
	}
}

/*
 * Cleanup all temporary slots created in current session.
 */
void
ReplicationSlotCleanup(void)
{
	int			i;

	Assert(MyReplicationSlot == NULL);

restart:
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (!s->in_use)
			continue;

		SpinLockAcquire(&s->mutex);
		if (s->active_pid == MyProcPid)
		{
			Assert(s->data.persistency == RS_TEMPORARY);
			SpinLockRelease(&s->mutex);
			LWLockRelease(ReplicationSlotControlLock);	/* avoid deadlock */

			ReplicationSlotDropPtr(s);

			ConditionVariableBroadcast(&s->active_cv);
			goto restart;
		}
		else
			SpinLockRelease(&s->mutex);
	}

	LWLockRelease(ReplicationSlotControlLock);
}

/*
 * Permanently drop replication slot identified by the passed in name.
 */
void
ReplicationSlotDrop(const char *name, bool nowait)
{
	Assert(MyReplicationSlot == NULL);

	ReplicationSlotAcquire(name, nowait);

	ReplicationSlotDropAcquired();
}

/*
 * Permanently drop the currently acquired replication slot.
 */
static void
ReplicationSlotDropAcquired(void)
{
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(MyReplicationSlot != NULL);

	/* slot isn't acquired anymore */
	MyReplicationSlot = NULL;

	ReplicationSlotDropPtr(slot);

	/* POLAR: compute the oldest apply lsn among all replication slots and set it. */
	polar_compute_and_set_oldest_apply_lsn();
}

/*
 * Permanently drop the replication slot which will be released by the point
 * this function returns.
 */
static void
ReplicationSlotDropPtr(ReplicationSlot *slot)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];

	/* POLAR */
	char		replslot_dir[MAXPGPATH] = {0};
	char		local_slot_path[MAXPGPATH] = {0};

	/*
	 * If some other backend ran this code concurrently with us, we might try
	 * to delete a slot with a certain name while someone else was trying to
	 * create a slot with the same name.
	 */
	LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

	/* Generate pathnames. */
	snprintf(replslot_dir, MAXPGPATH, "%s", "pg_replslot");
	if (POLAR_PERSIST_LOGICAL_SLOT(slot) || POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(slot))
		polar_make_file_path_level2(replslot_dir, "pg_replslot");

	snprintf(path, MAXPGPATH, "%s/%s", replslot_dir, NameStr(slot->data.name));
	snprintf(tmppath, MAXPGPATH, "%s/%s.tmp", replslot_dir, NameStr(slot->data.name));
	snprintf(local_slot_path, MAXPGPATH, "%s/%s", "pg_replslot", NameStr(slot->data.name));

	/*
	 * Rename the slot directory on disk, so that we'll no longer recognize
	 * this as a valid slot.  Note that if this fails, we've got to mark the
	 * slot inactive before bailing out.  If we're dropping an ephemeral or a
	 * temporary slot, we better never fail hard as the caller won't expect
	 * the slot to survive and this might get called during error handling.
	 */
	if (polar_rename(path, tmppath) == 0)
	{
		/*
		 * We need to fsync() the directory we just renamed and its parent to
		 * make sure that our changes are on disk in a crash-safe fashion.  If
		 * fsync() fails, we can't be sure whether the changes are on disk or
		 * not.  For now, we handle that by panicking;
		 * StartupReplicationSlots() will try to straighten it out after
		 * restart.
		 */
		START_CRIT_SECTION();
		fsync_fname(tmppath, true, true);
		fsync_fname(replslot_dir, true, true);
		END_CRIT_SECTION();
	}
	else
	{
		bool		fail_softly = slot->data.persistency != RS_PERSISTENT;

		SpinLockAcquire(&slot->mutex);
		slot->active_pid = 0;
		SpinLockRelease(&slot->mutex);

		/* wake up anyone waiting on this slot */
		ConditionVariableBroadcast(&slot->active_cv);

		ereport(fail_softly ? WARNING : ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						path, tmppath)));
	}

	/*
	 * The slot is definitely gone.  Lock out concurrent scans of the array
	 * long enough to kill it.  It's OK to clear the active PID here without
	 * grabbing the mutex because nobody else can be scanning the array here,
	 * and nobody can be attached to this slot and thus access it without
	 * scanning the array.
	 *
	 * Also wake up processes waiting for it.
	 */
	LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
	slot->active_pid = 0;
	slot->in_use = false;
	LWLockRelease(ReplicationSlotControlLock);
	ConditionVariableBroadcast(&slot->active_cv);

	/*
	 * Slot is dead and doesn't prevent resource removal anymore, recompute
	 * limits.
	 */
	ReplicationSlotsComputeRequiredXmin(false);
	ReplicationSlotsComputeRequiredLSN();

	/* POLAR: remove local slot path which may store spilled files*/
	if (POLAR_PERSIST_LOGICAL_SLOT(slot))
	{
		if (!rmtree(local_slot_path, true))
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove directory \"%s\" for spill file", local_slot_path)));
	}

	/*
	 * If removing the directory fails, the worst thing that will happen is
	 * that the user won't be able to create a new slot with the same name
	 * until the next server restart.  We warn about it, but that's all.
	 */
	if (!rmtree(tmppath, true))
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\"", tmppath)));

	/*
	 * We release this at the very end, so that nobody starts trying to create
	 * a slot while we're still cleaning up the detritus of the old one.
	 */
	LWLockRelease(ReplicationSlotAllocationLock);

	/*
	 * POLAR: After dropping slot, call polar_release_ddl_waiters to
	 * wake apply queue. In case of no WalSender process active, no process will
	 * call polar_release_ddl_waiters. So backend will wait permanently. Here we
	 * solve this problem by calling polar_release_ddl_waiters after each
	 * dropping slot. When none of slots is defined, polar_release_ddl_waiters
	 * will wake the whole apply queue.
	 */
    if (!polar_release_ddl_waiters())
    {
        ereport(LOG, (errmsg("No replica replication slot exists, so we wake all backend.")));
    }
}

/*
 * Serialize the currently acquired slot's state from memory to disk, thereby
 * guaranteeing the current state will survive a crash.
 */
void
ReplicationSlotSave(void)
{
	char		path[MAXPGPATH];

	Assert(MyReplicationSlot != NULL);

	sprintf(path, "pg_replslot/%s", NameStr(MyReplicationSlot->data.name));
	if (POLAR_PERSIST_LOGICAL_SLOT(MyReplicationSlot) ||
			POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(MyReplicationSlot))
		polar_make_file_path_level3(path, "pg_replslot", NameStr(MyReplicationSlot->data.name));
	SaveSlotToPath(MyReplicationSlot, path, ERROR);
}

/*
 * Signal that it would be useful if the currently acquired slot would be
 * flushed out to disk.
 *
 * Note that the actual flush to disk can be delayed for a long time, if
 * required for correctness explicitly do a ReplicationSlotSave().
 */
void
ReplicationSlotMarkDirty(void)
{
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(MyReplicationSlot != NULL);

	SpinLockAcquire(&slot->mutex);
	MyReplicationSlot->just_dirtied = true;
	MyReplicationSlot->dirty = true;
	SpinLockRelease(&slot->mutex);
}

/*
 * Convert a slot that's marked as RS_EPHEMERAL to a RS_PERSISTENT slot,
 * guaranteeing it will be there after an eventual crash.
 */
void
ReplicationSlotPersist(void)
{
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(slot != NULL);
	Assert(slot->data.persistency != RS_PERSISTENT);

	SpinLockAcquire(&slot->mutex);
	slot->data.persistency = RS_PERSISTENT;
	SpinLockRelease(&slot->mutex);

	ReplicationSlotMarkDirty();
	ReplicationSlotSave();
}

/*
 * Compute the oldest xmin across all slots and store it in the ProcArray.
 *
 * If already_locked is true, ProcArrayLock has already been acquired
 * exclusively.
 */
void
ReplicationSlotsComputeRequiredXmin(bool already_locked)
{
	int			i;
	TransactionId agg_xmin = InvalidTransactionId;
	TransactionId agg_catalog_xmin = InvalidTransactionId;

	Assert(ReplicationSlotCtl != NULL);

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
		TransactionId effective_xmin;
		TransactionId effective_catalog_xmin;

		if (!s->in_use)
			continue;

		SpinLockAcquire(&s->mutex);
		effective_xmin = s->effective_xmin;
		effective_catalog_xmin = s->effective_catalog_xmin;
		SpinLockRelease(&s->mutex);

		/* check the data xmin */
		if (TransactionIdIsValid(effective_xmin) &&
			(!TransactionIdIsValid(agg_xmin) ||
			 TransactionIdPrecedes(effective_xmin, agg_xmin)))
			agg_xmin = effective_xmin;

		/* check the catalog xmin */
		if (TransactionIdIsValid(effective_catalog_xmin) &&
			(!TransactionIdIsValid(agg_catalog_xmin) ||
			 TransactionIdPrecedes(effective_catalog_xmin, agg_catalog_xmin)))
			agg_catalog_xmin = effective_catalog_xmin;
	}

	LWLockRelease(ReplicationSlotControlLock);

	ProcArraySetReplicationSlotXmin(agg_xmin, agg_catalog_xmin, already_locked);
}

/*
 * Compute the oldest restart LSN across all slots and inform xlog module.
 */
void
ReplicationSlotsComputeRequiredLSN(void)
{
	int			i;
	XLogRecPtr	min_required = InvalidXLogRecPtr;

	Assert(ReplicationSlotCtl != NULL);

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
		XLogRecPtr	restart_lsn;
		XLogRecPtr 	replay_lsn;

		if (!s->in_use)
			continue;

		SpinLockAcquire(&s->mutex);
		restart_lsn = s->data.restart_lsn;
		/* POLAR: Add replayed lsn compare */
		replay_lsn = s->data.polar_replica_apply_lsn;
		/* POLAR end */
		SpinLockRelease(&s->mutex);

		if (restart_lsn != InvalidXLogRecPtr &&
			(min_required == InvalidXLogRecPtr ||
			 restart_lsn < min_required))
			min_required = restart_lsn;

		/* POLAR: Add replayed lsn compare */
		if (replay_lsn != InvalidXLogRecPtr &&
			(min_required == InvalidXLogRecPtr ||
			 replay_lsn < min_required))
			min_required = replay_lsn;
		/* POLAR end */
	}
	LWLockRelease(ReplicationSlotControlLock);

	XLogSetReplicationSlotMinimumLSN(min_required);
}

/*
 * Compute the oldest WAL LSN required by *logical* decoding slots..
 *
 * Returns InvalidXLogRecPtr if logical decoding is disabled or no logical
 * slots exist.
 *
 * NB: this returns a value >= ReplicationSlotsComputeRequiredLSN(), since it
 * ignores physical replication slots.
 *
 * The results aren't required frequently, so we don't maintain a precomputed
 * value like we do for ComputeRequiredLSN() and ComputeRequiredXmin().
 */
XLogRecPtr
ReplicationSlotsComputeLogicalRestartLSN(void)
{
	XLogRecPtr	result = InvalidXLogRecPtr;
	int			i;

	if (max_replication_slots <= 0)
		return InvalidXLogRecPtr;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s;
		XLogRecPtr	restart_lsn;

		s = &ReplicationSlotCtl->replication_slots[i];

		/* cannot change while ReplicationSlotCtlLock is held */
		if (!s->in_use)
			continue;

		/* we're only interested in logical slots */
		if (!SlotIsLogical(s))
			continue;

		/* read once, it's ok if it increases while we're checking */
		SpinLockAcquire(&s->mutex);
		restart_lsn = s->data.restart_lsn;
		SpinLockRelease(&s->mutex);

		if (result == InvalidXLogRecPtr ||
			restart_lsn < result)
			result = restart_lsn;
	}

	LWLockRelease(ReplicationSlotControlLock);

	return result;
}

/*
 * ReplicationSlotsCountDBSlots -- count the number of slots that refer to the
 * passed database oid.
 *
 * Returns true if there are any slots referencing the database. *nslots will
 * be set to the absolute number of slots in the database, *nactive to ones
 * currently active.
 */
bool
ReplicationSlotsCountDBSlots(Oid dboid, int *nslots, int *nactive)
{
	int			i;

	*nslots = *nactive = 0;

	if (max_replication_slots <= 0)
		return false;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s;

		s = &ReplicationSlotCtl->replication_slots[i];

		/* cannot change while ReplicationSlotCtlLock is held */
		if (!s->in_use)
			continue;

		/* only logical slots are database specific, skip */
		if (!SlotIsLogical(s))
			continue;

		/* not our database, skip */
		if (s->data.database != dboid)
			continue;

		/* count slots with spinlock held */
		SpinLockAcquire(&s->mutex);
		(*nslots)++;
		if (s->active_pid != 0)
			(*nactive)++;
		SpinLockRelease(&s->mutex);
	}
	LWLockRelease(ReplicationSlotControlLock);

	if (*nslots > 0)
		return true;
	return false;
}

/*
 * ReplicationSlotsDropDBSlots -- Drop all db-specific slots relating to the
 * passed database oid. The caller should hold an exclusive lock on the
 * pg_database oid for the database to prevent creation of new slots on the db
 * or replay from existing slots.
 *
 * Another session that concurrently acquires an existing slot on the target DB
 * (most likely to drop it) may cause this function to ERROR. If that happens
 * it may have dropped some but not all slots.
 *
 * This routine isn't as efficient as it could be - but we don't drop
 * databases often, especially databases with lots of slots.
 */
void
ReplicationSlotsDropDBSlots(Oid dboid)
{
	int			i;

	if (max_replication_slots <= 0)
		return;

restart:
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s;
		char	   *slotname;
		int			active_pid;

		s = &ReplicationSlotCtl->replication_slots[i];

		/* cannot change while ReplicationSlotCtlLock is held */
		if (!s->in_use)
			continue;

		/* only logical slots are database specific, skip */
		if (!SlotIsLogical(s))
			continue;

		/* not our database, skip */
		if (s->data.database != dboid)
			continue;

		/* acquire slot, so ReplicationSlotDropAcquired can be reused  */
		SpinLockAcquire(&s->mutex);
		/* can't change while ReplicationSlotControlLock is held */
		slotname = NameStr(s->data.name);
		active_pid = s->active_pid;
		if (active_pid == 0)
		{
			MyReplicationSlot = s;
			s->active_pid = MyProcPid;
		}
		SpinLockRelease(&s->mutex);

		/*
		 * Even though we hold an exclusive lock on the database object a
		 * logical slot for that DB can still be active, e.g. if it's
		 * concurrently being dropped by a backend connected to another DB.
		 *
		 * That's fairly unlikely in practice, so we'll just bail out.
		 */
		if (active_pid)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("replication slot \"%s\" is active for PID %d",
							slotname, active_pid)));

		/*
		 * To avoid duplicating ReplicationSlotDropAcquired() and to avoid
		 * holding ReplicationSlotControlLock over filesystem operations,
		 * release ReplicationSlotControlLock and use
		 * ReplicationSlotDropAcquired.
		 *
		 * As that means the set of slots could change, restart scan from the
		 * beginning each time we release the lock.
		 */
		LWLockRelease(ReplicationSlotControlLock);
		ReplicationSlotDropAcquired();
		goto restart;
	}
	LWLockRelease(ReplicationSlotControlLock);
}


/*
 * Check whether the server's configuration supports using replication
 * slots.
 */
void
CheckSlotRequirements(void)
{
	/*
	 * NB: Adding a new requirement likely means that RestoreSlotFromDisk()
	 * needs the same check.
	 */

	if (max_replication_slots == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("replication slots can only be used if max_replication_slots > 0"))));

	if (wal_level < WAL_LEVEL_REPLICA)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slots can only be used if wal_level >= replica")));
}

/*
 * Reserve WAL for the currently active slot.
 *
 * Compute and set restart_lsn in a manner that's appropriate for the type of
 * the slot and concurrency safe.
 */
void
ReplicationSlotReserveWal(void)
{
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(slot != NULL);
	Assert(slot->data.restart_lsn == InvalidXLogRecPtr);

	/*
	 * The replication slot mechanism is used to prevent removal of required
	 * WAL. As there is no interlock between this routine and checkpoints, WAL
	 * segments could concurrently be removed when a now stale return value of
	 * ReplicationSlotsComputeRequiredLSN() is used. In the unlikely case that
	 * this happens we'll just retry.
	 */
	while (true)
	{
		XLogSegNo	segno;
		XLogRecPtr	restart_lsn;

		/*
		 * For logical slots log a standby snapshot and start logical decoding
		 * at exactly that position. That allows the slot to start up more
		 * quickly.
		 *
		 * That's not needed (or indeed helpful) for physical slots as they'll
		 * start replay at the last logged checkpoint anyway. Instead return
		 * the location of the last redo LSN. While that slightly increases
		 * the chance that we have to retry, it's where a base backup has to
		 * start replay at.
		 */
		if (!RecoveryInProgress() && SlotIsLogical(slot))
		{
			XLogRecPtr	flushptr;

			/* start at current insert position */
			restart_lsn = GetXLogInsertRecPtr();
			SpinLockAcquire(&slot->mutex);
			slot->data.restart_lsn = restart_lsn;
			SpinLockRelease(&slot->mutex);

			/* make sure we have enough information to start */
			flushptr = LogStandbySnapshot();

			/* and make sure it's fsynced to disk */
			XLogFlush(flushptr);
		}
		else
		{
			/* 
			 * POLAR: set restart lsn as the min received lsn when current node is datamax
			 * so that we can send wal as much as possible 
			 */ 
			if (!polar_is_datamax())
				restart_lsn = GetRedoRecPtr();
			else
				restart_lsn = polar_datamax_get_min_received_lsn(polar_datamax_ctl, NULL);
			SpinLockAcquire(&slot->mutex);
			slot->data.restart_lsn = restart_lsn;
			SpinLockRelease(&slot->mutex);
		}

		/* prevent WAL removal as fast as possible */
		ReplicationSlotsComputeRequiredLSN();

		/*
		 * If all required WAL is still there, great, otherwise retry. The
		 * slot should prevent further removal of WAL, unless there's a
		 * concurrent ReplicationSlotsComputeRequiredLSN() after we've written
		 * the new restart_lsn above, so normally we should never need to loop
		 * more than twice.
		 */
		XLByteToSeg(slot->data.restart_lsn, segno, wal_segment_size);
		if (XLogGetLastRemovedSegno() < segno)
			break;
	}
}

/*
 * Flush all replication slots to disk.
 *
 * This needn't actually be part of a checkpoint, but it's a convenient
 * location.
 */
void
CheckPointReplicationSlots(void)
{
	int			i;

	elog(DEBUG1, "performing replication slot checkpoint");

	/*
	 * Prevent any slot from being created/dropped while we're active. As we
	 * explicitly do *not* want to block iterating over replication_slots or
	 * acquiring a slot we cannot take the control lock - but that's OK,
	 * because holding ReplicationSlotAllocationLock is strictly stronger, and
	 * enough to guarantee that nobody can change the in_use bits on us.
	 */
	LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);

	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
		char		path[MAXPGPATH];

		if (!s->in_use)
			continue;

		/* save the slot to disk, locking is handled in SaveSlotToPath() */
		sprintf(path, "pg_replslot/%s", NameStr(s->data.name));
		if (POLAR_PERSIST_LOGICAL_SLOT(s) || POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(s))
			polar_make_file_path_level3(path, "pg_replslot", NameStr(s->data.name));

		SaveSlotToPath(s, path, LOG);
	}
	LWLockRelease(ReplicationSlotAllocationLock);
}

/*
 * Load all replication slots from disk into memory at server startup. This
 * needs to be run before we start crash recovery.
 */
void
StartupReplicationSlots(void)
{
	/* POLAR */
	char		polar_replslot_dir[MAXPGPATH] = {0};
	struct stat stat_buf;

	elog(DEBUG1, "starting up replication slots");

	if (POLAR_RESTORE_PERSISTED_LOGICAL_SLOT || POLAR_RESTORE_PERSISTED_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE)
	{
		elog(LOG, "enable persisted slot, read slot from polarstore.");
		polar_make_file_path_level2(polar_replslot_dir, "pg_replslot");
		if (polar_stat(polar_replslot_dir, &stat_buf) == 0)
		{
			/* Check for weird cases where it exists but isn't a directory */
			if (!S_ISDIR(stat_buf.st_mode))
				ereport(FATAL,
						(errmsg("required pg_replslot directory \"%s\" does not exists",
								polar_replslot_dir)));
		}
		else
		{
			ereport(LOG,
					(errmsg("creating missing pg_replslot directory \"%s\"", polar_replslot_dir)));
			if (polar_make_pg_directory(polar_replslot_dir) < 0)
				ereport(FATAL,
						(errmsg("could not create missing directory \"%s\": %m",
								polar_replslot_dir)));
		}
	}

	/* 
	 * POLAR: We have moved the previous restore logic to 
	 * the polar_restore_all_slots_from_dir function,
	 * because we may have to restore slots from both local disk and polarstore.
	 */
	polar_restore_all_slots_from_dir("pg_replslot", false);
	if (POLAR_RESTORE_PERSISTED_LOGICAL_SLOT || POLAR_RESTORE_PERSISTED_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE)
		polar_restore_all_slots_from_dir(polar_replslot_dir, true);

	/* currently no slots exist, we're done. */
	if (max_replication_slots <= 0)
		return;

	/* Now that we have recovered all the data, compute replication xmin */
	ReplicationSlotsComputeRequiredXmin(false);
	ReplicationSlotsComputeRequiredLSN();
}

/* ----
 * Manipulation of on-disk state of replication slots
 *
 * NB: none of the routines below should take any notice whether a slot is the
 * current one or not, that's all handled a layer above.
 * ----
 */
static void
CreateSlotOnDisk(ReplicationSlot *slot)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	struct stat st;

	/* POLAR */
	char 		replslot_dir[MAXPGPATH] = {0};
	char		local_slot_path[MAXPGPATH] = {0};
	struct stat st2;

	/*
	 * No need to take out the io_in_progress_lock, nobody else can see this
	 * slot yet, so nobody else will write. We're reusing SaveSlotToPath which
	 * takes out the lock, if we'd take the lock here, we'd deadlock.
	 */

	snprintf(replslot_dir, MAXPGPATH, "%s", "pg_replslot");
	if (POLAR_PERSIST_LOGICAL_SLOT(slot) || POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(slot))
		polar_make_file_path_level2(replslot_dir, "pg_replslot");

	snprintf(path, MAXPGPATH, "%s/%s", replslot_dir, NameStr(slot->data.name));
	snprintf(tmppath, MAXPGPATH, "%s/%s.tmp", replslot_dir, NameStr(slot->data.name));
	snprintf(local_slot_path, MAXPGPATH, "%s/%s", "pg_replslot", NameStr(slot->data.name));

	/*
	 * It's just barely possible that some previous effort to create or drop a
	 * slot with this name left a temp directory lying around. If that seems
	 * to be the case, try to remove it.  If the rmtree() fails, we'll error
	 * out at the MakePGDirectory() below, so we don't bother checking
	 * success.
	 */
	if (polar_stat(tmppath, &st) == 0 && S_ISDIR(st.st_mode))
	{
		if (POLAR_PERSIST_LOGICAL_SLOT(slot))
		{
			if (polar_stat(local_slot_path, &st2) == 0 && S_ISDIR(st2.st_mode))
				rmtree(local_slot_path, true);
		}
		rmtree(tmppath, true);
	}

	/* Create and fsync the temporary slot directory. */
	if (polar_make_pg_directory(tmppath) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m",
						tmppath)));
	fsync_fname(tmppath, true, true);

	/* POLAR: create slot directory on local disk for spilled files if need*/
	if (POLAR_PERSIST_LOGICAL_SLOT(slot))
	{
		if (polar_make_pg_directory(local_slot_path) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\" for spill file: %m", local_slot_path)));
		fsync_fname(local_slot_path, true, true);
	}

	/* Write the actual state file. */
	slot->dirty = true;			/* signal that we really need to write */
	SaveSlotToPath(slot, tmppath, ERROR);

	/* Rename the directory into place. */
	if (polar_rename(tmppath, path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));

	/*
	 * If we'd now fail - really unlikely - we wouldn't know whether this slot
	 * would persist after an OS crash or not - so, force a restart. The
	 * restart would try to fsync this again till it works.
	 */
	START_CRIT_SECTION();

	fsync_fname(path, true, true);
	fsync_fname(replslot_dir, true, true);

	END_CRIT_SECTION();
}

/*
 * Shared functionality between saving and creating a replication slot.
 */
static void
SaveSlotToPath(ReplicationSlot *slot, const char *dir, int elevel)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	ReplicationSlotOnDisk cp;
	bool		was_dirty;
	size_t		snap_build_ondisk_checksummed_size;

	/* POLAR */
	char		replslot_dir[MAXPGPATH] = {0};

	/* first check whether there's something to write out */
	SpinLockAcquire(&slot->mutex);
	was_dirty = slot->dirty;
	slot->just_dirtied = false;
	SpinLockRelease(&slot->mutex);

	/* and don't do anything if there's nothing to write */
	if (!was_dirty)
		return;

	LWLockAcquire(&slot->io_in_progress_lock, LW_EXCLUSIVE);

	/* silence valgrind :( */
	memset(&cp, 0, sizeof(ReplicationSlotOnDisk));

	sprintf(tmppath, "%s/state.tmp", dir);
	sprintf(path, "%s/state", dir);

	fd = polar_open_transient_file(tmppath, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
	if (fd < 0)
	{
		/*
		 * If not an ERROR, then release the lock before returning.  In case
		 * of an ERROR, the error recovery path automatically releases the
		 * lock, but no harm in explicitly releasing even in that case.  Note
		 * that LWLockRelease() could affect errno.
		 */
		int			save_errno = errno;

		LWLockRelease(&slot->io_in_progress_lock);
		errno = save_errno;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						tmppath)));
		return;
	}

	cp.magic = SLOT_MAGIC;
	INIT_CRC32C(cp.checksum);
	/* POLAR */
	if (!polar_enable_max_slot_wal_keep_size)
	{
		cp.version = POLAR_SLOT_VERSION_V2;
		cp.length = ReplicationSlotOnDiskV2Version3Size;
		snap_build_ondisk_checksummed_size = SnapBuildOnDiskChecksummedVersion3Size;
	}
	else
	{
		cp.version = POLAR_SLOT_VERSION_V3;
		cp.length = ReplicationSlotOnDiskV2Size;
		snap_build_ondisk_checksummed_size = SnapBuildOnDiskChecksummedSize;
	}
	/* POLAR end */

	SpinLockAcquire(&slot->mutex);

	memcpy(&cp.slotdata, &slot->data, sizeof(ReplicationSlotPersistentData));

	SpinLockRelease(&slot->mutex);

	COMP_CRC32C(cp.checksum,
				(char *) (&cp) + SnapBuildOnDiskNotChecksummedSize,
				snap_build_ondisk_checksummed_size);
	FIN_CRC32C(cp.checksum);

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_WRITE);
	if ((polar_write(fd, &cp, sizeof(cp))) != sizeof(cp))
	{
		int			save_errno = errno;

		pgstat_report_wait_end();
		CloseTransientFile(fd);
		LWLockRelease(&slot->io_in_progress_lock);

		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						tmppath)));
		return;
	}
	pgstat_report_wait_end();

	/* fsync the temporary file */
	pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_SYNC);
	if (polar_fsync(fd) != 0)
	{
		int			save_errno = errno;

		pgstat_report_wait_end();
		CloseTransientFile(fd);
		LWLockRelease(&slot->io_in_progress_lock);
		errno = save_errno;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						tmppath)));
		return;
	}
	pgstat_report_wait_end();

	CloseTransientFile(fd);

	/* rename to permanent file, fsync file and directory */
	if (polar_rename(tmppath, path) != 0)
	{
		int			save_errno = errno;

		LWLockRelease(&slot->io_in_progress_lock);
		errno = save_errno;
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));
		return;
	}

	/* Check CreateSlot() for the reasoning of using a crit. section. */
	START_CRIT_SECTION();

	fsync_fname(path, false, true);
	fsync_fname(dir, true, true);

	snprintf(replslot_dir, MAXPGPATH, "%s", "pg_replslot");
	if (POLAR_PERSIST_LOGICAL_SLOT(slot) || POLAR_PERSIST_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE(slot))
		polar_make_file_path_level2(replslot_dir, "pg_replslot");
	fsync_fname(replslot_dir, true, true);

	END_CRIT_SECTION();

	/*
	 * Successfully wrote, unset dirty bit, unless somebody dirtied again
	 * already.
	 */
	SpinLockAcquire(&slot->mutex);
	if (!slot->just_dirtied)
		slot->dirty = false;
	SpinLockRelease(&slot->mutex);

	LWLockRelease(&slot->io_in_progress_lock);
}

/*
 * Load a single slot from disk into memory.
 */
static void
RestoreSlotFromDisk(const char *name, bool polar_vfs)
{
	ReplicationSlotOnDisk cp;
	int			i;
	char		slotdir[MAXPGPATH + 12];
	char		path[MAXPGPATH + 22];
	int			fd;
	bool		restored = false;
	int			readBytes;
	pg_crc32c	checksum;

	/* POLAR */
	size_t		snap_build_ondisk_checksummed_size;
	size_t		replication_slot_ondisk_size;
	size_t		replication_slot_persistent_data_size;
	char 		replslot_dir[MAXPGPATH] = {0};
	char		polar_replslot_dir[MAXPGPATH] = {0};
	char		polar_slotdir[MAXPGPATH + 12] = {0};
	char		polar_path[MAXPGPATH + 22] = {0};
	char		local_slot_path[MAXPGPATH] = {0};
	struct stat st;

	/* no need to lock here, no concurrent access allowed yet */

	/* delete temp file if it exists */
	snprintf(replslot_dir, MAXPGPATH, "%s", "pg_replslot");
	if (polar_vfs)
		polar_make_file_path_level2(replslot_dir, "pg_replslot");

	sprintf(slotdir, "%s/%s", replslot_dir, name);
	sprintf(path, "%s/state.tmp", slotdir);
	sprintf(local_slot_path, "%s/%s", "pg_replslot", name);
	if (polar_unlink(path) < 0 && errno != ENOENT)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m", path)));

	sprintf(path, "%s/state", slotdir);

	if (polar_stat(path, &st) < 0 && errno == ENOENT)
	{
		elog(LOG, "Did not find stat file: %s, skip this slot.", path);
		return;
	}

	elog(DEBUG1, "restoring replication slot from \"%s\"", path);

	fd = polar_open_transient_file(path, O_RDWR | PG_BINARY);

	/*
	 * We do not need to handle this as we are rename()ing the directory into
	 * place only after we fsync()ed the state file.
	 */
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/*
	 * Sync state file before we're reading from it. We might have crashed
	 * while it wasn't synced yet and we shouldn't continue on that basis.
	 */
	pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC);
	if (polar_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						path)));
	}
	pgstat_report_wait_end();

	/* Also sync the parent directory */
	START_CRIT_SECTION();
	fsync_fname(slotdir, true, true);
	END_CRIT_SECTION();

	/* read part of statefile that's guaranteed to be version independent */
	pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_READ);
	readBytes = polar_read(fd, &cp, ReplicationSlotOnDiskConstantSize);
	pgstat_report_wait_end();
	if (readBytes != ReplicationSlotOnDiskConstantSize)
	{
		int			saved_errno = errno;

		CloseTransientFile(fd);
		errno = saved_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %u: %m",
						path, readBytes,
						(uint32) ReplicationSlotOnDiskConstantSize)));
	}

	/* verify magic */
	if (cp.magic != SLOT_MAGIC)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has wrong magic number: %u instead of %u",
						path, cp.magic, SLOT_MAGIC)));

	/* verify version */
	/* POLAR: Judge polar customized version number also */
	if (cp.version != SLOT_VERSION && 
		cp.version != POLAR_SLOT_VERSION_V2 && 
		cp.version != POLAR_SLOT_VERSION_V3)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has unsupported version %u",
						path, cp.version)));

	/* POLAR: Set size variable */
	if (cp.version == SLOT_VERSION)
	{
		snap_build_ondisk_checksummed_size = SnapBuildOnDiskChecksummedVersion2Size;
		replication_slot_ondisk_size = ReplicationSlotOnDiskV2Version2Size;
		replication_slot_persistent_data_size = ReplicationSlotPersistentDataVersion2Size;
	}
	else if (cp.version == POLAR_SLOT_VERSION_V2)
	{
		snap_build_ondisk_checksummed_size = SnapBuildOnDiskChecksummedVersion3Size;
		replication_slot_ondisk_size = ReplicationSlotOnDiskV2Version3Size;
		replication_slot_persistent_data_size = ReplicationSlotPersistentDataVersion3Size;
	}
	else
	{
		snap_build_ondisk_checksummed_size = SnapBuildOnDiskChecksummedSize;
		replication_slot_ondisk_size = ReplicationSlotOnDiskV2Size;
		replication_slot_persistent_data_size = sizeof(ReplicationSlotPersistentData);
	}
	/* POLAR end */

	/* boundary check on length */
	if (cp.length != replication_slot_ondisk_size)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("replication slot file \"%s\" has corrupted length %u",
						path, cp.length)));

	/* Now that we know the size, read the entire file */
	pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_READ);
	readBytes = polar_read(fd,
						   (char *) &cp + ReplicationSlotOnDiskConstantSize,
						   cp.length);
	pgstat_report_wait_end();
	if (readBytes != cp.length)
	{
		int			saved_errno = errno;

		CloseTransientFile(fd);
		errno = saved_errno;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\", read %d of %u: %m",
						path, readBytes, cp.length)));
	}

	CloseTransientFile(fd);

	/* now verify the CRC */
	INIT_CRC32C(checksum);
	COMP_CRC32C(checksum,
				(char *) &cp + SnapBuildOnDiskNotChecksummedSize,
				snap_build_ondisk_checksummed_size);
	FIN_CRC32C(checksum);

	if (!EQ_CRC32C(checksum, cp.checksum))
		ereport(PANIC,
				(errmsg("checksum mismatch for replication slot file \"%s\": is %u, should be %u",
						path, checksum, cp.checksum)));

	/*
	 * If we crashed with an ephemeral slot active, don't restore but delete
	 * it.
	 */
	if (cp.slotdata.persistency != RS_PERSISTENT)
	{
		if (!rmtree(slotdir, true))
		{
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not remove directory \"%s\"", slotdir)));
		}
		fsync_fname(replslot_dir, true, true);
		return;
	}

	/*
	 * Verify that requirements for the specific slot type are met. That's
	 * important because if these aren't met we're not guaranteed to retain
	 * all the necessary resources for the slot.
	 *
	 * NB: We have to do so *after* the above checks for ephemeral slots,
	 * because otherwise a slot that shouldn't exist anymore could prevent
	 * restarts.
	 *
	 * NB: Changing the requirements here also requires adapting
	 * CheckSlotRequirements() and CheckLogicalDecodingRequirements().
	 */
	if (cp.slotdata.database != InvalidOid && wal_level < WAL_LEVEL_LOGICAL)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("logical replication slot \"%s\" exists, but wal_level < logical",
						NameStr(cp.slotdata.name)),
				 errhint("Change wal_level to be logical or higher.")));
	else if (wal_level < WAL_LEVEL_REPLICA)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("physical replication slot \"%s\" exists, but wal_level < replica",
						NameStr(cp.slotdata.name)),
				 errhint("Change wal_level to be replica or higher.")));

	/*
	 * POLAR: rw read logical replication slot from local disk,
	 * we should move the slot to polarstore
	 */
	if (!polar_vfs && ((cp.slotdata.database != InvalidOid && POLAR_RESTORE_PERSISTED_LOGICAL_SLOT) ||
			(cp.slotdata.database == InvalidOid && POLAR_RESTORE_PERSISTED_PHYSICAL_SLOT_FOR_ONLINE_PROMOTE)))
	{
		polar_make_file_path_level2(polar_replslot_dir, "pg_replslot");
		sprintf(polar_slotdir, "%s/%s", polar_replslot_dir, name);
		sprintf(polar_path, "%s/state", polar_slotdir);

		if (polar_stat(polar_slotdir, &st) == 0 && S_ISDIR(st.st_mode))
			rmtree(polar_slotdir, true);

		if (polar_make_pg_directory(polar_slotdir) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m", polar_slotdir)));
		fsync_fname(polar_slotdir, true, true);

		fd = polar_open_transient_file(polar_path, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);

		if (fd < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", polar_path)));
		}

		pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_WRITE);
		if (polar_write(fd, &cp, sizeof(cp)) != sizeof(cp))
		{
			int 		save_errno = errno;

			pgstat_report_wait_end();
			CloseTransientFile(fd);

			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", polar_path)));
		}
		pgstat_report_wait_end();
		
		pgstat_report_wait_start(WAIT_EVENT_REPLICATION_SLOT_SYNC);
		if (polar_fsync(fd) != 0)
		{
			int			save_errno = errno;

			pgstat_report_wait_end();
			CloseTransientFile(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m", polar_path)));
		}
		pgstat_report_wait_end();
		CloseTransientFile(fd);

		if (!rmtree(slotdir, true))
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove directory \"%s\"", slotdir)));
		}
		fsync_fname(replslot_dir, true, true);
		return;
	}

	/* POLAR: restoring a logical slot, and we want to store spill file on local disk */
	if (polar_vfs && cp.slotdata.database != InvalidOid)
	{
		if (polar_stat(local_slot_path, &st) < 0 && errno == ENOENT)
		{
			if (polar_make_pg_directory(local_slot_path) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\" for spill file: %m", local_slot_path)));
			fsync_fname(local_slot_path, true, true);
		}
	}

	/* nothing can be active yet, don't lock anything */
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *slot;

		slot = &ReplicationSlotCtl->replication_slots[i];

		if (slot->in_use)
			continue;

		/* POLAR: Compatible code for polar customized slot version */
		/* restore the entire set of persistent data */
		memcpy(&slot->data, &cp.slotdata,
			replication_slot_persistent_data_size);
		if (cp.version == SLOT_VERSION)
		{
			slot->data.polar_replica_apply_lsn = InvalidXLogRecPtr;
			slot->data.polar_invalidated_at = InvalidXLogRecPtr;
		}
		else if (cp.version == POLAR_SLOT_VERSION_V2)
		{
			slot->data.polar_invalidated_at = InvalidXLogRecPtr;
		}

		ereport(LOG, (errmsg("restore slot %s with version %d, replay_lsn is %X/%X, restart_lsn is %X/%X",
				cp.slotdata.name.data, cp.version,
				(uint32)(slot->data.polar_replica_apply_lsn >> 32), (uint32)(slot->data.polar_replica_apply_lsn),

				(uint32)(slot->data.restart_lsn >> 32), (uint32)(slot->data.restart_lsn))));
		/* POLAR end */

		/* initialize in memory state */
		slot->effective_xmin = cp.slotdata.xmin;
		slot->effective_catalog_xmin = cp.slotdata.catalog_xmin;

		slot->candidate_catalog_xmin = InvalidTransactionId;
		slot->candidate_xmin_lsn = InvalidXLogRecPtr;
		slot->candidate_restart_lsn = InvalidXLogRecPtr;
		slot->candidate_restart_valid = InvalidXLogRecPtr;

		slot->node_name[0] = '\0';
		slot->node_host[0] = '\0';
		slot->node_port = 0;
		slot->node_release_date[0] = '\0';
		slot->node_version[0] = '\0';
		slot->node_slot_name[0] = '\0';
		slot->node_type = POLAR_UNKNOWN;
		slot->node_state = POLAR_NODE_OFFLINE;
		/* POLAR: it means we don't want send cluster info to this slot, unless the slot report node info */
		slot->polar_sent_cluster_info_generation = POLAR_CLUSTER_INFO_INVALID_GENERATION;

		slot->in_use = true;
		slot->active_pid = 0;

		restored = true;
		break;
	}

	if (!restored)
		ereport(PANIC,
				(errmsg("too many replication slots active before shutdown"),
				 errhint("Increase max_replication_slots and try again.")));
}

/* Compute the oldest apply lsn among all replicas. */
void
polar_compute_and_set_oldest_apply_lsn(void)
{
	uint32 		slotno = 0;
	XLogRecPtr	oldest_apply_lsn = InvalidXLogRecPtr;
	XLogRecPtr	oldest_lock_lsn = InvalidXLogRecPtr;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (slotno = 0; slotno < max_replication_slots; slotno++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[slotno];
		XLogRecPtr	apply_lsn;
		XLogRecPtr	lock_lsn;

		if (!s->in_use)
			continue;

		SpinLockAcquire(&s->mutex);
		apply_lsn = s->data.polar_replica_apply_lsn;
		lock_lsn = s->polar_replica_lock_lsn;
		SpinLockRelease(&s->mutex);

		if (!XLogRecPtrIsInvalid(apply_lsn) &&
			(XLogRecPtrIsInvalid(oldest_apply_lsn)
				|| oldest_apply_lsn > apply_lsn))
			oldest_apply_lsn = apply_lsn;

		if (!XLogRecPtrIsInvalid(lock_lsn) &&
			(XLogRecPtrIsInvalid(oldest_lock_lsn)
				|| oldest_lock_lsn > lock_lsn))
			oldest_lock_lsn = lock_lsn;
	}

	LWLockRelease(ReplicationSlotControlLock);

	polar_set_oldest_applied_lsn(oldest_apply_lsn, oldest_lock_lsn);
}

/*
 * POLAR: restore all slots from replslot_dir
 * This part of the code is extracted from StartupReplicationSlots 
 */
static void
polar_restore_all_slots_from_dir(char *replslot_dir, bool load_from_vfs)
{
	DIR		   *replication_dir;
	struct dirent *replication_de;

	/* restore all slots by iterating over all on-disk entries */
	replication_dir = polar_allocate_dir(replslot_dir);
	while ((replication_de = ReadDir(replication_dir, replslot_dir)) != NULL)
	{
		struct stat statbuf;
		char		path[MAXPGPATH + 12];

		if (strcmp(replication_de->d_name, ".") == 0 ||
			strcmp(replication_de->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", replslot_dir, replication_de->d_name);

		/* we're only creating directories here, skip if it's not our's */
		if (polar_lstat(path, &statbuf) == 0 && !S_ISDIR(statbuf.st_mode))
			continue;

		/* we crashed while a slot was being setup or deleted, clean up */
		if (pg_str_endswith(replication_de->d_name, ".tmp"))
		{
			if (!rmtree(path, true))
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						errmsg("could not remove directory \"%s\"", path)));
				continue;
			}
			fsync_fname(replslot_dir, true, true);
			continue;
		}

		/* looks like a slot in a normal state, restore */
		RestoreSlotFromDisk(replication_de->d_name, load_from_vfs);
	}
	FreeDir(replication_dir);
}

void
polar_reload_replication_slots_from_shared_storage(void)
{
	/* POLAR */
	char		polar_replslot_dir[MAXPGPATH] = {0};
	struct stat stat_buf;

	if (polar_enable_persisted_physical_slot)
	{
		ereport(LOG, (errmsg("enable persisted physical slot, reload slot from polarstore.")));
	}
	else
	{
		ereport(LOG, (errmsg("disable persisted slot.")));
		return;
	}

	polar_make_file_path_level2(polar_replslot_dir, "pg_replslot");
	if (polar_stat(polar_replslot_dir, &stat_buf) == 0)
	{
		/* Check for weird cases where it exists but isn't a directory */
		if (!S_ISDIR(stat_buf.st_mode))
			ereport(FATAL,
					(errmsg("required pg_replslot directory \"%s\" does not exists",
							polar_replslot_dir)));
	}
	else
	{
		ereport(LOG,
				(errmsg("creating missing pg_replslot directory \"%s\"", polar_replslot_dir)));
		if (polar_make_pg_directory(polar_replslot_dir) < 0)
			ereport(FATAL,
					(errmsg("could not create missing directory \"%s\": %m",
							polar_replslot_dir)));
	}

	LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

	polar_restore_all_slots_from_dir(polar_replslot_dir, true);

	LWLockRelease(ReplicationSlotAllocationLock);

	/* currently no slots exist, we're done. */
	if (max_replication_slots <= 0)
	{
		elog(LOG, "Before online promote max_replication_slots=%d", max_replication_slots);
		return;
	}

	/* Now that we have recovered all the data, compute replication xmin */
	ReplicationSlotsComputeRequiredXmin(false);
	ReplicationSlotsComputeRequiredLSN();
	polar_compute_and_set_oldest_apply_lsn();

	elog(LOG, "Before online promote oldest_applied_lsn=%lX", polar_get_oldest_applied_lsn());
}

/*
 * Mark the specified slot as used by this process.
 *
 * Only one of slot and name can be specified.
 * If slot == NULL, search for the slot with the given name.
 *
 * See comments about the return value in ReplicationSlotAcquire().
 */
static int
ReplicationSlotAcquireInternal(ReplicationSlot *slot, const char *name,
							   SlotAcquireBehavior behavior)
{
	ReplicationSlot *s;
	int			active_pid;

	AssertArg((slot == NULL) ^ (name == NULL));

retry:
	Assert(MyReplicationSlot == NULL);

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

	/*
	 * Search for the slot with the specified name if the slot to acquire is
	 * not given. If the slot is not found, we either return -1 or error out.
	 */
	s = slot ? slot : SearchNamedReplicationSlot(name);
	if (s == NULL || !s->in_use)
	{
		LWLockRelease(ReplicationSlotControlLock);

		if (behavior == SAB_Inquire)
			return -1;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("replication slot \"%s\" does not exist",
						name ? name : NameStr(slot->data.name))));
	}

	/*
	 * This is the slot we want; check if it's active under some other
	 * process.  In single user mode, we don't need this check.
	 */
	if (IsUnderPostmaster)
	{
		/*
		 * Get ready to sleep on the slot in case it is active if SAB_Block.
		 * (We may end up not sleeping, but we don't want to do this while
		 * holding the spinlock.)
		 */
		if (behavior == SAB_Block)
			ConditionVariablePrepareToSleep(&s->active_cv);

		SpinLockAcquire(&s->mutex);
		if (s->active_pid == 0)
			s->active_pid = MyProcPid;
		active_pid = s->active_pid;
		SpinLockRelease(&s->mutex);
	}
	else
		active_pid = MyProcPid;
	LWLockRelease(ReplicationSlotControlLock);

	/*
	 * If we found the slot but it's already active in another process, we
	 * either error out, return the PID of the owning process, or retry
	 * after a short wait, as caller specified.
	 */
	if (active_pid != MyProcPid)
	{
		if (behavior == SAB_Error)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("replication slot \"%s\" is active for PID %d",
							NameStr(s->data.name), active_pid)));
		else if (behavior == SAB_Inquire)
			return active_pid;

		/* Wait here until we get signaled, and then restart */
		ConditionVariableSleep(&s->active_cv,
							   WAIT_EVENT_REPLICATION_SLOT_DROP);
		ConditionVariableCancelSleep();
		goto retry;
	}
	else if (behavior == SAB_Block)
		ConditionVariableCancelSleep();	/* no sleep needed after all */

	/* Let everybody know we've modified this slot */
	ConditionVariableBroadcast(&s->active_cv);

	/* We made this slot active, so it's ours now. */
	MyReplicationSlot = s;

	/* success */
	return 0;
}

/*
 * Mark any slot that points to an LSN older than the given segment
 * as invalid; it requires WAL that's about to be removed.
 *
 * NB - this runs as part of checkpoint, so avoid raising errors if possible.
 */
void
InvalidateObsoleteReplicationSlots(XLogSegNo oldestSegno)
{
	XLogRecPtr	oldestLSN;
	int			i;

	if (!polar_enable_max_slot_wal_keep_size)
		return;

	XLogSegNoOffsetToRecPtr(oldestSegno, 0, wal_segment_size, oldestLSN);

restart:
	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
		XLogRecPtr	restart_lsn = InvalidXLogRecPtr;
		NameData	slotname;
		int		wspid;
		int		last_signaled_pid = 0;

		if (!s->in_use)
			continue;

		SpinLockAcquire(&s->mutex);
		slotname = s->data.name;
		restart_lsn = s->data.restart_lsn;
		SpinLockRelease(&s->mutex);

		if (XLogRecPtrIsInvalid(restart_lsn) || restart_lsn >= oldestLSN)
			continue;
		LWLockRelease(ReplicationSlotControlLock);
		CHECK_FOR_INTERRUPTS();

		/* Get ready to sleep on the slot in case it is active */
		ConditionVariablePrepareToSleep(&s->active_cv);

		for (;;)
		{
			/*
			 * Try to mark this slot as used by this process.
			 *
			 * Note that ReplicationSlotAcquireInternal(SAB_Inquire)
			 * should not cancel the prepared condition variable
			 * if this slot is active in other process. Because in this case
			 * we have to wait on that CV for the process owning
			 * the slot to be terminated, later.
			 */
			wspid = ReplicationSlotAcquireInternal(s, NULL, SAB_Inquire);

			/*
			 * Exit the loop if we successfully acquired the slot or
			 * the slot was dropped during waiting for the owning process
			 * to be terminated. For example, the latter case is likely to
			 * happen when the slot is temporary because it's automatically
			 * dropped by the termination of the owning process.
			 */
			if (wspid <= 0)
				break;

			/*
			 * Signal to terminate the process that owns the slot.
			 *
			 * There is the race condition where other process may own
			 * the slot after the process using it was terminated and before
			 * this process owns it. To handle this case, we signal again
			 * if the PID of the owning process is changed than the last.
			 *
			 * XXX This logic assumes that the same PID is not reused
			 * very quickly.
			 */
			if (last_signaled_pid != wspid)
			{
				ereport(LOG,
						(errmsg("terminating process %d because replication slot \"%s\" is too far behind",
								wspid, NameStr(slotname))));
				(void) kill(wspid, SIGTERM);
				last_signaled_pid = wspid;
			}

			ConditionVariableTimedSleep(&s->active_cv, 10,
										WAIT_EVENT_REPLICATION_SLOT_DROP);
		}
		ConditionVariableCancelSleep();

		/*
		 * Do nothing here and start from scratch if the slot has
		 * already been dropped.
		 */
		if (wspid == -1)
			goto restart;

		ereport(LOG,
				(errmsg("invalidating slot \"%s\" because its restart_lsn %X/%X exceeds max_slot_wal_keep_size",
						NameStr(slotname),
						LSN_FORMAT_ARGS(restart_lsn))));

		SpinLockAcquire(&s->mutex);
		s->data.polar_invalidated_at = s->data.restart_lsn;
		s->data.restart_lsn = InvalidXLogRecPtr;
		SpinLockRelease(&s->mutex);

		/* Make sure the invalidated state persists across server restart */
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
		ReplicationSlotRelease();

		/* if we did anything, start from scratch */
		goto restart;
	}
	LWLockRelease(ReplicationSlotControlLock);
}

/*
 * Search for the named replication slot.
 *
 * Return the replication slot if found, otherwise NULL.
 *
 * The caller must hold ReplicationSlotControlLock in shared mode.
 */
ReplicationSlot *
SearchNamedReplicationSlot(const char *name)
{
	int			i;
	ReplicationSlot	*slot = NULL;

	Assert(LWLockHeldByMeInMode(ReplicationSlotControlLock,
								LW_SHARED));

	for (i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0)
		{
			slot = s;
			break;
		}
	}

	return slot;
}
