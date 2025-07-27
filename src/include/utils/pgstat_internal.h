/* ----------
 * pgstat_internal.h
 *
 * Definitions for the PostgreSQL cumulative statistics system that should
 * only be needed by files implementing statistics support (rather than ones
 * reporting / querying stats).
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 * src/include/utils/pgstat_internal.h
 * ----------
 */
#ifndef PGSTAT_INTERNAL_H
#define PGSTAT_INTERNAL_H


#include "common/hashfn_unstable.h"
#include "lib/dshash.h"
#include "lib/ilist.h"
#include "pgstat.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"


/*
 * Types related to shared memory storage of statistics.
 *
 * Per-object statistics are stored in the "shared stats" hashtable. That
 * table's entries (PgStatShared_HashEntry) contain a pointer to the actual stats
 * data for the object (the size of the stats data varies depending on the
 * kind of stats). The table is keyed by PgStat_HashKey.
 *
 * Once a backend has a reference to a shared stats entry, it increments the
 * entry's refcount. Even after stats data is dropped (e.g., due to a DROP
 * TABLE), the entry itself can only be deleted once all references have been
 * released.
 *
 * These refcounts, in combination with a backend local hashtable
 * (pgStatEntryRefHash, with entries pointing to PgStat_EntryRef) in front of
 * the shared hash table, mean that most stats work can happen without
 * touching the shared hash table, reducing contention.
 *
 * Once there are pending stats updates for a table PgStat_EntryRef->pending
 * is allocated to contain a working space for as-of-yet-unapplied stats
 * updates. Once the stats are flushed, PgStat_EntryRef->pending is freed.
 *
 * Each stat kind in the shared hash table has a fixed member
 * PgStatShared_Common as the first element.
 */

/* struct for shared statistics hash entry key. */
typedef struct PgStat_HashKey
{
	PgStat_Kind kind;			/* statistics entry kind */
	Oid			dboid;			/* database ID. InvalidOid for shared objects. */
	uint64		objid;			/* object ID (table, function, etc.), or
								 * identifier. */
} PgStat_HashKey;

/*
 * Shared statistics hash entry. Doesn't itself contain any stats, but points
 * to them (with ->body). That allows the stats entries themselves to be of
 * variable size.
 */
typedef struct PgStatShared_HashEntry
{
	PgStat_HashKey key;			/* hash key */

	/*
	 * If dropped is set, backends need to release their references so that
	 * the memory for the entry can be freed. No new references may be made
	 * once marked as dropped.
	 */
	bool		dropped;

	/*
	 * Refcount managing lifetime of the entry itself (as opposed to the
	 * dshash entry pointing to it). The stats lifetime has to be separate
	 * from the hash table entry lifetime because we allow backends to point
	 * to a stats entry without holding a hash table lock (and some other
	 * reasons).
	 *
	 * As long as the entry is not dropped, 1 is added to the refcount
	 * representing that the entry should not be dropped. In addition each
	 * backend that has a reference to the entry needs to increment the
	 * refcount as long as it does.
	 *
	 * May only be incremented / decremented while holding at least a shared
	 * lock on the dshash partition containing the entry. It needs to be an
	 * atomic variable because multiple backends can increment the refcount
	 * with just a shared lock.
	 *
	 * When the refcount reaches 0 the entry needs to be freed.
	 */
	pg_atomic_uint32 refcount;

	/*
	 * Counter tracking the number of times the entry has been reused.
	 *
	 * Set to 0 when the entry is created, and incremented by one each time
	 * the shared entry is reinitialized with pgstat_reinit_entry().
	 *
	 * May only be incremented / decremented while holding at least a shared
	 * lock on the dshash partition containing the entry. Like refcount, it
	 * needs to be an atomic variable because multiple backends can increment
	 * the generation with just a shared lock.
	 */
	pg_atomic_uint32 generation;

	/*
	 * Pointer to shared stats. The stats entry always starts with
	 * PgStatShared_Common, embedded in a larger struct containing the
	 * PgStat_Kind specific stats fields.
	 */
	dsa_pointer body;
} PgStatShared_HashEntry;

/*
 * Common header struct for PgStatShared_*.
 */
typedef struct PgStatShared_Common
{
	uint32		magic;			/* just a validity cross-check */
	/* lock protecting stats contents (i.e. data following the header) */
	LWLock		lock;
} PgStatShared_Common;

/*
 * A backend local reference to a shared stats entry. As long as at least one
 * such reference exists, the shared stats entry will not be released.
 *
 * If there are pending stats update to the shared stats, these are stored in
 * ->pending.
 */
typedef struct PgStat_EntryRef
{
	/*
	 * Pointer to the PgStatShared_HashEntry entry in the shared stats
	 * hashtable.
	 */
	PgStatShared_HashEntry *shared_entry;

	/*
	 * Pointer to the stats data (i.e. PgStatShared_HashEntry->body), resolved
	 * as a local pointer, to avoid repeated dsa_get_address() calls.
	 */
	PgStatShared_Common *shared_stats;

	/*
	 * Copy of PgStatShared_HashEntry->generation, keeping locally track of
	 * the shared stats entry "generation" retrieved (number of times reused).
	 */
	uint32		generation;

	/*
	 * Pending statistics data that will need to be flushed to shared memory
	 * stats eventually. Each stats kind utilizing pending data defines what
	 * format its pending data has and needs to provide a
	 * PgStat_KindInfo->flush_pending_cb callback to merge pending entries
	 * into the shared stats hash table.
	 */
	void	   *pending;
	dlist_node	pending_node;	/* membership in pgStatPending list */
} PgStat_EntryRef;


/*
 * Some stats changes are transactional. To maintain those, a stack of
 * PgStat_SubXactStatus entries is maintained, which contain data pertaining
 * to the current transaction and its active subtransactions.
 */
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */

	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */

	/*
	 * Statistics for transactionally dropped objects need to be
	 * transactionally dropped as well. Collect the stats dropped in the
	 * current (sub-)transaction and only execute the stats drop when we know
	 * if the transaction commits/aborts. To handle replicas and crashes,
	 * stats drops are included in commit / abort records.
	 */
	dclist_head pending_drops;

	/*
	 * Tuple insertion/deletion counts for an open transaction can't be
	 * propagated into PgStat_TableStatus counters until we know if it is
	 * going to commit or abort.  Hence, we keep these counts in per-subxact
	 * structs that live in TopTransactionContext.  This data structure is
	 * designed on the assumption that subxacts won't usually modify very many
	 * tables.
	 */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;


/*
 * Metadata for a specific kind of statistics.
 */
typedef struct PgStat_KindInfo
{
	/*
	 * Do a fixed number of stats objects exist for this kind of stats (e.g.
	 * bgwriter stats) or not (e.g. tables).
	 */
	bool		fixed_amount:1;

	/*
	 * Can stats of this kind be accessed from another database? Determines
	 * whether a stats object gets included in stats snapshots.
	 */
	bool		accessed_across_databases:1;

	/* Should stats be written to the on-disk stats file? */
	bool		write_to_file:1;

	/*
	 * The size of an entry in the shared stats hash table (pointed to by
	 * PgStatShared_HashEntry->body).  For fixed-numbered statistics, this is
	 * the size of an entry in PgStat_ShmemControl->custom_data.
	 */
	uint32		shared_size;

	/*
	 * The offset of the statistics struct in the cached statistics snapshot
	 * PgStat_Snapshot, for fixed-numbered statistics.
	 */
	uint32		snapshot_ctl_off;

	/*
	 * The offset of the statistics struct in the containing shared memory
	 * control structure PgStat_ShmemControl, for fixed-numbered statistics.
	 */
	uint32		shared_ctl_off;

	/*
	 * The offset/size of statistics inside the shared stats entry. Used when
	 * [de-]serializing statistics to / from disk respectively. Separate from
	 * shared_size because [de-]serialization may not include in-memory state
	 * like lwlocks.
	 */
	uint32		shared_data_off;
	uint32		shared_data_len;

	/*
	 * The size of the pending data for this kind. E.g. how large
	 * PgStat_EntryRef->pending is. Used for allocations.
	 *
	 * 0 signals that an entry of this kind should never have a pending entry.
	 */
	uint32		pending_size;

	/*
	 * Perform custom actions when initializing a backend (standalone or under
	 * postmaster). Optional.
	 */
	void		(*init_backend_cb) (void);

	/*
	 * For variable-numbered stats: flush pending stats. Required if pending
	 * data is used. See flush_static_cb when dealing with stats data that
	 * that cannot use PgStat_EntryRef->pending.
	 */
	bool		(*flush_pending_cb) (PgStat_EntryRef *sr, bool nowait);

	/*
	 * For variable-numbered stats: delete pending stats. Optional.
	 */
	void		(*delete_pending_cb) (PgStat_EntryRef *sr);

	/*
	 * For variable-numbered stats: reset the reset timestamp. Optional.
	 */
	void		(*reset_timestamp_cb) (PgStatShared_Common *header, TimestampTz ts);

	/*
	 * For variable-numbered stats. Optional.
	 */
	void		(*to_serialized_name) (const PgStat_HashKey *key,
									   const PgStatShared_Common *header, NameData *name);
	bool		(*from_serialized_name) (const NameData *name, PgStat_HashKey *key);

	/*
	 * For fixed-numbered statistics: Initialize shared memory state.
	 *
	 * "stats" is the pointer to the allocated shared memory area.
	 */
	void		(*init_shmem_cb) (void *stats);

	/*
	 * For fixed-numbered or variable-numbered statistics: Flush pending stats
	 * entries, for stats kinds that do not use PgStat_EntryRef->pending.
	 *
	 * Returns true if some of the stats could not be flushed, due to lock
	 * contention for example. Optional.
	 *
	 * "pgstat_report_fixed" needs to be set to trigger the flush of pending
	 * stats.
	 */
	bool		(*flush_static_cb) (bool nowait);

	/*
	 * For fixed-numbered statistics: Reset All.
	 */
	void		(*reset_all_cb) (TimestampTz ts);

	/*
	 * For fixed-numbered statistics: Build snapshot for entry
	 */
	void		(*snapshot_cb) (void);

	/* name of the kind of stats */
	const char *const name;
} PgStat_KindInfo;


/*
 * List of SLRU names that we keep stats for.  There is no central registry of
 * SLRUs, so we use this fixed list instead.  The "other" entry is used for
 * all SLRUs without an explicit entry (e.g. SLRUs in extensions).
 *
 * This is only defined here so that SLRU_NUM_ELEMENTS is known for later type
 * definitions.
 */
static const char *const slru_names[] = {
	"commit_timestamp",
	"multixact_member",
	"multixact_offset",
	"notify",
	"serializable",
	"subtransaction",
	"transaction",
	"other"						/* has to be last */
};

#define SLRU_NUM_ELEMENTS	lengthof(slru_names)


/* ----------
 * Types and definitions for different kinds of fixed-amount stats.
 *
 * Single-writer stats use the changecount mechanism to achieve low-overhead
 * writes - they're obviously more performance critical than reads. Check the
 * definition of struct PgBackendStatus for some explanation of the
 * changecount mechanism.
 *
 * Because the obvious implementation of resetting single-writer stats isn't
 * compatible with that (another backend needs to write), we don't scribble on
 * shared stats while resetting. Instead, just record the current counter
 * values in a copy of the stats data, which is protected by ->lock. See
 * pgstat_fetch_stat_(archiver|bgwriter|checkpointer) for the reader side.
 *
 * The only exception to that is the stat_reset_timestamp in these structs,
 * which is protected by ->lock, because it has to be written by another
 * backend while resetting.
 * ----------
 */

typedef struct PgStatShared_Archiver
{
	/* lock protects ->reset_offset as well as stats->stat_reset_timestamp */
	LWLock		lock;
	uint32		changecount;
	PgStat_ArchiverStats stats;
	PgStat_ArchiverStats reset_offset;
} PgStatShared_Archiver;

typedef struct PgStatShared_BgWriter
{
	/* lock protects ->reset_offset as well as stats->stat_reset_timestamp */
	LWLock		lock;
	uint32		changecount;
	PgStat_BgWriterStats stats;
	PgStat_BgWriterStats reset_offset;
} PgStatShared_BgWriter;

typedef struct PgStatShared_Checkpointer
{
	/* lock protects ->reset_offset as well as stats->stat_reset_timestamp */
	LWLock		lock;
	uint32		changecount;
	PgStat_CheckpointerStats stats;
	PgStat_CheckpointerStats reset_offset;
} PgStatShared_Checkpointer;

/* Shared-memory ready PgStat_IO */
typedef struct PgStatShared_IO
{
	/*
	 * locks[i] protects stats.stats[i]. locks[0] also protects
	 * stats.stat_reset_timestamp.
	 */
	LWLock		locks[BACKEND_NUM_TYPES];
	PgStat_IO	stats;
} PgStatShared_IO;

typedef struct PgStatShared_SLRU
{
	/* lock protects ->stats */
	LWLock		lock;
	PgStat_SLRUStats stats[SLRU_NUM_ELEMENTS];
} PgStatShared_SLRU;

typedef struct PgStatShared_Wal
{
	/* lock protects ->stats */
	LWLock		lock;
	PgStat_WalStats stats;
} PgStatShared_Wal;



/* ----------
 * Types and definitions for different kinds of variable-amount stats.
 *
 * Each struct has to start with PgStatShared_Common, containing information
 * common across the different types of stats. Kind-specific data follows.
 * ----------
 */

typedef struct PgStatShared_Database
{
	PgStatShared_Common header;
	PgStat_StatDBEntry stats;
} PgStatShared_Database;

typedef struct PgStatShared_Relation
{
	PgStatShared_Common header;
	PgStat_StatTabEntry stats;
} PgStatShared_Relation;

typedef struct PgStatShared_Function
{
	PgStatShared_Common header;
	PgStat_StatFuncEntry stats;
} PgStatShared_Function;

typedef struct PgStatShared_Subscription
{
	PgStatShared_Common header;
	PgStat_StatSubEntry stats;
} PgStatShared_Subscription;

typedef struct PgStatShared_ReplSlot
{
	PgStatShared_Common header;
	PgStat_StatReplSlotEntry stats;
} PgStatShared_ReplSlot;

typedef struct PgStatShared_Backend
{
	PgStatShared_Common header;
	PgStat_Backend stats;
} PgStatShared_Backend;

/*
 * Central shared memory entry for the cumulative stats system.
 *
 * Fixed amount stats, the dynamic shared memory hash table for
 * non-fixed-amount stats, as well as remaining bits and pieces are all
 * reached from here.
 */
typedef struct PgStat_ShmemControl
{
	void	   *raw_dsa_area;

	/*
	 * Stats for variable-numbered objects are kept in this shared hash table.
	 * See comment above PgStat_Kind for details.
	 */
	dshash_table_handle hash_handle;	/* shared dbstat hash */

	/* Has the stats system already been shut down? Just a debugging check. */
	bool		is_shutdown;

	/*
	 * Whenever statistics for dropped objects could not be freed - because
	 * backends still have references - the dropping backend calls
	 * pgstat_request_entry_refs_gc() incrementing this counter. Eventually
	 * that causes backends to run pgstat_gc_entry_refs(), allowing memory to
	 * be reclaimed.
	 */
	pg_atomic_uint64 gc_request_count;

	/*
	 * Stats data for fixed-numbered objects.
	 */
	PgStatShared_Archiver archiver;
	PgStatShared_BgWriter bgwriter;
	PgStatShared_Checkpointer checkpointer;
	PgStatShared_IO io;
	PgStatShared_SLRU slru;
	PgStatShared_Wal wal;

	/*
	 * Custom stats data with fixed-numbered objects, indexed by (PgStat_Kind
	 * - PGSTAT_KIND_CUSTOM_MIN).
	 */
	void	   *custom_data[PGSTAT_KIND_CUSTOM_SIZE];

} PgStat_ShmemControl;


/*
 * Cached statistics snapshot
 */
typedef struct PgStat_Snapshot
{
	PgStat_FetchConsistency mode;

	/* time at which snapshot was taken */
	TimestampTz snapshot_timestamp;

	bool		fixed_valid[PGSTAT_KIND_BUILTIN_SIZE];

	PgStat_ArchiverStats archiver;

	PgStat_BgWriterStats bgwriter;

	PgStat_CheckpointerStats checkpointer;

	PgStat_IO	io;

	PgStat_SLRUStats slru[SLRU_NUM_ELEMENTS];

	PgStat_WalStats wal;

	/*
	 * Data in snapshot for custom fixed-numbered statistics, indexed by
	 * (PgStat_Kind - PGSTAT_KIND_CUSTOM_MIN).  Each entry is allocated in
	 * TopMemoryContext, for a size of PgStat_KindInfo->shared_data_len.
	 */
	bool		custom_valid[PGSTAT_KIND_CUSTOM_SIZE];
	void	   *custom_data[PGSTAT_KIND_CUSTOM_SIZE];

	/* to free snapshot in bulk */
	MemoryContext context;
	struct pgstat_snapshot_hash *stats;
} PgStat_Snapshot;


/*
 * Collection of backend-local stats state.
 */
typedef struct PgStat_LocalState
{
	PgStat_ShmemControl *shmem;
	dsa_area   *dsa;
	dshash_table *shared_hash;

	/* the current statistics snapshot */
	PgStat_Snapshot snapshot;
} PgStat_LocalState;


/*
 * Inline functions defined further below.
 */

static inline void pgstat_begin_changecount_write(uint32 *cc);
static inline void pgstat_end_changecount_write(uint32 *cc);
static inline uint32 pgstat_begin_changecount_read(uint32 *cc);
static inline bool pgstat_end_changecount_read(uint32 *cc, uint32 cc_before);

static inline void pgstat_copy_changecounted_stats(void *dst, void *src, size_t len,
												   uint32 *cc);

static inline int pgstat_cmp_hash_key(const void *a, const void *b, size_t size, void *arg);
static inline uint32 pgstat_hash_hash_key(const void *d, size_t size, void *arg);
static inline size_t pgstat_get_entry_len(PgStat_Kind kind);
static inline void *pgstat_get_entry_data(PgStat_Kind kind, PgStatShared_Common *entry);
static inline void *pgstat_get_custom_shmem_data(PgStat_Kind kind);
static inline void *pgstat_get_custom_snapshot_data(PgStat_Kind kind);


/*
 * Functions in pgstat.c
 */

extern const PgStat_KindInfo *pgstat_get_kind_info(PgStat_Kind kind);
extern void pgstat_register_kind(PgStat_Kind kind,
								 const PgStat_KindInfo *kind_info);

#ifdef USE_ASSERT_CHECKING
extern void pgstat_assert_is_up(void);
#else
#define pgstat_assert_is_up() ((void)true)
#endif

extern void pgstat_delete_pending_entry(PgStat_EntryRef *entry_ref);
extern PgStat_EntryRef *pgstat_prep_pending_entry(PgStat_Kind kind, Oid dboid,
												  uint64 objid,
												  bool *created_entry);
extern PgStat_EntryRef *pgstat_fetch_pending_entry(PgStat_Kind kind,
												   Oid dboid, uint64 objid);

extern void *pgstat_fetch_entry(PgStat_Kind kind, Oid dboid, uint64 objid);
extern void pgstat_snapshot_fixed(PgStat_Kind kind);


/*
 * Functions in pgstat_archiver.c
 */

extern void pgstat_archiver_init_shmem_cb(void *stats);
extern void pgstat_archiver_reset_all_cb(TimestampTz ts);
extern void pgstat_archiver_snapshot_cb(void);

/*
 * Functions in pgstat_backend.c
 */

/* flags for pgstat_flush_backend() */
#define PGSTAT_BACKEND_FLUSH_IO		(1 << 0)	/* Flush I/O statistics */
#define PGSTAT_BACKEND_FLUSH_WAL   (1 << 1) /* Flush WAL statistics */
#define PGSTAT_BACKEND_FLUSH_ALL   (PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL)

extern bool pgstat_flush_backend(bool nowait, bits32 flags);
extern bool pgstat_backend_flush_cb(bool nowait);
extern void pgstat_backend_reset_timestamp_cb(PgStatShared_Common *header,
											  TimestampTz ts);

/*
 * Functions in pgstat_bgwriter.c
 */

extern void pgstat_bgwriter_init_shmem_cb(void *stats);
extern void pgstat_bgwriter_reset_all_cb(TimestampTz ts);
extern void pgstat_bgwriter_snapshot_cb(void);


/*
 * Functions in pgstat_checkpointer.c
 */

extern void pgstat_checkpointer_init_shmem_cb(void *stats);
extern void pgstat_checkpointer_reset_all_cb(TimestampTz ts);
extern void pgstat_checkpointer_snapshot_cb(void);


/*
 * Functions in pgstat_database.c
 */

extern void pgstat_report_disconnect(Oid dboid);
extern void pgstat_update_dbstats(TimestampTz ts);
extern void AtEOXact_PgStat_Database(bool isCommit, bool parallel);

extern PgStat_StatDBEntry *pgstat_prep_database_pending(Oid dboid);
extern void pgstat_reset_database_timestamp(Oid dboid, TimestampTz ts);
extern bool pgstat_database_flush_cb(PgStat_EntryRef *entry_ref, bool nowait);
extern void pgstat_database_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts);


/*
 * Functions in pgstat_function.c
 */

extern bool pgstat_function_flush_cb(PgStat_EntryRef *entry_ref, bool nowait);


/*
 * Functions in pgstat_io.c
 */

extern void pgstat_flush_io(bool nowait);

extern bool pgstat_io_flush_cb(bool nowait);
extern void pgstat_io_init_shmem_cb(void *stats);
extern void pgstat_io_reset_all_cb(TimestampTz ts);
extern void pgstat_io_snapshot_cb(void);


/*
 * Functions in pgstat_relation.c
 */

extern void AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit);
extern void AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth);
extern void AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state);
extern void PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state);

extern bool pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait);
extern void pgstat_relation_delete_pending_cb(PgStat_EntryRef *entry_ref);


/*
 * Functions in pgstat_replslot.c
 */

extern void pgstat_replslot_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts);
extern void pgstat_replslot_to_serialized_name_cb(const PgStat_HashKey *key, const PgStatShared_Common *header, NameData *name);
extern bool pgstat_replslot_from_serialized_name_cb(const NameData *name, PgStat_HashKey *key);


/*
 * Functions in pgstat_shmem.c
 */

extern void pgstat_attach_shmem(void);
extern void pgstat_detach_shmem(void);

extern PgStat_EntryRef *pgstat_get_entry_ref(PgStat_Kind kind, Oid dboid, uint64 objid,
											 bool create, bool *created_entry);
extern bool pgstat_lock_entry(PgStat_EntryRef *entry_ref, bool nowait);
extern bool pgstat_lock_entry_shared(PgStat_EntryRef *entry_ref, bool nowait);
extern void pgstat_unlock_entry(PgStat_EntryRef *entry_ref);
extern bool pgstat_drop_entry(PgStat_Kind kind, Oid dboid, uint64 objid);
extern void pgstat_drop_all_entries(void);
extern void pgstat_drop_matching_entries(bool (*do_drop) (PgStatShared_HashEntry *, Datum),
										 Datum match_data);
extern PgStat_EntryRef *pgstat_get_entry_ref_locked(PgStat_Kind kind, Oid dboid, uint64 objid,
													bool nowait);
extern void pgstat_reset_entry(PgStat_Kind kind, Oid dboid, uint64 objid, TimestampTz ts);
extern void pgstat_reset_entries_of_kind(PgStat_Kind kind, TimestampTz ts);
extern void pgstat_reset_matching_entries(bool (*do_reset) (PgStatShared_HashEntry *, Datum),
										  Datum match_data,
										  TimestampTz ts);

extern void pgstat_request_entry_refs_gc(void);
extern PgStatShared_Common *pgstat_init_entry(PgStat_Kind kind,
											  PgStatShared_HashEntry *shhashent);


/*
 * Functions in pgstat_slru.c
 */

extern bool pgstat_slru_flush_cb(bool nowait);
extern void pgstat_slru_init_shmem_cb(void *stats);
extern void pgstat_slru_reset_all_cb(TimestampTz ts);
extern void pgstat_slru_snapshot_cb(void);


/*
 * Functions in pgstat_wal.c
 */

extern void pgstat_wal_init_backend_cb(void);
extern bool pgstat_wal_flush_cb(bool nowait);
extern void pgstat_wal_init_shmem_cb(void *stats);
extern void pgstat_wal_reset_all_cb(TimestampTz ts);
extern void pgstat_wal_snapshot_cb(void);


/*
 * Functions in pgstat_subscription.c
 */

extern bool pgstat_subscription_flush_cb(PgStat_EntryRef *entry_ref, bool nowait);
extern void pgstat_subscription_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts);


/*
 * Functions in pgstat_xact.c
 */

extern PgStat_SubXactStatus *pgstat_get_xact_stack_level(int nest_level);
extern void pgstat_drop_transactional(PgStat_Kind kind, Oid dboid, uint64 objid);
extern void pgstat_create_transactional(PgStat_Kind kind, Oid dboid, uint64 objid);


/*
 * Variables in pgstat.c
 */

/*
 * Track if *any* pending fixed-numbered statistics should be flushed to
 * shared memory.
 *
 * This flag can be switched to true by fixed-numbered statistics to let
 * pgstat_report_stat() know if it needs to go through one round of
 * reports, calling flush_static_cb for each fixed-numbered statistics
 * kind.  When this flag is not set, pgstat_report_stat() is able to do
 * a fast exit, knowing that there are no pending fixed-numbered statistics.
 *
 * Statistics callbacks should never reset this flag; pgstat_report_stat()
 * is in charge of doing that.
 */
extern PGDLLIMPORT bool pgstat_report_fixed;

/* Backend-local stats state */
extern PGDLLIMPORT PgStat_LocalState pgStatLocal;

/*
 * Implementation of inline functions declared above.
 */

/*
 * Helpers for changecount manipulation. See comments around struct
 * PgBackendStatus for details.
 */

static inline void
pgstat_begin_changecount_write(uint32 *cc)
{
	Assert((*cc & 1) == 0);

	START_CRIT_SECTION();
	(*cc)++;
	pg_write_barrier();
}

static inline void
pgstat_end_changecount_write(uint32 *cc)
{
	Assert((*cc & 1) == 1);

	pg_write_barrier();

	(*cc)++;

	END_CRIT_SECTION();
}

static inline uint32
pgstat_begin_changecount_read(uint32 *cc)
{
	uint32		before_cc = *cc;

	CHECK_FOR_INTERRUPTS();

	pg_read_barrier();

	return before_cc;
}

/*
 * Returns true if the read succeeded, false if it needs to be repeated.
 */
static inline bool
pgstat_end_changecount_read(uint32 *cc, uint32 before_cc)
{
	uint32		after_cc;

	pg_read_barrier();

	after_cc = *cc;

	/* was a write in progress when we started? */
	if (before_cc & 1)
		return false;

	/* did writes start and complete while we read? */
	return before_cc == after_cc;
}


/*
 * helper function for PgStat_KindInfo->snapshot_cb
 * PgStat_KindInfo->reset_all_cb callbacks.
 *
 * Copies out the specified memory area following change-count protocol.
 */
static inline void
pgstat_copy_changecounted_stats(void *dst, void *src, size_t len,
								uint32 *cc)
{
	uint32		cc_before;

	do
	{
		cc_before = pgstat_begin_changecount_read(cc);

		memcpy(dst, src, len);
	}
	while (!pgstat_end_changecount_read(cc, cc_before));
}

/* helpers for dshash / simplehash hashtables */
static inline int
pgstat_cmp_hash_key(const void *a, const void *b, size_t size, void *arg)
{
	Assert(size == sizeof(PgStat_HashKey) && arg == NULL);
	return memcmp(a, b, sizeof(PgStat_HashKey));
}

static inline uint32
pgstat_hash_hash_key(const void *d, size_t size, void *arg)
{
	const char *key = (const char *) d;

	Assert(size == sizeof(PgStat_HashKey) && arg == NULL);
	return fasthash32(key, size, 0);
}

/*
 * The length of the data portion of a shared memory stats entry (i.e. without
 * transient data such as refcounts, lwlocks, ...).
 */
static inline size_t
pgstat_get_entry_len(PgStat_Kind kind)
{
	return pgstat_get_kind_info(kind)->shared_data_len;
}

/*
 * Returns a pointer to the data portion of a shared memory stats entry.
 */
static inline void *
pgstat_get_entry_data(PgStat_Kind kind, PgStatShared_Common *entry)
{
	size_t		off = pgstat_get_kind_info(kind)->shared_data_off;

	Assert(off != 0 && off < PG_UINT32_MAX);

	return ((char *) (entry)) + off;
}

/*
 * Returns a pointer to the shared memory area of custom stats for
 * fixed-numbered statistics.
 */
static inline void *
pgstat_get_custom_shmem_data(PgStat_Kind kind)
{
	int			idx = kind - PGSTAT_KIND_CUSTOM_MIN;

	Assert(pgstat_is_kind_custom(kind));
	Assert(pgstat_get_kind_info(kind)->fixed_amount);

	return pgStatLocal.shmem->custom_data[idx];
}

/*
 * Returns a pointer to the portion of custom data for fixed-numbered
 * statistics in the current snapshot.
 */
static inline void *
pgstat_get_custom_snapshot_data(PgStat_Kind kind)
{
	int			idx = kind - PGSTAT_KIND_CUSTOM_MIN;

	Assert(pgstat_is_kind_custom(kind));
	Assert(pgstat_get_kind_info(kind)->fixed_amount);

	return pgStatLocal.snapshot.custom_data[idx];
}

#endif							/* PGSTAT_INTERNAL_H */
