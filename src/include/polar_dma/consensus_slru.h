/*-------------------------------------------------------------------------
 *
 * consensus_slru.h
 *		Simple LRU buffering for consensus logfiles
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	src/include/consensus/consensus_slru.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONSENSUS_SLRU_H
#define	CONSENSUS_SLRU_H

#include <pthread.h>
#include "storage/polar_fd.h"

#define CONSENSUS_SLRU_PAGES_PER_SEGMENT	32

#define CONSENSUS_SLRU_MAX_NAME_LENGTH	32

/*
 * Page status codes.  Note that these do not include the "dirty" bit.
 * page_dirty can be true only in the VALID or WRITE_IN_PROGRESS states;
 * in the latter case it implies that the page has been re-dirtied since
 * the write started.
 */
typedef enum
{
	SLRU_PAGE_EMPTY,			/* buffer is not in use */
	SLRU_PAGE_READ_IN_PROGRESS, /* page is being read in */
	SLRU_PAGE_VALID,			/* page is valid and not being written */
	SLRU_PAGE_WRITE_IN_PROGRESS /* page is being written out */
} ConsensusSlruPageStatus;

typedef struct consensus_slru_stat 
{
	const char *name;					/* slru name */
	uint		n_slots; 				/* buffer slots number */
	uint		n_page_status_stat[4];	/* SLRU_PAGE_VALID status page number */
 	uint 		n_wait_reading_count;   /* waitor number for reading slots */
	uint    n_wait_writing_count;   /* waitor number for writing slots */ 
	uint64_t	n_victim_count;  			/* total victim slot count */
	uint64_t	n_victim_write_count; /* total write victim slot count */
	uint64_t	n_slru_read_count; 		/* total ConsensusSimpleLruReadPage calls */
	uint64_t	n_slru_read_only_count; 		/* total ReadPage_ReadOnly but not ReadPage calls */
	uint64_t	n_slru_read_upgrade_count;	/* total ReadPage_ReadOnly upgrade to ReadPage calls */
	uint64_t	n_slru_write_count;		/* total SlruInternalWritePage calls */
	uint64_t	n_slru_zero_count;		/* total ConsensusSimpleLruZeroPage calls */
	uint64_t	n_slru_flush_count;		/* total ConsensusSimpleLruFlush calls */
	uint64_t	n_slru_truncate_backward_count; /* total ConsensusSimpleLruTruncateBackward calls */
	uint64_t	n_slru_truncate_forward_count;  /* total ConsensusSimpleLruTruncateForward calls */
	uint64_t	n_storage_read_count;  /* total slru slot read from storage counts */
	uint64_t	n_storage_write_count; /* total slru slot write to storage counts */
} consensus_slru_stat;

typedef bool (*flush_hook)(int slot);

#define CONSENSUS_SLRU_STATS_NUM 2
extern const consensus_slru_stat *consensus_slru_stats[CONSENSUS_SLRU_STATS_NUM];
extern int n_consensus_slru_stats;

/*
 * Shared-memory state
 */
typedef struct ConsensusSlruSharedData
{
	pthread_rwlock_t *control_lock;

	/* Number of buffers managed by this SLRU structure */
	int			num_slots;

	/* dirty page & flush point */
	int			first_dirty_slot;
	int		  first_dirty_offset;
	int			last_dirty_slot;
	int		  *next_dirty_slot;

	/*
	 * Arrays holding info for each buffer slot.  Page number is undefined
	 * when status is EMPTY, as is page_lru_count.
	 */
	char	  **page_buffer;
	ConsensusSlruPageStatus *page_status;
	bool	   *page_dirty;
	uint64	 *page_number;
	int		   *page_lru_count;

	/*----------
	 * We mark a page "most recently used" by setting
	 *		page_lru_count[slotno] = ++cur_lru_count;
	 * The oldest page is therefore the one with the highest value of
	 *		cur_lru_count - page_lru_count[slotno]
	 * The counts will eventually wrap around, but this calculation still
	 * works as long as no page's age exceeds INT_MAX counts.
	 *----------
	 */
	int			cur_lru_count;

	/*
	 * latest_page_number is the page number of the current end of the log;
	 */
	int			latest_page_number;

	char		name[CONSENSUS_SLRU_MAX_NAME_LENGTH];
	pthread_rwlock_t *buffer_locks;

	consensus_slru_stat stat;

	/* the slru file put into shared storage */
	bool    slru_file_in_shared_storage;

	int 		victim_pivot;
} ConsensusSlruSharedData;

typedef ConsensusSlruSharedData *ConsensusSlruShared;

/*
 * ConsensusSlruCtlData is an unshared structure that points to the active information
 * in shared memory.
 */
typedef struct ConsensusSlruCtlData
{
	ConsensusSlruShared	shared;
	int szblock;
	char Dir[64];
	const vfs_mgr *vfs_api;
	flush_hook before_flush_hook;
} ConsensusSlruCtlData;

typedef ConsensusSlruCtlData *ConsensusSlruCtl;

void ConsensusSlruStatsInit(void);
extern Size ConsensusSimpleLruShmemSize(int nslots, int szblock, int nlsns);
extern void ConsensusSimpleLruInit(ConsensusSlruCtl ctl, const char *name, 
		int szblock, int nslots, int nlsns, pthread_rwlock_t *ctllock, 
		flush_hook before_flush_hook, const char *subdir, bool polar_shared_file);
extern bool ConsensusSimpleLruValidateDir(ConsensusSlruCtl ctl);
extern int	ConsensusSimpleLruZeroPage(ConsensusSlruCtl ctl, uint64 pageno);
extern int ConsensusSimpleLruReadPage(ConsensusSlruCtl ctl, uint64 pageno, bool write_ok);
extern int ConsensusSimpleLruReadPage_ReadOnly(ConsensusSlruCtl ctl, uint64 pageno);
extern bool ConsensusSimpleLruWritePage(ConsensusSlruCtl ctl, int slotno, bool create_if_not_exists);
extern bool ConsensusSimpleLruFlush(ConsensusSlruCtl ctl, uint64 pageno);
extern void ConsensusSimpleLruTruncateBackward(ConsensusSlruCtl ctl, uint64 cutoffPage);
extern void ConsensusSimpleLruTruncateForward(ConsensusSlruCtl ctl, uint64 cutoffPage);

extern void consensus_slru_push_dirty(ConsensusSlruCtl ctl, int slotno, 
		int write_offset, bool head);
extern void consensus_slru_pop_dirty(ConsensusSlruCtl ctl, int slotno);

typedef bool (*ConsensusSlruScanCallback) (ConsensusSlruCtl ctl, char *filename, 
		uint64 segpage, void *data);
extern bool ConsensusSlruScanDirectory(ConsensusSlruCtl ctl, ConsensusSlruScanCallback callback, void *data);

/* ConsensusSlruScanDirectory public callbacks */
extern bool ConsensusSlruScanDirCbReportPresenceBackward(ConsensusSlruCtl ctl, char *filename,
		uint64 segpage, void *data);
extern bool ConsensusSlruScanDirCbReportPresenceForward(ConsensusSlruCtl ctl, char *filename,
		uint64 segpage, void *data);
bool consensus_slru_scan_dir_callback_delete_cutoff_forward(ConsensusSlruCtl ctl, 
		char *filename, uint64 segpage, void *data);
bool consensus_slru_scan_dir_callback_delete_cutoff_backward(ConsensusSlruCtl ctl, 
		char *filename, uint64 segpage, void *data);

#endif							/* SLRU_H */
