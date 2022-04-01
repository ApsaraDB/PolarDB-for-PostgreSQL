/*
 * lru.c
 *        Multi-partition LRU buffering for transaction status logfiles 
 *        which is based on slru.c
 *
 * We use a multiple partition least-recently-used scheme to manage a pool of page
 * buffers to reduce lock contention and provide interfaces to CTS to avoid 
 * using exclusive locks as far as possible.
 * For example: LruReadPage_ReadOnly_Locked() for CTSLogSetPageStatus to 
 * set/get commit timestamp on LRU page with only shared lock being held which provide high concurrency.
 * Written by  , 2020.06.19
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/lru.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/lru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "utils/tqual.h"
#include "utils/timestamp.h"
#include "storage/spin.h"
#include "distributed_txn/logical_clock.h"

#define LruFileName(ctl, path, seg) \
    snprintf(path, MAXPGPATH, "%s/%04X", (ctl)->Dir, seg)

/*
 * During SimpleLruFlush(), we will usually not need to write/fsync more
 * than one or two physical files, but we may need to write several pages
 * per file.  We can consolidate the I/O requests by leaving files open
 * until control returns to SimpleLruFlush().  This data structure remembers
 * which files are open.
 */
#define MAX_FLUSH_BUFFERS    128

typedef struct LruFlushData
{
    /* some statistic information for performance tuning */
    int            cache_miss_count;    /* during flush, missed cache in this buffer*/
    int            flushed_dirty_pages; /* how many dirty pages are flushed */
    

    /* cached file descriptors */
    int            num_files;        /* # files actually open */
    int            fd[MAX_FLUSH_BUFFERS];    /* their FD's */
    int            segno[MAX_FLUSH_BUFFERS];    /* their log seg#s */
} LruFlushData;

typedef struct LruFlushData *LruFlushPt;

/*
 * Macro to mark a buffer slot "most recently used".  Note multiple evaluation
 * of arguments!
 *
 * The reason for the if-test is that there are often many consecutive
 * accesses to the same page (particularly the latest page).  By suppressing
 * useless increments of cur_lru_count, we reduce the probability that old
 * pages' counts will "wrap around" and make them appear recently used.
 *
 * We allow this code to be executed concurrently by multiple processes within
 * SimpleLruReadPage_ReadOnly().  As long as int reads and writes are atomic,
 * this should not cause any completely-bogus values to enter the computation.
 * However, it is possible for either cur_lru_count or individual
 * page_lru_count entries to be "reset" to lower values than they should have,
 * in case a process is delayed while it executes this macro.  With care in
 * SlruSelectLRUPage(), this does little harm, and in any case the absolute
 * worst possible consequence is a nonoptimal choice of page to evict.  The
 * gain from allowing concurrent reads of SLRU pages seems worth it.
 */
#define LruRecentlyUsed(shared, slotno)    \
    do { \
        int        new_lru_count = (shared)->cur_lru_count; \
        if (new_lru_count != (shared)->page_lru_count[slotno]) { \
            (shared)->cur_lru_count = ++new_lru_count; \
            (shared)->page_lru_count[slotno] = new_lru_count; \
        } \
    } while (0)

/* Saved info for SlruReportIOError */
typedef enum
{
    LRU_OPEN_FAILED,
    LRU_SEEK_FAILED,
    LRU_READ_FAILED,
    LRU_WRITE_FAILED,
    LRU_FSYNC_FAILED,
    LRU_CLOSE_FAILED
} LruErrorCause;

static LruErrorCause lru_errcause;
static int    lru_errno;


static void LruZeroLSNs(LruCtl ctl, int partitionno, int slotno);
static void LruWaitIO(LruCtl ctl, int partitionno, int slotno);
static void LruInternalWritePage(LruCtl ctl, int partitionno, int slotno, LruFlushPt fdata);
static bool LruPhysicalReadPage(LruCtl ctl, int partitionno, int pageno, int slotno);
static bool LruPhysicalWritePage(LruCtl ctl, int partitionno, int pageno, int slotno,
                      LruFlushPt fdata);
static void LruReportIOError(LruCtl ctl, int pageno, TransactionId xid);
static int    LruSelectLRUPage(LruCtl ctl, int partitionno, int pageno);

static bool LruScanDirCbDeleteCutoff(LruCtl ctl, char *filename,
                          int segpage, void *data);
static void LruInternalDeleteSegment(LruCtl ctl, char *filename);

/*
 * Initialization of shared memory
 */

Size
LruShmemSize(int nslots, int nlsns)
{
    Size        sz;

    /* we assume nslots isn't so large as to risk overflow */
    sz = MAXALIGN(sizeof(LruSharedData));
    sz += MAXALIGN(nslots * sizeof(char *));    /* page_buffer[] */
    sz += MAXALIGN(nslots * sizeof(LruPageStatus));    /* page_status[] */
    sz += MAXALIGN(nslots * sizeof(bool));    /* page_dirty[] */
    sz += MAXALIGN(nslots * sizeof(int));    /* page_number[] */
    sz += MAXALIGN(nslots * sizeof(int));    /* page_lru_count[] */
    sz += MAXALIGN((nslots + 1) * sizeof(LWLockPadded));    /* buffer_locks[] */

    if (nlsns > 0)
        sz += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));    /* group_lsn[] */

    return BUFFERALIGN(sz) + BLCKSZ * nslots;
}

typedef struct lrubuftag
{
    int pageno;        
} LruBufferTag;


typedef struct
{
    LruBufferTag    tag;            /* Tag of a disk page */
    int                slotno;                /* Associated buffer ID */
} LruBufLookupEnt;

static HTAB *SharedLruBufHash;


static uint32
lru_hash(const void *key, Size keysize)
{
    const LruBufferTag *tagPtr = key;
    return tagPtr->pageno;
}

void
InitLruBufTable(int size);
uint32
LruBufTableHashCode(LruBufferTag *tagPtr);
int
LruBufTableLookup(LruBufferTag *tagPtr, uint32 hashcode);
void
LruBufTableDelete(LruBufferTag *tagPtr, uint32 hashcode);
int
LruBufTableInsert(LruBufferTag *tagPtr, uint32 hashcode, int slotno);

static int lru_cmp (const void *key1, const void *key2,
                                Size keysize)
{
    const LruBufferTag *tagPtr1 = key1, *tagPtr2 = key2;

    if(tagPtr1->pageno == tagPtr2->pageno)
        return 0;

    return 1;
}


void
InitLruBufTable(int size)
{
    HASHCTL        info;

    /* assume no locking is needed yet */

    /* BufferTag maps to Buffer */
    info.keysize = sizeof(LruBufferTag);
    info.entrysize = sizeof(LruBufLookupEnt);
    info.num_partitions = NUM_PARTITIONS;
    info.hash = lru_hash;
    info.match = lru_cmp;

    SharedLruBufHash = ShmemInitHash("Shared LRU Buffer Lookup Table",
                                  size, size,
                                  &info,
                                  HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FUNCTION | HASH_COMPARE);
}


uint32
LruBufTableHashCode(LruBufferTag *tagPtr)
{
    return get_hash_value(SharedLruBufHash, (void *) tagPtr);
}

/*
 * BufTableLookup
 *        Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
LruBufTableLookup(LruBufferTag *tagPtr, uint32 hashcode)
{
    LruBufLookupEnt *result;

    result = (LruBufLookupEnt *)
        hash_search_with_hash_value(SharedLruBufHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_FIND,
                                    NULL);

    if (!result)
        return -1;

    return result->slotno;
}

/*
 * BufTableInsert
 *        Insert a hashtable entry for given tag and buffer ID,
 *        unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
LruBufTableInsert(LruBufferTag *tagPtr, uint32 hashcode, int slotno)
{
    LruBufLookupEnt *result;
    bool        found;

    Assert(slotno >= 0);        /* -1 is reserved for not-in-table */

    result = (LruBufLookupEnt *)
        hash_search_with_hash_value(SharedLruBufHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_ENTER,
                                    &found);

    if (found)                    /* found something already in the table */
        return result->slotno;

    result->slotno = slotno;

    return -1;
}

/*
 * BufTableDelete
 *        Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
LruBufTableDelete(LruBufferTag *tagPtr, uint32 hashcode)
{
    LruBufLookupEnt *result;

    result = (LruBufLookupEnt *)
        hash_search_with_hash_value(SharedLruBufHash,
                                    (void *) tagPtr,
                                    hashcode,
                                    HASH_REMOVE,
                                    NULL);

    if (!result)                /* shouldn't happen */
        elog(ERROR, "shared lru buffer hash table corrupted");
}


Size
LruBufTableShmemSize(int size)
{
    return hash_estimate_size(size, sizeof(LruBufLookupEnt));
}

void
LruInit(LruCtl ctl, const char *name, int nslots, int nlsns, int nbufs,
              LWLock *ctllock, const char *subdir, int tranche_id)
{
    GlobalLruShared global_shared;
    LruShared    shared;
    bool        found;
    int            partitionno;
    char         full_name[64];

    global_shared = (GlobalLruShared) ShmemInitStruct("Global Shared Data",
                                          sizeof(GlobalLruSharedData),
                                          &found);
    if (!IsUnderPostmaster)
    {
        global_shared->ControlLock = ctllock;
        global_shared->latest_page_number = 0;
        
    }else
        Assert(found);
    ctl->global_shared = global_shared;
    for(partitionno = 0; partitionno < NUM_PARTITIONS; partitionno++){
        snprintf(full_name, 64, "%s:%d", name, partitionno);
        shared = (LruShared) ShmemInitStruct(full_name,
                                          LruShmemSize(nslots, nlsns),
                                          &found);

        if (!IsUnderPostmaster)
        {
            /* Initialize locks and shared memory area */
            char       *ptr;
            Size        offset;
            int            slotno;

            Assert(!found);

            memset(shared, 0, sizeof(LruSharedData));


            shared->num_slots = nslots;
            shared->lsn_groups_per_page = nlsns;

            shared->cur_lru_count = 0;

            /* shared->latest_page_number will be set later */
            shared->latest_page_number = 0;

            ptr = (char *) shared;
            offset = MAXALIGN(sizeof(LruSharedData));
            shared->page_buffer = (char **) (ptr + offset);
            offset += MAXALIGN(nslots * sizeof(char *));
            shared->page_status = (LruPageStatus *) (ptr + offset);
            offset += MAXALIGN(nslots * sizeof(LruPageStatus));
            shared->page_dirty = (bool *) (ptr + offset);
            offset += MAXALIGN(nslots * sizeof(bool));
            shared->page_number = (int *) (ptr + offset);
            offset += MAXALIGN(nslots * sizeof(int));
            shared->page_lru_count = (int *) (ptr + offset);
            offset += MAXALIGN(nslots * sizeof(int));

            /* Initialize LWLocks */
		    shared->buffer_locks = (LWLockPadded *) (ptr + offset);
		    offset += MAXALIGN((nslots + 1) * sizeof(LWLockPadded));

            if (nlsns > 0)
            {
                shared->group_lsn = (XLogRecPtr *) (ptr + offset);
                offset += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));
            }

            Assert(strlen(name) + 1 < LRU_MAX_NAME_LENGTH);
            strlcpy(shared->lwlock_tranche_name, name, LRU_MAX_NAME_LENGTH);
            shared->lwlock_tranche_id = tranche_id;
           
            SpinLockInit(&shared->group_lsn_lock);


            ptr += BUFFERALIGN(offset);
            for (slotno = 0; slotno < nslots; slotno++)
            {
                LWLockInitialize(&shared->buffer_locks[slotno].lock,
                                 shared->lwlock_tranche_id);

                shared->page_buffer[slotno] = ptr;
                shared->page_status[slotno] = LRU_PAGE_EMPTY;
                shared->page_dirty[slotno] = false;
                shared->page_lru_count[slotno] = 0;
                ptr += BLCKSZ;
            }
            /* initialize the partition lock */
            LWLockInitialize(&shared->buffer_locks[slotno].lock,
                                 shared->lwlock_tranche_id);
            
            /* Should fit to estimated shmem size */
		    Assert(ptr - (char *) shared <= LruShmemSize(nslots, nlsns));
        }
        else
            Assert(found);
        ctl->shared[partitionno] = shared;

        /* Register SLRU tranche in the main tranches array */
        LWLockRegisterTranche(shared->lwlock_tranche_id,
                          shared->lwlock_tranche_name);
    }


    /*
     * Initialize the unshared control struct, including directory path. We
     * assume caller set PagePrecedes.
     */
    
    ctl->do_fsync = true;        /* default behavior */
    StrNCpy(ctl->Dir, subdir, sizeof(ctl->Dir));
    
    InitLruBufTable(nbufs);
    
}

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
LruZeroPage(LruCtl ctl, int partitionno, int pageno)
{
    LruShared        shared = ctl->shared[partitionno];
    int                slotno;
    LruBufferTag    newTag;
    uint32             newHash;
    int             r PG_USED_FOR_ASSERTS_ONLY;

    if (enable_distri_print)
        elog(LOG, "zero pageno %d partitionno %d", pageno, partitionno);
    /* Find a suitable buffer slot for the page */
    slotno = LruSelectLRUPage(ctl, partitionno, pageno);
    Assert(shared->page_status[slotno] == LRU_PAGE_EMPTY ||
           (shared->page_status[slotno] == LRU_PAGE_VALID &&
            !shared->page_dirty[slotno]) ||
           shared->page_number[slotno] == pageno);

    if(shared->page_number[slotno] != pageno || 
        shared->page_status[slotno] == LRU_PAGE_EMPTY){
        
        INIT_LRUBUFTAG(newTag, pageno);
        newHash = LruBufTableHashCode(&newTag);
#ifdef LRU_CHECK
        {
            int tmpslotno = LruBufTableLookup(&newTag, newHash);
            if (tmpslotno >= 0)
                elog(ERROR, "insert an exitsing pageno %d to slotno %d existing %d", pageno, slotno, tmpslotno);
        }
#endif
        if (enable_distri_print)
            elog(LOG, "insert slotno %d pageno %d partitionno %d zeropage "INT64_FORMAT, 
                                slotno, pageno, partitionno, LogicalClockNow());
        r = LruBufTableInsert(&newTag, newHash, slotno);
        Assert(r == -1);
    }

    /* Mark the slot as containing this page */
    shared->page_number[slotno] = pageno;
    shared->page_status[slotno] = LRU_PAGE_VALID;
    shared->page_dirty[slotno] = true;
    LruRecentlyUsed(shared, slotno);

    /* Set the buffer to zeroes */
    MemSet(shared->page_buffer[slotno], 0, BLCKSZ);

    /* Set the LSNs for this new page to zero */
    LruZeroLSNs(ctl, partitionno, slotno);

    /* Assume this page is now the latest active page */
    
    ctl->global_shared->latest_page_number = pageno;
    
    shared->latest_page_number = pageno;

    return slotno;
}

/*
 * Zero all the LSNs we store for this slru page.
 *
 * This should be called each time we create a new page, and each time we read
 * in a page from disk into an existing buffer.  (Such an old page cannot
 * have any interesting LSNs, since we'd have flushed them before writing
 * the page in the first place.)
 *
 * This assumes that InvalidXLogRecPtr is bitwise-all-0.
 */
static void
LruZeroLSNs(LruCtl ctl, int partitionno, int slotno)
{
    LruShared    shared = ctl->shared[partitionno];

    if (shared->lsn_groups_per_page > 0)
        MemSet(&shared->group_lsn[slotno * shared->lsn_groups_per_page], 0,
               shared->lsn_groups_per_page * sizeof(XLogRecPtr));
}

/*
 * Wait for any active I/O on a page slot to finish.  (This does not
 * guarantee that new I/O hasn't been started before we return, though.
 * In fact the slot might not even contain the same page anymore.)
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
LruWaitIO(LruCtl ctl, int partitionno, int slotno)
{
    LruShared    shared = ctl->shared[partitionno];
    LWLock       *newPartitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;
    bool        heldGlobalLock = false;

    /* See notes at top of file */
    LWLockRelease(newPartitionLock);
    if (LWLockHeldByMe(ctl->global_shared->ControlLock))
    {
        heldGlobalLock = true;
        LWLockRelease(ctl->global_shared->ControlLock);
    }
    LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED);
    LWLockRelease(&shared->buffer_locks[slotno].lock);
    if (heldGlobalLock)
    {
        LWLockAcquire(ctl->global_shared->ControlLock, LW_EXCLUSIVE);
    }
    LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
    
    /*
     * If the slot is still in an io-in-progress state, then either someone
     * already started a new I/O on the slot, or a previous I/O failed and
     * neglected to reset the page state.  That shouldn't happen, really, but
     * it seems worth a few extra cycles to check and recover from it. We can
     * cheaply test for failure by seeing if the buffer lock is still held (we
     * assume that transaction abort would release the lock).
     */
    if (shared->page_status[slotno] == LRU_PAGE_READ_IN_PROGRESS ||
        shared->page_status[slotno] == LRU_PAGE_WRITE_IN_PROGRESS)
    {
        if (LWLockConditionalAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED))
        {
            /* indeed, the I/O must have failed */
            if (shared->page_status[slotno] == LRU_PAGE_READ_IN_PROGRESS)
            {
                LruBufferTag    tag;            /* previous identity of selected buffer */
                int                pageno = shared->page_number[slotno];
                uint32            hash;        /* hash value for tag */
                
                INIT_LRUBUFTAG(tag, pageno);
                hash = LruBufTableHashCode(&tag);
                if (enable_distri_print)
                    elog(LOG, "delete slotno %d pageno %d waitio", slotno, pageno); 
                LruBufTableDelete(&tag, hash);    
                shared->page_status[slotno] = LRU_PAGE_EMPTY;
            }
            else                /* write_in_progress */
            {
                shared->page_status[slotno] = LRU_PAGE_VALID;
                shared->page_dirty[slotno] = true;
            }
            LWLockRelease(&shared->buffer_locks[slotno].lock);
        }
    }
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the page is safe.  If write_ok is false then we
 * will not return the page until it is not undergoing active I/O.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
LruReadPage(LruCtl ctl, int partitionno, int pageno, bool write_ok,
                  TransactionId xid)
{// #lizard forgives
    LruShared        shared = ctl->shared[partitionno];
    LWLock               *partitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;
    
    if (!LWLockHeldByMe(partitionLock))
        elog(ERROR, "partition lock is not held partionno %d", partitionno);

    if (enable_distri_print)
        elog(LOG, "read page partition %d pageno %d xid %d", partitionno, pageno, xid);
    /* Outer loop handles restart if we must wait for someone else's I/O */
    for (;;)
    {
        int                slotno;
        bool            ok;
        uint32             newHash;
        LruBufferTag    newTag;
        int             r;

        /* See if page already is in memory; if not, pick victim slot */
        slotno = LruSelectLRUPage(ctl, partitionno, pageno);

        /* Did we find the page in memory? */
        if (shared->page_number[slotno] == pageno &&
            shared->page_status[slotno] != LRU_PAGE_EMPTY)
        {
            /*
             * If page is still being read in, we must wait for I/O.  Likewise
             * if the page is being written and the caller said that's not OK.
             */
            if (shared->page_status[slotno] == LRU_PAGE_READ_IN_PROGRESS ||
                (shared->page_status[slotno] == LRU_PAGE_WRITE_IN_PROGRESS &&
                 !write_ok))
            {
                LruWaitIO(ctl, partitionno, slotno);
                /* Now we must recheck state from the top */
                continue;
            }
            /* Otherwise, it's ready to use */
            LruRecentlyUsed(shared, slotno);
            if (enable_distri_print)
                elog(LOG, "retry found pageno %d partition %d slotno %d.", pageno, partitionno, slotno);
            return slotno;
        }

        /* We found no match; assert we selected a freeable slot */
        Assert(shared->page_status[slotno] == LRU_PAGE_EMPTY ||
               (shared->page_status[slotno] == LRU_PAGE_VALID &&
                !shared->page_dirty[slotno]));

        
        /* Mark the slot read-busy */
            
        INIT_LRUBUFTAG(newTag, pageno);

        newHash = LruBufTableHashCode(&newTag);

#ifdef LRU_CHECK
        {
            int tmpslotno = LruBufTableLookup(&newTag, newHash);
            if (tmpslotno >= 0)
                elog(ERROR, "insert an exitsing pageno %d to slotno %d existing %d", pageno, slotno, tmpslotno);
        }
#endif
        if (enable_distri_print)
            elog(LOG, "insert slotno %d pageno %d partitionno %d zeropage readpage", slotno, pageno, partitionno);

        r = LruBufTableInsert(&newTag, newHash, slotno);
        if(r != -1)
        {
            elog(ERROR, "slot %d should be available for pageno %d in partition %d.", slotno, pageno, partitionno); 
        }
       
        if (enable_distri_print)
            elog(LOG, "insert slotno %d pageno %d readpage finish "UINT64_FORMAT, slotno, pageno, LogicalClockNow());

        shared->page_number[slotno] = pageno;
        shared->page_status[slotno] = LRU_PAGE_READ_IN_PROGRESS;
        shared->page_dirty[slotno] = false;


        /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
        LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

        /* Release control lock while doing I/O */
        LWLockRelease(partitionLock);

        /* Do the read */
        ok = LruPhysicalReadPage(ctl, partitionno, pageno, slotno);

        /* Set the LSNs for this newly read-in page to zero */
        LruZeroLSNs(ctl, partitionno, slotno);
        
        
        /* Re-acquire control lock and update page state */
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);
        
        Assert(shared->page_number[slotno] == pageno &&
               shared->page_status[slotno] == LRU_PAGE_READ_IN_PROGRESS &&
               !shared->page_dirty[slotno]);

        
        shared->page_status[slotno] = ok ? LRU_PAGE_VALID : LRU_PAGE_EMPTY;
        
        
        LWLockRelease(&shared->buffer_locks[slotno].lock);
        
        /* Now it's okay to ereport if we failed */
        if (!ok)
        {
            /* If failed, delete the hash entry for this slot */
            INIT_LRUBUFTAG(newTag, pageno);
            newHash = LruBufTableHashCode(&newTag);
            if (enable_distri_print)
                elog(LOG, "delete slotno %d pageno %d readpage", slotno, pageno); 
            LruBufTableDelete(&newTag, newHash);    
            LruReportIOError(ctl, pageno, xid);
        }
        LruRecentlyUsed(shared, slotno);
        if (enable_distri_print)
            elog(LOG, "read pageno %d partition %d slotno %d.", pageno, partitionno, slotno);
        return slotno;
    }
}

int PagenoMappingPartitionno(LruCtl ctl, int pageno)
{
    
    LruBufferTag     newTag;
    uint32        newHash;        /* hash value for newTag */
    int partitionno;
    
    INIT_LRUBUFTAG(newTag,  pageno);
    newHash = LruBufTableHashCode(&newTag);
    partitionno = BufHashPartition(newHash);
    return partitionno;
}

LWLock * GetPartitionLock(LruCtl ctl, int partitionno)
{
    LruShared    shared = ctl->shared[partitionno];
    LWLock        *partitionLock;

    partitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;
    return partitionLock;

}


/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
int
LruReadPage_ReadOnly(LruCtl ctl, int partitionno, int pageno, TransactionId xid)
{
    LruShared    shared;
    int            slotno;
    LruBufferTag     newTag;
    uint32        newHash;        /* hash value for newTag */
    LWLock       *newPartitionLock;    /* buffer partition lock for it */
    
    
    INIT_LRUBUFTAG(newTag,  pageno);
    newHash = LruBufTableHashCode(&newTag);
    partitionno = BufHashPartition(newHash);
    shared = ctl->shared[partitionno];
    
    /* Try to find the page while holding only shared lock */
    newPartitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;

    LWLockAcquire(newPartitionLock, LW_SHARED);
    
    slotno = LruBufTableLookup(&newTag, newHash);
    
    /* See if page is already in a buffer */
    if(slotno >= 0)
    {
        if (shared->page_number[slotno] == pageno &&
            shared->page_status[slotno] != LRU_PAGE_EMPTY &&
            shared->page_status[slotno] != LRU_PAGE_READ_IN_PROGRESS)
        {
            /* See comments for SlruRecentlyUsed macro */
            LruRecentlyUsed(shared, slotno);
            if (enable_distri_print)
                elog(LOG, "found pageno %d partition %d slotno %d.", pageno, partitionno, slotno);
            return slotno;
        }
    }
    /* No luck, so switch to normal exclusive lock and do regular read */
    LWLockRelease(newPartitionLock);
    LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
    
    return LruReadPage(ctl, partitionno, pageno, true, xid);
}

/*
 * This is similar to LruReadPage_ReadOnly_Locked, but differs in that 
 * it does only find the slotno for the target pageno if the page is 
 * buffered.
 * Otherwise, return -1
 */ 
int
LruLookupSlotno_Locked(LruCtl ctl, int partitionno, int pageno)
{
     LruShared    shared = ctl->shared[partitionno];
    int            slotno;
    LruBufferTag     newTag;
    uint32        newHash;        /* hash value for newTag */
    LWLock       *partitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;    /* buffer partition lock for it */

    INIT_LRUBUFTAG(newTag,  pageno);
    newHash = LruBufTableHashCode(&newTag);
    if (BufHashPartition(newHash) != partitionno)
        elog(ERROR, "partition error %d expected %d", BufHashPartition(newHash), partitionno);
    
	Assert(LWLockHeldByMe(partitionLock));

    if (!LWLockHeldByMe(partitionLock))
        elog(ERROR, "partition lock is not held partionno %d", partitionno);

    slotno = LruBufTableLookup(&newTag, newHash);
    /* See if page is already in a buffer */
    if(slotno >= 0)
    {
        if (shared->page_number[slotno] == pageno &&
            shared->page_status[slotno] != LRU_PAGE_EMPTY &&
            shared->page_status[slotno] != LRU_PAGE_READ_IN_PROGRESS)
        {
            /* See comments for SlruRecentlyUsed macro */
            LruRecentlyUsed(shared, slotno);
            if (enable_distri_print)
                elog(LOG, "found pageno %d partition %d slotno %d.", pageno, partitionno, slotno);
            return slotno;
        }
    }

    return -1;
}

int
LruReadPage_ReadOnly_Locked(LruCtl ctl, int partitionno, int pageno,  bool write_ok, TransactionId xid)
{
    LruShared    shared = ctl->shared[partitionno];
    int            slotno;
    LruBufferTag     newTag;
    uint32        newHash;        /* hash value for newTag */
    LWLock       *partitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;    /* buffer partition lock for it */

    INIT_LRUBUFTAG(newTag,  pageno);
    newHash = LruBufTableHashCode(&newTag);
    if (BufHashPartition(newHash) != partitionno)
        elog(ERROR, "partition error %d expected %d", BufHashPartition(newHash), partitionno);
    
	Assert(LWLockHeldByMe(partitionLock));

    if (!LWLockHeldByMe(partitionLock))
        elog(ERROR, "partition lock is not held partionno %d", partitionno);

    slotno = LruBufTableLookup(&newTag, newHash);
    /* See if page is already in a buffer */
    if(slotno >= 0)
    {
        if (shared->page_number[slotno] == pageno &&
            shared->page_status[slotno] != LRU_PAGE_EMPTY &&
            shared->page_status[slotno] != LRU_PAGE_READ_IN_PROGRESS)
        {
            /* See comments for SlruRecentlyUsed macro */
            LruRecentlyUsed(shared, slotno);
            if (enable_distri_print)
                elog(LOG, "found pageno %d partition %d slotno %d.", pageno, partitionno, slotno);
            return slotno;
        }
    }
    /* No luck, so switch to normal exclusive lock and do regular read */
   
    
    /* Try to find the page while holding only shared lock */
    LWLockRelease(partitionLock);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);
    
    return LruReadPage(ctl, partitionno, pageno, write_ok, xid);
}

/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).  However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
LruInternalWritePage(LruCtl ctl, int partitionno, int slotno, LruFlushPt fdata)
{// #lizard forgives
    LruShared    shared = ctl->shared[partitionno];
    int            pageno = shared->page_number[slotno];
    bool        ok;
    bool        globalLockHeld = false;
    LWLock        *partitionLock = GetPartitionLock(ctl, partitionno);

    /* If a write is in progress, wait for it to finish */
    while (shared->page_status[slotno] == LRU_PAGE_WRITE_IN_PROGRESS &&
           shared->page_number[slotno] == pageno)
    {
        LruWaitIO(ctl, partitionno, slotno);
    }

    /*
     * Do nothing if page is not dirty, or if buffer no longer contains the
     * same page we were called for.
     */
    if (!shared->page_dirty[slotno] ||
        shared->page_status[slotno] != LRU_PAGE_VALID ||
        shared->page_number[slotno] != pageno)
        return;

    /*
     * Mark the slot write-busy, and clear the dirtybit.  After this point, a
     * transaction status update on this page will mark it dirty again.
     */
    shared->page_status[slotno] = LRU_PAGE_WRITE_IN_PROGRESS;
    shared->page_dirty[slotno] = false;

    /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
    LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

    LWLockRelease(partitionLock);
     /* Release control lock while doing I/O */
    if(LWLockHeldByMe(ctl->global_shared->ControlLock))
    {
        globalLockHeld = true;
        LWLockRelease(ctl->global_shared->ControlLock);

    }


    /* Do the write */
    ok = LruPhysicalWritePage(ctl, partitionno, pageno, slotno, fdata);

    /* If we failed, and we're in a flush, better close the files */
    if (!ok && fdata)
    {
        int            i;

        for (i = 0; i < fdata->num_files; i++)
            CloseTransientFile(fdata->fd[i]);
    }

    /* Re-acquire control lock and update page state */
    if (globalLockHeld)
    {
        LWLockAcquire(ctl->global_shared->ControlLock, LW_EXCLUSIVE);
    }

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);
   

    Assert(shared->page_number[slotno] == pageno &&
           shared->page_status[slotno] == LRU_PAGE_WRITE_IN_PROGRESS);

    /* If we failed to write, mark the page dirty again */
    if (!ok)
        shared->page_dirty[slotno] = true;

    shared->page_status[slotno] = LRU_PAGE_VALID;

    LWLockRelease(&shared->buffer_locks[slotno].lock);
    
    if (fdata)
        fdata->flushed_dirty_pages++;

    /* Now it's okay to ereport if we failed */
    if (!ok)
        LruReportIOError(ctl, pageno, InvalidTransactionId);
}

/*
 * Wrapper of SlruInternalWritePage, for external callers.
 * fdata is always passed a NULL here.
 */
void
LruWritePage(LruCtl ctl, int partitionno, int slotno)
{
    LruInternalWritePage(ctl, partitionno, slotno, NULL);
}

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
LruDoesPhysicalPageExist(LruCtl ctl, int pageno)
{
    int            segno = pageno / LRU_PAGES_PER_SEGMENT;
    int            rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int            offset = rpageno * BLCKSZ;
    char        path[MAXPGPATH];
    int            fd;
    bool        result;
    off_t        endpos;

    LruFileName(ctl, path, segno);

    fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
    if (fd < 0)
    {
        
        /* expected: file doesn't exist */
        if (errno == ENOENT)
            return false;

        /* report error normally */
        lru_errcause = LRU_OPEN_FAILED;
        lru_errno = errno;
        LruReportIOError(ctl, pageno, 0);
    }

    if ((endpos = lseek(fd, 0, SEEK_END)) < 0)
    {
        lru_errcause = LRU_OPEN_FAILED;
        lru_errno = errno;
        LruReportIOError(ctl, pageno, 0);
    }

    result = endpos >= (off_t) (offset + BLCKSZ);

    CloseTransientFile(fd);
    return result;
}

/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return FALSE and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
static bool
LruPhysicalReadPage(LruCtl ctl, int partitionno, int pageno, int slotno)
{
    LruShared    shared = ctl->shared[partitionno];
    int            segno = pageno / LRU_PAGES_PER_SEGMENT;
    int            rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int            offset = rpageno * BLCKSZ;
    char        path[MAXPGPATH];
    int            fd;

    
    LruFileName(ctl, path, segno);
    if (enable_distri_print)
        elog(LOG, "read page pageno %d slotno %d patitionno %d path %s offset %d.", 
                                            pageno, slotno, partitionno, path, offset);
    /*
     * In a crash-and-restart situation, it's possible for us to receive
     * commands to set the commit status of transactions whose bits are in
     * already-truncated segments of the commit log (see notes in
     * SlruPhysicalWritePage).  Hence, if we are InRecovery, allow the case
     * where the file doesn't exist, and return zeroes instead.
     */
    fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
    if (fd < 0)
    {
        if (errno != ENOENT || !InRecovery)
        {
            lru_errcause = LRU_OPEN_FAILED;
            lru_errno = errno;
            return false;
        }

        ereport(LOG,
                (errmsg("file \"%s\" doesn't exist, reading as zeroes",
                        path)));
        MemSet(shared->page_buffer[slotno], 0, BLCKSZ);
        return true;
    }

    if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
    {
        lru_errcause = LRU_SEEK_FAILED;
        lru_errno = errno;
        CloseTransientFile(fd);
        return false;
    }

    errno = 0;
    pgstat_report_wait_start(WAIT_EVENT_SLRU_READ);
    if (read(fd, shared->page_buffer[slotno], BLCKSZ) != BLCKSZ)
    {
        elog(ERROR, "read fails path %s partitionno %d slotno %d pageno %d ", 
                                                path, partitionno, slotno, pageno);
        pgstat_report_wait_end();
        lru_errcause = LRU_READ_FAILED;
        lru_errno = errno;
        CloseTransientFile(fd);
        return false;
    }
    pgstat_report_wait_end();

    if (CloseTransientFile(fd))
    {
        lru_errcause = LRU_CLOSE_FAILED;
        lru_errno = errno;
        return false;
    }

    return true;
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return FALSE and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * SimpleLruFlush, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * SimpleLruFlush.
 */
static bool
LruPhysicalWritePage(LruCtl ctl, int partitionno,  int pageno, int slotno, LruFlushPt fdata)
{// #lizard forgives
    LruShared    shared = ctl->shared[partitionno];
    int            segno = pageno / LRU_PAGES_PER_SEGMENT;
    int            rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int            offset = rpageno * BLCKSZ;
    char        path[MAXPGPATH];
    int            fd = -1;

    /*
     * Honor the write-WAL-before-data rule, if appropriate, so that we do not
     * write out data before associated WAL records.  This is the same action
     * performed during FlushBuffer() in the main buffer manager.
     */
    if (shared->group_lsn != NULL)
    {
        /*
         * We must determine the largest async-commit LSN for the page. This
         * is a bit tedious, but since this entire function is a slow path
         * anyway, it seems better to do this here than to maintain a per-page
         * LSN variable (which'd need an extra comparison in the
         * transaction-commit path).
         */
        XLogRecPtr    max_lsn;
        int            lsnindex,
                    lsnoff;

        lsnindex = slotno * shared->lsn_groups_per_page;
        max_lsn = shared->group_lsn[lsnindex++];
        for (lsnoff = 1; lsnoff < shared->lsn_groups_per_page; lsnoff++)
        {
            XLogRecPtr    this_lsn = shared->group_lsn[lsnindex++];

            if (max_lsn < this_lsn)
                max_lsn = this_lsn;
        }

        if (!XLogRecPtrIsInvalid(max_lsn))
        {
            /*
             * As noted above, elog(ERROR) is not acceptable here, so if
             * XLogFlush were to fail, we must PANIC.  This isn't much of a
             * restriction because XLogFlush is just about all critical
             * section anyway, but let's make sure.
             */
            START_CRIT_SECTION();
            XLogFlush(max_lsn);
            END_CRIT_SECTION();
        }
    }

    /*
     * During a Flush, we may already have the desired file open.
     */
    if (fdata)
    {
        int            i;

        for (i = 0; i < fdata->num_files; i++)
        {
            if (fdata->segno[i] == segno)
            {
                fd = fdata->fd[i];
                break;
            }
        }
    }

    if (fd < 0)
    {
        /*
         * If the file doesn't already exist, we should create it.  It is
         * possible for this to need to happen when writing a page that's not
         * first in its segment; we assume the OS can cope with that. (Note:
         * it might seem that it'd be okay to create files only when
         * SimpleLruZeroPage is called for the first page of a segment.
         * However, if after a crash and restart the REDO logic elects to
         * replay the log from a checkpoint before the latest one, then it's
         * possible that we will get commands to set transaction status of
         * transactions that have already been truncated from the commit log.
         * Easiest way to deal with that is to accept references to
         * nonexistent files here and in SlruPhysicalReadPage.)
         *
         * Note: it is possible for more than one backend to be executing this
         * code simultaneously for different pages of the same file. Hence,
         * don't use O_EXCL or O_TRUNC or anything like that.
         */
        LruFileName(ctl, path, segno);
        if (enable_distri_print)
            elog(LOG, "LruPhysicalWritePage: open file %s", path);
        fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
        if (fd < 0)
        {
            elog(LOG, "LruPhysicalWritePage: open file fails %s", path);
            lru_errcause = LRU_OPEN_FAILED;
            lru_errno = errno;
            return false;
        }

        if (fdata)
        {
            if (fdata->num_files < MAX_FLUSH_BUFFERS)
            {
                fdata->fd[fdata->num_files] = fd;
                fdata->segno[fdata->num_files] = segno;
                fdata->num_files++;
            }
            else
            {
                /*
                 * In the unlikely event that we exceed MAX_FLUSH_BUFFERS,
                 * fall back to treating it as a standalone write.
                 */
                fdata->cache_miss_count++;
                fdata = NULL;
            }
        }
    }

    if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
    {
        lru_errcause = LRU_SEEK_FAILED;
        lru_errno = errno;
        if (!fdata)
            CloseTransientFile(fd);
        return false;
    }

    errno = 0;
    pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
    if (enable_distri_print)
        elog(LOG, "LruPhysicalWritePage: WRITE file pageno %d partitionno %d path %s", pageno, partitionno, path);
    if (write(fd, shared->page_buffer[slotno], BLCKSZ) != BLCKSZ)
    {
        pgstat_report_wait_end();
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;
        lru_errcause = LRU_WRITE_FAILED;
        lru_errno = errno;
        if (!fdata)
            CloseTransientFile(fd);
        return false;
    }
    pgstat_report_wait_end();

    /*
     * If not part of Flush, need to fsync now.  We assume this happens
     * infrequently enough that it's not a performance issue.
     */
    if (!fdata)
    {
        pgstat_report_wait_start(WAIT_EVENT_SLRU_SYNC);
        if (ctl->do_fsync && pg_fsync(fd))
        {
            pgstat_report_wait_end();
            lru_errcause = LRU_FSYNC_FAILED;
            lru_errno = errno;
            CloseTransientFile(fd);
            return false;
        }
        pgstat_report_wait_end();

        if (CloseTransientFile(fd))
        {
            lru_errcause = LRU_CLOSE_FAILED;
            lru_errno = errno;
            return false;
        }
    }

    return true;
}

/*
 * Issue the error message after failure of SlruPhysicalReadPage or
 * SlruPhysicalWritePage.  Call this after cleaning up shared-memory state.
 */
static void
LruReportIOError(LruCtl ctl, int pageno, TransactionId xid)
{
    int            segno = pageno / LRU_PAGES_PER_SEGMENT;
    int            rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int            offset = rpageno * BLCKSZ;
    char        path[MAXPGPATH];

    LruFileName(ctl, path, segno);
    errno = lru_errno;
    switch (lru_errcause)
    {
        case LRU_OPEN_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not open file \"%s\": %m.", path)));
            break;
        case LRU_SEEK_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not seek in file \"%s\" to offset %u: %m.",
                               path, offset)));
            break;
        case LRU_READ_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not read from file \"%s\" at offset %u pageno %d: %m.",
                               path, offset, pageno)));
            break;
        case LRU_WRITE_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not write to file \"%s\" at offset %u: %m.",
                               path, offset)));
            break;
        case LRU_FSYNC_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not fsync file \"%s\": %m.",
                               path)));
            break;
        case LRU_CLOSE_FAILED:
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not access status of transaction %u", xid),
                     errdetail("Could not close file \"%s\": %m.",
                               path)));
            break;
        default:
            /* can't get here, we trust */
            elog(ERROR, "unrecognized SimpleLru error cause: %d",
                 (int) lru_errcause);
            break;
    }
}

/*
 * Select the slot to re-use when we need a free slot.
 *
 * The target page number is passed because we need to consider the
 * possibility that some other process reads in the target page while
 * we are doing I/O to free a slot.  Hence, check or recheck to see if
 * any slot already holds the target page, and return that slot if so.
 * Thus, the returned slot is *either* a slot already holding the pageno
 * (could be any state except EMPTY), *or* a freeable slot (state EMPTY
 * or CLEAN).
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
LruSelectLRUPage(LruCtl ctl, int partitionno, int pageno)
{// #lizard forgives
    LruShared    shared = ctl->shared[partitionno];
    LWLock               *partitionLock = &shared->buffer_locks[PARTITION_LOCK_IDX(shared)].lock;


    if (enable_distri_print)
        elog(LOG, "find free slotno for pageno %d",  pageno);
    /* Outer loop handles restart after I/O */
    for (;;)
    {
        int            slotno;
        int            cur_count;
        int            bestvalidslot = 0;    /* keep compiler quiet */
        int            best_valid_delta = -1;
        int            best_valid_page_number = 0; /* keep compiler quiet */
        int            bestinvalidslot = 0;    /* keep compiler quiet */
        int            best_invalid_delta = -1;
        int            best_invalid_page_number = 0;    /* keep compiler quiet */    
        LruBufferTag    newTag;
        uint32            newHash;        /* hash value for newTag */

        if (!LWLockHeldByMe(partitionLock))
            elog(ERROR, "partition lock is not held partionno %d", partitionno);

        INIT_LRUBUFTAG(newTag,  pageno);
        newHash = LruBufTableHashCode(&newTag);
    
        /* Try to find the page while holding only shared lock */
        slotno = LruBufTableLookup(&newTag, newHash);
        if(slotno >= 0)
        {
            if (shared->page_number[slotno] == pageno &&
                shared->page_status[slotno] != LRU_PAGE_EMPTY)
            {
                if (enable_distri_print)
                    elog(LOG, "find slotno %d pageno %d", slotno, pageno);
                return slotno;
            }
        }

        /*
         * If we find any EMPTY slot, just select that one. Else choose a
         * victim page to replace.  We normally take the least recently used
         * valid page, but we will never take the slot containing
         * latest_page_number, even if it appears least recently used.  We
         * will select a slot that is already I/O busy only if there is no
         * other choice: a read-busy slot will not be least recently used once
         * the read finishes, and waiting for an I/O on a write-busy slot is
         * inferior to just picking some other slot.  Testing shows the slot
         * we pick instead will often be clean, allowing us to begin a read at
         * once.
         *
         * Normally the page_lru_count values will all be different and so
         * there will be a well-defined LRU page.  But since we allow
         * concurrent execution of SlruRecentlyUsed() within
         * SimpleLruReadPage_ReadOnly(), it is possible that multiple pages
         * acquire the same lru_count values.  In that case we break ties by
         * choosing the furthest-back page.
         *
         * Notice that this next line forcibly advances cur_lru_count to a
         * value that is certainly beyond any value that will be in the
         * page_lru_count array after the loop finishes.  This ensures that
         * the next execution of SlruRecentlyUsed will mark the page newly
         * used, even if it's for a page that has the current counter value.
         * That gets us back on the path to having good data when there are
         * multiple pages with the same lru_count.
         */
        cur_count = (shared->cur_lru_count)++;
        for (slotno = 0; slotno < shared->num_slots; slotno++)
        {
            int            this_delta;
            int            this_page_number;

            if (shared->page_status[slotno] == LRU_PAGE_EMPTY)
            {
                if (enable_distri_print)
                    elog(LOG, "find empty slotno %d for pageno %d", slotno, pageno);
                return slotno;
            }
            this_delta = cur_count - shared->page_lru_count[slotno];
            if (this_delta < 0)
            {
                /*
                 * Clean up in case shared updates have caused cur_count
                 * increments to get "lost".  We back off the page counts,
                 * rather than trying to increase cur_count, to avoid any
                 * question of infinite loops or failure in the presence of
                 * wrapped-around counts.
                 */
                shared->page_lru_count[slotno] = cur_count;
                this_delta = 0;
            }
            this_page_number = shared->page_number[slotno];

            if (this_page_number == shared->latest_page_number)
            {
                continue;
            }
            
            if (shared->page_status[slotno] == LRU_PAGE_VALID)
            {
                if (this_delta > best_valid_delta ||
                    (this_delta == best_valid_delta &&
                     ctl->PagePrecedes(this_page_number,
                                       best_valid_page_number)))
                {
                    bestvalidslot = slotno;
                    best_valid_delta = this_delta;
                    best_valid_page_number = this_page_number;
                }
            }
            else
            {
                if (this_delta > best_invalid_delta ||
                    (this_delta == best_invalid_delta &&
                     ctl->PagePrecedes(this_page_number,
                                       best_invalid_page_number)))
                {
                    bestinvalidslot = slotno;
                    best_invalid_delta = this_delta;
                    best_invalid_page_number = this_page_number;
                }
            }
        }

        /*
         * If all pages (except possibly the latest one) are I/O busy, we'll
         * have to wait for an I/O to complete and then retry.  In that
         * unhappy case, we choose to wait for the I/O on the least recently
         * used slot, on the assumption that it was likely initiated first of
         * all the I/Os in progress and may therefore finish first.
         */
        if (best_valid_delta < 0)
        {
            LruWaitIO(ctl, partitionno, bestinvalidslot);
            continue;
        }


        /*
         * If the selected page is clean, we're set.
         */
        if (!shared->page_dirty[bestvalidslot]){
            int             oldPartitionno;
            LruBufferTag    oldTag;            /* previous identity of selected buffer */
            int                oldPageno;
            uint32            oldHash;        /* hash value for oldTag */

            oldPageno = shared->page_number[bestvalidslot];
            Assert(oldPageno != pageno);
            INIT_LRUBUFTAG(oldTag, oldPageno);
            oldHash = LruBufTableHashCode(&oldTag);
            oldPartitionno = BufHashPartition(oldHash);
            Assert(oldPartitionno == partitionno);
            if(oldPartitionno != partitionno)
                elog(ERROR, "partitionno differs old part %d page %d new part %d page %d", 
                                    oldPartitionno, partitionno, oldPageno, pageno);
            if (oldPageno == pageno)
                elog(ERROR, "replace the slot for the same page %d status %d", pageno, shared->page_status[bestvalidslot]);

            if (enable_distri_print)
                elog(LOG, "delete slotno %d pageno %d partitionno %d new pageno %d selectlru "UINT64_FORMAT, 
                        bestvalidslot, oldPageno, partitionno, pageno, LogicalClockNow());

            LruBufTableDelete(&oldTag, oldHash);
            
            if (enable_distri_print)
                elog(LOG, "delete slotno %d pageno %d partitionno %d new pageno %d selectlru finished "UINT64_FORMAT, 
                        bestvalidslot, oldPageno, partitionno, pageno, LogicalClockNow());

            return bestvalidslot;

        }

        /*
         * Write the page.
         */
        LruInternalWritePage(ctl, partitionno, bestvalidslot, NULL);

        /*
         * Now loop back and try again.  This is the easiest way of dealing
         * with corner cases such as the victim page being re-dirtied while we
         * wrote it.
         */
    }
}

/*
 * Flush dirty pages to disk during checkpoint or database shutdown
 */
static void
LruFlushPartition(LruCtl ctl, bool allow_redirtied, int partitionno, LruFlushPt sum_data)
{
    int slotno;
    int pageno = 0;
    int i;
    bool ok;

    LruFlushData fdata;
    LruShared shared = ctl->shared[partitionno];
    LWLock *partitionlock = GetPartitionLock(ctl, partitionno);

    memset(&fdata, 0, sizeof(LruFlushData));

    LWLockAcquire(partitionlock, LW_EXCLUSIVE);
    for (slotno = 0; slotno < shared->num_slots; slotno++)
    {
        LruInternalWritePage(ctl, partitionno, slotno, &fdata);

        /*
            * In some places (e.g. checkpoints), we cannot assert that the slot
            * is clean now, since another process might have re-dirtied it
            * already.  That's okay.
            */
        Assert(allow_redirtied ||
               shared->page_status[slotno] == LRU_PAGE_EMPTY ||
               (shared->page_status[slotno] == LRU_PAGE_VALID &&
                !shared->page_dirty[slotno]));
    }
    LWLockRelease(partitionlock);

    /*
     * Now fsync and close any files that were open
     */
    ok = true;
    for (i = 0; i < fdata.num_files; i++)
    {
        pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
        if (ctl->do_fsync && pg_fsync(fdata.fd[i]))
        {
            lru_errcause = LRU_FSYNC_FAILED;
            lru_errno = errno;
            pageno = fdata.segno[i] * LRU_PAGES_PER_SEGMENT;
            ok = false;
        }
        pgstat_report_wait_end();

        if (CloseTransientFile(fdata.fd[i]))
        {
            lru_errcause = LRU_CLOSE_FAILED;
            lru_errno = errno;
            pageno = fdata.segno[i] * LRU_PAGES_PER_SEGMENT;
            ok = false;
        }
    }
    
    sum_data->num_files += fdata.num_files;
    sum_data->cache_miss_count += fdata.cache_miss_count;
    sum_data->flushed_dirty_pages += fdata.flushed_dirty_pages;
   
    if (!ok)
        LruReportIOError(ctl, pageno, InvalidTransactionId);
}

void 
LruFlush(LruCtl ctl, bool allow_redirtied)
{
    LruFlushData    fdata;
    TimestampTz     start = GetCurrentTimestamp();
    TimestampTz     end;
    long            elapsed_secs;
    int             elapsed_micros;
    
    memset(&fdata, 0, sizeof(LruFlushData));
    
    int i;
    for (i = 0; i < NUM_PARTITIONS; i++)
    {
        LruFlushPartition(ctl, allow_redirtied, i, &fdata);
    }
    
    end = GetCurrentTimestamp();
    TimestampDifference(start, end, &elapsed_secs, &elapsed_micros);

    ereport(LOG,
            (errmsg("LruFlush info: fd_cache_miss_count %d, flushed_dirty_pages %d, "
                    "touched files %d, elapsed "INT64_FORMAT" millis",
                    fdata.cache_miss_count, fdata.flushed_dirty_pages,
                    fdata.num_files, elapsed_secs*1000+elapsed_micros/1000)));
}

/*
 * Remove all segments before the one holding the passed page number
 */
static void
LruTruncatePartition(LruCtl ctl, int partitionno, int cutoffPage)
{// #lizard forgives
    LruShared    shared = ctl->shared[partitionno];
    int            slotno;
    LWLock        *partitionlock = GetPartitionLock(ctl, partitionno);
        

    /*
     * The cutoff point is the start of the segment containing cutoffPage.
     */
    cutoffPage -= cutoffPage % LRU_PAGES_PER_SEGMENT;

    /*
     * Scan shared memory and remove any pages preceding the cutoff page, to
     * ensure we won't rewrite them later.  (Since this is normally called in
     * or just after a checkpoint, any dirty pages should have been flushed
     * already ... we're just being extra careful here.)
     */

    LWLockAcquire(ctl->global_shared->ControlLock, LW_EXCLUSIVE);
    LWLockAcquire(partitionlock, LW_EXCLUSIVE);
    
restart:;

    /*
     * While we are holding the lock, make an important safety check: the
     * planned cutoff point must be <= the current endpoint page. Otherwise we
     * have already wrapped around, and proceeding with the truncation would
     * risk removing the current segment.
     */
    
    if (ctl->PagePrecedes(ctl->global_shared->latest_page_number, cutoffPage))
    {
        LWLockRelease(partitionlock);
        LWLockRelease(ctl->global_shared->ControlLock);

        ereport(LOG,
                (errmsg("could not truncate directory \"%s\": apparent wraparound",
                        ctl->Dir)));
        return;
    }

    for (slotno = 0; slotno < shared->num_slots; slotno++)
    {
        if (shared->page_status[slotno] == LRU_PAGE_EMPTY)
            continue;
        if (!ctl->PagePrecedes(shared->page_number[slotno], cutoffPage))
            continue;

        /*
         * If page is clean, just change state to EMPTY (expected case).
         */
        if (shared->page_status[slotno] == LRU_PAGE_VALID &&
            !shared->page_dirty[slotno])
        {
            int             oldPartitionno PG_USED_FOR_ASSERTS_ONLY;
            LruBufferTag    oldTag;            /* previous identity of selected buffer */
            int                oldPageno;
            uint32            oldHash;        /* hash value for oldTag */
            
            oldPageno = shared->page_number[slotno];
            INIT_LRUBUFTAG(oldTag, oldPageno);
            oldHash = LruBufTableHashCode(&oldTag);
            oldPartitionno = BufHashPartition(oldHash);
            Assert(oldPartitionno == partitionno);
            
            if (enable_distri_print)
                elog(LOG, "delete slotno %d pageno %d truncate", slotno, oldPageno);

            LruBufTableDelete(&oldTag, oldHash);
            shared->page_status[slotno] = LRU_PAGE_EMPTY;
            if (enable_distri_print)
                elog(LOG, "truncate pageno %d partition %d slotno %d cutoffpage %d.", 
                                oldPageno, partitionno, slotno, cutoffPage);
            continue;
        }

        /*
         * Hmm, we have (or may have) I/O operations acting on the page, so
         * we've got to wait for them to finish and then start again. This is
         * the same logic as in SlruSelectLRUPage.  (XXX if page is dirty,
         * wouldn't it be OK to just discard it without writing it?  For now,
         * keep the logic the same as it was.)
         */
        
        if (shared->page_status[slotno] == LRU_PAGE_VALID)
            LruInternalWritePage(ctl, partitionno, slotno, NULL);
        else
            LruWaitIO(ctl, partitionno, slotno);

        goto restart;
    }
    LWLockRelease(partitionlock);
    LWLockRelease(ctl->global_shared->ControlLock);
}

void LruTruncate(LruCtl ctl, int cutoffPage)
{
    int partitionno;
    
    for(partitionno = 0; partitionno < NUM_PARTITIONS; partitionno++)
    {
        
        LruTruncatePartition(ctl, partitionno, cutoffPage);

    }
    /* Now we can remove the old segment(s) */
    cutoffPage -= cutoffPage % LRU_PAGES_PER_SEGMENT;
    (void) LruScanDirectory(ctl, LruScanDirCbDeleteCutoff, &cutoffPage);
}
/*
 * Delete an individual SLRU segment, identified by the filename.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
LruInternalDeleteSegment(LruCtl ctl, char *filename)
{
    char        path[MAXPGPATH];

    snprintf(path, MAXPGPATH, "%s/%s", ctl->Dir, filename);
    if(enable_distri_print)
    {
        ereport(LOG,
            (errmsg("removing file \"%s\"", path)));
    }
    unlink(path);
}


/*
 * SlruScanDirectory callback
 *        This callback reports true if there's any segment prior to the one
 *        containing the page passed as "data".
 */
bool
LruScanDirCbReportPresence(LruCtl ctl, char *filename, int segpage, void *data)
{
    int            cutoffPage = *(int *) data;

    cutoffPage -= cutoffPage % LRU_PAGES_PER_SEGMENT;

    if (ctl->PagePrecedes(segpage, cutoffPage))
        return true;            /* found one; don't iterate any more */

    return false;                /* keep going */
}

/*
 * SlruScanDirectory callback.
 *        This callback deletes segments prior to the one passed in as "data".
 */
static bool
LruScanDirCbDeleteCutoff(LruCtl ctl, char *filename, int segpage, void *data)
{
    int            cutoffPage = *(int *) data;

    if (ctl->PagePrecedes(segpage, cutoffPage))
        LruInternalDeleteSegment(ctl, filename);

    return false;                /* keep going */
}

/*
 * SlruScanDirectory callback.
 *        This callback deletes all segments.
 */
bool
LruScanDirCbDeleteAll(LruCtl ctl, char *filename, int segpage, void *data)
{
    LruInternalDeleteSegment(ctl, filename);

    return false;                /* keep going */
}

/*
 * Scan the SimpleLRU directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the SlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
LruScanDirectory(LruCtl ctl, LruScanCallback callback, void *data)
{
    bool        retval = false;
    DIR           *cldir;
    struct dirent *clde;
    int            segno;
    int            segpage;

    cldir = AllocateDir(ctl->Dir);
    while ((clde = ReadDir(cldir, ctl->Dir)) != NULL)
    {
        size_t        len;

        len = strlen(clde->d_name);

        if ((len == 4 || len == 5 || len == 6) &&
            strspn(clde->d_name, "0123456789ABCDEF") == len)
        {
            segno = (int) strtol(clde->d_name, NULL, 16);
            segpage = segno * LRU_PAGES_PER_SEGMENT;

            elog(DEBUG2, "LruScanDirectory invoking callback on %s/%s",
                 ctl->Dir, clde->d_name);
            retval = callback(ctl, clde->d_name, segpage, data);
            if (retval)
                break;
        }
    }
    FreeDir(cldir);

    return retval;
}
