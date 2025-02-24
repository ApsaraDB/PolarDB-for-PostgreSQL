/*-------------------------------------------------------------------------
 *
 * polar_login_history.c
 *
 * IDENTIFICATION
 *    external/polar_login_history/polar_login_history.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "commands/user.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "replication/walsender.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define POLAR_LOGIN_HISTORY_STRING_LENGTH 256

#define POLAR_LOGIN_HISTORY_FILE "polar_login_history"

typedef struct polar_login_history_lwlock
{
    LWLock *lock; /* protects hashtable search/modification */
} polar_login_history_lwlock;

/* Use a hash table to record the user's login history */
typedef struct polar_login_history_entry
{
    Oid         roloid;    /* role oid */
    TimestampTz logintime; /* last login time */
    char        ip[POLAR_LOGIN_HISTORY_STRING_LENGTH];          /* last login ip */
    char        application[POLAR_LOGIN_HISTORY_STRING_LENGTH]; /* application of the last login */
    uint8       failcount; /* number of failed logins since the last successful login */
} polar_login_history_entry;

static polar_login_history_lwlock *polar_login_history_lock = NULL;
static HTAB *polar_login_history_hash = NULL;

static bool polar_enable_login_history;
static int polar_max_login_history;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

void _PG_init(void);

/*
 * Estimate shared memory space needed.
 */
static Size
polar_login_history_memsize(void)
{
    Size size;

    size = MAXALIGN(sizeof(polar_login_history_lwlock));
    size = add_size(size, hash_estimate_size(polar_max_login_history, sizeof(polar_login_history_entry)));

    return size;
}

/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in polar_login_history_shmem_startup().
 */
static void
polar_login_history_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(polar_login_history_memsize());
    RequestNamedLWLockTranche("polar_login_history", 1);
}

static void
polar_save_information(void)
{
    FILE *file = NULL;
    int32 num_entries;
    HASH_SEQ_STATUS hash_seq;
    polar_login_history_entry *entry;

    /* Safety check ... shouldn't get here unless shmem is set up */
    if (!polar_login_history_lock || !polar_login_history_hash)
        return;

    file = AllocateFile(POLAR_LOGIN_HISTORY_FILE ".tmp", PG_BINARY_W);
    if (file == NULL)
        goto error;

    num_entries = hash_get_num_entries(polar_login_history_hash);
    if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
        goto error;

    /* Serialize to disk */
    hash_seq_init(&hash_seq, polar_login_history_hash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        if (fwrite(entry, sizeof(polar_login_history_entry), 1, file) != 1)
        {
            /* note: we assume hash_seq_term won't change errno */
            hash_seq_term(&hash_seq);
            goto error;
        }
    }

    if (FreeFile(file))
    {
        file = NULL;
        goto error;
    }

    /* Rename file into place, so we atomically replace any old one */
    (void) durable_rename(POLAR_LOGIN_HISTORY_FILE ".tmp", POLAR_LOGIN_HISTORY_FILE, LOG);

    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write file \"%s\": %m",
                    POLAR_LOGIN_HISTORY_FILE ".tmp")));
    if (file)
        FreeFile(file);
    unlink(POLAR_LOGIN_HISTORY_FILE ".tmp");
}

static void
polar_load_information(void)
{
    FILE *file = NULL;
    int32 num;

    /* Attempt to load old statistics from the dump file */
    file = AllocateFile(POLAR_LOGIN_HISTORY_FILE, PG_BINARY_R);
    if (file == NULL)
    {
        if (errno != ENOENT)
            goto read_error;
        return;
    }

    if (fread(&num, sizeof(int32), 1, file) != 1)
        goto read_error;

    for (int32 i = 0; i < num; i++)
    {
        polar_login_history_entry temp;
        polar_login_history_entry *entry;

        if (fread(&temp, sizeof(polar_login_history_entry), 1, file) != 1)
            goto read_error;

        /* Find or create an entry with desired hash code */
        entry = (polar_login_history_entry *) hash_search(polar_login_history_hash, &temp.roloid, HASH_ENTER_NULL, NULL);
        if (!entry)
        {
            /* There is no space in the hash table and some information will be lost */
            /* polar_login_history is not modified when you exit the process */
            polar_login_history_hash = NULL;
            ereport(FATAL, (errmsg("The memory is insufficient, please adjust polar_login_history.max")));
        }

        /* Copy in the actual stats */
        entry->roloid = temp.roloid;
        entry->logintime = temp.logintime;
        strncpy(entry->ip, temp.ip, POLAR_LOGIN_HISTORY_STRING_LENGTH - 1);
        entry->ip[POLAR_LOGIN_HISTORY_STRING_LENGTH - 1] = '\0';
        strncpy(entry->application, temp.application, POLAR_LOGIN_HISTORY_STRING_LENGTH - 1);
        entry->application[POLAR_LOGIN_HISTORY_STRING_LENGTH - 1] = '\0';
        entry->failcount = temp.failcount;
    }

    FreeFile(file);

    /*
     * Remove the persisted stats file so it's not included in
     * backups/replication standbys, etc.  A new file will be written on next
     * shutdown.
     */
    unlink(POLAR_LOGIN_HISTORY_FILE);

    return;

read_error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not read file \"%s\": %m",
                    POLAR_LOGIN_HISTORY_FILE)));
    if (file)
        FreeFile(file);
    /* If possible, throw away the bogus file; ignore any error */
    unlink(POLAR_LOGIN_HISTORY_FILE);
}

/*
 * shmem_shutdown hook: Dump login information into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
polar_login_history_shmem_shutdown(int code, Datum arg)
{
    /* Don't try to dump during a crash. */
    if (code)
        return;

    polar_save_information();
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing login information from file.
 */
static void
polar_login_history_shmem_startup(void)
{
    bool    found;
    HASHCTL info;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /*
     * Create or attach to the shared memory state, including hash table.
     */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    polar_login_history_lock = ShmemInitStruct("polar_login_history_lock",
                                               sizeof(polar_login_history_lwlock),
                                               &found);
    if (!found)
    {
        /* First time through ... */
        polar_login_history_lock->lock = &(GetNamedLWLockTranche("polar_login_history"))->lock;
    }

    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(polar_login_history_entry);
    polar_login_history_hash = ShmemInitHash("polar_login_history_hash",
                                             polar_max_login_history,
                                             polar_max_login_history,
                                             &info,
                                             HASH_ELEM | HASH_BLOBS);

    LWLockRelease(AddinShmemInitLock);

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to dump the statistics to disk.
     */
    if (!IsUnderPostmaster)
        on_shmem_exit(polar_login_history_shmem_shutdown, (Datum) 0);

    /*
     * Done if some other process already completed our initialization.
     */
    if (found)
        return;

    /*
     * Note: we don't bother with locks here, because there should be no other
     * processes running when this code is reached.
     */
    polar_load_information();
}

/*
 * When a user logs in to the database, the current login information is saved
 * regardless of whether the login succeeds or fails. When a user successfully logs in,
 * the last login information is displayed to the user.
 */
static void
polar_update_login_history(bool login_success)
{
    Oid roloid;
    polar_login_history_entry *entry;
    bool found;

    if (!polar_enable_login_history)
        return;

    if (!polar_login_history_lock || !polar_login_history_hash)
        return;

    /* Only the postgres process records the login information during the login process */
    if (am_walsender || !polar_login_flag)
        return;

    /*
     * Avoid duplicate recording of information due to errors reported during the recording process.
     * Therefore, the flag bit is set immediately to mark the end of the login process.
     */
    polar_login_flag = false;

    if (!MyProcPort || !MyProcPort->user_name)
        return;

    /* Function get_role_oid needs to be called in the transaction */
    if (IsTransactionState())
        roloid = get_role_oid(MyProcPort->user_name, true);
    else
    {
        StartTransactionCommand();
        roloid = get_role_oid(MyProcPort->user_name, true);
        CommitTransactionCommand();
    }
    /* If the user does not exist, return instead of reporting an error */
    if (!OidIsValid(roloid))
        return;

    LWLockAcquire(polar_login_history_lock->lock, LW_EXCLUSIVE);

    entry = (polar_login_history_entry *) hash_search(polar_login_history_hash, &roloid, HASH_ENTER_NULL, &found);
    if (!entry)
    {
        /* There is no space in the hash table */
        polar_login_history_hash = NULL;
        LWLockRelease(polar_login_history_lock->lock);

        ereport(FATAL,
                (errmsg("The current login information cannot be recorded because the memory " \
                        "is insufficient. Please adjust polar_login_history.max")));
    }

    if (found && login_success)
    {
        /* Print last login information */
        ereport(INFO,
                (errmsg("\n" \
                        "Last login: %s from %s using %s\n" \
                        "The number of failed login attempts since the last successful login was %d",
                        entry->logintime ? timestamptz_to_str(entry->logintime) : "unrecognized",
                        entry->ip ? entry->ip : "unrecognized",
                        entry->application ? entry->application : "unrecognized",
                        entry->failcount)));
    }

    entry->roloid = roloid;
    entry->logintime = MyStartTimestamp ? MyStartTimestamp : GetCurrentTimestamp();
    strncpy(entry->ip, MyProcPort->remote_host ? MyProcPort->remote_host : "unrecognized", POLAR_LOGIN_HISTORY_STRING_LENGTH - 1);
    entry->ip[POLAR_LOGIN_HISTORY_STRING_LENGTH - 1] = '\0';
    strncpy(entry->application, MyProcPort->application_name ? MyProcPort->application_name : "unrecognized", POLAR_LOGIN_HISTORY_STRING_LENGTH - 1);
    entry->application[POLAR_LOGIN_HISTORY_STRING_LENGTH - 1] = '\0';

    /* Update the number of login failures */
    if (login_success)
    {
        entry->failcount = 0;

        /* Print the first login information */
        if (!found)
            ereport(INFO,
                    (errmsg("\n" \
                            "First login: %s from %s using %s",
                            entry->logintime ? timestamptz_to_str(entry->logintime) : "unrecognized",
                            entry->ip ? entry->ip : "unrecognized",
                            entry->application ? entry->application : "unrecognized")));
    }
    else
    {
        if (!found)
            entry->failcount = 1;
        else
            entry->failcount += 1;
    }

    polar_save_information();
    LWLockRelease(polar_login_history_lock->lock);
}

/*
 * When dropping a user, the related login information is removed.
 */
static void
polar_remove_login_history(Oid roloid)
{
    if (!polar_enable_login_history)
        return;

    if (!polar_login_history_lock || !polar_login_history_hash)
        return;

    LWLockAcquire(polar_login_history_lock->lock, LW_EXCLUSIVE);

    /* Remove the key if it exists */
    hash_search(polar_login_history_hash, &roloid, HASH_REMOVE, NULL);
    polar_save_information();

    LWLockRelease(polar_login_history_lock->lock);
}

void
_PG_init(void)
{
    /* Define custom GUC variables */
    DefineCustomBoolVariable("polar_login_history.enable",
                             "Displays historical login information to the user",
                             NULL,
                             &polar_enable_login_history,
                             false,
                             PGC_POSTMASTER,
                             POLAR_GUC_IS_VISIBLE | POLAR_GUC_IS_CHANGABLE,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomIntVariable("polar_login_history.max",
                            "Sets the maximum number of users tracked by polar_login_history",
                            NULL,
                            &polar_max_login_history,
                            512,
                            128,
                            INT_MAX / 2,
                            PGC_POSTMASTER,
                            POLAR_GUC_IS_VISIBLE | POLAR_GUC_IS_CHANGABLE,
                            NULL,
                            NULL,
                            NULL);

    /* Install hooks */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = polar_login_history_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = polar_login_history_shmem_startup;

    polar_update_login_history_hook = polar_update_login_history;
    polar_remove_login_history_hook = polar_remove_login_history;
}
