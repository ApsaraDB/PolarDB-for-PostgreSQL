/*-------------------------------------------------------------------------
 *
 * txn_util.c
 *
 *      Distributed transaction support
 *
 * IDENTIFICATION
 * src/backend/polardbx/transam/txn_util.c
 *
 *-------------------------------------------------------------------------
 */

#include "pgxc/transam/txn_util.h"

#include <sys/time.h>

#include "access/xact.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "pgxc/transam/txn_coordinator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "postmaster/postmaster.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/tqual.h"

static GlobalTimestamp XactGlobalCommitTimestamp = 0;
static GlobalTimestamp XactGlobalPrepareTimestamp = 0;
static GlobalTimestamp XactLocalCommitTimestamp = 0;
static GlobalTimestamp XactLocalPrepareTimestamp = 0;

static TimestampTz GTMxactStartTimestamp = 0;
static char *globalXid = NULL;   /* consisting of the local node name and local xid on CN */
static uint64 globalXidVersion = 0;

bool is_distri_report = false;
bool enable_distri_debug		   = false;
bool enable_statistic			   = false;
bool xc_maintenance_mode		   = false;
int	 delay_before_acquire_committs = 0;
int	 delay_after_acquire_committs  = 0;

void SetGTMxactStartTimestamp(TimestampTz ts)
{
    GTMxactStartTimestamp = ts;
}

/*
 * Check if the given xid is form implicit 2PC
 */
bool
IsXidImplicit(const char *xid)
{
#define implicit2PC_head "_$XC$"
    const size_t implicit2PC_head_len = strlen(implicit2PC_head);

    if (strncmp(xid, implicit2PC_head, implicit2PC_head_len))
        return false;
    return true;
}

void
AtEOXact_Global(void)
{
    XactGlobalCommitTimestamp = InvalidGlobalTimestamp;
    XactGlobalPrepareTimestamp = InvalidGlobalTimestamp;
    XactLocalCommitTimestamp = InvalidGlobalTimestamp;
    XactLocalPrepareTimestamp = InvalidGlobalTimestamp;
    is_distri_report = false;
}


void SetGlobalCommitTimestamp(GlobalTimestamp timestamp)
{
    if (timestamp < XactGlobalPrepareTimestamp)
        elog(ERROR, "prepare timestamp should not lag behind commit timestamp: "
                    "prepare " UINT64_FORMAT "commit " UINT64_FORMAT,
             XactGlobalPrepareTimestamp, timestamp);

    XactGlobalCommitTimestamp = timestamp;
}

GlobalTimestamp
GetGlobalCommitTimestamp(void)
{
    return XactGlobalCommitTimestamp;
}

void SetGlobalPrepareTimestamp(GlobalTimestamp timestamp)
{

    XactGlobalPrepareTimestamp = timestamp;
}

GlobalTimestamp
GetGlobalPrepareTimestamp(void)
{
    return XactGlobalPrepareTimestamp;
}

void SetLocalCommitTimestamp(GlobalTimestamp timestamp)
{

    XactLocalCommitTimestamp = timestamp;
}

GlobalTimestamp
GetLocalCommitTimestamp(void)
{
    return XactLocalCommitTimestamp;
}

void SetLocalPrepareTimestamp(GlobalTimestamp timestamp)
{

    XactLocalPrepareTimestamp = timestamp;
}

GlobalTimestamp
GetLocalPrepareTimestamp(void)
{
    return XactLocalPrepareTimestamp;
}

static uint64 seq = 0;

static char *
AssignGlobalXidInternal(void)
{

    MemoryContext oldContext = CurrentMemoryContext;
    StringInfoData str;

    oldContext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&str);

    seq++;
    if (seq == PG_UINT64_MAX)
    {
        seq = 0;
    }

    appendStringInfo(&str, "%u:%u:"UINT64_FORMAT,
            PGXCNodeId - 1, MyProc->pgprocno, seq);

    MemoryContextSwitchTo(oldContext);
    globalXidVersion++;
    if(enable_distri_print)
    {
        elog(LOG, "assign global xid %s prono %d seq " UINT64_FORMAT UINT64_FORMAT,
            str.data, MyProc->pgprocno, seq, globalXidVersion);
    }

    if (strlen(str.data) + 1 > NAMEDATALEN)
    {
        elog(ERROR, "too long global xid node id %u procno %u seq "UINT64_FORMAT,
            PGXCNodeId - 1, MyProc->pgprocno, seq);
    }

    return str.data;
}

/* POLARX_TODO */
static void
StoreLocalGlobalXid(const char *globalXid)
{

}

void AssignGlobalXid(void)
{
    if (NULL == globalXid)
    {
        globalXid = AssignGlobalXidInternal();
        StoreLocalGlobalXid(globalXid);
    }
}

char *
GetGlobalXid(void)
{
    if(globalXid == NULL)
        ereport(PANIC,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("global xid is null xid %d version " UINT64_FORMAT, GetTopTransactionIdIfAny(), globalXidVersion)));
    return globalXid;
}

char *
GetGlobalXidNoCheck(void)
{
    return globalXid;
}


uint64
GetGlobalXidVersion(void)
{
    return globalXidVersion;
}

void
SetGlobalXid(const char *globalXidString)
{
    if(globalXid != NULL)
    {
        pfree(globalXid);
        globalXid = NULL;
        globalXidVersion = 0;
        if(enable_distri_print)
        {
            elog(LOG, "local global xid exists %s new %s", globalXid, globalXidString);
        }
    }

    globalXid = MemoryContextStrdup(TopMemoryContext, globalXidString);
    globalXidVersion++;

    if(enable_distri_print)
    {
        elog(LOG, "set global xid %s version " UINT64_FORMAT, globalXidString, globalXidVersion);
    }
}

void
ResetUsageCommon(struct rusage *save_r, struct timeval *save_t)
{
    getrusage(RUSAGE_SELF, save_r);
    gettimeofday(save_t, NULL);
}

void
ShowUsageCommon(const char *title, struct rusage *save_r, struct timeval *save_t)
{
    StringInfoData str;
    struct timeval user,
                sys;
    struct timeval elapse_t;
    struct rusage r;

    getrusage(RUSAGE_SELF, &r);
    gettimeofday(&elapse_t, NULL);
    memcpy((char *) &user, (char *) &r.ru_utime, sizeof(user));
    memcpy((char *) &sys, (char *) &r.ru_stime, sizeof(sys));
    if (elapse_t.tv_usec < save_t->tv_usec)
    {
        elapse_t.tv_sec--;
        elapse_t.tv_usec += 1000000;
    }
    if (r.ru_utime.tv_usec < save_r->ru_utime.tv_usec)
    {
        r.ru_utime.tv_sec--;
        r.ru_utime.tv_usec += 1000000;
    }
    if (r.ru_stime.tv_usec < save_r->ru_stime.tv_usec)
    {
        r.ru_stime.tv_sec--;
        r.ru_stime.tv_usec += 1000000;
    }

    /*
     * the only stats we don't show here are for memory usage -- i can't
     * figure out how to interpret the relevant fields in the rusage struct,
     * and they change names across o/s platforms, anyway. if you can figure
     * out what the entries mean, you can somehow extract resident set size,
     * shared text size, and unshared data and stack sizes.
     */
    initStringInfo(&str);

    appendStringInfoString(&str, "! system usage stats:\n");
    appendStringInfo(&str,
                     "!\t%ld.%06ld s user, %ld.%06ld s system, %ld.%06ld s elapsed\n",
                     (long) (r.ru_utime.tv_sec - save_r->ru_utime.tv_sec),
                     (long) (r.ru_utime.tv_usec - save_r->ru_utime.tv_usec),
                     (long) (r.ru_stime.tv_sec - save_r->ru_stime.tv_sec),
                     (long) (r.ru_stime.tv_usec - save_r->ru_stime.tv_usec),
                     (long) (elapse_t.tv_sec - save_t->tv_sec),
                     (long) (elapse_t.tv_usec - save_t->tv_usec));
    appendStringInfo(&str,
                     "!\t[%ld.%06ld s user, %ld.%06ld s system total]\n",
                     (long) user.tv_sec,
                     (long) user.tv_usec,
                     (long) sys.tv_sec,
                     (long) sys.tv_usec);
#if defined(HAVE_GETRUSAGE)
    appendStringInfo(&str,
                     "!\t%ld/%ld [%ld/%ld] filesystem blocks in/out\n",
                     r.ru_inblock - save_r->ru_inblock,
    /* they only drink coffee at dec */
                     r.ru_oublock - save_r->ru_oublock,
                     r.ru_inblock, r.ru_oublock);
    appendStringInfo(&str,
                       "!\t%ld/%ld [%ld/%ld] page faults/reclaims, %ld [%ld] swaps\n",
                     r.ru_majflt - save_r->ru_majflt,
                     r.ru_minflt - save_r->ru_minflt,
                     r.ru_majflt, r.ru_minflt,
                     r.ru_nswap - save_r->ru_nswap,
                     r.ru_nswap);
    appendStringInfo(&str,
                      "!\t%ld [%ld] signals rcvd, %ld/%ld [%ld/%ld] messages rcvd/sent\n",
                     r.ru_nsignals - save_r->ru_nsignals,
                     r.ru_nsignals,
                     r.ru_msgrcv - save_r->ru_msgrcv,
                     r.ru_msgsnd - save_r->ru_msgsnd,
                     r.ru_msgrcv, r.ru_msgsnd);
    appendStringInfo(&str,
                      "!\t%ld/%ld [%ld/%ld] voluntary/involuntary context switches\n",
                     r.ru_nvcsw - save_r->ru_nvcsw,
                     r.ru_nivcsw - save_r->ru_nivcsw,
                     r.ru_nvcsw, r.ru_nivcsw);
#endif                            /* HAVE_GETRUSAGE */

    /* remove trailing newline */
    if (str.data[str.len - 1] == '\n')
        str.data[--str.len] = '\0';

    ereport(LOG,
            (errmsg_internal("%s", title),
             errdetail_internal("%s", str.data)));

    pfree(str.data);
}

/*
 * Count how many coordinators and datanodes are involved in this transaction
 * so that we can save that information in the GID
 */
static void
pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
        int *coordCount, int coordNodeIds[])
{// #lizard forgives
    int i;
    PGXCNodeAllHandles *handles = get_current_handles();

    *dnCount = *coordCount = 0;
    for (i = 0; i < handles->dn_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->datanode_handles[i];
        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
            continue;
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                dnNodeIds[*dnCount] = conn->nodeid;
                *dnCount = *dnCount + 1;
            }
        }
    }

    for (i = 0; i < handles->co_conn_count; i++)
    {
        PGXCNodeHandle *conn = handles->coord_handles[i];
        /*
         * Skip empty slots
         */
        if (conn->sock == NO_SOCKET)
            continue;
        else if (conn->transaction_status == 'T')
        {
            if (!conn->read_only)
            {
                coordNodeIds[*coordCount] = conn->nodeid;
                *coordCount = *coordCount + 1;
            }
        }
    }
    pfree_pgxc_all_handles(handles);
}



char *
GetImplicit2PCGID(const char *head, bool localWrite)
{
	int			   dnCount = 0, coordCount = 0;
	int *		   dnNodeIds	= NULL;
	int *		   coordNodeIds = NULL;
	MemoryContext  oldContext	= CurrentMemoryContext;
	StringInfoData str;

	dnNodeIds = (int *) palloc(sizeof(int) * POLARX_MAX_DATANODE_NUMBER);
	if (dnNodeIds == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnNodeIds")));
    }

    coordNodeIds = (int*)palloc(sizeof(int) * POLARX_MAX_COORDINATOR_NUMBER);
    if (coordNodeIds == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coordNodeIds")));
    }

    oldContext = MemoryContextSwitchTo(TopTransactionContext);
    initStringInfo(&str);
    /*
     * Check how many coordinators and datanodes are involved in this
     * transaction.
     * MAX_IMPLICIT_2PC_STR_LEN (5 + 21 + 64 + 1 + 5 + 5)
     */

    pgxc_node_remote_count(&dnCount, dnNodeIds, &coordCount, coordNodeIds);
    appendStringInfo(&str, "%s%u:%s:%c:%d:%d",
            head,
            GetTopTransactionId(),
            PGXCNodeName,
            localWrite ? 'T' : 'F',
            dnCount,
            coordCount + (localWrite ? 1 : 0));

    MemoryContextSwitchTo(oldContext);

    if (dnNodeIds)
    {
        pfree(dnNodeIds);
        dnNodeIds = NULL;
    }

    if (coordNodeIds)
    {
        pfree(coordNodeIds);
        coordNodeIds = NULL;
    }

    return str.data;
}
