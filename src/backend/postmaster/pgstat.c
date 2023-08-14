/* ----------
 * pgstat.c
 *
 *	All the statistics collector stuff hacked up in one big, ugly file.
 *
 *	TODO:	- Separate collector, postmaster and backend stuff
 *			  into different files.
 *
 *			- Add some automatic call for pgstat vacuuming.
 *
 *			- Add a pgstat config column to pg_database, so this
 *			  entire thing can be enabled/disabled on a per db basis.
 *
 *	Copyright (c) 2001-2018, PostgreSQL Global Development Group
 *
 *	src/backend/postmaster/pgstat.c
 * ----------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>

/* POLAR */
#include <libpq/pqcomm.h>
/* POLAR end */

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "pgstat.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "common/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/backendid.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pg_shmem.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/ascii.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_coredump.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"

/* POLAR */
#include "commands/extension.h"
#include "replication/polar_priority_replication.h"
#include "storage/polar_io_stat.h"
#include "tcop/tcopprot.h"
#include "postmaster/polar_dispatcher.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_STAT_INTERVAL	500 /* Minimum time between stats file
									 * updates; in milliseconds. */

#define PGSTAT_RETRY_DELAY		10	/* How long to wait between checks for a
									 * new file; in milliseconds. */

#define PGSTAT_MAX_WAIT_TIME	10000	/* Maximum time to wait for a stats
										 * file update; in milliseconds. */

#define PGSTAT_INQ_INTERVAL		640 /* How often to ping the collector for a
									 * new file; in milliseconds. */

#define PGSTAT_RESTART_INTERVAL 60	/* How often to attempt to restart a
									 * failed statistics collector; in
									 * seconds. */

#define PGSTAT_POLL_LOOP_COUNT	(PGSTAT_MAX_WAIT_TIME / PGSTAT_RETRY_DELAY)
#define PGSTAT_INQ_LOOP_COUNT	(PGSTAT_INQ_INTERVAL / PGSTAT_RETRY_DELAY)

/* Minimum receive buffer size for the collector's socket. */
#define PGSTAT_MIN_RCVBUF		(100 * 1024)


/* ----------
 * The initial size hints for the hash tables used in the collector.
 * ----------
 */
#define PGSTAT_DB_HASH_SIZE		16
#define PGSTAT_TAB_HASH_SIZE	512
#define PGSTAT_FUNCTION_HASH_SIZE	512


/* ----------
 * Total number of backends including auxiliary
 *
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.) MaxBackends
 * includes autovacuum workers and background workers as well.
 * ----------
 */
#define NumBackendStatSlots_base (MaxBackends + NUM_AUXPROCTYPES)

/* POLAR: Shared Server */
#define NumBackendStatSlots		(NumBackendStatSlots_base + MaxPolarSessions)

/* ----------
 * GUC parameters
 * ----------
 */
bool		pgstat_track_activities = false;
bool		pgstat_track_counts = false;
int			pgstat_track_functions = TRACK_FUNC_OFF;
int			pgstat_track_activity_query_size = 1024;

/* POLAR */
bool		polar_enable_virtual_pid;
polar_postmaster_child_init_register polar_stat_hook = NULL;

/* ----------
 * Built from GUC parameter
 * ----------
 */
char	   *pgstat_stat_directory = NULL;
char	   *pgstat_stat_filename = NULL;
char	   *pgstat_stat_tmpname = NULL;

/*
 * BgWriter global statistics counters (unused in other processes).
 * Stored directly in a stats message structure so it can be sent
 * without needing to copy things around.  We assume this inits to zeroes.
 */
PgStat_MsgBgWriter BgWriterStats;

/* POLAR: stats for proxy */
PolarStat_Proxy *polar_stat_proxy = NULL;
polar_unsplittable_reason_t polar_unable_to_split_reason;
bool polar_stat_need_update_proxy_info;

/* ----------
 * Local data
 * ----------
 */
NON_EXEC_STATIC pgsocket pgStatSock = PGINVALID_SOCKET;

static struct sockaddr_storage pgStatAddr;

static time_t last_pgstat_start_time;

static bool pgStatRunningInCollector = false;

/*
 * Structures in which backends store per-table info that's waiting to be
 * sent to the collector.
 *
 * NOTE: once allocated, TabStatusArray structures are never moved or deleted
 * for the life of the backend.  Also, we zero out the t_id fields of the
 * contained PgStat_TableStatus structs whenever they are not actively in use.
 * This allows relcache pgstat_info pointers to be treated as long-lived data,
 * avoiding repeated searches in pgstat_initstats() when a relation is
 * repeatedly opened during a transaction.
 */
#define TABSTAT_QUANTUM		100 /* we alloc this many at a time */

typedef struct TabStatusArray
{
	struct TabStatusArray *tsa_next;	/* link to next array, if any */
	int			tsa_used;		/* # entries currently used */
	PgStat_TableStatus tsa_entries[TABSTAT_QUANTUM];	/* per-table data */
} TabStatusArray;

static TabStatusArray *pgStatTabList = NULL;

/*
 * pgStatTabHash entry: map from relation OID to PgStat_TableStatus pointer
 */
typedef struct TabStatHashEntry
{
	Oid			t_id;
	PgStat_TableStatus *tsa_entry;
} TabStatHashEntry;

/*
 * Hash table for O(1) t_id -> tsa_entry lookup
 */
static HTAB *pgStatTabHash = NULL;

/*
 * Backends store per-function info that's waiting to be sent to the collector
 * in this hash table (indexed by function OID).
 */
static HTAB *pgStatFunctions = NULL;

/*
 * Indicates if backend has some function stats that it hasn't yet
 * sent to the collector.
 */
static bool have_function_stats = false;

/*
 * Tuple insertion/deletion counts for an open transaction can't be propagated
 * into PgStat_TableStatus counters until we know if it is going to commit
 * or abort.  Hence, we keep these counts in per-subxact structs that live
 * in TopTransactionContext.  This data structure is designed on the assumption
 * that subxacts won't usually modify very many tables.
 */
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */
	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;

static PgStat_SubXactStatus *pgStatXactStack = NULL;

static int	pgStatXactCommit = 0;
static int	pgStatXactRollback = 0;
PgStat_Counter pgStatBlockReadTime = 0;
PgStat_Counter pgStatBlockWriteTime = 0;

/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	PgStat_Counter inserted_pre_trunc;	/* tuples inserted prior to truncate */
	PgStat_Counter updated_pre_trunc;	/* tuples updated prior to truncate */
	PgStat_Counter deleted_pre_trunc;	/* tuples deleted prior to truncate */
	Oid			t_id;			/* table's OID */
	bool		t_shared;		/* is it a shared catalog? */
	bool		t_truncated;	/* was the relation truncated? */
} TwoPhasePgStatRecord;

/*
 * Info about current "snapshot" of stats file
 */
static MemoryContext pgStatLocalContext = NULL;
static HTAB *pgStatDBHash = NULL;

/* Status for backends including auxiliary */
static LocalPgBackendStatus *localBackendStatusTable = NULL;

/* Total number of backends including auxiliary */
static int	localNumBackends = 0;

/*
 * Cluster wide statistics, kept in the stats collector.
 * Contains statistics that are not collected per database
 * or per table.
 */
static PgStat_ArchiverStats archiverStats;
static PgStat_GlobalStats globalStats;

/*
 * List of OIDs of databases we need to write out.  If an entry is InvalidOid,
 * it means to write only the shared-catalog stats ("DB 0"); otherwise, we
 * will write both that DB's data and the shared stats.
 */
static List *pending_write_requests = NIL;

/* Signal handler flags */
static volatile bool need_exit = false;
static volatile bool got_SIGHUP = false;

/*
 * Total time charged to functions so far in the current backend.
 * We use this to help separate "self" and "other" time charges.
 * (We assume this initializes to zero.)
 */
static instr_time total_func_time;


/* ----------
 * Local function forward declarations
 * ----------
 */
#ifdef EXEC_BACKEND
static pid_t pgstat_forkexec(void);
#endif

NON_EXEC_STATIC void PgstatCollectorMain(int argc, char *argv[]) pg_attribute_noreturn();
static void pgstat_exit(SIGNAL_ARGS);
static void pgstat_beshutdown_hook(int code, Datum arg);
static void pgstat_sighup_handler(SIGNAL_ARGS);

static PgStat_StatDBEntry *pgstat_get_db_entry(Oid databaseid, bool create);
static PgStat_StatTabEntry *pgstat_get_tab_entry(PgStat_StatDBEntry *dbentry,
					 Oid tableoid, bool create);
static void pgstat_write_statsfiles(bool permanent, bool allDbs);
static void pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry, bool permanent);
static HTAB *pgstat_read_statsfiles(Oid onlydb, bool permanent, bool deep);
static void pgstat_read_db_statsfile(Oid databaseid, HTAB *tabhash, HTAB *funchash, bool permanent);
static void backend_read_statsfile(void);
static void pgstat_read_current_status(void);

static bool pgstat_write_statsfile_needed(void);
static bool pgstat_db_requested(Oid databaseid);

static void pgstat_send_tabstat(PgStat_MsgTabstat *tsmsg);
static void pgstat_send_funcstats(void);
static HTAB *pgstat_collect_oids(Oid catalogid);

static PgStat_TableStatus *get_tabstat_entry(Oid rel_id, bool isshared);

static void pgstat_setup_memcxt(void);

static const char *pgstat_get_wait_activity(WaitEventActivity w);
static const char *pgstat_get_wait_client(WaitEventClient w);
static const char *pgstat_get_wait_ipc(WaitEventIPC w);
static const char *pgstat_get_wait_timeout(WaitEventTimeout w);
static const char *pgstat_get_wait_io(WaitEventIO w);

static void pgstat_setheader(PgStat_MsgHdr *hdr, StatMsgType mtype);
static void pgstat_send(void *msg, int len);

static void pgstat_recv_inquiry(PgStat_MsgInquiry *msg, int len);
static void pgstat_recv_tabstat(PgStat_MsgTabstat *msg, int len);
static void pgstat_recv_tabpurge(PgStat_MsgTabpurge *msg, int len);
static void pgstat_recv_dropdb(PgStat_MsgDropdb *msg, int len);
static void pgstat_recv_resetcounter(PgStat_MsgResetcounter *msg, int len);
static void pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter *msg, int len);
static void pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter *msg, int len);
static void pgstat_recv_autovac(PgStat_MsgAutovacStart *msg, int len);
static void pgstat_recv_vacuum(PgStat_MsgVacuum *msg, int len);
static void pgstat_recv_analyze(PgStat_MsgAnalyze *msg, int len);
static void pgstat_recv_archiver(PgStat_MsgArchiver *msg, int len);
static void pgstat_recv_bgwriter(PgStat_MsgBgWriter *msg, int len);
static void pgstat_recv_funcstat(PgStat_MsgFuncstat *msg, int len);
static void pgstat_recv_funcpurge(PgStat_MsgFuncpurge *msg, int len);
static void pgstat_recv_recoveryconflict(PgStat_MsgRecoveryConflict *msg, int len);
static void pgstat_recv_deadlock(PgStat_MsgDeadlock *msg, int len);
static void pgstat_recv_tempfile(PgStat_MsgTempFile *msg, int len);
/* POLAR */
static void pgstat_recv_delay_dml(PolarStat_MsgDelayDml *msg, int len);

/* POLAR */
static PgStat_Counter polar_pgstat_get_current_examined_row_count(void);
static void polar_log_beentry_proxy_state(volatile PgBackendStatus *beentry, const char *func);
static bool polar_backend_parse_attrs(volatile PgBackendStatus *beentry, int pid, 
									  bool *is_high_pri, bool *is_low_pri);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/* ----------
 * pgstat_init() -
 *
 *	Called from postmaster at startup. Create the resources required
 *	by the statistics collector process.  If unable to do so, do not
 *	fail --- better to let the postmaster start with stats collection
 *	disabled.
 * ----------
 */
void
pgstat_init(void)
{
	ACCEPT_TYPE_ARG3 alen;
	struct addrinfo *addrs = NULL,
			   *addr,
				hints;
	int			ret;
	fd_set		rset;
	struct timeval tv;
	char		test_byte;
	int			sel_res;
	int			tries = 0;

#define TESTBYTEVAL ((char) 199)

	/*
	 * This static assertion verifies that we didn't mess up the calculations
	 * involved in selecting maximum payload sizes for our UDP messages.
	 * Because the only consequence of overrunning PGSTAT_MAX_MSG_SIZE would
	 * be silent performance loss from fragmentation, it seems worth having a
	 * compile-time cross-check that we didn't.
	 */
	StaticAssertStmt(sizeof(PgStat_Msg) <= PGSTAT_MAX_MSG_SIZE,
					 "maximum stats message size exceeds PGSTAT_MAX_MSG_SIZE");

	/*
	 * Create the UDP socket for sending and receiving statistic messages
	 */
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;
	hints.ai_next = NULL;
	ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
	if (ret || !addrs)
	{
		ereport(LOG,
				(errmsg("could not resolve \"localhost\": %s",
						gai_strerror(ret))));
		goto startup_failed;
	}

	/*
	 * On some platforms, pg_getaddrinfo_all() may return multiple addresses
	 * only one of which will actually work (eg, both IPv6 and IPv4 addresses
	 * when kernel will reject IPv6).  Worse, the failure may occur at the
	 * bind() or perhaps even connect() stage.  So we must loop through the
	 * results till we find a working combination. We will generate LOG
	 * messages, but no error, for bogus combinations.
	 */
	for (addr = addrs; addr; addr = addr->ai_next)
	{
#ifdef HAVE_UNIX_SOCKETS
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;
#endif

		if (++tries > 1)
			ereport(LOG,
					(errmsg("trying another address for the statistics collector")));

		/*
		 * Create the socket.
		 */
		if ((pgStatSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not create socket for statistics collector: %m")));
			continue;
		}

		/*
		 * Bind it to a kernel assigned port on localhost and get the assigned
		 * port via getsockname().
		 */
		if (bind(pgStatSock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		alen = sizeof(pgStatAddr);
		if (getsockname(pgStatSock, (struct sockaddr *) &pgStatAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * Connect the socket to its own address.  This saves a few cycles by
		 * not having to respecify the target address on every send. This also
		 * provides a kernel-level check that only packets from this same
		 * address will be received.
		 */
		if (connect(pgStatSock, (struct sockaddr *) &pgStatAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not connect socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * Try to send and receive a one-byte test message on the socket. This
		 * is to catch situations where the socket can be created but will not
		 * actually pass data (for instance, because kernel packet filtering
		 * rules prevent it).
		 */
		test_byte = TESTBYTEVAL;

retry1:
		if (send(pgStatSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not send test message on socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * There could possibly be a little delay before the message can be
		 * received.  We arbitrarily allow up to half a second before deciding
		 * it's broken.
		 */
		for (;;)				/* need a loop to handle EINTR */
		{
			FD_ZERO(&rset);
			FD_SET(pgStatSock, &rset);

			tv.tv_sec = 0;
			tv.tv_usec = 500000;
			sel_res = select(pgStatSock + 1, &rset, NULL, NULL, &tv);
			if (sel_res >= 0 || errno != EINTR)
				break;
		}
		if (sel_res < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("select() failed in statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}
		if (sel_res == 0 || !FD_ISSET(pgStatSock, &rset))
		{
			/*
			 * This is the case we actually think is likely, so take pains to
			 * give a specific message for it.
			 *
			 * errno will not be set meaningfully here, so don't use it.
			 */
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("test message did not get through on socket for statistics collector")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		test_byte++;			/* just make sure variable is changed */

retry2:
		if (recv(pgStatSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry2;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive test message on socket for statistics collector: %m")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		if (test_byte != TESTBYTEVAL)	/* strictly paranoia ... */
		{
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("incorrect test message transmission on socket for statistics collector")));
			closesocket(pgStatSock);
			pgStatSock = PGINVALID_SOCKET;
			continue;
		}

		/* If we get here, we have a working socket */
		break;
	}

	/* Did we find a working address? */
	if (!addr || pgStatSock == PGINVALID_SOCKET)
		goto startup_failed;

	/*
	 * Set the socket to non-blocking IO.  This ensures that if the collector
	 * falls behind, statistics messages will be discarded; backends won't
	 * block waiting to send messages to the collector.
	 */
	if (!pg_set_noblock(pgStatSock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set statistics collector socket to nonblocking mode: %m")));
		goto startup_failed;
	}

	/*
	 * Try to ensure that the socket's receive buffer is at least
	 * PGSTAT_MIN_RCVBUF bytes, so that it won't easily overflow and lose
	 * data.  Use of UDP protocol means that we are willing to lose data under
	 * heavy load, but we don't want it to happen just because of ridiculously
	 * small default buffer sizes (such as 8KB on older Windows versions).
	 */
	{
		int			old_rcvbuf;
		int			new_rcvbuf;
		ACCEPT_TYPE_ARG3 rcvbufsize = sizeof(old_rcvbuf);

		if (getsockopt(pgStatSock, SOL_SOCKET, SO_RCVBUF,
					   (char *) &old_rcvbuf, &rcvbufsize) < 0)
		{
			elog(LOG, "getsockopt(SO_RCVBUF) failed: %m");
			/* if we can't get existing size, always try to set it */
			old_rcvbuf = 0;
		}

		new_rcvbuf = PGSTAT_MIN_RCVBUF;
		if (old_rcvbuf < new_rcvbuf)
		{
			if (setsockopt(pgStatSock, SOL_SOCKET, SO_RCVBUF,
						   (char *) &new_rcvbuf, sizeof(new_rcvbuf)) < 0)
				elog(LOG, "setsockopt(SO_RCVBUF) failed: %m");
		}
	}

	pg_freeaddrinfo_all(hints.ai_family, addrs);

	return;

startup_failed:
	ereport(LOG,
			(errmsg("disabling statistics collector for lack of working socket")));

	if (addrs)
		pg_freeaddrinfo_all(hints.ai_family, addrs);

	if (pgStatSock != PGINVALID_SOCKET)
		closesocket(pgStatSock);
	pgStatSock = PGINVALID_SOCKET;

	/*
	 * Adjust GUC variables to suppress useless activity, and for debugging
	 * purposes (seeing track_counts off is a clue that we failed here). We
	 * use PGC_S_OVERRIDE because there is no point in trying to turn it back
	 * on from postgresql.conf without a restart.
	 */
	SetConfigOption("track_counts", "off", PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * subroutine for pgstat_reset_all
 */
static void
pgstat_reset_remove_files(const char *directory)
{
	DIR		   *dir;
	struct dirent *entry;
	char		fname[MAXPGPATH * 2];

	dir = AllocateDir(directory, false);
	while ((entry = ReadDir(dir, directory)) != NULL)
	{
		int			nchars;
		Oid			tmp_oid;

		/*
		 * Skip directory entries that don't match the file names we write.
		 * See get_dbstat_filename for the database-specific pattern.
		 */
		if (strncmp(entry->d_name, "global.", 7) == 0)
			nchars = 7;
		else
		{
			nchars = 0;
			(void) sscanf(entry->d_name, "db_%u.%n",
						  &tmp_oid, &nchars);
			if (nchars <= 0)
				continue;
			/* %u allows leading whitespace, so reject that */
			if (strchr("0123456789", entry->d_name[3]) == NULL)
				continue;
		}

		if (strcmp(entry->d_name + nchars, "tmp") != 0 &&
			strcmp(entry->d_name + nchars, "stat") != 0)
			continue;

		snprintf(fname, sizeof(fname), "%s/%s", directory,
				 entry->d_name);
		unlink(fname);
	}
	FreeDir(dir);
}

/*
 * pgstat_reset_all() -
 *
 * Remove the stats files.  This is currently used only if WAL
 * recovery is needed after a crash.
 */
void
pgstat_reset_all(void)
{
	pgstat_reset_remove_files(pgstat_stat_directory);
	pgstat_reset_remove_files(PGSTAT_STAT_PERMANENT_DIRECTORY);
}

#ifdef EXEC_BACKEND

/*
 * pgstat_forkexec() -
 *
 * Format up the arglist for, then fork and exec, statistics collector process
 */
static pid_t
pgstat_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkcol";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif							/* EXEC_BACKEND */


/*
 * pgstat_start() -
 *
 *	Called from postmaster at startup or after an existing collector
 *	died.  Attempt to fire up a fresh statistics collector.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
int
pgstat_start(void)
{
	time_t		curtime;
	pid_t		pgStatPid;

	/*
	 * Check that the socket is there, else pgstat_init failed and we can do
	 * nothing useful.
	 */
	if (pgStatSock == PGINVALID_SOCKET)
		return 0;

	/*
	 * Do nothing if too soon since last collector start.  This is a safety
	 * valve to protect against continuous respawn attempts if the collector
	 * is dying immediately at launch.  Note that since we will be re-called
	 * from the postmaster main loop, we will get another chance later.
	 */
	curtime = time(NULL);
	if ((unsigned int) (curtime - last_pgstat_start_time) <
		(unsigned int) PGSTAT_RESTART_INTERVAL)
		return 0;
	last_pgstat_start_time = curtime;

	/*
	 * Okay, fork off the collector.
	 */
#ifdef EXEC_BACKEND
	switch ((pgStatPid = pgstat_forkexec()))
#else
	switch ((pgStatPid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork statistics collector: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/* Drop our connection to postmaster's shared memory, as well */
			dsm_detach_all();
			PGSharedMemoryDetach();

			PgstatCollectorMain(0, NULL);
			break;
#endif

		default:
			return (int) pgStatPid;
	}

	/* shouldn't get here */
	return 0;
}

void
allow_immediate_pgstat_restart(void)
{
	last_pgstat_start_time = 0;
}

/* ------------------------------------------------------------
 * Public functions used by backends follow
 *------------------------------------------------------------
 */


/* ----------
 * pgstat_report_stat() -
 *
 *	Must be called by processes that performs DML: tcop/postgres.c, logical
 *	receiver processes, SPI worker, etc. to send the so far collected
 *	per-table and function usage statistics to the collector.  Note that this
 *	is called only when not within a transaction, so it is fair to use
 *	transaction stop time as an approximation of current time.
 * ----------
 */
void
pgstat_report_stat(bool force)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_TableCounts all_zeroes;
	static TimestampTz last_report = 0;

	TimestampTz now;
	PgStat_MsgTabstat regular_msg;
	PgStat_MsgTabstat shared_msg;
	TabStatusArray *tsa;
	int			i;

	/* Don't expend a clock check if nothing to do */
	if ((pgStatTabList == NULL || pgStatTabList->tsa_used == 0) &&
		pgStatXactCommit == 0 && pgStatXactRollback == 0 &&
		!have_function_stats)
		return;

	/* POLAR: first init audit log status */
	MemSet(&polar_audit_log, 0, sizeof(Pg_audit_log));
	/* POLAR end */

	/*
	 * Don't send a message unless it's been at least PGSTAT_STAT_INTERVAL
	 * msec since we last sent one, or the caller wants to force stats out.
	 */
	now = GetCurrentTransactionStopTimestamp();
	if (!force &&
		!TimestampDifferenceExceeds(last_report, now, PGSTAT_STAT_INTERVAL))
		return;
	last_report = now;

	/*
	 * Destroy pgStatTabHash before we start invalidating PgStat_TableEntry
	 * entries it points to.  (Should we fail partway through the loop below,
	 * it's okay to have removed the hashtable already --- the only
	 * consequence is we'd get multiple entries for the same table in the
	 * pgStatTabList, and that's safe.)
	 */
	if (pgStatTabHash)
		hash_destroy(pgStatTabHash);
	pgStatTabHash = NULL;

	/*
	 * Scan through the TabStatusArray struct(s) to find tables that actually
	 * have counts, and build messages to send.  We have to separate shared
	 * relations from regular ones because the databaseid field in the message
	 * header has to depend on that.
	 */
	regular_msg.m_databaseid = MyDatabaseId;
	shared_msg.m_databaseid = InvalidOid;
	regular_msg.m_nentries = 0;
	shared_msg.m_nentries = 0;

	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		for (i = 0; i < tsa->tsa_used; i++)
		{
			PgStat_TableStatus *entry = &tsa->tsa_entries[i];
			PgStat_MsgTabstat *this_msg;
			PgStat_TableEntry *this_ent;

			/* Shouldn't have any pending transaction-dependent counts */
			Assert(entry->trans == NULL);

			/*
			 * Ignore entries that didn't accumulate any actual counts, such
			 * as indexes that were opened by the planner but not used.
			 */
			if (memcmp(&entry->t_counts, &all_zeroes,
					   sizeof(PgStat_TableCounts)) == 0)
				continue;

			/*
			 * OK, insert data into the appropriate message, and send if full.
			 */
			this_msg = entry->t_shared ? &shared_msg : &regular_msg;
			this_ent = &this_msg->m_entry[this_msg->m_nentries];
			this_ent->t_id = entry->t_id;
			memcpy(&this_ent->t_counts, &entry->t_counts,
				   sizeof(PgStat_TableCounts));
			if (++this_msg->m_nentries >= PGSTAT_NUM_TABENTRIES)
			{
				pgstat_send_tabstat(this_msg);
				this_msg->m_nentries = 0;
			}
		}
		/* zero out TableStatus structs after use */
		MemSet(tsa->tsa_entries, 0,
			   tsa->tsa_used * sizeof(PgStat_TableStatus));
		tsa->tsa_used = 0;
	}

	/*
	 * Send partial messages.  Make sure that any pending xact commit/abort
	 * gets counted, even if there are no table stats to send.
	 */
	if (regular_msg.m_nentries > 0 ||
		pgStatXactCommit > 0 || pgStatXactRollback > 0)
		pgstat_send_tabstat(&regular_msg);
	if (shared_msg.m_nentries > 0)
		pgstat_send_tabstat(&shared_msg);

	/* Now, send function statistics */
	pgstat_send_funcstats();
}

/*
 * Subroutine for pgstat_report_stat: finish and send a tabstat message
 */
static void
pgstat_send_tabstat(PgStat_MsgTabstat *tsmsg)
{
	int			n;
	int			len;

	/* It's unlikely we'd get here with no socket, but maybe not impossible */
	if (pgStatSock == PGINVALID_SOCKET)
		return;

	/*
	 * Report and reset accumulated xact commit/rollback and I/O timings
	 * whenever we send a normal tabstat message
	 */
	if (OidIsValid(tsmsg->m_databaseid))
	{
		tsmsg->m_xact_commit = pgStatXactCommit;
		tsmsg->m_xact_rollback = pgStatXactRollback;
		tsmsg->m_block_read_time = pgStatBlockReadTime;
		tsmsg->m_block_write_time = pgStatBlockWriteTime;
		pgStatXactCommit = 0;
		pgStatXactRollback = 0;
		pgStatBlockReadTime = 0;
		pgStatBlockWriteTime = 0;
	}
	else
	{
		tsmsg->m_xact_commit = 0;
		tsmsg->m_xact_rollback = 0;
		tsmsg->m_block_read_time = 0;
		tsmsg->m_block_write_time = 0;
	}

	n = tsmsg->m_nentries;
	len = offsetof(PgStat_MsgTabstat, m_entry[0]) +
		n * sizeof(PgStat_TableEntry);

	pgstat_setheader(&tsmsg->m_hdr, PGSTAT_MTYPE_TABSTAT);
	pgstat_send(tsmsg, len);
}

/*
 * Subroutine for pgstat_report_stat: populate and send a function stat message
 */
static void
pgstat_send_funcstats(void)
{
	/* we assume this inits to all zeroes: */
	static const PgStat_FunctionCounts all_zeroes;

	PgStat_MsgFuncstat msg;
	PgStat_BackendFunctionEntry *entry;
	HASH_SEQ_STATUS fstat;

	if (pgStatFunctions == NULL)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_FUNCSTAT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_nentries = 0;

	hash_seq_init(&fstat, pgStatFunctions);
	while ((entry = (PgStat_BackendFunctionEntry *) hash_seq_search(&fstat)) != NULL)
	{
		PgStat_FunctionEntry *m_ent;

		/* Skip it if no counts accumulated since last time */
		if (memcmp(&entry->f_counts, &all_zeroes,
				   sizeof(PgStat_FunctionCounts)) == 0)
			continue;

		/* need to convert format of time accumulators */
		m_ent = &msg.m_entry[msg.m_nentries];
		m_ent->f_id = entry->f_id;
		m_ent->f_numcalls = entry->f_counts.f_numcalls;
		m_ent->f_total_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_total_time);
		m_ent->f_self_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_self_time);

		if (++msg.m_nentries >= PGSTAT_NUM_FUNCENTRIES)
		{
			pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) +
						msg.m_nentries * sizeof(PgStat_FunctionEntry));
			msg.m_nentries = 0;
		}

		/* reset the entry's counts */
		MemSet(&entry->f_counts, 0, sizeof(PgStat_FunctionCounts));
	}

	if (msg.m_nentries > 0)
		pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) +
					msg.m_nentries * sizeof(PgStat_FunctionEntry));

	have_function_stats = false;
}


/* ----------
 * pgstat_vacuum_stat() -
 *
 *	Will tell the collector about objects he can get rid of.
 * ----------
 */
void
pgstat_vacuum_stat(void)
{
	HTAB	   *htab;
	PgStat_MsgTabpurge msg;
	PgStat_MsgFuncpurge f_msg;
	HASH_SEQ_STATUS hstat;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;
	int			len;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Read pg_database and make a list of OIDs of all existing databases
	 */
	htab = pgstat_collect_oids(DatabaseRelationId);

	/*
	 * Search the database hash table for dead databases and tell the
	 * collector to drop them.
	 */
	hash_seq_init(&hstat, pgStatDBHash);
	while ((dbentry = (PgStat_StatDBEntry *) hash_seq_search(&hstat)) != NULL)
	{
		Oid			dbid = dbentry->databaseid;

		CHECK_FOR_INTERRUPTS();

		/* the DB entry for shared tables (with InvalidOid) is never dropped */
		if (OidIsValid(dbid) &&
			hash_search(htab, (void *) &dbid, HASH_FIND, NULL) == NULL)
			pgstat_drop_database(dbid);
	}

	/* Clean up */
	hash_destroy(htab);

	/*
	 * Lookup our own database entry; if not found, nothing more to do.
	 */
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &MyDatabaseId,
												 HASH_FIND, NULL);
	if (dbentry == NULL || dbentry->tables == NULL)
		return;

	/*
	 * Similarly to above, make a list of all known relations in this DB.
	 */
	htab = pgstat_collect_oids(RelationRelationId);

	/*
	 * Initialize our messages table counter to zero
	 */
	msg.m_nentries = 0;

	/*
	 * Check for all tables listed in stats hashtable if they still exist.
	 */
	hash_seq_init(&hstat, dbentry->tables);
	while ((tabentry = (PgStat_StatTabEntry *) hash_seq_search(&hstat)) != NULL)
	{
		Oid			tabid = tabentry->tableid;

		CHECK_FOR_INTERRUPTS();

		if (hash_search(htab, (void *) &tabid, HASH_FIND, NULL) != NULL)
			continue;

		/*
		 * Not there, so add this table's Oid to the message
		 */
		msg.m_tableid[msg.m_nentries++] = tabid;

		/*
		 * If the message is full, send it out and reinitialize to empty
		 */
		if (msg.m_nentries >= PGSTAT_NUM_TABPURGE)
		{
			len = offsetof(PgStat_MsgTabpurge, m_tableid[0])
				+ msg.m_nentries * sizeof(Oid);

			pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
			msg.m_databaseid = MyDatabaseId;
			pgstat_send(&msg, len);

			msg.m_nentries = 0;
		}
	}

	/*
	 * Send the rest
	 */
	if (msg.m_nentries > 0)
	{
		len = offsetof(PgStat_MsgTabpurge, m_tableid[0])
			+ msg.m_nentries * sizeof(Oid);

		pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
		msg.m_databaseid = MyDatabaseId;
		pgstat_send(&msg, len);
	}

	/* Clean up */
	hash_destroy(htab);

	/*
	 * Now repeat the above steps for functions.  However, we needn't bother
	 * in the common case where no function stats are being collected.
	 */
	if (dbentry->functions != NULL &&
		hash_get_num_entries(dbentry->functions) > 0)
	{
		htab = pgstat_collect_oids(ProcedureRelationId);

		pgstat_setheader(&f_msg.m_hdr, PGSTAT_MTYPE_FUNCPURGE);
		f_msg.m_databaseid = MyDatabaseId;
		f_msg.m_nentries = 0;

		hash_seq_init(&hstat, dbentry->functions);
		while ((funcentry = (PgStat_StatFuncEntry *) hash_seq_search(&hstat)) != NULL)
		{
			Oid			funcid = funcentry->functionid;

			CHECK_FOR_INTERRUPTS();

			if (hash_search(htab, (void *) &funcid, HASH_FIND, NULL) != NULL)
				continue;

			/*
			 * Not there, so add this function's Oid to the message
			 */
			f_msg.m_functionid[f_msg.m_nentries++] = funcid;

			/*
			 * If the message is full, send it out and reinitialize to empty
			 */
			if (f_msg.m_nentries >= PGSTAT_NUM_FUNCPURGE)
			{
				len = offsetof(PgStat_MsgFuncpurge, m_functionid[0])
					+ f_msg.m_nentries * sizeof(Oid);

				pgstat_send(&f_msg, len);

				f_msg.m_nentries = 0;
			}
		}

		/*
		 * Send the rest
		 */
		if (f_msg.m_nentries > 0)
		{
			len = offsetof(PgStat_MsgFuncpurge, m_functionid[0])
				+ f_msg.m_nentries * sizeof(Oid);

			pgstat_send(&f_msg, len);
		}

		hash_destroy(htab);
	}
}


/* ----------
 * pgstat_collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result
 *	when done with it.  (However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static HTAB *
pgstat_collect_oids(Oid catalogid)
{
	HTAB	   *htab;
	HASHCTL		hash_ctl;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	hash_ctl.hcxt = CurrentMemoryContext;
	htab = hash_create("Temporary table of OIDs",
					   PGSTAT_TAB_HASH_SIZE,
					   &hash_ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	rel = heap_open(catalogid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid			thisoid = HeapTupleGetOid(tup);

		CHECK_FOR_INTERRUPTS();

		(void) hash_search(htab, (void *) &thisoid, HASH_ENTER, NULL);
	}
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);

	return htab;
}


/* ----------
 * pgstat_drop_database() -
 *
 *	Tell the collector that we just dropped a database.
 *	(If the message gets lost, we will still clean the dead DB eventually
 *	via future invocations of pgstat_vacuum_stat().)
 * ----------
 */
void
pgstat_drop_database(Oid databaseid)
{
	PgStat_MsgDropdb msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DROPDB);
	msg.m_databaseid = databaseid;
	pgstat_send(&msg, sizeof(msg));
}


/* ----------
 * pgstat_drop_relation() -
 *
 *	Tell the collector that we just dropped a relation.
 *	(If the message gets lost, we will still clean the dead entry eventually
 *	via future invocations of pgstat_vacuum_stat().)
 *
 *	Currently not used for lack of any good place to call it; we rely
 *	entirely on pgstat_vacuum_stat() to clean out stats for dead rels.
 * ----------
 */
#ifdef NOT_USED
void
pgstat_drop_relation(Oid relid)
{
	PgStat_MsgTabpurge msg;
	int			len;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	msg.m_tableid[0] = relid;
	msg.m_nentries = 1;

	len = offsetof(PgStat_MsgTabpurge, m_tableid[0]) + sizeof(Oid);

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, len);
}
#endif							/* NOT_USED */


/* ----------
 * pgstat_reset_counters() -
 *
 *	Tell the statistics collector to reset counters for our database.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_counters(void)
{
	PgStat_MsgResetcounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETCOUNTER);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Tell the statistics collector to reset cluster-wide shared counters.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_shared_counters(const char *target)
{
	PgStat_MsgResetsharedcounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	if (strcmp(target, "archiver") == 0)
		msg.m_resettarget = RESET_ARCHIVER;
	else if (strcmp(target, "bgwriter") == 0)
		msg.m_resettarget = RESET_BGWRITER;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized reset target: \"%s\"", target),
				 errhint("Target must be \"archiver\" or \"bgwriter\".")));

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSHAREDCOUNTER);
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Tell the statistics collector to reset a single counter.
 *
 *	Permission checking for this function is managed through the normal
 *	GRANT system.
 * ----------
 */
void
pgstat_reset_single_counter(Oid objoid, PgStat_Single_Reset_Type type)
{
	PgStat_MsgResetsinglecounter msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSINGLECOUNTER);
	msg.m_databaseid = MyDatabaseId;
	msg.m_resettype = type;
	msg.m_objectid = objoid;

	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_report_autovac() -
 *
 *	Called from autovacuum.c to report startup of an autovacuum process.
 *	We are called before InitPostgres is done, so can't rely on MyDatabaseId;
 *	the db OID must be passed in, instead.
 * ----------
 */
void
pgstat_report_autovac(Oid dboid)
{
	PgStat_MsgAutovacStart msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_AUTOVAC_START);
	msg.m_databaseid = dboid;
	msg.m_start_time = GetCurrentTimestamp();

	pgstat_send(&msg, sizeof(msg));
}


/* ---------
 * pgstat_report_vacuum() -
 *
 *	Tell the collector about the table we just vacuumed.
 * ---------
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared,
					 PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
	PgStat_MsgVacuum msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_VACUUM);
	msg.m_databaseid = shared ? InvalidOid : MyDatabaseId;
	msg.m_tableoid = tableoid;
	msg.m_autovacuum = IsAutoVacuumWorkerProcess();
	msg.m_vacuumtime = GetCurrentTimestamp();
	msg.m_live_tuples = livetuples;
	msg.m_dead_tuples = deadtuples;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_analyze() -
 *
 *	Tell the collector about the table we just analyzed.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the changes_since_analyze counter.
 * --------
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter)
{
	PgStat_MsgAnalyze msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from what we send to the collector now, else they'll
	 * be double-counted after commit.  (This approach also ensures that the
	 * collector ends up with the right numbers if we abort instead of
	 * committing.)
	 */
	if (rel->pgstat_info != NULL)
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->t_counts.t_delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_ANALYZE);
	msg.m_databaseid = rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId;
	msg.m_tableoid = RelationGetRelid(rel);
	msg.m_autovacuum = IsAutoVacuumWorkerProcess();
	msg.m_resetcounter = resetcounter;
	msg.m_analyzetime = GetCurrentTimestamp();
	msg.m_live_tuples = livetuples;
	msg.m_dead_tuples = deadtuples;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Tell the collector about a Hot Standby recovery conflict.
 * --------
 */
void
pgstat_report_recovery_conflict(int reason)
{
	PgStat_MsgRecoveryConflict msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RECOVERYCONFLICT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_reason = reason;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Tell the collector about a deadlock detected.
 * --------
 */
void
pgstat_report_deadlock(void)
{
	PgStat_MsgDeadlock msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DEADLOCK);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Tell the collector about a temporary file.
 * --------
 */
void
pgstat_report_tempfile(size_t filesize)
{
	PgStat_MsgTempFile msg;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TEMPFILE);
	msg.m_databaseid = MyDatabaseId;
	msg.m_filesize = filesize;
	pgstat_send(&msg, sizeof(msg));
}


/* ----------
 * pgstat_ping() -
 *
 *	Send some junk data to the collector to increase traffic.
 * ----------
 */
void
pgstat_ping(void)
{
	PgStat_MsgDummy msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DUMMY);
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_inquiry() -
 *
 *	Notify collector that we need fresh data.
 * ----------
 */
static void
pgstat_send_inquiry(TimestampTz clock_time, TimestampTz cutoff_time, Oid databaseid)
{
	PgStat_MsgInquiry msg;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_INQUIRY);
	msg.clock_time = clock_time;
	msg.cutoff_time = cutoff_time;
	msg.databaseid = databaseid;
	pgstat_send(&msg, sizeof(msg));
}


/*
 * Initialize function call usage data.
 * Called by the executor before invoking a function.
 */
void
pgstat_init_function_usage(FunctionCallInfoData *fcinfo,
						   PgStat_FunctionCallUsage *fcu)
{
	PgStat_BackendFunctionEntry *htabent;
	bool		found;

	if (pgstat_track_functions <= fcinfo->flinfo->fn_stats)
	{
		/* stats not wanted */
		fcu->fs = NULL;
		return;
	}

	if (!pgStatFunctions)
	{
		/* First time through - initialize function stat table */
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(PgStat_BackendFunctionEntry);
		pgStatFunctions = hash_create("Function stat entries",
									  PGSTAT_FUNCTION_HASH_SIZE,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
	}

	/* Get the stats entry for this function, create if necessary */
	htabent = hash_search(pgStatFunctions, &fcinfo->flinfo->fn_oid,
						  HASH_ENTER, &found);
	if (!found)
		MemSet(&htabent->f_counts, 0, sizeof(PgStat_FunctionCounts));

	fcu->fs = &htabent->f_counts;

	/* save stats for this function, later used to compensate for recursion */
	fcu->save_f_total_time = htabent->f_counts.f_total_time;

	/* save current backend-wide total time */
	fcu->save_total = total_func_time;

	/* get clock time as of function start */
	INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/*
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 */
PgStat_BackendFunctionEntry *
find_funcstat_entry(Oid func_id)
{
	if (pgStatFunctions == NULL)
		return NULL;

	return (PgStat_BackendFunctionEntry *) hash_search(pgStatFunctions,
													   (void *) &func_id,
													   HASH_FIND, NULL);
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
 */
void
pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)
{
	PgStat_FunctionCounts *fs = fcu->fs;
	instr_time	f_total;
	instr_time	f_others;
	instr_time	f_self;

	/* stats not wanted? */
	if (fs == NULL)
		return;

	/* total elapsed time in this function call */
	INSTR_TIME_SET_CURRENT(f_total);
	INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

	/* self usage: elapsed minus anything already charged to other calls */
	f_others = total_func_time;
	INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
	f_self = f_total;
	INSTR_TIME_SUBTRACT(f_self, f_others);

	/* update backend-wide total time */
	INSTR_TIME_ADD(total_func_time, f_self);

	/*
	 * Compute the new f_total_time as the total elapsed time added to the
	 * pre-call value of f_total_time.  This is necessary to avoid
	 * double-counting any time taken by recursive calls of myself.  (We do
	 * not need any similar kluge for self time, since that already excludes
	 * any recursive calls.)
	 */
	INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

	/* update counters in function stats table */
	if (finalize)
		fs->f_numcalls++;
	fs->f_total_time = f_total;
	INSTR_TIME_ADD(fs->f_self_time, f_self);

	/* indicate that we have something to send */
	have_function_stats = true;
}


/* ----------
 * pgstat_initstats() -
 *
 *	Initialize a relcache entry to count access statistics.
 *	Called whenever a relation is opened.
 *
 *	We assume that a relcache entry's pgstat_info field is zeroed by
 *	relcache.c when the relcache entry is made; thereafter it is long-lived
 *	data.  We can avoid repeated searches of the TabStatus arrays when the
 *	same relation is touched repeatedly within a transaction.
 * ----------
 */
void
pgstat_initstats(Relation rel)
{
	Oid			rel_id = rel->rd_id;
	char		relkind = rel->rd_rel->relkind;

	/* We only count stats for things that have storage */
	if (!(relkind == RELKIND_RELATION ||
		  relkind == RELKIND_MATVIEW ||
		  relkind == RELKIND_INDEX ||
		  relkind == RELKIND_TOASTVALUE ||
		  relkind == RELKIND_SEQUENCE))
	{
		rel->pgstat_info = NULL;
		return;
	}

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_counts)
	{
		/* We're not counting at all */
		rel->pgstat_info = NULL;
		return;
	}

	/*
	 * If we already set up this relation in the current transaction, nothing
	 * to do.
	 */
	if (rel->pgstat_info != NULL &&
		rel->pgstat_info->t_id == rel_id)
		return;

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = get_tabstat_entry(rel_id, rel->rd_rel->relisshared);
}

/*
 * get_tabstat_entry - find or create a PgStat_TableStatus entry for rel
 */
static PgStat_TableStatus *
get_tabstat_entry(Oid rel_id, bool isshared)
{
	TabStatHashEntry *hash_entry;
	PgStat_TableStatus *entry;
	TabStatusArray *tsa;
	bool		found;

	/*
	 * Create hash table if we don't have it already.
	 */
	if (pgStatTabHash == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TabStatHashEntry);

		pgStatTabHash = hash_create("pgstat TabStatusArray lookup hash table",
									TABSTAT_QUANTUM,
									&ctl,
									HASH_ELEM | HASH_BLOBS);
	}

	/*
	 * Find an entry or create a new one.
	 */
	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new entry with null pointer */
		hash_entry->tsa_entry = NULL;
	}

	/*
	 * If entry is already valid, we're done.
	 */
	if (hash_entry->tsa_entry)
		return hash_entry->tsa_entry;

	/*
	 * Locate the first pgStatTabList entry with free space, making a new list
	 * entry if needed.  Note that we could get an OOM failure here, but if so
	 * we have left the hashtable and the list in a consistent state.
	 */
	if (pgStatTabList == NULL)
	{
		/* Set up first pgStatTabList entry */
		pgStatTabList = (TabStatusArray *)
			MemoryContextAllocZero(TopMemoryContext,
								   sizeof(TabStatusArray));
	}

	tsa = pgStatTabList;
	while (tsa->tsa_used >= TABSTAT_QUANTUM)
	{
		if (tsa->tsa_next == NULL)
			tsa->tsa_next = (TabStatusArray *)
				MemoryContextAllocZero(TopMemoryContext,
									   sizeof(TabStatusArray));
		tsa = tsa->tsa_next;
	}

	/*
	 * Allocate a PgStat_TableStatus entry within this list entry.  We assume
	 * the entry was already zeroed, either at creation or after last use.
	 */
	entry = &tsa->tsa_entries[tsa->tsa_used++];
	entry->t_id = rel_id;
	entry->t_shared = isshared;

	/*
	 * Now we can fill the entry in pgStatTabHash.
	 */
	hash_entry->tsa_entry = entry;

	return entry;
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 * If no entry, return NULL, don't create a new one
 *
 * Note: if we got an error in the most recent execution of pgstat_report_stat,
 * it's possible that an entry exists but there's no hashtable entry for it.
 * That's okay, we'll treat this case as "doesn't exist".
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	TabStatHashEntry *hash_entry;

	/* If hashtable doesn't exist, there are no entries at all */
	if (!pgStatTabHash)
		return NULL;

	hash_entry = hash_search(pgStatTabHash, &rel_id, HASH_FIND, NULL);
	if (!hash_entry)
		return NULL;

	/* Note that this step could also return NULL, but that's correct */
	return hash_entry->tsa_entry;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static PgStat_SubXactStatus *
get_tabstat_stack_level(int nest_level)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		xact_state = (PgStat_SubXactStatus *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(PgStat_SubXactStatus));
		xact_state->nest_level = nest_level;
		xact_state->prev = pgStatXactStack;
		xact_state->first = NULL;
		pgStatXactStack = xact_state;
	}
	return xact_state;
}

/*
 * add_tabstat_xact_level - add a new (sub)transaction state record
 */
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = get_tabstat_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}

/*
 * pgstat_count_heap_insert - count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * pgstat_count_heap_update - count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_updated++;

		/* t_tuples_hot_updated is nontransactional, so just advance it */
		if (hot)
			pgstat_info->t_counts.t_tuples_hot_updated++;
	}
}

/*
 * pgstat_count_heap_delete - count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * pgstat_truncate_save_counters
 *
 * Whenever a table is truncated, we save its i/u/d counters so that they can
 * be cleared, and if the (sub)xact that executed the truncate later aborts,
 * the counters can be restored to the saved (pre-truncate) values.  Note we do
 * this on the first truncate in any particular subxact level only.
 */
static void
pgstat_truncate_save_counters(PgStat_TableXactStatus *trans)
{
	if (!trans->truncated)
	{
		trans->inserted_pre_trunc = trans->tuples_inserted;
		trans->updated_pre_trunc = trans->tuples_updated;
		trans->deleted_pre_trunc = trans->tuples_deleted;
		trans->truncated = true;
	}
}

/*
 * pgstat_truncate_restore_counters - restore counters when a truncate aborts
 */
static void
pgstat_truncate_restore_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncated)
	{
		trans->tuples_inserted = trans->inserted_pre_trunc;
		trans->tuples_updated = trans->updated_pre_trunc;
		trans->tuples_deleted = trans->deleted_pre_trunc;
	}
}

/*
 * pgstat_count_truncate - update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
	{
		/* We have to log the effect at the proper transactional level */
		int			nest_level = GetCurrentTransactionNestLevel();

		if (pgstat_info->trans == NULL ||
			pgstat_info->trans->nest_level != nest_level)
			add_tabstat_xact_level(pgstat_info, nest_level);

		pgstat_truncate_save_counters(pgstat_info->trans);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * pgstat_update_heap_dead_tuples - update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so t_delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	PgStat_TableStatus *pgstat_info = rel->pgstat_info;

	if (pgstat_info != NULL)
		pgstat_info->t_counts.t_delta_dead_tuples -= delta;
}


/* ----------
 * AtEOXact_PgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void
AtEOXact_PgStat(bool isCommit, bool parallel)
{
	PgStat_SubXactStatus *xact_state;

	/* Don't count parallel worker transaction stats */
	if (!parallel)
	{
		/*
		 * Count transaction commit or abort.  (We use counters, not just
		 * bools, in case the reporting message isn't sent right away.)
		 */
		if (isCommit)
			pgStatXactCommit++;
		else
			pgStatXactRollback++;
	}

	/*
	 * Transfer transactional insert/update counts into the base tabstat
	 * entries.  We don't bother to free any of the transactional state, since
	 * it's all in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			/* restore pre-truncate stats (if any) in case of aborted xact */
			if (!isCommit)
				pgstat_truncate_restore_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
			tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
			tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
			if (isCommit)
			{
				tabstat->t_counts.t_truncated = trans->truncated;
				if (trans->truncated)
				{
					/* forget live/dead stats seen by backend thus far */
					tabstat->t_counts.t_delta_live_tuples = 0;
					tabstat->t_counts.t_delta_dead_tuples = 0;
				}
				/* insert adds a live tuple, delete removes one */
				tabstat->t_counts.t_delta_live_tuples +=
					trans->tuples_inserted - trans->tuples_deleted;
				/* update and delete each create a dead tuple */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_updated + trans->tuples_deleted;
				/* insert, update, delete each count as one change event */
				tabstat->t_counts.t_changed_tuples +=
					trans->tuples_inserted + trans->tuples_updated +
					trans->tuples_deleted;
			}
			else
			{
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				/* an aborted xact generates no changed_tuple events */
			}
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/* ----------
 * AtEOSubXact_PgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void
AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * Transfer transactional insert/update counts into the next higher
	 * subtransaction state.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		PgStat_TableXactStatus *trans;
		PgStat_TableXactStatus *next_trans;

		/* delink xact_state from stack immediately to simplify reuse case */
		pgStatXactStack = xact_state->prev;

		for (trans = xact_state->first; trans != NULL; trans = next_trans)
		{
			PgStat_TableStatus *tabstat;

			next_trans = trans->next;
			Assert(trans->nest_level == nestDepth);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);
			if (isCommit)
			{
				if (trans->upper && trans->upper->nest_level == nestDepth - 1)
				{
					if (trans->truncated)
					{
						/* propagate the truncate status one level up */
						pgstat_truncate_save_counters(trans->upper);
						/* replace upper xact stats with ours */
						trans->upper->tuples_inserted = trans->tuples_inserted;
						trans->upper->tuples_updated = trans->tuples_updated;
						trans->upper->tuples_deleted = trans->tuples_deleted;
					}
					else
					{
						trans->upper->tuples_inserted += trans->tuples_inserted;
						trans->upper->tuples_updated += trans->tuples_updated;
						trans->upper->tuples_deleted += trans->tuples_deleted;
					}
					tabstat->trans = trans->upper;
					pfree(trans);
				}
				else
				{
					/*
					 * When there isn't an immediate parent state, we can just
					 * reuse the record instead of going through a
					 * palloc/pfree pushup (this works since it's all in
					 * TopTransactionContext anyway).  We have to re-link it
					 * into the parent level, though, and that might mean
					 * pushing a new entry into the pgStatXactStack.
					 */
					PgStat_SubXactStatus *upper_xact_state;

					upper_xact_state = get_tabstat_stack_level(nestDepth - 1);
					trans->next = upper_xact_state->first;
					upper_xact_state->first = trans;
					trans->nest_level = nestDepth - 1;
				}
			}
			else
			{
				/*
				 * On abort, update top-level tabstat counts, then forget the
				 * subtransaction
				 */

				/* first restore values obliterated by truncate */
				pgstat_truncate_restore_counters(trans);
				/* count attempted actions regardless of commit/abort */
				tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted;
				tabstat->t_counts.t_tuples_updated += trans->tuples_updated;
				tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted;
				/* inserted tuples are dead, deleted tuples are unaffected */
				tabstat->t_counts.t_delta_dead_tuples +=
					trans->tuples_inserted + trans->tuples_updated;
				tabstat->trans = trans->upper;
				pfree(trans);
			}
		}
		pfree(xact_state);
	}
}


/*
 * AtPrepare_PgStat
 *		Save the transactional stats state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending
 * transaction-dependent stats work.
 */
void
AtPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);
		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;
			TwoPhasePgStatRecord record;

			Assert(trans->nest_level == 1);
			Assert(trans->upper == NULL);
			tabstat = trans->parent;
			Assert(tabstat->trans == trans);

			record.tuples_inserted = trans->tuples_inserted;
			record.tuples_updated = trans->tuples_updated;
			record.tuples_deleted = trans->tuples_deleted;
			record.inserted_pre_trunc = trans->inserted_pre_trunc;
			record.updated_pre_trunc = trans->updated_pre_trunc;
			record.deleted_pre_trunc = trans->deleted_pre_trunc;
			record.t_id = tabstat->t_id;
			record.t_shared = tabstat->t_shared;
			record.t_truncated = trans->truncated;

			RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
								   &record, sizeof(TwoPhasePgStatRecord));
		}
	}
}

/*
 * PostPrepare_PgStat
 *		Clean up after successful PREPARE.
 *
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the stats collector immediately, while the effects on live
 * and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void
PostPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * We don't bother to free any of the transactional state, since it's all
	 * in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		PgStat_TableXactStatus *trans;

		for (trans = xact_state->first; trans != NULL; trans = trans->next)
		{
			PgStat_TableStatus *tabstat;

			tabstat = trans->parent;
			tabstat->trans = NULL;
		}
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_truncated = rec->t_truncated;
	if (rec->t_truncated)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->t_counts.t_delta_live_tuples = 0;
		pgstat_info->t_counts.t_delta_dead_tuples = 0;
	}
	pgstat_info->t_counts.t_delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->t_counts.t_changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->t_truncated)
	{
		rec->tuples_inserted = rec->inserted_pre_trunc;
		rec->tuples_updated = rec->updated_pre_trunc;
		rec->tuples_deleted = rec->deleted_pre_trunc;
	}
	pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted;
	pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated;
	pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted;
	pgstat_info->t_counts.t_delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}


/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one database or NULL. NULL doesn't mean
 *	that the database doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatDBEntry *
pgstat_fetch_stat_dbentry(Oid dbid)
{
	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Lookup the requested database; return NULL if not found
	 */
	return (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
											  (void *) &dbid,
											  HASH_FIND, NULL);
}


/* ----------
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	Oid			dbid;
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * If not done for this transaction, read the statistics collector stats
	 * file into some hash tables.
	 */
	backend_read_statsfile();

	/*
	 * Lookup our database, then look in its table hash table.
	 */
	dbid = MyDatabaseId;
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &dbid,
												 HASH_FIND, NULL);
	if (dbentry != NULL && dbentry->tables != NULL)
	{
		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &relid,
													   HASH_FIND, NULL);
		if (tabentry)
			return tabentry;
	}

	/*
	 * If we didn't find it, maybe it's a shared table.
	 */
	dbid = InvalidOid;
	dbentry = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												 (void *) &dbid,
												 HASH_FIND, NULL);
	if (dbentry != NULL && dbentry->tables != NULL)
	{
		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &relid,
													   HASH_FIND, NULL);
		if (tabentry)
			return tabentry;
	}

	return NULL;
}


/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 * ----------
 */
PgStat_StatFuncEntry *
pgstat_fetch_stat_funcentry(Oid func_id)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatFuncEntry *funcentry = NULL;

	/* load the stats file if needed */
	backend_read_statsfile();

	/* Lookup our database, then find the requested function.  */
	dbentry = pgstat_fetch_stat_dbentry(MyDatabaseId);
	if (dbentry != NULL && dbentry->functions != NULL)
	{
		funcentry = (PgStat_StatFuncEntry *) hash_search(dbentry->functions,
														 (void *) &func_id,
														 HASH_FIND, NULL);
	}

	return funcentry;
}


/* ----------
 * pgstat_fetch_stat_beentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	our local copy of the current-activity entry for one backend.
 *
 *	NB: caller is responsible for a check if the user is permitted to see
 *	this info (especially the querystring).
 * ----------
 */
PgBackendStatus *
pgstat_fetch_stat_beentry(int beid)
{
	pgstat_read_current_status();

	if (beid < 1 || beid > localNumBackends)
		return NULL;

	return &localBackendStatusTable[beid - 1].backendStatus;
}


/* ----------
 * pgstat_fetch_stat_local_beentry() -
 *
 *	Like pgstat_fetch_stat_beentry() but with locally computed additions (like
 *	xid and xmin values of the backend)
 *
 *	NB: caller is responsible for a check if the user is permitted to see
 *	this info (especially the querystring).
 * ----------
 */
LocalPgBackendStatus *
pgstat_fetch_stat_local_beentry(int beid)
{
	pgstat_read_current_status();

	if (beid < 1 || beid > localNumBackends)
		return NULL;

	return &localBackendStatusTable[beid - 1];
}


/* ----------
 * pgstat_fetch_stat_numbackends() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the maximum current backend id.
 * ----------
 */
int
pgstat_fetch_stat_numbackends(void)
{
	pgstat_read_current_status();

	return localNumBackends;
}

/*
 * ---------
 * pgstat_fetch_stat_archiver() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the archiver statistics struct.
 * ---------
 */
PgStat_ArchiverStats *
pgstat_fetch_stat_archiver(void)
{
	backend_read_statsfile();

	return &archiverStats;
}


/*
 * ---------
 * pgstat_fetch_global() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the global statistics struct.
 * ---------
 */
PgStat_GlobalStats *
pgstat_fetch_global(void)
{
	backend_read_statsfile();

	return &globalStats;
}


/* ------------------------------------------------------------
 * Functions for management of the shared-memory PgBackendStatus array
 * ------------------------------------------------------------
 */

static PgBackendStatus *BackendStatusArray = NULL;
static PgBackendStatus *MyBEEntry = NULL;
static char *BackendAppnameBuffer = NULL;
static char *BackendClientHostnameBuffer = NULL;
static char *BackendActivityBuffer = NULL;
static Size BackendActivityBufferSize = 0;
#ifdef USE_SSL
static PgBackendSSLStatus *BackendSslStatusBuffer = NULL;
#endif


/*
 * Report shared-memory space needed by CreateSharedBackendStatus.
 */
Size
BackendStatusShmemSize(void)
{
	Size		size;

	/* BackendStatusArray: */
	size = mul_size(sizeof(PgBackendStatus), NumBackendStatSlots);
	/* BackendAppnameBuffer: */
	size = add_size(size,
					mul_size(NAMEDATALEN, NumBackendStatSlots));
	/* BackendClientHostnameBuffer: */
	size = add_size(size,
					mul_size(NAMEDATALEN, NumBackendStatSlots));
	/* BackendActivityBuffer: */
	size = add_size(size,
					mul_size(pgstat_track_activity_query_size, NumBackendStatSlots));
#ifdef USE_SSL
	/* BackendSslStatusBuffer: */
	size = add_size(size,
					mul_size(sizeof(PgBackendSSLStatus), NumBackendStatSlots));
#endif
	size = add_size(size, sizeof(PolarStat_Proxy));
	return size;
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
void
CreateSharedBackendStatus(void)
{
	Size		size;
	bool		found;
	int			i;
	char	   *buffer;

	/* Create or attach to the shared array */
	size = mul_size(sizeof(PgBackendStatus), NumBackendStatSlots);
	BackendStatusArray = (PgBackendStatus *)
		ShmemInitStruct("Backend Status Array", size, &found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(BackendStatusArray, 0, size);
	}

	/* Create or attach to the shared appname buffer */
	size = mul_size(NAMEDATALEN, NumBackendStatSlots);
	BackendAppnameBuffer = (char *)
		ShmemInitStruct("Backend Application Name Buffer", size, &found);

	if (!found)
	{
		MemSet(BackendAppnameBuffer, 0, size);

		/* Initialize st_appname pointers. */
		buffer = BackendAppnameBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_appname = buffer;
			buffer += NAMEDATALEN;
		}
	}

	/* Create or attach to the shared client hostname buffer */
	size = mul_size(NAMEDATALEN, NumBackendStatSlots);
	BackendClientHostnameBuffer = (char *)
		ShmemInitStruct("Backend Client Host Name Buffer", size, &found);

	if (!found)
	{
		MemSet(BackendClientHostnameBuffer, 0, size);

		/* Initialize st_clienthostname pointers. */
		buffer = BackendClientHostnameBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_clienthostname = buffer;
			buffer += NAMEDATALEN;
		}
	}

	/* Create or attach to the shared activity buffer */
	BackendActivityBufferSize = mul_size(pgstat_track_activity_query_size,
										 NumBackendStatSlots);
	BackendActivityBuffer = (char *)
		ShmemInitStruct("Backend Activity Buffer",
						BackendActivityBufferSize,
						&found);

	if (!found)
	{
		MemSet(BackendActivityBuffer, 0, BackendActivityBufferSize);

		/* Initialize st_activity pointers. */
		buffer = BackendActivityBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_activity_raw = buffer;
			buffer += pgstat_track_activity_query_size;
		}
	}

#ifdef USE_SSL
	/* Create or attach to the shared SSL status buffer */
	size = mul_size(sizeof(PgBackendSSLStatus), NumBackendStatSlots);
	BackendSslStatusBuffer = (PgBackendSSLStatus *)
		ShmemInitStruct("Backend SSL Status Buffer", size, &found);

	if (!found)
	{
		PgBackendSSLStatus *ptr;

		MemSet(BackendSslStatusBuffer, 0, size);

		/* Initialize st_sslstatus pointers. */
		ptr = BackendSslStatusBuffer;
		for (i = 0; i < NumBackendStatSlots; i++)
		{
			BackendStatusArray[i].st_sslstatus = ptr;
			ptr++;
		}
	}
#endif
	/* Create or attach to the shared SSL status buffer */
	size = sizeof(PolarStat_Proxy);
	polar_stat_proxy = (PolarStat_Proxy *)
		ShmemInitStruct("Proxy Stats Buffer", size, &found);

	if (!found)
		MemSet(polar_stat_proxy, 0, size);
}


/* ----------
 * pgstat_initialize() -
 *
 *	Initialize pgstats state, and set up our on-proc-exit hook.
 *	Called from InitPostgres and AuxiliaryProcessMain. For auxiliary process,
 *	MyBackendId is invalid. Otherwise, MyBackendId must be set,
 *	but we must not have started any transaction yet (since the
 *	exit hook must run after the last transaction exit).
 *	NOTE: MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 * ----------
 */
void
pgstat_initialize(void)
{
	/* Initialize MyBEEntry */
	if (MyBackendId != InvalidBackendId)
	{
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
		MyBEEntry = &BackendStatusArray[MyBackendId - 1];
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);

		/*
		 * Assign the MyBEEntry for an auxiliary process.  Since it doesn't
		 * have a BackendId, the slot is statically allocated based on the
		 * auxiliary process type (MyAuxProcType).  Backends use slots indexed
		 * in the range from 1 to MaxBackends (inclusive), so we use
		 * MaxBackends + AuxBackendType + 1 as the index of the slot for an
		 * auxiliary process.
		 */
		MyBEEntry = &BackendStatusArray[MaxBackends + MyAuxProcType];
	}

	/* Set up a process-exit hook to clean up */
	on_shmem_exit(pgstat_beshutdown_hook, 0);

	/* POLAR: io stat cleanup shmem when child process */
	if (polar_stat_hook)
		polar_stat_hook();
}

/* ----------
 * pgstat_bestart() -
 *
 *	Initialize this backend's entry in the PgBackendStatus array.
 *	Called from InitPostgres.
 *
 *	Apart from auxiliary processes, MyBackendId, MyDatabaseId,
 *	session userid, and application_name must be set for a
 *	backend (hence, this cannot be combined with pgstat_initialize).
 *	Note also that we must be inside a transaction if this isn't an aux
 *	process, as we may need to do encoding conversion on some strings.
 * ----------
 */
void
pgstat_bestart(void)
{
	volatile PgBackendStatus *vbeentry = MyBEEntry;
	PgBackendStatus lbeentry;
#ifdef USE_SSL
	PgBackendSSLStatus lsslstatus;
#endif

	Port	*current_port = MyProcPort;
	if (IS_POLAR_SESSION_SHARED())
	{
		vbeentry->session_local_id = -1;
		vbeentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		current_port = polar_session_info()->client_port;
		vbeentry->session_local_id = polar_session()->id;
		vbeentry->dispatcher_pid = polar_dispatcher_proc[polar_my_dispatcher_id].pid;
		elog(LOG, "pgstat_bestart %d, %d, %d, %d", polar_session()->id, 
			polar_session()->id, 
			polar_session()->id + NumBackendStatSlots_base, MyProcPid);
	}
#define MyProcPort current_port

	/* pgstats state must be initialized from pgstat_initialize() */
	Assert(vbeentry != NULL);

	/*
	 * To minimize the time spent modifying the PgBackendStatus entry, and
	 * avoid risk of errors inside the critical section, we first copy the
	 * shared-memory struct to a local variable, then modify the data in the
	 * local variable, then copy the local variable back to shared memory.
	 * Only the last step has to be inside the critical section.
	 *
	 * Most of the data we copy from shared memory is just going to be
	 * overwritten, but the struct's not so large that it's worth the
	 * maintenance hassle to copy only the needful fields.
	 */
	memcpy(&lbeentry,
		   (char *) vbeentry,
		   sizeof(PgBackendStatus));

	/* This struct can just start from zeroes each time, though */
#ifdef USE_SSL
	memset(&lsslstatus, 0, sizeof(lsslstatus));
#endif

	/*
	 * Now fill in all the fields of lbeentry, except for strings that are
	 * out-of-line data.  Those have to be handled separately, below.
	 */
	lbeentry.st_procpid = MySessionPid;

	if (MyBackendId != InvalidBackendId)
	{
		if (IsAutoVacuumLauncherProcess())
		{
			/* Autovacuum Launcher */
			lbeentry.st_backendType = B_AUTOVAC_LAUNCHER;
		}
		else if (IsAutoVacuumWorkerProcess())
		{
			/* Autovacuum Worker */
			lbeentry.st_backendType = B_AUTOVAC_WORKER;
		}
		else if (am_walsender)
		{
			/* Wal sender */
			lbeentry.st_backendType = B_WAL_SENDER;
		}
		else if (IsBackgroundWorker)
		{
			/* bgworker */
			lbeentry.st_backendType = B_BG_WORKER;
		}
		else if (IsPolarDispatcher)
		{
			lbeentry.st_backendType = B_POLAR_DISPATCHER;
		}
		else
		{
			/* client-backend */
			lbeentry.st_backendType = B_BACKEND;
		}
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);
		switch (MyAuxProcType)
		{
			case StartupProcess:
				lbeentry.st_backendType = B_STARTUP;
				break;
			case BgWriterProcess:
				lbeentry.st_backendType = B_BG_WRITER;
				break;
			case CheckpointerProcess:
				lbeentry.st_backendType = B_CHECKPOINTER;
				break;
			case WalWriterProcess:
				lbeentry.st_backendType = B_WAL_WRITER;
				break;
			case WalReceiverProcess:
				lbeentry.st_backendType = B_WAL_RECEIVER;
				break;
			case ConsensusProcess:
				lbeentry.st_backendType = B_CONSENSUS;
				break;
			case PolarWalPipelinerProcess:
				lbeentry.st_backendType = B_POLAR_WAL_PIPELINER;
				break;
			case LogIndexBgWriterProcess:
				lbeentry.st_backendType = B_BG_LOGINDEX;
				break;
			case FlogBgInserterProcess:
				lbeentry.st_backendType = B_BG_FLOG_INSERTER;
				break;
			case FlogBgWriterProcess:
				lbeentry.st_backendType = B_BG_FLOG_WRITER;
				break;
			default:
				elog(FATAL, "unrecognized process type: %d",
					 (int) MyAuxProcType);
		}
	}

	/*
	 * If we have a MyProcPort, use its session start time (for consistency,
	 * and to save a kernel call).
	 */
	if (MyProcPort)
		lbeentry.st_proc_start_timestamp = MyProcPort->SessionStartTime;
	else
		lbeentry.st_proc_start_timestamp = GetCurrentTimestamp();

	lbeentry.st_activity_start_timestamp = 0;
	lbeentry.st_state_start_timestamp = 0;
	lbeentry.last_wait_start_timestamp = 0;
	lbeentry.st_xact_start_timestamp = 0;
	lbeentry.st_databaseid = MyDatabaseId;

	/* We have userid for client-backends, wal-sender and bgworker processes */
	if (lbeentry.st_backendType == B_BACKEND
		|| lbeentry.st_backendType == B_WAL_SENDER
		|| lbeentry.st_backendType == B_BG_WORKER)
		lbeentry.st_userid = GetSessionUserId();
	else
		lbeentry.st_userid = InvalidOid;

	/*
	 * We may not have a MyProcPort (eg, if this is the autovacuum process).
	 * If so, use all-zeroes client address, which is dealt with specially in
	 * pg_stat_get_backend_client_addr and pg_stat_get_backend_client_port.
	 */
	if (MyProcPort)
		memcpy(&lbeentry.st_clientaddr, &MyProcPort->raddr,
			   sizeof(lbeentry.st_clientaddr));
	else
		MemSet(&lbeentry.st_clientaddr, 0, sizeof(lbeentry.st_clientaddr));

	/* POLAR */
	if (MyProcPort && MyProcPort->polar_proxy)
	{
		lbeentry.polar_st_proxy = true;
		lbeentry.polar_st_origin_addr = MyProcPort->polar_origin_addr;
		if (polar_enable_virtual_pid && !polar_in_replica_mode())
		{
			lbeentry.polar_st_virtual_pid = MySessionPid + POLAR_BASE_VIRTUAL_PID;
			lbeentry.polar_st_cancel_key = MyCancelKey;
		}
		else
		{
			lbeentry.polar_st_virtual_pid = 0;
			lbeentry.polar_st_cancel_key = 0;
		}
	}
	else
	{
		SockAddr zeroAddr;
		lbeentry.polar_st_proxy = false;
		memset(&zeroAddr, 0, sizeof(zeroAddr));
		lbeentry.polar_st_origin_addr = zeroAddr;
		lbeentry.polar_st_virtual_pid = 0;
		lbeentry.polar_st_cancel_key = 0;
	}
	/* POLAR end */

#ifdef USE_SSL
	if (MyProcPort && MyProcPort->ssl != NULL)
	{
		lbeentry.st_ssl = true;
		lsslstatus.ssl_bits = be_tls_get_cipher_bits(MyProcPort);
		lsslstatus.ssl_compression = be_tls_get_compression(MyProcPort);
		strlcpy(lsslstatus.ssl_version, be_tls_get_version(MyProcPort), NAMEDATALEN);
		strlcpy(lsslstatus.ssl_cipher, be_tls_get_cipher(MyProcPort), NAMEDATALEN);
		be_tls_get_peerdn_name(MyProcPort, lsslstatus.ssl_clientdn, NAMEDATALEN);
	}
	else
	{
		lbeentry.st_ssl = false;
	}
#else
	lbeentry.st_ssl = false;
#endif

	lbeentry.st_state = STATE_UNDEFINED;
	lbeentry.st_progress_command = PROGRESS_COMMAND_INVALID;
	lbeentry.st_progress_command_target = InvalidOid;

	/*
	 * we don't zero st_progress_param here to save cycles; nobody should
	 * examine it until st_progress_command has been set to something other
	 * than PROGRESS_COMMAND_INVALID
	 */

	/*
	 * We're ready to enter the critical section that fills the shared-memory
	 * status entry.  We follow the protocol of bumping st_changecount before
	 * and after; and make sure it's even afterwards.  We use a volatile
	 * pointer here to ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(vbeentry);

	/* make sure we'll memcpy the same st_changecount back */
	lbeentry.st_changecount = vbeentry->st_changecount;

	memcpy((char *) vbeentry,
		   &lbeentry,
		   sizeof(PgBackendStatus));

	/*
	 * We can write the out-of-line strings and structs using the pointers
	 * that are in lbeentry; this saves some de-volatilizing messiness.
	 */
	lbeentry.st_appname[0] = '\0';
	if (MyProcPort && MyProcPort->remote_hostname)
		strlcpy(lbeentry.st_clienthostname, MyProcPort->remote_hostname,
				NAMEDATALEN);
	else
		lbeentry.st_clienthostname[0] = '\0';
	lbeentry.st_activity_raw[0] = '\0';
	/* Also make sure the last byte in each string area is always 0 */
	lbeentry.st_appname[NAMEDATALEN - 1] = '\0';
	lbeentry.st_clienthostname[NAMEDATALEN - 1] = '\0';
	lbeentry.st_activity_raw[pgstat_track_activity_query_size - 1] = '\0';

#undef MyProcPort

#ifdef USE_SSL
	memcpy(lbeentry.st_sslstatus, &lsslstatus, sizeof(PgBackendSSLStatus));
#endif

	PGSTAT_END_WRITE_ACTIVITY(vbeentry);

	/* Update app name to current GUC setting */
	if (application_name)
		pgstat_report_appname(application_name);
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgstat_beshutdown_hook(int code, Datum arg)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (IS_POLAR_SESSION_SHARED())
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];

	/*
	 * If we got as far as discovering our own database ID, we can report what
	 * we did to the collector.  Otherwise, we'd be sending an invalid
	 * database ID, so forget it.  (This means that accesses to pg_database
	 * during failed backend starts might never get counted.)
	 */
	if (OidIsValid(MyDatabaseId))
		pgstat_report_stat(true);

	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->st_procpid = 0;	/* mark invalid */

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

void
polar_pgstat_beshutdown(int id)
{
	volatile PgBackendStatus *beentry = &BackendStatusArray[id + NumBackendStatSlots_base];
	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->st_procpid = 0;	/* mark invalid */
	beentry->session_local_id = 0;
	beentry->last_backend_pid = 0;
	beentry->dispatcher_pid = 0;
	beentry->saved_guc_count = 0;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
	ELOG_PSS(DEBUG1, "polar_pgstat_beshutdown %d", id);
}

/* ----------
 * pgstat_report_activity() -
 *
 *	Called from tcop/postgres.c to report what the backend is actually doing
 *	(but note cmd_str can be NULL for certain cases).
 *
 * All updates of the status entry follow the protocol of bumping
 * st_changecount before and after.  We use a volatile pointer here to
 * ensure the compiler doesn't try to get cute.
 * ----------
 */
void
pgstat_report_activity(BackendState state, const char *cmd_str)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	TimestampTz start_timestamp;
	TimestampTz current_timestamp;
	TimestampTz last_wait_start_timestamp = 0;
	int			len = 0;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
		last_wait_start_timestamp = polar_session()->last_wait_start_timestamp;
	}

	TRACE_POSTGRESQL_STATEMENT_STATUS(cmd_str);

	if (!beentry)
		return;

	if (!pgstat_track_activities)
	{
		if (beentry->st_state != STATE_DISABLED)
		{
			volatile PGPROC *proc = MyProc;

			/*
			 * track_activities is disabled, but we last reported a
			 * non-disabled state.  As our final update, change the state and
			 * clear fields we will not be updating anymore.
			 */
			PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
			beentry->st_state = STATE_DISABLED;
			beentry->st_state_start_timestamp = 0;
			beentry->last_wait_start_timestamp = 0;
			beentry->st_activity_raw[0] = '\0';
			beentry->st_activity_start_timestamp = 0;
			/* st_xact_start_timestamp and wait_event_info are also disabled */
			beentry->st_xact_start_timestamp = 0;
			proc->wait_event_info = 0;
			PGSTAT_END_WRITE_ACTIVITY(beentry);
		}
		return;
	}
	/* POLAR: count audit log information */
	if (state == STATE_RUNNING)
	{
		MemSet(&polar_audit_log, 0, sizeof(Pg_audit_log));
		polar_audit_log.examined_row_count = polar_pgstat_get_current_examined_row_count();
	}
	/* POLAR end */

	/* POLAR: debug info for xact split */
	if (unlikely(polar_enable_xact_split_debug) && cmd_str != NULL
		&& strstr(cmd_str, "POLAR_XACT_SPLIT_DEBUG") != NULL)
		elog(LOG, "xact split: execute in: %s; query: %s; xids: %s", polar_in_replica_mode() ? "RO" : "RW",
			 cmd_str, polar_xact_split_xids);
	/* POLAR end */

	/*
	 * To minimize the time spent modifying the entry, and avoid risk of
	 * errors inside the critical section, fetch all the needed data first.
	 */
	start_timestamp = GetCurrentStatementStartTimestamp();
	if (cmd_str != NULL)
	{
		/*
		 * Compute length of to-be-stored string unaware of multi-byte
		 * characters. For speed reasons that'll get corrected on read, rather
		 * than computed every write.
		 */
		len = Min(strlen(cmd_str), pgstat_track_activity_query_size - 1);
	}
	current_timestamp = GetCurrentTimestamp();

	/*
	 * Now update the status entry
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;
	beentry->st_state = state;
	beentry->st_state_start_timestamp = current_timestamp;
	beentry->last_wait_start_timestamp = last_wait_start_timestamp;

	if (cmd_str != NULL)
	{
		memcpy((char *) beentry->st_activity_raw, cmd_str, len);
		beentry->st_activity_raw[len] = '\0';
		beentry->st_activity_start_timestamp = start_timestamp;
	}

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * polar_report_queryid() -
 *
 * Set queryid in own backend entry
 * 
 * POLAR *
 *-----------
 */
void
polar_report_queryid(int64 queryid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->queryid = queryid;
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_start_command() -
 *
 * Set st_progress_command (and st_progress_command_target) in own backend
 * entry.  Also, zero-initialize st_progress_param array.
 *-----------
 */
void
pgstat_progress_start_command(ProgressCommandType cmdtype, Oid relid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	beentry->st_progress_command = cmdtype;
	beentry->st_progress_command_target = relid;
	MemSet(&beentry->st_progress_param, 0, sizeof(beentry->st_progress_param));
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_param() -
 *
 * Update index'th member in st_progress_param[] of own backend entry.
 *-----------
 */
void
pgstat_progress_update_param(int index, int64 val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	Assert(index >= 0 && index < PGSTAT_NUM_PROGRESS_PARAM);

	if (!beentry || !pgstat_track_activities)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	beentry->st_progress_param[index] = val;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_update_multi_param() -
 *
 * Update multiple members in st_progress_param[] of own backend entry.
 * This is atomic; readers won't see intermediate states.
 *-----------
 */
void
pgstat_progress_update_multi_param(int nparam, const int *index,
								   const int64 *val)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			i;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!beentry || !pgstat_track_activities || nparam == 0)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	for (i = 0; i < nparam; ++i)
	{
		Assert(index[i] >= 0 && index[i] < PGSTAT_NUM_PROGRESS_PARAM);

		beentry->st_progress_param[index[i]] = val[i];
	}

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*-----------
 * pgstat_progress_end_command() -
 *
 * Reset st_progress_command (and st_progress_command_target) in own backend
 * entry.  This signals the end of the command.
 *-----------
 */
void
pgstat_progress_end_command(void)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (!beentry || !pgstat_track_activities)
		return;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (beentry->st_progress_command == PROGRESS_COMMAND_INVALID)
		return;

	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;
	beentry->st_progress_command = PROGRESS_COMMAND_INVALID;
	beentry->st_progress_command_target = InvalidOid;
	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/* ----------
 * pgstat_report_appname() -
 *
 *	Called to update our application name.
 * ----------
 */
void
pgstat_report_appname(const char *appname)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			len;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!beentry)
		return;

	/* This should be unnecessary if GUC did its job, but be safe */
	len = pg_mbcliplen(appname, strlen(appname), NAMEDATALEN - 1);

	/*
	 * Update my status entry, following the protocol of bumping
	 * st_changecount before and after.  We use a volatile pointer here to
	 * ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	memcpy((char *) beentry->st_appname, appname, len);
	beentry->st_appname[len] = '\0';

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/*
 * Report current transaction start timestamp as the specified value.
 * Zero means there is no active transaction.
 */
void
pgstat_report_xact_timestamp(TimestampTz tstamp)
{
	volatile PgBackendStatus *beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!pgstat_track_activities || !beentry)
		return;

	/*
	 * Update my status entry, following the protocol of bumping
	 * st_changecount before and after.  We use a volatile pointer here to
	 * ensure the compiler doesn't try to get cute.
	 */
	PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
	beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	beentry->saved_guc_count = saved_guc_count;

	beentry->st_xact_start_timestamp = tstamp;

	PGSTAT_END_WRITE_ACTIVITY(beentry);
}

/* ----------
 * pgstat_read_current_status() -
 *
 *	Copy the current contents of the PgBackendStatus array to local memory,
 *	if not already done in this transaction.
 * ----------
 */
static void
pgstat_read_current_status(void)
{
	volatile PgBackendStatus *beentry;
	LocalPgBackendStatus *localtable;
	LocalPgBackendStatus *localentry;
	char	   *localappname,
			   *localclienthostname,
			   *localactivity;
#ifdef USE_SSL
	PgBackendSSLStatus *localsslstatus;
#endif
	int			i;

	Assert(!pgStatRunningInCollector);
	if (localBackendStatusTable)
		return;					/* already done */

	pgstat_setup_memcxt();

	localtable = (LocalPgBackendStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(LocalPgBackendStatus) * NumBackendStatSlots);
	localappname = (char *)
		MemoryContextAlloc(pgStatLocalContext,
						   NAMEDATALEN * NumBackendStatSlots);
	localclienthostname = (char *)
		MemoryContextAlloc(pgStatLocalContext,
						   NAMEDATALEN * NumBackendStatSlots);
	localactivity = (char *)
		MemoryContextAlloc(pgStatLocalContext,
						   pgstat_track_activity_query_size * NumBackendStatSlots);
#ifdef USE_SSL
	localsslstatus = (PgBackendSSLStatus *)
		MemoryContextAlloc(pgStatLocalContext,
						   sizeof(PgBackendSSLStatus) * NumBackendStatSlots);
#endif

	localNumBackends = 0;

	beentry = BackendStatusArray;
	localentry = localtable;
	for (i = 1; i <= NumBackendStatSlots; i++)
	{
		/*
		 * Follow the protocol of retrying if st_changecount changes while we
		 * copy the entry, or if it's odd.  (The check for odd is needed to
		 * cover the case where we are able to completely copy the entry while
		 * the source backend is between increment steps.)	We use a volatile
		 * pointer here to ensure the compiler doesn't try to get cute.
		 */
		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_begin_read_activity(beentry, before_changecount);

			localentry->backendStatus.st_procpid = beentry->st_procpid;
			/* Skip all the data-copying work if entry is not in use */
			if (localentry->backendStatus.st_procpid > 0)
			{
				memcpy(&localentry->backendStatus, (char *) beentry, sizeof(PgBackendStatus));

				/*
				 * For each PgBackendStatus field that is a pointer, copy the
				 * pointed-to data, then adjust the local copy of the pointer
				 * field to point at the local copy of the data.
				 *
				 * strcpy is safe even if the string is modified concurrently,
				 * because there's always a \0 at the end of the buffer.
				 */
				strcpy(localappname, (char *) beentry->st_appname);
				localentry->backendStatus.st_appname = localappname;
				strcpy(localclienthostname, (char *) beentry->st_clienthostname);
				localentry->backendStatus.st_clienthostname = localclienthostname;
				strcpy(localactivity, (char *) beentry->st_activity_raw);
				localentry->backendStatus.st_activity_raw = localactivity;
#ifdef USE_SSL
				if (beentry->st_ssl)
				{
					memcpy(localsslstatus, beentry->st_sslstatus, sizeof(PgBackendSSLStatus));
					localentry->backendStatus.st_sslstatus = localsslstatus;
				}
#endif
				/* POLAR: we must know backend id here for stat */
				localentry->backendStatus.backendid = i;
				/* POLAR end */
			}

			pgstat_end_read_activity(beentry, after_changecount);

			if (pgstat_read_activity_complete(before_changecount,
											  after_changecount))
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		beentry++;
		/* Only valid entries get included into the local array */
		if (localentry->backendStatus.st_procpid > 0)
		{
			BackendIdGetTransactionIds(i,
									   &localentry->backend_xid,
									   &localentry->backend_xmin);

			localentry++;
			localappname += NAMEDATALEN;
			localclienthostname += NAMEDATALEN;
			localactivity += pgstat_track_activity_query_size;
#ifdef USE_SSL
			localsslstatus++;
#endif
			localNumBackends++;
			Assert(localNumBackends <= NumBackendStatSlots);
		}
	}

	/* Set the pointer only after completion of a valid table */
	localBackendStatusTable = localtable;
}

/* ----------
 * pgstat_get_wait_event_type() -
 *
 *	Return a string representing the current wait event type, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event_type(uint32 wait_event_info)
{
	uint32		classId;
	const char *event_type;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_type = "LWLock";
			break;
		case PG_WAIT_LOCK:
			event_type = "Lock";
			break;
		case PG_WAIT_BUFFER_PIN:
			event_type = "BufferPin";
			break;
		case PG_WAIT_ACTIVITY:
			event_type = "Activity";
			break;
		case PG_WAIT_CLIENT:
			event_type = "Client";
			break;
		case PG_WAIT_EXTENSION:
			event_type = "Extension";
			break;
		case PG_WAIT_IPC:
			event_type = "IPC";
			break;
		case PG_WAIT_TIMEOUT:
			event_type = "Timeout";
			break;
		case PG_WAIT_IO:
			event_type = "IO";
			break;
		default:
			event_type = "???";
			break;
	}

	return event_type;
}

/* ----------
 * pgstat_get_wait_event() -
 *
 *	Return a string representing the current wait event, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event(uint32 wait_event_info)
{
	uint32		classId;
	uint16		eventId;
	const char *event_name;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & 0xFF000000;
	eventId = wait_event_info & 0x0000FFFF;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_name = GetLWLockIdentifier(classId, eventId);
			break;
		case PG_WAIT_LOCK:
			event_name = GetLockNameFromTagType(eventId);
			break;
		case PG_WAIT_BUFFER_PIN:
			event_name = "BufferPin";
			break;
		case PG_WAIT_ACTIVITY:
			{
				WaitEventActivity w = (WaitEventActivity) wait_event_info;

				event_name = pgstat_get_wait_activity(w);
				break;
			}
		case PG_WAIT_CLIENT:
			{
				WaitEventClient w = (WaitEventClient) wait_event_info;

				event_name = pgstat_get_wait_client(w);
				break;
			}
		case PG_WAIT_EXTENSION:
			event_name = "Extension";
			break;
		case PG_WAIT_IPC:
			{
				WaitEventIPC w = (WaitEventIPC) wait_event_info;

				event_name = pgstat_get_wait_ipc(w);
				break;
			}
		case PG_WAIT_TIMEOUT:
			{
				WaitEventTimeout w = (WaitEventTimeout) wait_event_info;

				event_name = pgstat_get_wait_timeout(w);
				break;
			}
		case PG_WAIT_IO:
			{
				WaitEventIO w = (WaitEventIO) wait_event_info;

				event_name = pgstat_get_wait_io(w);
				break;
			}
		default:
			event_name = "unknown wait event";
			break;
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_activity() -
 *
 * Convert WaitEventActivity to string.
 * ----------
 */
static const char *
pgstat_get_wait_activity(WaitEventActivity w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_ARCHIVER_MAIN:
			event_name = "ArchiverMain";
			break;
		case WAIT_EVENT_AUTOVACUUM_MAIN:
			event_name = "AutoVacuumMain";
			break;
		case WAIT_EVENT_BGWRITER_HIBERNATE:
			event_name = "BgWriterHibernate";
			break;
		case WAIT_EVENT_BGWRITER_MAIN:
			event_name = "BgWriterMain";
			break;
		case WAIT_EVENT_CHECKPOINTER_MAIN:
			event_name = "CheckpointerMain";
			break;
		case WAIT_EVENT_LOGICAL_LAUNCHER_MAIN:
			event_name = "LogicalLauncherMain";
			break;
		case WAIT_EVENT_LOGICAL_APPLY_MAIN:
			event_name = "LogicalApplyMain";
			break;
		case WAIT_EVENT_PGSTAT_MAIN:
			event_name = "PgStatMain";
			break;
		case WAIT_EVENT_RECOVERY_WAL_ALL:
			event_name = "RecoveryWalAll";
			break;
		case WAIT_EVENT_RECOVERY_WAL_STREAM:
			event_name = "RecoveryWalStream";
			break;
		case WAIT_EVENT_SYSLOGGER_MAIN:
			event_name = "SysLoggerMain";
			break;
		case WAIT_EVENT_WAL_RECEIVER_MAIN:
			event_name = "WalReceiverMain";
			break;
		case WAIT_EVENT_WAL_SENDER_MAIN:
			event_name = "WalSenderMain";
			break;
		case WAIT_EVENT_WAL_WRITER_MAIN:
			event_name = "WalWriterMain";
			break;
		/* POLAR: new add func for dml delay */
		case WAIT_EVENT_DELAY_DML:
			event_name = "PolarDmlDelay";
			break;
		/* POLAR end */
		/* POLAR: main func for async redo */
		/*no cover begin*/
		case WAIT_EVENT_ASYNC_REDO_MAIN:
			event_name = "AsyncRedoMain";
			break;
		/*no cover end*/
		case WAIT_EVENT_ASYNC_DDL_LOCK_REPLAY_MAIN:
			event_name = "AsyncDdlLockReplayMain";
			break;
		case WAIT_EVENT_CONSENSUS_MAIN:
			event_name = "ConsensusMain";
			break;
		case WAIT_EVENT_DATAMAX_MAIN:
			event_name = "DataMaxMain";
			break;
		case WAIT_EVENT_POLAR_SUB_TASK_MAIN:
			event_name = "PolarSubTaskMain";
			break;
		case WAIT_EVENT_LOGINDEX_BG_MAIN:
			event_name = "LogIndexBgMain";
			break;
		/*no cover begin*/
		case WAIT_EVENT_FLOG_WRITE_BG_MAIN:
			event_name = "FlashbackLogWriteBgMain";
			break;
		case WAIT_EVENT_FLOG_INSERT_BG_MAIN:
			event_name = "FlashbackLogInsertBgMain";
			break;
		/*no cover end*/
		/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_client() -
 *
 * Convert WaitEventClient to string.
 * ----------
 */
static const char *
pgstat_get_wait_client(WaitEventClient w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_CLIENT_READ:
			event_name = "ClientRead";
			break;
		case WAIT_EVENT_CLIENT_WRITE:
			event_name = "ClientWrite";
			break;
		case WAIT_EVENT_LIBPQWALRECEIVER_CONNECT:
			event_name = "LibPQWalReceiverConnect";
			break;
		case WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE:
			event_name = "LibPQWalReceiverReceive";
			break;
		case WAIT_EVENT_SSL_OPEN_SERVER:
			event_name = "SSLOpenServer";
			break;
		case WAIT_EVENT_WAL_RECEIVER_WAIT_START:
			event_name = "WalReceiverWaitStart";
			break;
		case WAIT_EVENT_WAL_SENDER_WAIT_WAL:
			event_name = "WalSenderWaitForWAL";
			break;
		case WAIT_EVENT_WAL_SENDER_WRITE_DATA:
			event_name = "WalSenderWriteData";
			break;
		case WAIT_EVENT_PX_MOTION_WAIT_RECV:
			event_name = "PXMotionWaitRecv";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_ipc() -
 *
 * Convert WaitEventIPC to string.
 * ----------
 */
static const char *
pgstat_get_wait_ipc(WaitEventIPC w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BGWORKER_SHUTDOWN:
			event_name = "BgWorkerShutdown";
			break;
		case WAIT_EVENT_BGWORKER_STARTUP:
			event_name = "BgWorkerStartup";
			break;
		case WAIT_EVENT_BTREE_PAGE:
			event_name = "BtreePage";
			break;
		case WAIT_EVENT_EXECUTE_GATHER:
			event_name = "ExecuteGather";
			break;
		case WAIT_EVENT_HASH_BATCH_ALLOCATING:
			event_name = "Hash/Batch/Allocating";
			break;
		case WAIT_EVENT_HASH_BATCH_ELECTING:
			event_name = "Hash/Batch/Electing";
			break;
		case WAIT_EVENT_HASH_BATCH_LOADING:
			event_name = "Hash/Batch/Loading";
			break;
		case WAIT_EVENT_HASH_BUILD_ALLOCATING:
			event_name = "Hash/Build/Allocating";
			break;
		case WAIT_EVENT_HASH_BUILD_ELECTING:
			event_name = "Hash/Build/Electing";
			break;
		case WAIT_EVENT_HASH_BUILD_HASHING_INNER:
			event_name = "Hash/Build/HashingInner";
			break;
		case WAIT_EVENT_HASH_BUILD_HASHING_OUTER:
			event_name = "Hash/Build/HashingOuter";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATING:
			event_name = "Hash/GrowBatches/Allocating";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_DECIDING:
			event_name = "Hash/GrowBatches/Deciding";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_ELECTING:
			event_name = "Hash/GrowBatches/Electing";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_FINISHING:
			event_name = "Hash/GrowBatches/Finishing";
			break;
		case WAIT_EVENT_HASH_GROW_BATCHES_REPARTITIONING:
			event_name = "Hash/GrowBatches/Repartitioning";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATING:
			event_name = "Hash/GrowBuckets/Allocating";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_ELECTING:
			event_name = "Hash/GrowBuckets/Electing";
			break;
		case WAIT_EVENT_HASH_GROW_BUCKETS_REINSERTING:
			event_name = "Hash/GrowBuckets/Reinserting";
			break;
		case WAIT_EVENT_LOGICAL_SYNC_DATA:
			event_name = "LogicalSyncData";
			break;
		case WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE:
			event_name = "LogicalSyncStateChange";
			break;
		case WAIT_EVENT_MQ_INTERNAL:
			event_name = "MessageQueueInternal";
			break;
		case WAIT_EVENT_MQ_PUT_MESSAGE:
			event_name = "MessageQueuePutMessage";
			break;
		case WAIT_EVENT_MQ_RECEIVE:
			event_name = "MessageQueueReceive";
			break;
		case WAIT_EVENT_MQ_SEND:
			event_name = "MessageQueueSend";
			break;
		case WAIT_EVENT_PARALLEL_FINISH:
			event_name = "ParallelFinish";
			break;
		case WAIT_EVENT_PARALLEL_BITMAP_SCAN:
			event_name = "ParallelBitmapScan";
			break;
		case WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN:
			event_name = "ParallelCreateIndexScan";
			break;
		case WAIT_EVENT_PROCARRAY_GROUP_UPDATE:
			event_name = "ProcArrayGroupUpdate";
			break;
		case WAIT_EVENT_CLOG_GROUP_UPDATE:
			event_name = "ClogGroupUpdate";
			break;
		case WAIT_EVENT_REPLICATION_ORIGIN_DROP:
			event_name = "ReplicationOriginDrop";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_DROP:
			event_name = "ReplicationSlotDrop";
			break;
		case WAIT_EVENT_SAFE_SNAPSHOT:
			event_name = "SafeSnapshot";
			break;
		case WAIT_EVENT_SMGR_DROP_SYNC:
			event_name = "SmgrDropSync";
			break;
		case WAIT_EVENT_SYNC_REP:
			event_name = "SyncRep";
			break;
		case WAIT_EVENT_CONSENSUS_COMMIT:
			event_name = "ConsensusCommit";
			break;
		case WAIT_EVENT_CONSENSUS:
			event_name = "Consensus";
			break;
		case WAIT_EVENT_WAL_PIPELINE_WAIT_RECENT_WRITTEN_SPACE:
			event_name = "PolarWALPipelineWaitRecentWrittenSpace";
			break;
		case WAIT_EVENT_WAL_PIPELINE_WAIT_UNFLUSHED_XLOG_SLOT:
			event_name = "PolarWALPipelineWaitUnflushedXlogSlot";
			break;
			
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_timeout() -
 *
 * Convert WaitEventTimeout to string.
 * ----------
 */
static const char *
pgstat_get_wait_timeout(WaitEventTimeout w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BASE_BACKUP_THROTTLE:
			event_name = "BaseBackupThrottle";
			break;
		case WAIT_EVENT_PG_SLEEP:
			event_name = "PgSleep";
			break;
		case WAIT_EVENT_RECOVERY_APPLY_DELAY:
			event_name = "RecoveryApplyDelay";
			break;
			/* no default case, so that compiler will warn */
	}

	return event_name;
}

/* ----------
 * pgstat_get_wait_io() -
 *
 * Convert WaitEventIO to string.
 * ----------
 */
static const char *
pgstat_get_wait_io(WaitEventIO w)
{
	const char *event_name = "unknown wait event";

	switch (w)
	{
		case WAIT_EVENT_BUFFILE_READ:
			event_name = "BufFileRead";
			break;
		case WAIT_EVENT_BUFFILE_WRITE:
			event_name = "BufFileWrite";
			break;
		case WAIT_EVENT_CONTROL_FILE_READ:
			event_name = "ControlFileRead";
			break;
		case WAIT_EVENT_CONTROL_FILE_SYNC:
			event_name = "ControlFileSync";
			break;
		case WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE:
			event_name = "ControlFileSyncUpdate";
			break;
		case WAIT_EVENT_CONTROL_FILE_WRITE:
			event_name = "ControlFileWrite";
			break;
		case WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE:
			event_name = "ControlFileWriteUpdate";
			break;
		case WAIT_EVENT_COPY_FILE_READ:
			event_name = "CopyFileRead";
			break;
		case WAIT_EVENT_COPY_FILE_WRITE:
			event_name = "CopyFileWrite";
			break;
		case WAIT_EVENT_DATA_FILE_EXTEND:
			event_name = "DataFileExtend";
			break;
		case WAIT_EVENT_DATA_FILE_FLUSH:
			event_name = "DataFileFlush";
			break;
		case WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC:
			event_name = "DataFileImmediateSync";
			break;
		case WAIT_EVENT_DATA_FILE_PREFETCH:
			event_name = "DataFilePrefetch";
			break;
		case WAIT_EVENT_DATA_FILE_READ:
			event_name = "DataFileRead";
			break;
		case WAIT_EVENT_DATA_FILE_SYNC:
			event_name = "DataFileSync";
			break;
		case WAIT_EVENT_DATA_FILE_TRUNCATE:
			event_name = "DataFileTruncate";
			break;
		case WAIT_EVENT_DATA_FILE_WRITE:
			event_name = "DataFileWrite";
			break;
		case WAIT_EVENT_DSM_FILL_ZERO_WRITE:
			event_name = "DSMFillZeroWrite";
			break;
		/*no cover begin*/
		case WAIT_EVENT_KMGR_FILE_READ:
			event_name = "KmgrFileRead";
			break;
		case WAIT_EVENT_KMGR_FILE_SYNC:
			event_name = "KmgrFileSync";
			break;
		case WAIT_EVENT_KMGR_FILE_WRITE:
			event_name = "KmgrFileWrite";
			break;
		/*no cover end*/
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ:
			event_name = "LockFileAddToDataDirRead";
			break;
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC:
			event_name = "LockFileAddToDataDirSync";
			break;
		case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE:
			event_name = "LockFileAddToDataDirWrite";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_READ:
			event_name = "LockFileCreateRead";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_SYNC:
			event_name = "LockFileCreateSync";
			break;
		case WAIT_EVENT_LOCK_FILE_CREATE_WRITE:
			event_name = "LockFileCreateWrite";
			break;
		case WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ:
			event_name = "LockFileReCheckDataDirRead";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC:
			event_name = "LogicalRewriteCheckpointSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC:
			event_name = "LogicalRewriteMappingSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE:
			event_name = "LogicalRewriteMappingWrite";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_SYNC:
			event_name = "LogicalRewriteSync";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE:
			event_name = "LogicalRewriteTruncate";
			break;
		case WAIT_EVENT_LOGICAL_REWRITE_WRITE:
			event_name = "LogicalRewriteWrite";
			break;
		case WAIT_EVENT_RELATION_MAP_READ:
			event_name = "RelationMapRead";
			break;
		case WAIT_EVENT_RELATION_MAP_SYNC:
			event_name = "RelationMapSync";
			break;
		case WAIT_EVENT_RELATION_MAP_WRITE:
			event_name = "RelationMapWrite";
			break;
		case WAIT_EVENT_REORDER_BUFFER_READ:
			event_name = "ReorderBufferRead";
			break;
		case WAIT_EVENT_REORDER_BUFFER_WRITE:
			event_name = "ReorderBufferWrite";
			break;
		case WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ:
			event_name = "ReorderLogicalMappingRead";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_READ:
			event_name = "ReplicationSlotRead";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC:
			event_name = "ReplicationSlotRestoreSync";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_SYNC:
			event_name = "ReplicationSlotSync";
			break;
		case WAIT_EVENT_REPLICATION_SLOT_WRITE:
			event_name = "ReplicationSlotWrite";
			break;
		case WAIT_EVENT_SLRU_FLUSH_SYNC:
			event_name = "SLRUFlushSync";
			break;
		case WAIT_EVENT_SLRU_READ:
			event_name = "SLRURead";
			break;
		case WAIT_EVENT_SLRU_SYNC:
			event_name = "SLRUSync";
			break;
		case WAIT_EVENT_SLRU_WRITE:
			event_name = "SLRUWrite";
			break;
		case WAIT_EVENT_SNAPBUILD_READ:
			event_name = "SnapbuildRead";
			break;
		case WAIT_EVENT_SNAPBUILD_SYNC:
			event_name = "SnapbuildSync";
			break;
		case WAIT_EVENT_SNAPBUILD_WRITE:
			event_name = "SnapbuildWrite";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC:
			event_name = "TimelineHistoryFileSync";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE:
			event_name = "TimelineHistoryFileWrite";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_READ:
			event_name = "TimelineHistoryRead";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_SYNC:
			event_name = "TimelineHistorySync";
			break;
		case WAIT_EVENT_TIMELINE_HISTORY_WRITE:
			event_name = "TimelineHistoryWrite";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_READ:
			event_name = "TwophaseFileRead";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_SYNC:
			event_name = "TwophaseFileSync";
			break;
		case WAIT_EVENT_TWOPHASE_FILE_WRITE:
			event_name = "TwophaseFileWrite";
			break;
		case WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ:
			event_name = "WALSenderTimelineHistoryRead";
			break;
		case WAIT_EVENT_WAL_BOOTSTRAP_SYNC:
			event_name = "WALBootstrapSync";
			break;
		case WAIT_EVENT_WAL_BOOTSTRAP_WRITE:
			event_name = "WALBootstrapWrite";
			break;
		case WAIT_EVENT_WAL_COPY_READ:
			event_name = "WALCopyRead";
			break;
		case WAIT_EVENT_WAL_COPY_SYNC:
			event_name = "WALCopySync";
			break;
		case WAIT_EVENT_WAL_COPY_WRITE:
			event_name = "WALCopyWrite";
			break;
		case WAIT_EVENT_WAL_INIT_SYNC:
			event_name = "WALInitSync";
			break;
		case WAIT_EVENT_WAL_INIT_WRITE:
			event_name = "WALInitWrite";
			break;
		case WAIT_EVENT_WAL_READ:
			event_name = "WALRead";
			break;
		case WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN:
			event_name = "WALSyncMethodAssign";
			break;
		case WAIT_EVENT_WAL_WRITE:
			event_name = "WALWrite";
			break;
		case WAIT_EVENT_WAL_PREAD:
			event_name = "WALPread";
			break;
		case WAIT_EVENT_WAL_PWRITE:
			event_name = "WALPwrite";
			break;
		case WAIT_EVENT_DATA_FILE_PWRITE_EXTEND:
			event_name = "DataFilePwriteExtend";
			break;
		case WAIT_EVENT_DATA_FILE_PWRITE:
			event_name = "DataFilePwrite";
			break;
		case WAIT_EVENT_DATA_FILE_PREAD:
			event_name = "DataFilePread";
			break;
		case WAIT_EVENT_DATA_FILE_BATCH_PWRITE_EXTEND:
			event_name = "DataFileBatchPwriteExtend";
			break;
		case WAIT_EVENT_DATA_VFS_FILE_OPEN:
			event_name = "VFSFileOpen";
			break;
		case WAIT_EVENT_DATA_VFS_FILE_LSEEK:
			event_name = "VFSFileLseek";
			break;
		/* POLAR: Wait Events - logindex */
		case WAIT_EVENT_LOGINDEX_META_WRITE:
			event_name = "LogIndexMetaWrite";
			break;
		case WAIT_EVENT_LOGINDEX_META_READ:
			event_name = "LogIndexMetaRead";
			break;
		case WAIT_EVENT_LOGINDEX_META_FLUSH:
			event_name = "LogIndexMetaFlush";
			break;
		case WAIT_EVENT_LOGINDEX_TBL_WRITE:
			event_name = "LogIndexTblWrite";
			break;
		case WAIT_EVENT_LOGINDEX_TBL_READ:
			event_name = "LogIndexTblRead";
			break;
		case WAIT_EVENT_LOGINDEX_TBL_FLUSH:
			event_name = "LogIndexTblFlush";
			break;
		case  WAIT_EVENT_LOGINDEX_QUEUE_SPACE:
			event_name = "LogIndexQueueFreeup";
			break;
		case WAIT_EVENT_LOGINDEX_WAIT_ACTIVE:
			event_name = "LogIndexWaitActive";
			break;
		/*no cover begin*/
		case WAIT_EVENT_LOGINDEX_WAIT_FULLPAGE:
			event_name = "LogIndexWaitFullpage";
			break;
		case WAIT_EVENT_FULLPAGE_FILE_INIT_WRITE:
			event_name = "FullpageFileInitWrite";
			break;
		case WAIT_EVENT_REL_SIZE_CACHE_WRITE:
			event_name = "RelSizeCacheWrite";
			break;
		case WAIT_EVENT_REL_SIZE_CACHE_READ:
			event_name = "RelSizeCacheRead";
			break;
		/*no cover end*/
		case WAIT_EVENT_SYSLOG_PIPE_WRITE:
			event_name = "SyslogPipeWrite";
			break;
		case WAIT_EVENT_SYSLOG_CHANNEL_WRITE:
			event_name = "SyslogChannelWrite";
			break;
		case WAIT_EVENT_CACHE_LOCAL_OPEN:
			event_name = "PolarCacheLocalOpen";
			break;
		case WAIT_EVENT_CACHE_LOCAL_READ:
			event_name = "PolarCacheLocalRead";
			break;
		case WAIT_EVENT_CACHE_LOCAL_WRITE:
			event_name = "PolarCacheLocalWrite";
			break;
		case WAIT_EVENT_CACHE_LOCAL_LSEEK:
			event_name = "PolarCacheLocalLSeek";
			break;
		case WAIT_EVENT_CACHE_LOCAL_UNLINK:
			event_name = "PolarCacheLocalUnlink";
			break;
		case WAIT_EVENT_CACHE_LOCAL_SYNC:
			event_name = "PolarCacheLocalSync";
			break;
		case WAIT_EVENT_CACHE_LOCAL_STAT:
			event_name = "PolarCacheLocalStat";
			break;
		case WAIT_EVENT_CACHE_SHARED_OPEN:
			event_name = "PolarCacheSharedOpen";
			break;
		case WAIT_EVENT_CACHE_SHARED_READ:
			event_name = "PolarCacheSharedRead";
			break;
		case WAIT_EVENT_CACHE_SHARED_WRITE:
			event_name = "PolarCacheSharedWrite";
			break;
		case  WAIT_EVENT_CACHE_SHARED_LSEEK:
			event_name = "PolarCacheSharedLSeek";
			break;
		case WAIT_EVENT_CACHE_SHARED_UNLINK:
			event_name = "PolarCacheSharedUnlink";
			break;
		case WAIT_EVENT_CACHE_SHARED_SYNC:
			event_name = "PolarCacheSharedSync";
			break;
		case WAIT_EVENT_CACHE_SHARED_STAT:
			event_name = "PolarCacheSharedStat";
			break;
		case WAIT_EVENT_DATAMAX_META_READ:
			event_name = "PolarDataMaxMetaRead";
			break;
		case WAIT_EVENT_DATAMAX_META_WRITE:
			event_name = "PolarDataMaxMetaWrite";
			break;
		case WAIT_EVENT_WAL_PIPELINE_COMMIT_WAIT:
		 	event_name = "PolarWALPipelineCommitWait";
		    break;
		/*no cover begin*/
		case WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_WRITE:
			event_name = "PolarFlashbackLogCtlFileWrite";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_READ:
			event_name = "PolarFlashbackLogCtlFileRead";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_SYNC:
			event_name = "PolarFlashbackLogCtlFileSync";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_INIT_WRITE:
			event_name = "PolarFlashbackLogInitWrite";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_INIT_SYNC:
			event_name = "PolarFlashbackLogInitSync";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_WRITE:
			event_name = "PolarFlashbackLogWrite";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_READ:
			event_name = "PolarFlashbackLogRead";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_WRITE:
			event_name = "PolarFlashbackLogHistoryFileWrite";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_READ:
			event_name = "PolarFlashbackLogHistoryFileRead";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_SYNC:
			event_name = "PolarFlashbackLogHistoryFileSync";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_BUF_READY:
			event_name = "PolarFlashbackLogBufferReady";
			break;
		case WAIT_EVENT_FLASHBACK_LOG_INSERT:
			event_name = "PolarFlashbackLogInsert";
			break;
		case WAIT_EVENT_FLASHBACK_POINT_FILE_WRITE:
			event_name = "PolarFlashbackPointFileWrite";
			break;
		case WAIT_EVENT_FLASHBACK_POINT_FILE_READ:
			event_name = "PolarFlashbackPointFileRead";
			break;
		case WAIT_EVENT_FLASHBACK_POINT_FILE_SYNC:
			event_name = "PolarFlashbackPointFileSync";
			break;
		case WAIT_EVENT_FRA_CTL_FILE_READ:
			event_name = "PolarFraCtlFileRead";
			break;
		case WAIT_EVENT_FRA_CTL_FILE_WRITE:
			event_name = "PolarFraCtlFileWrite";
			break;
		case WAIT_EVENT_FRA_CTL_FILE_SYNC:
			event_name = "PolarFraCtlFileSync";
			break;
		/*no cover end*/
		/* POLAR end */
			/* no default case, so that compiler will warn */
	}

	return event_name;
}


/* ----------
 * pgstat_get_backend_current_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.  This looks directly at the BackendStatusArray,
 *	and so will provide current information regardless of the age of our
 *	transaction's snapshot of the status array.
 *
 *	It is the caller's responsibility to invoke this only for backends whose
 *	state is expected to remain stable while the result is in use.  The
 *	only current use is in deadlock reporting, where we can expect that
 *	the target backend is blocked on a lock.  (There are corner cases
 *	where the target's wait could get aborted while we are looking at it,
 *	but the very worst consequence is to return a pointer to a string
 *	that's been changed, so we won't worry too much.)
 *
 *	Note: return strings for special cases match pg_stat_get_backend_activity.
 * ----------
 */
const char *
pgstat_get_backend_current_activity(int pid, bool checkUser)
{
	PgBackendStatus *beentry;
	int			i;

	beentry = BackendStatusArray;
	for (i = 1; i <= MaxBackends; i++)
	{
		/*
		 * Although we expect the target backend's entry to be stable, that
		 * doesn't imply that anyone else's is.  To avoid identifying the
		 * wrong backend, while we check for a match to the desired PID we
		 * must follow the protocol of retrying if st_changecount changes
		 * while we examine the entry, or if it's odd.  (This might be
		 * unnecessary, since fetching or storing an int is almost certainly
		 * atomic, but let's play it safe.)  We use a volatile pointer here to
		 * ensure the compiler doesn't try to get cute.
		 */
		volatile PgBackendStatus *vbeentry = beentry;
		bool		found;

		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_begin_read_activity(vbeentry, before_changecount);

			found = (vbeentry->st_procpid == pid);

			pgstat_end_read_activity(vbeentry, after_changecount);

			if (pgstat_read_activity_complete(before_changecount,
											  after_changecount))
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}

		if (found)
		{
			/* Now it is safe to use the non-volatile pointer */
			if (checkUser && !superuser() &&
				beentry->st_userid != GetUserId() &&
				!(polar_superuser() &&
				  polar_has_more_privs_than_role(GetUserId(), beentry->st_userid)))
				return "<insufficient privilege>";
			else if (*(beentry->st_activity_raw) == '\0')
				return "<command string not enabled>";
			else
			{
				/* this'll leak a bit of memory, but that seems acceptable */
				return pgstat_clip_activity(beentry->st_activity_raw);
			}
		}

		beentry++;
	}

	/* If we get here, caller is in error ... */
	return "<backend information not available>";
}

/* ----------
 * pgstat_get_crashed_backend_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.  Like the function above, but reads shared memory with
 *	the expectation that it may be corrupt.  On success, copy the string
 *	into the "buffer" argument and return that pointer.  On failure,
 *	return NULL.
 *
 *	This function is only intended to be used by the postmaster to report the
 *	query that crashed a backend.  In particular, no attempt is made to
 *	follow the correct concurrency protocol when accessing the
 *	BackendStatusArray.  But that's OK, in the worst case we'll return a
 *	corrupted message.  We also must take care not to trip on ereport(ERROR).
 * ----------
 */
const char *
pgstat_get_crashed_backend_activity(int pid, char *buffer, int buflen)
{
	volatile PgBackendStatus *beentry;
	int			i;

	beentry = BackendStatusArray;

	/*
	 * We probably shouldn't get here before shared memory has been set up,
	 * but be safe.
	 */
	if (beentry == NULL || BackendActivityBuffer == NULL)
		return NULL;

	for (i = 1; i <= MaxBackends; i++)
	{
		if (beentry->st_procpid == pid)
		{
			/* Read pointer just once, so it can't change after validation */
			const char *activity = beentry->st_activity_raw;
			const char *activity_last;

			/*
			 * We mustn't access activity string before we verify that it
			 * falls within the BackendActivityBuffer. To make sure that the
			 * entire string including its ending is contained within the
			 * buffer, subtract one activity length from the buffer size.
			 */
			activity_last = BackendActivityBuffer + BackendActivityBufferSize
				- pgstat_track_activity_query_size;

			if (activity < BackendActivityBuffer ||
				activity > activity_last)
				return NULL;

			/* If no string available, no point in a report */
			if (activity[0] == '\0')
				return NULL;

			/*
			 * Copy only ASCII-safe characters so we don't run into encoding
			 * problems when reporting the message; and be sure not to run off
			 * the end of memory.  As only ASCII characters are reported, it
			 * doesn't seem necessary to perform multibyte aware clipping.
			 */
			ascii_safe_strlcpy(buffer, activity,
							   Min(buflen, pgstat_track_activity_query_size));

			return buffer;
		}

		beentry++;
	}

	/* PID not found */
	return NULL;
}

const char *
pgstat_get_backend_desc(BackendType backendType)
{
	const char *backendDesc = "unknown process type";

	switch (backendType)
	{
		case B_AUTOVAC_LAUNCHER:
			backendDesc = "autovacuum launcher";
			break;
		case B_AUTOVAC_WORKER:
			backendDesc = "autovacuum worker";
			break;
		case B_BACKEND:
			backendDesc = "client backend";
			break;
		case B_BG_WORKER:
			backendDesc = "background worker";
			break;
		case B_BG_WRITER:
			backendDesc = "background writer";
			break;
		case B_CHECKPOINTER:
			backendDesc = "checkpointer";
			break;
		case B_STARTUP:
			backendDesc = "startup";
			break;
		case B_WAL_RECEIVER:
			backendDesc = "walreceiver";
			break;
		case B_WAL_SENDER:
			backendDesc = "walsender";
			break;
		case B_WAL_WRITER:
			backendDesc = "walwriter";
			break;
		case B_CONSENSUS:
			backendDesc = "consensus";
			break;
		case B_POLAR_WAL_PIPELINER:
			backendDesc = "polar wal pipeliner";
			break;
		case B_BG_LOGINDEX:
			backendDesc = "background logindex writer";
			break;
		case B_BG_FLOG_INSERTER:
			backendDesc = "background flashback log inserter";
			break;
		case B_BG_FLOG_WRITER:
			backendDesc = "background flashback log writer";
			break;
		case B_POLAR_DISPATCHER:
			backendDesc = "polar dispatcher";
			break;
	}

	return backendDesc;
}

/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */


/* ----------
 * pgstat_setheader() -
 *
 *		Set common header fields in a statistics message
 * ----------
 */
static void
pgstat_setheader(PgStat_MsgHdr *hdr, StatMsgType mtype)
{
	hdr->m_type = mtype;
}


/* ----------
 * pgstat_send() -
 *
 *		Send out one statistics message to the collector
 * ----------
 */
static void
pgstat_send(void *msg, int len)
{
	int			rc;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	((PgStat_MsgHdr *) msg)->m_size = len;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(pgStatSock, msg, len, 0);
	} while (rc < 0 && errno == EINTR);

#ifdef USE_ASSERT_CHECKING
	/* In debug builds, log send failures ... */
	if (rc < 0)
		elog(LOG, "could not send to statistics collector: %m");
#endif
}

/* ----------
 * pgstat_send_archiver() -
 *
 *	Tell the collector about the WAL file that we successfully
 *	archived or failed to archive.
 * ----------
 */
void
pgstat_send_archiver(const char *xlog, bool failed)
{
	PgStat_MsgArchiver msg;

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_ARCHIVER);
	msg.m_failed = failed;
	StrNCpy(msg.m_xlog, xlog, sizeof(msg.m_xlog));
	msg.m_timestamp = GetCurrentTimestamp();
	pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_bgwriter() -
 *
 *		Send bgwriter statistics to the collector
 * ----------
 */
void
pgstat_send_bgwriter(void)
{
	/* We assume this initializes to zeroes */
	static const PgStat_MsgBgWriter all_zeroes;

	/*
	 * This function can be called even if nothing at all has happened. In
	 * this case, avoid sending a completely empty message to the stats
	 * collector.
	 */
	if (memcmp(&BgWriterStats, &all_zeroes, sizeof(PgStat_MsgBgWriter)) == 0)
		return;

	/*
	 * Prepare and send the message
	 */
	pgstat_setheader(&BgWriterStats.m_hdr, PGSTAT_MTYPE_BGWRITER);
	pgstat_send(&BgWriterStats, sizeof(BgWriterStats));

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&BgWriterStats, 0, sizeof(BgWriterStats));
}


/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.  This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
NON_EXEC_STATIC void
PgstatCollectorMain(int argc, char *argv[])
{
	int			len;
	PgStat_Msg	msg;
	int			wr;

	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGHUP and SIGQUIT.  Note we don't need a SIGUSR1 handler to
	 * support latch operations, because we only use a local latch.
	 */
	pqsignal(SIGHUP, pgstat_sighup_handler);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, pgstat_exit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* POLAR : register for coredump print */
#ifndef _WIN32
#ifdef SIGILL
	pqsignal(SIGILL, polar_program_error_handler);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, polar_program_error_handler);
#endif
#ifdef SIGBUS
	pqsignal(SIGBUS, polar_program_error_handler);
#endif
#endif	/* _WIN32 */
	/* POLAR: end */

	PG_SETMASK(&UnBlockSig);

	/*
	 * Identify myself via ps
	 */
	init_ps_display("stats collector", "", "", "");

	/*
	 * Read in existing stats files or initialize the stats to zero.
	 */
	pgStatRunningInCollector = true;
	pgStatDBHash = pgstat_read_statsfiles(InvalidOid, true, true);

	/*
	 * Loop to process messages until we get SIGQUIT or detect ungraceful
	 * death of our parent postmaster.
	 *
	 * For performance reasons, we don't want to do ResetLatch/WaitLatch after
	 * every message; instead, do that only after a recv() fails to obtain a
	 * message.  (This effectively means that if backends are sending us stuff
	 * like mad, we won't notice postmaster death until things slack off a
	 * bit; which seems fine.)	To do that, we have an inner loop that
	 * iterates as long as recv() succeeds.  We do recognize got_SIGHUP inside
	 * the inner loop, which means that such interrupts will get serviced but
	 * the latch won't get cleared until next time there is a break in the
	 * action.
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Quit if we get SIGQUIT from the postmaster.
		 */
		if (need_exit)
			break;

		/*
		 * Inner loop iterates as long as we keep getting messages, or until
		 * need_exit becomes set.
		 */
		while (!need_exit)
		{
			/*
			 * Reload configuration if we got SIGHUP from the postmaster.
			 */
			if (got_SIGHUP)
			{
				got_SIGHUP = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			/*
			 * Write the stats file(s) if a new request has arrived that is
			 * not satisfied by existing file(s).
			 */
			if (pgstat_write_statsfile_needed())
				pgstat_write_statsfiles(false, false);

			/*
			 * Try to receive and process a message.  This will not block,
			 * since the socket is set to non-blocking mode.
			 *
			 * XXX On Windows, we have to force pgwin32_recv to cooperate,
			 * despite the previous use of pg_set_noblock() on the socket.
			 * This is extremely broken and should be fixed someday.
			 */
#ifdef WIN32
			pgwin32_noblock = 1;
#endif

			len = recv(pgStatSock, (char *) &msg,
					   sizeof(PgStat_Msg), 0);

#ifdef WIN32
			pgwin32_noblock = 0;
#endif

			if (len < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					break;		/* out of inner loop */
				ereport(ERROR,
						(errcode_for_socket_access(),
						 errmsg("could not read statistics message: %m")));
			}

			/*
			 * We ignore messages that are smaller than our common header
			 */
			if (len < sizeof(PgStat_MsgHdr))
				continue;

			/*
			 * The received length must match the length in the header
			 */
			if (msg.msg_hdr.m_size != len)
				continue;

			/*
			 * O.K. - we accept this message.  Process it.
			 */
			switch (msg.msg_hdr.m_type)
			{
				case PGSTAT_MTYPE_DUMMY:
					break;

				case PGSTAT_MTYPE_INQUIRY:
					pgstat_recv_inquiry((PgStat_MsgInquiry *) &msg, len);
					break;

				case PGSTAT_MTYPE_TABSTAT:
					pgstat_recv_tabstat((PgStat_MsgTabstat *) &msg, len);
					break;

				case PGSTAT_MTYPE_TABPURGE:
					pgstat_recv_tabpurge((PgStat_MsgTabpurge *) &msg, len);
					break;

				case PGSTAT_MTYPE_DROPDB:
					pgstat_recv_dropdb((PgStat_MsgDropdb *) &msg, len);
					break;

				case PGSTAT_MTYPE_RESETCOUNTER:
					pgstat_recv_resetcounter((PgStat_MsgResetcounter *) &msg,
											 len);
					break;

				case PGSTAT_MTYPE_RESETSHAREDCOUNTER:
					pgstat_recv_resetsharedcounter(
												   (PgStat_MsgResetsharedcounter *) &msg,
												   len);
					break;

				case PGSTAT_MTYPE_RESETSINGLECOUNTER:
					pgstat_recv_resetsinglecounter(
												   (PgStat_MsgResetsinglecounter *) &msg,
												   len);
					break;

				case PGSTAT_MTYPE_AUTOVAC_START:
					pgstat_recv_autovac((PgStat_MsgAutovacStart *) &msg, len);
					break;

				case PGSTAT_MTYPE_VACUUM:
					pgstat_recv_vacuum((PgStat_MsgVacuum *) &msg, len);
					break;

				case PGSTAT_MTYPE_ANALYZE:
					pgstat_recv_analyze((PgStat_MsgAnalyze *) &msg, len);
					break;

				case PGSTAT_MTYPE_ARCHIVER:
					pgstat_recv_archiver((PgStat_MsgArchiver *) &msg, len);
					break;

				case PGSTAT_MTYPE_BGWRITER:
					pgstat_recv_bgwriter((PgStat_MsgBgWriter *) &msg, len);
					break;

				case PGSTAT_MTYPE_FUNCSTAT:
					pgstat_recv_funcstat((PgStat_MsgFuncstat *) &msg, len);
					break;

				case PGSTAT_MTYPE_FUNCPURGE:
					pgstat_recv_funcpurge((PgStat_MsgFuncpurge *) &msg, len);
					break;

				case PGSTAT_MTYPE_RECOVERYCONFLICT:
					pgstat_recv_recoveryconflict((PgStat_MsgRecoveryConflict *) &msg, len);
					break;

				case PGSTAT_MTYPE_DEADLOCK:
					pgstat_recv_deadlock((PgStat_MsgDeadlock *) &msg, len);
					break;

				case PGSTAT_MTYPE_TEMPFILE:
					pgstat_recv_tempfile((PgStat_MsgTempFile *) &msg, len);
					break;

				case POLAR_PGSTAT_MTYPE_DELAY_DML:
					pgstat_recv_delay_dml((PolarStat_MsgDelayDml *) &msg, len);
					break;

				default:
					break;
			}
		}						/* end of inner message-processing loop */

		/* Sleep until there's something to do */
#ifndef WIN32
		wr = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE,
							   pgStatSock, -1L,
							   WAIT_EVENT_PGSTAT_MAIN);
#else

		/*
		 * Windows, at least in its Windows Server 2003 R2 incarnation,
		 * sometimes loses FD_READ events.  Waking up and retrying the recv()
		 * fixes that, so don't sleep indefinitely.  This is a crock of the
		 * first water, but until somebody wants to debug exactly what's
		 * happening there, this is the best we can do.  The two-second
		 * timeout matches our pre-9.2 behavior, and needs to be short enough
		 * to not provoke "using stale statistics" complaints from
		 * backend_read_statsfile.
		 */
		wr = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT,
							   pgStatSock,
							   2 * 1000L /* msec */ ,
							   WAIT_EVENT_PGSTAT_MAIN);
#endif

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (wr & WL_POSTMASTER_DEATH)
			break;
	}							/* end of outer loop */

	/*
	 * Save the final stats to reuse at next startup.
	 */
	pgstat_write_statsfiles(true, true);

	exit(0);
}


/* SIGQUIT signal handler for collector process */
static void
pgstat_exit(SIGNAL_ARGS)
{
	int			save_errno = errno;

	need_exit = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGHUP handler for collector process */
static void
pgstat_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Subroutine to clear stats in a database entry
 *
 * Tables and functions hashes are initialized to empty.
 */
static void
reset_dbentry_counters(PgStat_StatDBEntry *dbentry)
{
	HASHCTL		hash_ctl;

	dbentry->n_xact_commit = 0;
	dbentry->n_xact_rollback = 0;
	dbentry->n_blocks_fetched = 0;
	dbentry->n_blocks_hit = 0;
	dbentry->n_tuples_returned = 0;
	dbentry->n_tuples_fetched = 0;
	dbentry->n_tuples_inserted = 0;
	dbentry->n_tuples_updated = 0;
	dbentry->n_tuples_deleted = 0;
	dbentry->last_autovac_time = 0;
	dbentry->n_conflict_tablespace = 0;
	dbentry->n_conflict_lock = 0;
	dbentry->n_conflict_snapshot = 0;
	dbentry->n_conflict_bufferpin = 0;
	dbentry->n_conflict_startup_deadlock = 0;
	dbentry->n_temp_files = 0;
	dbentry->n_temp_bytes = 0;
	dbentry->n_deadlocks = 0;
	dbentry->n_block_read_time = 0;
	dbentry->n_block_write_time = 0;

	/* POLAR: bulk read stats */
	dbentry->polar_n_bulk_read_calls = 0;
	dbentry->polar_n_bulk_read_calls_IO = 0;
	dbentry->polar_n_bulk_read_blocks_IO = 0;
	dbentry->polar_delay_dml_count = 0;
	/* POLAR end */

	dbentry->stat_reset_timestamp = GetCurrentTimestamp();
	dbentry->stats_timestamp = 0;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
	dbentry->tables = hash_create("Per-database table",
								  PGSTAT_TAB_HASH_SIZE,
								  &hash_ctl,
								  HASH_ELEM | HASH_BLOBS);

	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
	dbentry->functions = hash_create("Per-database function",
									 PGSTAT_FUNCTION_HASH_SIZE,
									 &hash_ctl,
									 HASH_ELEM | HASH_BLOBS);
}

/*
 * Lookup the hash table entry for the specified database. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatDBEntry *
pgstat_get_db_entry(Oid databaseid, bool create)
{
	PgStat_StatDBEntry *result;
	bool		found;
	HASHACTION	action = (create ? HASH_ENTER : HASH_FIND);

	/* Lookup or create the hash table entry for this database */
	result = (PgStat_StatDBEntry *) hash_search(pgStatDBHash,
												&databaseid,
												action, &found);

	if (!create && !found)
		return NULL;

	/*
	 * If not found, initialize the new one.  This creates empty hash tables
	 * for tables and functions, too.
	 */
	if (!found)
		reset_dbentry_counters(result);

	return result;
}


/*
 * Lookup the hash table entry for the specified table. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatTabEntry *
pgstat_get_tab_entry(PgStat_StatDBEntry *dbentry, Oid tableoid, bool create)
{
	PgStat_StatTabEntry *result;
	bool		found;
	HASHACTION	action = (create ? HASH_ENTER : HASH_FIND);

	/* Lookup or create the hash table entry for this table */
	result = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
												 &tableoid,
												 action, &found);

	if (!create && !found)
		return NULL;

	/* If not found, initialize the new one. */
	if (!found)
	{
		result->numscans = 0;
		result->tuples_returned = 0;
		result->tuples_fetched = 0;
		result->tuples_inserted = 0;
		result->tuples_updated = 0;
		result->tuples_deleted = 0;
		result->tuples_hot_updated = 0;
		result->n_live_tuples = 0;
		result->n_dead_tuples = 0;
		result->changes_since_analyze = 0;
		result->blocks_fetched = 0;
		result->blocks_hit = 0;
		result->vacuum_timestamp = 0;
		result->vacuum_count = 0;
		result->autovac_vacuum_timestamp = 0;
		result->autovac_vacuum_count = 0;
		result->analyze_timestamp = 0;
		result->analyze_count = 0;
		result->autovac_analyze_timestamp = 0;
		result->autovac_analyze_count = 0;

		/* POLAR: bulk read stats */
		result->polar_bulk_read_calls = 0;
		result->polar_bulk_read_calls_IO = 0;
		result->polar_bulk_read_blocks_IO = 0;
		/* POLAR end */
	}

	return result;
}


/* ----------
 * pgstat_write_statsfiles() -
 *		Write the global statistics file, as well as requested DB files.
 *
 *	'permanent' specifies writing to the permanent files not temporary ones.
 *	When true (happens only when the collector is shutting down), also remove
 *	the temporary files so that backends starting up under a new postmaster
 *	can't read old data before the new collector is ready.
 *
 *	When 'allDbs' is false, only the requested databases (listed in
 *	pending_write_requests) will be written; otherwise, all databases
 *	will be written.
 * ----------
 */
static void
pgstat_write_statsfiles(bool permanent, bool allDbs)
{
	HASH_SEQ_STATUS hstat;
	PgStat_StatDBEntry *dbentry;
	FILE	   *fpout;
	int32		format_id;
	const char *tmpfile = permanent ? PGSTAT_STAT_PERMANENT_TMPFILE : pgstat_stat_tmpname;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;
	int			rc;

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Set the timestamp of the stats file.
	 */
	globalStats.stats_timestamp = GetCurrentTimestamp();

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write global stats struct
	 */
	rc = fwrite(&globalStats, sizeof(globalStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Write archiver stats struct
	 */
	rc = fwrite(&archiverStats, sizeof(archiverStats), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database table.
	 */
	hash_seq_init(&hstat, pgStatDBHash);
	while ((dbentry = (PgStat_StatDBEntry *) hash_seq_search(&hstat)) != NULL)
	{
		/*
		 * Write out the table and function stats for this DB into the
		 * appropriate per-DB stat file, if required.
		 */
		if (allDbs || pgstat_db_requested(dbentry->databaseid))
		{
			/* Make DB's timestamp consistent with the global stats */
			dbentry->stats_timestamp = globalStats.stats_timestamp;

			pgstat_write_db_statsfile(dbentry, permanent);
		}

		/*
		 * Write out the DB entry. We don't write the tables or functions
		 * pointers, since they're of no use to any other process.
		 */
		fputc('D', fpout);
		rc = fwrite(dbentry, offsetof(PgStat_StatDBEntry, tables), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}

	if (permanent)
		unlink(pgstat_stat_filename);

	/*
	 * Now throw away the list of requests.  Note that requests sent after we
	 * started the write are still waiting on the network socket.
	 */
	list_free(pending_write_requests);
	pending_write_requests = NIL;
}

/*
 * return the filename for a DB stat file; filename is the output buffer,
 * of length len.
 */
static void
get_dbstat_filename(bool permanent, bool tempname, Oid databaseid,
					char *filename, int len)
{
	int			printed;

	/* NB -- pgstat_reset_remove_files knows about the pattern this uses */
	printed = snprintf(filename, len, "%s/db_%u.%s",
					   permanent ? PGSTAT_STAT_PERMANENT_DIRECTORY :
					   pgstat_stat_directory,
					   databaseid,
					   tempname ? "tmp" : "stat");
	if (printed >= len)
		elog(ERROR, "overlength pgstat path");
}

/* ----------
 * pgstat_write_db_statsfile() -
 *		Write the stat file for a single database.
 *
 *	If writing to the permanent file (happens when the collector is
 *	shutting down only), remove the temporary file so that backends
 *	starting up under a new postmaster can't read the old data before
 *	the new collector is ready.
 * ----------
 */
static void
pgstat_write_db_statsfile(PgStat_StatDBEntry *dbentry, bool permanent)
{
	HASH_SEQ_STATUS tstat;
	HASH_SEQ_STATUS fstat;
	PgStat_StatTabEntry *tabentry;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpout;
	int32		format_id;
	Oid			dbid = dbentry->databaseid;
	int			rc;
	char		tmpfile[MAXPGPATH];
	char		statfile[MAXPGPATH];

	get_dbstat_filename(permanent, true, dbid, tmpfile, MAXPGPATH);
	get_dbstat_filename(permanent, false, dbid, statfile, MAXPGPATH);

	elog(DEBUG2, "writing stats file \"%s\"", statfile);

	/*
	 * Open the statistics temp file to write out the current values.
	 */
	fpout = AllocateFile(tmpfile, PG_BINARY_W);
	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open temporary statistics file \"%s\": %m",
						tmpfile)));
		return;
	}

	/*
	 * Write the file header --- currently just a format ID.
	 */
	format_id = PGSTAT_FILE_FORMAT_ID;
	rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
	(void) rc;					/* we'll check for error with ferror */

	/*
	 * Walk through the database's access stats per table.
	 */
	hash_seq_init(&tstat, dbentry->tables);
	while ((tabentry = (PgStat_StatTabEntry *) hash_seq_search(&tstat)) != NULL)
	{
		fputc('T', fpout);
		rc = fwrite(tabentry, sizeof(PgStat_StatTabEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * Walk through the database's function stats table.
	 */
	hash_seq_init(&fstat, dbentry->functions);
	while ((funcentry = (PgStat_StatFuncEntry *) hash_seq_search(&fstat)) != NULL)
	{
		fputc('F', fpout);
		rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
		(void) rc;				/* we'll check for error with ferror */
	}

	/*
	 * No more output to be done. Close the temp file and replace the old
	 * pgstat.stat with it.  The ferror() check replaces testing for error
	 * after each individual fputc or fwrite above.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary statistics file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary statistics file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, statfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m",
						tmpfile, statfile)));
		unlink(tmpfile);
	}

	if (permanent)
	{
		get_dbstat_filename(false, false, dbid, statfile, MAXPGPATH);

		elog(DEBUG2, "removing temporary stats file \"%s\"", statfile);
		unlink(statfile);
	}
}

/* ----------
 * pgstat_read_statsfiles() -
 *
 *	Reads in some existing statistics collector files and returns the
 *	databases hash table that is the top level of the data.
 *
 *	If 'onlydb' is not InvalidOid, it means we only want data for that DB
 *	plus the shared catalogs ("DB 0").  We'll still populate the DB hash
 *	table for all databases, but we don't bother even creating table/function
 *	hash tables for other databases.
 *
 *	'permanent' specifies reading from the permanent files not temporary ones.
 *	When true (happens only when the collector is starting up), remove the
 *	files after reading; the in-memory status is now authoritative, and the
 *	files would be out of date in case somebody else reads them.
 *
 *	If a 'deep' read is requested, table/function stats are read, otherwise
 *	the table/function hash tables remain empty.
 * ----------
 */
static HTAB *
pgstat_read_statsfiles(Oid onlydb, bool permanent, bool deep)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatDBEntry dbbuf;
	HASHCTL		hash_ctl;
	HTAB	   *dbhash;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;

	/*
	 * The tables will live in pgStatLocalContext.
	 */
	pgstat_setup_memcxt();

	/*
	 * Create the DB hashtable
	 */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(PgStat_StatDBEntry);
	hash_ctl.hcxt = pgStatLocalContext;
	dbhash = hash_create("Databases hash", PGSTAT_DB_HASH_SIZE, &hash_ctl,
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Clear out global and archiver statistics so they start from zero in
	 * case we can't load an existing statsfile.
	 */
	memset(&globalStats, 0, sizeof(globalStats));
	memset(&archiverStats, 0, sizeof(archiverStats));

	/*
	 * Set the current timestamp (will be kept only in case we can't load an
	 * existing statsfile).
	 */
	globalStats.stat_reset_timestamp = GetCurrentTimestamp();
	archiverStats.stat_reset_timestamp = globalStats.stat_reset_timestamp;

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return dbhash;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(&globalStats, 1, sizeof(globalStats), fpin) != sizeof(globalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&globalStats, 0, sizeof(globalStats));
		goto done;
	}

	/*
	 * In the collector, disregard the timestamp we read from the permanent
	 * stats file; we should be willing to write a temp stats file immediately
	 * upon the first request from any backend.  This only matters if the old
	 * file's timestamp is less than PGSTAT_STAT_INTERVAL ago, but that's not
	 * an unusual scenario.
	 */
	if (pgStatRunningInCollector)
		globalStats.stats_timestamp = 0;

	/*
	 * Read archiver stats struct
	 */
	if (fread(&archiverStats, 1, sizeof(archiverStats), fpin) != sizeof(archiverStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		memset(&archiverStats, 0, sizeof(archiverStats));
		goto done;
	}

	/*
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'D'	A PgStat_StatDBEntry struct describing a database
				 * follows.
				 */
			case 'D':
				if (fread(&dbbuf, 1, offsetof(PgStat_StatDBEntry, tables),
						  fpin) != offsetof(PgStat_StatDBEntry, tables))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Add to the DB hash
				 */
				dbentry = (PgStat_StatDBEntry *) hash_search(dbhash,
															 (void *) &dbbuf.databaseid,
															 HASH_ENTER,
															 &found);
				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(dbentry, &dbbuf, sizeof(PgStat_StatDBEntry));
				dbentry->tables = NULL;
				dbentry->functions = NULL;

				/*
				 * In the collector, disregard the timestamp we read from the
				 * permanent stats file; we should be willing to write a temp
				 * stats file immediately upon the first request from any
				 * backend.
				 */
				if (pgStatRunningInCollector)
					dbentry->stats_timestamp = 0;

				/*
				 * Don't create tables/functions hashtables for uninteresting
				 * databases.
				 */
				if (onlydb != InvalidOid)
				{
					if (dbbuf.databaseid != onlydb &&
						dbbuf.databaseid != InvalidOid)
						break;
				}

				memset(&hash_ctl, 0, sizeof(hash_ctl));
				hash_ctl.keysize = sizeof(Oid);
				hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
				hash_ctl.hcxt = pgStatLocalContext;
				dbentry->tables = hash_create("Per-database table",
											  PGSTAT_TAB_HASH_SIZE,
											  &hash_ctl,
											  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

				hash_ctl.keysize = sizeof(Oid);
				hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
				hash_ctl.hcxt = pgStatLocalContext;
				dbentry->functions = hash_create("Per-database function",
												 PGSTAT_FUNCTION_HASH_SIZE,
												 &hash_ctl,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

				/*
				 * If requested, read the data from the database-specific
				 * file.  Otherwise we just leave the hashtables empty.
				 */
				if (deep)
					pgstat_read_db_statsfile(dbentry->databaseid,
											 dbentry->tables,
											 dbentry->functions,
											 permanent);

				break;

			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	/* If requested to read the permanent file, also get rid of it. */
	if (permanent)
	{
		elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
		unlink(statfile);
	}

	return dbhash;
}


/* ----------
 * pgstat_read_db_statsfile() -
 *
 *	Reads in the existing statistics collector file for the given database,
 *	filling the passed-in tables and functions hash tables.
 *
 *	As in pgstat_read_statsfiles, if the permanent file is requested, it is
 *	removed after reading.
 *
 *	Note: this code has the ability to skip storing per-table or per-function
 *	data, if NULL is passed for the corresponding hashtable.  That's not used
 *	at the moment though.
 * ----------
 */
static void
pgstat_read_db_statsfile(Oid databaseid, HTAB *tabhash, HTAB *funchash,
						 bool permanent)
{
	PgStat_StatTabEntry *tabentry;
	PgStat_StatTabEntry tabbuf;
	PgStat_StatFuncEntry funcbuf;
	PgStat_StatFuncEntry *funcentry;
	FILE	   *fpin;
	int32		format_id;
	bool		found;
	char		statfile[MAXPGPATH];

	get_dbstat_filename(permanent, false, databaseid, statfile, MAXPGPATH);

	/*
	 * Try to open the stats file. If it doesn't exist, the backends simply
	 * return zero for anything and the collector simply starts from scratch
	 * with empty counters.
	 *
	 * ENOENT is a possibility if the stats collector is not running or has
	 * not yet written the stats file the first time.  Any other failure
	 * condition is suspicious.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		goto done;
	}

	/*
	 * We found an existing collector stats file. Read it and put all the
	 * hashtable entries into place.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'T'	A PgStat_StatTabEntry follows.
				 */
			case 'T':
				if (fread(&tabbuf, 1, sizeof(PgStat_StatTabEntry),
						  fpin) != sizeof(PgStat_StatTabEntry))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Skip if table data not wanted.
				 */
				if (tabhash == NULL)
					break;

				tabentry = (PgStat_StatTabEntry *) hash_search(tabhash,
															   (void *) &tabbuf.tableid,
															   HASH_ENTER, &found);

				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(tabentry, &tabbuf, sizeof(tabbuf));
				break;

				/*
				 * 'F'	A PgStat_StatFuncEntry follows.
				 */
			case 'F':
				if (fread(&funcbuf, 1, sizeof(PgStat_StatFuncEntry),
						  fpin) != sizeof(PgStat_StatFuncEntry))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * Skip if function data not wanted.
				 */
				if (funchash == NULL)
					break;

				funcentry = (PgStat_StatFuncEntry *) hash_search(funchash,
																 (void *) &funcbuf.functionid,
																 HASH_ENTER, &found);

				if (found)
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				memcpy(funcentry, &funcbuf, sizeof(funcbuf));
				break;

				/*
				 * 'E'	The EOF marker of a complete stats file.
				 */
			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);

	if (permanent)
	{
		elog(DEBUG2, "removing permanent stats file \"%s\"", statfile);
		unlink(statfile);
	}
}

/* ----------
 * pgstat_read_db_statsfile_timestamp() -
 *
 *	Attempt to determine the timestamp of the last db statfile write.
 *	Returns true if successful; the timestamp is stored in *ts.
 *
 *	This needs to be careful about handling databases for which no stats file
 *	exists, such as databases without a stat entry or those not yet written:
 *
 *	- if there's a database entry in the global file, return the corresponding
 *	stats_timestamp value.
 *
 *	- if there's no db stat entry (e.g. for a new or inactive database),
 *	there's no stats_timestamp value, but also nothing to write so we return
 *	the timestamp of the global statfile.
 * ----------
 */
static bool
pgstat_read_db_statsfile_timestamp(Oid databaseid, bool permanent,
								   TimestampTz *ts)
{
	PgStat_StatDBEntry dbentry;
	PgStat_GlobalStats myGlobalStats;
	PgStat_ArchiverStats myArchiverStats;
	FILE	   *fpin;
	int32		format_id;
	const char *statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : pgstat_stat_filename;

	/*
	 * Try to open the stats file.  As above, anything but ENOENT is worthy of
	 * complaining about.
	 */
	if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL)
	{
		if (errno != ENOENT)
			ereport(pgStatRunningInCollector ? LOG : WARNING,
					(errcode_for_file_access(),
					 errmsg("could not open statistics file \"%s\": %m",
							statfile)));
		return false;
	}

	/*
	 * Verify it's of the expected format.
	 */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PGSTAT_FILE_FORMAT_ID)
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read global stats struct
	 */
	if (fread(&myGlobalStats, 1, sizeof(myGlobalStats),
			  fpin) != sizeof(myGlobalStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/*
	 * Read archiver stats struct
	 */
	if (fread(&myArchiverStats, 1, sizeof(myArchiverStats),
			  fpin) != sizeof(myArchiverStats))
	{
		ereport(pgStatRunningInCollector ? LOG : WARNING,
				(errmsg("corrupted statistics file \"%s\"", statfile)));
		FreeFile(fpin);
		return false;
	}

	/* By default, we're going to return the timestamp of the global file. */
	*ts = myGlobalStats.stats_timestamp;

	/*
	 * We found an existing collector stats file.  Read it and look for a
	 * record for the requested database.  If found, use its timestamp.
	 */
	for (;;)
	{
		switch (fgetc(fpin))
		{
				/*
				 * 'D'	A PgStat_StatDBEntry struct describing a database
				 * follows.
				 */
			case 'D':
				if (fread(&dbentry, 1, offsetof(PgStat_StatDBEntry, tables),
						  fpin) != offsetof(PgStat_StatDBEntry, tables))
				{
					ereport(pgStatRunningInCollector ? LOG : WARNING,
							(errmsg("corrupted statistics file \"%s\"",
									statfile)));
					goto done;
				}

				/*
				 * If this is the DB we're looking for, save its timestamp and
				 * we're done.
				 */
				if (dbentry.databaseid == databaseid)
				{
					*ts = dbentry.stats_timestamp;
					goto done;
				}

				break;

			case 'E':
				goto done;

			default:
				ereport(pgStatRunningInCollector ? LOG : WARNING,
						(errmsg("corrupted statistics file \"%s\"",
								statfile)));
				goto done;
		}
	}

done:
	FreeFile(fpin);
	return true;
}

/*
 * If not already done, read the statistics collector stats file into
 * some hash tables.  The results will be kept until pgstat_clear_snapshot()
 * is called (typically, at end of transaction).
 */
static void
backend_read_statsfile(void)
{
	TimestampTz min_ts = 0;
	TimestampTz ref_ts = 0;
	Oid			inquiry_db;
	int			count;

	/* already read it? */
	if (pgStatDBHash)
		return;
	Assert(!pgStatRunningInCollector);

	/*
	 * In a normal backend, we check staleness of the data for our own DB, and
	 * so we send MyDatabaseId in inquiry messages.  In the autovac launcher,
	 * check staleness of the shared-catalog data, and send InvalidOid in
	 * inquiry messages so as not to force writing unnecessary data.
	 */
	if (IsAutoVacuumLauncherProcess())
		inquiry_db = InvalidOid;
	else
		inquiry_db = MyDatabaseId;

	/*
	 * Loop until fresh enough stats file is available or we ran out of time.
	 * The stats inquiry message is sent repeatedly in case collector drops
	 * it; but not every single time, as that just swamps the collector.
	 */
	for (count = 0; count < PGSTAT_POLL_LOOP_COUNT; count++)
	{
		bool		ok;
		TimestampTz file_ts = 0;
		TimestampTz cur_ts;

		CHECK_FOR_INTERRUPTS();

		ok = pgstat_read_db_statsfile_timestamp(inquiry_db, false, &file_ts);

		cur_ts = GetCurrentTimestamp();
		/* Calculate min acceptable timestamp, if we didn't already */
		if (count == 0 || cur_ts < ref_ts)
		{
			/*
			 * We set the minimum acceptable timestamp to PGSTAT_STAT_INTERVAL
			 * msec before now.  This indirectly ensures that the collector
			 * needn't write the file more often than PGSTAT_STAT_INTERVAL. In
			 * an autovacuum worker, however, we want a lower delay to avoid
			 * using stale data, so we use PGSTAT_RETRY_DELAY (since the
			 * number of workers is low, this shouldn't be a problem).
			 *
			 * We don't recompute min_ts after sleeping, except in the
			 * unlikely case that cur_ts went backwards.  So we might end up
			 * accepting a file a bit older than PGSTAT_STAT_INTERVAL.  In
			 * practice that shouldn't happen, though, as long as the sleep
			 * time is less than PGSTAT_STAT_INTERVAL; and we don't want to
			 * tell the collector that our cutoff time is less than what we'd
			 * actually accept.
			 */
			ref_ts = cur_ts;
			if (IsAutoVacuumWorkerProcess())
				min_ts = TimestampTzPlusMilliseconds(ref_ts,
													 -PGSTAT_RETRY_DELAY);
			else
				min_ts = TimestampTzPlusMilliseconds(ref_ts,
													 -PGSTAT_STAT_INTERVAL);
		}

		/*
		 * If the file timestamp is actually newer than cur_ts, we must have
		 * had a clock glitch (system time went backwards) or there is clock
		 * skew between our processor and the stats collector's processor.
		 * Accept the file, but send an inquiry message anyway to make
		 * pgstat_recv_inquiry do a sanity check on the collector's time.
		 */
		if (ok && file_ts > cur_ts)
		{
			/*
			 * A small amount of clock skew between processors isn't terribly
			 * surprising, but a large difference is worth logging.  We
			 * arbitrarily define "large" as 1000 msec.
			 */
			if (file_ts >= TimestampTzPlusMilliseconds(cur_ts, 1000))
			{
				char	   *filetime;
				char	   *mytime;

				/* Copy because timestamptz_to_str returns a static buffer */
				filetime = pstrdup(timestamptz_to_str(file_ts));
				mytime = pstrdup(timestamptz_to_str(cur_ts));
				elog(LOG, "stats collector's time %s is later than backend local time %s",
					 filetime, mytime);
				pfree(filetime);
				pfree(mytime);
			}

			pgstat_send_inquiry(cur_ts, min_ts, inquiry_db);
			break;
		}

		/* Normal acceptance case: file is not older than cutoff time */
		if (ok && file_ts >= min_ts)
			break;

		/* Not there or too old, so kick the collector and wait a bit */
		if ((count % PGSTAT_INQ_LOOP_COUNT) == 0)
			pgstat_send_inquiry(cur_ts, min_ts, inquiry_db);

		pg_usleep(PGSTAT_RETRY_DELAY * 1000L);
	}

	if (count >= PGSTAT_POLL_LOOP_COUNT)
		ereport(LOG,
				(errmsg("using stale statistics instead of current ones "
						"because stats collector is not responding")));

	/*
	 * Autovacuum launcher wants stats about all databases, but a shallow read
	 * is sufficient.  Regular backends want a deep read for just the tables
	 * they can see (MyDatabaseId + shared catalogs).
	 */
	if (IsAutoVacuumLauncherProcess())
		pgStatDBHash = pgstat_read_statsfiles(InvalidOid, false, false);
	else
		pgStatDBHash = pgstat_read_statsfiles(MyDatabaseId, false, true);
}


/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create pgStatLocalContext, if not already done.
 * ----------
 */
static void
pgstat_setup_memcxt(void)
{
	if (!pgStatLocalContext)
		pgStatLocalContext = AllocSetContextCreate(TopMemoryContext,
												   "Statistics snapshot",
												   ALLOCSET_SMALL_SIZES);
}


/* ----------
 * pgstat_clear_snapshot() -
 *
 *	Discard any data collected in the current transaction.  Any subsequent
 *	request will cause new snapshots to be read.
 *
 *	This is also invoked during transaction commit or abort to discard
 *	the no-longer-wanted snapshot.
 * ----------
 */
void
pgstat_clear_snapshot(void)
{
	/* Release memory, if any was allocated */
	if (pgStatLocalContext)
		MemoryContextDelete(pgStatLocalContext);

	/* Reset variables */
	pgStatLocalContext = NULL;
	pgStatDBHash = NULL;
	localBackendStatusTable = NULL;
	localNumBackends = 0;
}


/* ----------
 * pgstat_recv_inquiry() -
 *
 *	Process stat inquiry requests.
 * ----------
 */
static void
pgstat_recv_inquiry(PgStat_MsgInquiry *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	elog(DEBUG2, "received inquiry for database %u", msg->databaseid);

	/*
	 * If there's already a write request for this DB, there's nothing to do.
	 *
	 * Note that if a request is found, we return early and skip the below
	 * check for clock skew.  This is okay, since the only way for a DB
	 * request to be present in the list is that we have been here since the
	 * last write round.  It seems sufficient to check for clock skew once per
	 * write round.
	 */
	if (list_member_oid(pending_write_requests, msg->databaseid))
		return;

	/*
	 * Check to see if we last wrote this database at a time >= the requested
	 * cutoff time.  If so, this is a stale request that was generated before
	 * we updated the DB file, and we don't need to do so again.
	 *
	 * If the requestor's local clock time is older than stats_timestamp, we
	 * should suspect a clock glitch, ie system time going backwards; though
	 * the more likely explanation is just delayed message receipt.  It is
	 * worth expending a GetCurrentTimestamp call to be sure, since a large
	 * retreat in the system clock reading could otherwise cause us to neglect
	 * to update the stats file for a long time.
	 */
	dbentry = pgstat_get_db_entry(msg->databaseid, false);
	if (dbentry == NULL)
	{
		/*
		 * We have no data for this DB.  Enter a write request anyway so that
		 * the global stats will get updated.  This is needed to prevent
		 * backend_read_statsfile from waiting for data that we cannot supply,
		 * in the case of a new DB that nobody has yet reported any stats for.
		 * See the behavior of pgstat_read_db_statsfile_timestamp.
		 */
	}
	else if (msg->clock_time < dbentry->stats_timestamp)
	{
		TimestampTz cur_ts = GetCurrentTimestamp();

		if (cur_ts < dbentry->stats_timestamp)
		{
			/*
			 * Sure enough, time went backwards.  Force a new stats file write
			 * to get back in sync; but first, log a complaint.
			 */
			char	   *writetime;
			char	   *mytime;

			/* Copy because timestamptz_to_str returns a static buffer */
			writetime = pstrdup(timestamptz_to_str(dbentry->stats_timestamp));
			mytime = pstrdup(timestamptz_to_str(cur_ts));
			elog(LOG,
				 "stats_timestamp %s is later than collector's time %s for database %u",
				 writetime, mytime, dbentry->databaseid);
			pfree(writetime);
			pfree(mytime);
		}
		else
		{
			/*
			 * Nope, it's just an old request.  Assuming msg's clock_time is
			 * >= its cutoff_time, it must be stale, so we can ignore it.
			 */
			return;
		}
	}
	else if (msg->cutoff_time <= dbentry->stats_timestamp)
	{
		/* Stale request, ignore it */
		return;
	}

	/*
	 * We need to write this DB, so create a request.
	 */
	pending_write_requests = lappend_oid(pending_write_requests,
										 msg->databaseid);
}


/* ----------
 * pgstat_recv_tabstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void
pgstat_recv_tabstat(PgStat_MsgTabstat *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;
	int			i;
	bool		found;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	/*
	 * Update database-wide stats.
	 */
	dbentry->n_xact_commit += (PgStat_Counter) (msg->m_xact_commit);
	dbentry->n_xact_rollback += (PgStat_Counter) (msg->m_xact_rollback);
	dbentry->n_block_read_time += msg->m_block_read_time;
	dbentry->n_block_write_time += msg->m_block_write_time;

	/*
	 * Process all table entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		PgStat_TableEntry *tabmsg = &(msg->m_entry[i]);

		tabentry = (PgStat_StatTabEntry *) hash_search(dbentry->tables,
													   (void *) &(tabmsg->t_id),
													   HASH_ENTER, &found);

		if (!found)
		{
			/*
			 * If it's a new table entry, initialize counters to the values we
			 * just got.
			 */
			tabentry->numscans = tabmsg->t_counts.t_numscans;
			tabentry->tuples_returned = tabmsg->t_counts.t_tuples_returned;
			tabentry->tuples_fetched = tabmsg->t_counts.t_tuples_fetched;
			tabentry->tuples_inserted = tabmsg->t_counts.t_tuples_inserted;
			tabentry->tuples_updated = tabmsg->t_counts.t_tuples_updated;
			tabentry->tuples_deleted = tabmsg->t_counts.t_tuples_deleted;
			tabentry->tuples_hot_updated = tabmsg->t_counts.t_tuples_hot_updated;
			tabentry->n_live_tuples = tabmsg->t_counts.t_delta_live_tuples;
			tabentry->n_dead_tuples = tabmsg->t_counts.t_delta_dead_tuples;
			tabentry->changes_since_analyze = tabmsg->t_counts.t_changed_tuples;
			tabentry->blocks_fetched = tabmsg->t_counts.t_blocks_fetched;
			tabentry->blocks_hit = tabmsg->t_counts.t_blocks_hit;

			/* POLAR: bulk read stats */
			tabentry->polar_bulk_read_calls = tabmsg->t_counts.polar_t_bulk_read_calls;
			tabentry->polar_bulk_read_calls_IO = tabmsg->t_counts.polar_t_bulk_read_calls_IO;
			tabentry->polar_bulk_read_blocks_IO = tabmsg->t_counts.polar_t_bulk_read_blocks_IO;
			/* POLAR end */

			tabentry->vacuum_timestamp = 0;
			tabentry->vacuum_count = 0;
			tabentry->autovac_vacuum_timestamp = 0;
			tabentry->autovac_vacuum_count = 0;
			tabentry->analyze_timestamp = 0;
			tabentry->analyze_count = 0;
			tabentry->autovac_analyze_timestamp = 0;
			tabentry->autovac_analyze_count = 0;
		}
		else
		{
			/*
			 * Otherwise add the values to the existing entry.
			 */
			tabentry->numscans += tabmsg->t_counts.t_numscans;
			tabentry->tuples_returned += tabmsg->t_counts.t_tuples_returned;
			tabentry->tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
			tabentry->tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
			tabentry->tuples_updated += tabmsg->t_counts.t_tuples_updated;
			tabentry->tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
			tabentry->tuples_hot_updated += tabmsg->t_counts.t_tuples_hot_updated;
			/* If table was truncated, first reset the live/dead counters */
			if (tabmsg->t_counts.t_truncated)
			{
				tabentry->n_live_tuples = 0;
				tabentry->n_dead_tuples = 0;
			}
			tabentry->n_live_tuples += tabmsg->t_counts.t_delta_live_tuples;
			tabentry->n_dead_tuples += tabmsg->t_counts.t_delta_dead_tuples;
			tabentry->changes_since_analyze += tabmsg->t_counts.t_changed_tuples;
			tabentry->blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
			tabentry->blocks_hit += tabmsg->t_counts.t_blocks_hit;

			/* POLAR: bulk read stats */
			tabentry->polar_bulk_read_calls += tabmsg->t_counts.polar_t_bulk_read_calls;
			tabentry->polar_bulk_read_calls_IO += tabmsg->t_counts.polar_t_bulk_read_calls_IO;
			tabentry->polar_bulk_read_blocks_IO += tabmsg->t_counts.polar_t_bulk_read_blocks_IO;
			/* POLAR end */
		}

		/* Clamp n_live_tuples in case of negative delta_live_tuples */
		tabentry->n_live_tuples = Max(tabentry->n_live_tuples, 0);
		/* Likewise for n_dead_tuples */
		tabentry->n_dead_tuples = Max(tabentry->n_dead_tuples, 0);

		/*
		 * Add per-table stats to the per-database entry, too.
		 */
		dbentry->n_tuples_returned += tabmsg->t_counts.t_tuples_returned;
		dbentry->n_tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
		dbentry->n_tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
		dbentry->n_tuples_updated += tabmsg->t_counts.t_tuples_updated;
		dbentry->n_tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
		dbentry->n_blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
		dbentry->n_blocks_hit += tabmsg->t_counts.t_blocks_hit;

		/* POLAR: bulk read stats */
		dbentry->polar_n_bulk_read_calls += tabmsg->t_counts.polar_t_bulk_read_calls;
		dbentry->polar_n_bulk_read_calls_IO += tabmsg->t_counts.polar_t_bulk_read_calls_IO;
		dbentry->polar_n_bulk_read_blocks_IO += tabmsg->t_counts.polar_t_bulk_read_blocks_IO;
		/* POLAR end */
	}
}


/* ----------
 * pgstat_recv_tabpurge() -
 *
 *	Arrange for dead table removal.
 * ----------
 */
static void
pgstat_recv_tabpurge(PgStat_MsgTabpurge *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	int			i;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || !dbentry->tables)
		return;

	/*
	 * Process all table entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		/* Remove from hashtable if present; we don't care if it's not. */
		(void) hash_search(dbentry->tables,
						   (void *) &(msg->m_tableid[i]),
						   HASH_REMOVE, NULL);
	}
}


/* ----------
 * pgstat_recv_dropdb() -
 *
 *	Arrange for dead database removal
 * ----------
 */
static void
pgstat_recv_dropdb(PgStat_MsgDropdb *msg, int len)
{
	Oid			dbid = msg->m_databaseid;
	PgStat_StatDBEntry *dbentry;

	/*
	 * Lookup the database in the hashtable.
	 */
	dbentry = pgstat_get_db_entry(dbid, false);

	/*
	 * If found, remove it (along with the db statfile).
	 */
	if (dbentry)
	{
		char		statfile[MAXPGPATH];

		get_dbstat_filename(false, false, dbid, statfile, MAXPGPATH);

		elog(DEBUG2, "removing stats file \"%s\"", statfile);
		unlink(statfile);

		if (dbentry->tables != NULL)
			hash_destroy(dbentry->tables);
		if (dbentry->functions != NULL)
			hash_destroy(dbentry->functions);

		if (hash_search(pgStatDBHash,
						(void *) &dbid,
						HASH_REMOVE, NULL) == NULL)
			ereport(ERROR,
					(errmsg("database hash table corrupted during cleanup --- abort")));
	}
}


/* ----------
 * pgstat_recv_resetcounter() -
 *
 *	Reset the statistics for the specified database.
 * ----------
 */
static void
pgstat_recv_resetcounter(PgStat_MsgResetcounter *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	/*
	 * Lookup the database in the hashtable.  Nothing to do if not there.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	if (!dbentry)
		return;

	/*
	 * We simply throw away all the database's table entries by recreating a
	 * new hash table for them.
	 */
	if (dbentry->tables != NULL)
		hash_destroy(dbentry->tables);
	if (dbentry->functions != NULL)
		hash_destroy(dbentry->functions);

	dbentry->tables = NULL;
	dbentry->functions = NULL;

	/*
	 * Reset database-level stats, too.  This creates empty hash tables for
	 * tables and functions.
	 */
	reset_dbentry_counters(dbentry);
}

/* ----------
 * pgstat_recv_resetshared() -
 *
 *	Reset some shared statistics of the cluster.
 * ----------
 */
static void
pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter *msg, int len)
{
	if (msg->m_resettarget == RESET_BGWRITER)
	{
		/* Reset the global background writer statistics for the cluster. */
		memset(&globalStats, 0, sizeof(globalStats));
		globalStats.stat_reset_timestamp = GetCurrentTimestamp();
	}
	else if (msg->m_resettarget == RESET_ARCHIVER)
	{
		/* Reset the archiver statistics for the cluster. */
		memset(&archiverStats, 0, sizeof(archiverStats));
		archiverStats.stat_reset_timestamp = GetCurrentTimestamp();
	}

	/*
	 * Presumably the sender of this message validated the target, don't
	 * complain here if it's not valid
	 */
}

/* ----------
 * pgstat_recv_resetsinglecounter() -
 *
 *	Reset a statistics for a single object
 * ----------
 */
static void
pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	if (!dbentry)
		return;

	/* Set the reset timestamp for the whole database */
	dbentry->stat_reset_timestamp = GetCurrentTimestamp();

	/* Remove object if it exists, ignore it if not */
	if (msg->m_resettype == RESET_TABLE)
		(void) hash_search(dbentry->tables, (void *) &(msg->m_objectid),
						   HASH_REMOVE, NULL);
	else if (msg->m_resettype == RESET_FUNCTION)
		(void) hash_search(dbentry->functions, (void *) &(msg->m_objectid),
						   HASH_REMOVE, NULL);
}

/* ----------
 * pgstat_recv_autovac() -
 *
 *	Process an autovacuum signalling message.
 * ----------
 */
static void
pgstat_recv_autovac(PgStat_MsgAutovacStart *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	/*
	 * Store the last autovacuum time in the database's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->last_autovac_time = msg->m_start_time;
}

/* ----------
 * pgstat_recv_vacuum() -
 *
 *	Process a VACUUM message.
 * ----------
 */
static void
pgstat_recv_vacuum(PgStat_MsgVacuum *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true);

	tabentry->n_live_tuples = msg->m_live_tuples;
	tabentry->n_dead_tuples = msg->m_dead_tuples;

	if (msg->m_autovacuum)
	{
		tabentry->autovac_vacuum_timestamp = msg->m_vacuumtime;
		tabentry->autovac_vacuum_count++;
	}
	else
	{
		tabentry->vacuum_timestamp = msg->m_vacuumtime;
		tabentry->vacuum_count++;
	}
}

/* ----------
 * pgstat_recv_analyze() -
 *
 *	Process an ANALYZE message.
 * ----------
 */
static void
pgstat_recv_analyze(PgStat_MsgAnalyze *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	PgStat_StatTabEntry *tabentry;

	/*
	 * Store the data in the table's hashtable entry.
	 */
	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true);

	tabentry->n_live_tuples = msg->m_live_tuples;
	tabentry->n_dead_tuples = msg->m_dead_tuples;

	/*
	 * If commanded, reset changes_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (msg->m_resetcounter)
		tabentry->changes_since_analyze = 0;

	if (msg->m_autovacuum)
	{
		tabentry->autovac_analyze_timestamp = msg->m_analyzetime;
		tabentry->autovac_analyze_count++;
	}
	else
	{
		tabentry->analyze_timestamp = msg->m_analyzetime;
		tabentry->analyze_count++;
	}
}


/* ----------
 * pgstat_recv_archiver() -
 *
 *	Process a ARCHIVER message.
 * ----------
 */
static void
pgstat_recv_archiver(PgStat_MsgArchiver *msg, int len)
{
	if (msg->m_failed)
	{
		/* Failed archival attempt */
		++archiverStats.failed_count;
		memcpy(archiverStats.last_failed_wal, msg->m_xlog,
			   sizeof(archiverStats.last_failed_wal));
		archiverStats.last_failed_timestamp = msg->m_timestamp;
	}
	else
	{
		/* Successful archival operation */
		++archiverStats.archived_count;
		memcpy(archiverStats.last_archived_wal, msg->m_xlog,
			   sizeof(archiverStats.last_archived_wal));
		archiverStats.last_archived_timestamp = msg->m_timestamp;
	}
}

/* ----------
 * pgstat_recv_bgwriter() -
 *
 *	Process a BGWRITER message.
 * ----------
 */
static void
pgstat_recv_bgwriter(PgStat_MsgBgWriter *msg, int len)
{
	globalStats.timed_checkpoints += msg->m_timed_checkpoints;
	globalStats.requested_checkpoints += msg->m_requested_checkpoints;
	globalStats.checkpoint_write_time += msg->m_checkpoint_write_time;
	globalStats.checkpoint_sync_time += msg->m_checkpoint_sync_time;
	globalStats.buf_written_checkpoints += msg->m_buf_written_checkpoints;
	globalStats.buf_written_clean += msg->m_buf_written_clean;
	globalStats.maxwritten_clean += msg->m_maxwritten_clean;
	globalStats.buf_written_backend += msg->m_buf_written_backend;
	globalStats.buf_fsync_backend += msg->m_buf_fsync_backend;
	globalStats.buf_alloc += msg->m_buf_alloc;
}

/* ----------
 * pgstat_recv_recoveryconflict() -
 *
 *	Process a RECOVERYCONFLICT message.
 * ----------
 */
static void
pgstat_recv_recoveryconflict(PgStat_MsgRecoveryConflict *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	switch (msg->m_reason)
	{
		case PROCSIG_RECOVERY_CONFLICT_DATABASE:

			/*
			 * Since we drop the information about the database as soon as it
			 * replicates, there is no point in counting these conflicts.
			 */
			break;
		case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
			dbentry->n_conflict_tablespace++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_LOCK:
			dbentry->n_conflict_lock++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
			dbentry->n_conflict_snapshot++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
			dbentry->n_conflict_bufferpin++;
			break;
		case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
			dbentry->n_conflict_startup_deadlock++;
			break;
	}
}

/* ----------
 * pgstat_recv_deadlock() -
 *
 *	Process a DEADLOCK message.
 * ----------
 */
static void
pgstat_recv_deadlock(PgStat_MsgDeadlock *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->n_deadlocks++;
}

/* ----------
 * pgstat_recv_tempfile() -
 *
 *	Process a TEMPFILE message.
 * ----------
 */
static void
pgstat_recv_tempfile(PgStat_MsgTempFile *msg, int len)
{
	PgStat_StatDBEntry *dbentry;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	dbentry->n_temp_bytes += msg->m_filesize;
	dbentry->n_temp_files += 1;
}

/* ----------
 * pgstat_recv_funcstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void
pgstat_recv_funcstat(PgStat_MsgFuncstat *msg, int len)
{
	PgStat_FunctionEntry *funcmsg = &(msg->m_entry[0]);
	PgStat_StatDBEntry *dbentry;
	PgStat_StatFuncEntry *funcentry;
	int			i;
	bool		found;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

	/*
	 * Process all function entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++, funcmsg++)
	{
		funcentry = (PgStat_StatFuncEntry *) hash_search(dbentry->functions,
														 (void *) &(funcmsg->f_id),
														 HASH_ENTER, &found);

		if (!found)
		{
			/*
			 * If it's a new function entry, initialize counters to the values
			 * we just got.
			 */
			funcentry->f_numcalls = funcmsg->f_numcalls;
			funcentry->f_total_time = funcmsg->f_total_time;
			funcentry->f_self_time = funcmsg->f_self_time;
		}
		else
		{
			/*
			 * Otherwise add the values to the existing entry.
			 */
			funcentry->f_numcalls += funcmsg->f_numcalls;
			funcentry->f_total_time += funcmsg->f_total_time;
			funcentry->f_self_time += funcmsg->f_self_time;
		}
	}
}

/* ----------
 * pgstat_recv_funcpurge() -
 *
 *	Arrange for dead function removal.
 * ----------
 */
static void
pgstat_recv_funcpurge(PgStat_MsgFuncpurge *msg, int len)
{
	PgStat_StatDBEntry *dbentry;
	int			i;

	dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

	/*
	 * No need to purge if we don't even know the database.
	 */
	if (!dbentry || !dbentry->functions)
		return;

	/*
	 * Process all function entries in the message.
	 */
	for (i = 0; i < msg->m_nentries; i++)
	{
		/* Remove from hashtable if present; we don't care if it's not. */
		(void) hash_search(dbentry->functions,
						   (void *) &(msg->m_functionid[i]),
						   HASH_REMOVE, NULL);
	}
}

/* ----------
 * pgstat_write_statsfile_needed() -
 *
 *	Do we need to write out any stats files?
 * ----------
 */
static bool
pgstat_write_statsfile_needed(void)
{
	if (pending_write_requests != NIL)
		return true;

	/* Everything was written recently */
	return false;
}

/* ----------
 * pgstat_db_requested() -
 *
 *	Checks whether stats for a particular DB need to be written to a file.
 * ----------
 */
static bool
pgstat_db_requested(Oid databaseid)
{
	/*
	 * If any requests are outstanding at all, we should write the stats for
	 * shared catalogs (the "database" with OID 0).  This ensures that
	 * backends will see up-to-date stats for shared catalogs, even though
	 * they send inquiry messages mentioning only their own DB.
	 */
	if (databaseid == InvalidOid && pending_write_requests != NIL)
		return true;

	/* Search to see if there's an open request to write this database. */
	if (list_member_oid(pending_write_requests, databaseid))
		return true;

	return false;
}

/*
 * Convert a potentially unsafely truncated activity string (see
 * PgBackendStatus.st_activity_raw's documentation) into a correctly truncated
 * one.
 *
 * The returned string is allocated in the caller's memory context and may be
 * freed.
 */
char *
pgstat_clip_activity(const char *raw_activity)
{
	char	   *activity;
	int			rawlen;
	int			cliplen;

	/*
	 * Some callers, like pgstat_get_backend_current_activity(), do not
	 * guarantee that the buffer isn't concurrently modified. We try to take
	 * care that the buffer is always terminated by a NUL byte regardless, but
	 * let's still be paranoid about the string's length. In those cases the
	 * underlying buffer is guaranteed to be pgstat_track_activity_query_size
	 * large.
	 */
	activity = pnstrdup(raw_activity, pgstat_track_activity_query_size - 1);

	/* now double-guaranteed to be NUL terminated */
	rawlen = strlen(activity);

	/*
	 * All supported server-encodings make it possible to determine the length
	 * of a multi-byte character from its first byte (this is not the case for
	 * client encodings, see GB18030). As st_activity is always stored using
	 * server encoding, this allows us to perform multi-byte aware truncation,
	 * even if the string earlier was truncated in the middle of a multi-byte
	 * character.
	 */
	cliplen = pg_mbcliplen(activity, rawlen,
						   pgstat_track_activity_query_size - 1);

	activity[cliplen] = '\0';

	return activity;
}

/*
 * POLAR: polar_pgstat_get_current_examined_row_count
 */
PgStat_Counter
polar_pgstat_get_current_examined_row_count(void)
{
	PgStat_TableStatus *entry;
	TabStatusArray *tsa;
	int			i;
	PgStat_Counter total_rows = 0;

	/*
	 * Count examined row counts for all relations.
	 */
	for (tsa = pgStatTabList; tsa != NULL; tsa = tsa->tsa_next)
	{
		for (i = 0; i < tsa->tsa_used; i++)
		{
			entry = &tsa->tsa_entries[i];
			total_rows += entry->t_counts.t_tuples_returned
						+ entry->t_counts.t_tuples_fetched;
		}
	}
	return total_rows;
}

/*
 * POLAR: for elog.c get polar audit log examined row count
 */
PgStat_Counter
polar_get_audit_log_row_count(void)
{
	return polar_pgstat_get_current_examined_row_count()
				- polar_audit_log.examined_row_count;
}

/*
 * POLAR: log beentry infomation of proxy.
 * Just log, no changecount check here.
 */
static void
polar_log_beentry_proxy_state(volatile PgBackendStatus *beentry, const char *func)
{
	if (beentry)
		elog(DEBUG2, "%s: pid: %d, in_proxy: %d, vpid: %d, ckey: %d",
					 func, beentry->st_procpid, beentry->polar_st_proxy,
					 beentry->polar_st_virtual_pid, beentry->polar_st_cancel_key);
	else
		elog(DEBUG2, "%s: beentry is NULL", func);
}

/*
 * POLAR: set virtual pid for proxy and check this pid.
 */
void
polar_pgstat_set_virtual_pid(int virtual_pid)
{
	volatile PgBackendStatus *my_beentry = MyBEEntry;
	volatile PgBackendStatus *beentry = NULL;
	int i;
	int			saved_guc_count = 0;

	if (!polar_enable_virtual_pid)
		return;

	if (IS_POLAR_SESSION_SHARED())
	{
		my_beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!my_beentry)
		elog(ERROR, "POLAR: Unable to set virtual pid for beentry is NULL");
	if (!my_beentry->polar_st_proxy)
		elog(ERROR, "POLAR: Unable to set virtual pid when not under proxy mode");
	if (!POLAR_IS_VIRTUAL_PID(virtual_pid))
		elog(ERROR, "POLAR: Invalid virtual pid: %d, should between (10^7, 2*10^7)", virtual_pid);

	if (unlikely(polar_enable_debug))
	{
		polar_log_beentry_proxy_state(my_beentry, __func__);
		elog(DEBUG2, "%s: new_vpid: %d", __func__, virtual_pid);
	}

	/*  The virtual pid has already been set to this session */
	if (my_beentry->polar_st_virtual_pid == virtual_pid)
		return;

	beentry = BackendStatusArray;
	for (i = 1; i <= MaxBackends; i++)
	{
		int			save_procpid = 0;
		int			save_polar_virtual_pid = InvalidPid;

		/*
		 * Follow the protocol of retrying if st_changecount changes while we
		 * copy the entry, or if it's odd.  (The check for odd is needed to
		 * cover the case where we are able to completely copy the entry while
		 * the source backend is between increment steps.)	We use a volatile
		 * pointer here to ensure the compiler doesn't try to get cute.
		 */
		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_save_changecount_before(beentry, before_changecount);

			save_procpid = beentry->st_procpid;
			save_polar_virtual_pid = beentry->polar_st_virtual_pid;

			pgstat_save_changecount_after(beentry, after_changecount);
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}
		/*
		 * Found a conflict virtual pid, report ERROR.
		 */
		if (save_procpid > 0 && unlikely(save_polar_virtual_pid == virtual_pid))
		{
			if (unlikely(polar_enable_debug))
				polar_log_beentry_proxy_state(beentry, __func__);
			elog(ERROR, "POLAR: The virtual pid %d is already in use", virtual_pid);
		}
		beentry++;
	}

	pgstat_increment_changecount_before(my_beentry);
	my_beentry->last_backend_pid = MyProcPid; /* POLAR: Shared Server */
	my_beentry->saved_guc_count = saved_guc_count;
	my_beentry->polar_st_virtual_pid = virtual_pid;
	pgstat_increment_changecount_after(my_beentry);
}

/*
 * POLAR: set cancel key for proxy.
 */
void
polar_pgstat_set_cancel_key(int32 cancel_key)
{
	volatile PgBackendStatus *my_beentry = MyBEEntry;
	int			saved_guc_count = 0;

	if (!polar_enable_virtual_pid)
		return;

	if (IS_POLAR_SESSION_SHARED())
	{
		my_beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		saved_guc_count = polar_session_info()->saved_guc_count;
	}

	if (!my_beentry)
		elog(ERROR, "POLAR: Unable to set cancel key for beentry is NULL");
	if (!my_beentry->polar_st_proxy)
		elog(ERROR, "POLAR: Unable to set cancel key when not under proxy mode");

	if (unlikely(polar_enable_debug))
	{
		polar_log_beentry_proxy_state(my_beentry, __func__);
		elog(DEBUG2, "%s: new_ckey: %d", __func__, cancel_key);
	}

	pgstat_increment_changecount_before(my_beentry);
	my_beentry->last_backend_pid = MyProcPid;
	my_beentry->saved_guc_count = saved_guc_count;
	my_beentry->polar_st_cancel_key = cancel_key;
	pgstat_increment_changecount_after(my_beentry);
}

/*
 * POLAR: get real pid for proxy.
 * The pid passed in might be a real pid, so we just return it.
 * Otherwise we search it in PgBackendStatus.
 * Every proc can get the real pid through it's virtual pid, which
 * means every proc can cancel/terminate a virtual pid.
 *
 * Return:	= 0 stand for not find real pid through this virtual pid
 * 			= -1 (InvalidPid) stand for the cancel_key is not correct!
 * 			> 0 stand for a real system pid
 */
int
polar_pgstat_get_real_pid(int pid, int32 cancel_key, bool auth_cancel_key, bool force)
{
	volatile PgBackendStatus *beentry = NULL;
	int			i = 0;
	int			save_procpid = 0;
	long		save_polar_cancel_key = 0;
	int			save_polar_virtual_pid = InvalidPid;

	Assert(!pgStatRunningInCollector);

	if (polar_enable_debug)
		elog(DEBUG2, "%s, pid: %d, cancel_key: %d, auth_cancel_key: %d",
					__func__, pid, cancel_key, auth_cancel_key);

	/* this pid is the real pid */
	if (POLAR_IS_REAL_PID(pid) && !force)
		return pid;

	beentry = BackendStatusArray;
	for (i = 1; i <= MaxBackends; i++)
	{
		/*
		 * Follow the protocol of retrying if st_changecount changes while we
		 * copy the entry, or if it's odd.  (The check for odd is needed to
		 * cover the case where we are able to completely copy the entry while
		 * the source backend is between increment steps.)	We use a volatile
		 * pointer here to ensure the compiler doesn't try to get cute.
		 */
		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_save_changecount_before(beentry, before_changecount);

			save_procpid = beentry->st_procpid;
			save_polar_virtual_pid = beentry->polar_st_virtual_pid;
			save_polar_cancel_key = beentry->polar_st_cancel_key;

			pgstat_save_changecount_after(beentry, after_changecount);
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}
		/*
		 * Found a match process id, but cancel key is wrong, so we return
		 * -1, mark it
		 */
		if (save_procpid > 0 && save_polar_virtual_pid == pid)
		{
			if (unlikely(polar_enable_debug))
				polar_log_beentry_proxy_state(beentry, __func__);
			/* Found a match; return current real process id */
			if (!auth_cancel_key || save_polar_cancel_key == cancel_key)
				return save_procpid;

			return InvalidPid;
		}
		if (force && save_procpid > 0 && save_procpid == pid)
		{
			if (unlikely(polar_enable_debug))
				polar_log_beentry_proxy_state(beentry, __func__);
			/* Found a match; return current real process id */
			if (!auth_cancel_key || save_polar_cancel_key == cancel_key)
				return save_procpid;
			else if (force)
				elog(ERROR, "POLAR: No corresponding real pid: %d", pid);
			else
				return InvalidPid;
		}
		beentry++;
	}
	/* Not found, so we return 0 which is not a correct pid , mark it */
	if (force)
		elog(ERROR, "POLAR: No corresponding real pid");
	return 0;
}

/*
 * POLAR: get virtual pid for proxy.
 * The pid passed in must be a real pid. We search it in
 * PgBackendStatus, and return the virtual pid.
 * Every proc can get the virtual pid through real pid, which
 * means all proc can see a virtual pid.
 *
 * Return:	< 10000000 and > 1 stand for real_pid
 *			> 10000000 stand for a virtual pid
 */
int
polar_pgstat_get_virtual_pid(int real_pid, bool force)
{
	volatile PgBackendStatus *my_beentry = MyBEEntry;
	volatile PgBackendStatus *beentry = NULL;
	int			i = 0;
	int			save_procpid = 0;
	int			save_polar_virtual_pid = InvalidPid;
	bool		save_polar_st_proxy = false;

	Assert(!pgStatRunningInCollector);

	if (!polar_enable_virtual_pid && !force)
		return real_pid;

	if (unlikely(polar_enable_debug))
	{
		polar_log_beentry_proxy_state(my_beentry, __func__);
		elog(DEBUG2, "%s, real_pid: %d", __func__, real_pid);
	}

	if (IS_POLAR_SESSION_SHARED())
		my_beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];

	/* This is the proc, no need for search */
	if (my_beentry && real_pid == MySessionPid)
	{
		if (my_beentry->polar_st_proxy && my_beentry->polar_st_virtual_pid != 0)
			return my_beentry->polar_st_virtual_pid;
		else
			return real_pid;
	}

	beentry = BackendStatusArray;
	for (i = 1; i <= MaxBackends; i++)
	{
		/*
		 * Follow the protocol of retrying if st_changecount changes while we
		 * copy the entry, or if it's odd.  (The check for odd is needed to
		 * cover the case where we are able to completely copy the entry while
		 * the source backend is between increment steps.)	We use a volatile
		 * pointer here to ensure the compiler doesn't try to get cute.
		 */
		for (;;)
		{
			int			before_changecount;
			int			after_changecount;

			pgstat_save_changecount_before(beentry, before_changecount);

			save_procpid = beentry->st_procpid;
			save_polar_virtual_pid = beentry->polar_st_virtual_pid;
			save_polar_st_proxy = beentry->polar_st_proxy;

			pgstat_save_changecount_after(beentry, after_changecount);
			if (before_changecount == after_changecount &&
				(before_changecount & 1) == 0)
				break;

			/* Make sure we can break out of loop if stuck... */
			CHECK_FOR_INTERRUPTS();
		}
		/* find process id, so return */
		if (save_procpid > 0 && save_procpid == real_pid)
		{
			if (unlikely(polar_enable_debug))
				polar_log_beentry_proxy_state(beentry, __func__);
			/* Found a match; return current real process id */
			if (save_polar_st_proxy && save_polar_virtual_pid != 0)
				return save_polar_virtual_pid;
			else if (force)
				elog(ERROR, "POLAR: No corresponding virtual pid: %d", real_pid);
			else
				return real_pid;
		}
		beentry++;
	}
	/* Not found, we return the real pid */
	if (force)
		elog(ERROR, "POLAR: No such pid: %d", real_pid);
	return real_pid;
}

/*
 * POLAR: get virtual pid for proxy by beentry.
 */
int
polar_pgstat_get_virtual_pid_by_beentry(PgBackendStatus *beentry)
{
	int			save_procpid = 0;
	int			save_polar_virtual_pid = InvalidPid;
	bool		save_polar_st_proxy = false;

	if (beentry == NULL)
		elog(ERROR, "POLAR: get vpid from a NULL beentry");

	if (!polar_enable_virtual_pid)
		return beentry->st_procpid;

	if (unlikely(polar_enable_debug))
		polar_log_beentry_proxy_state(beentry, __func__);

	/*
	 * Follow the protocol of retrying if st_changecount changes while we
	 * copy the entry, or if it's odd.  (The check for odd is needed to
	 * cover the case where we are able to completely copy the entry while
	 * the source backend is between increment steps.)	We use a volatile
	 * pointer here to ensure the compiler doesn't try to get cute.
	 */
	for (;;)
	{
		int			before_changecount;
		int			after_changecount;

		pgstat_save_changecount_before(beentry, before_changecount);

		save_procpid = beentry->st_procpid;
		save_polar_virtual_pid = beentry->polar_st_virtual_pid;
		save_polar_st_proxy = beentry->polar_st_proxy;

		pgstat_save_changecount_after(beentry, after_changecount);
		if (before_changecount == after_changecount &&
			(before_changecount & 1) == 0)
			break;

		/* Make sure we can break out of loop if stuck... */
		CHECK_FOR_INTERRUPTS();
	}
	if (save_polar_st_proxy && save_polar_virtual_pid != 0)
		return save_polar_virtual_pid;
	return save_procpid;
}

/*
 * POLAR: send message to collector process update delay dml
 * count.
 */
void
polar_update_delay_dml_count(void)
{
	PgStat_MsgDropdb msg;

	if (pgStatSock == PGINVALID_SOCKET)
		return;

	pgstat_setheader(&msg.m_hdr, POLAR_PGSTAT_MTYPE_DELAY_DML);
	msg.m_databaseid = MyDatabaseId;
	pgstat_send(&msg, sizeof(msg));
}

/*
 * POLAR: count the number of delay dml
 */
static void
pgstat_recv_delay_dml(PolarStat_MsgDelayDml *msg, int len)
{
	Oid			dbid = msg->m_databaseid;
	PgStat_StatDBEntry *dbentry;

	/*
	 * Lookup the database in the hashtable.
	 */
	dbentry = pgstat_get_db_entry(dbid, true);
	dbentry->polar_delay_dml_count++;
}

/*
 * POLAR: just for maxscale debug test
 */
void
polar_enable_proxy_for_unit_test(bool enable)
{
	PgBackendStatus *beentry = MyBEEntry;
	Port	*current_port = MyProcPort;

	if (IS_POLAR_SESSION_SHARED())
	{
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];
		current_port = polar_session_info()->client_port;
	}

	Assert(current_port && beentry);
	current_port->polar_proxy = enable;
	beentry->polar_st_proxy = enable;
}

/*
 * POLAR: identify a walsender, whether it's a walsender with a high priority 
 * replication standby, or a walsender with a low priority replication standby.
 */
bool
polar_walsender_parse_attrs(int pid, bool *is_high_pri, bool *is_low_pri)
{
	volatile PgBackendStatus *beentry;
	int		i;

	beentry = BackendStatusArray;

	*is_high_pri = *is_low_pri = false;

	for (i = 1; i <= MaxBackends; i++)
	{
		volatile PgBackendStatus *vbeentry = beentry;
		bool		found = false;

		found = polar_backend_parse_attrs(vbeentry, pid, is_high_pri, is_low_pri);

		if (found)
			return true;

		beentry++;
	}

	return false;
}

bool
polar_walsender_parse_my_attrs(bool *is_high_pri, bool *is_low_pri)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (IS_POLAR_SESSION_SHARED())
		beentry = &BackendStatusArray[polar_session()->id + NumBackendStatSlots_base];

	Assert(beentry != NULL);

	/* POLAR: should never return false */
	if (beentry == NULL)
		return false;

	polar_backend_parse_attrs(beentry, 0, is_high_pri, is_low_pri);

	return true;
}

/*
 * POLAR: return true if the backend's pid matches the pid parameter.
 * Identify weather it's a walsender of a high priority replication standby, 
 * or a walsender of a low priority replication standby.
 */
static bool
polar_backend_parse_attrs(volatile PgBackendStatus *beentry, int pid, 
						  bool *is_high_pri, bool *is_low_pri)
{
	bool		found = false;

	if (!beentry)
		return false;

	for (;;)
	{
		int			before_changecount;
		int			after_changecount;

		pgstat_save_changecount_before(beentry, before_changecount);

		if (pid > 0)
			found = (beentry->st_procpid == pid);
		
		*is_high_pri = *is_low_pri = false;

		if (beentry->st_appname != NULL)
		{
			if (POLAR_HIGH_PRI_REP_STANDBYS_DEFINED())
				*is_high_pri = polar_find_in_string_list(beentry->st_appname,
					polar_high_priority_replication_standby_names);

			if (POLAR_LOW_PRI_REP_STANDBYS_DEFINED())
				*is_low_pri = polar_find_in_string_list(beentry->st_appname,
					polar_low_priority_replication_standby_names);
		}

		pgstat_save_changecount_after(beentry, after_changecount);

		if (before_changecount == after_changecount &&
			(before_changecount & 1) == 0)
			break;

		/*no cover begin*/
		/* Make sure we can break out of loop if stuck... */
		CHECK_FOR_INTERRUPTS();
		/*no cover end*/
	}

	return found;
}
