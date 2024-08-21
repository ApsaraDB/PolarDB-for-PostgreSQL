/*-------------------------------------------------------------------------
 *
 * postmaster.c
 *	  This program acts as a clearing house for requests to the
 *	  POSTGRES system.  Frontend programs send a startup message
 *	  to the Postmaster and the postmaster uses the info in the
 *	  message to setup a backend process.
 *
 *	  The postmaster also manages system-wide operations such as
 *	  startup and shutdown. The postmaster itself doesn't do those
 *	  operations, mind you --- it just forks off a subprocess to do them
 *	  at the right times.  It also takes care of resetting the system
 *	  if a backend crashes.
 *
 *	  The postmaster process creates the shared memory and semaphore
 *	  pools during startup, but as a rule does not touch them itself.
 *	  In particular, it is not a member of the PGPROC array of backends
 *	  and so it cannot participate in lock-manager operations.  Keeping
 *	  the postmaster away from shared memory operations makes it simpler
 *	  and more reliable.  The postmaster is almost always able to recover
 *	  from crashes of individual backends by resetting shared memory;
 *	  if it did much with shared memory then it would be prone to crashing
 *	  along with the backends.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/postmaster.c
 *
 * NOTES
 *
 * Initialization:
 *		The Postmaster sets up shared memory data structures
 *		for the backends.
 *
 * Synchronization:
 *		The Postmaster shares memory with the backends but should avoid
 *		touching shared memory, so as not to become stuck if a crashing
 *		backend screws up locks or shared memory.  Likewise, the Postmaster
 *		should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *		The Postmaster cleans up after backends if they have an emergency
 *		exit and/or core dump.
 *
 * Error Reporting:
 *		Use write_stderr() only for reporting "interactive" errors
 *		(essentially, bogus arguments on the command line).  Once the
 *		postmaster is launched, use ereport().
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/param.h>
#include <netdb.h>
#include <limits.h>

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef USE_BONJOUR
#include <dns_sd.h>
#endif

#ifdef USE_SYSTEMD
#include <systemd/sd-daemon.h>
#endif

#ifdef HAVE_PTHREAD_IS_THREADED_NP
#include <pthread.h>
#endif

#include "access/transam.h"
#include "access/xlog.h"
#include "bootstrap/bootstrap.h"
#include "catalog/pg_control.h"
#include "common/file_perm.h"
#include "common/ip.h"
#include "lib/ilist.h"
#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pg_getopt.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "replication/logicallauncher.h"
#include "replication/walsender.h"
#include "replication/slot.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/kmgr.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/dynamic_loader.h"
#include "utils/memutils.h"
#include "utils/pidfile.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"
#include "utils/varlena.h"

#ifdef EXEC_BACKEND
#include "storage/spin.h"
#endif

/* POLAR */
#include <arpa/inet.h>
#include <netinet/in.h>
#include "access/polar_logindex.h"
#include "access/polar_logindex_redo.h"
#include "storage/polar_fd.h"
#include "storage/polar_pbp.h"
#include "polar_dma/polar_dma.h"
#include "px/px_gang.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_log.h"
#include "postmaster/polar_dispatcher.h"
/* POLAR end */

/* POLAR */
typedef enum
{
	PM_RECEIVE_SIGINT = 0,
	PM_RECEIVE_SIGTERM,
	PM_RECEIVE_SIGQUIT,
	PM_ADVANCE_STATE_MACHINE,
	PM_SUBPROCESS_DIE,
	MAX_PM_SIGNALS
} polar_pmstate_change_reason;

/*
 * Possible types of a backend. Beyond being the possible bkend_type values in
 * struct bkend, these are OR-able request flag bits for SignalSomeChildren()
 * and CountChildren().
 */
#define BACKEND_TYPE_NORMAL		0x0001	/* normal backend */
#define BACKEND_TYPE_AUTOVAC	0x0002	/* autovacuum worker process */
#define BACKEND_TYPE_WALSND		0x0004	/* walsender process */
#define BACKEND_TYPE_BGWORKER	0x0008	/* bgworker process */
#define BACKEND_TYPE_ALL		0x000F	/* OR of all the above */

#define BACKEND_TYPE_WORKER		(BACKEND_TYPE_AUTOVAC | BACKEND_TYPE_BGWORKER)

/*
 * List of active backends (or child processes anyway; we don't actually
 * know whether a given child has become a backend or is still in the
 * authorization phase).  This is used mainly to keep track of how many
 * children we have and send them appropriate signals when necessary.
 *
 * "Special" children such as the startup, bgwriter and autovacuum launcher
 * tasks are not in this list.  Autovacuum worker and walsender are in it.
 * Also, "dead_end" children are in it: these are children launched just for
 * the purpose of sending a friendly rejection message to a would-be client.
 * We must track them because they are attached to shared memory, but we know
 * they will never become live backends.  dead_end children are not assigned a
 * PMChildSlot.
 *
 * Background workers are in this list, too.
 */
typedef struct bkend
{
	pid_t		pid;			/* process id of backend */
	int32		cancel_key;		/* cancel key for cancels for this backend */
	int			child_slot;		/* PMChildSlot for this backend, if any */

	/*
	 * Flavor of backend or auxiliary process.  Note that BACKEND_TYPE_WALSND
	 * backends initially announce themselves as BACKEND_TYPE_NORMAL, so if
	 * bkend_type is normal, you should check for a recent transition.
	 */
	int			bkend_type;
	bool		dead_end;		/* is it going to send an error and quit? */
	bool		bgworker_notify;	/* gets bgworker start/stop notifications */
	dlist_node	elem;			/* list link in BackendList */
} Backend;

static dlist_head BackendList = DLIST_STATIC_INIT(BackendList);

/* POLAR */
static int NormalBackendCount = 0;

#ifdef EXEC_BACKEND
static Backend *ShmemBackendArray;
#endif

BackgroundWorker *MyBgworkerEntry = NULL;



/* The socket number we are listening for connections on */
int			PostPortNumber;

/* The directory names for Unix socket(s) */
char	   *Unix_socket_directories;

/* The TCP listen address(es) */
char	   *ListenAddresses;

/*
 * ReservedBackends is the number of backends reserved for superuser use.
 * This number is taken out of the pool size given by MaxConnections so
 * number of backend slots available to non-superusers is
 * (MaxConnections - ReservedBackends).  Note what this really means is
 * "if there are <= ReservedBackends connections available, only superusers
 * can make new connections" --- pre-existing superuser connections don't
 * count against the limit.
 */
int			ReservedBackends;

/* The socket(s) we're listening to. */
#define MAXLISTEN	64

/* POLAR: Shared Server */
#define POLAR_MAXLISTEN 	(MAXLISTEN + POLAR_DISPATCHER_MAX_COUNT)
static pgsocket ListenSocket[POLAR_MAXLISTEN];
static bool dispatcher_had_restart = false;

/*
 * Set by the -o option
 */
static char ExtraOptions[MAXPGPATH];

/*
 * These globals control the behavior of the postmaster in case some
 * backend dumps core.  Normally, it kills all peers of the dead backend
 * and reinitializes shared memory.  By specifying -T or -n, we can have
 * the postmaster stop (rather than kill) peers and not reinitialize
 * shared data structures.  (Reinit is currently dead code, though.)
 */
static bool Reinit = true;
static int	SendStop = false;

/* still more option variables */
bool		EnableSSL = false;

int			PreAuthDelay = 0;
int			AuthenticationTimeout = 60;

bool		log_hostname;		/* for ps display and logging */
bool		Log_connections = false;
bool		Db_user_namespace = false;

bool		enable_bonjour = false;
char	   *bonjour_name;
bool		restart_after_crash = true;

/* Shared Server. startup packet received from dispatcher. */
char*       polar_dispatcher_startup_buf = NULL;
int32       polar_dispatcher_startup_len = 0;
/* POLAR end */

/* PIDs of special child processes; 0 when not running */
static pid_t StartupPID = 0,
			BgWriterPID = 0,
			CheckpointerPID = 0,
			WalWriterPID = 0,
			WalReceiverPID = 0,
			ConsensusPID = 0,
			AutoVacPID = 0,
			PgArchPID = 0,
			PgStatPID = 0,
			SysLoggerPID = 0,
			PolarWalPipelinerPID = 0,
			LogIndexBgPID = 0,
			FlogBgInserterPID = 0,
			FlogBgWriterPID = 0;

static pid_t SysLoggerPIDs[MAX_SYSLOGGER_NUM];   /* POLAR */

/* Startup process's status */
typedef enum
{
	STARTUP_NOT_RUNNING,
	STARTUP_RUNNING,
	STARTUP_SIGNALED,			/* we sent it a SIGQUIT or SIGKILL */
	STARTUP_CRASHED
} StartupStatusEnum;

static StartupStatusEnum StartupStatus = STARTUP_NOT_RUNNING;

/* Startup/shutdown state */
#define			NoShutdown		0
#define			SmartShutdown	1
#define			FastShutdown	2
#define			ImmediateShutdown	3

static int	Shutdown = NoShutdown;

static bool FatalError = false; /* T if recovering from backend crash */

/* POLAR: */
volatile sig_atomic_t polar_in_error_handling_process = false;

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 *
 * After doing all the postmaster initialization work, we enter PM_STARTUP
 * state and the startup process is launched. The startup process begins by
 * reading the control file and other preliminary initialization steps.
 * In a normal startup, or after crash recovery, the startup process exits
 * with exit code 0 and we switch to PM_RUN state.  However, archive recovery
 * is handled specially since it takes much longer and we would like to support
 * hot standby during archive recovery.
 *
 * When the startup process is ready to start archive recovery, it signals the
 * postmaster, and we switch to PM_RECOVERY state. The background writer and
 * checkpointer are launched, while the startup process continues applying WAL.
 * If Hot Standby is enabled, then, after reaching a consistent point in WAL
 * redo, startup process signals us again, and we switch to PM_HOT_STANDBY
 * state and begin accepting connections to perform read-only queries.  When
 * archive recovery is finished, the startup process exits with exit code 0
 * and we switch to PM_RUN state.
 *
 * Normal child backends can only be launched when we are in PM_RUN or
 * PM_HOT_STANDBY state.  (We also allow launch of normal
 * child backends in PM_WAIT_BACKUP state, but only for superusers.)
 * In other states we handle connection requests by launching "dead_end"
 * child processes, which will simply send the client an error message and
 * quit.  (We track these in the BackendList so that we can know when they
 * are all gone; this is important because they're still connected to shared
 * memory, and would interfere with an attempt to destroy the shmem segment,
 * possibly leading to SHMALL failure when we try to make a new one.)
 * In PM_WAIT_DEAD_END state we are waiting for all the dead_end children
 * to drain out of the system, and therefore stop accepting connection
 * requests at all until the last existing child has quit (which hopefully
 * will not be very long).
 *
 * Notice that this state variable does not distinguish *why* we entered
 * states later than PM_RUN --- Shutdown and FatalError must be consulted
 * to find that out.  FatalError is never true in PM_RECOVERY_* or PM_RUN
 * states, nor in PM_SHUTDOWN states (because we don't enter those states
 * when trying to recover from a crash).  It can be true in PM_STARTUP state,
 * because we don't clear it until we've successfully started WAL redo.
 */
typedef enum
{
	PM_INIT,					/* postmaster starting */
	PM_STARTUP,					/* waiting for startup subprocess */
	PM_RECOVERY,				/* in archive recovery mode */
	PM_HOT_STANDBY,				/* in hot standby mode */
	PM_RUN,						/* normal "database is alive" state */
	PM_WAIT_BACKUP,				/* waiting for online backup mode to end */
	PM_WAIT_READONLY,			/* waiting for read only backends to exit */
	PM_WAIT_BACKENDS,			/* waiting for live backends to exit */
	PM_SHUTDOWN,				/* waiting for checkpointer to do shutdown
								 * ckpt */
	PM_SHUTDOWN_2,				/* waiting for archiver and walsenders to
								 * finish */
	PM_WAIT_DEAD_END,			/* waiting for dead_end children to exit */
	PM_NO_CHILDREN,				/* all important children have exited */
	PM_DATAMAX 				/* in datamax mode */
} PMState;

static PMState pmState = PM_INIT;

/* Start time of SIGKILL timeout during immediate shutdown or child crash */
/* Zero means timeout is not running */
static time_t AbortStartTime = 0;

/* Length of said timeout */
#define SIGKILL_CHILDREN_AFTER_SECS		5

static bool ReachedNormalRunning = false;	/* T if we've reached PM_RUN */

bool		ClientAuthInProgress = false;	/* T during new-client
											 * authentication */

bool		redirection_done = false;	/* stderr redirected for syslogger? */

/* received START_AUTOVAC_LAUNCHER signal */
static volatile sig_atomic_t start_autovac_launcher = false;

/* the launcher needs to be signalled to communicate some condition */
static volatile bool avlauncher_needs_signal = false;

/* received START_WALRECEIVER signal */
static volatile sig_atomic_t WalReceiverRequested = false;

/* set when there's a worker that needs to be started up */
static volatile bool StartWorkerNeeded = true;
static volatile bool HaveCrashedWorker = false;

#ifndef HAVE_STRONG_RANDOM
/*
 * State for assigning cancel keys.
 * Also, the global MyCancelKey passes the cancel key assigned to a given
 * backend from the postmaster to that backend (via fork).
 */
static unsigned int random_seed = 0;
static struct timeval random_start_time;
#endif

#ifdef USE_SSL
/* Set when and if SSL has been initialized properly */
static bool LoadedSSL = false;
#endif

#ifdef USE_BONJOUR
static DNSServiceRef bonjour_sdref = NULL;
#endif

/*
 * postmaster.c - function prototypes
 */
static void CloseServerPorts(int status, Datum arg);
static void unlink_external_pid_file(int status, Datum arg);
static void getInstallationPaths(const char *argv0);
static void checkControlFile(void);
static Port *ConnCreate(int serverFd);
static void ConnFree(Port *port);
static Port* polar_conn_create_from_dispatcher(int serverFd, char** startup_buf, int32* startup_len);
static void reset_shared(int port);
static void SIGHUP_handler(SIGNAL_ARGS);
static void pmdie(SIGNAL_ARGS);
static void reaper(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void startup_die(SIGNAL_ARGS);
static void dummy_handler(SIGNAL_ARGS);
static void StartupPacketTimeoutHandler(void);
static void CleanupBackend(int pid, int exitstatus);
static bool CleanupBackgroundWorker(int pid, int exitstatus);
static void HandleChildCrash(int pid, int exitstatus, const char *procname);
static void LogChildExit(int lev, const char *procname,
			 int pid, int exitstatus);
static void PostmasterStateMachine(polar_pmstate_change_reason reason, pid_t pid);
static void BackendInitialize(Port *port);
static void BackendRun(Port *port) pg_attribute_noreturn();
static void ExitPostmaster(int status) pg_attribute_noreturn();
static int	ServerLoop(void);
static int	BackendStartup(Port *port);
static int	polar_get_startup_packet(bool SSLdone, char** polar_buf, int32* polar_len);
static void SendNegotiateProtocolVersion(List *unrecognized_protocol_options);
static int	initMasks(fd_set *rmask);
static int  polar_init_dispatcher_masks(fd_set *rmask, int numSockets);

static void report_fork_failure_to_client(Port *port, int errnum);
static CAC_state canAcceptConnections(int backend_type);
static void signal_child(pid_t pid, int signal);
static bool SignalSomeChildren(int signal, int targets, bool polar_skip_mount_process);
static void TerminateChildren(int signal);

/* POLAR: default not skip polar_vfs process. */
#define SignalChildren(sig)			   SignalSomeChildren(sig, BACKEND_TYPE_ALL, false)

static int	CountChildren(int target, bool skip_polar_mount_process);
static bool assign_backendlist_entry(RegisteredBgWorker *rw);
static void maybe_start_bgworkers(bool polar_start_mount_process);
static bool CreateOptsFile(int argc, char *argv[], char *fullprogname);
static pid_t StartChildProcess(AuxProcType type);
static void StartAutovacuumWorker(void);
static void MaybeStartWalReceiver(void);
static void InitPostmasterDeathWatchHandle(void);

static void polar_dispatcher_start(void);
static void polar_dispatcher_handle_signal(int signal);
static void polar_dispatcher_worker_start(int id);
static bool polar_encode_origin_conn(char *host, char *port, SockAddr* sock);
static void polar_state_machine_change_check(polar_pmstate_change_reason reason, pid_t pid);

/* POLARDB */
static void polar_load_polar_node_static_config(bool force_create);
static void polar_log_pmstate(void);
static void polar_postmaster_online_promote(void);
/* POLARDB end */

/*
 * Archiver is allowed to start up at the current postmaster state?
 *
 * If WAL archiving is enabled always, we are allowed to start archiver
 * even during recovery.
 */
#define PgArchStartupAllowed()	\
	((XLogArchivingActive() && pmState == PM_RUN) ||	\
	 (XLogArchivingAlways() &&	\
	  (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)))

#ifdef EXEC_BACKEND

#ifdef WIN32
#define WNOHANG 0				/* ignored, so any integer value will do */

static pid_t waitpid(pid_t pid, int *exitstatus, int options);
static void WINAPI pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired);

static HANDLE win32ChildQueue;

typedef struct
{
	HANDLE		waitHandle;
	HANDLE		procHandle;
	DWORD		procId;
} win32_deadchild_waitinfo;
#endif							/* WIN32 */

static pid_t backend_forkexec(Port *port);
static pid_t internal_forkexec(int argc, char *argv[], Port *port);

/* Type for a socket that can be inherited to a client process */
#ifdef WIN32
typedef struct
{
	SOCKET		origsocket;		/* Original socket value, or PGINVALID_SOCKET
								 * if not a socket */
	WSAPROTOCOL_INFO wsainfo;
} InheritableSocket;
#else
typedef int InheritableSocket;
#endif

/*
 * Structure contains all variables passed to exec:ed backends
 */
typedef struct
{
	Port		port;
	InheritableSocket portsocket;
	char		DataDir[MAXPGPATH];
	pgsocket	ListenSocket[POLAR_MAXLISTEN];
	int32		MyCancelKey;
	int			MyPMChildSlot;
#ifndef WIN32
	unsigned long UsedShmemSegID;
#else
	void	   *ShmemProtectiveRegion;
	HANDLE		UsedShmemSegID;
#endif
	void	   *UsedShmemSegAddr;
	slock_t    *ShmemLock;
	VariableCache ShmemVariableCache;
	Backend    *ShmemBackendArray;
#ifndef HAVE_SPINLOCKS
	PGSemaphore *SpinlockSemaArray;
#endif
	int			NamedLWLockTrancheRequests;
	NamedLWLockTranche *NamedLWLockTrancheArray;
	LWLockPadded *MainLWLockArray;
	slock_t    *ProcStructLock;
	PROC_HDR   *ProcGlobal;
	PGPROC	   *AuxiliaryProcs;
	PGPROC	   *PreparedXactProcs;
	PMSignalData *PMSignalState;
	InheritableSocket pgStatSock;
	pid_t		PostmasterPid;
	TimestampTz PgStartTime;
	TimestampTz PgReloadTime;
	pg_time_t	first_syslogger_file_time;
	bool		redirection_done;
	bool		IsBinaryUpgrade;
	int			max_safe_fds;
	int			MaxBackends;

	/* POLAR px */
	int			MaxNormalBackends;

#ifdef WIN32
	HANDLE		PostmasterHandle;
	HANDLE		initial_signal_pipe;
	HANDLE		syslogPipe[2];
	HANDLE      syslogChannels[MAX_SYS_LOGGER_CHANNEL_NUM][2];
#else
	int			postmaster_alive_fds[2];
	int			syslogPipe[2];
	int         syslogChannels[MAX_SYS_LOGGER_CHANNEL_NUM][2];
#endif
	char		my_exec_path[MAXPGPATH];
	char		pkglib_path[MAXPGPATH];
	char		ExtraOptions[MAXPGPATH];
} BackendParameters;

static void read_backend_variables(char *id, Port *port);
static void restore_backend_variables(BackendParameters *param, Port *port);

#ifndef WIN32
static bool save_backend_variables(BackendParameters *param, Port *port);
#else
static bool save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid);
#endif

static void ShmemBackendArrayAdd(Backend *bn);
static void ShmemBackendArrayRemove(Backend *bn);
#endif							/* EXEC_BACKEND */

#define StartupDataBase()		StartChildProcess(StartupProcess)
#define StartBackgroundWriter() StartChildProcess(BgWriterProcess)
#define StartCheckpointer()		StartChildProcess(CheckpointerProcess)
#define StartWalWriter()		StartChildProcess(WalWriterProcess)
#define StartWalReceiver()		StartChildProcess(WalReceiverProcess)
#define StartConsensus()            StartChildProcess(ConsensusProcess)
#define StartPolarWalPipeliner()	StartChildProcess(PolarWalPipelinerProcess)
#define StartLogIndexBgWriter() StartChildProcess(LogIndexBgWriterProcess)
#define StartFlogBgInserter()   StartChildProcess(FlogBgInserterProcess)
#define StartFlogBgWriter()     StartChildProcess(FlogBgWriterProcess)

/* Macros to check exit status of a child process */
#define EXIT_STATUS_0(st)  ((st) == 0)
#define EXIT_STATUS_1(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 1)
#define EXIT_STATUS_3(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 3)

#ifndef WIN32
/*
 * File descriptors for pipe used to monitor if postmaster is alive.
 * First is POSTMASTER_FD_WATCH, second is POSTMASTER_FD_OWN.
 */
int			postmaster_alive_fds[2] = {-1, -1};
#else
/* Process handle of postmaster used for the same purpose on Windows */
HANDLE		PostmasterHandle;
#endif

/*
 * Postmaster main entry point
 */
void
PostmasterMain(int argc, char *argv[])
{
	int			opt;
	int			status;
	char	   *userDoption = NULL;
	bool		listen_addr_saved = false;
	int			i;
	char	   *output_config_variable = NULL;

	MySessionPid = MyProcPid = PostmasterPid = getpid();

	MyStartTime = time(NULL);

	IsPostmasterEnvironment = true;

	/*
	 * We should not be creating any files or directories before we check the
	 * data directory (see checkDataDir()), but just in case set the umask to
	 * the most restrictive (owner-only) permissions.
	 *
	 * checkDataDir() will reset the umask based on the data directory
	 * permissions.
	 */
	umask(PG_MODE_MASK_OWNER);

	/*
	 * Initialize random(3) so we don't get the same values in every run.
	 *
	 * Note: the seed is pretty predictable from externally-visible facts such
	 * as postmaster start time, so avoid using random() for security-critical
	 * random values during postmaster startup.  At the time of first
	 * connection, PostmasterRandom will select a hopefully-more-random seed.
	 */
	srandom((unsigned int) (MyProcPid ^ MyStartTime));

	/*
	 * By default, palloc() requests in the postmaster will be allocated in
	 * the PostmasterContext, which is space that can be recycled by backends.
	 * Allocated data that needs to be available to backends should be
	 * allocated in TopMemoryContext.
	 */
	PostmasterContext = AllocSetContextCreate(TopMemoryContext,
											  "Postmaster",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(PostmasterContext);

	/* Initialize paths to installation files */
	getInstallationPaths(argv[0]);

	/*
	 * Set up signal handlers for the postmaster process.
	 *
	 * In the postmaster, we want to install non-ignored handlers *without*
	 * SA_RESTART.  This is because they'll be blocked at all times except
	 * when ServerLoop is waiting for something to happen, and during that
	 * window, we want signals to exit the select(2) wait so that ServerLoop
	 * can respond if anything interesting happened.  On some platforms,
	 * signals marked SA_RESTART would not cause the select() wait to end.
	 * Child processes will generally want SA_RESTART, but we expect them to
	 * set up their own handlers before unblocking signals.
	 *
	 * CAUTION: when changing this list, check for side-effects on the signal
	 * handling setup of child processes.  See tcop/postgres.c,
	 * bootstrap/bootstrap.c, postmaster/bgwriter.c, postmaster/walwriter.c,
	 * postmaster/autovacuum.c, postmaster/pgarch.c, postmaster/pgstat.c,
	 * postmaster/syslogger.c, postmaster/bgworker.c and
	 * postmaster/checkpointer.c.
	 */
	pqinitmask();
	PG_SETMASK(&BlockSig);

	pqsignal_no_restart(SIGHUP, SIGHUP_handler);	/* reread config file and
													 * have children do same */
	pqsignal_no_restart(SIGINT, pmdie); /* send SIGTERM and shut down */
	pqsignal_no_restart(SIGQUIT, pmdie);	/* send SIGQUIT and die */
	pqsignal_no_restart(SIGTERM, pmdie);	/* wait for children and shut down */
	pqsignal(SIGALRM, SIG_IGN); /* ignored */
	pqsignal(SIGPIPE, SIG_IGN); /* ignored */
	pqsignal_no_restart(SIGUSR1, sigusr1_handler);	/* message from child
													 * process */
	pqsignal_no_restart(SIGUSR2, dummy_handler);	/* unused, reserve for
													 * children */
	pqsignal_no_restart(SIGCHLD, reaper);	/* handle child termination */
	pqsignal(SIGTTIN, SIG_IGN); /* ignored */
	pqsignal(SIGTTOU, SIG_IGN); /* ignored */
	/* ignore SIGXFSZ, so that ulimit violations work like disk full */
#ifdef SIGXFSZ
	pqsignal(SIGXFSZ, SIG_IGN); /* ignored */
#endif

	/*
	 * Options setup
	 */
	InitializeGUCOptions();

	opterr = 1;

	/*
	 * Parse command-line options.  CAUTION: keep this in sync with
	 * tcop/postgres.c (the option sets should not conflict) and with the
	 * common help() function in main/main.c.
	 */
	while ((opt = getopt(argc, argv, "B:bc:C:D:d:EeFf:h:ijk:lN:nOo:Pp:r:S:sTt:W:-:")) != -1)
	{
		switch (opt)
		{
			case 'B':
				SetConfigOption("shared_buffers", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'b':
				/* Undocumented flag used for binary upgrades */
				IsBinaryUpgrade = true;
				break;

			case 'C':
				output_config_variable = strdup(optarg);
				break;

			case 'D':
				userDoption = strdup(optarg);
				break;

			case 'd':
				set_debug_options(atoi(optarg), PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'E':
				SetConfigOption("log_statement", "all", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'F':
				SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, PGC_POSTMASTER, PGC_S_ARGV))
				{
					write_stderr("%s: invalid argument for option -f: \"%s\"\n",
								 progname, optarg);
					ExitPostmaster(1);
				}
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'j':
				/* only used by interactive backend */
				break;

			case 'k':
				SetConfigOption("unix_socket_directories", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'l':
				SetConfigOption("ssl", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'n':
				/* Don't reinit shared mem after abnormal exit */
				Reinit = false;
				break;

			case 'O':
				SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'o':
				/* Other options to pass to the backend on the command line */
				snprintf(ExtraOptions + strlen(ExtraOptions),
						 sizeof(ExtraOptions) - strlen(ExtraOptions),
						 " %s", optarg);
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'p':
				SetConfigOption("port", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'r':
				/* only used by single-user backend */
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 's':
				SetConfigOption("log_statement_stats", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'T':

				/*
				 * In the event that some backend dumps core, send SIGSTOP,
				 * rather than SIGQUIT, to all its peers.  This lets the wily
				 * post_hacker collect core dumps from everyone.
				 */
				SendStop = true;
				/* 
				 * POLAR: set guc parameter polar_enable_send_stop at the same time when specified -T on command line
				 * avoid the value of SendStop being reset by polar_enable_send_stop in postgresql.conf
				 * the priority of setting guc parameter by command line is higher than that by postgresql.conf
				 */
				SetConfigOption("polar_enable_send_stop", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
					{
						SetConfigOption(tmp, "true", PGC_POSTMASTER, PGC_S_ARGV);
					}
					else
					{
						write_stderr("%s: invalid argument for option -t: \"%s\"\n",
									 progname, optarg);
						ExitPostmaster(1);
					}
					break;
				}

			case 'W':
				SetConfigOption("post_auth_delay", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (opt == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}

					SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
					free(name);
					if (value)
						free(value);
					break;
				}

			default:
				write_stderr("Try \"%s --help\" for more information.\n",
							 progname);
				ExitPostmaster(1);
		}
	}

	/*
	 * Postmaster accepts no non-option switch arguments.
	 */
	if (optind < argc)
	{
		write_stderr("%s: invalid argument: \"%s\"\n",
					 progname, argv[optind]);
		write_stderr("Try \"%s --help\" for more information.\n",
					 progname);
		ExitPostmaster(1);
	}

	/*
	 * Locate the proper configuration files and data directory, and read
	 * postgresql.conf for the first time.
	 */
	if (!SelectConfigFiles(userDoption, progname))
		ExitPostmaster(2);

	if (output_config_variable != NULL)
	{
		const char *config_val;
		/*
		 * "-C guc" was specified, so print GUC's value and exit.  No extra
		 * permission check is needed because the user is reading inside the
		 * data dir.
		 * POLAR: load polar_node_static.conf if exists, not force to create it.
		 */
		if (polar_enable_node_static_config)
			polar_load_polar_node_static_config(false);
		config_val = GetConfigOption(output_config_variable, false, false);

		puts(config_val ? config_val : "");
		ExitPostmaster(0);
	}

	/* Verify that DataDir looks reasonable */
	checkDataDir();

	if (polar_enable_node_static_config)
		polar_load_polar_node_static_config(true);
	/* Check that pg_control exists */
	checkControlFile();

	/* And switch working directory into it */
	ChangeToDataDir();

	/*
	 * Check for invalid combinations of GUC settings.
	 */
	if (ReservedBackends + max_wal_senders >= MaxConnections)
	{
		write_stderr("%s: superuser_reserved_connections (%d) plus max_wal_senders (%d) must be less than max_connections (%d)\n",
					 progname,
					 ReservedBackends, max_wal_senders, MaxConnections);
		ExitPostmaster(1);
	}
	if (XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("WAL archival cannot be enabled when wal_level is \"minimal\"")));
	if (max_wal_senders > 0 && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("WAL streaming (max_wal_senders > 0) requires wal_level \"replica\" or \"logical\"")));

	/*
	 * Other one-time internal sanity checks can go here, if they are fast.
	 * (Put any slow processing further down, after postmaster.pid creation.)
	 */
	if (!CheckDateTokenTables())
	{
		write_stderr("%s: invalid datetoken tables, please fix\n", progname);
		ExitPostmaster(1);
	}

	/*
	 * Now that we are done processing the postmaster arguments, reset
	 * getopt(3) library so that it will work correctly in subprocesses.
	 */
	optind = 1;
#ifdef HAVE_INT_OPTRESET
	optreset = 1;				/* some systems need this too */
#endif

	/* For debugging: display postmaster environment */
	{
		extern char **environ;
		char	  **p;

		ereport(DEBUG3,
				(errmsg_internal("%s: PostmasterMain: initial environment dump:",
								 progname)));
		ereport(DEBUG3,
				(errmsg_internal("-----------------------------------------")));
		for (p = environ; *p; ++p)
			ereport(DEBUG3,
					(errmsg_internal("\t%s", *p)));
		ereport(DEBUG3,
				(errmsg_internal("-----------------------------------------")));
	}

	/* 
	 * POLAR: Delete shared memory stat file before lockfile created.
	 * The shmem_stat_file may be unreliable after database crashes.
	 * Delete before lockfile to make correct assurance. 
	 * Add to on_proc_exit callbacks once here to avoid add repeatly 
	 * when regress test kill process randomly.
	 */
	polar_unlink_shmem_stat_file(0, 0);
	on_proc_exit(polar_unlink_shmem_stat_file, 0);
	/* POLAR end */

	/*
	 * Create lockfile for data directory.
	 *
	 * We want to do this before we try to grab the input sockets, because the
	 * data directory interlock is more reliable than the socket-file
	 * interlock (thanks to whoever decided to put socket files in /tmp :-().
	 * For the same reason, it's best to grab the TCP socket(s) before the
	 * Unix socket(s).
	 *
	 * Also note that this internally sets up the on_proc_exit function that
	 * is responsible for removing both data directory and socket lockfiles;
	 * so it must happen before opening sockets so that at exit, the socket
	 * lockfiles go away after CloseServerPorts runs.
	 */
	CreateDataDirLockFile(true);

	/*
	 * Read the control file (for error checking and config info).
	 *
	 * Since we verify the control file's CRC, this has a useful side effect
	 * on machines where we need a run-time test for CRC support instructions.
	 * The postmaster will do the test once at startup, and then its child
	 * processes will inherit the correct function pointer and not need to
	 * repeat the test.
	 */
	LocalProcessControlFile(false);

	/*
	 * Initialize SSL library, if specified.
	 */
#ifdef USE_SSL
	if (EnableSSL)
	{
		(void) secure_initialize(true);
		LoadedSSL = true;
	}
#endif

	/* POLAR: init node type before load shared libraries which may need to check node type. */
	polar_init_node_type();
	/* POLAR end */

	/*
	 * Register the apply launcher.  Since it registers a background worker,
	 * it needs to be called before InitializeMaxBackends(), and it's probably
	 * a good idea to call it before any modules had chance to take the
	 * background worker slots.
	 */
	ApplyLauncherRegister();

	/*
	 * POLAR: Calculate MaxBackends before loading shared preload libraries.
	 * Because some libraries may need MaxBackends to caculate size of shared
	 * memory in _PG_init(). It will prevent shared memory which they actually
	 * alloc later from being larger than the request value.
	 */
	InitializeMaxBackends();

	/*
	 * process any libraries that should be preloaded at postmaster start
	 */
	process_shared_preload_libraries();

	/*
	 * Establish input sockets.
	 *
	 * First, mark them all closed, and set up an on_proc_exit function that's
	 * charged with closing the sockets again at postmaster shutdown.
	 */
	for (i = 0; i < POLAR_MAXLISTEN; i++)
		ListenSocket[i] = PGINVALID_SOCKET;

	on_proc_exit(CloseServerPorts, 0);

	if (ListenAddresses)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of ListenAddresses */
		rawstring = pstrdup(ListenAddresses);

		/* Parse string into list of hostnames */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"listen_addresses")));
		}

		foreach(l, elemlist)
		{
			char	   *curhost = (char *) lfirst(l);

			if (strcmp(curhost, "*") == 0)
				status = StreamServerPort(AF_UNSPEC, NULL,
										  (unsigned short) PostPortNumber,
										  NULL,
										  ListenSocket, MAXLISTEN);
			else
				status = StreamServerPort(AF_UNSPEC, curhost,
										  (unsigned short) PostPortNumber,
										  NULL,
										  ListenSocket, MAXLISTEN);

			if (status == STATUS_OK)
			{
				success++;
				/* record the first successful host addr in lockfile */
				if (!listen_addr_saved)
				{
					AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, curhost);
					listen_addr_saved = true;
				}
			}
			else
				ereport(WARNING,
						(errmsg("could not create listen socket for \"%s\"",
								curhost)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("could not create any TCP/IP sockets")));

		list_free(elemlist);
		pfree(rawstring);
	}

#ifdef USE_BONJOUR
	/* Register for Bonjour only if we opened TCP socket(s) */
	if (enable_bonjour && ListenSocket[0] != PGINVALID_SOCKET)
	{
		DNSServiceErrorType err;

		/*
		 * We pass 0 for interface_index, which will result in registering on
		 * all "applicable" interfaces.  It's not entirely clear from the
		 * DNS-SD docs whether this would be appropriate if we have bound to
		 * just a subset of the available network interfaces.
		 */
		err = DNSServiceRegister(&bonjour_sdref,
								 0,
								 0,
								 bonjour_name,
								 "_postgresql._tcp.",
								 NULL,
								 NULL,
								 pg_hton16(PostPortNumber),
								 0,
								 NULL,
								 NULL,
								 NULL);
		if (err != kDNSServiceErr_NoError)
			elog(LOG, "DNSServiceRegister() failed: error code %ld",
				 (long) err);

		/*
		 * We don't bother to read the mDNS daemon's reply, and we expect that
		 * it will automatically terminate our registration when the socket is
		 * closed at postmaster termination.  So there's nothing more to be
		 * done here.  However, the bonjour_sdref is kept around so that
		 * forked children can close their copies of the socket.
		 */
	}
#endif

#ifdef HAVE_UNIX_SOCKETS
	if (Unix_socket_directories)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);

			status = StreamServerPort(AF_UNIX, NULL,
									  (unsigned short) PostPortNumber,
									  socketdir,
									  ListenSocket, MAXLISTEN);

			if (status == STATUS_OK)
			{
				success++;
				/* record the first successful Unix socket in lockfile */
				if (success == 1)
					AddToDataDirLockFile(LOCK_FILE_LINE_SOCKET_DIR, socketdir);
			}
			else
				ereport(WARNING,
						(errmsg("could not create Unix-domain socket in directory \"%s\"",
								socketdir)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("could not create any Unix-domain sockets")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}
#endif

	/*
	 * check that we have some socket to listen on
	 */
	if (ListenSocket[0] == PGINVALID_SOCKET)
		ereport(FATAL,
				(errmsg("no socket created for listening")));

	/*
	 * If no valid TCP ports, write an empty line for listen address,
	 * indicating the Unix socket must be used.  Note that this line is not
	 * added to the lock file until there is a socket backing it.
	 */
	if (!listen_addr_saved)
		AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, "");

	/*
	 * Set up shared memory and semaphores.
	 */
	reset_shared(PostPortNumber);

	/*
	 * Estimate number of openable files.  This must happen after setting up
	 * semaphores, because on some platforms semaphores count as open files.
	 */
	set_max_safe_fds();

	/*
	 * Set reference point for stack-depth checking.
	 */
	set_stack_base();

	/*
	 * Initialize pipe (or process handle on Windows) that allows children to
	 * wake up from sleep on postmaster death.
	 */
	InitPostmasterDeathWatchHandle();

#ifdef WIN32

	/*
	 * Initialize I/O completion port used to deliver list of dead children.
	 */
	win32ChildQueue = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 1);
	if (win32ChildQueue == NULL)
		ereport(FATAL,
				(errmsg("could not create I/O completion port for child queue")));
#endif

	/*
	 * Record postmaster options.  We delay this till now to avoid recording
	 * bogus options (eg, NBuffers too high for available memory).
	 */
	if (!CreateOptsFile(argc, argv, my_exec_path))
		ExitPostmaster(1);

#ifdef EXEC_BACKEND
	/* Write out nondefault GUC settings for child processes to use */
	write_nondefault_variables(PGC_POSTMASTER);
#endif

	/*
	 * Write the external PID file if requested
	 */
	if (external_pid_file)
	{
		FILE	   *fpidfile = fopen(external_pid_file, "w");

		if (fpidfile)
		{
			fprintf(fpidfile, "%d\n", MyProcPid);
			fclose(fpidfile);

			/* Make PID file world readable */
			if (chmod(external_pid_file, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) != 0)
				write_stderr("%s: could not change permissions of external PID file \"%s\": %s\n",
							 progname, external_pid_file, strerror(errno));
		}
		else
			write_stderr("%s: could not write external PID file \"%s\": %s\n",
						 progname, external_pid_file, strerror(errno));

		on_proc_exit(unlink_external_pid_file, 0);
	}

	/*
	 * Remove old temporary files.  At this point there can be no other
	 * Postgres processes running in this directory, so this should be safe.
	 */
	RemovePgTempFiles();

	/*
	 * Forcibly remove the files signaling a standby promotion request.
	 * Otherwise, the existence of those files triggers a promotion too early,
	 * whether a user wants that or not.
	 *
	 * This removal of files is usually unnecessary because they can exist
	 * only during a few moments during a standby promotion. However there is
	 * a race condition: if pg_ctl promote is executed and creates the files
	 * during a promotion, the files can stay around even after the server is
	 * brought up to new master. Then, if new standby starts by using the
	 * backup taken from that master, the files can exist at the server
	 * startup and should be removed in order to avoid an unexpected
	 * promotion.
	 *
	 * Note that promotion signal files need to be removed before the startup
	 * process is invoked. Because, after that, they can be used by
	 * postmaster's SIGUSR1 signal handler.
	 */
	RemovePromoteSignalFiles();

	/* Remove any outdated file holding the current log filenames. */
	if (unlink(LOG_METAINFO_DATAFILE) < 0 && errno != ENOENT)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						LOG_METAINFO_DATAFILE)));

	/*
	 * If enabled, start up syslogger collection subprocess
	 */
	if (!polar_enable_multi_syslogger) 
		SysLoggerPID = SysLogger_Start(0);
	else 
	{
		/* POLAR */
		int i; 
		memset(SysLoggerPIDs, 0, sizeof(pid_t) * MAX_SYSLOGGER_NUM);
		for (i = 0; i < polar_num_of_sysloggers; i++)
			SysLoggerPIDs[i] = SysLogger_Start(i);
		/* POLAR end */
	}

	/*
	 * Reset whereToSendOutput from DestDebug (its starting state) to
	 * DestNone. This stops ereport from sending log messages to stderr unless
	 * Log_destination permits.  We don't do this until the postmaster is
	 * fully launched, since startup failures may as well be reported to
	 * stderr.
	 *
	 * If we are in fact disabling logging to stderr, first emit a log message
	 * saying so, to provide a breadcrumb trail for users who may not remember
	 * that their logging is configured to go somewhere else.
	 */
	if (!(Log_destination & LOG_DESTINATION_STDERR))
		ereport(LOG,
				(errmsg("ending log output to stderr"),
				 errhint("Future log output will go to log destination \"%s\".",
						 Log_destination_string)));

	whereToSendOutput = DestNone;

	/*
	 * Initialize stats collection subsystem (this does NOT start the
	 * collector process!)
	 */
	pgstat_init();

	/*
	 * Initialize the autovacuum subsystem (again, no process start yet)
	 */
	autovac_init();

	/*
	 * Load configuration files for client authentication.
	 */
	if (!load_hba())
	{
		/*
		 * It makes no sense to continue if we fail to load the HBA file,
		 * since there is no way to connect to the database in this case.
		 */
		ereport(FATAL,
				(errmsg("could not load pg_hba.conf")));
	}
	if (!load_ident())
	{
		/*
		 * We can start up without the IDENT file, although it means that you
		 * cannot log in using any of the authentication methods that need a
		 * user name mapping. load_ident() already logged the details of error
		 * to the log.
		 */
	}

#ifdef HAVE_PTHREAD_IS_THREADED_NP

	/*
	 * On macOS, libintl replaces setlocale() with a version that calls
	 * CFLocaleCopyCurrent() when its second argument is "" and every relevant
	 * environment variable is unset or empty.  CFLocaleCopyCurrent() makes
	 * the process multithreaded.  The postmaster calls sigprocmask() and
	 * calls fork() without an immediate exec(), both of which have undefined
	 * behavior in a multithreaded program.  A multithreaded postmaster is the
	 * normal case on Windows, which offers neither fork() nor sigprocmask().
	 */
	if (pthread_is_threaded_np() != 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("postmaster became multithreaded during startup"),
				 errhint("Set the LC_ALL environment variable to a valid locale.")));
#endif

	/*
	 * Remember postmaster startup time
	 */
	PgStartTime = GetCurrentTimestamp();
#ifndef HAVE_STRONG_RANDOM
	/* RandomCancelKey wants its own copy */
	gettimeofday(&random_start_time, NULL);
#endif

	/*
	 * Report postmaster status in the postmaster.pid file, to allow pg_ctl to
	 * see what's happening.
	 */
	AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING);

	if (POLAR_FILE_IN_SHARED_STORAGE())
	{
		elog(LOG, "polardb try start vfs process");
		maybe_start_bgworkers(true);
		polar_set_vfs_function_ready();
		polar_mount();
		elog(LOG, "polardb start vfs process success");

		/* POLAR: remove POLAR_REPLICA_BOOTED_FILE when not in replica node */
		if (!polar_in_replica_mode())
			polar_remove_replica_booted_file();

		/*
		 * POLAR: Initialize the local global directories for replica or standby
		 * copy pg_control file from shared storage to local.
		 * for replica, move the copy of base directories and other trans status log files to startup process
		 * so that postmaster can return and respond to the detection of cm timely
		 * for standby, only pg_control file is needed to copy
		 */
		polar_init_global_dir_for_replica_or_standby();

		/* POLAR: load control file after polar_init_global_dir_for_replica_or_standby */
		elog(LOG, "polardb load controlfile in postmaster");
		polar_load_and_check_controlfile();		
	}

	/* POLAR: must be called after polar_load_and_check_controlfile */
	polar_try_reuse_buffer_pool();

	/*
	 * Initialize cluster encryption key manager.
	 */
	InitializeKmgr();

	/* POLAR:Start flashback log background writer and inserter*/
	if (polar_is_flog_enabled(flog_instance))
		FLOG_START_BGWRITER_AND_INSERTER;

	/*
	 * We're ready to rock and roll...
	 */
	StartupPID = StartupDataBase();
	Assert(StartupPID != 0);
	StartupStatus = STARTUP_RUNNING;
	pmState = PM_STARTUP;
	polar_log_pmstate();

	/* Some workers may be scheduled to start now */
	maybe_start_bgworkers(false);

	/* POLAR: Shared Server */
	polar_dispatcher_start();
	/* POLAR end */

	status = ServerLoop();

	/*
	 * ServerLoop probably shouldn't ever return, but if it does, close down.
	 */
	ExitPostmaster(status != STATUS_OK);

	abort();					/* not reached */
}


/*
 * on_proc_exit callback to close server's listen sockets
 */
static void
CloseServerPorts(int status, Datum arg)
{
	int			i;

	/*
	 * First, explicitly close all the socket FDs.  We used to just let this
	 * happen implicitly at postmaster exit, but it's better to close them
	 * before we remove the postmaster.pid lockfile; otherwise there's a race
	 * condition if a new postmaster wants to re-use the TCP port number.
	 */
	for (i = 0; i < POLAR_MAXLISTEN; i++)
	{
		if (ListenSocket[i] != PGINVALID_SOCKET)
		{
			StreamClose(ListenSocket[i]);
			ListenSocket[i] = PGINVALID_SOCKET;
		}
	}

	/*
	 * Next, remove any filesystem entries for Unix sockets.  To avoid race
	 * conditions against incoming postmasters, this must happen after closing
	 * the sockets and before removing lock files.
	 */
	RemoveSocketFiles();

	/*
	 * We don't do anything about socket lock files here; those will be
	 * removed in a later on_proc_exit callback.
	 */
}

/*
 * on_proc_exit callback to delete external_pid_file
 */
static void
unlink_external_pid_file(int status, Datum arg)
{
	if (external_pid_file)
		unlink(external_pid_file);
}


/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
static void
getInstallationPaths(const char *argv0)
{
	DIR		   *pdir;

	/* Locate the postgres executable itself */
	if (find_my_exec(argv0, my_exec_path) < 0)
		elog(FATAL, "%s: could not locate my own executable path", argv0);

#ifdef EXEC_BACKEND
	/* Locate executable backend before we change working directory */
	if (find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR,
						postgres_exec_path) < 0)
		ereport(FATAL,
				(errmsg("%s: could not locate matching postgres executable",
						argv0)));
#endif

	/*
	 * Locate the pkglib directory --- this has to be set early in case we try
	 * to load any modules from it in response to postgresql.conf entries.
	 */
	get_pkglib_path(my_exec_path, pkglib_path);

	/*
	 * Verify that there's a readable directory there; otherwise the Postgres
	 * installation is incomplete or corrupt.  (A typical cause of this
	 * failure is that the postgres executable has been moved or hardlinked to
	 * some directory that's not a sibling of the installation lib/
	 * directory.)
	 */
	pdir = AllocateDir(pkglib_path, false);
	if (pdir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						pkglib_path),
				 errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been moved away from its proper location.",
						 my_exec_path)));
	FreeDir(pdir);

	/*
	 * XXX is it worth similarly checking the share/ directory?  If the lib/
	 * directory is there, then share/ probably is too.
	 */
}

/*
 * Check that pg_control exists in the correct location in the data directory.
 *
 * No attempt is made to validate the contents of pg_control here.  This is
 * just a sanity check to see if we are looking at a real data directory.
 */
static void
checkControlFile(void)
{
	char		path[MAXPGPATH];
	FILE	   *fp;

	snprintf(path, sizeof(path), "%s/global/pg_control", DataDir);

	/*
	 * POLAR: At this moment the shared storage is not mounted.
	 * check it later.
	 */
	if (POLAR_FILE_IN_SHARED_STORAGE())
		return;

	fp = AllocateFile(path, PG_BINARY_R);
	if (fp == NULL)
	{
		write_stderr("%s: could not find the database system\n"
					 "Expected to find it in the directory \"%s\",\n"
					 "but could not open file \"%s\": %s\n",
					 progname, DataDir, path, strerror(errno));
		ExitPostmaster(2);
	}
	FreeFile(fp);
}

/*
 * Determine how long should we let ServerLoop sleep.
 *
 * In normal conditions we wait at most one minute, to ensure that the other
 * background tasks handled by ServerLoop get done even when no requests are
 * arriving.  However, if there are background workers waiting to be started,
 * we don't actually sleep so that they are quickly serviced.  Other exception
 * cases are as shown in the code.
 */
static void
DetermineSleepTime(struct timeval *timeout)
{
	TimestampTz next_wakeup = 0;

	/*
	 * Normal case: either there are no background workers at all, or we're in
	 * a shutdown sequence (during which we ignore bgworkers altogether).
	 */
	if (Shutdown > NoShutdown ||
		(!StartWorkerNeeded && !HaveCrashedWorker))
	{
		if (AbortStartTime != 0)
		{
			/* time left to abort; clamp to 0 in case it already expired */
			timeout->tv_sec = SIGKILL_CHILDREN_AFTER_SECS -
				(time(NULL) - AbortStartTime);
			timeout->tv_sec = Max(timeout->tv_sec, 0);
			timeout->tv_usec = 0;
		}
		else
		{
			timeout->tv_sec = 60;
			timeout->tv_usec = 0;
		}
		return;
	}

	if (StartWorkerNeeded)
	{
		timeout->tv_sec = 0;
		timeout->tv_usec = 0;
		return;
	}

	if (HaveCrashedWorker)
	{
		slist_mutable_iter siter;

		/*
		 * When there are crashed bgworkers, we sleep just long enough that
		 * they are restarted when they request to be.  Scan the list to
		 * determine the minimum of all wakeup times according to most recent
		 * crash time and requested restart interval.
		 */
		slist_foreach_modify(siter, &BackgroundWorkerList)
		{
			RegisteredBgWorker *rw;
			TimestampTz this_wakeup;

			rw = slist_container(RegisteredBgWorker, rw_lnode, siter.cur);

			if (rw->rw_crashed_at == 0)
				continue;

			if (rw->rw_worker.bgw_restart_time == BGW_NEVER_RESTART
				|| rw->rw_terminate)
			{
				ForgetBackgroundWorker(&siter);
				continue;
			}

			this_wakeup = TimestampTzPlusMilliseconds(rw->rw_crashed_at,
													  1000L * rw->rw_worker.bgw_restart_time);
			if (next_wakeup == 0 || this_wakeup < next_wakeup)
				next_wakeup = this_wakeup;
		}
	}

	if (next_wakeup != 0)
	{
		long		secs;
		int			microsecs;

		TimestampDifference(GetCurrentTimestamp(), next_wakeup,
							&secs, &microsecs);
		timeout->tv_sec = secs;
		timeout->tv_usec = microsecs;

		/* Ensure we don't exceed one minute */
		if (timeout->tv_sec > 60)
		{
			timeout->tv_sec = 60;
			timeout->tv_usec = 0;
		}
	}
	else
	{
		timeout->tv_sec = 60;
		timeout->tv_usec = 0;
	}
}

/**
 * This function tries to estimate workload of proxy.
 * We have a lot of information about proxy state in polar_dispatcher_proc array:
 * total number of clients, SSL clients, backends, traffic, number of transactions,...
 * So in principle it is possible to implement much more sophisticated evaluation function,
 * but right now we take in account only number of clients and SSL connections (which requires much more CPU)
 */
static uint64
GetConnectionProxyWorkload(int id)
{
	return polar_dispatcher_proc[id].n_clients + polar_dispatcher_proc[id].n_ssl_clients * 3;
}

/**
 * Choose connection pool for this session.
 * Right now sessions can not be moved between pools (in principle it is not so difficult to implement it),
 * so to support order balancing we should do some smart work here.
 */
static PolarDispatcherProc*
polar_select_dispatcher(void)
{
	/* index used for round-robin distribution of connections between dispatchers */
	static int polar_current_dispatcher_index; 
	int i;
	uint64 min_workload;
	int least_loaded_proxy;

	if (polar_ss_dispatcher_count == 1)
		return &polar_dispatcher_proc[0];

	switch (polar_ss_client_schedule_policy)
	{
	  case CLIENT_SCHEDULE_ROUND_ROBIN:
		return &polar_dispatcher_proc[polar_current_dispatcher_index++ % polar_ss_dispatcher_count];

	  case CLIENT_SCHEDULE_RANDOM:
		return &polar_dispatcher_proc[random() % polar_ss_dispatcher_count];

	  case CLIENT_SCHEDULE_LOAD_BALANCING:
		min_workload = GetConnectionProxyWorkload(0);
		least_loaded_proxy = 0;
		for (i = 1; i < polar_ss_dispatcher_count; i++)
		{
			int workload = GetConnectionProxyWorkload(i);
			if (workload < min_workload)
			{
				min_workload = workload;
				least_loaded_proxy = i;
			}
		}
		return &polar_dispatcher_proc[least_loaded_proxy];

	  default:
		elog(ERROR, "invalid polar_ss_client_schedule_policy: %d", polar_ss_client_schedule_policy);
	}
	return NULL;
}


/*
 * Main idle loop of postmaster
 *
 * NB: Needs to be called with signals blocked
 */
static int
ServerLoop(void)
{
	fd_set		readmask, readmask_base;
	int			nSockets, nSockets_base = 0;
	time_t		last_lockfile_recheck_time,
				last_touch_time;

	last_lockfile_recheck_time = last_touch_time = time(NULL);

	nSockets = initMasks(&readmask);

	if (POLAR_SHARED_SERVER_RUNNING())
	{
		nSockets_base = nSockets;
		memcpy((char *) &readmask_base, (char *) &readmask, sizeof(fd_set));
	}

	for (;;)
	{
		fd_set		rmask;
		int			selres;
		time_t		now;

		if (POLAR_SHARED_SERVER_RUNNING() && dispatcher_had_restart)
		{
			dispatcher_had_restart = false;
			memcpy((char *) &readmask, (char *) &readmask_base, sizeof(fd_set));
			nSockets = polar_init_dispatcher_masks(&readmask, nSockets_base);
		}

		/*
		 * Wait for a connection request to arrive.
		 *
		 * We block all signals except while sleeping. That makes it safe for
		 * signal handlers, which again block all signals while executing, to
		 * do nontrivial work.
		 *
		 * If we are in PM_WAIT_DEAD_END state, then we don't want to accept
		 * any new connections, so we don't call select(), and just sleep.
		 */
		memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

		if (pmState == PM_WAIT_DEAD_END)
		{
			PG_SETMASK(&UnBlockSig);

			pg_usleep(100000L); /* 100 msec seems reasonable */
			selres = 0;

			PG_SETMASK(&BlockSig);
		}
		else
		{
			/* must set timeout each time; some OSes change it! */
			struct timeval timeout;

			/* Needs to run with blocked signals! */
			DetermineSleepTime(&timeout);

			PG_SETMASK(&UnBlockSig);

			selres = select(nSockets, &rmask, NULL, NULL, &timeout);

			PG_SETMASK(&BlockSig);
		}

		/* Now check the select() result */
		if (selres < 0)
		{
			if (errno != EINTR && errno != EWOULDBLOCK)
			{
				ereport(LOG,
						(errcode_for_socket_access(),
						 errmsg("select() failed in postmaster: %m")));
				return STATUS_ERROR;
			}
		}

		/*
		 * New connection pending on any of our sockets? If so, fork a child
		 * process to deal with it.
		 */
		if (selres > 0)
		{
			int			i;

			for (i = 0; i < MAXLISTEN; i++)
			{
				if (ListenSocket[i] == PGINVALID_SOCKET)
					break;
				if (FD_ISSET(ListenSocket[i], &rmask))
				{
					Port	   *port;

					port = ConnCreate(ListenSocket[i]);
					if (port)
					{
						if (POLAR_SHARED_SERVER_RUNNING())
						{
							PolarDispatcherProc *proc = polar_select_dispatcher();
							if (polar_pg_send_sock(proc->pipes[0], port->sock) < 0)
								elog(LOG, "could not send socket to connection pool: %m");
						}
						else
							BackendStartup(port);
						/*
						 * We no longer need the open socket or port structure
						 * in this process
						 */
						StreamClose(port->sock);
						ConnFree(port);
					}
				}
			}

			if (POLAR_SHARED_SERVER_RUNNING())
			{
				int			i;
				/* Shared Server. Check for data from dispatcher */
				for (i = 0; i < polar_ss_dispatcher_count; i++)
				{
					if (FD_ISSET(ListenSocket[MAXLISTEN + i], &rmask))
					{
						Port* port = polar_conn_create_from_dispatcher(ListenSocket[MAXLISTEN + i],
																	&polar_dispatcher_startup_buf,
																	&polar_dispatcher_startup_len);
						if (port)
						{
							polar_my_dispatcher_id = i;
							BackendStartup(port);

							/*
							* We no longer need the open socket or port structure
							* in this process
							*/
							StreamClose(port->sock);
							ConnFree(port);
						}
						if (polar_dispatcher_startup_buf)
						{
							free(polar_dispatcher_startup_buf);
							polar_dispatcher_startup_buf = NULL;
							polar_dispatcher_startup_len = 0;
						}
					}
				}
			}
		}

		/* If we have lost the log collector, try to start a new one */
		if (!polar_enable_multi_syslogger) 
		{
			if (SysLoggerPID == 0 && Logging_collector)
				SysLoggerPID = SysLogger_Start(0);
		} 
		else 
		{
			/* POLAR */
			if (Logging_collector) 
			{
				int i;
				for (i = 0; i < polar_num_of_sysloggers; i++) 
				{
					if (SysLoggerPIDs[i] == 0)
						SysLoggerPIDs[i] = SysLogger_Start(i);
				}
			}
			/* POLAR end */
		}

		/*
		 * If no background writer process is running, and we are not in a
		 * state that prevents it, start one.  It doesn't matter if this
		 * fails, we'll just try again later.  Likewise for the checkpointer.
		 */
		if (pmState == PM_RUN || pmState == PM_RECOVERY ||
			pmState == PM_HOT_STANDBY)
		{
			if (CheckpointerPID == 0)
				CheckpointerPID = StartCheckpointer();
			if (BgWriterPID == 0)
				BgWriterPID = StartBackgroundWriter();

			/* POLAR:start logindex background process */
			if (polar_logindex_redo_instance && LogIndexBgPID == 0)
				LogIndexBgPID = StartLogIndexBgWriter();

			/* POLAR:start flashback log background writer and inserter */
			if (polar_is_flog_enabled(flog_instance))
				FLOG_START_BGWRITER_AND_INSERTER;
		}
		/* POLAR: start flashback log background inserter and writer in startup */
		else if (pmState == PM_STARTUP && polar_is_flog_enabled(flog_instance))
			FLOG_START_BGWRITER_AND_INSERTER;

		/*
		 * Likewise, if we have lost the walwriter process, try to start a new
		 * one.  But this is needed only in normal operation (else we cannot
		 * be writing any new WAL).
		 */
		if (WalWriterPID == 0 && pmState == PM_RUN && !polar_in_replica_mode())
			WalWriterPID = StartWalWriter();

		/*
		 * If we have lost the autovacuum launcher, try to start a new one. We
		 * don't want autovacuum to run in binary upgrade mode because
		 * autovacuum might update relfrozenxid for empty tables before the
		 * physical files are put in place.
		 */
		if (!IsBinaryUpgrade && AutoVacPID == 0 &&
			(AutoVacuumingActive() || start_autovac_launcher) &&
			pmState == PM_RUN)
		{
			AutoVacPID = StartAutoVacLauncher();
			if (AutoVacPID != 0)
				start_autovac_launcher = false; /* signal processed */
		}

		/* If we have lost the stats collector, try to start a new one */
		if (PgStatPID == 0 &&
			(pmState == PM_RUN || pmState == PM_HOT_STANDBY))
			PgStatPID = pgstat_start();

		/* If we have lost the archiver, try to start a new one. */
		if (PgArchPID == 0 && PgArchStartupAllowed())
			PgArchPID = pgarch_start();

		/* If we need to signal the autovacuum launcher, do so now */
		if (avlauncher_needs_signal)
		{
			avlauncher_needs_signal = false;
			if (AutoVacPID != 0)
				kill(AutoVacPID, SIGUSR2);
		}

		/* If we need to start a WAL receiver, try to do that now */
		if (WalReceiverRequested)
			MaybeStartWalReceiver();

		if (pmState == PM_RUN)
			polar_dispatcher_start();

		/* Get other worker processes running, if needed */
		if (StartWorkerNeeded || HaveCrashedWorker)
			maybe_start_bgworkers(false);

#ifdef HAVE_PTHREAD_IS_THREADED_NP

		/*
		 * With assertions enabled, check regularly for appearance of
		 * additional threads.  All builds check at start and exit.
		 */
		Assert(pthread_is_threaded_np() == 0);
#endif

		/*
		 * Lastly, check to see if it's time to do some things that we don't
		 * want to do every single time through the loop, because they're a
		 * bit expensive.  Note that there's up to a minute of slop in when
		 * these tasks will be performed, since DetermineSleepTime() will let
		 * us sleep at most that long; except for SIGKILL timeout which has
		 * special-case logic there.
		 */
		now = time(NULL);

		/*
		 * If we already sent SIGQUIT to children and they are slow to shut
		 * down, it's time to send them SIGKILL.  This doesn't happen
		 * normally, but under certain conditions backends can get stuck while
		 * shutting down.  This is a last measure to get them unwedged.
		 *
		 * Note we also do this during recovery from a process crash.
		 */
		if ((Shutdown >= ImmediateShutdown || (FatalError && !SendStop)) &&
			AbortStartTime != 0 &&
			(now - AbortStartTime) >= SIGKILL_CHILDREN_AFTER_SECS)
		{
			/* We were gentle with them before. Not anymore */
			TerminateChildren(SIGKILL);
			/* reset flag so we don't SIGKILL again */
			AbortStartTime = 0;
		}

		/*
		 * Once a minute, verify that postmaster.pid hasn't been removed or
		 * overwritten.  If it has, we force a shutdown.  This avoids having
		 * postmasters and child processes hanging around after their database
		 * is gone, and maybe causing problems if a new database cluster is
		 * created in the same place.  It also provides some protection
		 * against a DBA foolishly removing postmaster.pid and manually
		 * starting a new postmaster.  Data corruption is likely to ensue from
		 * that anyway, but we can minimize the damage by aborting ASAP.
		 */
		if (now - last_lockfile_recheck_time >= 1 * SECS_PER_MINUTE)
		{
			if (!RecheckDataDirLockFile())
			{
				ereport(LOG,
						(errmsg("performing immediate shutdown because data directory lock file is invalid")));
				kill(MyProcPid, SIGQUIT);
			}
			last_lockfile_recheck_time = now;
		}

		/*
		 * Touch Unix socket and lock files every 58 minutes, to ensure that
		 * they are not removed by overzealous /tmp-cleaning tasks.  We assume
		 * no one runs cleaners with cutoff times of less than an hour ...
		 */
		if (now - last_touch_time >= 58 * SECS_PER_MINUTE)
		{
			TouchSocketFiles();
			TouchSocketLockFiles();
			last_touch_time = now;
		}
	}
}

/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
static int
initMasks(fd_set *rmask)
{
	int			maxsock = -1;
	int			i;

	FD_ZERO(rmask);

	for (i = 0; i < MAXLISTEN; i++)
	{
		int			fd = ListenSocket[i];

		if (fd == PGINVALID_SOCKET)
			break;
		FD_SET(fd, rmask);

		if (fd > maxsock)
			maxsock = fd;
	}

	return maxsock + 1;
}

/*
 * Shared Server. Init wait event set for dispatchers.
 */
static int
polar_init_dispatcher_masks(fd_set *rmask, int numSockets)
{
	int i;
	for (i = 0; i < polar_ss_dispatcher_count; i++)
    {
        PolarDispatcherProc  *proc = &polar_dispatcher_proc[i];

		FD_SET(proc->pipes[0], rmask);
		ListenSocket[MAXLISTEN + i] = proc->pipes[0];

        if (proc->pipes[0] > numSockets)
            numSockets = proc->pipes[0];
    }
    return numSockets + 1;
}


/*
 * Read a client's startup packet and do something according to it.
 *
 * Returns STATUS_OK or STATUS_ERROR, or might call ereport(FATAL) and
 * not return at all.
 *
 * (Note that ereport(FATAL) stuff is sent to the client, so only use it
 * if that's what you want.  Return STATUS_ERROR if you don't want to
 * send anything to the client, which would typically be appropriate
 * if we detect a communications failure.)
 */
static int
polar_get_startup_packet(bool SSLdone, char** polar_buf, int32* polar_len)
{
	int32		len;
	void	   *buf;

	pq_startmsgread();
	if (pq_getbytes((char *) &len, 4) == EOF)
	{
		/*
		 * EOF after SSLdone probably means the client didn't like our
		 * response to NEGOTIATE_SSL_CODE.  That's not an error condition, so
		 * don't clutter the log with a complaint.
		 */
		if (!SSLdone)
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete startup packet")));
		return STATUS_ERROR;
	}

	len = pg_ntoh32(len);
	len -= 4;

	if (len < (int32) sizeof(ProtocolVersion) ||
		len > MAX_STARTUP_PACKET_LENGTH)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid length of startup packet")));
		return STATUS_ERROR;
	}

	/*
	 * Allocate at least the size of an old-style startup packet, plus one
	 * extra byte, and make sure all are zeroes.  This ensures we will have
	 * null termination of all strings, in both fixed- and variable-length
	 * packet layouts.
	 */
	if (len <= (int32) sizeof(StartupPacket))
		buf = palloc0(sizeof(StartupPacket) + 1);
	else
		buf = palloc0(len + 1);

	*polar_buf = buf;
	*polar_len = len;

	if (pq_getbytes(buf, len) == EOF)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete startup packet")));
		return STATUS_ERROR;
	}
	pq_endmsgread();
	return STATUS_OK;
}

/* POLAR: Shared Server */
int
polar_parse_startup_packet(Port *port, MemoryContext memctx, void* buf, int len, bool SSLdone)
{
	ProtocolVersion proto;
	MemoryContext oldcontext;
    char       *pxid = NULL;

	am_walsender = false;
	am_db_walsender = false;
	am_px_worker = false;

	/*
	 * The first field is either a protocol version number or a special
	 * request code.
	 */
	port->proto = proto = pg_ntoh32(*((ProtocolVersion *) buf));

	if (proto == CANCEL_REQUEST_CODE)
	{
		processCancelRequest(port, buf);
		/* Not really an error, but we don't want to proceed further */
		return STATUS_ERROR;
	}

	/* POLAR */
	ereport(DEBUG2, (errmsg("ProcessStartupPacket, normal backend count=%d, max=%d",
		NormalBackendCount, MaxNormalBackends)));

	if (MaxNormalBackends > 0 &&
		NormalBackendCount > 0 &&
		NormalBackendCount >= MaxNormalBackends)
	{
		ereport(LOG,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("Sorry, too many connections already")));
		report_fork_failure_to_client(port, ERRCODE_TOO_MANY_CONNECTIONS);
		return STATUS_ERROR;
	}

	if (proto == NEGOTIATE_SSL_CODE && !SSLdone)
	{
		char		SSLok;
		char*       polar_buf = NULL;
		int32       polar_len = 0;
		int         polar_status;

#ifdef USE_SSL
		/* No SSL when disabled or on Unix sockets */
		if (!LoadedSSL || IS_AF_UNIX(port->laddr.addr.ss_family))
			SSLok = 'N';
		else
			SSLok = 'S';		/* Support for SSL */
#else
		SSLok = 'N';			/* No support for SSL */
#endif

retry1:
		if (send(port->sock, &SSLok, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(COMMERROR,
					(errcode_for_socket_access(),
					 errmsg("failed to send SSL negotiation response: %m")));
			return STATUS_ERROR;	/* close the connection */
		}

#ifdef USE_SSL
		if (SSLok == 'S' && secure_open_server(port) == -1)
			return STATUS_ERROR;
#endif
		/*
		 * At this point we should have no data already buffered.  If we do,
		 * it was received before we performed the SSL handshake, so it wasn't
		 * encrypted and indeed may have been injected by a man-in-the-middle.
		 * We report this case to the client.
		 */
		if (pq_buffer_has_data())
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("received unencrypted data after SSL request"),
					 errdetail("This could be either a client-software bug or evidence of an attempted man-in-the-middle attack.")));


		/* POLAR: Shared Server */
		polar_status = polar_get_startup_packet(true, &polar_buf, &polar_len);
		/* regular startup packet, cancel, etc packet should follow... */
		/* but not another SSL negotiation request */
		if (STATUS_OK == polar_status)
			polar_status = polar_parse_startup_packet(port, memctx, polar_buf, polar_len, true);

		if (polar_buf != NULL)
			pfree(polar_buf);
		return polar_status;
		/* POLAR: end */
	}

	/* Could add additional special packet types here */

	/*
	 * Set FrontendProtocol now so that ereport() knows what format to send if
	 * we fail during startup.
	 */
	FrontendProtocol = proto;

	/* Check that the major protocol version is in range. */
	if (PG_PROTOCOL_MAJOR(proto) < PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST) ||
		PG_PROTOCOL_MAJOR(proto) > PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST))
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported frontend protocol %u.%u: server supports %u.0 to %u.%u",
						PG_PROTOCOL_MAJOR(proto), PG_PROTOCOL_MINOR(proto),
						PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST),
						PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST),
						PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST))));

	/*
	 * Now fetch parameters out of startup packet and save them into the Port
	 * structure.  All data structures attached to the Port struct must be
	 * allocated in TopMemoryContext so that they will remain available in a
	 * running backend (even after PostmasterContext is destroyed).  We need
	 * not worry about leaking this storage on failure, since we aren't in the
	 * postmaster process anymore.
	 */
	oldcontext = MemoryContextSwitchTo(memctx);

	if (PG_PROTOCOL_MAJOR(proto) >= 3)
	{
		int32		offset = sizeof(ProtocolVersion);
		List	   *unrecognized_protocol_options = NIL;
		char 		*origin_host = NULL;
		char 		*origin_port = NULL;

		/*
		 * Scan packet body for name/option pairs.  We can assume any string
		 * beginning within the packet body is null-terminated, thanks to
		 * zeroing extra byte above.
		 */
		port->guc_options = NIL;

		while (offset < len)
		{
			char	   *nameptr = ((char *) buf) + offset;
			int32		valoffset;
			char	   *valptr;

			if (*nameptr == '\0')
				break;			/* found packet terminator */
			valoffset = offset + strlen(nameptr) + 1;
			if (valoffset >= len)
				break;			/* missing value, will complain below */
			valptr = ((char *) buf) + valoffset;

			if (strcmp(nameptr, "database") == 0)
				port->database_name = pstrdup(valptr);
			else if (strcmp(nameptr, "user") == 0)
				port->user_name = pstrdup(valptr);
			else if (strcmp(nameptr, "options") == 0)
				port->cmdline_options = pstrdup(valptr);
			/* POLAR px */
			else if (strcmp(nameptr, "pxid") == 0)
			{
				pxid = pstrdup(valptr);
				am_px_worker = true;
			}
			/* POLAR shared backend */
			else if (strcmp(nameptr, "replication") == 0)
			{
				/* POLAR: send only committed WAL defaultly if consistent replication is enabled */
				polar_dma_non_committed_walsender = !polar_dma_consistent_replication;

				/*
				 * Due to backward compatibility concerns the replication
				 * parameter is a hybrid beast which allows the value to be
				 * either boolean or the string 'database'. The latter
				 * connects to a specific database which is e.g. required for
				 * logical decoding while.
				 */
				if (strcmp(valptr, "database") == 0)
				{
					am_walsender = true;
					am_db_walsender = true;

					/* POLAR: send only committed WAL for logical replication if in DMA mode*/
					polar_dma_non_committed_walsender = false;
				}
				else if (strcmp(valptr, "dma") == 0)
				{
					am_walsender = true;
					polar_dma_non_committed_walsender = true;
					elog(DEBUG5, "Process DMA replication startup packet");
				}
				else if (!parse_bool(valptr, &am_walsender))
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									"replication",
									valptr),
							 errhint("Valid values are: \"false\", 0, \"true\", 1, \"database\".")));
			}
			else if (strncmp(nameptr, "_pq_.", 5) == 0)
			{
				/*
				 * Any option beginning with _pq_. is reserved for use as a
				 * protocol-level option, but at present no such options are
				 * defined.
				 */
				unrecognized_protocol_options =
					lappend(unrecognized_protocol_options, pstrdup(nameptr));
			}
			/* POLAR: Maxscale support fields */
			else if (strcmp(nameptr, "_polar_origin_client_ip") == 0 && polar_enable_maxscale_support)
			{
				origin_host = pstrdup(valptr);
			}
			else if (strcmp(nameptr, "_polar_origin_client_port") == 0 && polar_enable_maxscale_support)
			{
				origin_port = pstrdup(valptr);
			}
			else if (strcmp(nameptr, "_polar_send_lsn") == 0 && polar_enable_maxscale_support)
			{
				if (!parse_bool(valptr, &port->polar_send_lsn))
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									"_polar_send_lsn",
									valptr),
							 errhint("Valid values are: \"false\", 0, \"true\", 1.")));
			}
			else if (strcmp(nameptr, "_polar_send_xact") == 0 && polar_enable_maxscale_support)
			{
				if (!parse_bool(valptr, &port->polar_send_xact))
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									"_polar_send_xact",
									valptr),
							 errhint("Valid values are: \"false\", 0, \"true\", 1.")));
			}
			else if (strcmp(nameptr, "_polar_proxy_use_ssl") == 0 && polar_enable_maxscale_support)
			{
				if (!parse_bool(valptr, &port->polar_proxy_ssl_in_use))
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									"_polar_proxy_use_ssl",
									valptr),
							 errhint("Valid values are: \"false\", 0, \"true\", 1.")));
			}
			else if (strcmp(nameptr, "_polar_proxy_ssl_version") == 0 && polar_enable_maxscale_support)
				port->polar_proxy_ssl_version = pstrdup(valptr);
			else if (strcmp(nameptr, "_polar_proxy_ssl_cipher_name") == 0 && polar_enable_maxscale_support)
				port->polar_proxy_ssl_cipher_name = pstrdup(valptr);
			else if (strncmp(nameptr, "_polar_", 7) == 0 && polar_enable_maxscale_support)
			{
				/* POLAR: For compatibilioty, we reserve all options with _polar_ prefix. */
			}
			/* POLAR end */
			else
			{
				/* Assume it's a generic GUC option */
				port->guc_options = lappend(port->guc_options,
											pstrdup(nameptr));
				port->guc_options = lappend(port->guc_options,
											pstrdup(valptr));
			}
			offset = valoffset + strlen(valptr) + 1;
		}

		/* POLAR: Handle maxscale startup packet */
		if (origin_host == NULL || origin_port == NULL)
		{
			port->polar_proxy = false;
		}
		else if (!polar_enable_maxscale_support)
		{
			port->polar_proxy = false;
			ereport(LOG, (errmsg("[Maxscale] Maxscale support is disabled.")));
		}
		else
		{
			if (Log_connections)
				ereport(LOG,
						(errmsg("[Maxscale] Direct ip and port, host=%s, port=%s, encoded_host=%d, encoded_port=%d",
								port->remote_host, port->remote_port,
								(int)(((struct sockaddr_in *)&port->raddr.addr)->sin_addr.s_addr),
								(int)(((struct sockaddr_in *)&port->raddr.addr)->sin_port))));

			/* Origin connection information is ready */
			if (!polar_encode_origin_conn(origin_host, origin_port, &port->polar_origin_addr))
			{
				ereport(FATAL,
					(errmsg("[Maxscale] Construct origin address fail, origin_host=%s, origin_port=%s, proxy_host=%s, proxy_port=%s",
							origin_host, origin_port, port->remote_host, port->remote_port)));
			}
			else
			{
				char *proxy_host, *proxy_port;

				port->polar_proxy = true;
				proxy_host = port->remote_host;
				proxy_port = port->remote_port;
				port->remote_host = origin_host;
				port->remote_port = origin_port;

				if (Log_connections)
					ereport(LOG,
							(errmsg("[Maxscale] Connection from proxy, origin_host=%s, origin_port=%s, proxy_host=%s, proxy_port=%s",
									port->remote_host, port->remote_port, proxy_host, proxy_port)));

				/*
				 * TODO: we don't free space of origin remote_host and remote_port,
				 * but it's a small piece of process space
				 */
			}
			
			/* POLAR: Check ssl info completeness */
			if (port->polar_proxy_ssl_in_use &&
				(port->polar_proxy_ssl_version == NULL || port->polar_proxy_ssl_cipher_name == NULL))
			{
				ereport(LOG, (errmsg("[Maxscale] ssl info is not complete.")));
			}
		}
		/* POLAR end */

		/*
		 * If we didn't find a packet terminator exactly at the end of the
		 * given packet length, complain.
		 */
		if (offset != len - 1)
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid startup packet layout: expected terminator as last byte")));

		/*
		 * If the client requested a newer protocol version or if the client
		 * requested any protocol options we didn't recognize, let them know
		 * the newest minor protocol version we do support and the names of
		 * any unrecognized options.
		 */
		if (PG_PROTOCOL_MINOR(proto) > PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST) ||
			unrecognized_protocol_options != NIL)
			SendNegotiateProtocolVersion(unrecognized_protocol_options);
	}
	else
	{
		/*
		 * Get the parameters from the old-style, fixed-width-fields startup
		 * packet as C strings.  The packet destination was cleared first so a
		 * short packet has zeros silently added.  We have to be prepared to
		 * truncate the pstrdup result for oversize fields, though.
		 */
		StartupPacket *packet = (StartupPacket *) buf;

		port->database_name = pstrdup(packet->database);
		if (strlen(port->database_name) > sizeof(packet->database))
			port->database_name[sizeof(packet->database)] = '\0';
		port->user_name = pstrdup(packet->user);
		if (strlen(port->user_name) > sizeof(packet->user))
			port->user_name[sizeof(packet->user)] = '\0';
		port->cmdline_options = pstrdup(packet->options);
		if (strlen(port->cmdline_options) > sizeof(packet->options))
			port->cmdline_options[sizeof(packet->options)] = '\0';
		port->guc_options = NIL;
	}

	/* Check a user name was given. */
	if (port->user_name == NULL || port->user_name[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("no PostgreSQL user name specified in startup packet")));

	/* The database defaults to the user name. */
	if (port->database_name == NULL || port->database_name[0] == '\0')
		port->database_name = pstrdup(port->user_name);

	/* POLAR: Shared Server */
	port->polar_startup_gucs_hash = 0;
	if (port->cmdline_options != NULL)
		port->polar_startup_gucs_hash = polar_murmurhash2(port->cmdline_options, 
										strlen(port->cmdline_options), port->polar_startup_gucs_hash);

	elog(DEBUG1, "startup_gucs cmd: %u, %s", port->polar_startup_gucs_hash,
		port->cmdline_options ? port->cmdline_options : "null");

	if (port->guc_options != NULL)
	{
		ListCell   *gucopts = list_head(port->guc_options);
		while (gucopts)
		{
			char	   *name;
			char	   *value;

			name = lfirst(gucopts);
			gucopts = lnext(gucopts);

			value = lfirst(gucopts);
			gucopts = lnext(gucopts);

			port->polar_startup_gucs_hash = polar_murmurhash2(name, strlen(name), port->polar_startup_gucs_hash);
			port->polar_startup_gucs_hash = polar_murmurhash2(value, strlen(value), port->polar_startup_gucs_hash);

			elog(DEBUG1, "startup_gucs opt: %u, %s='%s'", port->polar_startup_gucs_hash, name, value);
		}
	}
	else
		elog(DEBUG1, "startup_gucs opt: null");

	if (Db_user_namespace)
	{
		/*
		 * If user@, it is a global user, remove '@'. We only want to do this
		 * if there is an '@' at the end and no earlier in the user string or
		 * they may fake as a local user of another database attaching to this
		 * database.
		 */
		if (strchr(port->user_name, '@') ==
			port->user_name + strlen(port->user_name) - 1)
			*strchr(port->user_name, '@') = '\0';
		else
		{
			/* Append '@' and dbname */
			port->user_name = psprintf("%s@%s", port->user_name, port->database_name);
		}
	}

	/*
	 * Truncate given database and user names to length of a Postgres name.
	 * This avoids lookup failures when overlength names are given.
	 */
	if (strlen(port->database_name) >= NAMEDATALEN)
		port->database_name[NAMEDATALEN - 1] = '\0';
	if (strlen(port->user_name) >= NAMEDATALEN)
		port->user_name[NAMEDATALEN - 1] = '\0';

	/*
	 * Normal walsender backends, e.g. for streaming replication, are not
	 * connected to a particular database. But walsenders used for logical
	 * replication need to connect to a specific database. We allow streaming
	 * replication commands to be issued even if connected to a database as it
	 * can make sense to first make a basebackup and then stream changes
	 * starting from that.
	 */
	if (am_walsender && !am_db_walsender)
		port->database_name[0] = '\0';

	/*
	 * PX: Process "pxid" parameter string for qExec startup.
	 */
	if (pxid)
		pxgang_parse_pxid_params(port, pxid);

	/*
	 * Done putting stuff in TopMemoryContext.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * If we're going to reject the connection due to database state, say so
	 * now instead of wasting cycles on an authentication exchange. (This also
	 * allows a pg_ping utility to be written.)
	 */
	switch (port->canAcceptConnections)
	{
		case CAC_STARTUP:
			/*
			 * POLAR: check if it's under STANDBY_SNAPSHOT_PENDING state. if so,
			 * we will report this state to client through error message.
			 */
			if (polar_get_hot_standby_state() != STANDBY_SNAPSHOT_PENDING)
				ereport(FATAL,
						(errcode(ERRCODE_CANNOT_CONNECT_NOW),
						errmsg("the database system is starting up")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_CANNOT_CONNECT_NOW),
						errmsg("the database system is starting up,"
							   "under STANDBY_SNAPSHOT_PENDING state.")));
			break;
		case CAC_SHUTDOWN:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errmsg("the database system is shutting down")));
			break;
		case CAC_RECOVERY:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errmsg("the database system is in recovery mode")));
			break;
		case CAC_TOOMANY:
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("sorry, too many clients already")));
			break;
		case CAC_WAITBACKUP:
			/* OK for now, will check in InitPostgres */
			break;
		case CAC_OK:
			break;
	}

	return STATUS_OK;
}

/*
 * Send a NegotiateProtocolVersion to the client.  This lets the client know
 * that they have requested a newer minor protocol version than we are able
 * to speak.  We'll speak the highest version we know about; the client can,
 * of course, abandon the connection if that's a problem.
 *
 * We also include in the response a list of protocol options we didn't
 * understand.  This allows clients to include optional parameters that might
 * be present either in newer protocol versions or third-party protocol
 * extensions without fear of having to reconnect if those options are not
 * understood, while at the same time making certain that the client is aware
 * of which options were actually accepted.
 */
static void
SendNegotiateProtocolVersion(List *unrecognized_protocol_options)
{
	StringInfoData buf;
	ListCell   *lc;

	pq_beginmessage(&buf, 'v'); /* NegotiateProtocolVersion */
	pq_sendint32(&buf, PG_PROTOCOL_LATEST);
	pq_sendint32(&buf, list_length(unrecognized_protocol_options));
	foreach(lc, unrecognized_protocol_options)
		pq_sendstring(&buf, lfirst(lc));
	pq_endmessage(&buf);

	/* no need to flush, some other message will follow */
}

/*
 * The client has sent a cancel request packet, not a normal
 * start-a-new-connection packet.  Perform the necessary processing.
 * Nothing is sent back to the client.
 */
void
processCancelRequest(Port *port, void *pkt)
{
	CancelRequestPacket *canc = (CancelRequestPacket *) pkt;
	int			backendPID;
	int32		cancelAuthCode;
	Backend    *bp;

#ifndef EXEC_BACKEND
	dlist_iter	iter;
#else
	int			i;
#endif

	backendPID = (int) pg_ntoh32(canc->backendPID);
	cancelAuthCode = (int32) pg_ntoh32(canc->cancelAuthCode);
	elog(DEBUG1, "PID %d(%d)in cancel request", backendPID, cancelAuthCode);
	/* POLAR: cancel request for virtual pid */
	if (POLAR_IS_VIRTUAL_PID(backendPID))
	{
		backendPID = polar_pgstat_get_real_pid(backendPID, cancelAuthCode, true, false);
		if (backendPID == 0)
		{
			/* No matching backend */
			ereport(LOG,
					(errmsg("PID %d in cancel request did not match any process",
							backendPID)));
			return;
		}
		else if (backendPID < 0)
		{
			/* Right PID, wrong key: no way, Jose */
			ereport(LOG,
					(errmsg("wrong key in cancel request for process %d",
							backendPID)));
			return;
		}
		else if (!POLAR_IS_SESSION_ID(backendPID))
		{
			/* Found a match; signal that backend to cancel current op */
			ereport(DEBUG2,
					(errmsg_internal("processing cancel request: sending SIGINT to process %d",
									backendPID)));
			signal_child(backendPID, SIGINT);
			return;
		}
	}

	if (POLAR_IS_SESSION_ID(backendPID))
	{
		if (!polar_send_signal(backendPID, &cancelAuthCode, SIGINT))
		{
			/* Right PID, wrong key: no way, Jose */
			ereport(WARNING,
					(errmsg("wrong key in cancel request for process %d",
							backendPID)));
		}
		else
		{
			ereport(LOG,
					(errmsg_internal("processing cancel request: process %d will killed",
										backendPID)));
		}
		return;
	}
	/* POLAR end */

	/*
	 * See if we have a matching backend.  In the EXEC_BACKEND case, we can no
	 * longer access the postmaster's own backend list, and must rely on the
	 * duplicate array in shared memory.
	 */
#ifndef EXEC_BACKEND
	dlist_foreach(iter, &BackendList)
	{
		bp = dlist_container(Backend, elem, iter.cur);
#else
	for (i = MaxLivePostmasterChildren() - 1; i >= 0; i--)
	{
		bp = (Backend *) &ShmemBackendArray[i];
#endif
		if (bp->pid == backendPID)
		{
			if (bp->cancel_key == cancelAuthCode)
			{
				/* Found a match; signal that backend to cancel current op */
				ereport(DEBUG2,
						(errmsg_internal("processing cancel request: sending SIGINT to process %d",
										 backendPID)));
				signal_child(bp->pid, SIGINT);
			}
			else
				/* Right PID, wrong key: no way, Jose */
				ereport(LOG,
						(errmsg("wrong key in cancel request for process %d",
								backendPID)));
			return;
		}
	}

	/* No matching backend */
	ereport(LOG,
			(errmsg("PID %d in cancel request did not match any process",
					backendPID)));
}

/*
 * canAcceptConnections --- check to see if database state allows connections
 * of the specified type.  backend_type can be BACKEND_TYPE_NORMAL,
 * BACKEND_TYPE_AUTOVAC, or BACKEND_TYPE_BGWORKER.  (Note that we don't yet
 * know whether a NORMAL connection might turn into a walsender.)
 */
static CAC_state
canAcceptConnections(int backend_type)
{
	CAC_state	result = CAC_OK;

	/*
	 * Can't start backends when in startup/shutdown/inconsistent recovery
	 * state.  We treat autovac workers the same as user backends for this
	 * purpose.  However, bgworkers are excluded from this test; we expect
	 * bgworker_should_start_now() decided whether the DB state allows them.
	 *
	 * In state PM_WAIT_BACKUP only superusers can connect (this must be
	 * allowed so that a superuser can end online backup mode); we return
	 * CAC_WAITBACKUP code to indicate that this must be checked later. Note
	 * that neither CAC_OK nor CAC_WAITBACKUP can safely be returned until we
	 * have checked for too many children.
	 */
	if (pmState != PM_RUN &&
		backend_type != BACKEND_TYPE_BGWORKER)
	{
		if (pmState == PM_WAIT_BACKUP)
			result = CAC_WAITBACKUP;	/* allow superusers only */
		else if (Shutdown > NoShutdown)
			return CAC_SHUTDOWN;	/* shutdown is pending */
		else if (!FatalError &&
				 (pmState == PM_STARTUP ||
				  pmState == PM_RECOVERY))
			return CAC_STARTUP; /* normal startup */
		else if (!FatalError &&
				 (pmState == PM_HOT_STANDBY ||
				 /* POLAR */
				 pmState == PM_DATAMAX))
				 /* POLAR end */
			result = CAC_OK;	/* connection OK during hot standby */
		else
			return CAC_RECOVERY;	/* else must be crash recovery */
	}

	/*
	 * Don't start too many children.
	 *
	 * We allow more connections here than we can have backends because some
	 * might still be authenticating; they might fail auth, or some existing
	 * backend might exit before the auth cycle is completed.  The exact
	 * MaxBackends limit is enforced when a new backend tries to join the
	 * shared-inval backend array.
	 *
	 * The limit here must match the sizes of the per-child-process arrays;
	 * see comments for MaxLivePostmasterChildren().
	 */
	if (CountChildren(BACKEND_TYPE_ALL, false) >= MaxLivePostmasterChildren())
		result = CAC_TOOMANY;

	return result;
}


/*
 * ConnCreate -- create a local connection data structure
 *
 * Returns NULL on failure, other than out-of-memory which is fatal.
 */
static Port *
ConnCreate(int serverFd)
{
	Port	   *port;

	if (!(port = (Port *) calloc(1, sizeof(Port))))
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		ExitPostmaster(1);
	}

	if (StreamConnection(serverFd, port, false) != STATUS_OK)
	{
		if (port->sock != PGINVALID_SOCKET)
			StreamClose(port->sock);
		ConnFree(port);
		return NULL;
	}

	/*
	 * Allocate GSSAPI specific state struct
	 */
#ifndef EXEC_BACKEND
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port->gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port->gss)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		ExitPostmaster(1);
	}
#endif
#endif

	return port;
}


/*
 * ConnFree -- free a local connection data structure
 */
static void
ConnFree(Port *conn)
{
#ifdef USE_SSL
	secure_close(conn);
#endif
	if (conn->gss)
		free(conn->gss);
	free(conn);
}

/* POLAR: Shared Server */
/* ConnCreate -- create a local connection data structure with */
static Port*
polar_conn_create_from_dispatcher(int serverFd, char** startup_buf, int32* startup_len)
{
	Port	             *port;
	pgsocket             backend_sock;
	int32                total;
	int32                offset;
	ssize_t              rc;

	backend_sock = polar_pg_recv_sock(serverFd);
	/* socketpair between postmaster an dispatcher should not have error. */
	Assert(backend_sock != PGINVALID_SOCKET);

	if (backend_sock == PGINVALID_SOCKET)
		goto io_error;

	/* get the startup len */
	for (offset = 0; offset < 4; offset += rc)
	{
		while ((rc = recv(serverFd, ((char*)&total) + offset, 4 - offset, 0)) < 0 && errno == EINTR);
		if (rc <= 0)
			goto io_error;
	}

	total = pg_ntoh32(total);
	total -= 4;
	*startup_len = total;

	*startup_buf = malloc(total);
	if (!*startup_buf)
		goto oom_error;

	/* get the total startup packet */
	for (offset = 0; offset < total; offset += rc)
	{
		while ((rc = recv(serverFd, *startup_buf + offset, total - offset, 0)) < 0 && errno == EINTR);
		if (rc <= 0)
			goto io_error;
	}

	if (!(port = (Port *) calloc(1, sizeof(Port))))
		goto oom_error;

	port->sock = backend_sock;
	if (StreamConnection(serverFd, port, true) != STATUS_OK)
	{
		StreamClose(port->sock);
		ConnFree(port);
		return NULL;
	}

	/*
	 * Allocate GSSAPI specific state struct
	 */
#ifndef EXEC_BACKEND
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port->gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port->gss)
		goto oom_error;
#endif
#endif
	return port;

 oom_error:
	ereport(LOG,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of memory")));
	ExitPostmaster(1);
	return NULL;

 io_error:
	Assert(false); /* socketpair between postmaster an dispatcher should not have error. */
	return NULL;
}

/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.  The postmaster still has
 * them open, of course.
 *
 * Note: we pass am_syslogger as a boolean because we don't want to set
 * the global variable yet when this is called.
 */
void
ClosePostmasterPorts(bool am_syslogger)
{
	int			i;

#ifndef WIN32

	/*
	 * Close the write end of postmaster death watch pipe. It's important to
	 * do this as early as possible, so that if postmaster dies, others won't
	 * think that it's still running because we're holding the pipe open.
	 */
	if (close(postmaster_alive_fds[POSTMASTER_FD_OWN]))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not close postmaster death monitoring pipe in child process: %m")));
	postmaster_alive_fds[POSTMASTER_FD_OWN] = -1;
#endif

	/* Close the listen sockets */
	for (i = 0; i < POLAR_MAXLISTEN; i++)
	{
		if (ListenSocket[i] != PGINVALID_SOCKET)
		{
			StreamClose(ListenSocket[i]);
			ListenSocket[i] = PGINVALID_SOCKET;
		}
	}

	/* If using syslogger, close the read side of the pipe */
	if (!am_syslogger)
	{
#ifndef WIN32
		if (!polar_enable_multi_syslogger)
		{
			if (syslogPipe[0] >= 0)
				close(syslogPipe[0]);
			syslogPipe[0] = -1;
		} 
		else 
		{
			/* POLAR */
			int i = 0;
			for (; i < polar_num_of_sysloggers; i++) {
				if (syslogChannels[i][0] >= 0)
					close(syslogChannels[i][0]);
				syslogChannels[i][0] = -1;
			}
			/* POLAR end */
		}
#else
		if (!polar_enable_multi_syslogger) 
		{
			if (syslogPipe[0])
				CloseHandle(syslogPipe[0]);
			syslogPipe[0] = 0;
		} 
		else 
		{
			int i = 0;
			for (; i < polar_num_of_sysloggers; i++) {
				if (syslogChannels[i][0])
					CloseHandle(syslogChannels[i][0])
				syslogChannels[i][0] = 0;
			}
		}
#endif
	}

#ifdef USE_BONJOUR
	/* If using Bonjour, close the connection to the mDNS daemon */
	if (bonjour_sdref)
		close(DNSServiceRefSockFD(bonjour_sdref));
#endif
}


/*
 * reset_shared -- reset shared memory and semaphores
 */
static void
reset_shared(int port)
{
	/*
	 * Create or re-create shared memory and semaphores.
	 *
	 * Note: in each "cycle of life" we will normally assign the same IPC keys
	 * (if using SysV shmem and/or semas), since the port number is used to
	 * determine IPC keys.  This helps ensure that we will clean up dead IPC
	 * objects if the postmaster crashes and is restarted.
	 */
	CreateSharedMemoryAndSemaphores(port);
}


/*
 * SIGHUP -- reread config files, and tell children to do same
 */
static void
SIGHUP_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	if (Shutdown <= SmartShutdown)
	{
		ereport(LOG,
				(errmsg("received SIGHUP, reloading configuration files")));
		ProcessConfigFile(PGC_SIGHUP);
		SignalChildren(SIGHUP);
		if (StartupPID != 0)
			signal_child(StartupPID, SIGHUP);
		if (BgWriterPID != 0)
			signal_child(BgWriterPID, SIGHUP);
		if (CheckpointerPID != 0)
			signal_child(CheckpointerPID, SIGHUP);
		if (WalWriterPID != 0)
			signal_child(WalWriterPID, SIGHUP);
		if (WalReceiverPID != 0)
			signal_child(WalReceiverPID, SIGHUP);
		if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
			signal_child(ConsensusPID, SIGHUP);
		if (AutoVacPID != 0)
			signal_child(AutoVacPID, SIGHUP);
		if (PgArchPID != 0)
			signal_child(PgArchPID, SIGHUP);
		if (LogIndexBgPID != 0)
			signal_child(LogIndexBgPID, SIGHUP);
		if (FlogBgWriterPID != 0)
			signal_child(FlogBgWriterPID, SIGHUP);
		if (FlogBgInserterPID != 0)
			signal_child(FlogBgInserterPID, SIGHUP);

		if (!polar_enable_multi_syslogger)
		{
			if (SysLoggerPID != 0)
				signal_child(SysLoggerPID, SIGHUP);
		} 
		else 
		{
			/* POLAR */
			int i = 0;
			for (; i < polar_num_of_sysloggers; i++)
			{
				if (SysLoggerPIDs[i] != 0)
					signal_child(SysLoggerPIDs[i], SIGHUP);
			}
			/* POLAR end */
		}
		if (PgStatPID != 0)
			signal_child(PgStatPID, SIGHUP);

		polar_dispatcher_handle_signal(SIGHUP);

		/* Reload authentication config files too */
		if (!load_hba())
			ereport(LOG,
					(errmsg("pg_hba.conf was not reloaded")));

		if (!load_ident())
			ereport(LOG,
					(errmsg("pg_ident.conf was not reloaded")));

#ifdef USE_SSL
		/* Reload SSL configuration as well */
		if (EnableSSL)
		{
			if (secure_initialize(false) == 0)
				LoadedSSL = true;
			else
				ereport(LOG,
						(errmsg("SSL configuration was not reloaded")));
		}
		else
		{
			secure_destroy();
			LoadedSSL = false;
		}
#endif

#ifdef EXEC_BACKEND
		/* Update the starting-point file for future children */
		write_nondefault_variables(PGC_SIGHUP);
#endif
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}


/*
 * pmdie -- signal handler for processing various postmaster signals.
 */
static void
pmdie(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	ereport(DEBUG2,
			(errmsg_internal("postmaster received signal %d",
							 postgres_signal_arg)));

	switch (postgres_signal_arg)
	{
		case SIGTERM:

			/*
			 * Smart Shutdown:
			 *
			 * Wait for children to end their work, then shut down.
			 */
			if (Shutdown >= SmartShutdown)
				break;
			Shutdown = SmartShutdown;
			ereport(LOG,
					(errmsg("received smart shutdown request")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			if (pmState == PM_RUN || pmState == PM_RECOVERY ||
				pmState == PM_HOT_STANDBY || pmState == PM_STARTUP ||
				/* POLAR */
				pmState == PM_DATAMAX)
				/* POLAR end */
			{
				/* autovac workers are told to shut down immediately */
				/* and bgworkers too; does this need tweaking? */
				SignalSomeChildren(SIGTERM,
								   BACKEND_TYPE_AUTOVAC | BACKEND_TYPE_BGWORKER, false);
				/* and the autovac launcher too */
				if (AutoVacPID != 0)
					signal_child(AutoVacPID, SIGTERM);
				/* and the bgwriter too */
				if (BgWriterPID != 0)
					signal_child(BgWriterPID, SIGTERM);
				/* and the walwriter too */
				if (WalWriterPID != 0)
					signal_child(WalWriterPID, SIGTERM);
				/* POLAR: and the logindex background process too */
				if (LogIndexBgPID != 0)
					signal_child(LogIndexBgPID, SIGTERM);
				/* and the polar_consensus too */
				if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
					signal_child(ConsensusPID, SIGINT);
				/* POLAR: and the flashback log background inserter process too */
				if (FlogBgInserterPID != 0)
					signal_child(FlogBgInserterPID, SIGTERM);
				/* POLAR: and the flashback log background writer process too */
				if (FlogBgWriterPID != 0)
					signal_child(FlogBgWriterPID, SIGTERM);
					
				polar_dispatcher_handle_signal(SIGTERM);

				/*
				 * If we're in recovery, we can't kill the startup process
				 * right away, because at present doing so does not release
				 * its locks.  We might want to change this in a future
				 * release.  For the time being, the PM_WAIT_READONLY state
				 * indicates that we're waiting for the regular (read only)
				 * backends to die off; once they do, we'll kill the startup
				 * and walreceiver processes.
				 */
				pmState = (pmState == PM_RUN) ?
					PM_WAIT_BACKUP : PM_WAIT_READONLY;
				polar_log_pmstate();
			}

			/*
			 * Now wait for online backup mode to end and backends to exit. If
			 * that is already the case, PostmasterStateMachine will take the
			 * next step.
			 */
			PostmasterStateMachine(PM_RECEIVE_SIGTERM, 0);
			break;

		case SIGINT:

			/*
			 * Fast Shutdown:
			 *
			 * Abort all children with SIGTERM (rollback active transactions
			 * and exit) and shut down when they are gone.
			 */
			if (Shutdown >= FastShutdown)
				break;
			Shutdown = FastShutdown;
			ereport(LOG,
					(errmsg("received fast shutdown request")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			if (StartupPID != 0)
				signal_child(StartupPID, SIGTERM);
			if (BgWriterPID != 0)
				signal_child(BgWriterPID, SIGTERM);
			if (WalReceiverPID != 0)
				signal_child(WalReceiverPID, SIGTERM);
			if (LogIndexBgPID != 0)
				signal_child(LogIndexBgPID, SIGTERM);
			if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
				signal_child(ConsensusPID, SIGINT);
			if (FlogBgInserterPID != 0)
				signal_child(FlogBgInserterPID, SIGTERM);
			if (FlogBgWriterPID != 0)
				signal_child(FlogBgWriterPID, SIGTERM);

			if (pmState == PM_STARTUP || pmState == PM_RECOVERY)
			{
				SignalSomeChildren(SIGTERM, BACKEND_TYPE_BGWORKER, false);

				/*
				 * Only startup, bgwriter, walreceiver, possibly bgworkers,
				 * and/or checkpointer should be active in this state; we just
				 * signaled the first four, and we don't want to kill
				 * checkpointer yet.
				 */
				pmState = PM_WAIT_BACKENDS;
				polar_log_pmstate();
			}
			else if (pmState == PM_RUN ||
					 pmState == PM_WAIT_BACKUP ||
					 pmState == PM_WAIT_READONLY ||
					 pmState == PM_WAIT_BACKENDS ||
					 pmState == PM_HOT_STANDBY ||
					 /* POLAR */
					 pmState == PM_DATAMAX)
					 /* POLAR end */
			{
				ereport(LOG,
						(errmsg("aborting any active transactions")));
				/* shut down all backends and workers */
				SignalSomeChildren(SIGTERM,
								   BACKEND_TYPE_NORMAL | BACKEND_TYPE_AUTOVAC |
								   BACKEND_TYPE_BGWORKER, false);
				/* and the autovac launcher too */
				if (AutoVacPID != 0)
					signal_child(AutoVacPID, SIGTERM);
				/* and the walwriter too */
				if (WalWriterPID != 0)
					signal_child(WalWriterPID, SIGTERM);

				polar_dispatcher_handle_signal(SIGTERM);

				pmState = PM_WAIT_BACKENDS;
				polar_log_pmstate();
			}

			/*
			 * Now wait for backends to exit.  If there are none,
			 * PostmasterStateMachine will take the next step.
			 */
			PostmasterStateMachine(PM_RECEIVE_SIGINT, 0);
			break;

		case SIGQUIT:

			/*
			 * Immediate Shutdown:
			 *
			 * abort all children with SIGQUIT, wait for them to exit,
			 * terminate remaining ones with SIGKILL, then exit without
			 * attempt to properly shut down the data base system.
			 */
			if (Shutdown >= ImmediateShutdown)
				break;
			Shutdown = ImmediateShutdown;
			ereport(LOG,
					(errmsg("received immediate shutdown request")));

			/* POLAR: immediate shutdown not do pfs_umount */
			polar_in_error_handling_process = true;

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			TerminateChildren(SIGQUIT);
			polar_dispatcher_handle_signal(SIGQUIT);

			pmState = PM_WAIT_BACKENDS;
			polar_log_pmstate();

			/* set stopwatch for them to die */
			AbortStartTime = time(NULL);

			/*
			 * Now wait for backends to exit.  If there are none,
			 * PostmasterStateMachine will take the next step.
			 */
			PostmasterStateMachine(PM_RECEIVE_SIGQUIT, 0);
			break;
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}

/*
 * Reaper -- signal handler to cleanup after a child process dies.
 */
static void
reaper(SIGNAL_ARGS)
{
	int			save_errno = errno;
	int			pid;			/* process id of dead child process */
	int			exitstatus;		/* its exit status */
	int			polar_pid_copy = 0;

	PG_SETMASK(&BlockSig);

	ereport(DEBUG4,
			(errmsg_internal("reaping dead processes")));

	while ((pid = waitpid(-1, &exitstatus, WNOHANG)) > 0)
	{
		polar_pid_copy = pid;

		ereport(DEBUG4,
				(errmsg_internal("reaping dead processes %d", pid)));

		/*
		 * Check if this child was a startup process.
		 */
		if (pid == StartupPID)
		{
			StartupPID = 0;

			/*
			 * Startup process exited in response to a shutdown request (or it
			 * completed normally regardless of the shutdown request).
			 */
			if (Shutdown > NoShutdown &&
				(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
			{
				StartupStatus = STARTUP_NOT_RUNNING;
				pmState = PM_WAIT_BACKENDS;
				polar_log_pmstate();
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			if (EXIT_STATUS_3(exitstatus))
			{
				ereport(LOG,
						(errmsg("shutdown at recovery target")));
				StartupStatus = STARTUP_NOT_RUNNING;
				Shutdown = SmartShutdown;
				TerminateChildren(SIGTERM);
				pmState = PM_WAIT_BACKENDS;
				polar_log_pmstate();
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			/*
			 * Unexpected exit of startup process (including FATAL exit)
			 * during PM_STARTUP is treated as catastrophic. There are no
			 * other processes running yet, so we can just exit.
			 */
			if (pmState == PM_STARTUP && !EXIT_STATUS_0(exitstatus))
			{
				LogChildExit(LOG, _("startup process"),
							 pid, exitstatus);
				ereport(LOG,
						(errmsg("aborting startup due to startup process failure")));
				ExitPostmaster(1);
			}

			/*
			 * After PM_STARTUP, any unexpected exit (including FATAL exit) of
			 * the startup process is catastrophic, so kill other children,
			 * and set StartupStatus so we don't try to reinitialize after
			 * they're gone.  Exception: if StartupStatus is STARTUP_SIGNALED,
			 * then we previously sent the startup process a SIGQUIT; so
			 * that's probably the reason it died, and we do want to try to
			 * restart in that case.
			 */
			if (!EXIT_STATUS_0(exitstatus))
			{
				if (StartupStatus == STARTUP_SIGNALED)
					StartupStatus = STARTUP_NOT_RUNNING;
				else
					StartupStatus = STARTUP_CRASHED;
				HandleChildCrash(pid, exitstatus,
								 _("startup process"));
				continue;
			}

			/*
			 * Startup succeeded, commence normal operations
			 */
			StartupStatus = STARTUP_NOT_RUNNING;
			FatalError = false;
			Assert(AbortStartTime == 0);
			ReachedNormalRunning = true;
			pmState = PM_RUN;
			polar_log_pmstate();

			/* 
			 * Must startup pipeliner before checkpointer,
			 * because only wal pipeliner can write wal log.
			 */
			if (POLAR_WAL_PIPELINER_ENABLE() && PolarWalPipelinerPID == 0 && !polar_in_replica_mode())
			{
				PolarWalPipelinerPID = StartPolarWalPipeliner();
				/* wait untill wal pipeliner ready */
				while (ProcGlobal->polar_wal_pipeliner_latch == NULL)
					SPIN_DELAY();	
			}

			/*
			 * Crank up the background tasks, if we didn't do that already
			 * when we entered consistent recovery state.  It doesn't matter
			 * if this fails, we'll just try again later.
			 */
			if (CheckpointerPID == 0)
				CheckpointerPID = StartCheckpointer();
			if (BgWriterPID == 0)
				BgWriterPID = StartBackgroundWriter();
			if (WalWriterPID == 0 && !polar_in_replica_mode())
				WalWriterPID = StartWalWriter();
			/* POLAR: start logindex background process */
			if (polar_logindex_redo_instance && LogIndexBgPID == 0)
				LogIndexBgPID = StartLogIndexBgWriter();
			/* POLAR: start flashback log background inserter and writer */
			if (polar_is_flog_enabled(flog_instance))
				FLOG_START_BGWRITER_AND_INSERTER;

			/*
			 * Likewise, start other special children as needed.  In a restart
			 * situation, some of them may be alive already.
			 */
			if (!IsBinaryUpgrade && AutoVacuumingActive() && AutoVacPID == 0)
				AutoVacPID = StartAutoVacLauncher();
			if (PgArchStartupAllowed() && PgArchPID == 0)
				PgArchPID = pgarch_start();
			if (PgStatPID == 0)
				PgStatPID = pgstat_start();

			polar_dispatcher_start();

			/* workers may be scheduled to start now */
			maybe_start_bgworkers(false);

			/* at this point we are really open for business */
			ereport(LOG,
					(errmsg("database system is ready to accept connections")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY);
#ifdef USE_SYSTEMD
			sd_notify(0, "READY=1");
#endif

			continue;
		}

		/*
		 * Was it the bgwriter?  Normal exit can be ignored; we'll start a new
		 * one at the next iteration of the postmaster's main loop, if
		 * necessary.  Any other exit condition is treated as a crash.
		 */
		if (pid == BgWriterPID)
		{
			BgWriterPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("background writer process"));
			continue;
		}

		/*
		 * Was it the checkpointer?
		 */
		if (pid == CheckpointerPID)
		{
			CheckpointerPID = 0;
			if (EXIT_STATUS_0(exitstatus) && pmState == PM_SHUTDOWN)
			{
				/*
				 * OK, we saw normal exit of the checkpointer after it's been
				 * told to shut down.  We expect that it wrote a shutdown
				 * checkpoint.  (If for some reason it didn't, recovery will
				 * occur on next postmaster start.)
				 *
				 * At this point we should have no normal backend children
				 * left (else we'd not be in PM_SHUTDOWN state) but we might
				 * have dead_end children to wait for.
				 *
				 * If we have an archiver subprocess, tell it to do a last
				 * archive cycle and quit. Likewise, if we have walsender
				 * processes, tell them to send any remaining WAL and quit.
				 */
				Assert(Shutdown > NoShutdown);

				/* Waken archiver for the last time */
				if (PgArchPID != 0)
					signal_child(PgArchPID, SIGUSR2);

				/*
				 * Waken walsenders for the last time. No regular backends
				 * should be around anymore.
				 *
				 * POLAR: skip polar_vfs process.
				 */
				SignalSomeChildren(SIGUSR2, BACKEND_TYPE_ALL, true);

				pmState = PM_SHUTDOWN_2;
				polar_log_pmstate();

				if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
					signal_child(ConsensusPID, SIGTERM);

				if (PolarWalPipelinerPID != 0)
					signal_child(PolarWalPipelinerPID, SIGUSR2);
				/*
				 * We can also shut down the stats collector now; there's
				 * nothing left for it to do.
				 */
				if (PgStatPID != 0)
					signal_child(PgStatPID, SIGQUIT);
			}
			else
			{
				/*
				 * Any unexpected exit of the checkpointer (including FATAL
				 * exit) is treated as a crash.
				 */
				HandleChildCrash(pid, exitstatus,
								 _("checkpointer process"));
			}

			continue;
		}

		/*
		 * Was it the wal writer?  Normal exit can be ignored; we'll start a
		 * new one at the next iteration of the postmaster's main loop, if
		 * necessary.  Any other exit condition is treated as a crash.
		 */
		if (pid == WalWriterPID)
		{
			WalWriterPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("WAL writer process"));
			continue;
		}

		if (pid == PolarWalPipelinerPID)
		{
			PolarWalPipelinerPID = 0;
			HandleChildCrash(pid, exitstatus,
						_("WAL pipeliner process"));
			continue;
		}

		/*
		 * Was it the wal receiver?  If exit status is zero (normal) or one
		 * (FATAL exit), we assume everything is all right just like normal
		 * backends.  (If we need a new wal receiver, we'll start one at the
		 * next iteration of the postmaster's main loop.)
		 */
		if (pid == WalReceiverPID)
		{
			WalReceiverPID = 0;
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("WAL receiver process"));
			continue;
		}

		/*
		 * Was it the autovacuum launcher?	Normal exit can be ignored; we'll
		 * start a new one at the next iteration of the postmaster's main
		 * loop, if necessary.  Any other exit condition is treated as a
		 * crash.
		 */
		if (pid == AutoVacPID)
		{
			AutoVacPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("autovacuum launcher process"));
			continue;
		}

		if (POLAR_SHARED_SERVER_RUNNING()) 
		{
			int i = 0;
			for (; i < polar_ss_dispatcher_count; i++) 
			{
				if (pid == polar_dispatcher_proc[i].pid) 
				{
					polar_dispatcher_proc[i].pid = 0;

					if (!EXIT_STATUS_0(exitstatus))
						HandleChildCrash(pid, exitstatus,
								 _("polar dispatcher process"));
					break;
				}
			}
			if (i < polar_ss_dispatcher_count)
				continue;
		}


		/*
		 * Was it the archiver?  If so, just try to start a new one; no need
		 * to force reset of the rest of the system.  (If fail, we'll try
		 * again in future cycles of the main loop.).  Unless we were waiting
		 * for it to shut down; don't restart it in that case, and
		 * PostmasterStateMachine() will advance to the next shutdown step.
		 */
		if (pid == PgArchPID)
		{
			PgArchPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("archiver process"),
							 pid, exitstatus);
			if (PgArchStartupAllowed())
				PgArchPID = pgarch_start();
			continue;
		}

		/*
		 * Was it the statistics collector?  If so, just try to start a new
		 * one; no need to force reset of the rest of the system.  (If fail,
		 * we'll try again in future cycles of the main loop.)
		 */
		if (pid == PgStatPID)
		{
			PgStatPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("statistics collector process"),
							 pid, exitstatus);
			if (pmState == PM_RUN || pmState == PM_HOT_STANDBY)
				PgStatPID = pgstat_start();
			continue;
		}

		/* Was it the system logger?  If so, try to start a new one */
		if (!polar_enable_multi_syslogger) 
		{
			if (pid == SysLoggerPID)
			{
				SysLoggerPID = 0;
				/* for safety's sake, launch new logger *first* */
				SysLoggerPID = SysLogger_Start(0);
				if (!EXIT_STATUS_0(exitstatus))
					LogChildExit(LOG, _("system logger process"),
								 pid, exitstatus);
				continue;
			}
		} 
		else 
		{
			/* POLAR */
			int i = 0;
			for (; i < polar_num_of_sysloggers; i++) 
			{
				if (pid == SysLoggerPIDs[i]) 
				{
					SysLoggerPIDs[i] = SysLogger_Start(i);
					if (!EXIT_STATUS_0(exitstatus))
						LogChildExit(LOG, _("system logger process"),
									 pid, exitstatus);
					break;
				}
			}
			if (i < polar_num_of_sysloggers)
				continue;
			/* POLAR end */
		}

		/*
		 * Any unexpected exit of the consensus (including FATAL
		 * exit) is treated as a crash.
		 */
		if (POLAR_ENABLE_DMA() && pid == ConsensusPID)
		{
			ConsensusPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus, _("consensus process"));
			continue;
		}

		if (pid == LogIndexBgPID)
		{
			LogIndexBgPID = 0;

			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus, _("LogIndex background process"));
			continue;
		}

		if (pid == FlogBgInserterPID)
		{
			FlogBgInserterPID = 0;

			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus, _("Flashback log background writer process"));
			continue;
		}

		if (pid == FlogBgWriterPID)
		{
			FlogBgWriterPID = 0;

			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus, _("Flashback log background inserter process"));
			continue;
		}

		/* Was it one of our background workers? */
		if (CleanupBackgroundWorker(pid, exitstatus))
		{
			/* have it be restarted */
			HaveCrashedWorker = true;
			continue;
		}

		/*
		 * Else do standard backend child cleanup.
		 */
		CleanupBackend(pid, exitstatus);
	}							/* loop over pending child-death reports */

	/*
	 * After cleaning out the SIGCHLD queue, see if we have any state changes
	 * or actions to make.
	 */
	PostmasterStateMachine(PM_SUBPROCESS_DIE, polar_pid_copy);

	/* Done with signal handler */
	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}

/*
 * Scan the bgworkers list and see if the given PID (which has just stopped
 * or crashed) is in it.  Handle its shutdown if so, and return true.  If not a
 * bgworker, return false.
 *
 * This is heavily based on CleanupBackend.  One important difference is that
 * we don't know yet that the dying process is a bgworker, so we must be silent
 * until we're sure it is.
 */
static bool
CleanupBackgroundWorker(int pid,
						int exitstatus) /* child's exit status */
{
	char		namebuf[MAXPGPATH];
	slist_mutable_iter iter;

	slist_foreach_modify(iter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);

		if (rw->rw_pid != pid)
			continue;

#ifdef WIN32
		/* see CleanupBackend */
		if (exitstatus == ERROR_WAIT_NO_CHILDREN)
			exitstatus = 0;
#endif

		snprintf(namebuf, MAXPGPATH, _("background worker \"%s\""),
				 rw->rw_worker.bgw_type);


		if (!EXIT_STATUS_0(exitstatus))
		{
			/* Record timestamp, so we know when to restart the worker. */
			rw->rw_crashed_at = GetCurrentTimestamp();
		}
		else
		{
			/* Zero exit status means terminate */
			rw->rw_crashed_at = 0;
			rw->rw_terminate = true;
		}

		/*
		 * Additionally, for shared-memory-connected workers, just like a
		 * backend, any exit status other than 0 or 1 is considered a crash
		 * and causes a system-wide restart.
		 */
		if ((rw->rw_worker.bgw_flags & BGWORKER_SHMEM_ACCESS) != 0)
		{
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
			{
				HandleChildCrash(pid, exitstatus, namebuf);
				return true;
			}
		}

		/*
		 * We must release the postmaster child slot whether this worker is
		 * connected to shared memory or not, but we only treat it as a crash
		 * if it is in fact connected.
		 */
		if (!ReleasePostmasterChildSlot(rw->rw_child_slot) &&
			(rw->rw_worker.bgw_flags & BGWORKER_SHMEM_ACCESS) != 0)
		{
			HandleChildCrash(pid, exitstatus, namebuf);
			return true;
		}

		/* Get it out of the BackendList and clear out remaining data */
		dlist_delete(&rw->rw_backend->elem);
#ifdef EXEC_BACKEND
		ShmemBackendArrayRemove(rw->rw_backend);
#endif

		/*
		 * It's possible that this background worker started some OTHER
		 * background worker and asked to be notified when that worker started
		 * or stopped.  If so, cancel any notifications destined for the
		 * now-dead backend.
		 */
		if (rw->rw_backend->bgworker_notify)
			BackgroundWorkerStopNotifications(rw->rw_pid);
		free(rw->rw_backend);
		rw->rw_backend = NULL;
		rw->rw_pid = 0;
		rw->rw_child_slot = 0;
		ReportBackgroundWorkerExit(&iter);	/* report child death */

		LogChildExit(EXIT_STATUS_0(exitstatus) ? DEBUG1 : LOG,
					 namebuf, pid, exitstatus);

		return true;
	}

	return false;
}

/*
 * CleanupBackend -- cleanup after terminated backend.
 *
 * Remove all local state associated with backend.
 *
 * If you change this, see also CleanupBackgroundWorker.
 */
static void
CleanupBackend(int pid,
			   int exitstatus)	/* child's exit status. */
{
	dlist_mutable_iter iter;

	LogChildExit(DEBUG2, _("server process"), pid, exitstatus);

	/*
	 * If a backend dies in an ugly way then we must signal all other backends
	 * to quickdie.  If exit status is zero (normal) or one (FATAL exit), we
	 * assume everything is all right and proceed to remove the backend from
	 * the active backend list.
	 */

#ifdef WIN32

	/*
	 * On win32, also treat ERROR_WAIT_NO_CHILDREN (128) as nonfatal case,
	 * since that sometimes happens under load when the process fails to start
	 * properly (long before it starts using shared memory). Microsoft reports
	 * it is related to mutex failure:
	 * http://archives.postgresql.org/pgsql-hackers/2010-09/msg00790.php
	 */
	if (exitstatus == ERROR_WAIT_NO_CHILDREN)
	{
		LogChildExit(LOG, _("server process"), pid, exitstatus);
		exitstatus = 0;
	}
#endif

	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
	{
		HandleChildCrash(pid, exitstatus, _("server process"));
		return;
	}

	dlist_foreach_modify(iter, &BackendList)
	{
		Backend    *bp = dlist_container(Backend, elem, iter.cur);

		if (bp->pid == pid)
		{
			if (!bp->dead_end)
			{
				if (!ReleasePostmasterChildSlot(bp->child_slot))
				{
					/*
					 * Uh-oh, the child failed to clean itself up.  Treat as a
					 * crash after all.
					 */
					HandleChildCrash(pid, exitstatus, _("server process"));
					return;
				}
#ifdef EXEC_BACKEND
				ShmemBackendArrayRemove(bp);
#endif
			}
			if (bp->bgworker_notify)
			{
				/*
				 * This backend may have been slated to receive SIGUSR1 when
				 * some background worker started or stopped.  Cancel those
				 * notifications, as we don't want to signal PIDs that are not
				 * PostgreSQL backends.  This gets skipped in the (probably
				 * very common) case where the backend has never requested any
				 * such notifications.
				 */
				BackgroundWorkerStopNotifications(bp->pid);
			}
			dlist_delete(iter.cur);

			/* POLAR */
			NormalBackendCount -= (BACKEND_TYPE_NORMAL == bp->bkend_type ? 1 : 0);

			free(bp);
			break;
		}
	}
}

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, checkpointer,
 * walwriter, autovacuum, or background worker.
 *
 * The objectives here are to clean up our local state about the child
 * process, and to signal all other remaining children to quickdie.
 */
static void
HandleChildCrash(int pid, int exitstatus, const char *procname)
{
	dlist_mutable_iter iter;
	slist_iter	siter;
	Backend    *bp;
	bool		take_action;

	/*
	 * We only log messages and send signals if this is the first process
	 * crash and we're not doing an immediate shutdown; otherwise, we're only
	 * here to update postmaster's idea of live processes.  If we have already
	 * signalled children, nonzero exit status is to be expected, so don't
	 * clutter log.
	 */
	take_action = !FatalError && Shutdown != ImmediateShutdown;

	if (take_action)
	{
		LogChildExit(LOG, procname, pid, exitstatus);
		ereport(LOG,
				(errmsg("terminating any other active server processes")));
	}

	/* Process background workers. */
	slist_foreach(siter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = slist_container(RegisteredBgWorker, rw_lnode, siter.cur);
		if (rw->rw_pid == 0)
			continue;			/* not running */
		if (rw->rw_pid == pid)
		{
			/*
			 * Found entry for freshly-dead worker, so remove it.
			 */
			(void) ReleasePostmasterChildSlot(rw->rw_child_slot);
			dlist_delete(&rw->rw_backend->elem);
#ifdef EXEC_BACKEND
			ShmemBackendArrayRemove(rw->rw_backend);
#endif
			free(rw->rw_backend);
			rw->rw_backend = NULL;
			rw->rw_pid = 0;
			rw->rw_child_slot = 0;
			/* don't reset crashed_at */
			/* don't report child stop, either */
			/* Keep looping so we can signal remaining workers */
		}
		else
		{
			/*
			 * This worker is still alive.  Unless we did so already, tell it
			 * to commit hara-kiri.
			 *
			 * SIGQUIT is the special signal that says exit without proc_exit
			 * and let the user know what's going on. But if SendStop is set
			 * (-T on command line or set guc parameter polar_enable_send_stop), then we send SIGSTOP instead, so that we
			 * can get core dumps from all backends by hand.
			 */
			if (take_action)
			{
				ereport(DEBUG2,
						(errmsg_internal("sending %s to process %d",
										 (SendStop ? "SIGSTOP" : "SIGQUIT"),
										 (int) rw->rw_pid)));
				signal_child(rw->rw_pid, (SendStop ? SIGSTOP : SIGQUIT));
			}
		}
	}

	/* Process regular backends */
	dlist_foreach_modify(iter, &BackendList)
	{
		bp = dlist_container(Backend, elem, iter.cur);

		if (bp->pid == pid)
		{
			/*
			 * Found entry for freshly-dead backend, so remove it.
			 */
			if (!bp->dead_end)
			{
				(void) ReleasePostmasterChildSlot(bp->child_slot);
#ifdef EXEC_BACKEND
				ShmemBackendArrayRemove(bp);
#endif
			}
			dlist_delete(iter.cur);

			/* POLAR */
			NormalBackendCount -= (BACKEND_TYPE_NORMAL == bp->bkend_type ? 1 : 0);

			free(bp);
			/* Keep looping so we can signal remaining backends */
		}
		else
		{
			/*
			 * This backend is still alive.  Unless we did so already, tell it
			 * to commit hara-kiri.
			 *
			 * SIGQUIT is the special signal that says exit without proc_exit
			 * and let the user know what's going on. But if SendStop is set
			 * (-T on command line or set guc parameter polar_enable_send_stop), then we send SIGSTOP instead, so that we
			 * can get core dumps from all backends by hand.
			 *
			 * We could exclude dead_end children here, but at least in the
			 * SIGSTOP case it seems better to include them.
			 *
			 * Background workers were already processed above; ignore them
			 * here.
			 */
			if (bp->bkend_type == BACKEND_TYPE_BGWORKER)
				continue;

			if (take_action)
			{
				ereport(DEBUG2,
						(errmsg_internal("sending %s to process %d",
										 (SendStop ? "SIGSTOP" : "SIGQUIT"),
										 (int) bp->pid)));
				signal_child(bp->pid, (SendStop ? SIGSTOP : SIGQUIT));
			}
		}
	}

	/* Take care of the startup process too */
	if (pid == StartupPID)
	{
		StartupPID = 0;
		StartupStatus = STARTUP_CRASHED;
	}
	else if (StartupPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) StartupPID)));
		signal_child(StartupPID, (SendStop ? SIGSTOP : SIGQUIT));
		StartupStatus = STARTUP_SIGNALED;
	}

	/* Take care of the bgwriter too */
	if (pid == BgWriterPID)
		BgWriterPID = 0;
	else if (BgWriterPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) BgWriterPID)));
		signal_child(BgWriterPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the checkpointer too */
	if (pid == CheckpointerPID)
		CheckpointerPID = 0;
	else if (CheckpointerPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) CheckpointerPID)));
		signal_child(CheckpointerPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the walwriter too */
	if (pid == WalWriterPID)
		WalWriterPID = 0;
	else if (WalWriterPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) WalWriterPID)));
		signal_child(WalWriterPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	if (pid == PolarWalPipelinerPID)
		PolarWalPipelinerPID = 0;
	else if (PolarWalPipelinerPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) PolarWalPipelinerPID)));
		signal_child(PolarWalPipelinerPID, (SendStop ? SIGSTOP : SIGQUIT));
	}
	
	/* Take care of the walreceiver too */
	if (pid == WalReceiverPID)
		WalReceiverPID = 0;
	else if (WalReceiverPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) WalReceiverPID)));
		signal_child(WalReceiverPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the autovacuum launcher too */
	if (pid == AutoVacPID)
		AutoVacPID = 0;
	else if (AutoVacPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) AutoVacPID)));
		signal_child(AutoVacPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	if (POLAR_SHARED_SERVER_RUNNING()) 
	{
		int i = 0;
		for (; i < polar_ss_dispatcher_count; i++) 
		{
			if (pid == polar_dispatcher_proc[i].pid)
			{
				polar_dispatcher_proc[i].pid = 0;
			}
			else if (polar_dispatcher_proc[i].pid != 0 && take_action)
			{
				ereport(LOG,
						(errmsg_internal("sending %s to process %d",
										(SendStop ? "SIGSTOP" : "SIGQUIT"),
										(int) polar_dispatcher_proc[i].pid)));
				signal_child(polar_dispatcher_proc[i].pid, (SendStop ? SIGSTOP : SIGQUIT));
			}
		}
	}

	/*
	 * Force a power-cycle of the pgarch process too.  (This isn't absolutely
	 * necessary, but it seems like a good idea for robustness, and it
	 * simplifies the state-machine logic in the case where a shutdown request
	 * arrives during crash processing.)
	 */
	if (PgArchPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 "SIGQUIT",
								 (int) PgArchPID)));
		signal_child(PgArchPID, SIGQUIT);
	}

	/* POLAR: Take care of the logindex background process too */
	if (pid == LogIndexBgPID)
		LogIndexBgPID = 0;
	else if (LogIndexBgPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) LogIndexBgPID)));
		signal_child(LogIndexBgPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/*
	 * Force a power-cycle of the pgstat process too.  (This isn't absolutely
	 * necessary, but it seems like a good idea for robustness, and it
	 * simplifies the state-machine logic in the case where a shutdown request
	 * arrives during crash processing.)
	 */
	if (PgStatPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 "SIGQUIT",
								 (int) PgStatPID)));
		signal_child(PgStatPID, SIGQUIT);
		allow_immediate_pgstat_restart();
	}

	/* Take care of the consensus process too */
	if (POLAR_ENABLE_DMA() && ConsensusPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d", "SIGQUIT", 
									(int) ConsensusPID)));
		signal_child(ConsensusPID, SIGQUIT);
	}

	/* POLAR: Take care of the flashback log inserter process too */
	if (pid == FlogBgInserterPID)
		FlogBgInserterPID = 0;
	else if (FlogBgInserterPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) FlogBgInserterPID)));
		signal_child(FlogBgInserterPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* POLAR: Take care of the flashback log writer process too */
	if (pid == FlogBgWriterPID)
		FlogBgWriterPID = 0;
	else if (FlogBgWriterPID != 0 && take_action)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) FlogBgWriterPID)));
		signal_child(FlogBgWriterPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* We do NOT restart the syslogger */

	if (Shutdown != ImmediateShutdown)
		FatalError = true;

	/* We now transit into a state of waiting for children to die */
	if (pmState == PM_RECOVERY ||
		pmState == PM_HOT_STANDBY ||
		/* POLAR */
		pmState == PM_DATAMAX ||
		/* POLAR end */
		pmState == PM_RUN ||
		pmState == PM_WAIT_BACKUP ||
		pmState == PM_WAIT_READONLY ||
		pmState == PM_SHUTDOWN)
	{
		pmState = PM_WAIT_BACKENDS;
		polar_log_pmstate();
	}

	/*
	 * .. and if this doesn't happen quickly enough, now the clock is ticking
	 * for us to kill them without mercy.
	 */
	if (AbortStartTime == 0)
		AbortStartTime = time(NULL);
}

/*
 * Log the death of a child process.
 */
static void
LogChildExit(int lev, const char *procname, int pid, int exitstatus)
{
	/*
	 * size of activity_buffer is arbitrary, but set equal to default
	 * track_activity_query_size
	 */
	char		activity_buffer[1024];
	const char *activity = NULL;

	if (!EXIT_STATUS_0(exitstatus))
		activity = pgstat_get_crashed_backend_activity(pid,
													   activity_buffer,
													   sizeof(activity_buffer));

	if (WIFEXITED(exitstatus))
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) exited with exit code %d",
						procname, pid, WEXITSTATUS(exitstatus)),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
	else if (WIFSIGNALED(exitstatus))
	{
#if defined(WIN32)
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) was terminated by exception 0x%X",
						procname, pid, WTERMSIG(exitstatus)),
				 errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value."),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
#else
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) was terminated by signal %d: %s",
						procname, pid, WTERMSIG(exitstatus),
						pg_strsignal(WTERMSIG(exitstatus))),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
#endif
	}
	else
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) exited with unrecognized status %d",
						procname, pid, exitstatus),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
}

/*
 * Advance the postmaster's state machine and take actions as appropriate
 *
 * This is common code for pmdie(), reaper() and sigusr1_handler(), which
 * receive the signals that might mean we need to change state.
 */
static void
PostmasterStateMachine(polar_pmstate_change_reason reason, pid_t pid)
{
	if (pmState == PM_WAIT_BACKUP)
	{
		/*
		 * PM_WAIT_BACKUP state ends when online backup mode is not active.
		 */
		if (!BackupInProgress())
		{
			pmState = PM_WAIT_BACKENDS;
			polar_log_pmstate();
		}
	}

	if (pmState == PM_WAIT_READONLY)
	{
		/*
		 * PM_WAIT_READONLY state ends when we have no regular backends that
		 * have been started during recovery.  We kill the startup and
		 * walreceiver processes and transition to PM_WAIT_BACKENDS.  Ideally,
		 * we might like to kill these processes first and then wait for
		 * backends to die off, but that doesn't work at present because
		 * killing the startup process doesn't release its locks.
		 */
		if (CountChildren(BACKEND_TYPE_NORMAL, false) == 0)
		{
			if (StartupPID != 0)
				signal_child(StartupPID, SIGTERM);
			if (WalReceiverPID != 0)
				signal_child(WalReceiverPID, SIGTERM);
			pmState = PM_WAIT_BACKENDS;
			polar_log_pmstate();
		}
	}

	/*
	 * If we are in a state-machine state that implies waiting for backends to
	 * exit, see if they're all gone, and change state if so.
	 */
	if (pmState == PM_WAIT_BACKENDS)
	{
		/*
		 * PM_WAIT_BACKENDS state ends when we have no regular backends
		 * (including autovac workers), no bgworkers (including unconnected
		 * ones), and no walwriter, autovac launcher or bgwriter.  If we are
		 * doing crash recovery or an immediate shutdown then we expect the
		 * checkpointer to exit as well, otherwise not. The archiver, stats,
		 * and syslogger processes are disregarded since they are not
		 * connected to shared memory; we also disregard dead_end children
		 * here. Walsenders are also disregarded, they will be terminated
		 * later after writing the checkpoint record, like the archiver
		 * process.
		 */
		if (CountChildren(BACKEND_TYPE_NORMAL | BACKEND_TYPE_WORKER, true) == 0 &&
			StartupPID == 0 &&
			WalReceiverPID == 0 &&
			BgWriterPID == 0 &&
			(CheckpointerPID == 0 ||
			 (!FatalError && Shutdown < ImmediateShutdown)) &&
			WalWriterPID == 0 &&
			AutoVacPID == 0 &&
			LogIndexBgPID == 0 &&
			FlogBgWriterPID == 0 &&
			FlogBgInserterPID == 0)
		{
			if (Shutdown >= ImmediateShutdown || FatalError)
			{
				/*
				 * Start waiting for dead_end children to die.  This state
				 * change causes ServerLoop to stop creating new ones.
				 */
				pmState = PM_WAIT_DEAD_END;
				polar_log_pmstate();

				/*
				 * We already SIGQUIT'd the archiver and stats processes, if
				 * any, when we started immediate shutdown or entered
				 * FatalError state.
				 */
			}
			else
			{
				/*
				 * If we get here, we are proceeding with normal shutdown. All
				 * the regular children are gone, and it's time to tell the
				 * checkpointer to do a shutdown checkpoint.
				 */
				Assert(Shutdown > NoShutdown);
				/* Start the checkpointer if not running */
				if (CheckpointerPID == 0)
					CheckpointerPID = StartCheckpointer();
				/* And tell it to shut down */
				if (CheckpointerPID != 0)
				{
					signal_child(CheckpointerPID, SIGUSR2);
					pmState = PM_SHUTDOWN;
					polar_log_pmstate();
				}
				else
				{
					/*
					 * If we failed to fork a checkpointer, just shut down.
					 * Any required cleanup will happen at next restart. We
					 * set FatalError so that an "abnormal shutdown" message
					 * gets logged when we exit.
					 */
					FatalError = true;
					pmState = PM_WAIT_DEAD_END;
					polar_log_pmstate();

					/* Kill the walsenders, archiver and stats collector too */
					SignalChildren(SIGQUIT);
					if (PgArchPID != 0)
						signal_child(PgArchPID, SIGQUIT);
					if (PgStatPID != 0)
						signal_child(PgStatPID, SIGQUIT);
					if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
						signal_child(ConsensusPID, SIGQUIT);
					if (PolarWalPipelinerPID != 0)
						signal_child(PolarWalPipelinerPID, SIGQUIT);
				}
			}
		}
	}

	if (pmState == PM_SHUTDOWN_2)
	{
		/*
		 * PM_SHUTDOWN_2 state ends when there's no other children than
		 * dead_end children left. There shouldn't be any regular backends
		 * left by now anyway; what we're really waiting for is walsenders and
		 * archiver.
		 */
		if (PgArchPID == 0 && CountChildren(BACKEND_TYPE_ALL, false) == 0 &&
				(!POLAR_ENABLE_DMA() || ConsensusPID == 0) &&
			PolarWalPipelinerPID == 0 &&
			WalReceiverPID == 0)
		{
			pmState = PM_WAIT_DEAD_END;
			polar_log_pmstate();
		}
		/* POLAR: If polar_vfs is the last one backend process, then kill it. */
		else if (PgArchPID == 0 && CountChildren(BACKEND_TYPE_ALL, false) == 1 &&
				(!POLAR_ENABLE_DMA() || ConsensusPID == 0) &&
			WalReceiverPID == 0)
		{
			SignalSomeChildren(SIGUSR2, BACKEND_TYPE_ALL, false);
		}
		/* POLAR end */
	}

	if (pmState == PM_WAIT_DEAD_END)
	{
		/*
		 * PM_WAIT_DEAD_END state ends when the BackendList is entirely empty
		 * (ie, no dead_end children remain), and the archiver and stats
		 * collector are gone too.
		 *
		 * The reason we wait for those two is to protect them against a new
		 * postmaster starting conflicting subprocesses; this isn't an
		 * ironclad protection, but it at least helps in the
		 * shutdown-and-immediately-restart scenario.  Note that they have
		 * already been sent appropriate shutdown signals, either during a
		 * normal state transition leading up to PM_WAIT_DEAD_END, or during
		 * FatalError processing.
		 */
		if (dlist_is_empty(&BackendList) &&
			PgArchPID == 0 && PgStatPID == 0 && 
			PolarWalPipelinerPID == 0 &&
			(!POLAR_ENABLE_DMA() || ConsensusPID == 0))
		{
			/* These other guys should be dead already */
			Assert(StartupPID == 0);
			Assert(WalReceiverPID == 0);
			Assert(BgWriterPID == 0);
			Assert(CheckpointerPID == 0);
			Assert(WalWriterPID == 0);
			Assert(AutoVacPID == 0);
			Assert(PolarWalPipelinerPID == 0);
			Assert(LogIndexBgPID == 0);
			Assert(FlogBgInserterPID == 0);
			Assert(FlogBgWriterPID == 0);
			/* syslogger is not considered here */
			pmState = PM_NO_CHILDREN;
			polar_log_pmstate();
		}
	}

	/*
	 * If we've been told to shut down, we exit as soon as there are no
	 * remaining children.  If there was a crash, cleanup will occur at the
	 * next startup.  (Before PostgreSQL 8.3, we tried to recover from the
	 * crash before exiting, but that seems unwise if we are quitting because
	 * we got SIGTERM from init --- there may well not be time for recovery
	 * before init decides to SIGKILL us.)
	 *
	 * Note that the syslogger continues to run.  It will exit when it sees
	 * EOF on its input pipe, which happens when there are no more upstream
	 * processes.
	 */
	if (Shutdown > NoShutdown && pmState == PM_NO_CHILDREN)
	{
		if (FatalError)
		{
			ereport(LOG, (errmsg("abnormal database system shutdown")));
			ExitPostmaster(1);
		}
		else
		{
			/*
			 * Terminate exclusive backup mode to avoid recovery after a clean
			 * fast shutdown.  Since an exclusive backup can only be taken
			 * during normal running (and not, for example, while running
			 * under Hot Standby) it only makes sense to do this if we reached
			 * normal running. If we're still in recovery, the backup file is
			 * one we're recovering *from*, and we must keep it around so that
			 * recovery restarts from the right place.
			 */
			if (ReachedNormalRunning)
				CancelBackup();

			/* Normal exit from the postmaster is here */
			ExitPostmaster(0);
		}
	}

	/*
	 * If the startup process failed, or the user does not want an automatic
	 * restart after backend crashes, wait for all non-syslogger children to
	 * exit, and then exit postmaster.  We don't try to reinitialize when the
	 * startup process fails, because more than likely it will just fail again
	 * and we will keep trying forever.
	 */
	if (pmState == PM_NO_CHILDREN &&
		(StartupStatus == STARTUP_CRASHED || !restart_after_crash))
		ExitPostmaster(1);

	/*
	 * If we need to recover from a crash, wait for all non-syslogger children
	 * to exit, then reset shmem and StartupDataBase.
	 */
	if (FatalError && pmState == PM_NO_CHILDREN)
	{
		ereport(LOG,
			(errmsg("all server processes terminated; reinitializing")));
		polar_state_machine_change_check(reason, pid);
		if (polar_in_error_handling_process == false)
		{
			/* POLAR: mark error handling process */
			polar_in_error_handling_process = true;

			/* allow background workers to immediately restart */
			ResetBackgroundWorkerCrashTimes();

			shmem_exit(1);

			/* POLAR: reinitialize node type */
			polar_init_node_type();

			/* POLAR: reset vfs */
			polar_reset_vfs_switch();

			/* re-read control file into local memory */
			LocalProcessControlFile(true);

			reset_shared(PostPortNumber);

			if (POLAR_FILE_IN_SHARED_STORAGE())
			{
				elog(LOG, "polardb try start vfs process");
				maybe_start_bgworkers(true);
				polar_set_vfs_function_ready();
				polar_mount();
				elog(LOG, "polardb start vfs process success");

				/* POLAR: remove POLAR_REPLICA_BOOTED_FILE when not in replica node */
				if (!polar_in_replica_mode())
					polar_remove_replica_booted_file();

				/*
				 * POLAR: Initialize the local global directories for replica or standby
				 * copy pg_control file from shared storage to local.
				 * for replica, move the copy of base directories and other trans status log files to startup process
				 * so that postmaster can return and respond to the detection of cm timely
				 * for standby, only pg_control file is needed to copy
				 */
				polar_init_global_dir_for_replica_or_standby();

				/* POLAR: load control file after polar_init_global_dir_for_replica_or_standby */
				elog(LOG, "polardb load controlfile in postmaster");
				polar_load_and_check_controlfile();
			}

			/* POLAR: must be called after polar_load_and_check_controlfile */
			polar_try_reuse_buffer_pool();

			StartupPID = StartupDataBase();
			Assert(StartupPID != 0);
			StartupStatus = STARTUP_RUNNING;
			pmState = PM_STARTUP;
			polar_log_pmstate();
			/* crash recovery started, reset SIGKILL flag */
			AbortStartTime = 0;

			/* POLAR: process end */
			polar_in_error_handling_process = false;
		}
		else
			elog(WARNING, "unexpected reinitializing");
	}
}


/*
 * Send a signal to a postmaster child process
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.  For signals that are generally interpreted in the
 * appropriate fashion, we signal the entire process group not just the
 * direct child process.  This allows us to, for example, SIGQUIT a blocked
 * archive_recovery script, or SIGINT a script being run by a backend via
 * system().
 *
 * There is a race condition for recently-forked children: they might not
 * have executed setsid() yet.  So we signal the child directly as well as
 * the group.  We assume such a child will handle the signal before trying
 * to spawn any grandchild processes.  We also assume that signaling the
 * child twice will not cause any problems.
 */
static void
signal_child(pid_t pid, int signal)
{
	if (kill(pid, signal) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) pid, signal);
#ifdef HAVE_SETSID
	switch (signal)
	{
		case SIGINT:
		case SIGTERM:
		case SIGQUIT:
		case SIGSTOP:
		case SIGKILL:
			if (kill(-pid, signal) < 0)
				elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) (-pid), signal);
			break;
		default:
			break;
	}
#endif
}

/*
 * Send a signal to the targeted children (but NOT special children;
 * dead_end children are never signaled, either).
 *
 * POLAR: polar_skip_mount_process decide whether to skip polar_vfs process or not.
 */
static bool
SignalSomeChildren(int signal, int target, bool polar_skip_mount_process)
{
	dlist_iter	iter;
	bool		signaled = false;

	dlist_foreach(iter, &BackendList)
	{
		Backend    *bp = dlist_container(Backend, elem, iter.cur);

		if (bp->dead_end)
			continue;

		/*
		 * Since target == BACKEND_TYPE_ALL is the most common case, we test
		 * it first and avoid touching shared memory for every child.
		 */
		if (target != BACKEND_TYPE_ALL)
		{
			/*
			 * Assign bkend_type for any recently announced WAL Sender
			 * processes.
			 */
			if (bp->bkend_type == BACKEND_TYPE_NORMAL &&
				IsPostmasterChildWalSender(bp->child_slot))
			{
				/* POLAR */
				NormalBackendCount--;
				bp->bkend_type = BACKEND_TYPE_WALSND;
			}

			if (!(target & bp->bkend_type))
				continue;
		}

		ereport(DEBUG4,
				(errmsg_internal("sending signal %d to process %d",
								 signal, (int) bp->pid)));
		signal_child(bp->pid, signal);
		signaled = true;
	}
	return signaled;
}

/*
 * Send a termination signal to children.  This considers all of our children
 * processes, except syslogger and dead_end backends.
 */
static void
TerminateChildren(int signal)
{
	SignalChildren(signal);
	if (StartupPID != 0)
	{
		signal_child(StartupPID, signal);
		if (signal == SIGQUIT || signal == SIGKILL)
			StartupStatus = STARTUP_SIGNALED;
	}
	if (BgWriterPID != 0)
		signal_child(BgWriterPID, signal);
	if (CheckpointerPID != 0)
		signal_child(CheckpointerPID, signal);
	if (WalWriterPID != 0)
		signal_child(WalWriterPID, signal);
	if (WalReceiverPID != 0)
		signal_child(WalReceiverPID, signal);
	if (AutoVacPID != 0)
		signal_child(AutoVacPID, signal);
	if (PgArchPID != 0)
		signal_child(PgArchPID, signal);
	if (PgStatPID != 0)
		signal_child(PgStatPID, signal);
	if (LogIndexBgPID != 0)
		signal_child(LogIndexBgPID, signal);
	if (POLAR_ENABLE_DMA() && ConsensusPID != 0)
		signal_child(ConsensusPID, signal);
	if (PolarWalPipelinerPID != 0)
		signal_child(PolarWalPipelinerPID, signal);
	if (FlogBgWriterPID != 0)
		signal_child(FlogBgWriterPID, signal);
	if (FlogBgInserterPID != 0)
		signal_child(FlogBgInserterPID, signal);

	polar_dispatcher_handle_signal(signal);
}

/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 *
 * Note: if you change this code, also consider StartAutovacuumWorker.
 */
static int
BackendStartup(Port *port)
{
	Backend    *bn;				/* for backend cleanup */
	pid_t		pid;

	/*
	 * Create backend data structure.  Better before the fork() so we can
	 * handle failure cleanly.
	 */
	bn = (Backend *) malloc(sizeof(Backend));
	if (!bn)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return STATUS_ERROR;
	}

	/*
	 * Compute the cancel key that will be assigned to this backend. The
	 * backend will have its own copy in the forked-off process' value of
	 * MyCancelKey, so that it can transmit the key to the frontend.
	 */
	if (!RandomCancelKey(&MyCancelKey))
	{
		free(bn);
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate random cancel key")));
		return STATUS_ERROR;
	}

	bn->cancel_key = MyCancelKey;

	/* Pass down canAcceptConnections state */
	port->canAcceptConnections = canAcceptConnections(BACKEND_TYPE_NORMAL);
	bn->dead_end = (port->canAcceptConnections != CAC_OK &&
					port->canAcceptConnections != CAC_WAITBACKUP);

	/*
	 * Unless it's a dead_end child, assign it a child slot number
	 */
	if (!bn->dead_end)
		bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
	else
		bn->child_slot = 0;

	/* Hasn't asked to be notified about any bgworkers yet */
	bn->bgworker_notify = false;

#ifdef EXEC_BACKEND
	pid = backend_forkexec(port);
#else							/* !EXEC_BACKEND */
	pid = fork_process();
	if (pid == 0)				/* child */
	{
		free(bn);

		/* Detangle from postmaster */
		InitPostmasterChild();

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Perform additional initialization and collect startup packet */
		BackendInitialize(port);

		/* And run the backend */
		BackendRun(port);
	}
#endif							/* EXEC_BACKEND */

	if (pid < 0)
	{
		/* in parent, fork failed */
		int			save_errno = errno;

		if (!bn->dead_end)
			(void) ReleasePostmasterChildSlot(bn->child_slot);
		free(bn);
		errno = save_errno;
		ereport(LOG,
				(errmsg("could not fork new process for connection: %m")));
		report_fork_failure_to_client(port, save_errno);
		return STATUS_ERROR;
	}

	/* in parent, successful fork */
	ereport(DEBUG2,
			(errmsg_internal("forked new backend, pid=%d socket=%d",
							 (int) pid, (int) port->sock)));

	/*
	 * Everything's been successful, it's safe to add this backend to our list
	 * of backends.
	 */
	bn->pid = pid;
	bn->bkend_type = BACKEND_TYPE_NORMAL;	/* Can change later to WALSND */
	dlist_push_head(&BackendList, &bn->elem);
	/* POLAR */
	NormalBackendCount++;

	/* POLAR */
	ereport(DEBUG2, (errmsg("BackendStartup, normal backend count=%d, max=%d",
		NormalBackendCount, MaxNormalBackends)));

#ifdef EXEC_BACKEND
	if (!bn->dead_end)
		ShmemBackendArrayAdd(bn);
#endif

	return STATUS_OK;
}

/*
 * Try to report backend fork() failure to client before we close the
 * connection.  Since we do not care to risk blocking the postmaster on
 * this connection, we set the connection to non-blocking and try only once.
 *
 * This is grungy special-purpose code; we cannot use backend libpq since
 * it's not up and running.
 */
static void
report_fork_failure_to_client(Port *port, int errnum)
{
	char		buffer[1000];
	int			rc;

	/* Format the error message packet (always V2 protocol) */
	snprintf(buffer, sizeof(buffer), "E%s%s\n",
			 _("could not fork new process for connection: "),
			 strerror(errnum));

	/* Set port to non-blocking.  Don't do send() if this fails */
	if (!pg_set_noblock(port->sock))
		return;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(port->sock, buffer, strlen(buffer) + 1, 0);
	} while (rc < 0 && errno == EINTR);
}


/*
 * BackendInitialize -- initialize an interactive (postmaster-child)
 *				backend process, and collect the client's startup packet.
 *
 * returns: nothing.  Will not return at all if there's any failure.
 *
 * Note: this code does not depend on having any access to shared memory.
 * In the EXEC_BACKEND case, we are physically attached to shared memory
 * but have not yet set up most of our local pointers to shmem structures.
 */
static void
BackendInitialize(Port *port)
{
	int			status;
	int			ret;
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];
	char		remote_ps_data[NI_MAXHOST];

	/* Save port etc. for ps status */
	MyProcPort = port;

	/*
	 * PreAuthDelay is a debugging aid for investigating problems in the
	 * authentication cycle: it can be set in postgresql.conf to allow time to
	 * attach to the newly-forked backend with a debugger.  (See also
	 * PostAuthDelay, which we allow clients to pass through PGOPTIONS, but it
	 * is not honored until after authentication.)
	 */
	if (PreAuthDelay > 0)
		pg_usleep(PreAuthDelay * 1000000L);

	/* This flag will remain set until InitPostgres finishes authentication */
	ClientAuthInProgress = true;	/* limit visibility of log messages */

	/* save process start time */
	port->SessionStartTime = GetCurrentTimestamp();
	MyStartTime = timestamptz_to_time_t(port->SessionStartTime);

	/* set these to empty in case they are needed before we set them up */
	port->remote_host = "";
	port->remote_port = "";

	/* POLAR orgin connection info init */
	MemSet(&port->polar_origin_addr, 0, sizeof(port->polar_origin_addr));
	port->polar_proxy = false;
	port->polar_send_lsn = false;
	port->polar_proxy_ssl_in_use = false;
	port->polar_proxy_ssl_cipher_name = NULL;
	port->polar_proxy_ssl_version = NULL;
	/* POLAR end */

	/*
	 * Initialize libpq and enable reporting of ereport errors to the client.
	 * Must do this now because authentication uses libpq to send messages.
	 */
	pq_init();					/* initialize libpq to talk to client */
	whereToSendOutput = DestRemote; /* now safe to ereport to client */

	/*
	 * We arrange for a simple exit(1) if we receive SIGTERM or SIGQUIT or
	 * timeout while trying to collect the startup packet.  Otherwise the
	 * postmaster cannot shutdown the database FAST or IMMED cleanly if a
	 * buggy client fails to send the packet promptly.  XXX it follows that
	 * the remainder of this function must tolerate losing control at any
	 * instant.  Likewise, any pg_on_exit_callback registered before or during
	 * this function must be prepared to execute at any instant between here
	 * and the end of this function.  Furthermore, affected callbacks execute
	 * partially or not at all when a second exit-inducing signal arrives
	 * after proc_exit_prepare() decrements on_proc_exit_index.  (Thanks to
	 * that mechanic, callbacks need not anticipate more than one call.)  This
	 * is fragile; it ought to instead follow the norm of handling interrupts
	 * at selected, safe opportunities.
	 *
	 * POLAR:
	 * As SIGTERM/SIGQUIT/SIGALRM use same sighandle func - proc_exit ->exit,
	 * it is non reentrant function, we need disable these signals.
	 */
	pqsignal_with_sigsets(SIGTERM, startup_die, SIGQUIT, SIGALRM);
	pqsignal_with_sigsets(SIGQUIT, startup_die, SIGTERM, SIGALRM);
	InitializeTimeouts_with_sigsets(SIGQUIT, SIGTERM);		/* establishes SIGALRM handler */
	PG_SETMASK(&StartupBlockSig);

	/*
	 * Get the remote host name and port for logging and status display.
	 */
	remote_host[0] = '\0';
	remote_port[0] = '\0';
	if ((ret = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
								  remote_host, sizeof(remote_host),
								  remote_port, sizeof(remote_port),
								  (log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV)) != 0)
		ereport(WARNING,
				(errmsg_internal("pg_getnameinfo_all() failed: %s",
								 gai_strerror(ret))));
	if (remote_port[0] == '\0')
		snprintf(remote_ps_data, sizeof(remote_ps_data), "%s", remote_host);
	else
		snprintf(remote_ps_data, sizeof(remote_ps_data), "%s(%s)", remote_host, remote_port);

	/*
	 * Save remote_host and remote_port in port structure (after this, they
	 * will appear in log_line_prefix data for log messages).
	 */
	port->remote_host = strdup(remote_host);
	port->remote_port = strdup(remote_port);

	/* And now we can issue the Log_connections message, if wanted */
	if (Log_connections)
	{
		if (remote_port[0])
			ereport(LOG,
					(errmsg("connection received: host=%s port=%s",
							remote_host,
							remote_port)));
		else
			ereport(LOG,
					(errmsg("connection received: host=%s",
							remote_host)));
	}

	/*
	 * If we did a reverse lookup to name, we might as well save the results
	 * rather than possibly repeating the lookup during authentication.
	 *
	 * Note that we don't want to specify NI_NAMEREQD above, because then we'd
	 * get nothing useful for a client without an rDNS entry.  Therefore, we
	 * must check whether we got a numeric IPv4 or IPv6 address, and not save
	 * it into remote_hostname if so.  (This test is conservative and might
	 * sometimes classify a hostname as numeric, but an error in that
	 * direction is safe; it only results in a possible extra lookup.)
	 */
	if (log_hostname &&
		ret == 0 &&
		strspn(remote_host, "0123456789.") < strlen(remote_host) &&
		strspn(remote_host, "0123456789ABCDEFabcdef:") < strlen(remote_host))
		port->remote_hostname = strdup(remote_host);

	/*
	 * Ready to begin client interaction.  We will give up and exit(1) after a
	 * time delay, so that a broken client can't hog a connection
	 * indefinitely.  PreAuthDelay and any DNS interactions above don't count
	 * against the time limit.
	 *
	 * Note: AuthenticationTimeout is applied here while waiting for the
	 * startup packet, and then again in InitPostgres for the duration of any
	 * authentication operations.  So a hostile client could tie up the
	 * process for nearly twice AuthenticationTimeout before we kick him off.
	 *
	 * Note: because PostgresMain will call InitializeTimeouts again, the
	 * registration of STARTUP_PACKET_TIMEOUT will be lost.  This is okay
	 * since we never use it again after this function.
	 */
	RegisterTimeout(STARTUP_PACKET_TIMEOUT, StartupPacketTimeoutHandler);
	enable_timeout_after(STARTUP_PACKET_TIMEOUT, AuthenticationTimeout * 1000);

	/*
	 * Receive the startup packet (which might turn out to be a cancel request
	 * packet).
	 */
	if (POLAR_SHARED_SERVER_RUNNING() && polar_dispatcher_startup_buf != NULL)
	{
		/* this port is received from dispatcher. Don't support SSL now. */
		status = polar_parse_startup_packet(port,
											TopMemoryContext,
											polar_dispatcher_startup_buf,
											polar_dispatcher_startup_len,
											true);
		free(polar_dispatcher_startup_buf);
		polar_dispatcher_startup_buf = NULL;
	}
	else
	{
		char* polar_buf = NULL;
		int32 polar_len = 0;
		status = polar_get_startup_packet(false, &polar_buf, &polar_len);
		if (STATUS_OK == status)
			status = polar_parse_startup_packet(port, TopMemoryContext, polar_buf, polar_len, false);
		if (polar_buf != NULL)
			pfree(polar_buf);
	}

	/*
	 * Stop here if it was bad or a cancel packet.  ProcessStartupPacket
	 * already did any appropriate error reporting.
	 */
	if (status != STATUS_OK)
		proc_exit(0);

	/*
	 * Now that we have the user and database name, we can set the process
	 * title for ps.  It's good to do this as early as possible in startup.
	 *
	 * For a walsender, the ps display is set in the following form:
	 *
	 * postgres: walsender <user> <host> <activity>
	 *
	 * To achieve that, we pass "walsender" as username and username as dbname
	 * to init_ps_display(). XXX: should add a new variant of
	 * init_ps_display() to avoid abusing the parameters like this.
	 */
	if (am_walsender)
		init_ps_display(pgstat_get_backend_desc(B_WAL_SENDER), port->user_name, remote_ps_data,
						update_process_title ? "authentication" : "");
	else
		init_ps_display(port->user_name, port->database_name, remote_ps_data,
						update_process_title ? "authentication" : "");

	/*
	 * Disable the timeout, and prevent SIGTERM/SIGQUIT again.
	 */
	disable_timeout(STARTUP_PACKET_TIMEOUT, false);
	PG_SETMASK(&BlockSig);
}


/*
 * BackendRun -- set up the backend's argument list and invoke PostgresMain()
 *
 * returns:
 *		Shouldn't return at all.
 *		If PostgresMain() fails, return status.
 */
static void
BackendRun(Port *port)
{
	char	  **av;
	int			maxac;
	int			ac;
	long		secs;
	int			usecs;
	int			i;

	/*
	 * Don't want backend to be able to see the postmaster random number
	 * generator state.  We have to clobber the static random_seed *and* start
	 * a new random sequence in the random() library function.
	 */
#ifndef HAVE_STRONG_RANDOM
	random_seed = 0;
	random_start_time.tv_usec = 0;
#endif
	/* slightly hacky way to convert timestamptz into integers */
	TimestampDifference(0, port->SessionStartTime, &secs, &usecs);
	srandom((unsigned int) (MyProcPid ^ (usecs << 12) ^ secs));

	/*
	 * Now, build the argv vector that will be given to PostgresMain.
	 *
	 * The maximum possible number of commandline arguments that could come
	 * from ExtraOptions is (strlen(ExtraOptions) + 1) / 2; see
	 * pg_split_opts().
	 */
	maxac = 2;					/* for fixed args supplied below */
	maxac += (strlen(ExtraOptions) + 1) / 2;

	av = (char **) MemoryContextAlloc(TopMemoryContext,
									  maxac * sizeof(char *));
	ac = 0;

	av[ac++] = "postgres";

	/*
	 * Pass any backend switches specified with -o on the postmaster's own
	 * command line.  We assume these are secure.
	 */
	pg_split_opts(av, &ac, ExtraOptions);

	av[ac] = NULL;

	Assert(ac < maxac);

	/*
	 * Debug: print arguments being passed to backend
	 */
	ereport(DEBUG3,
			(errmsg_internal("%s child[%d]: starting with (",
							 progname, (int) getpid())));
	for (i = 0; i < ac; ++i)
		ereport(DEBUG3,
				(errmsg_internal("\t%s", av[i])));
	ereport(DEBUG3,
			(errmsg_internal(")")));

	/*
	 * Make sure we aren't in PostmasterContext anymore.  (We can't delete it
	 * just yet, though, because InitPostgres will need the HBA data.)
	 */
	MemoryContextSwitchTo(TopMemoryContext);

	PostgresMain(ac, av, port->database_name, port->user_name);
}


#ifdef EXEC_BACKEND

/*
 * postmaster_forkexec -- fork and exec a postmaster subprocess
 *
 * The caller must have set up the argv array already, except for argv[2]
 * which will be filled with the name of the temp variable file.
 *
 * Returns the child process PID, or -1 on fork failure (a suitable error
 * message has been logged on failure).
 *
 * All uses of this routine will dispatch to SubPostmasterMain in the
 * child process.
 */
pid_t
postmaster_forkexec(int argc, char *argv[])
{
	Port		port;

	/* This entry point passes dummy values for the Port variables */
	memset(&port, 0, sizeof(port));
	return internal_forkexec(argc, argv, &port);
}

/*
 * backend_forkexec -- fork/exec off a backend process
 *
 * Some operating systems (WIN32) don't have fork() so we have to simulate
 * it by storing parameters that need to be passed to the child and
 * then create a new child process.
 *
 * returns the pid of the fork/exec'd process, or -1 on failure
 */
static pid_t
backend_forkexec(Port *port)
{
	char	   *av[4];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkbackend";
	av[ac++] = NULL;			/* filled in by internal_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return internal_forkexec(ac, av, port);
}

#ifndef WIN32

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	static unsigned long tmpBackendFileNum = 0;
	pid_t		pid;
	char		tmpfilename[MAXPGPATH];
	BackendParameters param;
	FILE	   *fp;

	if (!save_backend_variables(&param, port))
		return -1;				/* log made by save_backend_variables */

	/* Calculate name for temp file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 MyProcPid, ++tmpBackendFileNum);

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_W);
	if (!fp)
	{
		/*
		 * As in OpenTemporaryFileInTablespace, try to make the temp-file
		 * directory, ignoring errors.
		 */
		(void) MakePGDirectory(PG_TEMP_FILES_DIR, false);

		fp = AllocateFile(tmpfilename, PG_BINARY_W);
		if (!fp)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							tmpfilename)));
			return -1;
		}
	}

	if (fwrite(&param, sizeof(param), 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		return -1;
	}

	/* Release file */
	if (FreeFile(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		return -1;
	}

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Insert temp file name after --fork argument */
	argv[2] = tmpfilename;

	/* Fire off execv in child */
	if ((pid = fork_process()) == 0)
	{
		if (execv(postgres_exec_path, argv) < 0)
		{
			ereport(LOG,
					(errmsg("could not execute server process \"%s\": %m",
							postgres_exec_path)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	return pid;					/* Parent returns pid, or -1 on fork failure */
}
#else							/* WIN32 */

/*
 * internal_forkexec win32 implementation
 *
 * - starts backend using CreateProcess(), in suspended state
 * - writes out backend variables to the parameter file
 *	- during this, duplicates handles and sockets required for
 *	  inheritance into the new process
 * - resumes execution of the new process once the backend parameter
 *	 file is complete.
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	int			retry_count = 0;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	int			i;
	int			j;
	char		cmdLine[MAXPGPATH * 2];
	HANDLE		paramHandle;
	BackendParameters *param;
	SECURITY_ATTRIBUTES sa;
	char		paramHandleStr[32];
	win32_deadchild_waitinfo *childinfo;

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Resume here if we need to retry */
retry:

	/* Set up shared memory for parameter passing */
	ZeroMemory(&sa, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	paramHandle = CreateFileMapping(INVALID_HANDLE_VALUE,
									&sa,
									PAGE_READWRITE,
									0,
									sizeof(BackendParameters),
									NULL);
	if (paramHandle == INVALID_HANDLE_VALUE)
	{
		elog(LOG, "could not create backend parameter file mapping: error code %lu",
			 GetLastError());
		return -1;
	}

	param = MapViewOfFile(paramHandle, FILE_MAP_WRITE, 0, 0, sizeof(BackendParameters));
	if (!param)
	{
		elog(LOG, "could not map backend parameter memory: error code %lu",
			 GetLastError());
		CloseHandle(paramHandle);
		return -1;
	}

	/* Insert temp file name after --fork argument */
#ifdef _WIN64
	sprintf(paramHandleStr, "%llu", (LONG_PTR) paramHandle);
#else
	sprintf(paramHandleStr, "%lu", (DWORD) paramHandle);
#endif
	argv[2] = paramHandleStr;

	/* Format the cmd line */
	cmdLine[sizeof(cmdLine) - 1] = '\0';
	cmdLine[sizeof(cmdLine) - 2] = '\0';
	snprintf(cmdLine, sizeof(cmdLine) - 1, "\"%s\"", postgres_exec_path);
	i = 0;
	while (argv[++i] != NULL)
	{
		j = strlen(cmdLine);
		snprintf(cmdLine + j, sizeof(cmdLine) - 1 - j, " \"%s\"", argv[i]);
	}
	if (cmdLine[sizeof(cmdLine) - 2] != '\0')
	{
		elog(LOG, "subprocess command line too long");
		return -1;
	}

	memset(&pi, 0, sizeof(pi));
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);

	/*
	 * Create the subprocess in a suspended state. This will be resumed later,
	 * once we have written out the parameter file.
	 */
	if (!CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, CREATE_SUSPENDED,
					   NULL, NULL, &si, &pi))
	{
		elog(LOG, "CreateProcess call failed: %m (error code %lu)",
			 GetLastError());
		return -1;
	}

	if (!save_backend_variables(param, port, pi.hProcess, pi.dwProcessId))
	{
		/*
		 * log made by save_backend_variables, but we have to clean up the
		 * mess with the half-started process
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate unstarted process: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		return -1;				/* log made by save_backend_variables */
	}

	/* Drop the parameter shared memory that is now inherited to the backend */
	if (!UnmapViewOfFile(param))
		elog(LOG, "could not unmap view of backend parameter file: error code %lu",
			 GetLastError());
	if (!CloseHandle(paramHandle))
		elog(LOG, "could not close handle to backend parameter file: error code %lu",
			 GetLastError());

	/*
	 * Reserve the memory region used by our main shared memory segment before
	 * we resume the child process.  Normally this should succeed, but if ASLR
	 * is active then it might sometimes fail due to the stack or heap having
	 * gotten mapped into that range.  In that case, just terminate the
	 * process and retry.
	 */
	if (!pgwin32_ReserveSharedMemoryRegion(pi.hProcess))
	{
		/* pgwin32_ReserveSharedMemoryRegion already made a log entry */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate process that failed to reserve memory: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		if (++retry_count < 100)
			goto retry;
		ereport(LOG,
				(errmsg("giving up after too many tries to reserve shared memory"),
				 errhint("This might be caused by ASLR or antivirus software.")));
		return -1;
	}

	/*
	 * Now that the backend variables are written out, we start the child
	 * thread so it can start initializing while we set up the rest of the
	 * parent state.
	 */
	if (ResumeThread(pi.hThread) == -1)
	{
		if (!TerminateProcess(pi.hProcess, 255))
		{
			ereport(LOG,
					(errmsg_internal("could not terminate unstartable process: error code %lu",
									 GetLastError())));
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			return -1;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		ereport(LOG,
				(errmsg_internal("could not resume thread of unstarted process: error code %lu",
								 GetLastError())));
		return -1;
	}

	/*
	 * Queue a waiter for to signal when this child dies. The wait will be
	 * handled automatically by an operating system thread pool.
	 *
	 * Note: use malloc instead of palloc, since it needs to be thread-safe.
	 * Struct will be free():d from the callback function that runs on a
	 * different thread.
	 */
	childinfo = malloc(sizeof(win32_deadchild_waitinfo));
	if (!childinfo)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	childinfo->procHandle = pi.hProcess;
	childinfo->procId = pi.dwProcessId;

	if (!RegisterWaitForSingleObject(&childinfo->waitHandle,
									 pi.hProcess,
									 pgwin32_deadchild_callback,
									 childinfo,
									 INFINITE,
									 WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD))
		ereport(FATAL,
				(errmsg_internal("could not register process for wait: error code %lu",
								 GetLastError())));

	/* Don't close pi.hProcess here - the wait thread needs access to it */

	CloseHandle(pi.hThread);

	return pi.dwProcessId;
}
#endif							/* WIN32 */


/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkFOO"
 * (where FOO indicates which postmaster child we are to become), and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.  Remaining arguments go to the
 * subprocess FooMain() routine.
 */
void
SubPostmasterMain(int argc, char *argv[])
{
	Port		port;

	/* In EXEC_BACKEND case we will not have inherited these settings */
	IsPostmasterEnvironment = true;
	whereToSendOutput = DestNone;

	/* Setup as postmaster child */
	InitPostmasterChild();

	/* Setup essential subsystems (to ensure elog() behaves sanely) */
	InitializeGUCOptions();

	/* Check we got appropriate args */
	if (argc < 3)
		elog(FATAL, "invalid subpostmaster invocation");

	/* Read in the variables file */
	memset(&port, 0, sizeof(Port));
	read_backend_variables(argv[2], &port);

	/* Close the postmaster's sockets (as soon as we know them) */
	ClosePostmasterPorts(strcmp(argv[1], "--forklog") == 0);

	/*
	 * Set reference point for stack-depth checking
	 */
	set_stack_base();

	/*
	 * Set up memory area for GSS information. Mirrors the code in ConnCreate
	 * for the non-exec case.
	 */
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port.gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port.gss)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
#endif

	/*
	 * If appropriate, physically re-attach to shared memory segment. We want
	 * to do this before going any further to ensure that we can attach at the
	 * same address the postmaster used.  On the other hand, if we choose not
	 * to re-attach, we may have other cleanup to do.
	 *
	 * If testing EXEC_BACKEND on Linux, you should run this as root before
	 * starting the postmaster:
	 *
	 * echo 0 >/proc/sys/kernel/randomize_va_space
	 *
	 * This prevents using randomized stack and code addresses that cause the
	 * child process's memory map to be different from the parent's, making it
	 * sometimes impossible to attach to shared memory at the desired address.
	 * Return the setting to its old value (usually '1' or '2') when finished.
	 */
	if (strcmp(argv[1], "--forkbackend") == 0 ||
		strcmp(argv[1], "--forkavlauncher") == 0 ||
		strcmp(argv[1], "--forkavworker") == 0 ||
		strcmp(argv[1], "--forkboot") == 0 ||
		strncmp(argv[1], "--forkbgworker=", 15) == 0)
		PGSharedMemoryReAttach();
	else
		PGSharedMemoryNoReAttach();

	/* autovacuum needs this set before calling InitProcess */
	if (strcmp(argv[1], "--forkavlauncher") == 0)
		AutovacuumLauncherIAm();
	if (strcmp(argv[1], "--forkavworker") == 0)
		AutovacuumWorkerIAm();

	/*
	 * Start our win32 signal implementation. This has to be done after we
	 * read the backend variables, because we need to pick up the signal pipe
	 * from the parent process.
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	/* In EXEC_BACKEND case we will not have inherited these settings */
	pqinitmask();
	PG_SETMASK(&BlockSig);

	/* Read in remaining GUC variables */
	read_nondefault_variables();

	/*
	 * Check that the data directory looks valid, which will also check the
	 * privileges on the data directory and update our umask and file/group
	 * variables for creating files later.  Note: this should really be done
	 * before we create any files or directories.
	 */
	checkDataDir();

	/*
	 * (re-)read control file, as it contains config. The postmaster will
	 * already have read this, but this process doesn't know about that.
	 */
	LocalProcessControlFile(false);

	/*
	 * Reload any libraries that were preloaded by the postmaster.  Since we
	 * exec'd this process, those libraries didn't come along with us; but we
	 * should load them into all child processes to be consistent with the
	 * non-EXEC_BACKEND behavior.
	 */
	process_shared_preload_libraries();

	/* Run backend or appropriate child */
	if (strcmp(argv[1], "--forkbackend") == 0)
	{
		Assert(argc == 3);		/* shouldn't be any more args */

		/*
		 * Need to reinitialize the SSL library in the backend, since the
		 * context structures contain function pointers and cannot be passed
		 * through the parameter file.
		 *
		 * If for some reason reload fails (maybe the user installed broken
		 * key files), soldier on without SSL; that's better than all
		 * connections becoming impossible.
		 *
		 * XXX should we do this in all child processes?  For the moment it's
		 * enough to do it in backend children.
		 */
#ifdef USE_SSL
		if (EnableSSL)
		{
			if (secure_initialize(false) == 0)
				LoadedSSL = true;
			else
				ereport(LOG,
						(errmsg("SSL configuration could not be loaded in child process")));
		}
#endif

		/*
		 * Perform additional initialization and collect startup packet.
		 *
		 * We want to do this before InitProcess() for a couple of reasons: 1.
		 * so that we aren't eating up a PGPROC slot while waiting on the
		 * client. 2. so that if InitProcess() fails due to being out of
		 * PGPROC slots, we have already initialized libpq and are able to
		 * report the error to the client.
		 */
		BackendInitialize(&port);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(0);

		/* And run the backend */
		BackendRun(&port);		/* does not return */
	}
	if (strcmp(argv[1], "--forkboot") == 0)
	{
		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitAuxiliaryProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(0);

		AuxiliaryProcessMain(argc - 2, argv + 2);	/* does not return */
	}
	if (strcmp(argv[1], "--forkavlauncher") == 0)
	{
		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(0);

		AutoVacLauncherMain(argc - 2, argv + 2);	/* does not return */
	}
	if (strcmp(argv[1], "--forkavworker") == 0)
	{
		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(0);

		AutoVacWorkerMain(argc - 2, argv + 2);	/* does not return */
	}
	if (strncmp(argv[1], "--forkbgworker=", 15) == 0)
	{
		int			shmem_slot;

		/* do this as early as possible; in particular, before InitProcess() */
		IsBackgroundWorker = true;

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(0);

		/* Fetch MyBgworkerEntry from shared memory */
		shmem_slot = atoi(argv[1] + 15);
		MyBgworkerEntry = BackgroundWorkerEntry(shmem_slot);

		StartBackgroundWorker();
	}
	if (strcmp(argv[1], "--forkarch") == 0)
	{
		/* Do not want to attach to shared memory */

		PgArchiverMain(argc, argv); /* does not return */
	}
	if (strcmp(argv[1], "--forkcol") == 0)
	{
		/* Do not want to attach to shared memory */

		PgstatCollectorMain(argc, argv);	/* does not return */
	}
	if (strcmp(argv[1], "--forklog") == 0)
	{
		/* Do not want to attach to shared memory */

		SysLoggerMain(argc, argv);	/* does not return */
	}

	abort();					/* shouldn't get here */
}
#endif							/* EXEC_BACKEND */


/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
static void
ExitPostmaster(int status)
{
#ifdef HAVE_PTHREAD_IS_THREADED_NP

	/*
	 * There is no known cause for a postmaster to become multithreaded after
	 * startup.  Recheck to account for the possibility of unknown causes.
	 * This message uses LOG level, because an unclean shutdown at this point
	 * would usually not look much different from a clean shutdown.
	 */
	if (pthread_is_threaded_np() != 0)
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg_internal("postmaster became multithreaded"),
				 errdetail("Please report this to <pgsql-bugs@postgresql.org>.")));
#endif

	/* should cleanup shared memory and kill all backends */

	/*
	 * Not sure of the semantics here.  When the Postmaster dies, should the
	 * backends all be killed? probably not.
	 *
	 * MUST		-- vadim 05-10-1999
	 */

	/* 
	 * POLAR: record end lsn when polar_enable_promote_wait_for_walreceive_done = on
	 * used to verify whether standby has received all xlog
	 */
	if (polar_enable_promote_wait_for_walreceive_done && polar_in_master_mode())
		elog(LOG,"Primary postmaster exit, last insert position: %lX", GetInsertRecPtr());
	/* POLAR end */
	proc_exit(status);
}

/*
 * sigusr1_handler - handle signal conditions from child processes
 */
static void
sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	/* Process background worker state change. */
	if (CheckPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE))
	{
		BackgroundWorkerStateChange();
		StartWorkerNeeded = true;
	}

	/*
	 * RECOVERY_STARTED and BEGIN_HOT_STANDBY signals are ignored in
	 * unexpected states. If the startup process quickly starts up, completes
	 * recovery, exits, we might process the death of the startup process
	 * first. We don't want to go back to recovery in that case.
	 */
	if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED) &&
		pmState == PM_STARTUP && Shutdown == NoShutdown)
	{
		/* WAL redo has started. We're out of reinitialization. */
		FatalError = false;
		Assert(AbortStartTime == 0);

		/*
		 * Crank up the background tasks.  It doesn't matter if this fails,
		 * we'll just try again later.
		 */
		Assert(CheckpointerPID == 0);
		CheckpointerPID = StartCheckpointer();
		Assert(BgWriterPID == 0);
		BgWriterPID = StartBackgroundWriter();
		Assert(!polar_is_dma_data_node() || ConsensusPID == 0);
		if (polar_is_dma_data_node())
			ConsensusPID = StartConsensus();

		/* POLAR: start logindex background process */
		if (polar_logindex_redo_instance)
		{
			Assert(LogIndexBgPID == 0);
			LogIndexBgPID = StartLogIndexBgWriter();
		}

		/*
		 * Start the archiver if we're responsible for (re-)archiving received
		 * files.
		 */
		Assert(PgArchPID == 0);
		if (XLogArchivingAlways())
			PgArchPID = pgarch_start();

		/*
		 * If we aren't planning to enter hot standby mode later, treat
		 * RECOVERY_STARTED as meaning we're out of startup, and report status
		 * accordingly.
		 */
		if (polar_is_dma_data_node() || !EnableHotStandby)
		{
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STANDBY);
#ifdef USE_SYSTEMD
			sd_notify(0, "READY=1");
#endif
		}

		pmState = PM_RECOVERY;
		polar_log_pmstate();
	}
	if (CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY) &&
		pmState == PM_RECOVERY && Shutdown == NoShutdown)
	{
		/*
		 * Likewise, start other special children as needed.
		 */
		Assert(PgStatPID == 0);
		PgStatPID = pgstat_start();

		ereport(LOG,
				(errmsg("database system is ready to accept read only connections")));

		/* Report status */
		AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY);
#ifdef USE_SYSTEMD
		sd_notify(0, "READY=1");
#endif

		pmState = PM_HOT_STANDBY;
		polar_log_pmstate();
		/* Some workers may be scheduled to start now */
		StartWorkerNeeded = true;
	}

	/* POLAR: add DataMax state */
	if (CheckPostmasterSignal(PGSIGNAL_BEGIN_DATAMAX) &&
		pmState == PM_HOT_STANDBY && Shutdown == NoShutdown)
	{
		/* 
		 * POLAR: WAL redo done, enter datamax mode when received signal.
		 * hot_standby is on to enable cascade replication in datamax mode
		 * so we turn into PM_DATAMAX from PM_HOT_STANDBY
		 */
		FatalError = false;
		Assert(AbortStartTime == 0);

		Assert(!polar_is_dma_logger_node() || ConsensusPID == 0);
		if (polar_is_dma_logger_node())
			ConsensusPID = StartConsensus();

		ereport(LOG,
				(errmsg("database system is entering datamax mode.")));

		AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_DATAMAX);
		pmState = PM_DATAMAX;
		
		polar_log_pmstate();

		StartWorkerNeeded = true;
	}
	/* POLAR end */

	if (StartWorkerNeeded || HaveCrashedWorker)
		maybe_start_bgworkers(false);

	if (CheckPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER) &&
		PgArchPID != 0)
	{
		/*
		 * Send SIGUSR1 to archiver process, to wake it up and begin archiving
		 * next WAL file.
		 */
		signal_child(PgArchPID, SIGUSR1);
	}

	if (!polar_enable_multi_syslogger) 
	{
		if (CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) &&
			SysLoggerPID != 0)
		{
			/* Tell syslogger to rotate logfile */
			signal_child(SysLoggerPID, SIGUSR1);
		}
	} 
	else 
	{
		/* POLAR */
		if (CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) &&
			polar_num_of_sysloggers > 0)
		{
			int i = 0;
			for (; i < polar_num_of_sysloggers; i++) 
			{
				if (SysLoggerPIDs[i] != 0)
					signal_child(SysLoggerPIDs[i], SIGUSR1);
			}
		}
		/* POLAR end */
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER) &&
		Shutdown == NoShutdown)
	{
		/*
		 * Start one iteration of the autovacuum daemon, even if autovacuuming
		 * is nominally not enabled.  This is so we can have an active defense
		 * against transaction ID wraparound.  We set a flag for the main loop
		 * to do it rather than trying to do it here --- this is because the
		 * autovac process itself may send the signal, and we want to handle
		 * that by launching another iteration as soon as the current one
		 * completes.
		 */
		start_autovac_launcher = true;
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER) &&
		Shutdown == NoShutdown)
	{
		/* The autovacuum launcher wants us to start a worker process. */
		StartAutovacuumWorker();
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER))
	{
		/* Startup Process wants us to start the walreceiver process. */
		/* Start immediately if possible, else remember request for later. */
		WalReceiverRequested = true;
		MaybeStartWalReceiver();
	}

	/*
	 * Try to advance postmaster's state machine, if a child requests it.
	 *
	 * Be careful about the order of this action relative to sigusr1_handler's
	 * other actions.  Generally, this should be after other actions, in case
	 * they have effects PostmasterStateMachine would need to know about.
	 * However, we should do it before the CheckPromoteSignal step, which
	 * cannot have any (immediate) effect on the state machine, but does
	 * depend on what state we're in now.
	 */
	if (CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE))
	{
		/* Advance postmaster's state machine */
		PostmasterStateMachine(PM_ADVANCE_STATE_MACHINE, 0);
	}

	if (StartupPID != 0 &&
		(pmState == PM_STARTUP || pmState == PM_RECOVERY ||
		 pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY) &&
		CheckPromoteSignal())
	{
		if (POLAR_LOGINDEX_ENABLE_ONLINE_PROMOTE())
			polar_postmaster_online_promote();
		else
		{
			/* Tell startup process to finish recovery */
			signal_child(StartupPID, SIGUSR2);
		}
	}

	if (POLAR_ENABLE_DMA() &&
		CheckPostmasterSignal(PMSIGNAL_CONSENS_UPGRADE) &&
			StartupPID != 0 &&
			(pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_DATAMAX))
	{
		ereport(LOG,
				(errmsg("postmaster sigusr1_handler signaled, became leader")));
		polar_signal_recovery_state_change(true, false);
	}

	if (POLAR_ENABLE_DMA() &&
		CheckPostmasterSignal(PMSIGNAL_CONSENS_LEADER_CHANGE) &&
			StartupPID != 0 &&
			(pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_DATAMAX))
	{
		ereport(LOG,
				(errmsg("postmaster sigusr1_handler signaled, log term switch")));
		polar_signal_recovery_state_change(false, false);
	}

	if (POLAR_ENABLE_DMA() && 
		CheckPostmasterSignal(PMSIGNAL_CONSENS_LEADER_RESUME) &&
			(pmState == PM_RECOVERY ||
			 pmState == PM_HOT_STANDBY ||
			 pmState == PM_RUN ||
			 pmState == PM_DATAMAX ||
			 pmState == PM_WAIT_BACKUP ||
			 pmState == PM_WAIT_READONLY ||
			 pmState == PM_WAIT_BACKENDS ||
			 pmState == PM_SHUTDOWN))
	{
		ereport(LOG,
				(errmsg("postmaster sigusr1_handler signaled, old leader change "
								"back to new leader")));
		polar_signal_recovery_state_change(false, true);
	}

	if (polar_is_dma_logger_node() && 
		CheckPostmasterSignal(PMSIGNAL_CONSENS_DOWNGRADE) &&
			(pmState == PM_RECOVERY ||
			 pmState == PM_DATAMAX ||
			 pmState == PM_WAIT_READONLY ||
			 pmState == PM_WAIT_BACKENDS ||
			 pmState == PM_SHUTDOWN))
	{
		ereport(LOG,
				(errmsg("postmaster sigusr1_handler signaled, old leader change to follower")));
		polar_signal_recovery_state_change(false, false);
	}

	if (polar_is_dma_data_node() &&
		CheckPostmasterSignal(PMSIGNAL_CONSENS_DOWNGRADE) &&
			(pmState == PM_RECOVERY ||
			 pmState == PM_HOT_STANDBY ||
			 pmState == PM_RUN ||
			 pmState == PM_DATAMAX ||
			 pmState == PM_WAIT_BACKUP ||
			 pmState == PM_WAIT_READONLY ||
			 pmState == PM_WAIT_BACKENDS ||
			 pmState == PM_SHUTDOWN))
	{
		ereport(LOG,
				(errmsg("postmaster sigusr1_handler signaled, old leader change to follower")));
		TerminateChildren(SIGQUIT);
		FatalError = true;
		pmState = PM_WAIT_BACKENDS;
		polar_log_pmstate();
		PostmasterStateMachine(PM_RECEIVE_SIGQUIT, 0);
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}

/*
 * SIGTERM or SIGQUIT while processing startup packet.
 * Clean up and exit(1).
 *
 * XXX: possible future improvement: try to send a message indicating
 * why we are disconnecting.  Problem is to be sure we don't block while
 * doing so, nor mess up SSL initialization.  In practice, if the client
 * has wedged here, it probably couldn't do anything with the message anyway.
 */
static void
startup_die(SIGNAL_ARGS)
{
	proc_exit(1);
}

/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.  (See notes in
 * tcop/postgres.c.)
 */
static void
dummy_handler(SIGNAL_ARGS)
{
}

/*
 * Timeout while processing startup packet.
 * As for startup_die(), we clean up and exit(1).
 */
static void
StartupPacketTimeoutHandler(void)
{
	proc_exit(1);
}


/*
 * Generate a random cancel key.
 */
bool
RandomCancelKey(int32 *cancel_key)
{
#ifdef HAVE_STRONG_RANDOM
	return pg_strong_random((char *) cancel_key, sizeof(int32));
#else

	/*
	 * If built with --disable-strong-random, use plain old erand48.
	 *
	 * We cannot use pg_backend_random() in postmaster, because it stores its
	 * state in shared memory.
	 */
	static unsigned short seed[3];

	/*
	 * Select a random seed at the time of first receiving a request.
	 */
	if (random_seed == 0)
	{
		struct timeval random_stop_time;

		gettimeofday(&random_stop_time, NULL);

		seed[0] = (unsigned short) random_start_time.tv_usec;
		seed[1] = (unsigned short) (random_stop_time.tv_usec) ^ (random_start_time.tv_usec >> 16);
		seed[2] = (unsigned short) (random_stop_time.tv_usec >> 16);

		random_seed = 1;
	}

	*cancel_key = pg_jrand48(seed);

	return true;
#endif
}

/*
 * Count up number of child processes of specified types (dead_end children
 * are always excluded).
 */
static int
CountChildren(int target, bool skip_polar_mount_process)
{
	dlist_iter	iter;
	int			cnt = 0;

	dlist_foreach(iter, &BackendList)
	{
		Backend    *bp = dlist_container(Backend, elem, iter.cur);

		if (bp->dead_end)
			continue;

		/*
		 * Since target == BACKEND_TYPE_ALL is the most common case, we test
		 * it first and avoid touching shared memory for every child.
		 */
		if (target != BACKEND_TYPE_ALL)
		{
			/*
			 * Assign bkend_type for any recently announced WAL Sender
			 * processes.
			 */
			if (bp->bkend_type == BACKEND_TYPE_NORMAL &&
				IsPostmasterChildWalSender(bp->child_slot))
			{
				/* POLAR */
				NormalBackendCount--;
				bp->bkend_type = BACKEND_TYPE_WALSND;
			}

			if (!(target & bp->bkend_type))
				continue;

		}

		cnt++;
	}
	return cnt;
}


/*
 * StartChildProcess -- start an auxiliary process for the postmaster
 *
 * "type" determines what kind of child will be started.  All child types
 * initially go to AuxiliaryProcessMain, which will handle common setup.
 *
 * Return value of StartChildProcess is subprocess' PID, or 0 if failed
 * to start subprocess.
 */
static pid_t
StartChildProcess(AuxProcType type)
{
	pid_t		pid;
	char	   *av[10];
	int			ac = 0;
	char		typebuf[32];


	/*
	 * Set up command-line arguments for subprocess
	 */
	av[ac++] = "postgres";

#ifdef EXEC_BACKEND
	av[ac++] = "--forkboot";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
#endif

	snprintf(typebuf, sizeof(typebuf), "-x%d", type);
	av[ac++] = typebuf;

	av[ac] = NULL;
	Assert(ac < lengthof(av));

#ifdef EXEC_BACKEND
	pid = postmaster_forkexec(ac, av);
#else							/* !EXEC_BACKEND */
	pid = fork_process();

	if (pid == 0)				/* child */
	{
		InitPostmasterChild();

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Release postmaster's working memory context */
		MemoryContextSwitchTo(TopMemoryContext);
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;

		AuxiliaryProcessMain(ac, av);
		ExitPostmaster(0);
	}
#endif							/* EXEC_BACKEND */

	if (pid < 0)
	{
		/* in parent, fork failed */
		int			save_errno = errno;

		errno = save_errno;
		switch (type)
		{
			case StartupProcess:
				ereport(LOG,
						(errmsg("could not fork startup process: %m")));
				break;
			case BgWriterProcess:
				ereport(LOG,
						(errmsg("could not fork background writer process: %m")));
				break;
			case CheckpointerProcess:
				ereport(LOG,
						(errmsg("could not fork checkpointer process: %m")));
				break;
			case WalWriterProcess:
				ereport(LOG,
						(errmsg("could not fork WAL writer process: %m")));
				break;
			case WalReceiverProcess:
				ereport(LOG,
						(errmsg("could not fork WAL receiver process: %m")));
				break;
			case ConsensusProcess:
				ereport(LOG,
						(errmsg("could not fork consensus process: %m")));
				break;
			case PolarWalPipelinerProcess:
				ereport(LOG,
						(errmsg("could not fork polar wal pipeliner process: %m")));
				break;
			case LogIndexBgWriterProcess:
				ereport(LOG,
						(errmsg("could not fork logindex bg writer process: %m")));
				break;
			case FlogBgInserterProcess:
				ereport(LOG,
						(errmsg("could not fork flashback log bg inserter process: %m")));
				break;
			case FlogBgWriterProcess:
				ereport(LOG,
						(errmsg("could not fork flashback log bg writer process: %m")));
				break;
			default:
				ereport(LOG,
						(errmsg("could not fork process: %m")));
				break;
		}

		/*
		 * fork failure is fatal during startup, but there's no need to choke
		 * immediately if starting other child types fails.
		 */
		if (type == StartupProcess)
			ExitPostmaster(1);
		return 0;
	}

	/*
	 * in parent, successful fork
	 */
	return pid;
}

/*
 * StartAutovacuumWorker
 *		Start an autovac worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void
StartAutovacuumWorker(void)
{
	Backend    *bn;

	/*
	 * If not in condition to run a process, don't try, but handle it like a
	 * fork failure.  This does not normally happen, since the signal is only
	 * supposed to be sent by autovacuum launcher when it's OK to do it, but
	 * we have to check to avoid race-condition problems during DB state
	 * changes.
	 */
	if (canAcceptConnections(BACKEND_TYPE_AUTOVAC) == CAC_OK)
	{
		/*
		 * Compute the cancel key that will be assigned to this session. We
		 * probably don't need cancel keys for autovac workers, but we'd
		 * better have something random in the field to prevent unfriendly
		 * people from sending cancels to them.
		 */
		if (!RandomCancelKey(&MyCancelKey))
		{
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not generate random cancel key")));
			return;
		}

		bn = (Backend *) malloc(sizeof(Backend));
		if (bn)
		{
			bn->cancel_key = MyCancelKey;

			/* Autovac workers are not dead_end and need a child slot */
			bn->dead_end = false;
			bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
			bn->bgworker_notify = false;

			bn->pid = StartAutoVacWorker();
			if (bn->pid > 0)
			{
				bn->bkend_type = BACKEND_TYPE_AUTOVAC;
				dlist_push_head(&BackendList, &bn->elem);
#ifdef EXEC_BACKEND
				ShmemBackendArrayAdd(bn);
#endif
				/* all OK */
				return;
			}

			/*
			 * fork failed, fall through to report -- actual error message was
			 * logged by StartAutoVacWorker
			 */
			(void) ReleasePostmasterChildSlot(bn->child_slot);
			free(bn);
		}
		else
			ereport(LOG,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	/*
	 * Report the failure to the launcher, if it's running.  (If it's not, we
	 * might not even be connected to shared memory, so don't try to call
	 * AutoVacWorkerFailed.)  Note that we also need to signal it so that it
	 * responds to the condition, but we don't do that here, instead waiting
	 * for ServerLoop to do it.  This way we avoid a ping-pong signalling in
	 * quick succession between the autovac launcher and postmaster in case
	 * things get ugly.
	 */
	if (AutoVacPID != 0)
	{
		AutoVacWorkerFailed();
		avlauncher_needs_signal = true;
	}
}

static void
polar_dispatcher_start(void)
{
	if (POLAR_SHARED_SERVER_RUNNING())
	{
		int i;
		for (i = 0; i < polar_ss_dispatcher_count; i++)
		{
			if (0 == polar_dispatcher_proc[i].pid)
			{
				PolarDispatcherProc  *proc = &polar_dispatcher_proc[i];

				if (socketpair(AF_UNIX, SOCK_STREAM, 0, proc->pipes) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
								errmsg_internal("could not create socket pair for launching sessions: %m")));

				polar_dispatcher_worker_start(i);

				dispatcher_had_restart = true;
			}
		}
	}
}

/*
 * Send signal to dispatcher
 */
static void polar_dispatcher_handle_signal(int signal)
{
	if (POLAR_SHARED_SERVER_RUNNING())
	{
		int i;
		elog(LOG, "polar_dispatcher_handle_signal %d", signal);
		for (i = 0; i < polar_ss_dispatcher_count; i++)
		{
			if (polar_dispatcher_proc[i].pid != 0)
				signal_child(polar_dispatcher_proc[i].pid, signal);
		}
	}
}

/*
 * polar_dispatcher_worker_start
 *		Start an shared server dispatcher worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void
polar_dispatcher_worker_start(int id)
{
	pid_t		pid;

	polar_my_dispatcher_id = id;

	switch ((pid = fork_process()))
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork dispatcher worker process: %m")));
			return ;

		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			dispatcher_main(0, NULL);
			break;
		default:
			elog(LOG, "Start dispatcher process id:%d, pid:%d", polar_my_dispatcher_id, pid);
			polar_dispatcher_proc[id].id = id;
			polar_dispatcher_proc[id].pid = pid;
	}
}

/*
 * MaybeStartWalReceiver
 *		Start the WAL receiver process, if not running and our state allows.
 *
 * Note: if WalReceiverPID is already nonzero, it might seem that we should
 * clear WalReceiverRequested.  However, there's a race condition if the
 * walreceiver terminates and the startup process immediately requests a new
 * one: it's quite possible to get the signal for the request before reaping
 * the dead walreceiver process.  Better to risk launching an extra
 * walreceiver than to miss launching one we need.  (The walreceiver code
 * has logic to recognize that it should go away if not needed.)
 */
static void
MaybeStartWalReceiver(void)
{
	if (WalReceiverPID == 0 &&
		(pmState == PM_STARTUP || pmState == PM_RECOVERY ||
		 pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY ||
		 /* POLAR: Walreceiver can be start in DataMax mode. */
		 pmState == PM_DATAMAX) &&
		 /* POLAR end */
		Shutdown == NoShutdown)
	{
		WalReceiverPID = StartWalReceiver();
		if (WalReceiverPID != 0)
			WalReceiverRequested = false;
		/* else leave the flag set, so we'll try again later */
	}
}


/*
 * Create the opts file
 */
static bool
CreateOptsFile(int argc, char *argv[], char *fullprogname)
{
	FILE	   *fp;
	int			i;

#define OPTS_FILE	"postmaster.opts"

	if ((fp = fopen(OPTS_FILE, "w")) == NULL)
	{
		elog(LOG, "could not create file \"%s\": %m", OPTS_FILE);
		return false;
	}

	fprintf(fp, "%s", fullprogname);
	for (i = 1; i < argc; i++)
		fprintf(fp, " \"%s\"", argv[i]);
	fputs("\n", fp);

	if (fclose(fp))
	{
		elog(LOG, "could not write file \"%s\": %m", OPTS_FILE);
		return false;
	}

	return true;
}


/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of entries needed in per-child-process arrays
 * (the PMChildFlags array, and if EXEC_BACKEND the ShmemBackendArray).
 * These arrays include regular backends, autovac workers, walsenders
 * and background workers, but not special children nor dead_end children.
 * This allows the arrays to have a fixed maximum size, to wit the same
 * too-many-children limit enforced by canAcceptConnections().  The exact value
 * isn't too critical as long as it's more than MaxBackends.
 */
int
MaxLivePostmasterChildren(void)
{
	return 2 * (MaxConnections + autovacuum_max_workers + 1 +
				MaxPolarDispatcher +/* POLAR: Shared Server */
				max_worker_processes);
}

/*
 * Connect background worker to a database.
 */
void
BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags)
{
	BackgroundWorker *worker = MyBgworkerEntry;

	/* XXX is this the right errcode? */
	if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("database connection requirement not indicated during registration")));

	InitPostgres(dbname, InvalidOid, username, InvalidOid, NULL, (flags & BGWORKER_BYPASS_ALLOWCONN) != 0);

	/* it had better not gotten out of "init" mode yet */
	if (!IsInitProcessingMode())
		ereport(ERROR,
				(errmsg("invalid processing mode in background worker")));
	SetProcessingMode(NormalProcessing);
}

/*
 * Connect background worker to a database using OIDs.
 */
void
BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid, uint32 flags)
{
	BackgroundWorker *worker = MyBgworkerEntry;

	/* XXX is this the right errcode? */
	if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("database connection requirement not indicated during registration")));

	InitPostgres(NULL, dboid, NULL, useroid, NULL, (flags & BGWORKER_BYPASS_ALLOWCONN) != 0);

	/* it had better not gotten out of "init" mode yet */
	if (!IsInitProcessingMode())
		ereport(ERROR,
				(errmsg("invalid processing mode in background worker")));
	SetProcessingMode(NormalProcessing);
}

/*
 * Block/unblock signals in a background worker
 */
void
BackgroundWorkerBlockSignals(void)
{
	PG_SETMASK(&BlockSig);
}

void
BackgroundWorkerUnblockSignals(void)
{
	PG_SETMASK(&UnBlockSig);
}

#ifdef EXEC_BACKEND
static pid_t
bgworker_forkexec(int shmem_slot)
{
	char	   *av[10];
	int			ac = 0;
	char		forkav[MAXPGPATH];

	snprintf(forkav, MAXPGPATH, "--forkbgworker=%d", shmem_slot);

	av[ac++] = "postgres";
	av[ac++] = forkav;
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif

/*
 * Start a new bgworker.
 * Starting time conditions must have been checked already.
 *
 * Returns true on success, false on failure.
 * In either case, update the RegisteredBgWorker's state appropriately.
 *
 * This code is heavily based on autovacuum.c, q.v.
 */
static bool
do_start_bgworker(RegisteredBgWorker *rw)
{
	pid_t		worker_pid;

	Assert(rw->rw_pid == 0);

	/*
	 * Allocate and assign the Backend element.  Note we must do this before
	 * forking, so that we can handle failures (out of memory or child-process
	 * slots) cleanly.
	 *
	 * Treat failure as though the worker had crashed.  That way, the
	 * postmaster will wait a bit before attempting to start it again; if we
	 * tried again right away, most likely we'd find ourselves hitting the
	 * same resource-exhaustion condition.
	 */
	if (!assign_backendlist_entry(rw))
	{
		rw->rw_crashed_at = GetCurrentTimestamp();
		return false;
	}

	ereport(DEBUG1,
			(errmsg("starting background worker process \"%s\"",
					rw->rw_worker.bgw_name)));

#ifdef EXEC_BACKEND
	switch ((worker_pid = bgworker_forkexec(rw->rw_shmem_slot)))
#else
	switch ((worker_pid = fork_process()))
#endif
	{
		case -1:
			/* in postmaster, fork failed ... */
			ereport(LOG,
					(errmsg("could not fork worker process: %m")));
			/* undo what assign_backendlist_entry did */
			ReleasePostmasterChildSlot(rw->rw_child_slot);
			rw->rw_child_slot = 0;
			free(rw->rw_backend);
			rw->rw_backend = NULL;
			/* mark entry as crashed, so we'll try again later */
			rw->rw_crashed_at = GetCurrentTimestamp();
			break;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			/*
			 * Before blowing away PostmasterContext, save this bgworker's
			 * data where it can find it.
			 */
			MyBgworkerEntry = (BackgroundWorker *)
				MemoryContextAlloc(TopMemoryContext, sizeof(BackgroundWorker));
			memcpy(MyBgworkerEntry, &rw->rw_worker, sizeof(BackgroundWorker));

			/* Release postmaster's working memory context */
			MemoryContextSwitchTo(TopMemoryContext);
			MemoryContextDelete(PostmasterContext);
			PostmasterContext = NULL;

			StartBackgroundWorker();

			exit(1);			/* should not get here */
			break;
#endif
		default:
			/* in postmaster, fork successful ... */
			rw->rw_pid = worker_pid;
			rw->rw_backend->pid = rw->rw_pid;
			ReportBackgroundWorkerPID(rw);
			/* add new worker to lists of backends */
			dlist_push_head(&BackendList, &rw->rw_backend->elem);
#ifdef EXEC_BACKEND
			ShmemBackendArrayAdd(rw->rw_backend);
#endif
			return true;
	}

	return false;
}

/*
 * Does the current postmaster state require starting a worker with the
 * specified start_time?
 */
static bool
bgworker_should_start_now(BgWorkerStartTime start_time)
{
	switch (pmState)
	{
		case PM_NO_CHILDREN:
		case PM_WAIT_DEAD_END:
		case PM_SHUTDOWN_2:
		case PM_SHUTDOWN:
		case PM_WAIT_BACKENDS:
		case PM_WAIT_READONLY:
		case PM_WAIT_BACKUP:
			break;

		case PM_RUN:
			if (start_time == BgWorkerStart_RecoveryFinished)
				return true;
			/* fall through */

		case PM_HOT_STANDBY:
		/* POLAR */
		case PM_DATAMAX:
		/* POLAR end */
			if (start_time == BgWorkerStart_ConsistentState)
				return true;
			/* fall through */

		case PM_RECOVERY:
		case PM_STARTUP:
		case PM_INIT:
			if (start_time == BgWorkerStart_PostmasterStart)
				return true;
			/* fall through */

	}

	return false;
}

/*
 * Allocate the Backend struct for a connected background worker, but don't
 * add it to the list of backends just yet.
 *
 * On failure, return false without changing any worker state.
 *
 * Some info from the Backend is copied into the passed rw.
 */
static bool
assign_backendlist_entry(RegisteredBgWorker *rw)
{
	Backend    *bn;

	/*
	 * Check that database state allows another connection.  Currently the
	 * only possible failure is CAC_TOOMANY, so we just log an error message
	 * based on that rather than checking the error code precisely.
	 */
	if (canAcceptConnections(BACKEND_TYPE_BGWORKER) != CAC_OK)
	{
		ereport(LOG,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no slot available for new worker process")));
		return false;
	}

	/*
	 * Compute the cancel key that will be assigned to this session. We
	 * probably don't need cancel keys for background workers, but we'd better
	 * have something random in the field to prevent unfriendly people from
	 * sending cancels to them.
	 */
	if (!RandomCancelKey(&MyCancelKey))
	{
		ereport(LOG,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate random cancel key")));
		return false;
	}

	bn = malloc(sizeof(Backend));
	if (bn == NULL)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return false;
	}

	bn->cancel_key = MyCancelKey;
	bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
	bn->bkend_type = BACKEND_TYPE_BGWORKER;
	bn->dead_end = false;
	bn->bgworker_notify = false;

	rw->rw_backend = bn;
	rw->rw_child_slot = bn->child_slot;

	return true;
}

/*
 * If the time is right, start background worker(s).
 *
 * As a side effect, the bgworker control variables are set or reset
 * depending on whether more workers may need to be started.
 *
 * We limit the number of workers started per call, to avoid consuming the
 * postmaster's attention for too long when many such requests are pending.
 * As long as StartWorkerNeeded is true, ServerLoop will not block and will
 * call this function again after dealing with any other issues.
 */
static void
maybe_start_bgworkers(bool polar_start_mount_process)
{
#define MAX_BGWORKERS_TO_LAUNCH 100
	int			num_launched = 0;
	TimestampTz now = 0;
	slist_mutable_iter iter;

	/*
	 * During crash recovery, we have no need to be called until the state
	 * transition out of recovery.
	 */
	if (FatalError && polar_start_mount_process == false)
	{
		StartWorkerNeeded = false;
		HaveCrashedWorker = false;
		return;
	}

	/* Don't need to be called again unless we find a reason for it below */
	StartWorkerNeeded = false;
	HaveCrashedWorker = false;

	slist_foreach_modify(iter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);

		/* ignore if already running */
		if (rw->rw_pid != 0)
			continue;

		/* if marked for death, clean up and remove from list */
		if (rw->rw_terminate)
		{
			ForgetBackgroundWorker(&iter);
			continue;
		}

		/*
		 * If this worker has crashed previously, maybe it needs to be
		 * restarted (unless on registration it specified it doesn't want to
		 * be restarted at all).  Check how long ago did a crash last happen.
		 * If the last crash is too recent, don't start it right away; let it
		 * be restarted once enough time has passed.
		 */
		if (rw->rw_crashed_at != 0)
		{
			if (rw->rw_worker.bgw_restart_time == BGW_NEVER_RESTART)
			{
				int			notify_pid;

				notify_pid = rw->rw_worker.bgw_notify_pid;

				ForgetBackgroundWorker(&iter);

				/* Report worker is gone now. */
				if (notify_pid != 0)
					kill(notify_pid, SIGUSR1);

				continue;
			}

			/* read system time only when needed */
			if (now == 0)
				now = GetCurrentTimestamp();

			if (!TimestampDifferenceExceeds(rw->rw_crashed_at, now,
											rw->rw_worker.bgw_restart_time * 1000))
			{
				/* Set flag to remember that we have workers to start later */
				HaveCrashedWorker = true;
				continue;
			}
		}

		if (bgworker_should_start_now(rw->rw_worker.bgw_start_time))
		{
			/* reset crash time before trying to start worker */
			rw->rw_crashed_at = 0;

			if (polar_start_mount_process)
				continue;

			/*
			 * Try to start the worker.
			 *
			 * On failure, give up processing workers for now, but set
			 * StartWorkerNeeded so we'll come back here on the next iteration
			 * of ServerLoop to try again.  (We don't want to wait, because
			 * there might be additional ready-to-run workers.)  We could set
			 * HaveCrashedWorker as well, since this worker is now marked
			 * crashed, but there's no need because the next run of this
			 * function will do that.
			 */
			if (!do_start_bgworker(rw))
			{
				StartWorkerNeeded = true;
				return;
			}

			/*
			 * If we've launched as many workers as allowed, quit, but have
			 * ServerLoop call us again to look for additional ready-to-run
			 * workers.  There might not be any, but we'll find out the next
			 * time we run.
			 */
			if (++num_launched >= MAX_BGWORKERS_TO_LAUNCH)
			{
				StartWorkerNeeded = true;
				return;
			}
		}
	}
}

/*
 * When a backend asks to be notified about worker state changes, we
 * set a flag in its backend entry.  The background worker machinery needs
 * to know when such backends exit.
 */
bool
PostmasterMarkPIDForWorkerNotify(int pid)
{
	dlist_iter	iter;
	Backend    *bp;

	dlist_foreach(iter, &BackendList)
	{
		bp = dlist_container(Backend, elem, iter.cur);
		if (bp->pid == pid)
		{
			bp->bgworker_notify = true;
			return true;
		}
	}

	/*
	 * POLAR: Logindex background process start parallel process to replay.
	 * Check whether it's LogIndexBgPID register notification for background process
	 */
	if (pid != 0 && pid == LogIndexBgPID)
		return true;

	return false;
}

#ifdef EXEC_BACKEND

/*
 * The following need to be available to the save/restore_backend_variables
 * functions.  They are marked NON_EXEC_STATIC in their home modules.
 */
extern slock_t *ShmemLock;
extern slock_t *ProcStructLock;
extern PGPROC *AuxiliaryProcs;
extern PMSignalData *PMSignalState;
extern pgsocket pgStatSock;
extern pg_time_t first_syslogger_file_time;

#ifndef WIN32
#define write_inheritable_socket(dest, src, childpid) ((*(dest) = (src)), true)
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static bool write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE child);
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src,
						 pid_t childPid);
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src);
#endif


/* Save critical backend variables into the BackendParameters struct */
#ifndef WIN32
static bool
save_backend_variables(BackendParameters *param, Port *port)
#else
static bool
save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid)
#endif
{
	memcpy(&param->port, port, sizeof(Port));
	if (!write_inheritable_socket(&param->portsocket, port->sock, childPid))
		return false;

	strlcpy(param->DataDir, DataDir, MAXPGPATH);

	memcpy(&param->ListenSocket, &ListenSocket, sizeof(ListenSocket));

	param->MyCancelKey = MyCancelKey;
	param->MyPMChildSlot = MyPMChildSlot;

#ifdef WIN32
	param->ShmemProtectiveRegion = ShmemProtectiveRegion;
#endif
	param->UsedShmemSegID = UsedShmemSegID;
	param->UsedShmemSegAddr = UsedShmemSegAddr;

	param->ShmemLock = ShmemLock;
	param->ShmemVariableCache = ShmemVariableCache;
	param->ShmemBackendArray = ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	param->SpinlockSemaArray = SpinlockSemaArray;
#endif
	param->NamedLWLockTrancheRequests = NamedLWLockTrancheRequests;
	param->NamedLWLockTrancheArray = NamedLWLockTrancheArray;
	param->MainLWLockArray = MainLWLockArray;
	param->ProcStructLock = ProcStructLock;
	param->ProcGlobal = ProcGlobal;
	param->AuxiliaryProcs = AuxiliaryProcs;
	param->PreparedXactProcs = PreparedXactProcs;
	param->PMSignalState = PMSignalState;
	if (!write_inheritable_socket(&param->pgStatSock, pgStatSock, childPid))
		return false;

	param->PostmasterPid = PostmasterPid;
	param->PgStartTime = PgStartTime;
	param->PgReloadTime = PgReloadTime;
	param->first_syslogger_file_time = first_syslogger_file_time;

	param->redirection_done = redirection_done;
	param->IsBinaryUpgrade = IsBinaryUpgrade;
	param->max_safe_fds = max_safe_fds;

	param->MaxBackends = MaxBackends;

	/* POLAR */
	param->MaxNormalBackends = MaxNormalBackends;

#ifdef WIN32
	param->PostmasterHandle = PostmasterHandle;
	if (!write_duplicated_handle(&param->initial_signal_pipe,
								 pgwin32_create_signal_listener(childPid),
								 childProcess))
		return false;
#else
	memcpy(&param->postmaster_alive_fds, &postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&param->syslogPipe, &syslogPipe, sizeof(syslogPipe));

	strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

	strlcpy(param->pkglib_path, pkglib_path, MAXPGPATH);

	strlcpy(param->ExtraOptions, ExtraOptions, MAXPGPATH);

	return true;
}


#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static bool
write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
{
	HANDLE		hChild = INVALID_HANDLE_VALUE;

	if (!DuplicateHandle(GetCurrentProcess(),
						 src,
						 childProcess,
						 &hChild,
						 0,
						 TRUE,
						 DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
	{
		ereport(LOG,
				(errmsg_internal("could not duplicate handle to be written to backend parameter file: error code %lu",
								 GetLastError())));
		return false;
	}

	*dest = hChild;
	return true;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static bool
write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
{
	dest->origsocket = src;
	if (src != 0 && src != PGINVALID_SOCKET)
	{
		/* Actual socket */
		if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0)
		{
			ereport(LOG,
					(errmsg("could not duplicate socket %d for use in backend: error code %d",
							(int) src, WSAGetLastError())));
			return false;
		}
	}
	return true;
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void
read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
{
	SOCKET		s;

	if (src->origsocket == PGINVALID_SOCKET || src->origsocket == 0)
	{
		/* Not a real socket! */
		*dest = src->origsocket;
	}
	else
	{
		/* Actual socket, so create from structure */
		s = WSASocket(FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  &src->wsainfo,
					  0,
					  0);
		if (s == INVALID_SOCKET)
		{
			write_stderr("could not create inherited socket: error code %d\n",
						 WSAGetLastError());
			exit(1);
		}
		*dest = s;

		/*
		 * To make sure we don't get two references to the same socket, close
		 * the original one. (This would happen when inheritance actually
		 * works..
		 */
		closesocket(src->origsocket);
	}
}
#endif

static void
read_backend_variables(char *id, Port *port)
{
	BackendParameters param;

#ifndef WIN32
	/* Non-win32 implementation reads from file */
	FILE	   *fp;

	/* Open file */
	fp = AllocateFile(id, PG_BINARY_R);
	if (!fp)
	{
		write_stderr("could not open backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	if (fread(&param, sizeof(param), 1, fp) != 1)
	{
		write_stderr("could not read from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	/* Release file */
	FreeFile(fp);
	if (unlink(id) != 0)
	{
		write_stderr("could not remove file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}
#else
	/* Win32 version uses mapped file */
	HANDLE		paramHandle;
	BackendParameters *paramp;

#ifdef _WIN64
	paramHandle = (HANDLE) _atoi64(id);
#else
	paramHandle = (HANDLE) atol(id);
#endif
	paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
	if (!paramp)
	{
		write_stderr("could not map view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	memcpy(&param, paramp, sizeof(BackendParameters));

	if (!UnmapViewOfFile(paramp))
	{
		write_stderr("could not unmap view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	if (!CloseHandle(paramHandle))
	{
		write_stderr("could not close handle to backend parameter variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}
#endif

	restore_backend_variables(&param, port);
}

/* Restore critical backend variables from the BackendParameters struct */
static void
restore_backend_variables(BackendParameters *param, Port *port)
{
	memcpy(port, &param->port, sizeof(Port));
	read_inheritable_socket(&port->sock, &param->portsocket);

	SetDataDir(param->DataDir);

	memcpy(&ListenSocket, &param->ListenSocket, sizeof(ListenSocket));

	MyCancelKey = param->MyCancelKey;
	MyPMChildSlot = param->MyPMChildSlot;

#ifdef WIN32
	ShmemProtectiveRegion = param->ShmemProtectiveRegion;
#endif
	UsedShmemSegID = param->UsedShmemSegID;
	UsedShmemSegAddr = param->UsedShmemSegAddr;

	ShmemLock = param->ShmemLock;
	ShmemVariableCache = param->ShmemVariableCache;
	ShmemBackendArray = param->ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	SpinlockSemaArray = param->SpinlockSemaArray;
#endif
	NamedLWLockTrancheRequests = param->NamedLWLockTrancheRequests;
	NamedLWLockTrancheArray = param->NamedLWLockTrancheArray;
	MainLWLockArray = param->MainLWLockArray;
	ProcStructLock = param->ProcStructLock;
	ProcGlobal = param->ProcGlobal;
	AuxiliaryProcs = param->AuxiliaryProcs;
	PreparedXactProcs = param->PreparedXactProcs;
	PMSignalState = param->PMSignalState;
	read_inheritable_socket(&pgStatSock, &param->pgStatSock);

	PostmasterPid = param->PostmasterPid;
	PgStartTime = param->PgStartTime;
	PgReloadTime = param->PgReloadTime;
	first_syslogger_file_time = param->first_syslogger_file_time;

	redirection_done = param->redirection_done;
	IsBinaryUpgrade = param->IsBinaryUpgrade;
	max_safe_fds = param->max_safe_fds;

	MaxBackends = param->MaxBackends;

	/* POLAR */
	MaxNormalBackends = param->MaxNormalBackends;

#ifdef WIN32
	PostmasterHandle = param->PostmasterHandle;
	pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#else
	memcpy(&postmaster_alive_fds, &param->postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

	strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

	strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

	strlcpy(ExtraOptions, param->ExtraOptions, MAXPGPATH);
}


Size
ShmemBackendArraySize(void)
{
	return mul_size(MaxLivePostmasterChildren(), sizeof(Backend));
}

void
ShmemBackendArrayAllocation(void)
{
	Size		size = ShmemBackendArraySize();

	ShmemBackendArray = (Backend *) ShmemAlloc(size);
	/* Mark all slots as empty */
	memset(ShmemBackendArray, 0, size);
}

static void
ShmemBackendArrayAdd(Backend *bn)
{
	/* The array slot corresponding to my PMChildSlot should be free */
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == 0);
	ShmemBackendArray[i] = *bn;
}

static void
ShmemBackendArrayRemove(Backend *bn)
{
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == bn->pid);
	/* Mark the slot as empty */
	ShmemBackendArray[i].pid = 0;
}
#endif							/* EXEC_BACKEND */


#ifdef WIN32

/*
 * Subset implementation of waitpid() for Windows.  We assume pid is -1
 * (that is, check all child processes) and options is WNOHANG (don't wait).
 */
static pid_t
waitpid(pid_t pid, int *exitstatus, int options)
{
	DWORD		dwd;
	ULONG_PTR	key;
	OVERLAPPED *ovl;

	/*
	 * Check if there are any dead children. If there are, return the pid of
	 * the first one that died.
	 */
	if (GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0))
	{
		*exitstatus = (int) key;
		return dwd;
	}

	return -1;
}

/*
 * Note! Code below executes on a thread pool! All operations must
 * be thread safe! Note that elog() and friends must *not* be used.
 */
static void WINAPI
pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
{
	win32_deadchild_waitinfo *childinfo = (win32_deadchild_waitinfo *) lpParameter;
	DWORD		exitcode;

	if (TimerOrWaitFired)
		return;					/* timeout. Should never happen, since we use
								 * INFINITE as timeout value. */

	/*
	 * Remove handle from wait - required even though it's set to wait only
	 * once
	 */
	UnregisterWaitEx(childinfo->waitHandle, NULL);

	if (!GetExitCodeProcess(childinfo->procHandle, &exitcode))
	{
		/*
		 * Should never happen. Inform user and set a fixed exitcode.
		 */
		write_stderr("could not read exit code for process\n");
		exitcode = 255;
	}

	if (!PostQueuedCompletionStatus(win32ChildQueue, childinfo->procId, (ULONG_PTR) exitcode, NULL))
		write_stderr("could not post child completion status\n");

	/*
	 * Handle is per-process, so we close it here instead of in the
	 * originating thread
	 */
	CloseHandle(childinfo->procHandle);

	/*
	 * Free struct that was allocated before the call to
	 * RegisterWaitForSingleObject()
	 */
	free(childinfo);

	/* Queue SIGCHLD signal */
	pg_queue_signal(SIGCHLD);
}
#endif							/* WIN32 */

/*
 * Initialize one and only handle for monitoring postmaster death.
 *
 * Called once in the postmaster, so that child processes can subsequently
 * monitor if their parent is dead.
 */
static void
InitPostmasterDeathWatchHandle(void)
{
#ifndef WIN32

	/*
	 * Create a pipe. Postmaster holds the write end of the pipe open
	 * (POSTMASTER_FD_OWN), and children hold the read end. Children can pass
	 * the read file descriptor to select() to wake up in case postmaster
	 * dies, or check for postmaster death with a (read() == 0). Children must
	 * close the write end as soon as possible after forking, because EOF
	 * won't be signaled in the read end until all processes have closed the
	 * write fd. That is taken care of in ClosePostmasterPorts().
	 */
	Assert(MyProcPid == PostmasterPid);
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create pipe to monitor postmaster death: %m")));

	/*
	 * Set O_NONBLOCK to allow testing for the fd's presence with a read()
	 * call.
	 */
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));
#else

	/*
	 * On Windows, we use a process handle for the same purpose.
	 */
	if (DuplicateHandle(GetCurrentProcess(),
						GetCurrentProcess(),
						GetCurrentProcess(),
						&PostmasterHandle,
						0,
						TRUE,
						DUPLICATE_SAME_ACCESS) == 0)
		ereport(FATAL,
				(errmsg_internal("could not duplicate postmaster handle: error code %lu",
								 GetLastError())));
#endif							/* WIN32 */
}

/*
 * POLAR: Construct SockAddr according to host and port.
 * The input parameters should be guaranteed to be valid.
 *
 * This func can only support ipv4 for now.
 *
 * The output parameter sock will be modified even when fail
 * encoding.
 */
static bool
polar_encode_origin_conn(char *host, char *port, SockAddr* sock)
{
#define polar_set_sock_port(is_v6, sock, port_num) \
	do { \
		if (is_v6) \
			((struct sockaddr_in6 *)&(sock)->addr)->sin6_port = htons((uint16)(port_num)); \
		else \
			((struct sockaddr_in *)&(sock)->addr)->sin_port = htons((uint16)(port_num)); \
	} while(0)
#define polar_set_sock_addr(is_v6, sock, host, ret) \
	do { \
		if (is_v6) \
			(ret) = inet_pton(AF_INET6, (host), &((struct sockaddr_in6 *)&(sock)->addr)->sin6_addr); \
		else \
			(ret) = inet_pton(AF_INET, (host), &((struct sockaddr_in *)&(sock)->addr)->sin_addr); \
	} while(0)

	long port_num;
	int ret;
	bool ipv6 = false;

	if (strchr(host, ':') != NULL)
		ipv6 = true;

	port_num = strtol(port, NULL, 10);

	if (errno == ERANGE || port_num < 0 || port_num > PG_UINT16_MAX)
		return false;
	
	polar_set_sock_port(ipv6, sock, port_num);
	polar_set_sock_addr(ipv6, sock, host, ret);
	if (ret != 1)
		return false;

	sock->addr.ss_family = ipv6 ? AF_INET6 : AF_INET;
	sock->salen = sizeof(sock->addr);

	return true;
#undef polar_set_sock_port
#undef polar_set_sock_addr
}

bool
polar_have_crashed_worker(void)
{
	return HaveCrashedWorker;
}

/* POLAR: do some check, leave log for debug */
static void
polar_state_machine_change_check(polar_pmstate_change_reason reason, pid_t pid)
{
	switch (reason)
	{
		case PM_RECEIVE_SIGINT:
			elog(LOG, "polardb prepare reinitializing after receive SIGINT");
			break;

		case PM_RECEIVE_SIGTERM:
			elog(LOG, "polardb prepare reinitializing after receive SIGTERM");
			break;

		case PM_RECEIVE_SIGQUIT:
			elog(LOG, "polardb prepare reinitializing after receive SIGQUIT");
			break;

		case PM_ADVANCE_STATE_MACHINE:
			elog(LOG, "polardb prepare reinitializing after sigusr1_handler advance");
			break;

		case PM_SUBPROCESS_DIE:
			elog(LOG, "polardb prepare reinitializing after subprocess %d die", pid);
			break;

		default:
			elog(ERROR,"polardb unknown state machine change reason %d", reason);
			break;
	}

	return;
}

static void
polar_load_polar_node_static_config(bool force_create)
{
	char		path[MAXPGPATH] = {0};
	struct 		stat stat_buf;
	snprintf(path, sizeof(path), "%s/%s", DataDir, PLOAR_NODE_CONF_FILENAME);

	if (stat(path, &stat_buf) != 0)  
	{
		if (!force_create)
			return;
		polar_create_polar_node_static_config(path);
		ereport(LOG,
			(errmsg("%s does not exist, we create it, This is best done on "
					"the first boot and not on restart", path)));
	}
	else
	{
		polar_process_polar_node_static_config();
	}
}

/* POLAR: Track and log postmaster's state. */
static void
polar_log_pmstate(void)
{
	switch(pmState)
	{
		case PM_INIT:
			elog(LOG, "postmaster is in PM_INIT state.");
			break;
		case PM_STARTUP:
			elog(LOG, "postmaster is in PM_STARTUP state.");
			break;
		case PM_RECOVERY:
			elog(LOG, "postmaster is in PM_RECOVERY state.");
			break;
		case PM_HOT_STANDBY:
			elog(LOG, "postmaster is in PM_HOT_STANDBY state.");
			break;
		case PM_RUN:
			elog(LOG, "postmaster is in PM_RUN state.");
			break;
		case PM_WAIT_BACKUP:
			elog(LOG, "postmaster is in PM_WAIT_BACKUP state.");
			break;
		case PM_WAIT_READONLY:
			elog(LOG, "postmaster is in PM_WAIT_READONLY state.");
			break;
		case PM_WAIT_BACKENDS:
			elog(LOG, "postmaster is in PM_WAIT_BACKENDS state.");
			break;
		case PM_SHUTDOWN:
			elog(LOG, "postmaster is in PM_SHUTDOWN state.");
			break;
		case PM_SHUTDOWN_2:
			elog(LOG, "postmaster is in PM_SHUTDOWN_2 state.");
			break;
		case PM_WAIT_DEAD_END:
			elog(LOG, "postmaster is in PM_WAIT_DEAD_END state.");
			break;
		case PM_NO_CHILDREN:
			elog(LOG, "postmaster is in PM_NO_CHILDREN state.");
			break;
		case PM_DATAMAX:
			elog(LOG, "postmaster is in PM_DATAMAX state.");
			break;
		default:
			elog(LOG, "postmaster is in unknown state.");
			break;
	}
}

static int
polar_postmaster_find_background_worker(const char *bgw_name)
{
	int pid = 0;

	slist_mutable_iter iter;

	slist_foreach_modify(iter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);

		if (rw->rw_pid == 0)
			continue;

		if (strcmp(rw->rw_worker.bgw_name, bgw_name) != 0)
			continue;

		pid = rw->rw_pid;
		break;
	}

	return pid;
}

static void
polar_postmaster_online_promote(void)
{
	int polar_worker;

	static bool promoted = false;

	if (promoted)
		return;

	promoted = true;

	/* Refuse any connection until startup replay all wal to logindex table */
	pmState = PM_STARTUP;
	polar_log_pmstate();

	/* Terminate any existing backend process */
	SignalSomeChildren(SIGTERM, BACKEND_TYPE_NORMAL, true);

	/* remount shared storage from ro to rw */
	polar_remount();

	/* Tell startup process to finish recovery */
	signal_child(StartupPID, SIGUSR2);

	/* Tell polar worker process that is doing online promote */
	polar_worker = polar_postmaster_find_background_worker(POLAR_WORKER_PROCESS_NAME);
	if (polar_worker != 0)
		signal_child(polar_worker, SIGUSR2);

	signal_child(LogIndexBgPID, SIGUSR2);
}


/*
 * POLAR: set value of SendStop according to guc parameter polar_enable_send_stop
 * so that it can be set not only by specifying -T when start postgres
 */
void
polar_assign_enable_send_stop(bool newval, void *extra)
{
	SendStop = newval;
}
