/*-------------------------------------------------------------------------
 *
 * miscinit.c
 *	  miscellaneous initialization support stuff
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/miscinit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/param.h>
#include <signal.h>
#include <time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <grp.h>
#include <pwd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif

#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "common/file_perm.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pidfile.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

/* POLAR */
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/polar_csnlog.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "storage/polar_fd.h"
#include "utils/polar_local_cache.h"

#define DIRECTORY_LOCK_FILE		"postmaster.pid"

/* POLAR */
#define POLAR_SHMEM_FILE		"polar_shmem"
#define polar_shmem_file_exists(status) \
	(status == SHMEM_FILE_INVALID || status == SHMEM_FILE_SHM_NOT_IN_USE)

ProcessingMode Mode = InitProcessing;

/* List of lock files to be removed at proc exit */
static List *lock_files = NIL;

static Latch LocalLatchData;

/* POLAR */
static void polar_remove_old_shmem_info_file(void);
static PolarShmemFileStatus polar_shmem_file_status(unsigned long *key, unsigned long *id);
static void polar_remove_local_cache(void);

/* ----------------------------------------------------------------
 *		ignoring system indexes support stuff
 *
 * NOTE: "ignoring system indexes" means we do not use the system indexes
 * for lookups (either in hardwired catalog accesses or in planner-generated
 * plans).  We do, however, still update the indexes when a catalog
 * modification is made.
 * ----------------------------------------------------------------
 */

bool		IgnoreSystemIndexes = false;


/* ----------------------------------------------------------------
 *				database path / name support stuff
 * ----------------------------------------------------------------
 */

void
SetDatabasePath(const char *path)
{
	/* This should happen only once per process */
	Assert(!DatabasePath);
	DatabasePath = MemoryContextStrdup(TopMemoryContext, path);
}

/*
 * Validate the proposed data directory.
 *
 * Also initialize file and directory create modes and mode mask.
 */
void
checkDataDir(void)
{
	struct stat stat_buf;

	Assert(DataDir);

	if (stat(DataDir, &stat_buf) != 0)
	{
		if (errno == ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("data directory \"%s\" does not exist",
							DataDir)));
		else
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read permissions of directory \"%s\": %m",
							DataDir)));
	}

	/* eventual chdir would fail anyway, but let's test ... */
	if (!S_ISDIR(stat_buf.st_mode))
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("specified data directory \"%s\" is not a directory",
						DataDir)));

	/*
	 * Check that the directory belongs to my userid; if not, reject.
	 *
	 * This check is an essential part of the interlock that prevents two
	 * postmasters from starting in the same directory (see CreateLockFile()).
	 * Do not remove or weaken it.
	 *
	 * XXX can we safely enable this check on Windows?
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (stat_buf.st_uid != geteuid())
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data directory \"%s\" has wrong ownership",
						DataDir),
				 errhint("The server must be started by the user that owns the data directory.")));
#endif

	/*
	 * Check if the directory has correct permissions.  If not, reject.
	 *
	 * Only two possible modes are allowed, 0700 and 0750.  The latter mode
	 * indicates that group read/execute should be allowed on all newly
	 * created files and directories.
	 *
	 * XXX temporarily suppress check when on Windows, because there may not
	 * be proper support for Unix-y file permissions.  Need to think of a
	 * reasonable check to apply on Windows.
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (stat_buf.st_mode & PG_MODE_MASK_GROUP)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data directory \"%s\" has invalid permissions",
						DataDir),
				 errdetail("Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).")));
#endif

	/*
	 * Reset creation modes and mask based on the mode of the data directory.
	 *
	 * The mask was set earlier in startup to disallow group permissions on
	 * newly created files and directories.  However, if group read/execute
	 * are present on the data directory then modify the create modes and mask
	 * to allow group read/execute on newly created files and directories and
	 * set the data_directory_mode GUC.
	 *
	 * Suppress when on Windows, because there may not be proper support for
	 * Unix-y file permissions.
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	SetDataDirectoryCreatePerm(stat_buf.st_mode);

	umask(pg_mode_mask);
	data_directory_mode = pg_dir_create_mode;
#endif

	/* Check for PG_VERSION */
	ValidatePgVersion(DataDir);
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set DataDir directly.
 */
void
SetDataDir(const char *dir)
{
	char	   *new;

	AssertArg(dir);

	/* If presented path is relative, convert to absolute */
	new = make_absolute_path(dir);

	if (DataDir)
		free(DataDir);
	DataDir = new;
}

/*
 * Change working directory to DataDir.  Most of the postmaster and backend
 * code assumes that we are in DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
void
ChangeToDataDir(void)
{
	AssertState(DataDir);

	if (chdir(DataDir) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not change directory to \"%s\": %m",
						DataDir)));
}


/* ----------------------------------------------------------------
 *	User ID state
 *
 * We have to track several different values associated with the concept
 * of "user ID".
 *
 * AuthenticatedUserId is determined at connection start and never changes.
 *
 * SessionUserId is initially the same as AuthenticatedUserId, but can be
 * changed by SET SESSION AUTHORIZATION (if AuthenticatedUserIsSuperuser).
 * This is the ID reported by the SESSION_USER SQL function.
 *
 * OuterUserId is the current user ID in effect at the "outer level" (outside
 * any transaction or function).  This is initially the same as SessionUserId,
 * but can be changed by SET ROLE to any role that SessionUserId is a
 * member of.  (XXX rename to something like CurrentRoleId?)
 *
 * CurrentUserId is the current effective user ID; this is the one to use
 * for all normal permissions-checking purposes.  At outer level this will
 * be the same as OuterUserId, but it changes during calls to SECURITY
 * DEFINER functions, as well as locally in some specialized commands.
 *
 * SecurityRestrictionContext holds flags indicating reason(s) for changing
 * CurrentUserId.  In some cases we need to lock down operations that are
 * not directly controlled by privilege settings, and this provides a
 * convenient way to do it.
 * ----------------------------------------------------------------
 */
static Oid	AuthenticatedUserId = InvalidOid;
static Oid	SessionUserId = InvalidOid;
static Oid	OuterUserId = InvalidOid;
static Oid	CurrentUserId = InvalidOid;

/* We also have to remember the superuser state of some of these levels */
static bool AuthenticatedUserIsSuperuser = false;
static bool SessionUserIsSuperuser = false;

static int	SecurityRestrictionContext = 0;

/* We also remember if a SET ROLE is currently active */
static bool SetRoleIsActive = false;

/*
 * Initialize the basic environment for a postmaster child
 *
 * Should be called as early as possible after the child's startup.
 */
void
InitPostmasterChild(void)
{
	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

	MySessionPid = MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);	/* set our start time in case we call elog */

	/*
	 * make sure stderr is in binary mode before anything can possibly be
	 * written to it, in case it's actually the syslogger pipe, so the pipe
	 * chunking protocol isn't disturbed. Non-logpipe data gets translated on
	 * redirection (e.g. via pg_ctl -l) anyway.
	 */
#ifdef WIN32
	_setmode(fileno(stderr), _O_BINARY);
#endif

	/* We don't want the postmaster's proc_exit() handlers */
	on_exit_reset();

	/*
	 * POLAR: pfs tls init when subprocess forked
	 * we need set cleanup function.
	 */
	polar_register_tls_cleanup();

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	MyLatch = &LocalLatchData;
	InitLatch(MyLatch);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too. Not all processes will have
	 * children, but for consistency we make all postmaster child processes do
	 * this.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif
}

/*
 * Initialize the basic environment for a standalone process.
 *
 * argv0 has to be suitable to find the program's executable.
 */
void
InitStandaloneProcess(const char *argv0)
{
	Assert(!IsPostmasterEnvironment);

	MySessionPid = MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);	/* set our start time in case we call elog */

	/*
	 * Initialize random() for the first time, like PostmasterMain() would.
	 * In a regular IsUnderPostmaster backend, BackendRun() computes a
	 * high-entropy seed before any user query.  Fewer distinct initial seeds
	 * can occur here.
	 */
	srandom((unsigned int) (MyProcPid ^ MyStartTime));

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	MyLatch = &LocalLatchData;
	InitLatch(MyLatch);

	/* Compute paths, no postmaster to inherit from */
	if (my_exec_path[0] == '\0')
	{
		if (find_my_exec(argv0, my_exec_path) < 0)
			elog(FATAL, "%s: could not locate my own executable path",
				 argv0);
	}

	if (pkglib_path[0] == '\0')
		get_pkglib_path(my_exec_path, pkglib_path);
}

void
SwitchToSharedLatch(void)
{
	Assert(MyLatch == &LocalLatchData);
	Assert(MyProc != NULL);

	MyLatch = &MyProc->procLatch;

	if (FeBeWaitSet)
		ModifyWaitEvent(FeBeWaitSet, 1, WL_LATCH_SET, MyLatch);

	/*
	 * Set the shared latch as the local one might have been set. This
	 * shouldn't normally be necessary as code is supposed to check the
	 * condition before waiting for the latch, but a bit care can't hurt.
	 */
	SetLatch(MyLatch);
}

void
SwitchBackToLocalLatch(void)
{
	Assert(MyLatch != &LocalLatchData);
	Assert(MyProc != NULL && MyLatch == &MyProc->procLatch);

	MyLatch = &LocalLatchData;

	if (FeBeWaitSet)
		ModifyWaitEvent(FeBeWaitSet, 1, WL_LATCH_SET, MyLatch);

	SetLatch(MyLatch);
}

/*
 * GetUserId - get the current effective user ID.
 *
 * Note: there's no SetUserId() anymore; use SetUserIdAndSecContext().
 */
Oid
GetUserId(void)
{
	AssertState(OidIsValid(CurrentUserId));
	return CurrentUserId;
}


/*
 * GetOuterUserId/SetOuterUserId - get/set the outer-level user ID.
 */
Oid
GetOuterUserId(void)
{
	AssertState(OidIsValid(OuterUserId));
	return OuterUserId;
}


static void
SetOuterUserId(Oid userid)
{
	AssertState(SecurityRestrictionContext == 0);
	AssertArg(OidIsValid(userid));
	OuterUserId = userid;

	/* We force the effective user ID to match, too */
	CurrentUserId = userid;
}


/*
 * GetSessionUserId/SetSessionUserId - get/set the session user ID.
 */
Oid
GetSessionUserId(void)
{
	AssertState(OidIsValid(SessionUserId));
	return SessionUserId;
}


void
SetSessionUserId(Oid userid, bool is_superuser)
{
	AssertState(SecurityRestrictionContext == 0);
	AssertArg(OidIsValid(userid));
	SessionUserId = userid;
	SessionUserIsSuperuser = is_superuser;
	SetRoleIsActive = false;

	/* We force the effective user IDs to match, too */
	OuterUserId = userid;
	CurrentUserId = userid;
}

/*
 * GetAuthenticatedUserId - get the authenticated user ID
 */
Oid
GetAuthenticatedUserId(void)
{
	AssertState(OidIsValid(AuthenticatedUserId));
	return AuthenticatedUserId;
}


/*
 * GetUserIdAndSecContext/SetUserIdAndSecContext - get/set the current user ID
 * and the SecurityRestrictionContext flags.
 *
 * Currently there are three valid bits in SecurityRestrictionContext:
 *
 * SECURITY_LOCAL_USERID_CHANGE indicates that we are inside an operation
 * that is temporarily changing CurrentUserId via these functions.  This is
 * needed to indicate that the actual value of CurrentUserId is not in sync
 * with guc.c's internal state, so SET ROLE has to be disallowed.
 *
 * SECURITY_RESTRICTED_OPERATION indicates that we are inside an operation
 * that does not wish to trust called user-defined functions at all.  The
 * policy is to use this before operations, e.g. autovacuum and REINDEX, that
 * enumerate relations of a database or schema and run functions associated
 * with each found relation.  The relation owner is the new user ID.  Set this
 * as soon as possible after locking the relation.  Restore the old user ID as
 * late as possible before closing the relation; restoring it shortly after
 * close is also tolerable.  If a command has both relation-enumerating and
 * non-enumerating modes, e.g. ANALYZE, both modes set this bit.  This bit
 * prevents not only SET ROLE, but various other changes of session state that
 * normally is unprotected but might possibly be used to subvert the calling
 * session later.  An example is replacing an existing prepared statement with
 * new code, which will then be executed with the outer session's permissions
 * when the prepared statement is next used.  These restrictions are fairly
 * draconian, but the functions called in relation-enumerating operations are
 * really supposed to be side-effect-free anyway.
 *
 * SECURITY_NOFORCE_RLS indicates that we are inside an operation which should
 * ignore the FORCE ROW LEVEL SECURITY per-table indication.  This is used to
 * ensure that FORCE RLS does not mistakenly break referential integrity
 * checks.  Note that this is intentionally only checked when running as the
 * owner of the table (which should always be the case for referential
 * integrity checks).
 *
 * Unlike GetUserId, GetUserIdAndSecContext does *not* Assert that the current
 * value of CurrentUserId is valid; nor does SetUserIdAndSecContext require
 * the new value to be valid.  In fact, these routines had better not
 * ever throw any kind of error.  This is because they are used by
 * StartTransaction and AbortTransaction to save/restore the settings,
 * and during the first transaction within a backend, the value to be saved
 * and perhaps restored is indeed invalid.  We have to be able to get
 * through AbortTransaction without asserting in case InitPostgres fails.
 */
void
GetUserIdAndSecContext(Oid *userid, int *sec_context)
{
	*userid = CurrentUserId;
	*sec_context = SecurityRestrictionContext;
}

void
SetUserIdAndSecContext(Oid userid, int sec_context)
{
	CurrentUserId = userid;
	SecurityRestrictionContext = sec_context;
}


/*
 * InLocalUserIdChange - are we inside a local change of CurrentUserId?
 */
bool
InLocalUserIdChange(void)
{
	return (SecurityRestrictionContext & SECURITY_LOCAL_USERID_CHANGE) != 0;
}

/*
 * InSecurityRestrictedOperation - are we inside a security-restricted command?
 */
bool
InSecurityRestrictedOperation(void)
{
	return (SecurityRestrictionContext & SECURITY_RESTRICTED_OPERATION) != 0;
}

/*
 * InNoForceRLSOperation - are we ignoring FORCE ROW LEVEL SECURITY ?
 */
bool
InNoForceRLSOperation(void)
{
	return (SecurityRestrictionContext & SECURITY_NOFORCE_RLS) != 0;
}


/*
 * These are obsolete versions of Get/SetUserIdAndSecContext that are
 * only provided for bug-compatibility with some rather dubious code in
 * pljava.  We allow the userid to be set, but only when not inside a
 * security restriction context.
 */
void
GetUserIdAndContext(Oid *userid, bool *sec_def_context)
{
	*userid = CurrentUserId;
	*sec_def_context = InLocalUserIdChange();
}

void
SetUserIdAndContext(Oid userid, bool sec_def_context)
{
	/* We throw the same error SET ROLE would. */
	if (InSecurityRestrictedOperation())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot set parameter \"%s\" within security-restricted operation",
						"role")));
	CurrentUserId = userid;
	if (sec_def_context)
		SecurityRestrictionContext |= SECURITY_LOCAL_USERID_CHANGE;
	else
		SecurityRestrictionContext &= ~SECURITY_LOCAL_USERID_CHANGE;
}


/*
 * Check whether specified role has explicit REPLICATION privilege
 */
bool
has_rolreplication(Oid roleid)
{
	bool		result = false;
	HeapTuple	utup;

	utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (HeapTupleIsValid(utup))
	{
		result = ((Form_pg_authid) GETSTRUCT(utup))->rolreplication;
		ReleaseSysCache(utup);
	}
	return result;
}

/*
 * Initialize user identity during normal backend startup
 */
void
InitializeSessionUserId(const char *rolename, Oid roleid)
{
	HeapTuple	roleTup;
	Form_pg_authid rform;
	char	   *rname;

	/*
	 * Don't do scans if we're bootstrapping, none of the system catalogs
	 * exist yet, and they should be owned by postgres anyway.
	 */
	AssertState(!IsBootstrapProcessingMode());

	/* call only once */
	AssertState(!OidIsValid(AuthenticatedUserId));

	if (rolename != NULL)
	{
		roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
		if (!HeapTupleIsValid(roleTup))
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role \"%s\" does not exist", rolename)));
	}
	else
	{
		roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
		if (!HeapTupleIsValid(roleTup))
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role with OID %u does not exist", roleid)));
	}

	rform = (Form_pg_authid) GETSTRUCT(roleTup);
	roleid = HeapTupleGetOid(roleTup);
	rname = NameStr(rform->rolname);

	AuthenticatedUserId = roleid;
	AuthenticatedUserIsSuperuser = rform->rolsuper;

	/* This sets OuterUserId/CurrentUserId too */
	SetSessionUserId(roleid, AuthenticatedUserIsSuperuser);

	/* Also mark our PGPROC entry with the authenticated user id */
	/* (We assume this is an atomic store so no lock is needed) */
	MyProc->roleId = roleid;

	/* POLAR: mark role is superuser? */
	MyProc->issuper = rform->rolsuper;
	/* POLAR end */

	/*
	 * These next checks are not enforced when in standalone mode, so that
	 * there is a way to recover from sillinesses like "UPDATE pg_authid SET
	 * rolcanlogin = false;".
	 */
	if (IsUnderPostmaster)
	{
		/*
		 * Is role allowed to login at all?
		 */
		if (!rform->rolcanlogin)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role \"%s\" is not permitted to log in",
							rname)));

		/*
		 * Check connection limit for this role.
		 *
		 * There is a race condition here --- we create our PGPROC before
		 * checking for other PGPROCs.  If two backends did this at about the
		 * same time, they might both think they were over the limit, while
		 * ideally one should succeed and one fail.  Getting that to work
		 * exactly seems more trouble than it is worth, however; instead we
		 * just document that the connection limit is approximate.
		 */
		if (rform->rolconnlimit >= 0 &&
			!AuthenticatedUserIsSuperuser &&
			CountUserBackends(roleid) > rform->rolconnlimit)
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("too many connections for role \"%s\"",
							rname)));
	}

	/* Record username and superuser status as GUC settings too */
	SetConfigOption("session_authorization", rname,
					PGC_BACKEND, PGC_S_OVERRIDE);
	SetConfigOption("is_superuser",
					AuthenticatedUserIsSuperuser ? "on" : "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);

	ReleaseSysCache(roleTup);
}


/*
 * Initialize user identity during special backend startup
 */
void
InitializeSessionUserIdStandalone(void)
{
	/*
	 * This function should only be called in single-user mode, in autovacuum
	 * workers, and in background workers.
	 */
	AssertState(!IsUnderPostmaster || IsAutoVacuumWorkerProcess() || IsBackgroundWorker || IsPolarDispatcher);

	/* call only once */
	AssertState(!OidIsValid(AuthenticatedUserId));

	AuthenticatedUserId = BOOTSTRAP_SUPERUSERID;
	AuthenticatedUserIsSuperuser = true;

	SetSessionUserId(BOOTSTRAP_SUPERUSERID, true);
}


/*
 * Change session auth ID while running
 *
 * Only a superuser may set auth ID to something other than himself.  Note
 * that in case of multiple SETs in a single session, the original userid's
 * superuserness is what matters.  But we set the GUC variable is_superuser
 * to indicate whether the *current* session userid is a superuser.
 *
 * Note: this is not an especially clean place to do the permission check.
 * It's OK because the check does not require catalog access and can't
 * fail during an end-of-transaction GUC reversion, but we may someday
 * have to push it up into assign_session_authorization.
 */
void
SetSessionAuthorization(Oid userid, bool is_superuser)
{
	/* Must have authenticated already, else can't make permission check */
	AssertState(OidIsValid(AuthenticatedUserId));

	/* POLAR: polar_superuser is allowed change session auth id who is not superuser */
	if (userid != AuthenticatedUserId &&
		!AuthenticatedUserIsSuperuser &&
		!(polar_superuser_arg(AuthenticatedUserId) &&
		polar_has_more_privs_than_role(AuthenticatedUserId, userid)))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set session authorization")));

	SetSessionUserId(userid, is_superuser);

	SetConfigOption("is_superuser",
					is_superuser ? "on" : "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * Report current role id
 *		This follows the semantics of SET ROLE, ie return the outer-level ID
 *		not the current effective ID, and return InvalidOid when the setting
 *		is logically SET ROLE NONE.
 */
Oid
GetCurrentRoleId(void)
{
	if (SetRoleIsActive)
		return OuterUserId;
	else
		return InvalidOid;
}

/*
 * Change Role ID while running (SET ROLE)
 *
 * If roleid is InvalidOid, we are doing SET ROLE NONE: revert to the
 * session user authorization.  In this case the is_superuser argument
 * is ignored.
 *
 * When roleid is not InvalidOid, the caller must have checked whether
 * the session user has permission to become that role.  (We cannot check
 * here because this routine must be able to execute in a failed transaction
 * to restore a prior value of the ROLE GUC variable.)
 */
void
SetCurrentRoleId(Oid roleid, bool is_superuser)
{
	/*
	 * Get correct info if it's SET ROLE NONE
	 *
	 * If SessionUserId hasn't been set yet, just do nothing --- the eventual
	 * SetSessionUserId call will fix everything.  This is needed since we
	 * will get called during GUC initialization.
	 */
	if (!OidIsValid(roleid))
	{
		if (!OidIsValid(SessionUserId))
			return;

		roleid = SessionUserId;
		is_superuser = SessionUserIsSuperuser;

		SetRoleIsActive = false;
	}
	else
		SetRoleIsActive = true;

	SetOuterUserId(roleid);

	SetConfigOption("is_superuser",
					is_superuser ? "on" : "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);
}


/*
 * Get user name from user oid, returns NULL for nonexistent roleid if noerr
 * is true.
 */
char *
GetUserNameFromId(Oid roleid, bool noerr)
{
	HeapTuple	tuple;
	char	   *result;

	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (!HeapTupleIsValid(tuple))
	{
		if (!noerr)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("invalid role OID: %u", roleid)));
		result = NULL;
	}
	else
	{
		result = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
		ReleaseSysCache(tuple);
	}
	return result;
}


/*-------------------------------------------------------------------------
 *				Interlock-file support
 *
 * These routines are used to create both a data-directory lockfile
 * ($DATADIR/postmaster.pid) and Unix-socket-file lockfiles ($SOCKFILE.lock).
 * Both kinds of files contain the same info initially, although we can add
 * more information to a data-directory lockfile after it's created, using
 * AddToDataDirLockFile().  See pidfile.h for documentation of the contents
 * of these lockfiles.
 *
 * On successful lockfile creation, a proc_exit callback to remove the
 * lockfile is automatically created.
 *-------------------------------------------------------------------------
 */

/*
 * proc_exit callback to remove lockfiles.
 */
static void
UnlinkLockFiles(int status, Datum arg)
{
	ListCell   *l;

	foreach(l, lock_files)
	{
		char	   *curfile = (char *) lfirst(l);

		unlink(curfile);
		/* Should we complain if the unlink fails? */
	}
	/* Since we're about to exit, no need to reclaim storage */
	lock_files = NIL;

	/*
	 * Lock file removal should always be the last externally visible action
	 * of a postmaster or standalone backend, while we won't come here at all
	 * when exiting postmaster child processes.  Therefore, this is a good
	 * place to log completion of shutdown.  We could alternatively teach
	 * proc_exit() to do it, but that seems uglier.  In a standalone backend,
	 * use NOTICE elevel to be less chatty.
	 */
	ereport(IsPostmasterEnvironment ? LOG : NOTICE,
			(errmsg("database system is shut down")));
}

/*
 * Create a lockfile.
 *
 * filename is the path name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * socketDir is the Unix socket directory path to include (possibly empty).
 * isDDLock and refName are used to determine what error message to produce.
 */
static void
CreateLockFile(const char *filename, bool amPostmaster,
			   const char *socketDir,
			   bool isDDLock, const char *refName)
{
	int			fd;
	char		buffer[MAXPGPATH * 2 + 256];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		other_pid;
	pid_t		my_pid,
				my_p_pid,
				my_gp_pid;
	const char *envvar;

	/*
	 * If the PID in the lockfile is our own PID or our parent's or
	 * grandparent's PID, then the file must be stale (probably left over from
	 * a previous system boot cycle).  We need to check this because of the
	 * likelihood that a reboot will assign exactly the same PID as we had in
	 * the previous reboot, or one that's only one or two counts larger and
	 * hence the lockfile's PID now refers to an ancestor shell process.  We
	 * allow pg_ctl to pass down its parent shell PID (our grandparent PID)
	 * via the environment variable PG_GRANDPARENT_PID; this is so that
	 * launching the postmaster via pg_ctl can be just as reliable as
	 * launching it directly.  There is no provision for detecting
	 * further-removed ancestor processes, but if the init script is written
	 * carefully then all but the immediate parent shell will be root-owned
	 * processes and so the kill test will fail with EPERM.  Note that we
	 * cannot get a false negative this way, because an existing postmaster
	 * would surely never launch a competing postmaster or pg_ctl process
	 * directly.
	 */
	my_pid = getpid();

#ifndef WIN32
	my_p_pid = getppid();
#else

	/*
	 * Windows hasn't got getppid(), but doesn't need it since it's not using
	 * real kill() either...
	 */
	my_p_pid = 0;
#endif

	envvar = getenv("PG_GRANDPARENT_PID");
	if (envvar)
		my_gp_pid = atoi(envvar);
	else
		my_gp_pid = 0;

	/*
	 * We need a loop here because of race conditions.  But don't loop forever
	 * (for example, a non-writable $PGDATA directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600/0640.  See
		 * comments below.
		 */
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, pg_file_create_mode);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create lock file \"%s\": %m",
							filename)));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY, pg_file_create_mode);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open lock file \"%s\": %m",
							filename)));
		}
		pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_READ);
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read lock file \"%s\": %m",
							filename)));
		pgstat_report_wait_end();
		close(fd);

		if (len == 0)
		{
			ereport(FATAL,
					(errcode(ERRCODE_LOCK_FILE_EXISTS),
					 errmsg("lock file \"%s\" is empty", filename),
					 errhint("Either another server is starting, or the lock file is the remnant of a previous server startup crash.")));
		}

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);

		/* if pid < 0, the pid is for postgres, not postmaster */
		other_pid = (pid_t) (encoded_pid < 0 ? -encoded_pid : encoded_pid);

		if (other_pid <= 0)
			elog(FATAL, "bogus data in lock file \"%s\": \"%s\"",
				 filename, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * Per discussion above, my_pid, my_p_pid, and my_gp_pid can be
		 * ignored as false matches.
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist.
		 *
		 * We can treat the EPERM-error case as okay because that error
		 * implies that the existing process has a different userid than we
		 * do, which means it cannot be a competing postmaster.  A postmaster
		 * cannot successfully attach to a data directory owned by a userid
		 * other than its own, as enforced in checkDataDir(). Also, since we
		 * create the lockfiles mode 0600/0640, we'd have failed above if the
		 * lockfile belonged to another userid --- which means that whatever
		 * process kill() is reporting about isn't the one that made the
		 * lockfile.  (NOTE: this last consideration is the only one that
		 * keeps us from blowing away a Unix socket file belonging to an
		 * instance of Postgres being run by someone else, at least on
		 * machines where /tmp hasn't got a stickybit.)
		 */
		if (other_pid != my_pid && other_pid != my_p_pid &&
			other_pid != my_gp_pid)
		{
			if (kill(other_pid, 0) == 0 ||
				(errno != ESRCH && errno != EPERM))
			{
				/* lockfile belongs to a live process */
				ereport(FATAL,
						(errcode(ERRCODE_LOCK_FILE_EXISTS),
						 errmsg("lock file \"%s\" already exists",
								filename),
						 isDDLock ?
						 (encoded_pid < 0 ?
						  errhint("Is another postgres (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, refName) :
						  errhint("Is another postmaster (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, refName)) :
						 (encoded_pid < 0 ?
						  errhint("Is another postgres (PID %d) using socket file \"%s\"?",
								  (int) other_pid, refName) :
						  errhint("Is another postmaster (PID %d) using socket file \"%s\"?",
								  (int) other_pid, refName))));
			}
		}

		/*
		 * No, the creating process did not exist.  However, it could be that
		 * the postmaster crashed (or more likely was kill -9'd by a clueless
		 * admin) but has left orphan backends behind.  Check for this by
		 * looking to see if there is an associated shmem segment that is
		 * still in use.
		 *
		 * Note: because postmaster.pid is written in multiple steps, we might
		 * not find the shmem ID values in it; we can't treat that as an
		 * error.
		 */
		if (isDDLock)
		{
			char	   *ptr = buffer;
			unsigned long id1,
						id2;
			int			lineno;

			for (lineno = 1; lineno < LOCK_FILE_LINE_SHMEM_KEY; lineno++)
			{
				if ((ptr = strchr(ptr, '\n')) == NULL)
					break;
				ptr++;
			}

			if (ptr != NULL &&
				sscanf(ptr, "%lu %lu", &id1, &id2) == 2)
			{
				if (PGSharedMemoryIsInUse(id1, id2))
					ereport(FATAL,
							(errcode(ERRCODE_LOCK_FILE_EXISTS),
							 errmsg("pre-existing shared memory block (key %lu, ID %lu) is still in use",
									id1, id2),
							 errhint("Terminate any old server processes associated with data directory \"%s\".",
									 refName)));
			}
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.  Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(filename) < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove old lock file \"%s\": %m",
							filename),
					 errhint("The file seems accidentally left over, but "
							 "it could not be removed. Please remove the file "
							 "by hand and try again.")));
	}

	/*
	 * Successfully created the file, now fill it.  See comment in pidfile.h
	 * about the contents.  Note that we write the same first five lines into
	 * both datadir and socket lockfiles; although more stuff may get added to
	 * the datadir lockfile later.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n%s\n%ld\n%d\n%s\n",
			 amPostmaster ? (int) my_pid : -((int) my_pid),
			 DataDir,
			 (long) MyStartTime,
			 PostPortNumber,
			 socketDir);

	/*
	 * In a standalone backend, the next line (LOCK_FILE_LINE_LISTEN_ADDR)
	 * will never receive data, so fill it in as empty now.
	 */
	if (isDDLock && !amPostmaster)
		strlcat(buffer, "\n", sizeof(buffer));

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_WRITE);
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_SYNC);
	if (pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	pgstat_report_wait_end();
	if (close(fd) != 0)
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}

	/*
	 * Arrange to unlink the lock file(s) at proc_exit.  If this is the first
	 * one, set up the on_proc_exit function to do it; then add this lock file
	 * to the list of files to unlink.
	 */
	if (lock_files == NIL)
		on_proc_exit(UnlinkLockFiles, 0);

	/*
	 * Use lcons so that the lock files are unlinked in reverse order of
	 * creation; this is critical!
	 */
	lock_files = lcons(pstrdup(filename), lock_files);
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 *
 * Note that the socket directory path line is initially written as empty.
 * postmaster.c will rewrite it upon creating the first Unix socket.
 */
void
CreateDataDirLockFile(bool amPostmaster)
{
	CreateLockFile(DIRECTORY_LOCK_FILE, amPostmaster, "", true, DataDir);

	polar_create_shmem_info_file(amPostmaster);
}

/*
 * Create a lockfile for the specified Unix socket file.
 */
void
CreateSocketLockFile(const char *socketfile, bool amPostmaster,
					 const char *socketDir)
{
	char		lockfile[MAXPGPATH];

	snprintf(lockfile, sizeof(lockfile), "%s.lock", socketfile);
	CreateLockFile(lockfile, amPostmaster, socketDir, false, socketfile);
}

/*
 * TouchSocketLockFiles -- mark socket lock files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * lock files have a recent mod or access date.  That saves them
 * from being removed by overenthusiastic /tmp-directory-cleaner daemons.
 * (Another reason we should never have put the socket file in /tmp...)
 */
void
TouchSocketLockFiles(void)
{
	ListCell   *l;

	foreach(l, lock_files)
	{
		char	   *socketLockFile = (char *) lfirst(l);

		/* No need to touch the data directory lock file, we trust */
		if (strcmp(socketLockFile, DIRECTORY_LOCK_FILE) == 0)
			continue;

		/*
		 * utime() is POSIX standard, utimes() is a common alternative; if we
		 * have neither, fall back to actually reading the file (which only
		 * sets the access time not mod time, but that should be enough in
		 * most cases).  In all paths, we ignore errors.
		 */
#ifdef HAVE_UTIME
		utime(socketLockFile, NULL);
#else							/* !HAVE_UTIME */
#ifdef HAVE_UTIMES
		utimes(socketLockFile, NULL);
#else							/* !HAVE_UTIMES */
		int			fd;
		char		buffer[1];

		fd = open(socketLockFile, O_RDONLY | PG_BINARY, 0);
		if (fd >= 0)
		{
			read(fd, buffer, sizeof(buffer));
			close(fd);
		}
#endif							/* HAVE_UTIMES */
#endif							/* HAVE_UTIME */
	}
}


/*
 * Add (or replace) a line in the data directory lock file.
 * The given string should not include a trailing newline.
 *
 * Note: because we don't truncate the file, if we were to rewrite a line
 * with less data than it had before, there would be garbage after the last
 * line.  While we could fix that by adding a truncate call, that would make
 * the file update non-atomic, which we'd rather avoid.  Therefore, callers
 * should endeavor never to shorten a line once it's been written.
 */
void
AddToDataDirLockFile(int target_line, const char *str)
{
	int			fd;
	int			len;
	int			lineno;
	char	   *srcptr;
	char	   *destptr;
	char		srcbuffer[BLCKSZ];
	char		destbuffer[BLCKSZ];

	fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		return;
	}
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ);
	len = read(fd, srcbuffer, sizeof(srcbuffer) - 1);
	pgstat_report_wait_end();
	if (len < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return;
	}
	srcbuffer[len] = '\0';

	/*
	 * Advance over lines we are not supposed to rewrite, then copy them to
	 * destbuffer.
	 */
	srcptr = srcbuffer;
	for (lineno = 1; lineno < target_line; lineno++)
	{
		char	   *eol = strchr(srcptr, '\n');

		if (eol == NULL)
			break;				/* not enough lines in file yet */
		srcptr = eol + 1;
	}
	memcpy(destbuffer, srcbuffer, srcptr - srcbuffer);
	destptr = destbuffer + (srcptr - srcbuffer);

	/*
	 * Fill in any missing lines before the target line, in case lines are
	 * added to the file out of order.
	 */
	for (; lineno < target_line; lineno++)
	{
		if (destptr < destbuffer + sizeof(destbuffer))
			*destptr++ = '\n';
	}

	/*
	 * Write or rewrite the target line.
	 */
	snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s\n", str);
	destptr += strlen(destptr);

	/*
	 * If there are more lines in the old file, append them to destbuffer.
	 */
	if ((srcptr = strchr(srcptr, '\n')) != NULL)
	{
		srcptr++;
		snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s",
				 srcptr);
	}

	/*
	 * And rewrite the data.  Since we write in a single kernel call, this
	 * update should appear atomic to onlookers.
	 */
	len = strlen(destbuffer);
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE);
	if (lseek(fd, (off_t) 0, SEEK_SET) != 0 ||
		(int) write(fd, destbuffer, len) != len)
	{
		pgstat_report_wait_end();
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return;
	}
	pgstat_report_wait_end();
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC);
	if (pg_fsync(fd) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
	}
	pgstat_report_wait_end();
	if (close(fd) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
	}
}


/*
 * Recheck that the data directory lock file still exists with expected
 * content.  Return true if the lock file appears OK, false if it isn't.
 *
 * We call this periodically in the postmaster.  The idea is that if the
 * lock file has been removed or replaced by another postmaster, we should
 * do a panic database shutdown.  Therefore, we should return true if there
 * is any doubt: we do not want to cause a panic shutdown unnecessarily.
 * Transient failures like EINTR or ENFILE should not cause us to fail.
 * (If there really is something wrong, we'll detect it on a future recheck.)
 */
bool
RecheckDataDirLockFile(void)
{
	int			fd;
	int			len;
	long		file_pid;
	char		buffer[BLCKSZ];

	fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
	{
		/*
		 * There are many foreseeable false-positive error conditions.  For
		 * safety, fail only on enumerated clearly-something-is-wrong
		 * conditions.
		 */
		switch (errno)
		{
			case ENOENT:
			case ENOTDIR:
				/* disaster */
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m",
								DIRECTORY_LOCK_FILE)));
				return false;
			default:
				/* non-fatal, at least for now */
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m; continuing anyway",
								DIRECTORY_LOCK_FILE)));
				return true;
		}
	}
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ);
	len = read(fd, buffer, sizeof(buffer) - 1);
	pgstat_report_wait_end();
	if (len < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return true;			/* treat read failure as nonfatal */
	}
	buffer[len] = '\0';
	close(fd);
	file_pid = atol(buffer);
	if (file_pid == getpid())
		return true;			/* all is well */

	/* Trouble: someone's overwritten the lock file */
	ereport(LOG,
			(errmsg("lock file \"%s\" contains wrong PID: %ld instead of %ld",
					DIRECTORY_LOCK_FILE, file_pid, (long) getpid())));
	return false;
}


/*-------------------------------------------------------------------------
 *				Version checking support
 *-------------------------------------------------------------------------
 */

/*
 * Determine whether the PG_VERSION file in directory `path' indicates
 * a data version compatible with the version of this program.
 *
 * If compatible, return. Otherwise, ereport(FATAL).
 */
void
ValidatePgVersion(const char *path)
{
	char		full_path[MAXPGPATH];
	FILE	   *file;
	int			ret;
	long		file_major;
	long		my_major;
	char	   *endptr;
	char		file_version_string[64];
	const char *my_version_string = PG_VERSION;

	/* POLAR: FIXME not support fopen */
	if (POLAR_FILE_IN_SHARED_STORAGE())
		return;

	my_major = strtol(my_version_string, &endptr, 10);

	snprintf(full_path, sizeof(full_path), "%s/PG_VERSION", path);

	file = AllocateFile(full_path, "r");
	if (!file)
	{
		if (errno == ENOENT)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a valid data directory",
							path),
					 errdetail("File \"%s\" is missing.", full_path)));
		else
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", full_path)));
	}

	file_version_string[0] = '\0';
	ret = fscanf(file, "%63s", file_version_string);
	file_major = strtol(file_version_string, &endptr, 10);

	if (ret != 1 || endptr == file_version_string)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a valid data directory",
						path),
				 errdetail("File \"%s\" does not contain valid data.",
						   full_path),
				 errhint("You might need to initdb.")));

	FreeFile(file);

	if (my_major != file_major)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("database files are incompatible with server"),
				 errdetail("The data directory was initialized by PostgreSQL version %s, "
						   "which is not compatible with this version %s.",
						   file_version_string, my_version_string)));
}

/*-------------------------------------------------------------------------
 *				Library preload support
 *-------------------------------------------------------------------------
 */

/*
 * GUC variables: lists of library names to be preloaded at postmaster
 * start and at backend start
 */
char	   *session_preload_libraries_string = NULL;
char	   *shared_preload_libraries_string = NULL;
char	   *local_preload_libraries_string = NULL;

/* Flag telling that we are loading shared_preload_libraries */
bool		process_shared_preload_libraries_in_progress = false;

/* POLAR: used for shared libraries update when binary upgrade */
char *polar_internal_shared_preload_libraries = NULL;

/*
 * load the shared libraries listed in 'libraries'
 *
 * 'gucname': name of GUC variable, for error reports
 * 'restricted': if true, force libraries to be in $libdir/plugins/
 */
static void
load_libraries(const char *libraries, const char *gucname, bool restricted)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;

	if (libraries == NULL || libraries[0] == '\0')
		return;					/* nothing to do */

	/* Need a modifiable copy of string */
	rawstring = pstrdup(libraries);

	/* Parse string into list of filename paths */
	if (!SplitDirectoriesString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		list_free_deep(elemlist);
		pfree(rawstring);
		ereport(LOG,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid list syntax in parameter \"%s\"",
						gucname)));
		return;
	}

	foreach(l, elemlist)
	{
		/* Note that filename was already canonicalized */
		char	   *filename = (char *) lfirst(l);
		char	   *expanded = NULL;

		/* If restricting, insert $libdir/plugins if not mentioned already */
		if (restricted && first_dir_separator(filename) == NULL)
		{
			expanded = psprintf("$libdir/plugins/%s", filename);
			filename = expanded;
		}
		load_file(filename, restricted);
		ereport(DEBUG1,
				(errmsg("loaded library \"%s\"", filename)));
		if (expanded)
			pfree(expanded);
	}

	list_free_deep(elemlist);
	pfree(rawstring);
}

/*
 * process any libraries that should be preloaded at postmaster start
 */
void
process_shared_preload_libraries(void)
{
	process_shared_preload_libraries_in_progress = true;

	/*
	 * POLAR: we load the internal libraries with high priority, if
	 * the internal libraries have presented in shared_preload_libraries,
	 * it doesn't matter, it allows to call load_libraries with the same
	 * library many times, load_libraries will skip to load a library
	 * which has been loaded already
	 */
	load_libraries(polar_internal_shared_preload_libraries,
				   "polar_internal_shared_preload_libraries",
				   false);
	/* POLAR end */

	load_libraries(shared_preload_libraries_string,
				   "shared_preload_libraries",
				   false);
	process_shared_preload_libraries_in_progress = false;
}

/*
 * process any libraries that should be preloaded at backend start
 */
void
process_session_preload_libraries(void)
{
	load_libraries(session_preload_libraries_string,
				   "session_preload_libraries",
				   false);
	load_libraries(local_preload_libraries_string,
				   "local_preload_libraries",
				   true);
}

void
pg_bindtextdomain(const char *domain)
{
#ifdef ENABLE_NLS
	if (my_exec_path[0] != '\0')
	{
		char		locale_path[MAXPGPATH];

		get_locale_path(my_exec_path, locale_path);
		bindtextdomain(domain, locale_path);
		pg_bind_textdomain_codeset(domain);
	}
#endif
}

/* POLAR: set base dir in shared storage */
void
polar_set_database_path(const char *path)
{
	/* This should happen only once per process */
	Assert(!polar_database_path);
	polar_database_path = MemoryContextStrdup(TopMemoryContext, path);
}


/*
 * Remove all subdirectories and files in a directory
 */
void
polar_remove_file_in_dir(const char *dirname)
{
	DIR           *dir;
	struct dirent *de;
	char          rm_path[MAXPGPATH * 2];
	struct stat   statbuf;

	dir = AllocateDir(dirname, false);

	if (dir == NULL)
		ereport(ERROR,
		        (errcode_for_file_access(),
			        errmsg("could not open directory \"%s\": %m", dirname)));

	while ((de = ReadDir(dir, dirname)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(rm_path, sizeof(rm_path), "%s/%s", dirname, de->d_name);
		if (lstat(rm_path, &statbuf) < 0)
			ereport(LOG,
			        (errcode_for_file_access(),
				        errmsg("could not stat file \"%s\": %m", rm_path)));

		if (S_ISDIR(statbuf.st_mode))
		{
			/* recursively remove contents, then directory itself */
			polar_remove_file_in_dir(rm_path);

			if (rmdir(rm_path) < 0)
				ereport(LOG,
				        (errcode_for_file_access(),
					        errmsg("could not remove directory \"%s\": %m", rm_path)));
		}
		else
		{
			if (unlink(rm_path) < 0)
				ereport(LOG,
				        (errcode_for_file_access(),
					        errmsg("could not remove file \"%s\": %m", rm_path)));
		}
	}

	FreeDir(dir);
}

/*
 * POLAR: judge whether the gap between replica's latest redoPtr and master's latest redoPtr exceeds the size specified
 * return true when 1)the gap exceeds the size, 2)local pg_control file doesn't exists, 3) redoPtr of master < redoPtr of replica
 * return false when the gap is within the size
 */
bool
polar_replica_behind_exceed_size(uint64 size)
{
	char	polar_control_file_path[MAXPGPATH];
	ControlFileData *controlfile = NULL;
	XLogRecPtr	local_redo = InvalidXLogRecPtr;
	XLogRecPtr  shared_redo = InvalidXLogRecPtr;
	int			saved_errno;
	bool		result;

	if (!polar_enable_shared_storage_mode || !polar_in_replica_mode())
		return false;
	
	controlfile = palloc(sizeof(ControlFileData));

	/* POLAR: open and check local pg_control file */
	snprintf(polar_control_file_path, MAXPGPATH, "%s", XLOG_CONTROL_FILE);
	result = polar_read_control_file(polar_control_file_path, controlfile, LOG, &saved_errno);

	/* POLAR: return true when local pg_control file doesn't exist, need to copy it from shared storage */
	if (result == false)
	{
		if (saved_errno == ENOENT)
		{
			pfree(controlfile);
			return true;
		}
	}
	else
	{
		/* POLAR: get redoPtr of local pg_control file */
		local_redo = controlfile->checkPointCopy.redo;
	}

	/* POLAR: open and check pg_control file in shared storage */
	MemSet(controlfile, 0, sizeof(ControlFileData));
	polar_make_file_path_level2(polar_control_file_path, XLOG_CONTROL_FILE);
	if (polar_read_control_file(polar_control_file_path, controlfile, PANIC, &saved_errno))
	{
		/* POLAR: get redoPtr of shared pg_control file */
		shared_redo = controlfile->checkPointCopy.redo;
	}
	pfree(controlfile);
	elog(LOG, "shared_redo:%lX, local_redo:%lX", shared_redo, local_redo);

	/* POLAR: shared_redo maybe smaller than local_redo when in backup and recovery scene, need to copy it from shared storage */
	if (shared_redo < local_redo)
	{
		elog(LOG, "redoPtr of pg_control in shared storage is smaller than that in local");
		return true;
	}
	return (shared_redo - local_redo > size);
}

/*
 * POLAR: judge whether it is needed for replica to copy base dirs and other trans status log files 
 * from shared storage to local, only do the copy process when this func return true
 * return true when: 
 * 1) first boot after initdb or demote from a primary
 * 2) polar_replica_redo_gap_limit is 0, which means the judgement is disabled
 * 3) the gap between replica's latest redoPtr and master's latest redoPtr exceeds the size specified
 */
bool
polar_need_copy_local_dir_for_replica(void)
{
	char	   path[MAXPGPATH];
	struct stat stat;

	if (!polar_enable_shared_storage_mode || !polar_in_replica_mode())
		return false;
		
	/* 
	 * POLAR: return true when POLAR_REPLICA_BOOTED_FILE doesn't exist regardless of the redoptr gap
	 * which indicates it is the first time for replica node to boot after initdb or demote from a primary
	 * and create POLAR_REPLICA_BOOTED_FILE file after copy process is executed successfully
	 * so that next time we could judge whether to execute copy process depend on the redoptr gap
	 */
	snprintf((path), MAXPGPATH, "%s/%s", DataDir, POLAR_REPLICA_BOOTED_FILE);
	if (polar_stat(path, &stat) != 0)
	{
		/* POLAR: set replica_update_dirs_by_redo as FALSE, which means we need update dirs by copy */
		polar_set_replica_update_dirs_by_redo(FALSE);
		return true;
	}

	/* 
	 * POLAR: return true when 1)polar_replica_redo_gap_limit is 0, which means the comparison is disabled
	 * 2) the gap exceeds polar_replica_redo_gap_limit, polar_replica_redo_gap_limit unit is KB
	 */
	if (polar_replica_redo_gap_limit == 0 || polar_replica_behind_exceed_size(polar_replica_redo_gap_limit * 1024))
	{
		/* 
		 * POLAR: set replica_update_dirs_by_redo as FALSE
		 * so that startup process can copy local dirs for replica after postmaster process 
		 * copy pg_control file form shared storage to local, otherwise startup process will skip the copy process
		 * because the gap is 0 after pg_control file is copied
		 */
		polar_set_replica_update_dirs_by_redo(FALSE);
		return true;
	}
	polar_set_replica_update_dirs_by_redo(TRUE);
	return false;
}

/*
 * POLAR: copy base dirs and other trans status log files from shared storage to local for replica
 * called by startup process
 */ 
void
polar_copy_dirs_from_shared_storage_to_local(void)
{
	char	   *dirs_to_copy[] = {"pg_csnlog", "pg_twophase", "pg_xact", "pg_commit_ts", "pg_multixact", NULL};
	char	   *base = "base";
	char	  **cur_dir = dirs_to_copy;
	char        ss_path[MAXPGPATH * 2];
	char        to_path[MAXPGPATH * 2];
	DIR		   *dir;
	struct dirent *de;
	int         read_dir_err;

	/* For base directory, copy the new files from shared storage */
	if (mkdir(base, S_IRWXU) != 0 && EEXIST != errno)
		ereport(ERROR,
		        (errcode_for_file_access(),
			        errmsg("could not create directory \"%s\": %m", base)));

	/* Copy base subdirectories to local */
	polar_make_file_path_level2(ss_path, base);

	/*
	 * For polar store, the readdir will cache a dir entry, if the dir is deleted
	 * when readdir, it will fail. So we should retry.
	 */
read_dir_failed:

	read_dir_err = 0;
	dir = polar_allocate_dir(ss_path);
	if (dir == NULL)
		ereport(ERROR,
		        (errcode_for_file_access(),
			        errmsg("could not open directory \"%s\": %m", ss_path)));

	while ((de = polar_read_dir_ext(dir, ss_path, WARNING, &read_dir_err)) != NULL)
	{
		struct stat fst;

		/* If we got a cancel signal during the copy of the directory, quit */
		CHECK_FOR_INTERRUPTS();

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(to_path, sizeof(to_path), "%s/%s", base, de->d_name);
		polar_make_file_path_level2(ss_path, to_path);

		if (polar_stat(ss_path, &fst) < 0)
		{
			/* May be deleted after ReadDir */
			ereport(INFO,
			        (errcode_for_file_access(),
				        errmsg("could not stat file \"%s\": %m", ss_path)));

			/* Skip it, continue to deal with other directories */
			continue;
		}

		if (S_ISDIR(fst.st_mode))
		{
			/* If it is nonexistent, create one, if has error, skip it */
			if (mkdir(to_path, S_IRWXU) != 0 && EEXIST != errno)
				ereport(INFO,
				     (errcode_for_file_access(),
					     errmsg("could not create directory \"%s\": %m", to_path)));
		}
	}

	FreeDir(dir);

	if (read_dir_err == ENOENT)
	{
		ereport(LOG,
				(errcode_for_file_access(),
					errmsg("When readdir, some entries were deleted, retry.")));
		goto read_dir_failed;
	}

	/* Copy other trans status log files */
	while (*cur_dir != NULL)
	{
		bool skip_open_dir_err = false;

		/* 
		 * POLAR csn
		 * When hot upgrade to csn version, old rw instance has no csnlog directory,
		 * we set skip_dir_err to true when copy csnlog in replica mode
	     */
		if (strncmp(*cur_dir, "pg_csnlog", 9) == 0)
			skip_open_dir_err = true;

		/* Copy files from shared storage to local. */
		polar_make_file_path_level2(ss_path, *cur_dir);
		polar_copydir(ss_path, *cur_dir, true, true, true, skip_open_dir_err);
		cur_dir++;
	}

	/* 
	 * POLAR: create the polar_replica_booted file after copy process is done successfully
	 * so that we can copy relevant dirs at next boot time if the previous copy process failed
	 */
	polar_create_replica_booted_file();
	
}

/*
 * POLAR: Initialize the local directories
 * for primary and standby, remove local cache file
 * for replica, if replica doesn't update local dirs by redo, which occurs when 
 * 1) the gap between replica's latest redoPtr and vmaster's latest redoPtr exceeds polar_replica_redo_gap_limit
 * 2) replica node is newly created or demoted from a master node
 * then remove local cache file and copy them from shared storage to local, execute this copy process 
 * in startup process so that postmaster can respond to the detection of cm timely
 */
void
polar_init_local_dir(void)
{
	if (IsUnderPostmaster && polar_enable_shared_storage_mode)
	{
		if (!polar_replica_update_dirs_by_redo())
		{
			polar_remove_local_cache();
			if (polar_in_replica_mode())
			{
				elog(LOG,"copy dirs from shared storage to local for replica");
				polar_copy_dirs_from_shared_storage_to_local();
			}
		}
	}
}

/*
 * POLAR: copy pg_control file from shared storage to local for replica or standby
 * for standby, maintain local copy of control file to ensure promote
 * can be triggered successfully.
 */
void polar_copy_pg_control_from_shared_storage_to_local(void)
{
	char	   *global = "global";
	char	   *control = "pg_control";
	char        ss_path[MAXPGPATH * 2];
	char        to_path[MAXPGPATH * 2];

	/* For global directory, if exists, clean it and copy the pg_control file */
	if (mkdir(global, S_IRWXU) != 0)
	{
		if (EEXIST == errno)
			polar_remove_file_in_dir(global);
		else
			ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not create directory \"%s\": %m", global)));
	}

	/* Copy the pg_control file */
	snprintf(to_path, sizeof(to_path), "%s/%s", global, control);
	polar_make_file_path_level2(ss_path, to_path);

	polar_copy_file(ss_path, to_path, false);
}

/*
 * POLAR: Initialize the local global directories for replica or standby node
 * copy pg_control file from shared storage to local, called by postmaster
 * 
 * for replica, only copy when 1)the gap between replica's latest redoPtr and 
 * master's latest redoPtr exceeds polar_replica_redo_gap_limit
 * 2) replica node is newly created or demoted from a master node
 */
void
polar_init_global_dir_for_replica_or_standby(void)
{
	if (!IsUnderPostmaster
		&& polar_enable_shared_storage_mode
		&& ((polar_in_replica_mode() && polar_need_copy_local_dir_for_replica())
			 || polar_is_standby()))
	{
		elog(LOG,"copy pg_control file from shared storage to local");
		polar_copy_pg_control_from_shared_storage_to_local();
	}
}

/* 
 * POLAR: create the POLAR_REPLICA_BOOTED_FILE file so that we could judge whether to execute copy dirs process 
 * depend on the redoptr gap next time when replica starts
 */
void
polar_create_replica_booted_file()
{
	struct stat stat;
	char	   path[MAXPGPATH];
	int		   fd;

	snprintf((path), MAXPGPATH, "%s/%s", DataDir, POLAR_REPLICA_BOOTED_FILE);
	if (polar_stat(path, &stat) != 0)
	{
		fd = polar_open_transient_file(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
		if (fd < 0)
			ereport(WARNING,
					(errcode_for_file_access(),
						errmsg("could not create file \"%s\": %m, local dir initialization will be executed next time when replica start regardless of the redoptr gap ",
						path)));
		else
		{
			CloseTransientFile(fd);
			elog(LOG, "create file \"%s\"", POLAR_REPLICA_BOOTED_FILE);
		}
	}
}

/*
 * POLAR: detele POLAR_REPLICA_BOOTED_FILE if current node is not replica 
 * so that we can execute local dir initilization when master demote to a replica
 */
void
polar_remove_replica_booted_file()
{
	struct stat stat;
	char	   path[MAXPGPATH];

	snprintf((path), MAXPGPATH, "%s/%s", DataDir, POLAR_REPLICA_BOOTED_FILE);
	if (polar_stat(path, &stat) == 0)
	{
		if (polar_unlink(path) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
						errmsg("could not remove file \"%s\": %m", path)));
		else
			elog(LOG, "remove file \"%s\"", POLAR_REPLICA_BOOTED_FILE);	
	}
}

/*
 * POLAR: Create file to store polar persisted shared memory key and ID,
 * like CreateLockFile.
 */
void
polar_create_shmem_info_file(bool am_postmaster)
{
	int		fd;
	int		ntries;
	unsigned long key = 0, id = 0;
	PolarShmemFileStatus status;

	if (!am_postmaster)
		return;

	if (!polar_enable_persisted_buffer_pool)
	{
		polar_remove_old_shmem_info_file();
		return;
	}

	for (ntries = 0;; ntries++)
	{
		fd = open(POLAR_SHMEM_FILE, O_RDWR | O_CREAT | O_EXCL, pg_file_create_mode);
		if (fd >= 0)
			break;

		/* Couldn't create the shmem file. Probably it already exists. */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			ereport(FATAL,
					(errcode_for_file_access(), errmsg(
						"could not create polar persisted shmem file \"%s\": %m",
						POLAR_SHMEM_FILE)));

		status = polar_shmem_file_status(&key, &id);
		if (status == SHMEM_FILE_NOT_EXIST)
			continue;

		if (status == SHMEM_FILE_SHM_IN_USE)
			elog(FATAL,
				 "pre-existing polar shared memory block (key %lu, ID %lu) is still in use.",
				 key, id);

		if (polar_shmem_file_exists(status))
		{
			elog(LOG,
				 "polar shmem file %s (key %lu, ID %lu) already exists, try to reuse it.",
				 POLAR_SHMEM_FILE, key, id);
			break;
		}
	}

	/*
	 * We do not unlink the lock file(s) at proc_exit. So do not add this file
	 * to lock_files.
	 */
}

/*
 * POLAR: Store polar shmem key and ID in a separate file. We will check it
 * when reuse the shared memory. We always cover the old content.
 */
void
polar_write_shmem_info_to_file(unsigned long key, unsigned long id)
{
	int		fd;
	int		len;
	char	str[64];

	sprintf(str, "%9lu %9lu\n", key, id);
	len = strlen(str);

	fd = open(POLAR_SHMEM_FILE, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
	{
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"could not open polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));
	}

	errno = 0;
	if (lseek(fd, (off_t)0, SEEK_SET) != 0 ||
		(int) write(fd, str, len) != len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		close(fd);
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"could not write to polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));
	}

	if (pg_fsync(fd) != 0)
	{
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"could not writer to polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));
	}

	if (close(fd) != 0)
	{
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"could not write to polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));
	}

	elog(LOG, "Write polar shmem info (key %lu, ID %lu) to file.", key, id);
}

/*
 * POLAR: Read shmem key and ID from the file. If skip_enoent is true, we return
 * false when the file does not exist.
 */
PolarShmemFileStatus
polar_read_shmem_info_from_file(unsigned long *key,
								unsigned long *id,
								bool skip_enoent)
{
	int		fd;
	int		len;
	char	buffer[MAXPGPATH * 2 + 256];

	fd = open(POLAR_SHMEM_FILE, O_RDONLY, pg_file_create_mode);
	if (fd < 0)
	{
		if (errno == ENOENT && skip_enoent)
			return SHMEM_FILE_NOT_EXIST;
		else
		{
			ereport(FATAL,
					(errcode_for_file_access(), errmsg(
						"could not open polar shmem file \"%s\": %m",
						POLAR_SHMEM_FILE)));
		}
	}

	if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"count not read polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));

	if (close(fd) != 0)
	{
		ereport(FATAL,
				(errcode_for_file_access(), errmsg(
					"could not read from polar shmem file \"%s\": %m",
					POLAR_SHMEM_FILE)));
	}

	if (sscanf(buffer, "%lu %lu", key, id) == 2)
		return SHMEM_FILE_VALID;
	else
		return SHMEM_FILE_INVALID;
}

static PolarShmemFileStatus
polar_shmem_file_status(unsigned long *key, unsigned long *id)
{
	PolarShmemFileStatus status = polar_read_shmem_info_from_file(key, id, true);
	if (status != SHMEM_FILE_VALID)
		return status;

	/*
	 * POLAR: Already read valid shared memory key and ID from file, now
	 * check if it can be reused.
	 */
	if (PGSharedMemoryIsInUse(*key, *id))
		return SHMEM_FILE_SHM_IN_USE;
	else
		return SHMEM_FILE_SHM_NOT_IN_USE;
}

/*
 * check if polar shmem info file exists, if yes, try to delete the
 * shared memory.
 */
static void
polar_remove_old_shmem_info_file(void)
{
	PolarShmemFileStatus status;
	unsigned long key = 0, id = 0;

	status = polar_shmem_file_status(&key, &id);
	if (status == SHMEM_FILE_NOT_EXIST)
		return;

	if (status == SHMEM_FILE_SHM_IN_USE)
		elog(FATAL,
			 "pre-existing polar shared memory block (key %lu, ID %lu) is still in use.",
			 key, id);

	if (polar_shmem_file_exists(status))
	{
		/* Delete shared memory and delete file. */
		if (polar_delete_shmem(id))
		{
			if (unlink(POLAR_SHMEM_FILE) < 0)
				ereport(FATAL,
						(errcode_for_file_access(),
							errmsg("could not remove old shmem file \"%s\": %m",
								   POLAR_SHMEM_FILE),
							errhint(
								"The file seems accidentally left over, but "
								"it could not be removed. Please remove the file "
								"by hand and try again.")));

			elog(LOG,
				 "Delete polar persisted shared memory (key %lu, ID %lu) and remove old polar shmem info file.",
				 key, id);
		}
		else
		{
			elog(FATAL,
				 "polar shared memory can not be deleted. Please delete it by hand and try again. key %lu ID %lu",
				 key, id);
		}
	}
}

/*
 * POLAR px
 * get AuthenticatedUserIsSuperuser
 */
bool
px_authenticated_user_is_superuser(void)
{
	AssertState(OidIsValid(AuthenticatedUserId));
	return AuthenticatedUserIsSuperuser;
}

/* POLAR: remove local cache */
static void
polar_remove_local_cache(void)
{
	polar_remove_clog_local_cache_file();
	polar_remove_commit_ts_local_cache_file();
	polar_remove_multixcat_local_cache_file();
	polar_remove_csnlog_local_cache_file();
}