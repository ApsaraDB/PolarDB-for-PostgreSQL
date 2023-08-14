/*-------------------------------------------------------------------------
 *
 * postinit.c
 *	  postgres initialization utilities
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/postinit.c
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/session.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_tablespace.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/tqual.h"

/* POLAR */
#include "postmaster/syslogger.h"
#include "storage/polar_fd.h"
#include "polar_dma/polar_dma.h"
#include <math.h>
#include "postmaster/polar_dispatcher.h"

/* POLAR px */
#include "px/px_timeout.h"
#include "px/px_util.h"
#include "px/px_vars.h"
/* POLAR end */

static HeapTuple GetDatabaseTuple(const char *dbname);
static HeapTuple GetDatabaseTupleByOid(Oid dboid);
static void PerformAuthentication(Port *port);
static void CheckMyDatabase(const char *name, bool am_superuser, bool override_allow_connections);
static void InitCommunication(void);
static void ShutdownPostgres(int code, Datum arg);
static void StatementTimeoutHandler(void);
static void LockTimeoutHandler(void);
static void IdleInTransactionSessionTimeoutHandler(void);
static bool ThereIsAtLeastOneRole(void);
static void process_startup_options(Port *port, bool am_superuser);
static void process_settings(Oid databaseid, Oid roleid);

#ifdef USE_PX
extern void InitPXOPT();
extern void TerminatePXOPT();
#endif

/*** InitPostgres support ***/


/*
 * GetDatabaseTuple -- fetch the pg_database row for a database
 *
 * This is used during backend startup when we don't yet have any access to
 * system catalogs in general.  In the worst case, we can seqscan pg_database
 * using nothing but the hard-wired descriptor that relcache.c creates for
 * pg_database.  In more typical cases, relcache.c was able to load
 * descriptors for both pg_database and its indexes from the shared relcache
 * cache file, and so we can do an indexscan.  criticalSharedRelcachesBuilt
 * tells whether we got the cached descriptors.
 */
static HeapTuple
GetDatabaseTuple(const char *dbname)
{
	HeapTuple	tuple;
	Relation	relation;
	SysScanDesc scan;
	ScanKeyData key[1];

	/*
	 * form a scan key
	 */
	ScanKeyInit(&key[0],
				Anum_pg_database_datname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(dbname));

	/*
	 * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
	 * built the critical shared relcache entries (i.e., we're starting up
	 * without a shared relcache cache file).
	 */
	relation = heap_open(DatabaseRelationId, AccessShareLock);
	scan = systable_beginscan(relation, DatabaseNameIndexId,
							  criticalSharedRelcachesBuilt,
							  NULL,
							  1, key);

	tuple = systable_getnext(scan);

	/* Must copy tuple before releasing buffer */
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);

	/* all done */
	systable_endscan(scan);
	heap_close(relation, AccessShareLock);

	return tuple;
}

/*
 * GetDatabaseTupleByOid -- as above, but search by database OID
 */
static HeapTuple
GetDatabaseTupleByOid(Oid dboid)
{
	HeapTuple	tuple;
	Relation	relation;
	SysScanDesc scan;
	ScanKeyData key[1];

	/*
	 * form a scan key
	 */
	ScanKeyInit(&key[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dboid));

	/*
	 * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
	 * built the critical shared relcache entries (i.e., we're starting up
	 * without a shared relcache cache file).
	 */
	relation = heap_open(DatabaseRelationId, AccessShareLock);
	scan = systable_beginscan(relation, DatabaseOidIndexId,
							  criticalSharedRelcachesBuilt,
							  NULL,
							  1, key);

	tuple = systable_getnext(scan);

	/* Must copy tuple before releasing buffer */
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);

	/* all done */
	systable_endscan(scan);
	heap_close(relation, AccessShareLock);

	return tuple;
}


/*
 * PerformAuthentication -- authenticate a remote client
 *
 * returns: nothing.  Will not return at all if there's any failure.
 */
static void
PerformAuthentication(Port *port)
{
	/* This should be set already, but let's make sure */
	ClientAuthInProgress = true;	/* limit visibility of log messages */

	/*
	 * In EXEC_BACKEND case, we didn't inherit the contents of pg_hba.conf
	 * etcetera from the postmaster, and have to load them ourselves.
	 *
	 * FIXME: [fork/exec] Ugh.  Is there a way around this overhead?
	 */
#ifdef EXEC_BACKEND

	/*
	 * load_hba() and load_ident() want to work within the PostmasterContext,
	 * so create that if it doesn't exist (which it won't).  We'll delete it
	 * again later, in PostgresMain.
	 */
	if (PostmasterContext == NULL)
		PostmasterContext = AllocSetContextCreate(TopMemoryContext,
												  "Postmaster",
												  ALLOCSET_DEFAULT_SIZES);

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
		 * It is ok to continue if we fail to load the IDENT file, although it
		 * means that you cannot log in using any of the authentication
		 * methods that need a user name mapping. load_ident() already logged
		 * the details of error to the log.
		 */
	}
#endif

	/*
	 * Set up a timeout in case a buggy or malicious client fails to respond
	 * during authentication.  Since we're inside a transaction and might do
	 * database access, we have to use the statement_timeout infrastructure.
	 */
	enable_timeout_after(STATEMENT_TIMEOUT, AuthenticationTimeout * 1000);

	/*
	 * Now perform authentication exchange.
	 */
	ClientAuthentication(port); /* might not return, if failure */

	/*
	 * Done with authentication.  Disable the timeout, and log if needed.
	 */
	disable_timeout(STATEMENT_TIMEOUT, false);

	if (Log_connections)
	{
		if (am_walsender)
		{
#ifdef USE_SSL
			if (port->ssl_in_use)
				ereport(LOG,
						(errmsg("replication connection authorized: user=%s SSL enabled (protocol=%s, cipher=%s, bits=%d, compression=%s)",
								port->user_name,
								be_tls_get_version(port),
								be_tls_get_cipher(port),
								be_tls_get_cipher_bits(port),
								be_tls_get_compression(port) ? _("on") : _("off"))));
			else
#endif
				ereport(LOG,
						(errmsg("replication connection authorized: user=%s",
								port->user_name)));
		}
		else
		{
#ifdef USE_SSL
			if (port->ssl_in_use)
				ereport(LOG,
						(errmsg("connection authorized: user=%s database=%s SSL enabled (protocol=%s, cipher=%s, bits=%d, compression=%s)",
								port->user_name, port->database_name,
								be_tls_get_version(port),
								be_tls_get_cipher(port),
								be_tls_get_cipher_bits(port),
								be_tls_get_compression(port) ? _("on") : _("off"))));
			else
#endif
				ereport(LOG,
						(errmsg("connection authorized: user=%s database=%s",
								port->user_name, port->database_name)));

			if (polar_enable_multi_syslogger)
			{
				ErrorData edata;
				POLAR_ERROR_DATA_INIT_FOR_AUDITLOG(edata);
				polar_write_audit_log(&edata,
						"connection authorized: user=%s database=%s",
								port->user_name, port->database_name);
			}
		}
	}

	set_ps_display("startup", false);

	ClientAuthInProgress = false;	/* client_min_messages is active now */
}


/*
 * CheckMyDatabase -- fetch information from the pg_database entry for our DB
 */
static void
CheckMyDatabase(const char *name, bool am_superuser, bool override_allow_connections)
{
	HeapTuple	tup;
	Form_pg_database dbform;
	char	   *collate;
	char	   *ctype;

	ELOG_PSS(DEBUG1, "CheckMyDatabase %s", name);

	/* Fetch our pg_database row normally, via syscache */
	tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
	dbform = (Form_pg_database) GETSTRUCT(tup);

	/* This recheck is strictly paranoia */
	if (strcmp(name, NameStr(dbform->datname)) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" has disappeared from pg_database",
						name),
				 errdetail("Database OID %u now seems to belong to \"%s\".",
						   MyDatabaseId, NameStr(dbform->datname))));

	/*
	 * Check permissions to connect to the database.
	 *
	 * These checks are not enforced when in standalone mode, so that there is
	 * a way to recover from disabling all access to all databases, for
	 * example "UPDATE pg_database SET datallowconn = false;".
	 *
	 * We do not enforce them for autovacuum worker processes either.
	 */
	if (IsUnderPostmaster && !IsAutoVacuumWorkerProcess())
	{
		/*
		 * Check that the database is currently allowing connections.
		 */
		if (!dbform->datallowconn && !override_allow_connections)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("database \"%s\" is not currently accepting connections",
							name)));

		/*
		 * Check privilege to connect to the database.  (The am_superuser test
		 * is redundant, but since we have the flag, might as well check it
		 * and save a few cycles.)
		 */
		if (!am_superuser &&
			pg_database_aclcheck(MyDatabaseId, GetUserId(),
								 ACL_CONNECT) != ACLCHECK_OK)
			ereport(FATAL,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for database \"%s\"", name),
					 errdetail("User does not have CONNECT privilege.")));

		/*
		 * Check connection limit for this database.
		 *
		 * There is a race condition here --- we create our PGPROC before
		 * checking for other PGPROCs.  If two backends did this at about the
		 * same time, they might both think they were over the limit, while
		 * ideally one should succeed and one fail.  Getting that to work
		 * exactly seems more trouble than it is worth, however; instead we
		 * just document that the connection limit is approximate.
		 */
		if (dbform->datconnlimit >= 0 &&
			!am_superuser &&
			CountDBConnections(MyDatabaseId) > dbform->datconnlimit)
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("too many connections for database \"%s\"",
							name)));
	}

	/*
	 * OK, we're golden.  Next to-do item is to save the encoding info out of
	 * the pg_database tuple.
	 */
	SetDatabaseEncoding(dbform->encoding);
	/* Record it as a GUC internal option, too */
	SetConfigOption("server_encoding", GetDatabaseEncodingName(),
					PGC_INTERNAL, PGC_S_OVERRIDE);
	/* If we have no other source of client_encoding, use server encoding */
	SetConfigOption("client_encoding", GetDatabaseEncodingName(),
					PGC_BACKEND, PGC_S_DYNAMIC_DEFAULT);

	/* assign locale variables */
	collate = NameStr(dbform->datcollate);
	ctype = NameStr(dbform->datctype);

	if (pg_perm_setlocale(LC_COLLATE, collate) == NULL)
		ereport(FATAL,
				(errmsg("database locale is incompatible with operating system"),
				 errdetail("The database was initialized with LC_COLLATE \"%s\", "
						   " which is not recognized by setlocale().", collate),
				 errhint("Recreate the database with another locale or install the missing locale.")));

	if (pg_perm_setlocale(LC_CTYPE, ctype) == NULL)
		ereport(FATAL,
				(errmsg("database locale is incompatible with operating system"),
				 errdetail("The database was initialized with LC_CTYPE \"%s\", "
						   " which is not recognized by setlocale().", ctype),
				 errhint("Recreate the database with another locale or install the missing locale.")));

	/* Make the locale settings visible as GUC variables, too */
	SetConfigOption("lc_collate", collate, PGC_INTERNAL, PGC_S_OVERRIDE);
	SetConfigOption("lc_ctype", ctype, PGC_INTERNAL, PGC_S_OVERRIDE);

	check_strxfrm_bug();

	ReleaseSysCache(tup);
}



/* --------------------------------
 *		InitCommunication
 *
 *		This routine initializes stuff needed for ipc, locking, etc.
 *		it should be called something more informative.
 * --------------------------------
 */
static void
InitCommunication(void)
{
	/*
	 * initialize shared memory and semaphores appropriately.
	 */
	if (!IsUnderPostmaster)		/* postmaster already did this */
	{
		/*
		 * We're running a postgres bootstrap process or a standalone backend.
		 * Though we won't listen on PostPortNumber, use it to select a shmem
		 * key.  This increases the chance of detecting a leftover live
		 * backend of this DataDir.
		 */
		CreateSharedMemoryAndSemaphores(PostPortNumber);
	}
}


/*
 * pg_split_opts -- split a string of options and append it to an argv array
 *
 * The caller is responsible for ensuring the argv array is large enough.  The
 * maximum possible number of arguments added by this routine is
 * (strlen(optstr) + 1) / 2.
 *
 * Because some option values can contain spaces we allow escaping using
 * backslashes, with \\ representing a literal backslash.
 */
void
pg_split_opts(char **argv, int *argcp, const char *optstr)
{
	StringInfoData s;

	initStringInfo(&s);

	while (*optstr)
	{
		bool		last_was_escape = false;

		resetStringInfo(&s);

		/* skip over leading space */
		while (isspace((unsigned char) *optstr))
			optstr++;

		if (*optstr == '\0')
			break;

		/*
		 * Parse a single option, stopping at the first space, unless it's
		 * escaped.
		 */
		while (*optstr)
		{
			if (isspace((unsigned char) *optstr) && !last_was_escape)
				break;

			if (!last_was_escape && *optstr == '\\')
				last_was_escape = true;
			else
			{
				last_was_escape = false;
				appendStringInfoChar(&s, *optstr);
			}

			optstr++;
		}

		/* now store the option in the next argv[] position */
		argv[(*argcp)++] = pstrdup(s.data);
	}

	pfree(s.data);
}

/*
 * Initialize MaxBackends value from config options.
 *
 * This must be called after modules have had the chance to register background
 * workers in shared_preload_libraries, and before shared memory size is
 * determined.
 *
 * Note that in EXEC_BACKEND environment, the value is passed down from
 * postmaster to subprocesses via BackendParameters in SubPostmasterMain; only
 * postmaster itself and processes not under postmaster control should call
 * this.
 */
void
InitializeMaxBackends(void)
{
	Assert(MaxBackends == 0);

	MaxPolarDispatcher = 0;
	MaxPolarSessions = 0;
	MaxPolarSharedBackends = 0;
	MaxPolarSessionsPerDispatcher = 0;
	MaxPolarSharedBackendsPerDispatcher = 0;

	if (POLAR_SHARED_SERVER_RUNNING())
	{
		MaxPolarDispatcher = polar_ss_dispatcher_count;
		MaxPolarSessions = MaxConnections;
		if (polar_ss_backend_max_count > 0)
			MaxPolarSharedBackends = polar_ss_backend_max_count;
		else if (polar_ss_backend_max_count == 0)
			MaxPolarSharedBackends = MaxConnections;
		else
			MaxPolarSharedBackends = MaxConnections / (0 - polar_ss_backend_max_count);
		MaxPolarSessionsPerDispatcher = (int)floor(MaxPolarSessions / polar_ss_dispatcher_count);
		MaxPolarSharedBackendsPerDispatcher = (int)floor(MaxPolarSharedBackends / polar_ss_dispatcher_count);
	}

	/* the extra unit accounts for the autovacuum launcher */
	MaxBackends = MaxConnections + autovacuum_max_workers + 1 +
		MaxPolarDispatcher +/* POLAR: Shared Server */
		max_worker_processes;

	/* POLAR px */
	MaxNormalBackends
		= (int)floor(polar_max_normal_backends_factor * MaxConnections);

	/* internal error because the values were all checked previously */
	if (MaxBackends > MAX_BACKENDS)
		elog(ERROR, "too many backends configured");
}

/*
 * Early initialization of a backend (either standalone or under postmaster).
 * This happens even before InitPostgres.
 *
 * This is separate from InitPostgres because it is also called by auxiliary
 * processes, such as the background writer process, which may not call
 * InitPostgres at all.
 */
void
BaseInit(void)
{
	/*
	 * Attach to shared memory and semaphores, and initialize our
	 * input/output/debugging file descriptors.
	 */
	InitCommunication();
	DebugFileOpen();

	/* Do local initialization of file, storage and buffer managers */
	InitFileAccess();
	smgrinit();
	InitBufferPoolAccess();
}


/* --------------------------------
 * InitPostgres
 *		Initialize POSTGRES.
 *
 * The database can be specified by name, using the in_dbname parameter, or by
 * OID, using the dboid parameter.  In the latter case, the actual database
 * name can be returned to the caller in out_dbname.  If out_dbname isn't
 * NULL, it must point to a buffer of size NAMEDATALEN.
 *
 * Similarly, the username can be passed by name, using the username parameter,
 * or by OID using the useroid parameter.
 *
 * In bootstrap mode no parameters are used.  The autovacuum launcher process
 * doesn't use any parameters either, because it only goes far enough to be
 * able to read pg_database; it doesn't connect to any particular database.
 * In walsender mode only username is used.
 *
 * As of PostgreSQL 8.2, we expect InitProcess() was already called, so we
 * already have a PGPROC struct ... but it's not completely filled in yet.
 *
 * Note:
 *		Be very careful with the order of calls in the InitPostgres function.
 * --------------------------------
 */
void
InitPostgres(const char *in_dbname, Oid dboid, const char *username,
			 Oid useroid, char *out_dbname, bool override_allow_connections)
{
	bool		bootstrap = IsBootstrapProcessingMode();
	bool		am_superuser;
	char	   *fullpath;
	char		dbname[NAMEDATALEN];

	/* POLAR */
	int		nosupercount = 0, supercount = 0;

	elog(DEBUG3, "InitPostgres is_shared:%d, session_id:%d", 
		 polar_current_session->is_shared, polar_current_session->session_id);

	/*
	 * Add my PGPROC struct to the ProcArray.
	 *
	 * Once I have done this, I am visible to other backends!
	 */
	InitProcessPhase2();


#ifdef USE_PX
	/* Initialize PXOPT */
	px_OptimizerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												   "PXOPT Top-level Memory Context",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);

	InitPXOPT();
#endif

	/*
	 * Initialize my entry in the shared-invalidation manager's array of
	 * per-backend data.
	 *
	 * Sets up MyBackendId, a unique backend identifier.
	 */
	MyBackendId = InvalidBackendId;

	SharedInvalBackendInit(false);

	if (MyBackendId > MaxBackends || MyBackendId <= 0)
		elog(FATAL, "bad backend ID: %d", MyBackendId);

	/* Now that we have a BackendId, we can participate in ProcSignal */
	ProcSignalInit(MyBackendId);

	/*
	 * Also set up timeout handlers needed for backend operation.  We need
	 * these in every case except bootstrap.
	 */
	if (!bootstrap)
	{
		RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);
		RegisterTimeout(STATEMENT_TIMEOUT, StatementTimeoutHandler);
		RegisterTimeout(LOCK_TIMEOUT, LockTimeoutHandler);
		RegisterTimeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
						IdleInTransactionSessionTimeoutHandler);

		/* POLAR px: register wait lock timer and get a valid timeout id. */
		px_wait_lock_timer_id = RegisterTimeout(USER_TIMEOUT,
												px_wait_lock_timeout_handler);
		Assert(IS_VALID_TIMER_ID(px_wait_lock_timer_id));
		/* POLAR end */
	}

	/*
	 * bufmgr needs another initialization call too
	 */
	InitBufferPoolBackend();

	/*
	 * Initialize local process's access to XLOG.
	 */
	if (IsUnderPostmaster)
	{
		/*
		 * The postmaster already started the XLOG machinery, but we need to
		 * call InitXLOGAccess(), if the system isn't in hot-standby mode.
		 * This is handled by calling RecoveryInProgress and ignoring the
		 * result.
		 */
		(void) RecoveryInProgress();
	}
	else
	{

		/*
		 * We are either a bootstrap process or a standalone backend. Either
		 * way, start up the XLOG machinery, and register to have it closed
		 * down at exit.
		 */
#ifdef USE_DMA
		/* POLAR: disable consensus */
		polar_enable_dma = false;
#endif
		StartupXLOG();
		on_shmem_exit(ShutdownXLOG, 0);
	}

	/* POLAR: Shared Server */
	polar_ss_shmem_aset_init_backend();

	/*
	 * Initialize the relation cache and the system catalog caches.  Note that
	 * no catalog access happens here; we only set up the hashtable structure.
	 * We must do this before starting a transaction because transaction abort
	 * would try to touch these hashtables.
	 */
	RelationCacheInitialize();
	InitCatalogCache();
	InitPlanCache();

	/* Initialize portal manager */
	EnablePortalManager();

	/* Initialize stats collection --- must happen before first xact */
	if (!bootstrap)
		pgstat_initialize();

	/*
	 * Load relcache entries for the shared system catalogs.  This must create
	 * at least entries for pg_database and catalogs used for authentication.
	 */
	RelationCacheInitializePhase2();

	/*
	 * Set up process-exit callback to do pre-shutdown cleanup.  This is the
	 * first before_shmem_exit callback we register; thus, this will be the
	 * last thing we do before low-level modules like the buffer manager begin
	 * to close down.  We need to have this in place before we begin our first
	 * transaction --- if we fail during the initialization transaction, as is
	 * entirely possible, we need the AbortTransaction call to clean up.
	 */
	before_shmem_exit(ShutdownPostgres, 0);

	/* The autovacuum launcher is done here */
	if (IsAutoVacuumLauncherProcess())
	{
		/* report this backend in the PgBackendStatus array */
		pgstat_bestart();

		return;
	}

	/*
	 * Start a new transaction here before first access to db, and get a
	 * snapshot.  We don't have a use for the snapshot itself, but we're
	 * interested in the secondary effect that it sets RecentGlobalXmin. (This
	 * is critical for anything that reads heap pages, because HOT may decide
	 * to prune them even if the process doesn't attempt to modify any
	 * tuples.)
	 */
	if (!bootstrap)
	{
		/* statement_timestamp must be set for timeouts to work correctly */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();

		/*
		 * transaction_isolation will have been set to the default by the
		 * above.  If the default is "serializable", and we are in hot
		 * standby, we will fail if we don't change it to something lower.
		 * Fortunately, "read committed" is plenty good enough.
		 */
		XactIsoLevel = XACT_READ_COMMITTED;

		(void) GetTransactionSnapshot();
	}

	/*
	 * Perform client authentication if necessary, then figure out our
	 * postgres user ID, and see if we are a superuser.
	 *
	 * In standalone mode and in autovacuum worker processes, we use a fixed
	 * ID, otherwise we figure it out from the authenticated user name.
	 */
	if (bootstrap || IsAutoVacuumWorkerProcess())
	{
		InitializeSessionUserIdStandalone();
		am_superuser = true;
	}
	else if (!IsUnderPostmaster)
	{
		InitializeSessionUserIdStandalone();
		am_superuser = true;
		if (!ThereIsAtLeastOneRole())
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("no roles are defined in this database system"),
					 errhint("You should immediately run CREATE USER \"%s\" SUPERUSER;.",
							 username != NULL ? username : "postgres")));
	}
	else if (IsBackgroundWorker)
	{
		if (username == NULL && !OidIsValid(useroid))
		{
			InitializeSessionUserIdStandalone();
			am_superuser = true;
		}
		else
		{
			InitializeSessionUserId(username, useroid);
			am_superuser = superuser();
		}
	}
	else
	{
		/* normal multiuser case */
		Assert(MyProcPort != NULL);
		PerformAuthentication(MyProcPort);
		InitializeSessionUserId(username, useroid);
		am_superuser = superuser();
	}

	/*
	 * If we're trying to shut down, only superusers can connect, and new
	 * replication connections are not allowed.
	 */
	if ((!POLAR_ENABLE_DMA() && (!am_superuser || am_walsender)) &&
		MyProcPort != NULL &&
		MyProcPort->canAcceptConnections == CAC_WAITBACKUP)
	{
		if (am_walsender)
			ereport(FATAL,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("new replication connections are not allowed during database shutdown")));
		else
			ereport(FATAL,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to connect during database shutdown")));
	}

	/*
	 * Binary upgrades only allowed super-user connections
	 */
	if (IsBinaryUpgrade && !am_superuser)
	{
		ereport(FATAL,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to connect in binary upgrade mode")));
	}

	/*
	 * The last few connections slots are reserved for superusers and replication
	 * connections. Whenever connections is replication connections or superuser,
	 * we all allow connection. But in official design, they refuse replication
	 * connections where it is up to superuser_reserved_connections.
	 */
	if (!am_superuser &&
		ReservedBackends > 0 &&
		!HaveNFreeProcs(ReservedBackends))
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("remaining connection slots are reserved for non-replication superuser connections")));

	/* POLAR: we can get all user num and superuser num in one scan procarray*/
	polar_get_nosuper_and_super_conn_count(&nosupercount, &supercount);
	/*
	 * POLAR: for all user(not including walsender and superuser), check if all connections exceeded
	 * polar_max_non_super_conns. Note We do not restrict walsender any more,we must keep slave
	 * alive
	 */
	if (!am_superuser && polar_max_non_super_conns >= 0 &&
		polar_max_non_super_conns < nosupercount)
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("Sorry, too many clients already")));
	/*
	 * POLAR: for super(not including walsenders), check if all connections exceeded
	 * polar_max_super_conns. Note We do not restrict walsender any more,we must
	 * keep slave alive
	 */
	if (am_superuser && !am_walsender && polar_max_super_conns >= 0 &&
		polar_max_super_conns < supercount)
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("Sorry, number of superuser connections has exceeded polar_max_super_conns:%d", polar_max_super_conns)));

	/*
	 * POLAR: reserve connections for polar_superuser, when the connection counts
	 * exceeded polar_reserve_polar_super_conns, new connections are fobidden. Note that
	 * the priority of superuser is higher than polar_super_user, the limit is only
	 * valid for normal users.
	 */
	if (!polar_superuser() && !am_superuser && !am_walsender &&
		polar_max_non_super_conns >= 0 && polar_reserved_polar_super_conns >= 0 &&
		nosupercount + polar_reserved_polar_super_conns > polar_max_non_super_conns)
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("Sorry, number of normal user connections (%d) plus polar super user connections (%d) have exceeded limits",
				 nosupercount, polar_reserved_polar_super_conns)));
	/* POLAR end */

	/* Check replication permissions needed for walsender processes. */
	if (am_walsender)
	{
		Assert(!bootstrap);

		if (!superuser() && !has_rolreplication(GetUserId()))
			ereport(FATAL,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser or replication role to start walsender")));
	}

	/*
	 * If this is a plain walsender only supporting physical replication, we
	 * don't want to connect to any particular database. Just finish the
	 * backend startup by processing any options from the startup packet, and
	 * we're done.
	 */
	if (am_walsender && !am_db_walsender)
	{
		/* process any options passed in the startup packet */
		if (MyProcPort != NULL)
			process_startup_options(MyProcPort, am_superuser);

		/* Apply PostAuthDelay as soon as we've read all options */
		if (PostAuthDelay > 0)
			pg_usleep(PostAuthDelay * 1000000L);

		/* initialize client encoding */
		InitializeClientEncoding();

		/* report this backend in the PgBackendStatus array */
		pgstat_bestart();

		/* close the transaction we started above */
		CommitTransactionCommand();

		return;
	}

	/*
	 * Set up the global variables holding database id and default tablespace.
	 * But note we won't actually try to touch the database just yet.
	 *
	 * We take a shortcut in the bootstrap case, otherwise we have to look up
	 * the db's entry in pg_database.
	 */
	if (bootstrap)
	{
		MyDatabaseId = TemplateDbOid;
		MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
	}
	else if (in_dbname != NULL)
	{
		HeapTuple	tuple;
		Form_pg_database dbform;

		tuple = GetDatabaseTuple(in_dbname);
		if (!HeapTupleIsValid(tuple))
			ereport(FATAL,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database \"%s\" does not exist", in_dbname)));
		dbform = (Form_pg_database) GETSTRUCT(tuple);
		MyDatabaseId = HeapTupleGetOid(tuple);
		MyDatabaseTableSpace = dbform->dattablespace;
		/* take database name from the caller, just for paranoia */
		strlcpy(dbname, in_dbname, sizeof(dbname));
	}
	else if (OidIsValid(dboid))
	{
		/* caller specified database by OID */
		HeapTuple	tuple;
		Form_pg_database dbform;

		tuple = GetDatabaseTupleByOid(dboid);
		if (!HeapTupleIsValid(tuple))
			ereport(FATAL,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database %u does not exist", dboid)));
		dbform = (Form_pg_database) GETSTRUCT(tuple);
		MyDatabaseId = HeapTupleGetOid(tuple);
		MyDatabaseTableSpace = dbform->dattablespace;
		Assert(MyDatabaseId == dboid);
		strlcpy(dbname, NameStr(dbform->datname), sizeof(dbname));
		/* pass the database name back to the caller */
		if (out_dbname)
			strcpy(out_dbname, dbname);
	}
	else
	{
		/*
		 * If this is a background worker not bound to any particular
		 * database, we're done now.  Everything that follows only makes sense
		 * if we are bound to a specific database.  We do need to close the
		 * transaction we started before returning.
		 */
		if (!bootstrap)
		{
			pgstat_bestart();
			CommitTransactionCommand();
		}
		return;
	}

	/*
	 * Now, take a writer's lock on the database we are trying to connect to.
	 * If there is a concurrently running DROP DATABASE on that database, this
	 * will block us until it finishes (and has committed its update of
	 * pg_database).
	 *
	 * Note that the lock is not held long, only until the end of this startup
	 * transaction.  This is OK since we will advertise our use of the
	 * database in the ProcArray before dropping the lock (in fact, that's the
	 * next thing to do).  Anyone trying a DROP DATABASE after this point will
	 * see us in the array once they have the lock.  Ordering is important for
	 * this because we don't want to advertise ourselves as being in this
	 * database until we have the lock; otherwise we create what amounts to a
	 * deadlock with CountOtherDBBackends().
	 *
	 * Note: use of RowExclusiveLock here is reasonable because we envision
	 * our session as being a concurrent writer of the database.  If we had a
	 * way of declaring a session as being guaranteed-read-only, we could use
	 * AccessShareLock for such sessions and thereby not conflict against
	 * CREATE DATABASE.
	 */
	if (!bootstrap)
		LockSharedObject(DatabaseRelationId, MyDatabaseId, 0,
						 RowExclusiveLock);

	/*
	 * Now we can mark our PGPROC entry with the database ID.
	 *
	 * We assume this is an atomic store so no lock is needed; though actually
	 * things would work fine even if it weren't atomic.  Anyone searching the
	 * ProcArray for this database's ID should hold the database lock, so they
	 * would not be executing concurrently with this store.  A process looking
	 * for another database's ID could in theory see a chance match if it read
	 * a partially-updated databaseId value; but as long as all such searches
	 * wait and retry, as in CountOtherDBBackends(), they will certainly see
	 * the correct value on their next try.
	 */
	MyProc->databaseId = MyDatabaseId;

	/*
	 * We established a catalog snapshot while reading pg_authid and/or
	 * pg_database; but until we have set up MyDatabaseId, we won't react to
	 * incoming sinval messages for unshared catalogs, so we won't realize it
	 * if the snapshot has been invalidated.  Assume it's no good anymore.
	 */
	InvalidateCatalogSnapshot();

	/*
	 * Recheck pg_database to make sure the target database hasn't gone away.
	 * If there was a concurrent DROP DATABASE, this ensures we will die
	 * cleanly without creating a mess.
	 */
	if (!bootstrap)
	{
		HeapTuple	tuple;

		tuple = GetDatabaseTuple(dbname);
		if (!HeapTupleIsValid(tuple) ||
			MyDatabaseId != HeapTupleGetOid(tuple) ||
			MyDatabaseTableSpace != ((Form_pg_database) GETSTRUCT(tuple))->dattablespace)
			ereport(FATAL,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database \"%s\" does not exist", dbname),
					 errdetail("It seems to have just been dropped or renamed.")));
	}

	/*
	 * Now we should be able to access the database directory safely. Verify
	 * it's there and looks reasonable.
	 */
	fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace, false);

	if (!bootstrap)
	{
		if (access(fullpath, F_OK) == -1)
		{
			if (errno == ENOENT)
				ereport(FATAL,
						(errcode(ERRCODE_UNDEFINED_DATABASE),
						 errmsg("database \"%s\" does not exist",
								dbname),
						 errdetail("The database subdirectory \"%s\" is missing.",
								   fullpath)));
			else
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not access directory \"%s\": %m",
								fullpath)));
		}

		ValidatePgVersion(fullpath);
	}

	SetDatabasePath(fullpath);

	if (POLAR_FILE_IN_SHARED_STORAGE())
	{
		char	   *polar_fullpath = NULL;

		polar_fullpath = polar_get_database_path(MyDatabaseId, MyDatabaseTableSpace);
		if (!bootstrap)
		{
			if (polar_access(polar_fullpath, F_OK) == -1)
			{
				if (errno == ENOENT)
					ereport(FATAL,
							(errcode(ERRCODE_UNDEFINED_DATABASE),
							 errmsg("database \"%s\" does not exist",
									dbname),
							 errdetail("The database subdirectory \"%s\" is missing.",
									   fullpath)));
				else
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not access directory \"%s\": %m",
									fullpath)));
			}
		}
		polar_set_database_path(polar_fullpath);
	}

	/*
	 * It's now possible to do real access to the system catalogs.
	 *
	 * Load relcache entries for the system catalogs.  This must create at
	 * least the minimum set of "nailed-in" cache entries.
	 */
	RelationCacheInitializePhase3();

	/* set up ACL framework (so CheckMyDatabase can check permissions) */
	initialize_acl();

	/*
	 * Re-read the pg_database row for our database, check permissions and set
	 * up database-specific GUC settings.  We can't do this until all the
	 * database-access infrastructure is up.  (Also, it wants to know if the
	 * user is a superuser, so the above stuff has to happen first.)
	 */
	if (!bootstrap)
		CheckMyDatabase(dbname, am_superuser, override_allow_connections);

	/*
	 * Now process any command-line switches and any additional GUC variable
	 * settings passed in the startup packet.   We couldn't do this before
	 * because we didn't know if client is a superuser.
	 */
	if (MyProcPort != NULL)
		process_startup_options(MyProcPort, am_superuser);

	/* Process pg_db_role_setting options */
	process_settings(MyDatabaseId, GetSessionUserId());

	/* Apply PostAuthDelay as soon as we've read all options */
	if (PostAuthDelay > 0)
		pg_usleep(PostAuthDelay * 1000000L);

	/*
	 * Initialize various default states that can't be set up until we've
	 * selected the active user and gotten the right GUC settings.
	 */

	/* set default namespace search path */
	InitializeSearchPath();

	/* initialize client encoding */
	InitializeClientEncoding();

	/* Initialize this backend's session state. */
	InitializeSession();

	/* report this backend in the PgBackendStatus array */
	if (!bootstrap)
		pgstat_bestart();

	/* close the transaction we started above */
	if (!bootstrap)
		CommitTransactionCommand();
}

/*
 * Process any command-line switches and any additional GUC variable
 * settings passed in the startup packet.
 */
static void
process_startup_options(Port *port, bool am_superuser)
{
	GucContext	gucctx;
	ListCell   *gucopts;

	gucctx = am_superuser ? PGC_SU_BACKEND : PGC_BACKEND;

	/*
	 * First process any command-line switches that were included in the
	 * startup packet, if we are in a regular backend.
	 */
	if (port->cmdline_options != NULL)
	{
		/*
		 * The maximum possible number of commandline arguments that could
		 * come from port->cmdline_options is (strlen + 1) / 2; see
		 * pg_split_opts().
		 */
		char	  **av;
		int			maxac;
		int			ac;

		ELOG_PSS(DEBUG1, "process_startup_options '%s'", port->cmdline_options);

		maxac = 2 + (strlen(port->cmdline_options) + 1) / 2;

		av = (char **) palloc(maxac * sizeof(char *));
		ac = 0;

		av[ac++] = "postgres";

		pg_split_opts(av, &ac, port->cmdline_options);

		av[ac] = NULL;

		Assert(ac < maxac);

		(void) process_postgres_switches(ac, av, gucctx, NULL);
	}

	/*
	 * Process any additional GUC variable settings passed in startup packet.
	 * These are handled exactly like command-line variables.
	 */
	gucopts = list_head(port->guc_options);
	while (gucopts)
	{
		char	   *name;
		char	   *value;

		name = lfirst(gucopts);
		gucopts = lnext(gucopts);

		value = lfirst(gucopts);
		gucopts = lnext(gucopts);

		ELOG_PSS(DEBUG1, "process startup packet guc_options '%s', '%s'", name, value);

		SetConfigOption(name, value, gucctx, PGC_S_CLIENT);
	}
}

/* POLAR: Shared Server */
void
polar_process_startup_options(struct Port *port, bool am_superuser)
{
	SetCurrentStatementStartTimestamp();

	process_startup_options(port, am_superuser);

	/* Process pg_db_role_setting options */
	process_settings(MyDatabaseId, GetSessionUserId());

	// /* set default namespace search path */
	InitializeSearchPath();

	/* Apply PostAuthDelay as soon as we've read all options */
	if (PostAuthDelay > 0)
		pg_usleep(PostAuthDelay * 1000000L);
}

/*
 * Load GUC settings from pg_db_role_setting.
 *
 * We try specific settings for the database/role combination, as well as
 * general for this database and for this user.
 */
static void
process_settings(Oid databaseid, Oid roleid)
{
	Relation	relsetting;
	Snapshot	snapshot;

	if (!IsUnderPostmaster)
		return;

	ELOG_PSS(DEBUG1, "process_settings %d, %d", databaseid, roleid);
	relsetting = heap_open(DbRoleSettingRelationId, AccessShareLock);

	/* read all the settings under the same snapshot for efficiency */
	snapshot = RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId));

	/* Later settings are ignored if set earlier. */
	ApplySetting(snapshot, databaseid, roleid, relsetting, PGC_S_DATABASE_USER);
	ApplySetting(snapshot, InvalidOid, roleid, relsetting, PGC_S_USER);
	ApplySetting(snapshot, databaseid, InvalidOid, relsetting, PGC_S_DATABASE);

	/*
	 * POLAR: if indicated, disable global settings for super users. Since we have
	 * allowed non-super users to set global settings which include
	 * search_patch/client_encoding/etc that could have impact on super user's
	 * session, we have to disable applying global settings (set by ALTER ROLE
	 * ALL SET ....) for super users
	 */
	if (!MyProc->issuper || polar_apply_global_guc_for_super)
		ApplySetting(snapshot, InvalidOid, InvalidOid, relsetting, PGC_S_GLOBAL);
	/* POLAR end */

	UnregisterSnapshot(snapshot);
	heap_close(relsetting, AccessShareLock);
}

/*
 * Backend-shutdown callback.  Do cleanup that we want to be sure happens
 * before all the supporting modules begin to nail their doors shut via
 * their own callbacks.
 *
 * User-level cleanup, such as temp-relation removal and UNLISTEN, happens
 * via separate callbacks that execute before this one.  We don't combine the
 * callbacks because we still want this one to happen if the user-level
 * cleanup fails.
 */
static void
ShutdownPostgres(int code, Datum arg)
{
	/* Make sure we've killed any active transaction */
	AbortOutOfAnyTransaction();

#ifdef USE_PX
	TerminatePXOPT();

	if (px_OptimizerMemoryContext != NULL)
		MemoryContextDelete(px_OptimizerMemoryContext);
#endif

	/*
	 * Shared Server exit callback function that must be called before release lock,
	 * because dsa_free still need the lock.
	 */
	polar_shared_server_exit();

	/*
	 * User locks are not released by transaction end, so be sure to release
	 * them explicitly.
	 */
	LockReleaseAll(USER_LOCKMETHOD, true);
}


/*
 * STATEMENT_TIMEOUT handler: trigger a query-cancel interrupt.
 */
static void
StatementTimeoutHandler(void)
{
	int			sig = SIGINT;

	/*
	 * During authentication the timeout is used to deal with
	 * authentication_timeout - we want to quit in response to such timeouts.
	 */
	if (ClientAuthInProgress)
		sig = SIGTERM;

#ifdef HAVE_SETSID
	/* try to signal whole process group */
	kill(-MyProcPid, sig);
#endif
	kill(MyProcPid, sig);
}

/*
 * LOCK_TIMEOUT handler: trigger a query-cancel interrupt.
 */
static void
LockTimeoutHandler(void)
{
#ifdef HAVE_SETSID
	/* try to signal whole process group */
	kill(-MyProcPid, SIGINT);
#endif
	kill(MyProcPid, SIGINT);
}

static void
IdleInTransactionSessionTimeoutHandler(void)
{
	IdleInTransactionSessionTimeoutPending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

/*
 * Returns true if at least one role is defined in this database cluster.
 */
static bool
ThereIsAtLeastOneRole(void)
{
	Relation	pg_authid_rel;
	HeapScanDesc scan;
	bool		result;

	pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);

	scan = heap_beginscan_catalog(pg_authid_rel, 0, NULL);
	result = (heap_getnext(scan, ForwardScanDirection) != NULL);

	heap_endscan(scan);
	heap_close(pg_authid_rel, AccessShareLock);

	return result;
}

/*
 * POLAR: 
 * 	  Initialization for making polar dynamic bgworker visible 
 * 		in pg_stat_activity and polar_stat_activity
 *    Then how other backends implement this?
 *    	* client backends: InitPostgres()
 *    	* static bg worker: also InitPostgres()
 *    	* Auxiliary proc: in AuxiliaryProcessMain
 * 	  
 *    So we need to borrow codes from InitPostgres/AuxiliaryProcessMain and 
 * 		we'd better minimize this work for not involving additional initialization
 * 		and reducing the side effects.
 */
void
polar_init_dynamic_bgworker_in_backends(void)
{
	bool		bootstrap = IsBootstrapProcessingMode();

	/* 
	 * POLAR: Add PGPROC struct to the ProcArray 
	 *  we do this to show myself activity in pg_stat_activity
	 * 
	 *  but now it would cause standby panic when btree_xlog_delete_get_latestRemovedXid
	 *  because CountDBBackends > 0
	 */
	// InitProcessPhase2();

	/* POLAR: Get MyBackendId */
	MyBackendId = InvalidBackendId;

	SharedInvalBackendInit(false);

	if (MyBackendId > MaxBackends || MyBackendId <= 0)
		elog(FATAL, "bad backend ID: %d", MyBackendId);

	/* POLAR: Now that we have a BackendId, we can participate in ProcSignal */
	ProcSignalInit(MyBackendId);

	/* POLAR: Initialize stats collection */
	if (!bootstrap)
		pgstat_initialize();

	before_shmem_exit(ShutdownPostgres, 0);

	/* POLAR: init session user id because we are B_BG_WORKER */
	InitializeSessionUserIdStandalone();

	pgstat_bestart();

	SetProcessingMode(NormalProcessing);
}

void 
polar_session_info_init(PolarSessionContext *session, PolarSessionContext *old_session)
{
	Assert(session->is_shared);

	if (!session->info->is_inited)
	{
		pgstat_bestart();

		session->info->roleid = GetSessionUserId();
		ELOG_PSS(DEBUG1, "polar_session_info_init session %p, %d",
				session, session->session_id);
	}
	else
	{
		if (session->is_shared && session->info->is_inited && !old_session->info->is_inited)
		{
			char    remote_ps_data[NI_MAXHOST];
			MyProcPort->polar_startup_gucs_hash = session->info->client_port->polar_startup_gucs_hash;
			if (session->info->client_port->remote_port[0] == '\0')
				snprintf(remote_ps_data, sizeof(remote_ps_data), "%s",
						session->info->client_port->remote_host);
			else
				snprintf(remote_ps_data, sizeof(remote_ps_data), "%s(%s)",
						session->info->client_port->remote_host,
						session->info->client_port->remote_port);

			StartTransactionCommand();
			GetTransactionSnapshot();

			/* process startup guc */
			polar_process_startup_options(session->info->client_port, superuser());

			CommitTransactionCommand();

			polar_ss_init_ps_display(MyProcPort->user_name, MyProcPort->database_name, 
									MyProcPort->polar_startup_gucs_hash, remote_ps_data,
									update_process_title ? "idle" : "");

			old_session->info->is_inited = true;
			ELOG_PSS(DEBUG1, "polar_session_info_init local %p, %d",
					session, session->session_id);
		}

		Assert(MyProcPort->polar_startup_gucs_hash ==
			session->info->client_port->polar_startup_gucs_hash);
	}
}