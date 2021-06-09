/*-------------------------------------------------------------------------
 *
 * xlogarchive.c
 *		Functions for archiving WAL files and restoring from the archive.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogarchive.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "replication/walsender.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"

/* POLAR */
#include "polar_datamax/polar_datamax.h"
#include "utils/guc.h"
#include "polar_dma/polar_dma.h"
/* POLAR end */

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, fill "path" with its complete path (note that this will be
 * a temp file name that doesn't follow the normal naming convention), and
 * return true.
 *
 * If not successful, fill "path" with the name of the normal on-line file
 * (which may or may not actually exist, but we'll try to use it), and return
 * false.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 *
 * When 'cleanupEnabled' is false, refrain from deleting any old WAL segments
 * in the archive. This is used when fetching the initial checkpoint record,
 * when we are not yet sure how far back we need the WAL.
 */
bool
RestoreArchivedFile(char *path, const char *xlogfname,
					const char *recovername, off_t expectedSize,
					bool cleanupEnabled)
{
	char		xlogpath[MAXPGPATH];
	char		xlogRestoreCmd[MAXPGPATH];
	char		lastRestartPointFname[MAXPGPATH];
	char	   *dp;
	char	   *endp;
	const char *sp;
	int			rc;
	struct stat stat_buf;
	XLogSegNo	restartSegNo;
	XLogRecPtr	restartRedoPtr;
	TimeLineID	restartTli;

	/* In standby mode, restore_command might not be supplied */
	if (recoveryRestoreCommand == NULL)
		goto not_available;

	/*
	 * When doing archive recovery, we always prefer an archived log file even
	 * if a file of the same name exists in XLOGDIR.  The reason is that the
	 * file in XLOGDIR could be an old, un-filled or partly-filled version
	 * that was copied and restored as part of backing up $PGDATA.
	 *
	 * We could try to optimize this slightly by checking the local copy
	 * lastchange timestamp against the archived copy, but we have no API to
	 * do this, nor can we guarantee that the lastchange timestamp was
	 * preserved correctly when we copied to archive. Our aim is robustness,
	 * so we elect not to do this.
	 *
	 * If we cannot obtain the log file from the archive, however, we will try
	 * to use the XLOGDIR file if it exists.  This is so that we can make use
	 * of log segments that weren't yet transferred to the archive.
	 *
	 * Notice that we don't actually overwrite any files when we copy back
	 * from archive because the restore_command may inadvertently restore
	 * inappropriate xlogs, or they may be corrupt, so we may wish to fallback
	 * to the segments remaining in current XLOGDIR later. The
	 * copy-from-archive filename is always the same, ensuring that we don't
	 * run out of disk space on long recoveries.
	 */
	snprintf(xlogpath, MAXPGPATH, XLOGDIR "/%s", recovername);

	/*
	 * Make sure there is no existing file named recovername.
	 */
	if (stat(xlogpath, &stat_buf) != 0)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							xlogpath)));
	}
	else
	{
		if (unlink(xlogpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							xlogpath)));
	}

	/*
	 * Calculate the archive file cutoff point for use during log shipping
	 * replication. All files earlier than this point can be deleted from the
	 * archive, though there is no requirement to do so.
	 *
	 * If cleanup is not enabled, initialise this with the filename of
	 * InvalidXLogRecPtr, which will prevent the deletion of any WAL files
	 * from the archive because of the alphabetic sorting property of WAL
	 * filenames.
	 *
	 * Once we have successfully located the redo pointer of the checkpoint
	 * from which we start recovery we never request a file prior to the redo
	 * pointer of the last restartpoint. When redo begins we know that we have
	 * successfully located it, so there is no need for additional status
	 * flags to signify the point when we can begin deleting WAL files from
	 * the archive.
	 */
	if (cleanupEnabled)
	{
		GetOldestRestartPoint(&restartRedoPtr, &restartTli);
		XLByteToSeg(restartRedoPtr, restartSegNo, wal_segment_size);
		XLogFileName(lastRestartPointFname, restartTli, restartSegNo,
					 wal_segment_size);
		/* we shouldn't need anything earlier than last restart point */
		Assert(strcmp(lastRestartPointFname, xlogfname) <= 0);
	}
	else
		XLogFileName(lastRestartPointFname, 0, 0L, wal_segment_size);

	/*
	 * construct the command to be executed
	 */
	dp = xlogRestoreCmd;
	endp = xlogRestoreCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = recoveryRestoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of target file */
					sp++;
					StrNCpy(dp, xlogpath, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of desired file */
					sp++;
					StrNCpy(dp, xlogfname, endp - dp);
					dp += strlen(dp);
					break;
				case 'r':
					/* %r: filename of last restartpoint */
					sp++;
					StrNCpy(dp, lastRestartPointFname, endp - dp);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	ereport(DEBUG3,
			(errmsg_internal("executing restore command \"%s\"",
							 xlogRestoreCmd)));

	/*
	 * Check signals before restore command and reset afterwards.
	 */
	PreRestoreCommand();

	/*
	 * Copy xlog from archival storage to XLOGDIR
	 */
	rc = system(xlogRestoreCmd);

	PostRestoreCommand();

	if (rc == 0)
	{
		/*
		 * command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				int			elevel;

				/*
				 * If we find a partial file in standby mode, we assume it's
				 * because it's just being copied to the archive, and keep
				 * trying.
				 *
				 * Otherwise treat a wrong-sized file as FATAL to ensure the
				 * DBA would notice it, but is that too strong? We could try
				 * to plow ahead with a local copy of the file ... but the
				 * problem is that there probably isn't one, and we'd
				 * incorrectly conclude we've reached the end of WAL and we're
				 * done recovering ...
				 */
				if (StandbyMode && stat_buf.st_size < expectedSize)
					elevel = DEBUG1;
				else
					elevel = FATAL;
				ereport(elevel,
						(errmsg("archive file \"%s\" has wrong size: %lu instead of %lu",
								xlogfname,
								(unsigned long) stat_buf.st_size,
								(unsigned long) expectedSize)));
				return false;
			}
			else
			{
				ereport(LOG,
						(errmsg("restored log file \"%s\" from archive",
								xlogfname)));
				strcpy(path, xlogpath);
				return true;
			}
		}
		else
		{
			/* stat failed */
			if (errno != ENOENT)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								xlogpath)));
		}
	}

	/*
	 * Remember, we rollforward UNTIL the restore fails so failure here is
	 * just part of the process... that makes it difficult to determine
	 * whether the restore failed because there isn't an archive to restore,
	 * or because the administrator has specified the restore program
	 * incorrectly.  We have to assume the former.
	 *
	 * However, if the failure was due to any sort of signal, it's best to
	 * punt and abort recovery.  (If we "return false" here, upper levels will
	 * assume that recovery is complete and start up the database!) It's
	 * essential to abort on child SIGINT and SIGQUIT, because per spec
	 * system() ignores SIGINT and SIGQUIT while waiting; if we see one of
	 * those it's a good bet we should have gotten it too.
	 *
	 * On SIGTERM, assume we have received a fast shutdown request, and exit
	 * cleanly. It's pure chance whether we receive the SIGTERM first, or the
	 * child process. If we receive it first, the signal handler will call
	 * proc_exit, otherwise we do it here. If we or the child process received
	 * SIGTERM for any other reason than a fast shutdown request, postmaster
	 * will perform an immediate shutdown when it sees us exiting
	 * unexpectedly.
	 *
	 * We treat hard shell errors such as "command not found" as fatal, too.
	 */
	if (wait_result_is_signal(rc, SIGTERM))
		proc_exit(1);

	ereport(wait_result_is_any_signal(rc, true) ? FATAL : DEBUG2,
			(errmsg("could not restore file \"%s\" from archive: %s",
					xlogfname, wait_result_to_str(rc))));

not_available:

	/*
	 * if an archived file is not available, there might still be a version of
	 * this file in XLOGDIR, so return that as the filename to open.
	 *
	 * In many recovery scenarios we expect this to fail also, but if so that
	 * just means we've reached the end of WAL.
	 */
	snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
	return false;
}

/*
 * Attempt to execute an external shell command during recovery.
 *
 * 'command' is the shell command to be executed, 'commandName' is a
 * human-readable name describing the command emitted in the logs. If
 * 'failOnSignal' is true and the command is killed by a signal, a FATAL
 * error is thrown. Otherwise a WARNING is emitted.
 *
 * This is currently used for recovery_end_command and archive_cleanup_command.
 */
void
ExecuteRecoveryCommand(const char *command, const char *commandName, bool failOnSignal)
{
	char		xlogRecoveryCmd[MAXPGPATH];
	char		lastRestartPointFname[MAXPGPATH];
	char	   *dp;
	char	   *endp;
	const char *sp;
	int			rc;
	XLogSegNo	restartSegNo;
	XLogRecPtr	restartRedoPtr;
	TimeLineID	restartTli;

	Assert(command && commandName);

	/*
	 * Calculate the archive file cutoff point for use during log shipping
	 * replication. All files earlier than this point can be deleted from the
	 * archive, though there is no requirement to do so.
	 */
	GetOldestRestartPoint(&restartRedoPtr, &restartTli);
	XLByteToSeg(restartRedoPtr, restartSegNo, wal_segment_size);
	XLogFileName(lastRestartPointFname, restartTli, restartSegNo,
				 wal_segment_size);

	/*
	 * construct the command to be executed
	 */
	dp = xlogRecoveryCmd;
	endp = xlogRecoveryCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'r':
					/* %r: filename of last restartpoint */
					sp++;
					StrNCpy(dp, lastRestartPointFname, endp - dp);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	ereport(DEBUG3,
			(errmsg_internal("executing %s \"%s\"", commandName, command)));

	/*
	 * execute the constructed command
	 */
	rc = system(xlogRecoveryCmd);
	if (rc != 0)
	{
		/*
		 * If the failure was due to any sort of signal, it's best to punt and
		 * abort recovery.  See comments in RestoreArchivedFile().
		 */
		ereport((failOnSignal && wait_result_is_any_signal(rc, true)) ? FATAL : WARNING,
		/*------
		   translator: First %s represents a recovery.conf parameter name like
		  "recovery_end_command", the 2nd is the value of that parameter, the
		  third an already translated error message. */
				(errmsg("%s \"%s\": %s", commandName,
						command, wait_result_to_str(rc))));
	}
}


/*
 * A file was restored from the archive under a temporary filename (path),
 * and now we want to keep it. Rename it under the permanent filename in
 * in pg_wal (xlogfname), replacing any existing file with the same name.
 */
void
KeepFileRestoredFromArchive(const char *path, const char *xlogfname)
{
	char		xlogfpath[MAXPGPATH];
	bool		reload = false;
	struct stat statbuf;

	snprintf(xlogfpath, MAXPGPATH, XLOGDIR "/%s", xlogfname);

	if (stat(xlogfpath, &statbuf) == 0)
	{
		char		oldpath[MAXPGPATH];

#ifdef WIN32
		static unsigned int deletedcounter = 1;

		/*
		 * On Windows, if another process (e.g a walsender process) holds the
		 * file open in FILE_SHARE_DELETE mode, unlink will succeed, but the
		 * file will still show up in directory listing until the last handle
		 * is closed, and we cannot rename the new file in its place until
		 * that. To avoid that problem, rename the old file to a temporary
		 * name first. Use a counter to create a unique filename, because the
		 * same file might be restored from the archive multiple times, and a
		 * walsender could still be holding onto an old deleted version of it.
		 */
		snprintf(oldpath, MAXPGPATH, "%s.deleted%u",
				 xlogfpath, deletedcounter++);
		if (rename(xlogfpath, oldpath) != 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rename file \"%s\" to \"%s\": %m",
							xlogfpath, oldpath)));
		}
#else
		/* same-size buffers, so this never truncates */
		strlcpy(oldpath, xlogfpath, MAXPGPATH);
#endif
		if (unlink(oldpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							xlogfpath)));
		reload = true;
	}

	durable_rename(path, xlogfpath, ERROR);

	/*
	 * Create .done file forcibly to prevent the restored segment from being
	 * archived again later.
	 */
	if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
		XLogArchiveForceDone(xlogfname);
	else
		XLogArchiveNotify(xlogfname);

	/*
	 * If the existing file was replaced, since walsenders might have it open,
	 * request them to reload a currently-open segment. This is only required
	 * for WAL segments, walsenders don't hold other files open, but there's
	 * no harm in doing this too often, and we don't know what kind of a file
	 * we're dealing with here.
	 */
	if (reload)
		WalSndRqstFileReload();

	/*
	 * Signal walsender that new WAL has arrived. Again, this isn't necessary
	 * if we restored something other than a WAL segment, but it does no harm
	 * either.
	 */
	WalSndWakeup();
}

/*
 * XLogArchiveNotify
 *
 * Create an archive notification file
 *
 * The name of the notification file is the message that will be picked up
 * by the archiver, e.g. we write 0000000100000001000000C6.ready
 * and the archiver then knows to archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
void
XLogArchiveNotify(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	FILE	   *fd;

	if (polar_enable_dma)
	{
		char		archiveLocalReady[MAXPGPATH];
		struct stat stat_buf;

		/* If .local exists, rename it to .ready */
		StatusFilePath(archiveLocalReady, xlog, ".local");
		if (stat(archiveLocalReady, &stat_buf) == 0)
		{
			(void) durable_rename(archiveLocalReady, archiveStatusPath, WARNING);
			return;
		}
	}

	/* insert an otherwise empty file called <XLOG>.ready */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	fd = AllocateFile(archiveStatusPath, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}

	/* Notify archiver that it's got something to do */
	/* POLAR: No need to notify archiver in datamax mode */
	if (IsUnderPostmaster && !polar_is_dma_logger_mode)
		SendPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER);
}

/*
 * Convenience routine to notify using segment number representation of filename
 */
void
XLogArchiveNotifySeg(XLogSegNo segno)
{
	char		xlog[MAXFNAMELEN];

	XLogFileName(xlog, ThisTimeLineID, segno, wal_segment_size);

	/*
	 * POLAR: in dma mode, rename to .local firstly, rename it to .ready by
	 * archiver after consensus commit
	 */
	if (polar_enable_dma)
		polar_dma_xlog_archive_notify(xlog, true);
	else
		XLogArchiveNotify(xlog);
}

/*
 * XLogArchiveForceDone
 *
 * Emit notification forcibly that an XLOG segment file has been successfully
 * archived, by creating <XLOG>.done regardless of whether <XLOG>.ready
 * exists or not.
 */
void
XLogArchiveForceDone(const char *xlog)
{
	char		archiveReady[MAXPGPATH];
	char		archiveDone[MAXPGPATH];
	struct stat stat_buf;
	FILE	   *fd;

	/* Exit if already known done */
	StatusFilePath(archiveDone, xlog, ".done");
	if (stat(archiveDone, &stat_buf) == 0)
		return;

	/*
	 * POLAR: in DMA mode, delete .paxos if exists. rename .local to .done if
	 * exists
	 */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveReady, xlog, ".paxos");
		if (stat(archiveReady, &stat_buf) == 0)
		{
			unlink(archiveReady);
		}

		StatusFilePath(archiveReady, xlog, ".local");
		if (stat(archiveReady, &stat_buf) == 0)
		{
			(void) durable_rename(archiveReady, archiveDone, WARNING);
			return;
		}
	}
	/* POLAR end */

	/* If .ready exists, rename it to .done */
	StatusFilePath(archiveReady, xlog, ".ready");
	if (stat(archiveReady, &stat_buf) == 0)
	{
		(void) durable_rename(archiveReady, archiveDone, WARNING);
		return;
	}

	/* insert an otherwise empty file called <XLOG>.done */
	fd = AllocateFile(archiveDone, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveDone)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveDone)));
		return;
	}
}

/*
 * XLogArchiveCheckDone
 *
 * This is called when we are ready to delete or recycle an old XLOG segment
 * file or backup history file.  If it is okay to delete it then return true.
 * If it is not time to delete it, make sure a .ready file exists, and return
 * false.
 *
 * If <XLOG>.done exists, then return true; else if <XLOG>.ready exists,
 * then return false; else create <XLOG>.ready and return false.
 *
 * The reason we do things this way is so that if the original attempt to
 * create <XLOG>.ready fails, we'll retry during subsequent checkpoints.
 */
bool
XLogArchiveCheckDone(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;
	bool		inRecovery = RecoveryInProgress();

	/*
	 * The file is always deletable if archive_mode is "off".  On standbys
	 * archiving is disabled if archive_mode is "on", and enabled with
	 * "always".  On a primary, archiving is enabled if archive_mode is "on"
	 * or "always".
	 */
	if (!((XLogArchivingActive() && !inRecovery) ||
		  (XLogArchivingAlways() && inRecovery)))
		return true;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/*
	 * POLAR: in dma mode, check for ready.local --- this means archiver is
	 * still busy with it
	 */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveStatusPath, xlog, ".local");
		if (stat(archiveStatusPath, &stat_buf) == 0)
			return false;
	}
	/* POLAR: end */

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Retry creation of the .ready file */
	XLogArchiveNotify(xlog);
	return false;
}

/*
 * XLogArchiveIsBusy
 *
 * Check to see if an XLOG segment file is still unarchived.
 * This is almost but not quite the inverse of XLogArchiveCheckDone: in
 * the first place we aren't chartered to recreate the .ready file, and
 * in the second place we should consider that if the file is already gone
 * then it's not busy.  (This check is needed to handle the race condition
 * that a checkpoint already deleted the no-longer-needed file.)
 */
bool
XLogArchiveIsBusy(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/* POLAR: check for .local --- this means archiver is still busy with it */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveStatusPath, xlog, ".local");
		if (stat(archiveStatusPath, &stat_buf) == 0)
			return true;
	}
	/* POLAR end */

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/*
	 * Check to see if the WAL file has been removed by checkpoint, which
	 * implies it has already been archived, and explains why we can't see a
	 * status file for it.
	 */
	snprintf(archiveStatusPath, MAXPGPATH, XLOGDIR "/%s", xlog);
	if (stat(archiveStatusPath, &stat_buf) != 0 &&
		errno == ENOENT)
		return false;

	return true;
}

/*
 * XLogArchiveIsReadyOrDone
 *
 * Check to see if an XLOG segment file has a .ready or .done file.
 * This is similar to XLogArchiveIsBusy(), but returns true if the file
 * is already archived or is about to be archived.
 *
 * This is currently only used at recovery.  During normal operation this
 * would be racy: the file might get removed or marked with .ready as we're
 * checking it, or immediately after we return.
 */
bool
XLogArchiveIsReadyOrDone(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* POLAR: check for .local --- this means archiver is still busy with it */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveStatusPath, xlog, ".local");
		if (stat(archiveStatusPath, &stat_buf) == 0)
			return true;
	}
	/* POLAR end */

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * XLogArchiveIsReady
 *
 * Check to see if an XLOG segment file has an archive notification (.ready)
 * file.
 */
bool
XLogArchiveIsReady(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* POLAR: in dma mode, check .local file firstly */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveStatusPath, xlog, ".local");
		if (stat(archiveStatusPath, &stat_buf) == 0)
			return true;
	}
	/* POLAR: end */

	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * XLogArchiveCleanup
 *
 * Cleanup archive notification file(s) for a particular xlog segment
 */
void
XLogArchiveCleanup(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];

	/* Remove the .done file */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	unlink(archiveStatusPath);
	/* should we complain about failure? */

	/* Remove the .ready file if present --- normally it shouldn't be */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	unlink(archiveStatusPath);
	/* should we complain about failure? */

	/* POLAR: Remove the .local file if present --- normally it shouldn't be */
	if (polar_enable_dma)
	{
		StatusFilePath(archiveStatusPath, xlog, ".local");
		unlink(archiveStatusPath);
		StatusFilePath(archiveStatusPath, xlog, ".paxos");
		unlink(archiveStatusPath);
	}
	/* should we complain about failure? */
	/* POLAR: end */
}

/*
 * polar_dma_xlog_archive_notify_local
 *
 * Create an archive notification file
 *
 * In DMA mode, The name of the notification file is the message that will be
 * picked up by the archiver, e.g. we write 0000000100000001000000C6.local
 * and the archiver then rename it to .ready if all record consensus commit
 * and the archiver then archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
void
polar_dma_xlog_archive_notify(const char *xlog, bool local)
{
	char		archiveStatusPath[MAXPGPATH];
	FILE	   *fd;

	/* POLAR: if ready or done, ignore it */
	if (local && XLogArchiveIsReadyOrDone(xlog))
		return;

	/* insert an otherwise empty file called <XLOG>.local or <XLOG>.paxos */
	StatusFilePath(archiveStatusPath, xlog, local ? ".local" : ".paxos");

	fd = AllocateFile(archiveStatusPath, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}

	/* Notify archiver that it's got something to do */
	if (IsUnderPostmaster)
		SendPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER);
}
