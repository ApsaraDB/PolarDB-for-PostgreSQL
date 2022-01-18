/*-------------------------------------------------------------------------
 *
 * rmtree.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/rmtree.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <unistd.h>
#include <sys/stat.h>

/* POLAR */
#ifndef FRONTEND
#include "storage/polar_fd.h"
#endif

/*
 *	rmtree
 *
 *	Delete a directory tree recursively.
 *	Assumes path points to a valid directory.
 *	Deletes everything under path.
 *	If rmtopdir is true deletes the directory too.
 *	Returns true if successful, false if there was any problem.
 *	(The details of the problem are reported already, so caller
 *	doesn't really have to say anything more, but most do.)
 *
 *  POLAR: for BACKEND support local or shared storage
 */
bool
rmtree(const char *path, bool rmtopdir)
{
	bool		result = true;
	char		pathbuf[MAXPGPATH];
	char	  **filenames;
	char	  **filename;
	struct stat statbuf;

	/*
	 * we copy all the names out of the directory before we start modifying
	 * it.
	 */
	filenames = pgfnames(path);

	if (filenames == NULL)
		return false;

	/* now we have the names we can start removing things */
	for (filename = filenames; *filename; filename++)
	{
		snprintf(pathbuf, MAXPGPATH, "%s/%s", path, *filename);

		/*
		 * It's ok if the file is not there anymore; we were just about to
		 * delete it anyway.
		 *
		 * This is not an academic possibility. One scenario where this
		 * happens is when bgwriter has a pending unlink request for a file in
		 * a database that's being dropped. In dropdb(), we call
		 * ForgetDatabaseFsyncRequests() to flush out any such pending unlink
		 * requests, but because that's asynchronous, it's not guaranteed that
		 * the bgwriter receives the message in time.
		 */
#ifndef FRONTEND
		if (polar_lstat(pathbuf, &statbuf) != 0)
#else
		if (lstat(pathbuf, &statbuf) != 0)
#endif
		{
			if (errno != ENOENT)
			{
#ifndef FRONTEND
				elog(WARNING, "could not stat file or directory \"%s\": %m",
					 pathbuf);
#else
				fprintf(stderr, _("could not stat file or directory \"%s\": %s\n"),
						pathbuf, strerror(errno));
#endif
				result = false;
			}
			continue;
		}

		if (S_ISDIR(statbuf.st_mode))
		{
			/* call ourselves recursively for a directory */
			if (!rmtree(pathbuf, true))
			{
				/* we already reported the error */
				result = false;
			}
		}
		else
		{
#ifndef FRONTEND
			if (polar_unlink(pathbuf) != 0)
#else
			if (unlink(pathbuf) != 0)
#endif
			{
				if (errno != ENOENT)
				{
#ifndef FRONTEND
					elog(WARNING, "could not remove file or directory \"%s\": %m",
						 pathbuf);
#else
					fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"),
							pathbuf, strerror(errno));
#endif
					result = false;
				}
			}
		}
	}

	if (rmtopdir)
	{
#ifndef FRONTEND
		if (polar_rmdir(path) != 0)
#else
		if (rmdir(path) != 0)
#endif
		{
#ifndef FRONTEND
			elog(WARNING, "could not remove file or directory \"%s\": %m",
				 path);
#else
			fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"),
					path, strerror(errno));
#endif
			result = false;
		}
	}

	pgfnames_cleanup(filenames);

	return result;
}

