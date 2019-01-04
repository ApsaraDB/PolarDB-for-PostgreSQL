/*-------------------------------------------------------------------------
 *
 * pgfnames.c
 *	  directory handling functions
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/pgfnames.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <dirent.h>

/* POLAR */
#ifndef FRONTEND
#include <sys/stat.h>
#include "storage/polar_fd.h"
#endif

/*
 * pgfnames
 *
 * return a list of the names of objects in the argument directory.  Caller
 * must call pgfnames_cleanup later to free the memory allocated by this
 * function.
 */
char	  **
pgfnames(const char *path)
{
	DIR		   *dir;
	struct dirent *file;
	char	  **filenames;
	int			numnames = 0;
	int			fnsize = 200;	/* enough for many small dbs */

#ifndef FRONTEND
	dir = polar_opendir(path);
#else
	dir = opendir(path);
#endif
	if (dir == NULL)
	{
#ifndef FRONTEND
		elog(WARNING, "could not open directory \"%s\": %m", path);
#else
		fprintf(stderr, _("could not open directory \"%s\": %s\n"),
				path, strerror(errno));
#endif
		return NULL;
	}

	filenames = (char **) palloc(fnsize * sizeof(char *));

#ifndef FRONTEND
	while (errno = 0, (file = polar_readdir(dir)) != NULL)
#else
	while (errno = 0, (file = readdir(dir)) != NULL)
#endif
	{
		if (strcmp(file->d_name, ".") != 0 && strcmp(file->d_name, "..") != 0)
		{
			if (numnames + 1 >= fnsize)
			{
				fnsize *= 2;
				filenames = (char **) repalloc(filenames,
											   fnsize * sizeof(char *));
			}
			filenames[numnames++] = pstrdup(file->d_name);
		}
	}

	if (errno)
	{
#ifndef FRONTEND
		elog(WARNING, "could not read directory \"%s\": %m", path);
#else
		fprintf(stderr, _("could not read directory \"%s\": %s\n"),
				path, strerror(errno));
#endif
	}

	filenames[numnames] = NULL;

#ifndef FRONTEND
	if (polar_closedir(dir))
#else
	if (closedir(dir))
#endif
	{
#ifndef FRONTEND
		elog(WARNING, "could not close directory \"%s\": %m", path);
#else
		fprintf(stderr, _("could not close directory \"%s\": %s\n"),
				path, strerror(errno));
#endif
	}

	return filenames;
}


/*
 *	pgfnames_cleanup
 *
 *	deallocate memory used for filenames
 */
void
pgfnames_cleanup(char **filenames)
{
	char	  **fn;

	for (fn = filenames; *fn; fn++)
		pfree(*fn);

	pfree(filenames);
}
