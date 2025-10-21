/*-------------------------------------------------------------------------
 *
 * ts_locale.c
 *		locale compatibility layer for tsearch
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/ts_locale.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/string.h"
#include "storage/fd.h"
#include "tsearch/ts_locale.h"

static void tsearch_readline_callback(void *arg);


/* space for a single character plus a trailing NUL */
#define WC_BUF_LEN  2

int
t_isalpha(const char *ptr)
{
	pg_wchar	wstr[WC_BUF_LEN];
	int			wlen pg_attribute_unused();

	wlen = pg_mb2wchar_with_len(ptr, wstr, pg_mblen(ptr));
	Assert(wlen <= 1);

	/* pass single character, or NUL if empty */
	return pg_iswalpha(wstr[0], pg_database_locale());
}

int
t_isalnum(const char *ptr)
{
	pg_wchar	wstr[WC_BUF_LEN];
	int			wlen pg_attribute_unused();

	wlen = pg_mb2wchar_with_len(ptr, wstr, pg_mblen(ptr));
	Assert(wlen <= 1);

	/* pass single character, or NUL if empty */
	return pg_iswalnum(wstr[0], pg_database_locale());
}


/*
 * Set up to read a file using tsearch_readline().  This facility is
 * better than just reading the file directly because it provides error
 * context pointing to the specific line where a problem is detected.
 *
 * Expected usage is:
 *
 *		tsearch_readline_state trst;
 *
 *		if (!tsearch_readline_begin(&trst, filename))
 *			ereport(ERROR,
 *					(errcode(ERRCODE_CONFIG_FILE_ERROR),
 *					 errmsg("could not open stop-word file \"%s\": %m",
 *							filename)));
 *		while ((line = tsearch_readline(&trst)) != NULL)
 *			process line;
 *		tsearch_readline_end(&trst);
 *
 * Note that the caller supplies the ereport() for file open failure;
 * this is so that a custom message can be provided.  The filename string
 * passed to tsearch_readline_begin() must remain valid through
 * tsearch_readline_end().
 */
bool
tsearch_readline_begin(tsearch_readline_state *stp,
					   const char *filename)
{
	if ((stp->fp = AllocateFile(filename, "r")) == NULL)
		return false;
	stp->filename = filename;
	stp->lineno = 0;
	initStringInfo(&stp->buf);
	stp->curline = NULL;
	/* Setup error traceback support for ereport() */
	stp->cb.callback = tsearch_readline_callback;
	stp->cb.arg = stp;
	stp->cb.previous = error_context_stack;
	error_context_stack = &stp->cb;
	return true;
}

/*
 * Read the next line from a tsearch data file (expected to be in UTF-8), and
 * convert it to database encoding if needed. The returned string is palloc'd.
 * NULL return means EOF.
 */
char *
tsearch_readline(tsearch_readline_state *stp)
{
	char	   *recoded;

	/* Advance line number to use in error reports */
	stp->lineno++;

	/* Clear curline, it's no longer relevant */
	if (stp->curline)
	{
		if (stp->curline != stp->buf.data)
			pfree(stp->curline);
		stp->curline = NULL;
	}

	/* Collect next line, if there is one */
	if (!pg_get_line_buf(stp->fp, &stp->buf))
		return NULL;

	/* Validate the input as UTF-8, then convert to DB encoding if needed */
	recoded = pg_any_to_server(stp->buf.data, stp->buf.len, PG_UTF8);

	/* Save the correctly-encoded string for possible error reports */
	stp->curline = recoded;		/* might be equal to buf.data */

	/*
	 * We always return a freshly pstrdup'd string.  This is clearly necessary
	 * if pg_any_to_server() returned buf.data, and we need a second copy even
	 * if encoding conversion did occur.  The caller is entitled to pfree the
	 * returned string at any time, which would leave curline pointing to
	 * recycled storage, causing problems if an error occurs after that point.
	 * (It's preferable to return the result of pstrdup instead of the output
	 * of pg_any_to_server, because the conversion result tends to be
	 * over-allocated.  Since callers might save the result string directly
	 * into a long-lived dictionary structure, we don't want it to be a larger
	 * palloc chunk than necessary.  We'll reclaim the conversion result on
	 * the next call.)
	 */
	return pstrdup(recoded);
}

/*
 * Close down after reading a file with tsearch_readline()
 */
void
tsearch_readline_end(tsearch_readline_state *stp)
{
	/* Suppress use of curline in any error reported below */
	if (stp->curline)
	{
		if (stp->curline != stp->buf.data)
			pfree(stp->curline);
		stp->curline = NULL;
	}

	/* Release other resources */
	pfree(stp->buf.data);
	FreeFile(stp->fp);

	/* Pop the error context stack */
	error_context_stack = stp->cb.previous;
}

/*
 * Error context callback for errors occurring while reading a tsearch
 * configuration file.
 */
static void
tsearch_readline_callback(void *arg)
{
	tsearch_readline_state *stp = (tsearch_readline_state *) arg;

	/*
	 * We can't include the text of the config line for errors that occur
	 * during tsearch_readline() itself.  The major cause of such errors is
	 * encoding violations, and we daren't try to print error messages
	 * containing badly-encoded data.
	 */
	if (stp->curline)
		errcontext("line %d of configuration file \"%s\": \"%s\"",
				   stp->lineno,
				   stp->filename,
				   stp->curline);
	else
		errcontext("line %d of configuration file \"%s\"",
				   stp->lineno,
				   stp->filename);
}
