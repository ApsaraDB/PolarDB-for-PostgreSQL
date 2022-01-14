/*-------------------------------------------------------------------------
 *
 * pg_backup_tar.c
 *
 *	This file is copied from the 'files' format file, but dumps data into
 *	one temp file then sends it to the output TAR archive.
 *
 *	The tar format also includes a 'restore.sql' script which is there for
 *	the benefit of humans. This script is never used by pg_restore.
 *
 *	NOTE: If you untar the created 'tar' file, the resulting files are
 *	compatible with the 'directory' format. Please keep the two formats in
 *	sync.
 *
 *	See the headers to pg_backup_directory & pg_restore for more details.
 *
 * Copyright (c) 2000, Philip Warner
 *		Rights are granted to use this software in any way so long
 *		as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from it's use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup_tar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pg_backup_archiver.h"
#include "pg_backup_tar.h"
#include "pg_backup_utils.h"
#include "pgtar.h"
#include "common/file_utils.h"
#include "fe_utils/string_utils.h"

#include <sys/stat.h>
#include <ctype.h>
#include <limits.h>
#include <unistd.h>

static void _ArchiveEntry(ArchiveHandle *AH, TocEntry *te);
static void _StartData(ArchiveHandle *AH, TocEntry *te);
static void _WriteData(ArchiveHandle *AH, const void *data, size_t dLen);
static void _EndData(ArchiveHandle *AH, TocEntry *te);
static int	_WriteByte(ArchiveHandle *AH, const int i);
static int	_ReadByte(ArchiveHandle *);
static void _WriteBuf(ArchiveHandle *AH, const void *buf, size_t len);
static void _ReadBuf(ArchiveHandle *AH, void *buf, size_t len);
static void _CloseArchive(ArchiveHandle *AH);
static void _PrintTocData(ArchiveHandle *AH, TocEntry *te);
static void _WriteExtraToc(ArchiveHandle *AH, TocEntry *te);
static void _ReadExtraToc(ArchiveHandle *AH, TocEntry *te);
static void _PrintExtraToc(ArchiveHandle *AH, TocEntry *te);

static void _StartBlobs(ArchiveHandle *AH, TocEntry *te);
static void _StartBlob(ArchiveHandle *AH, TocEntry *te, Oid oid);
static void _EndBlob(ArchiveHandle *AH, TocEntry *te, Oid oid);
static void _EndBlobs(ArchiveHandle *AH, TocEntry *te);

#define K_STD_BUF_SIZE 1024


typedef struct
{
#ifdef HAVE_LIBZ
	gzFile		zFH;
#else
	FILE	   *zFH;
#endif
	FILE	   *nFH;
	FILE	   *tarFH;
	FILE	   *tmpFH;
	char	   *targetFile;
	char		mode;
	pgoff_t		pos;
	pgoff_t		fileLen;
	ArchiveHandle *AH;
} TAR_MEMBER;

typedef struct
{
	int			hasSeek;
	pgoff_t		filePos;
	TAR_MEMBER *blobToc;
	FILE	   *tarFH;
	pgoff_t		tarFHpos;
	pgoff_t		tarNextMember;
	TAR_MEMBER *FH;
	int			isSpecialScript;
	TAR_MEMBER *scriptTH;
} lclContext;

typedef struct
{
	TAR_MEMBER *TH;
	char	   *filename;
} lclTocEntry;

/* translator: this is a module name */
static const char *modulename = gettext_noop("tar archiver");

static void _LoadBlobs(ArchiveHandle *AH);

static TAR_MEMBER *tarOpen(ArchiveHandle *AH, const char *filename, char mode);
static void tarClose(ArchiveHandle *AH, TAR_MEMBER *TH);

#ifdef __NOT_USED__
static char *tarGets(char *buf, size_t len, TAR_MEMBER *th);
#endif
static int	tarPrintf(ArchiveHandle *AH, TAR_MEMBER *th, const char *fmt,...) pg_attribute_printf(3, 4);

static void _tarAddFile(ArchiveHandle *AH, TAR_MEMBER *th);
static TAR_MEMBER *_tarPositionTo(ArchiveHandle *AH, const char *filename);
static size_t tarRead(void *buf, size_t len, TAR_MEMBER *th);
static size_t tarWrite(const void *buf, size_t len, TAR_MEMBER *th);
static void _tarWriteHeader(TAR_MEMBER *th);
static int	_tarGetHeader(ArchiveHandle *AH, TAR_MEMBER *th);
static size_t _tarReadRaw(ArchiveHandle *AH, void *buf, size_t len, TAR_MEMBER *th, FILE *fh);

static size_t _scriptOut(ArchiveHandle *AH, const void *buf, size_t len);

/*
 *	Initializer
 */
void
InitArchiveFmt_Tar(ArchiveHandle *AH)
{
	lclContext *ctx;

	/* Assuming static functions, this can be copied for each format. */
	AH->ArchiveEntryPtr = _ArchiveEntry;
	AH->StartDataPtr = _StartData;
	AH->WriteDataPtr = _WriteData;
	AH->EndDataPtr = _EndData;
	AH->WriteBytePtr = _WriteByte;
	AH->ReadBytePtr = _ReadByte;
	AH->WriteBufPtr = _WriteBuf;
	AH->ReadBufPtr = _ReadBuf;
	AH->ClosePtr = _CloseArchive;
	AH->ReopenPtr = NULL;
	AH->PrintTocDataPtr = _PrintTocData;
	AH->ReadExtraTocPtr = _ReadExtraToc;
	AH->WriteExtraTocPtr = _WriteExtraToc;
	AH->PrintExtraTocPtr = _PrintExtraToc;

	AH->StartBlobsPtr = _StartBlobs;
	AH->StartBlobPtr = _StartBlob;
	AH->EndBlobPtr = _EndBlob;
	AH->EndBlobsPtr = _EndBlobs;
	AH->ClonePtr = NULL;
	AH->DeClonePtr = NULL;

	AH->WorkerJobDumpPtr = NULL;
	AH->WorkerJobRestorePtr = NULL;

	/*
	 * Set up some special context used in compressing data.
	 */
	ctx = (lclContext *) pg_malloc0(sizeof(lclContext));
	AH->formatData = (void *) ctx;
	ctx->filePos = 0;
	ctx->isSpecialScript = 0;

	/* Initialize LO buffering */
	AH->lo_buf_size = LOBBUFSIZE;
	AH->lo_buf = (void *) pg_malloc(LOBBUFSIZE);

	/*
	 * Now open the tar file, and load the TOC if we're in read mode.
	 */
	if (AH->mode == archModeWrite)
	{
		if (AH->fSpec && strcmp(AH->fSpec, "") != 0)
		{
			ctx->tarFH = fopen(AH->fSpec, PG_BINARY_W);
			if (ctx->tarFH == NULL)
				exit_horribly(modulename,
							  "could not open TOC file \"%s\" for output: %s\n",
							  AH->fSpec, strerror(errno));
		}
		else
		{
			ctx->tarFH = stdout;
			if (ctx->tarFH == NULL)
				exit_horribly(modulename,
							  "could not open TOC file for output: %s\n",
							  strerror(errno));
		}

		ctx->tarFHpos = 0;

		/*
		 * Make unbuffered since we will dup() it, and the buffers screw each
		 * other
		 */
		/* setvbuf(ctx->tarFH, NULL, _IONBF, 0); */

		ctx->hasSeek = checkSeek(ctx->tarFH);

		/*
		 * We don't support compression because reading the files back is not
		 * possible since gzdopen uses buffered IO which totally screws file
		 * positioning.
		 */
		if (AH->compression != 0)
			exit_horribly(modulename,
						  "compression is not supported by tar archive format\n");
	}
	else
	{							/* Read Mode */
		if (AH->fSpec && strcmp(AH->fSpec, "") != 0)
		{
			ctx->tarFH = fopen(AH->fSpec, PG_BINARY_R);
			if (ctx->tarFH == NULL)
				exit_horribly(modulename, "could not open TOC file \"%s\" for input: %s\n",
							  AH->fSpec, strerror(errno));
		}
		else
		{
			ctx->tarFH = stdin;
			if (ctx->tarFH == NULL)
				exit_horribly(modulename, "could not open TOC file for input: %s\n",
							  strerror(errno));
		}

		/*
		 * Make unbuffered since we will dup() it, and the buffers screw each
		 * other
		 */
		/* setvbuf(ctx->tarFH, NULL, _IONBF, 0); */

		ctx->tarFHpos = 0;

		ctx->hasSeek = checkSeek(ctx->tarFH);

		/*
		 * Forcibly unmark the header as read since we use the lookahead
		 * buffer
		 */
		AH->readHeader = 0;

		ctx->FH = (void *) tarOpen(AH, "toc.dat", 'r');
		ReadHead(AH);
		ReadToc(AH);
		tarClose(AH, ctx->FH);	/* Nothing else in the file... */
	}
}

/*
 * - Start a new TOC entry
 *	 Setup the output file name.
 */
static void
_ArchiveEntry(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *ctx;
	char		fn[K_STD_BUF_SIZE];

	ctx = (lclTocEntry *) pg_malloc0(sizeof(lclTocEntry));
	if (te->dataDumper != NULL)
	{
#ifdef HAVE_LIBZ
		if (AH->compression == 0)
			sprintf(fn, "%d.dat", te->dumpId);
		else
			sprintf(fn, "%d.dat.gz", te->dumpId);
#else
		sprintf(fn, "%d.dat", te->dumpId);
#endif
		ctx->filename = pg_strdup(fn);
	}
	else
	{
		ctx->filename = NULL;
		ctx->TH = NULL;
	}
	te->formatData = (void *) ctx;
}

static void
_WriteExtraToc(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *ctx = (lclTocEntry *) te->formatData;

	if (ctx->filename)
		WriteStr(AH, ctx->filename);
	else
		WriteStr(AH, "");
}

static void
_ReadExtraToc(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *ctx = (lclTocEntry *) te->formatData;

	if (ctx == NULL)
	{
		ctx = (lclTocEntry *) pg_malloc0(sizeof(lclTocEntry));
		te->formatData = (void *) ctx;
	}

	ctx->filename = ReadStr(AH);
	if (strlen(ctx->filename) == 0)
	{
		free(ctx->filename);
		ctx->filename = NULL;
	}
	ctx->TH = NULL;
}

static void
_PrintExtraToc(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *ctx = (lclTocEntry *) te->formatData;

	if (AH->public.verbose && ctx->filename != NULL)
		ahprintf(AH, "-- File: %s\n", ctx->filename);
}

static void
_StartData(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *tctx = (lclTocEntry *) te->formatData;

	tctx->TH = tarOpen(AH, tctx->filename, 'w');
}

static TAR_MEMBER *
tarOpen(ArchiveHandle *AH, const char *filename, char mode)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	TAR_MEMBER *tm;

#ifdef HAVE_LIBZ
	char		fmode[14];
#endif

	if (mode == 'r')
	{
		tm = _tarPositionTo(AH, filename);
		if (!tm)				/* Not found */
		{
			if (filename)
			{
				/*
				 * Couldn't find the requested file. Future: do SEEK(0) and
				 * retry.
				 */
				exit_horribly(modulename, "could not find file \"%s\" in archive\n", filename);
			}
			else
			{
				/* Any file OK, none left, so return NULL */
				return NULL;
			}
		}

#ifdef HAVE_LIBZ

		if (AH->compression == 0)
			tm->nFH = ctx->tarFH;
		else
			exit_horribly(modulename, "compression is not supported by tar archive format\n");
		/* tm->zFH = gzdopen(dup(fileno(ctx->tarFH)), "rb"); */
#else
		tm->nFH = ctx->tarFH;
#endif
	}
	else
	{
		int			old_umask;

		tm = pg_malloc0(sizeof(TAR_MEMBER));

		/*
		 * POSIX does not require, but permits, tmpfile() to restrict file
		 * permissions.  Given an OS crash after we write data, the filesystem
		 * might retain the data but forget tmpfile()'s unlink().  If so, the
		 * file mode protects confidentiality of the data written.
		 */
		old_umask = umask(S_IRWXG | S_IRWXO);

#ifndef WIN32
		tm->tmpFH = tmpfile();
#else

		/*
		 * On WIN32, tmpfile() generates a filename in the root directory,
		 * which requires administrative permissions on certain systems. Loop
		 * until we find a unique file name we can create.
		 */
		while (1)
		{
			char	   *name;
			int			fd;

			name = _tempnam(NULL, "pg_temp_");
			if (name == NULL)
				break;
			fd = open(name, O_RDWR | O_CREAT | O_EXCL | O_BINARY |
					  O_TEMPORARY, S_IRUSR | S_IWUSR);
			free(name);

			if (fd != -1)		/* created a file */
			{
				tm->tmpFH = fdopen(fd, "w+b");
				break;
			}
			else if (errno != EEXIST)	/* failure other than file exists */
				break;
		}
#endif

		if (tm->tmpFH == NULL)
			exit_horribly(modulename, "could not generate temporary file name: %s\n", strerror(errno));

		umask(old_umask);

#ifdef HAVE_LIBZ

		if (AH->compression != 0)
		{
			sprintf(fmode, "wb%d", AH->compression);
			tm->zFH = gzdopen(dup(fileno(tm->tmpFH)), fmode);
			if (tm->zFH == NULL)
				exit_horribly(modulename, "could not open temporary file\n");
		}
		else
			tm->nFH = tm->tmpFH;
#else

		tm->nFH = tm->tmpFH;
#endif

		tm->AH = AH;
		tm->targetFile = pg_strdup(filename);
	}

	tm->mode = mode;
	tm->tarFH = ctx->tarFH;

	return tm;
}

static void
tarClose(ArchiveHandle *AH, TAR_MEMBER *th)
{
	/*
	 * Close the GZ file since we dup'd. This will flush the buffers.
	 */
	if (AH->compression != 0)
		if (GZCLOSE(th->zFH) != 0)
			exit_horribly(modulename, "could not close tar member\n");

	if (th->mode == 'w')
		_tarAddFile(AH, th);	/* This will close the temp file */

	/*
	 * else Nothing to do for normal read since we don't dup() normal file
	 * handle, and we don't use temp files.
	 */

	if (th->targetFile)
		free(th->targetFile);

	th->nFH = NULL;
	th->zFH = NULL;
}

#ifdef __NOT_USED__
static char *
tarGets(char *buf, size_t len, TAR_MEMBER *th)
{
	char	   *s;
	size_t		cnt = 0;
	char		c = ' ';
	int			eof = 0;

	/* Can't read past logical EOF */
	if (len > (th->fileLen - th->pos))
		len = th->fileLen - th->pos;

	while (cnt < len && c != '\n')
	{
		if (_tarReadRaw(th->AH, &c, 1, th, NULL) <= 0)
		{
			eof = 1;
			break;
		}
		buf[cnt++] = c;
	}

	if (eof && cnt == 0)
		s = NULL;
	else
	{
		buf[cnt++] = '\0';
		s = buf;
	}

	if (s)
	{
		len = strlen(s);
		th->pos += len;
	}

	return s;
}
#endif

/*
 * Just read bytes from the archive. This is the low level read routine
 * that is used for ALL reads on a tar file.
 */
static size_t
_tarReadRaw(ArchiveHandle *AH, void *buf, size_t len, TAR_MEMBER *th, FILE *fh)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	size_t		avail;
	size_t		used = 0;
	size_t		res = 0;

	avail = AH->lookaheadLen - AH->lookaheadPos;
	if (avail > 0)
	{
		/* We have some lookahead bytes to use */
		if (avail >= len)		/* Just use the lookahead buffer */
			used = len;
		else
			used = avail;

		/* Copy, and adjust buffer pos */
		memcpy(buf, AH->lookahead + AH->lookaheadPos, used);
		AH->lookaheadPos += used;

		/* Adjust required length */
		len -= used;
	}

	/* Read the file if len > 0 */
	if (len > 0)
	{
		if (fh)
		{
			res = fread(&((char *) buf)[used], 1, len, fh);
			if (res != len && !feof(fh))
				READ_ERROR_EXIT(fh);
		}
		else if (th)
		{
			if (th->zFH)
			{
				res = GZREAD(&((char *) buf)[used], 1, len, th->zFH);
				if (res != len && !GZEOF(th->zFH))
				{
#ifdef HAVE_LIBZ
					int			errnum;
					const char *errmsg = gzerror(th->zFH, &errnum);

					exit_horribly(modulename,
								  "could not read from input file: %s\n",
								  errnum == Z_ERRNO ? strerror(errno) : errmsg);
#else
					exit_horribly(modulename,
								  "could not read from input file: %s\n",
								  strerror(errno));
#endif
				}
			}
			else
			{
				res = fread(&((char *) buf)[used], 1, len, th->nFH);
				if (res != len && !feof(th->nFH))
					READ_ERROR_EXIT(th->nFH);
			}
		}
		else
			exit_horribly(modulename, "internal error -- neither th nor fh specified in tarReadRaw()\n");
	}

	ctx->tarFHpos += res + used;

	return (res + used);
}

static size_t
tarRead(void *buf, size_t len, TAR_MEMBER *th)
{
	size_t		res;

	if (th->pos + len > th->fileLen)
		len = th->fileLen - th->pos;

	if (len <= 0)
		return 0;

	res = _tarReadRaw(th->AH, buf, len, th, NULL);

	th->pos += res;

	return res;
}

static size_t
tarWrite(const void *buf, size_t len, TAR_MEMBER *th)
{
	size_t		res;

	if (th->zFH != NULL)
		res = GZWRITE(buf, 1, len, th->zFH);
	else
		res = fwrite(buf, 1, len, th->nFH);

	th->pos += res;
	return res;
}

static void
_WriteData(ArchiveHandle *AH, const void *data, size_t dLen)
{
	lclTocEntry *tctx = (lclTocEntry *) AH->currToc->formatData;

	if (tarWrite(data, dLen, tctx->TH) != dLen)
		WRITE_ERROR_EXIT;

	return;
}

static void
_EndData(ArchiveHandle *AH, TocEntry *te)
{
	lclTocEntry *tctx = (lclTocEntry *) te->formatData;

	/* Close the file */
	tarClose(AH, tctx->TH);
	tctx->TH = NULL;
}

/*
 * Print data for a given file
 */
static void
_PrintFileData(ArchiveHandle *AH, char *filename)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	char		buf[4096];
	size_t		cnt;
	TAR_MEMBER *th;

	if (!filename)
		return;

	th = tarOpen(AH, filename, 'r');
	ctx->FH = th;

	while ((cnt = tarRead(buf, 4095, th)) > 0)
	{
		buf[cnt] = '\0';
		ahwrite(buf, 1, cnt, AH);
	}

	tarClose(AH, th);
}


/*
 * Print data for a given TOC entry
*/
static void
_PrintTocData(ArchiveHandle *AH, TocEntry *te)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	lclTocEntry *tctx = (lclTocEntry *) te->formatData;
	int			pos1;

	if (!tctx->filename)
		return;

	/*
	 * If we're writing the special restore.sql script, emit a suitable
	 * command to include each table's data from the corresponding file.
	 *
	 * In the COPY case this is a bit klugy because the regular COPY command
	 * was already printed before we get control.
	 */
	if (ctx->isSpecialScript)
	{
		if (te->copyStmt)
		{
			/* Abort the COPY FROM stdin */
			ahprintf(AH, "\\.\n");

			/*
			 * The COPY statement should look like "COPY ... FROM stdin;\n",
			 * see dumpTableData().
			 */
			pos1 = (int) strlen(te->copyStmt) - 13;
			if (pos1 < 6 || strncmp(te->copyStmt, "COPY ", 5) != 0 ||
				strcmp(te->copyStmt + pos1, " FROM stdin;\n") != 0)
				exit_horribly(modulename,
							  "unexpected COPY statement syntax: \"%s\"\n",
							  te->copyStmt);

			/* Emit all but the FROM part ... */
			ahwrite(te->copyStmt, 1, pos1, AH);
			/* ... and insert modified FROM */
			ahprintf(AH, " FROM '$$PATH$$/%s';\n\n", tctx->filename);
		}
		else
		{
			/* --inserts mode, no worries, just include the data file */
			ahprintf(AH, "\\i $$PATH$$/%s\n\n", tctx->filename);
		}

		return;
	}

	if (strcmp(te->desc, "BLOBS") == 0)
		_LoadBlobs(AH);
	else
		_PrintFileData(AH, tctx->filename);
}

static void
_LoadBlobs(ArchiveHandle *AH)
{
	Oid			oid;
	lclContext *ctx = (lclContext *) AH->formatData;
	TAR_MEMBER *th;
	size_t		cnt;
	bool		foundBlob = false;
	char		buf[4096];

	StartRestoreBlobs(AH);

	th = tarOpen(AH, NULL, 'r');	/* Open next file */
	while (th != NULL)
	{
		ctx->FH = th;

		if (strncmp(th->targetFile, "blob_", 5) == 0)
		{
			oid = atooid(&th->targetFile[5]);
			if (oid != 0)
			{
				ahlog(AH, 1, "restoring large object with OID %u\n", oid);

				StartRestoreBlob(AH, oid, AH->public.ropt->dropSchema);

				while ((cnt = tarRead(buf, 4095, th)) > 0)
				{
					buf[cnt] = '\0';
					ahwrite(buf, 1, cnt, AH);
				}
				EndRestoreBlob(AH, oid);
				foundBlob = true;
			}
			tarClose(AH, th);
		}
		else
		{
			tarClose(AH, th);

			/*
			 * Once we have found the first blob, stop at the first non-blob
			 * entry (which will be 'blobs.toc').  This coding would eat all
			 * the rest of the archive if there are no blobs ... but this
			 * function shouldn't be called at all in that case.
			 */
			if (foundBlob)
				break;
		}

		th = tarOpen(AH, NULL, 'r');
	}
	EndRestoreBlobs(AH);
}


static int
_WriteByte(ArchiveHandle *AH, const int i)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	char		b = i;			/* Avoid endian problems */

	if (tarWrite(&b, 1, ctx->FH) != 1)
		WRITE_ERROR_EXIT;

	ctx->filePos += 1;
	return 1;
}

static int
_ReadByte(ArchiveHandle *AH)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	size_t		res;
	unsigned char c;

	res = tarRead(&c, 1, ctx->FH);
	if (res != 1)
		/* We already would have exited for errors on reads, must be EOF */
		exit_horribly(modulename,
					  "could not read from input file: end of file\n");
	ctx->filePos += 1;
	return c;
}

static void
_WriteBuf(ArchiveHandle *AH, const void *buf, size_t len)
{
	lclContext *ctx = (lclContext *) AH->formatData;

	if (tarWrite(buf, len, ctx->FH) != len)
		WRITE_ERROR_EXIT;

	ctx->filePos += len;
}

static void
_ReadBuf(ArchiveHandle *AH, void *buf, size_t len)
{
	lclContext *ctx = (lclContext *) AH->formatData;

	if (tarRead(buf, len, ctx->FH) != len)
		/* We already would have exited for errors on reads, must be EOF */
		exit_horribly(modulename,
					  "could not read from input file: end of file\n");

	ctx->filePos += len;
	return;
}

static void
_CloseArchive(ArchiveHandle *AH)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	TAR_MEMBER *th;
	RestoreOptions *ropt;
	RestoreOptions *savRopt;
	DumpOptions *savDopt;
	int			savVerbose,
				i;

	if (AH->mode == archModeWrite)
	{
		/*
		 * Write the Header & TOC to the archive FIRST
		 */
		th = tarOpen(AH, "toc.dat", 'w');
		ctx->FH = th;
		WriteHead(AH);
		WriteToc(AH);
		tarClose(AH, th);		/* Not needed any more */

		/*
		 * Now send the data (tables & blobs)
		 */
		WriteDataChunks(AH, NULL);

		/*
		 * Now this format wants to append a script which does a full restore
		 * if the files have been extracted.
		 */
		th = tarOpen(AH, "restore.sql", 'w');

		(void) tarPrintf(AH, th, "--\n"
						 "-- NOTE:\n"
						 "--\n"
						 "-- File paths need to be edited. Search for $$PATH$$ and\n"
						 "-- replace it with the path to the directory containing\n"
						 "-- the extracted data files.\n"
						 "--\n");

		AH->CustomOutPtr = _scriptOut;

		ctx->isSpecialScript = 1;
		ctx->scriptTH = th;

		ropt = NewRestoreOptions();
		memcpy(ropt, AH->public.ropt, sizeof(RestoreOptions));
		ropt->filename = NULL;
		ropt->dropSchema = 1;
		ropt->compression = 0;
		ropt->superuser = NULL;
		ropt->suppressDumpWarnings = true;

		savDopt = AH->public.dopt;
		savRopt = AH->public.ropt;

		SetArchiveOptions((Archive *) AH, NULL, ropt);

		savVerbose = AH->public.verbose;
		AH->public.verbose = 0;

		RestoreArchive((Archive *) AH);

		SetArchiveOptions((Archive *) AH, savDopt, savRopt);

		AH->public.verbose = savVerbose;

		tarClose(AH, th);

		ctx->isSpecialScript = 0;

		/*
		 * EOF marker for tar files is two blocks of NULLs.
		 */
		for (i = 0; i < 512 * 2; i++)
		{
			if (fputc(0, ctx->tarFH) == EOF)
				WRITE_ERROR_EXIT;
		}

		/* Sync the output file if one is defined */
		if (AH->dosync && AH->fSpec)
			(void) fsync_fname(AH->fSpec, false, progname);
	}

	AH->FH = NULL;
}

static size_t
_scriptOut(ArchiveHandle *AH, const void *buf, size_t len)
{
	lclContext *ctx = (lclContext *) AH->formatData;

	return tarWrite(buf, len, ctx->scriptTH);
}

/*
 * BLOB support
 */

/*
 * Called by the archiver when starting to save all BLOB DATA (not schema).
 * This routine should save whatever format-specific information is needed
 * to read the BLOBs back into memory.
 *
 * It is called just prior to the dumper's DataDumper routine.
 *
 * Optional, but strongly recommended.
 *
 */
static void
_StartBlobs(ArchiveHandle *AH, TocEntry *te)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	char		fname[K_STD_BUF_SIZE];

	sprintf(fname, "blobs.toc");
	ctx->blobToc = tarOpen(AH, fname, 'w');
}

/*
 * Called by the archiver when the dumper calls StartBlob.
 *
 * Mandatory.
 *
 * Must save the passed OID for retrieval at restore-time.
 */
static void
_StartBlob(ArchiveHandle *AH, TocEntry *te, Oid oid)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	lclTocEntry *tctx = (lclTocEntry *) te->formatData;
	char		fname[255];
	char	   *sfx;

	if (oid == 0)
		exit_horribly(modulename, "invalid OID for large object (%u)\n", oid);

	if (AH->compression != 0)
		sfx = ".gz";
	else
		sfx = "";

	sprintf(fname, "blob_%u.dat%s", oid, sfx);

	tarPrintf(AH, ctx->blobToc, "%u %s\n", oid, fname);

	tctx->TH = tarOpen(AH, fname, 'w');
}

/*
 * Called by the archiver when the dumper calls EndBlob.
 *
 * Optional.
 *
 */
static void
_EndBlob(ArchiveHandle *AH, TocEntry *te, Oid oid)
{
	lclTocEntry *tctx = (lclTocEntry *) te->formatData;

	tarClose(AH, tctx->TH);
}

/*
 * Called by the archiver when finishing saving all BLOB DATA.
 *
 * Optional.
 *
 */
static void
_EndBlobs(ArchiveHandle *AH, TocEntry *te)
{
	lclContext *ctx = (lclContext *) AH->formatData;

	/* Write out a fake zero OID to mark end-of-blobs. */
	/* WriteInt(AH, 0); */

	tarClose(AH, ctx->blobToc);
}



/*------------
 * TAR Support
 *------------
 */

static int
tarPrintf(ArchiveHandle *AH, TAR_MEMBER *th, const char *fmt,...)
{
	char	   *p;
	size_t		len = 128;		/* initial assumption about buffer size */
	size_t		cnt;

	for (;;)
	{
		va_list		args;

		/* Allocate work buffer. */
		p = (char *) pg_malloc(len);

		/* Try to format the data. */
		va_start(args, fmt);
		cnt = pvsnprintf(p, len, fmt, args);
		va_end(args);

		if (cnt < len)
			break;				/* success */

		/* Release buffer and loop around to try again with larger len. */
		free(p);
		len = cnt;
	}

	cnt = tarWrite(p, cnt, th);
	free(p);
	return (int) cnt;
}

bool
isValidTarHeader(char *header)
{
	int			sum;
	int			chk = tarChecksum(header);

	sum = read_tar_number(&header[148], 8);

	if (sum != chk)
		return false;

	/* POSIX tar format */
	if (memcmp(&header[257], "ustar\0", 6) == 0 &&
		memcmp(&header[263], "00", 2) == 0)
		return true;
	/* GNU tar format */
	if (memcmp(&header[257], "ustar  \0", 8) == 0)
		return true;
	/* not-quite-POSIX format written by pre-9.3 pg_dump */
	if (memcmp(&header[257], "ustar00\0", 8) == 0)
		return true;

	return false;
}

/* Given the member, write the TAR header & copy the file */
static void
_tarAddFile(ArchiveHandle *AH, TAR_MEMBER *th)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	FILE	   *tmp = th->tmpFH;	/* Grab it for convenience */
	char		buf[32768];
	size_t		cnt;
	pgoff_t		len = 0;
	size_t		res;
	size_t		i,
				pad;

	/*
	 * Find file len & go back to start.
	 */
	if (fseeko(tmp, 0, SEEK_END) != 0)
		exit_horribly(modulename, "error during file seek: %s\n",
					  strerror(errno));
	th->fileLen = ftello(tmp);
	if (th->fileLen < 0)
		exit_horribly(modulename, "could not determine seek position in archive file: %s\n",
					  strerror(errno));
	if (fseeko(tmp, 0, SEEK_SET) != 0)
		exit_horribly(modulename, "error during file seek: %s\n",
					  strerror(errno));

	_tarWriteHeader(th);

	while ((cnt = fread(buf, 1, sizeof(buf), tmp)) > 0)
	{
		if ((res = fwrite(buf, 1, cnt, th->tarFH)) != cnt)
			WRITE_ERROR_EXIT;
		len += res;
	}
	if (!feof(tmp))
		READ_ERROR_EXIT(tmp);

	if (fclose(tmp) != 0)		/* This *should* delete it... */
		exit_horribly(modulename, "could not close temporary file: %s\n",
					  strerror(errno));

	if (len != th->fileLen)
	{
		char		buf1[32],
					buf2[32];

		snprintf(buf1, sizeof(buf1), INT64_FORMAT, (int64) len);
		snprintf(buf2, sizeof(buf2), INT64_FORMAT, (int64) th->fileLen);
		exit_horribly(modulename, "actual file length (%s) does not match expected (%s)\n",
					  buf1, buf2);
	}

	pad = ((len + 511) & ~511) - len;
	for (i = 0; i < pad; i++)
	{
		if (fputc('\0', th->tarFH) == EOF)
			WRITE_ERROR_EXIT;
	}

	ctx->tarFHpos += len + pad;
}

/* Locate the file in the archive, read header and position to data */
static TAR_MEMBER *
_tarPositionTo(ArchiveHandle *AH, const char *filename)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	TAR_MEMBER *th = pg_malloc0(sizeof(TAR_MEMBER));
	char		c;
	char		header[512];
	size_t		i,
				len,
				blks;
	int			id;

	th->AH = AH;

	/* Go to end of current file, if any */
	if (ctx->tarFHpos != 0)
	{
		char		buf1[100],
					buf2[100];

		snprintf(buf1, sizeof(buf1), INT64_FORMAT, (int64) ctx->tarFHpos);
		snprintf(buf2, sizeof(buf2), INT64_FORMAT, (int64) ctx->tarNextMember);
		ahlog(AH, 4, "moving from position %s to next member at file position %s\n",
			  buf1, buf2);

		while (ctx->tarFHpos < ctx->tarNextMember)
			_tarReadRaw(AH, &c, 1, NULL, ctx->tarFH);
	}

	{
		char		buf[100];

		snprintf(buf, sizeof(buf), INT64_FORMAT, (int64) ctx->tarFHpos);
		ahlog(AH, 4, "now at file position %s\n", buf);
	}

	/* We are at the start of the file, or at the next member */

	/* Get the header */
	if (!_tarGetHeader(AH, th))
	{
		if (filename)
			exit_horribly(modulename, "could not find header for file \"%s\" in tar archive\n", filename);
		else
		{
			/*
			 * We're just scanning the archive for the next file, so return
			 * null
			 */
			free(th);
			return NULL;
		}
	}

	while (filename != NULL && strcmp(th->targetFile, filename) != 0)
	{
		ahlog(AH, 4, "skipping tar member %s\n", th->targetFile);

		id = atoi(th->targetFile);
		if ((TocIDRequired(AH, id) & REQ_DATA) != 0)
			exit_horribly(modulename, "restoring data out of order is not supported in this archive format: "
						  "\"%s\" is required, but comes before \"%s\" in the archive file.\n",
						  th->targetFile, filename);

		/* Header doesn't match, so read to next header */
		len = ((th->fileLen + 511) & ~511); /* Padded length */
		blks = len >> 9;		/* # of 512 byte blocks */

		for (i = 0; i < blks; i++)
			_tarReadRaw(AH, &header[0], 512, NULL, ctx->tarFH);

		if (!_tarGetHeader(AH, th))
			exit_horribly(modulename, "could not find header for file \"%s\" in tar archive\n", filename);
	}

	ctx->tarNextMember = ctx->tarFHpos + ((th->fileLen + 511) & ~511);
	th->pos = 0;

	return th;
}

/* Read & verify a header */
static int
_tarGetHeader(ArchiveHandle *AH, TAR_MEMBER *th)
{
	lclContext *ctx = (lclContext *) AH->formatData;
	char		h[512];
	char		tag[100 + 1];
	int			sum = 0,
				chk = 0;
	pgoff_t		len;
	pgoff_t		hPos;
	bool		gotBlock = false;

	while (!gotBlock)
	{
		/* Save the pos for reporting purposes */
		hPos = ctx->tarFHpos;

		/* Read a 512 byte block, return EOF, exit if short */
		len = _tarReadRaw(AH, h, 512, NULL, ctx->tarFH);
		if (len == 0)			/* EOF */
			return 0;

		if (len != 512)
			exit_horribly(modulename,
						  ngettext("incomplete tar header found (%lu byte)\n",
								   "incomplete tar header found (%lu bytes)\n",
								   len),
						  (unsigned long) len);

		/* Calc checksum */
		chk = tarChecksum(h);
		sum = read_tar_number(&h[148], 8);

		/*
		 * If the checksum failed, see if it is a null block. If so, silently
		 * continue to the next block.
		 */
		if (chk == sum)
			gotBlock = true;
		else
		{
			int			i;

			for (i = 0; i < 512; i++)
			{
				if (h[i] != 0)
				{
					gotBlock = true;
					break;
				}
			}
		}
	}

	/* Name field is 100 bytes, might not be null-terminated */
	strlcpy(tag, &h[0], 100 + 1);

	len = read_tar_number(&h[124], 12);

	{
		char		posbuf[32];
		char		lenbuf[32];

		snprintf(posbuf, sizeof(posbuf), UINT64_FORMAT, (uint64) hPos);
		snprintf(lenbuf, sizeof(lenbuf), UINT64_FORMAT, (uint64) len);
		ahlog(AH, 3, "TOC Entry %s at %s (length %s, checksum %d)\n",
			  tag, posbuf, lenbuf, sum);
	}

	if (chk != sum)
	{
		char		posbuf[32];

		snprintf(posbuf, sizeof(posbuf), UINT64_FORMAT,
				 (uint64) ftello(ctx->tarFH));
		exit_horribly(modulename,
					  "corrupt tar header found in %s "
					  "(expected %d, computed %d) file position %s\n",
					  tag, sum, chk, posbuf);
	}

	th->targetFile = pg_strdup(tag);
	th->fileLen = len;

	return 1;
}


static void
_tarWriteHeader(TAR_MEMBER *th)
{
	char		h[512];

	tarCreateHeader(h, th->targetFile, NULL, th->fileLen,
					0600, 04000, 02000, time(NULL));

	/* Now write the completed header. */
	if (fwrite(h, 1, 512, th->tarFH) != 512)
		WRITE_ERROR_EXIT;
}
