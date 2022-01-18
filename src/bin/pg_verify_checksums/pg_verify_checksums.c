/*
 * pg_verify_checksums
 *
 * Verifies page level checksums in an offline cluster
 *
 *	Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *	src/bin/pg_verify_checksums/pg_verify_checksums.c
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/relpath.h"
#include "getopt_long.h"
#include "pg_getopt.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/fd.h"


static int64 files = 0;
static int64 blocks = 0;
static int64 badblocks = 0;
static ControlFileData *ControlFile;

static char *only_relfilenode = NULL;
static bool verbose = false;

static const char *progname;

static void
usage(void)
{
	printf(_("%s verifies data checksums in a PostgreSQL database cluster.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DATADIR]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -r RELFILENODE         check only relation with specified relfilenode\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}

/*
 * Definition of one element part of an exclusion list, used for files
 * to exclude from checksum validation.  "name" is the name of the file
 * or path to check for exclusion.  If "match_prefix" is true, any items
 * matching the name as prefix are excluded.
 */
struct exclude_list_item
{
	const char *name;
	bool		match_prefix;
};

/*
 * List of files excluded from checksum validation.
 *
 * Note: this list should be kept in sync with what basebackup.c includes.
 */
static const struct exclude_list_item skip[] = {
	{"pg_control", false},
	{"pg_filenode.map", false},
	{"pg_internal.init", true},
	{"PG_VERSION", false},
#ifdef EXEC_BACKEND
	{"config_exec_params", true},
#endif

	/* POLAR: Do not checksum for sqlprotect's files */
	{"polar_sqlprotect.stat", false},
	{"polar_sqlprotect.query", false},
	{"polar_sqlprotect.qtxt", false},
	{"polar_sql_protect_roles", false},
	{"polar_sql_protect_rels", false},
	{"polar_sql_protect_stats", false},
	/* POLAR end */

	{NULL, false}
};

static bool
skipfile(const char *fn)
{
	int		excludeIdx;

	for (excludeIdx = 0; skip[excludeIdx].name != NULL; excludeIdx++)
	{
		int		cmplen = strlen(skip[excludeIdx].name);

		if (!skip[excludeIdx].match_prefix)
			cmplen++;
		if (strncmp(skip[excludeIdx].name, fn, cmplen) == 0)
			return true;
	}

	return false;
}

static void
scan_file(const char *fn, BlockNumber segmentno)
{
	PGAlignedBlock buf;
	PageHeader	header = (PageHeader) buf.data;
	int			f;
	BlockNumber blockno;

	f = open(fn, O_RDONLY | PG_BINARY);
	if (f < 0)
	{
		fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		exit(1);
	}

	files++;

	for (blockno = 0;; blockno++)
	{
		uint16		csum;
		int			r = read(f, buf.data, BLCKSZ);

		if (r == 0)
			break;
		if (r != BLCKSZ)
		{
			fprintf(stderr, _("%s: could not read block %u in file \"%s\": read %d of %d\n"),
					progname, blockno, fn, r, BLCKSZ);
			exit(1);
		}
		blocks++;

		/* New pages have no checksum yet */
		if (PageIsNew(header))
			continue;

		csum = pg_checksum_page(buf.data, blockno + segmentno * RELSEG_SIZE);
		if (csum != header->pd_checksum)
		{
			if (ControlFile->data_checksum_version == PG_DATA_CHECKSUM_VERSION)
				fprintf(stderr, _("%s: checksum verification failed in file \"%s\", block %u: calculated checksum %X but block contains %X\n"),
						progname, fn, blockno, csum, header->pd_checksum);
			badblocks++;
		}
	}

	if (verbose)
		fprintf(stderr,
				_("%s: checksums verified in file \"%s\"\n"), progname, fn);

	close(f);
}

static void
scan_directory(const char *basedir, const char *subdir)
{
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
	dir = opendir(path);
	if (!dir)
	{
		fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"),
				progname, path, strerror(errno));
		exit(1);
	}
	while ((de = readdir(dir)) != NULL)
	{
		char		fn[MAXPGPATH];
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		/* Skip temporary files */
		if (strncmp(de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			continue;

		/* Skip temporary folders */
		if (strncmp(de->d_name,
					PG_TEMP_FILES_DIR,
					strlen(PG_TEMP_FILES_DIR)) == 0)
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", path, de->d_name);
		if (lstat(fn, &st) < 0)
		{
			fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
					progname, fn, strerror(errno));
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char		fnonly[MAXPGPATH];
			char	   *forkpath,
					   *segmentpath;
			BlockNumber segmentno = 0;

			if (skipfile(de->d_name))
				continue;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the relfilenode the file belongs to for
			 * filtering.
			 */
			strlcpy(fnonly, de->d_name, sizeof(fnonly));
			segmentpath = strchr(fnonly, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
				{
					fprintf(stderr, _("%s: invalid segment number %d in file name \"%s\"\n"),
							progname, segmentno, fn);
					exit(1);
				}
			}

			forkpath = strchr(fnonly, '_');
			if (forkpath != NULL)
				*forkpath++ = '\0';

			if (only_relfilenode && strcmp(only_relfilenode, fnonly) != 0)
				/* Relfilenode not to be included */
				continue;

			scan_file(fn, segmentno);
		}
#ifndef WIN32
		else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode))
#else
		else if (S_ISDIR(st.st_mode) || pgwin32_is_junction(fn))
#endif
		{
			/*
			 * If going through the entries of pg_tblspc, we assume to operate
			 * on tablespace locations where only TABLESPACE_VERSION_DIRECTORY
			 * is valid, resolving the linked locations and dive into them
			 * directly.
			 */
			if (strncmp("pg_tblspc", subdir, strlen("pg_tblspc")) == 0)
			{
				char		tblspc_path[MAXPGPATH];
				struct stat tblspc_st;

				/*
				 * Resolve tablespace location path and check whether
				 * TABLESPACE_VERSION_DIRECTORY exists.  Not finding a valid
				 * location is unexpected, since there should be no orphaned
				 * links and no links pointing to something else than a
				 * directory.
				 */
				snprintf(tblspc_path, sizeof(tblspc_path), "%s/%s/%s",
						 path, de->d_name, TABLESPACE_VERSION_DIRECTORY);

				if (lstat(tblspc_path, &tblspc_st) < 0)
				{
					fprintf(stderr, _("%s: could not stat file \"%s\": %s\n"),
							progname, tblspc_path, strerror(errno));
					exit(1);
				}

				/*
				 * Move backwards once as the scan needs to happen for the
				 * contents of TABLESPACE_VERSION_DIRECTORY.
				 */
				snprintf(tblspc_path, sizeof(tblspc_path), "%s/%s",
						 path, de->d_name);

				/* Looks like a valid tablespace location */
				scan_directory(tblspc_path,
							   TABLESPACE_VERSION_DIRECTORY);
			}
			else
			{
				scan_directory(path, de->d_name);
			}
		}
	}
	closedir(dir);
}

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	char	   *DataDir = NULL;
	int			c;
	int			option_index;
	bool		crc_ok;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_verify_checksums"));

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_verify_checksums (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:r:v", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'v':
				verbose = true;
				break;
			case 'D':
				DataDir = optarg;
				break;
			case 'r':
				if (atoi(optarg) == 0)
				{
					fprintf(stderr, _("%s: invalid relfilenode specification, must be numeric: %s\n"), progname, optarg);
					exit(1);
				}
				only_relfilenode = pstrdup(optarg);
				break;
			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (DataDir == NULL)
	{
		if (optind < argc)
			DataDir = argv[optind++];
		else
			DataDir = getenv("PGDATA");

		/* If no DataDir was specified, and none could be found, error out */
		if (DataDir == NULL)
		{
			fprintf(stderr, _("%s: no data directory specified\n"), progname);
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
			exit(1);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/* Check if cluster is running */
	ControlFile = get_controlfile(DataDir, progname, &crc_ok);
	if (!crc_ok)
	{
		fprintf(stderr, _("%s: pg_control CRC value is incorrect\n"), progname);
		exit(1);
	}

	if (ControlFile->pg_control_version != PG_CONTROL_VERSION)
	{
		fprintf(stderr, _("%s: cluster is not compatible with this version of pg_verify_checksums\n"),
				progname);
		exit(1);
	}

	if (ControlFile->blcksz != BLCKSZ)
	{
		fprintf(stderr, _("%s: database cluster is not compatible\n"),
				progname);
		fprintf(stderr, _("The database cluster was initialized with block size %u, but pg_verify_checksums was compiled with block size %u.\n"),
				ControlFile->blcksz, BLCKSZ);
		exit(1);
	}

	if (ControlFile->state != DB_SHUTDOWNED &&
		ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY)
	{
		fprintf(stderr, _("%s: cluster must be shut down to verify checksums\n"), progname);
		exit(1);
	}

	if (ControlFile->data_checksum_version == 0)
	{
		fprintf(stderr, _("%s: data checksums are not enabled in cluster\n"), progname);
		exit(1);
	}

	/* Scan all files */
	scan_directory(DataDir, "global");
	scan_directory(DataDir, "base");
	scan_directory(DataDir, "pg_tblspc");

	printf(_("Checksum scan completed\n"));
	printf(_("Data checksum version: %d\n"), ControlFile->data_checksum_version);
	printf(_("Files scanned:  %s\n"), psprintf(INT64_FORMAT, files));
	printf(_("Blocks scanned: %s\n"), psprintf(INT64_FORMAT, blocks));
	printf(_("Bad checksums:  %s\n"), psprintf(INT64_FORMAT, badblocks));

	if (badblocks > 0)
		return 1;

	return 0;
}
