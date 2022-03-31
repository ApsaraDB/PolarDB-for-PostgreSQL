/*
 * ------------------------------------------------------------------------
 *
 * pgxc_clean utility
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 2011-2012 Postgres-XC Development Group
 * 
 *	Recovers outstanding 2PC when after crashed nodes or entire cluster
 *  is recovered.
 *
 *  Depending upon how nodes/XC cluster fail, there could be outstanding
 *  2PC transactions which are partly prepared and partly commited/borted.
 *  Such transactions must be commited/aborted to remove them from the
 *  snapshot.
 *
 *  This utility checks if there's such outstanding transactions and
 *  cleans them up.
 *
 * Command syntax
 *
 * pgxc_clean [option ... ] [database] [user]
 *
 * Options are:
 *
 *  -a, --all				cleanup all the database avilable
 *  -d, --dbname=DBNAME		database name to clean up.   Multiple -d option
 *                          can be specified.
 *  -h, --host=HOSTNAME		Coordinator hostname to connect to.
 *  -N, --no-clean			only test.  no cleanup actually.
 *  -o, --output=FILENAME	output file name.
 *  -p, --port=PORT			Coordinator port number.
 *  -q, --quiet				do not print messages except for error, default.
 *  -s, --status			prints out 2PC status.
 *  -U, --username=USERNAME	database user name
 *  -v, --verbose			same as -s, plus prints result of each cleanup.
 *  -V, --version			prints out the version,
 *  -w, --no-password		never prompt for the password.
 *  -W, --password			prompt for the password,
 *  -?, --help				prints help message
 *
 * ------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <pwd.h>
#include <errno.h>
#include <string.h>
#include "pg_config.h"
#include "libpq-fe.h"
#include "getopt_long.h"
#include "pgxc_clean.h"
#include "txninfo.h"
#include "port.h"
#include "datatype/timestamp.h"
#include "postgres.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include <sys/time.h>
#include <stdint.h>

#define TXN_PREPARED 	0x0001
#define TXN_COMMITTED 	0x0002
#define TXN_ABORTED		0x0004
#define TXN_INPROGRESS	0x0008

/* Who I am */
const char *progname;
char *my_nodename;
int   my_nodeidx = -1;		/* Index in pgxc_clean_node_info */

/* Databases to clean */
bool clean_all_databases = false;		/* "--all" overrides specific database specification */

database_names *head_database_names = NULL;
database_names *last_database_names = NULL;

/* Coordinator to connect to */
char *coordinator_host = NULL;
int coordinator_port = -1;

int min_clean_xact_interval = 120; // min interval to clean 2pc xact (unit: second)
int min_clean_file_interval = 300; // min interval to clean 2pc file (unit: second)

typedef enum passwd_opt
{
	TRI_DEFAULT,
	TRI_YES,
	TRI_NO
} passwd_opt;

/* Miscellaneous */
char *output_filename = NULL;
char *username = NULL;
bool version_opt = false;
passwd_opt try_password_opt = TRI_DEFAULT;
bool status_opt = false;
bool no_clean_opt = false;
bool verbose_opt = false;
bool print_prepared_xact_only = false;
bool force_clean_2pc = false;
bool simple_clean_2pc = true; /* default: no HLC feature enabled. */
int  handle_2pc_mode = ROLLBACK_ALL_PREPARED_TXN;
FILE *outf;
FILE *errf;

/* Global variables */
node_info	*pgxc_clean_node_info;
int			pgxc_clean_node_count;

database_info *head_database_info;
database_info *last_database_info;

static bool have_password = false;
static char password[100];
static char password_prompt[256];

int total_prep_txn_count = 0; // total prepared xact num in cluster.
const char* twophase_file_not_found_str = "no coresponding 2pc file found";

/* Funcs */
static char *GetUserName(void);
static PGconn *loginDatabase(char *host, int port, char *user, char *password,
							char *dbname, const char *progname, char *encoding, char *password_prompt);
static void getMyNodename(PGconn *conn);
static void recover2PCForDatabase(database_info *db_info);
static void recover2PC(PGconn *conn, txn_info *txn, char *database_name);
static void getDatabaseList(PGconn *conn);
static void getNodeList(PGconn *conn);
static void showVersion(void);
static void add_to_database_list(char *dbname);
static void parse_pgxc_clean_options(int argc, char *argv[]);
static void usage(void);
static void getPreparedTxnList(PGconn *conn);
static void getTxnInfoOnOtherNodesAll(PGconn *conn);
static void do_commit(PGconn *conn, txn_info *txn);
static void do_abort(PGconn *conn, txn_info *txn);
static void do_commit_abort(PGconn *conn, txn_info *txn, bool is_commit);
static void getPreparedTxnListOfNode(PGconn *conn, int idx);
static void getPreparedTxnListOfNodeSimpleCleanMode(PGconn *conn, int idx);
static void do_commit_abort_simple_clean_mode(PGconn *conn, txn_info *txn, char *database_name, bool is_commit);

#ifdef POLARDB_X
/* POLARDBX_TRANSACTION list below is return value of polardbx_get_transaction_status */
#define POLARDBX_TRANSACTION_COMMITED 0
#define POLARDBX_TRANSACTION_ABORTED 1
#define POLARDBX_TRANSACTION_INPROGRESS 2
#define POLARDBX_TRANSACTION_TWOPHASE_FILE_NOT_FOUND 3

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE		2440588 /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */
#define SECS_PER_DAY			86400

typedef struct TwoPhaseFileDetail
{
	struct TwoPhaseFileDetail* next;
	int xid;
	int nodeidx;
	int64_t prepared_at;
	int64_t commit_timestamp;
	char *gid;
	char *participate_nodes;
	char *nodename;
	char *filename;
}TwoPhaseFileDetail;

typedef struct TwoPhaseFileInfo
{
	struct TwoPhaseFileInfo* next;
	char *gid; // search key.
	char *participate_node;
	TXN_STATUS *status; /* commit/abort/inprogress  */
	TXN_STATUS global_status;
	bool *is_participate;
	bool *is_fileexist;
	Timestamp prepare_time;
	Timestamp commit_time;
	TwoPhaseFileDetail *detail_head;
	TwoPhaseFileDetail *detail_last;
}TwoPhaseFileInfo;

TwoPhaseFileInfo *head_file_info = NULL;
TwoPhaseFileInfo *last_file_info = NULL;


TimestampTz getCommitTimestamp(PGconn *conn, char* gid, char* node_name);
static void printTwoPhaseFileList(void);
static void cleanObsoleteTwoPhaseFiles(void);
bool parseTwoPhaseFileDetail(char *detail, TwoPhaseFileDetail *twoPhaseFileDetail);
char* getTwoPhaseFileDetail(PGconn *conn, int idx, char* filename);
TwoPhaseFileInfo *find_file_info(char* gid);
TwoPhaseFileInfo* make_file_info(TwoPhaseFileDetail *detail);
static void		  addTwoPhasefileInfo(int node_idx, TwoPhaseFileDetail *detail);
static void getTwoPhaseFileOfNode(PGconn *conn, int idx);
static void printSingleTwoPhase(TwoPhaseFileInfo* fileinfo);
static TXN_STATUS getTxnStatusOnNode(PGconn *conn, char* gid, int node_idx, Timestamp* commit_timestamp);
static void getSingleFileStatusOnOtherNodes(PGconn *conn, TwoPhaseFileInfo* fileinfo);
static void getFileStatusOnOtherNodesAll(PGconn *conn);
static TXN_STATUS checkSingleFileGlobalStatus(TwoPhaseFileInfo* fileinfo);
static void checkFileGlobalStatus(PGconn *conn);
static void doDeleteTwoPhaseFile(PGconn *conn, char *gid, int nodeidx);
static void deleteSingleTwoPhaseFile(PGconn *conn, TwoPhaseFileInfo *fileinfo);
static void deleteTwoPhaseFile(PGconn *conn);
static void getTwoPhaseFileListOnDisk(PGconn *coord_conn);
static int getFileCount(char* filelist);
#endif
/*
 * Connection to the Coordinator
 */
PGconn *coord_conn;

/*
 *
 * Main
 *
 */
int main(int argc, char *argv[])
{

	/* Should setup pglocale when it is supported by XC core */

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			showVersion();
			exit(0);
		}
	}
	parse_pgxc_clean_options(argc, argv);

	/*
	 * Check missing arguments
	 */
	if (clean_all_databases == false && head_database_names == NULL)
	{
		fprintf(stderr, "%s: you must specify -a or -d option.\n", progname);
		exit(1);
	}

	/*
	 * Arrange my environment
	 */
	if (output_filename)
	{
		/* Prepare output filename */
		outf = fopen(output_filename, "w");
		if (outf == NULL)
		{
			fprintf(stderr, "%s: Cannot ope output file %s (%s)\n", progname, output_filename, strerror(errno));
			exit(1);
		}
		errf = outf;
	}
	else
	{
		outf = stdout;
		errf = stdout;
	}
	if (coordinator_host == NULL)
	{
		/* Default Coordinator host */
		if ((coordinator_host = getenv("PGHOST")) == NULL)
			coordinator_host = "localhost";
	}
	if (coordinator_port == -1)
	{
		/* Default Coordinator port */
		char *pgport;

		if ((pgport = getenv("PGPORT")) == NULL)
			coordinator_port = DEF_PGPORT;		/* pg_config.h */
		else
			coordinator_port = atoi(pgport);
	}
	if (username == NULL)
		strcpy(password_prompt, "Password: ");
	else
		sprintf(password_prompt, "Password for user %s: ", username);
	if (try_password_opt == TRI_YES)
	{
		simple_prompt(password_prompt, password, sizeof(password), false);
		have_password = true;
	}

	if (verbose_opt)
	{
		/* Print environments */
		fprintf(outf, "%s (%s): Cleanup outstanding 2PCs.\n", progname, PG_VERSION);
		/* Target databaess */
		fprintf(outf, "Target databases:");
		if (clean_all_databases)
			fprintf(outf, "(ALL)\n");
		else
		{
			database_names *cur_name;

			for(cur_name = head_database_names; cur_name; cur_name = cur_name->next)
				fprintf(outf, " %s", cur_name->database_name);
			fprintf(outf, "\n");
		}
		/* Username to use */
		fprintf(outf, "Username: %s\n", username ? username : "default");
		/* Status opt */
		fprintf(outf, "Status opt: %s\n", status_opt ? "on" : "off");
		/* No-dlean opt */
		fprintf(outf, "no-clean: %s\n", no_clean_opt ? "on" : "off");
	}

	/* Tweak options --> should be improved in the next releases */
	if (status_opt)
		verbose_opt = true;
	/* Connect to XC server */
	if (verbose_opt)
	{
		fprintf(outf, "%s: connecting to database \"%s\", host: \"%s\", port: %d\n",
				progname,
				clean_all_databases ? "postgres" : head_database_names->database_name,
				coordinator_host, coordinator_port);
	}
	coord_conn = loginDatabase(coordinator_host, coordinator_port, username, password,
							   clean_all_databases ? "postgres" : head_database_names->database_name,
							   progname, "auto", password_prompt);
	if (verbose_opt)
	{
		fprintf(outf, "%s: connected successfully\n", progname);
	}

	/*
	 * Get my nodename (connected Coordinator)
	 */
	getMyNodename(coord_conn);
	if (verbose_opt)
	{
		fprintf(outf, "%s: Connected to the node \"%s\"\n", progname, my_nodename);
	}

	/*
	 * Get available databases
	 *
	 * pgxc_clean assumes that all the database are available from the connecting Coordinator.
	 * Some (expert) DBA can create a database local to subset of the node by EXECUTE DIRECT.
	 * In this case, DBA may have to clean outstanding 2PC transactions manually or clean
	 * 2PC transactions by connecting pgxc_clean to different Coordinators.
	 *
	 * If such node-subset database is found to be used widely, pgxc_clean may need
	 * an extension to deal with this case.
	 */
	if (clean_all_databases)
		getDatabaseList(coord_conn);
	if (verbose_opt)
	{
		database_info *cur_database;

		fprintf(outf, "%s: Databases visible from the node \"%s\": ", progname, my_nodename);

		if (head_database_info)
		{
			for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
			{
				fprintf(outf, " \"%s\"", cur_database->database_name);
			}
			fputc('\n', outf);
		}
	}

	/*
	 * Get list of Coordinators
	 *
	 * As in the case of database, we clean transactions in visible nodes from the
	 * connecting Coordinator. DBA can also setup different node configuration
	 * at different Coordinators. In this case, DBA should be careful to choose
	 * appropriate Coordinator to clean up transactions.
	 */
	getNodeList(coord_conn);
	if (verbose_opt)
	{
		int ii;

		fprintf(outf, "%s: Node list visible from the node \"%s\"\n", progname, my_nodename);

		for (ii = 0; ii < pgxc_clean_node_count; ii++)
		{
			fprintf(outf, "Name: %s, host: %s, port: %d, type: %s\n",
					pgxc_clean_node_info[ii].node_name,
					pgxc_clean_node_info[ii].host,
					pgxc_clean_node_info[ii].port,
					pgxc_clean_node_info[ii].type == NODE_TYPE_COORD ? "coordinator" : "datanode");
		}
	}

	/*
	 * Get list of prepared statement
	 * NB: will collect all cluster nodes' prepared txn info.
	 */
	getPreparedTxnList(coord_conn);

	/*
	 * Check if there're any 2PC candidate to recover
	 */
	if (!check2PCExists())
	{
	    /* In simple clean mode, no extra twophase file left on disk, and the cleanup functions are undefined in kernel. */
        if (!simple_clean_2pc)
        {
            fprintf(errf, "%s: There's no prepared 2PC in this cluster. Checking 2pc files...\n", progname);
            cleanObsoleteTwoPhaseFiles();
        }
		exit(0);
	}

	if (print_prepared_xact_only)
	{
		fprintf(outf, "%s: Only print prepared xacts.  Exiting.\n", progname);
		exit(0);
	}


	/*
	 * Check status of each prepared transaction.  To do this, look into
	 * nodes where the transaction is not recorded as "prepared".
	 * Possible status are unknown (prepare has not been issued), committed or
	 * aborted.
	 */
	getTxnInfoOnOtherNodesAll(coord_conn);
	if (verbose_opt)
	{
		/* Print all the prepared transaction list */
		database_info *cur_db;

		fprintf(outf, "%s: 2PC transaction list.\n", progname);
		for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
		{
			txn_info *txn;

			fprintf(outf, "Database: \"%s\":\n", cur_db->database_name);

			for (txn = cur_db->head_txn_info; txn; txn = txn->next)
			{
				int ii;

				fprintf(outf, "    gid: %s, owner: %s\n", txn->gid, txn->owner);
				for (ii = 0; ii < pgxc_clean_node_count; ii++)
				{
					fprintf(outf, "        node: %s, status: %s, participate:%d\n",
							pgxc_clean_node_info[ii].node_name,
							str_txn_stat(txn->txn_stat[ii]),
							txn->nodeparts[ii]);
				}
			}
		}
	}

	/*
	 * Then disconnect from the database.
	 * I need to login to specified databases which 2PC is issued for.  Again, we assume
	 * that all the prepare is issued against the same database in each node, which
	 * current Coordinator does and there seems to be no way to violate this assumption.
	 */
	if (verbose_opt)
	{
		fprintf(outf, "%s: disconnecting\n", progname);
	}
	PQfinish(coord_conn);

	/*
	 * If --no-clean option is specified, we exit here.
	 */
	if (no_clean_opt)
	{
		fprintf(outf, "--no-clean opt is specified. Exiting.\n");
		exit(0);
	}

	/*
	 * Recover 2PC for specified databases
	 */
	if (clean_all_databases)
	{
		database_info *cur_database_info;

		for(cur_database_info = head_database_info; cur_database_info; cur_database_info = cur_database_info->next)
		{
			recover2PCForDatabase(cur_database_info);
		}
	}
	else
	{
		database_info *cur_database_info;
		database_names *cur_database_name;

		for(cur_database_name = head_database_names; cur_database_name; cur_database_name = cur_database_name->next)
		{
			cur_database_info = find_database_info(cur_database_name->database_name);
			if (cur_database_info)
			{
				recover2PCForDatabase(cur_database_info);
			}
		}
	}

    /* In simple clean mode, no extra twophase file left on disk, and the cleanup functions are undefined in kernel. */
    if (!simple_clean_2pc)
    {
        cleanObsoleteTwoPhaseFiles();
    }

	exit(0);
}

static void
getMyNodename(PGconn *conn)
{
	static const char *stmt = "SELECT pgxc_node_str()";
	PGresult *res;

	res = PQexec(conn, stmt);

	/* Error handling here */
	if (res)
		my_nodename = strdup(PQgetvalue(res, 0, 0));
	else
		my_nodename = strdup("unknown");

	PQclear(res);
}

static void
recover2PCForDatabase(database_info *db_info)
{
	PGconn 		*coord_conn;
	txn_info   	*cur_txn;

	if (verbose_opt)
		fprintf(outf, "%s: recovering 2PC for database \"%s\"\n", progname, db_info->database_name);
	coord_conn = loginDatabase(coordinator_host, coordinator_port, username, password, db_info->database_name,
							   progname, "auto", password_prompt);
	if (coord_conn == NULL)
	{
		fprintf(errf, "Could not connect to the database %s.\n", db_info->database_name);
		return;
	}

	if (verbose_opt)
		fprintf(outf, "%s: connected to the database \"%s\"\n", progname, db_info->database_name);
	for(cur_txn = db_info->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		recover2PC(coord_conn, cur_txn, db_info->database_name);
	}
	PQfinish(coord_conn);
}

/*
 * GetCurrentTimestamp -- get the current operating system time
 *
 * Result is in the form of a TimestampTz value, and is expressed to the
 * full precision of the gettimeofday() syscall
 */
TimestampTz
GetCurrentTimestamp(void)
{
	TimestampTz result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (TimestampTz) tp.tv_sec -
			 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

static void
recover2PC(PGconn *conn, txn_info *txn, char *database_name)
{
	TXN_STATUS txn_stat;

	if (simple_clean_2pc)
    {
	    /* simple clean mode will rollback or commit all prepared txns depends on setting: handle_2pc_mode */
	    if (ROLLBACK_ALL_PREPARED_TXN == handle_2pc_mode)
        {
            txn_stat = TXN_STATUS_ABORTED;
        }
	    else if (COMMIT_ALL_PREPARED_TXN == handle_2pc_mode)
        {
            txn_stat = TXN_STATUS_COMMITTED;
        }
	    else
        {
            fprintf(stderr, "Invalid handle_2pc_mode:%d\n", handle_2pc_mode);
            exit(-1);
        }
    }
	else
    {
        txn_stat = check_txn_global_status(txn);
    }
	if (verbose_opt)
	{
		fprintf(outf, "    Recovering TXN: gid: %s, owner: \"%s\", global status: %s\n",
				txn->gid, txn->owner, str_txn_stat(txn_stat));
	}

	if (!simple_clean_2pc && txn->prepare_timestamp_elapse < min_clean_xact_interval)
	{
		if (force_clean_2pc)
		{
			fprintf(outf, "txn gid:%s prepare_timestamp_elapse:%d, not reach clean timegap:%d. But configured to force clean.\n",
					txn->gid, txn->prepare_timestamp_elapse,
					min_clean_xact_interval);
		}
		else
		{
			fprintf(outf, "txn gid:%s prepare_timestamp_elapse:%d, not reach clean timegap:%d\n",
					txn->gid, txn->prepare_timestamp_elapse,
					min_clean_xact_interval);
			return;
		}
	}

	fprintf(outf, "txn gid:%s prepare_timestamp_elapse:%d,clean timegap:%d.\n",
			txn->gid, txn->prepare_timestamp_elapse,
			min_clean_xact_interval);

	switch (txn_stat)
	{
		case TXN_STATUS_FAILED:
		case TXN_STATUS_UNKNOWN:
			if (verbose_opt)
				fprintf(outf, "        Recovery not needed. global txn_stat:%s\n", str_txn_stat(txn_stat));
			return;
		case TXN_STATUS_PREPARED:
			if (verbose_opt)
				fprintf(outf, "        Recovery not needed. global txn_stat:%s\n", str_txn_stat(txn_stat));
			return;
		case TXN_STATUS_COMMITTED:
		    if (simple_clean_2pc)
            {
		        do_commit_abort_simple_clean_mode(conn, txn, database_name, true);
            }
		    else
            {
                if (txn->commit_timestamp == InvalidGlobalTimestamp)
                {
                    fprintf(errf, "        Recovery failed. global txn_stat:%s, but has no valid commit timestamp.\n", str_txn_stat(txn_stat));
                    exit(-1);
                }
                else
                {
                    do_commit(conn, txn);
                }
            }
			return;
		case TXN_STATUS_ABORTED:
		    if (simple_clean_2pc)
            {
                do_commit_abort_simple_clean_mode(conn, txn, database_name, false);
            }
		    else
            {
                do_abort(conn, txn);
            }
			return;
		case TXN_STATUS_INPROGRESS:
			fprintf(stderr, "        Can't recover a running transaction. global txn_stat:%s\n", str_txn_stat(txn_stat));
			//exit(1);
			return;
		default:
			fprintf(stderr, "        Unknown TXN status, pgxc_clean error. global txn_stat:%s\n", str_txn_stat(txn_stat));
			exit(1);
	}
	return;
}

static void
do_commit(PGconn *conn, txn_info *txn)
{
	do_commit_abort(conn, txn, true);
}

static void
do_abort(PGconn *conn, txn_info *txn)
{
	do_commit_abort(conn, txn, false);
}

static void
do_commit_abort(PGconn *conn, txn_info *txn, bool is_commit)
{
	int ii;
	bool has_error = false;

	char* EXEC_DIRECT_COMMIT_STMT = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ select txn_commit_prepared(''%s'', %lld);';";
	char* EXEC_DIRECT_ABORT_STMT = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ ROLLBACK PREPARED ''%s'';'";
	char* EXEC_DIRECT_CLEANUP_STMT = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ select polardbx_finish_global_transation(''%s'');'";

	char *stmt = (char *) malloc (128 + strlen(txn->gid));
	PGresult *res;
	char* res_s;
	ExecStatusType res_status;

	if (verbose_opt)
		fprintf(outf, "    %s... \n", is_commit ? "committing" : "aborting");
	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		memset(stmt, 0, 128 + strlen(txn->gid));
		if (txn->txn_stat[ii] == TXN_STATUS_PREPARED)
		{
			if (txn->nodeparts[ii] == 0)
			{
				fprintf(errf, "Txn gid:%s node:%s is expected to participate in 2pc.\n", txn->gid, pgxc_clean_node_info[ii].node_name);
				exit(-1);
			}
			if (txn->commit_timestamp == InvalidGlobalTimestamp && is_commit)
			{
				fprintf(errf, "Txn gid:%s has no commit timestamp.\n", txn->gid);
				exit(-1);
			}

			if (is_commit)
			{
				sprintf(stmt, EXEC_DIRECT_COMMIT_STMT,
						pgxc_clean_node_info[ii].node_name,
						txn->gid,
						txn->commit_timestamp);
			}
			else
			{
				sprintf(stmt, EXEC_DIRECT_ABORT_STMT,
						pgxc_clean_node_info[ii].node_name,
						txn->gid);
			}

			res = PQexec(conn, stmt);
			res_status = PQresultStatus(res);
			if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
			{
				fprintf(errf, "exec stmt:%s failed (%s), errmsg:%s, has_error set to true.\n",
						stmt, PQresultErrorMessage(res), pgxc_clean_node_info[ii].node_name);
				has_error = true;
			}
			if (verbose_opt)
			{
				if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
					fprintf(outf, "exec stmt:%s succeeded (%s).\n", stmt, pgxc_clean_node_info[ii].node_name);
				else
					fprintf(outf, "exec stmt:%s failed (%s: %s).\n",
							stmt,
							pgxc_clean_node_info[ii].node_name,
							PQresultErrorMessage(res));
			}
			else
			{
				if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
				{
					fprintf(errf, "Failed to recover TXN, gid: %s, owner: \"%s\", node: \"%s\", stmt:%s, (%s)\n",
							txn->gid, txn->owner, pgxc_clean_node_info[ii].node_name,
							stmt, PQresultErrorMessage(res));
				}
			}
			PQclear(res);
		}
	}

	if (has_error)
	{
		fprintf(errf, "has_error when %s txn, gid:%s. so skip clear 2pc files.\n", is_commit ? "commit" : "abort", txn->gid);
		free(stmt);
		return;
	}

	// clear 2pc files when commit 2pc
	if (is_commit)
	{
		for (ii = 0; ii < pgxc_clean_node_count; ii++)
		{
			memset(stmt, 0, 128 + strlen(txn->gid));
			// all nodes need to exec cleanup cmd
			sprintf(stmt, EXEC_DIRECT_CLEANUP_STMT,
					pgxc_clean_node_info[ii].node_name,
					txn->gid);
			res = PQexec(conn, stmt);
			res_status = PQresultStatus(res);
			if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
			{
				fprintf(outf, "exec stmt:%s failed on node:%s, errmsg:%s\n",
						stmt, pgxc_clean_node_info[ii].node_name, PQresultErrorMessage(res));
				continue;
			}
			res_s = PQgetvalue(res, 0, 0);
			fprintf(outf, "exec stmt:%s on node:%s, get return value:%s\n", stmt, pgxc_clean_node_info[ii].node_name, res_s);

			PQclear(res);
		}
	}

	free(stmt);
}

#if 0
static database_info *
find_database_info(char *dbname)
{
	database_info *cur_database_info;

	for(cur_database_info = head_database_info; cur_database_info; cur_database_info = cur_database_info->next)
	{
		if (strcmp(cur_database_info->database_name, dbname) == 0)
			return(cur_database_info);
	}
	return(NULL);
}
#endif


static PGconn *
loginDatabase(char *host, int port, char *user, char *password, char *dbname, const char *progname, char *encoding, char *password_prompt)
{
	bool new_pass = false;
	PGconn *coord_conn;
	char port_s[32];
#define PARAMS_ARRAY_SIZE 8
	const char *keywords[PARAMS_ARRAY_SIZE];
	const char *values[PARAMS_ARRAY_SIZE];

	sprintf(port_s, "%d", port);

	keywords[0] = "host";
	values[0] = host;
	keywords[1] = "port";
	values[1] = port_s;
	keywords[2] = "user";
	values[2] = user;
	keywords[3] = "password";
	keywords[4] = "dbname";
	values[4] = dbname;
	keywords[5] = "fallback_application_name";
	values[5] = progname;
	keywords[6] = "client_encoding";
	values[6] = encoding;
	keywords[7] = NULL;
	values[7] = NULL;

	/* Loop until we have a password if requested by backend */
	do
	{
		values[3] = password;

		new_pass = false;
		coord_conn = PQconnectdbParams(keywords, values, true);

		if (PQstatus(coord_conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(coord_conn) &&
			!have_password &&
			try_password_opt != TRI_NO)
		{
			PQfinish(coord_conn);
			simple_prompt(password_prompt, password, sizeof(password), false);
			have_password = true;
			new_pass = true;
		}
	} while (new_pass);

	return(coord_conn);
}

TimestampTz
getCommitTimestamp(PGconn *conn, char* gid, char* node_name)
{
	PGresult *res;
	char *res_s;
	char stmt[1024];
	TimestampTz commit_timestamp = InvalidGlobalTimestamp;
	char *STMT_COMMIT_TS = "EXECUTE DIRECT ON (%s) 'SELECT polardbx_get_2pc_commit_timestamp(''%s'');'";

	sprintf(stmt, STMT_COMMIT_TS, node_name, gid);
	res = PQexec(conn, stmt);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQgetisnull(res, 0, 0))
	{
		fprintf(errf, "Failed to get commit timestamp on node:%s for gid:%s, stmt:%s", node_name, gid, stmt);
		PQclear(res);
	}
	else
	{
		res_s = PQgetvalue(res, 0, 0);
		commit_timestamp = atoll(res_s);
		fprintf(errf, "get commit timestamp:" INT64_FORMAT " on node:%s for gid:%s, stmt:%s", commit_timestamp, node_name, gid, stmt);
	}

	return commit_timestamp;
}

static TXN_STATUS
getTxnStatus(PGconn *conn, txn_info* txn, int node_idx)
{
	char *node_name;
	char stmt[1024];
	PGresult *res;
	char* gid = txn->gid;
	int transaction_status;

	static const char *STMT_FORM = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ SELECT polardbx_get_transaction_status(''%s'');'";

	node_name = pgxc_clean_node_info[node_idx].node_name;
	sprintf(stmt, STMT_FORM, node_name, gid);

	res = PQexec(conn, stmt);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK ||
			PQgetisnull(res, 0, 0))
	{
		if (res == NULL)
		{
			fprintf(errf, "Failed to exec stmt:%s, res is NULL. set txn status to TXN_STATUS_UNKNOWN.\n",
					stmt);
		}
		else if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(errf, "Failed to exec stmt:%s, errmsg:%s. set txn status to TXN_STATUS_UNKNOWN.\n",
					stmt,
					res == NULL ? "res is null" : PQresultErrorMessage(res));
		}
		else if (PQgetisnull(res, 0, 0))
		{
			fprintf(errf, "get null output of exec stmt:%s, set txn status to TXN_STATUS_UNKNOWN.\n",
					stmt);
		}
		
		return TXN_STATUS_UNKNOWN;
	}
	
	transaction_status = atoi(PQgetvalue(res, 0, 0));
	
	if (POLARDBX_TRANSACTION_COMMITED == transaction_status)
	{
		// If status is commited, there must have valid commit timestamp on datanode.
		if (txn->commit_timestamp == InvalidGlobalTimestamp)
		{
			txn->commit_timestamp = getCommitTimestamp(conn, gid, node_name);
		}
		return TXN_STATUS_COMMITTED;
	}
	else if (POLARDBX_TRANSACTION_ABORTED == transaction_status)
	{
		return TXN_STATUS_ABORTED;
	}
	else if (POLARDBX_TRANSACTION_INPROGRESS == transaction_status)
	{
		return TXN_STATUS_INPROGRESS;
	}
	else if (POLARDBX_TRANSACTION_TWOPHASE_FILE_NOT_FOUND == transaction_status)
	{
		return TXN_STATUS_ABORTED;
	}
	else
	{
		fprintf(errf, "gid:%s on node:%s status invalid.", gid, node_name);
		exit(-1);
	}
	
	return TXN_STATUS_UNKNOWN;
}

static void
getTxnInfoOnOtherNodes(PGconn *conn, txn_info *txn)
{
	int ii;

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		if (txn->txn_stat[ii] == TXN_STATUS_INITIAL && txn->nodeparts[ii])
        {
		    if (!simple_clean_2pc)
                txn->txn_stat[ii] = getTxnStatus(conn, txn, ii);
		    else
            {
		        /* In simple clean mode, txn->txn_stat must be prepared when txn->nodeparts is 1. */
                fprintf(errf, "Error. simple clean mode will have txn TXN_STATUS_PREPARED, now got TXN_STATUS_INITIAL "
                              "in node:%s, txn gid:%s.\n", 
                            pgxc_clean_node_info[ii].node_name, txn->gid);
                exit(-1);
            }
                
        }
	}
}


static void
getTxnInfoOnOtherNodesForDatabase(PGconn *conn, database_info *database)
{
	txn_info *cur_txn = NULL;
	int nodeidx = -1;
	char* temp = NULL;

	if (simple_clean_2pc && cur_txn->participate_nodes)
    {
        fprintf(errf, "Error. simple clean mode will have empty participate_nodes info, now got %s.\n", cur_txn->participate_nodes);
        exit(-1);
    }
	
	for (cur_txn = database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		// get txn status on all other participated nodes.
		if ((NULL == cur_txn->participate_nodes) || cur_txn->participate_nodes[0] == '\0')
		{
            if (simple_clean_2pc)
            {
                /* 
                 * It's ok. we only need to handle the node which has prepared txns (all rollback/commit).
                 * cur_txn->nodeparts already marked when getPreparedTxnListOfNodeSImpleCleanMode
                 * */
            }
            else
            {
                fprintf(errf, "Txn gid:%s has invalid participate_node.\n", cur_txn->gid);
                exit(-1);
            }
		}
		else
		{
			char *tmp_participate_nodes = strdup(cur_txn->participate_nodes);
			temp = strtok(tmp_participate_nodes,",");
			while (temp)
			{
				nodeidx = find_node_index(temp);
				if (nodeidx == -1)
				{
					fprintf(errf, "Txn gid:%s invalid participate node info:%s. Unable to find node:%s\n",
							cur_txn->gid, cur_txn->participate_nodes, temp);
					exit(1);
				}
				if (cur_txn->nodeparts[nodeidx] == 0)
				{
					fprintf(outf, "NOTICE: database:%s, node:%s, gid:%s xid:%d has no prepared info.\n",
							database->database_name, temp, cur_txn->gid, cur_txn->xid[nodeidx]);
					cur_txn->nodeparts[nodeidx] = 1;
				}

				temp = strtok(NULL,",");
			}
			free(tmp_participate_nodes);
		}

		getTxnInfoOnOtherNodes(conn, cur_txn);
	}
}


static void
getTxnInfoOnOtherNodesAll(PGconn *conn)
{
	database_info *cur_database;

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
		fprintf(outf, "getTxnInfoOnOtherNodesAll on database:%s\n", cur_database->database_name);
		getTxnInfoOnOtherNodesForDatabase(conn, cur_database);
	}
}


static void
getPreparedTxnListOfNode(PGconn *conn, int idx)
{
	int prep_txn_count;
	int ii;
	PGresult *res;
	ExecStatusType pq_status;

#define MAX_STMT_LEN 1024

	/* SQL Statement */
	static const char *STMT_GET_PREP_TXN_ON_NODE
		= "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ SELECT TRANSACTION::text, GID::text, OWNER::text, DATABASE::text, "
		  "PARTICIPATE_NODES::text, floor(extract(epoch from (now()-prepared))), prepared::text, now()::text FROM POLARDBX_PREPARED_XACTS;'";
	char stmt[MAX_STMT_LEN];

	sprintf(stmt, STMT_GET_PREP_TXN_ON_NODE,
			pgxc_clean_node_info[idx].node_name);

	res = PQexec(conn, stmt);
	if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain prepared transaction list for node %s.(%s)\n",
				pgxc_clean_node_info[idx].node_name, res ? PQresultErrorMessage(res) : "");
		PQclear(res);
		//exit (1);
		return;
	}
	prep_txn_count = PQntuples(res);
	total_prep_txn_count += prep_txn_count;
	for (ii = 0; ii < prep_txn_count; ii++)
	{
		TransactionId xid;
		char *gid;
		char *owner;
		char *database_name;
		char *participate_nodes;
		int			  prepared_time_elapse;
		char *prepare_time;
		char *now_time;

		xid = atoi(PQgetvalue(res, ii, 0));
		gid = strdup(PQgetvalue(res, ii, 1));
		owner = strdup(PQgetvalue(res, ii, 2));
		database_name = strdup(PQgetvalue(res, ii, 3));
		participate_nodes = strdup(PQgetvalue(res, ii, 4));
		prepared_time_elapse = atoi(PQgetvalue(res, ii, 5));
		prepare_time = strdup(PQgetvalue(res, ii, 6));
		now_time = strdup(PQgetvalue(res, ii, 7));

		fprintf(outf, "nodename:%s, get gid:%s, xid:%d, owner:%s, database_name:%s, participate_nodes:%s, prepared_time_elapse:%d(s), prepare_time:%s, now_time:%s.\n",
				pgxc_clean_node_info[idx].node_name, gid, xid, owner, database_name, participate_nodes, prepared_time_elapse, prepare_time, now_time);
		add_txn_info(database_name, pgxc_clean_node_info[idx].node_name, gid, xid, owner, participate_nodes,
					 prepared_time_elapse,
					 TXN_STATUS_PREPARED);

		if (gid)
			free(gid);
		if (owner)
			free(owner);
		if (database_name)
			free(database_name);
		if (participate_nodes)
			free(participate_nodes);
		if (prepare_time)
			free(prepare_time);
		if (now_time)
			free(now_time);
	}
	PQclear(res);
}

static void
getPreparedTxnList(PGconn *conn)
{
	int ii;
	total_prep_txn_count = 0; // reset total_prep_txn_count to 0

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
	    if (simple_clean_2pc)
            getPreparedTxnListOfNodeSimpleCleanMode(conn, ii);
	    else
		    getPreparedTxnListOfNode(conn, ii);
	}

	if (print_prepared_xact_only)
	{
		fprintf(outf, "getPreparedTxnList: total_prep_txn_count=%d\n", total_prep_txn_count);
	}
}

static void
getDatabaseList(PGconn *conn)
{
	int database_count;
	int ii;
	PGresult *res;
	char *dbname;

	/* SQL Statement */
	static const char *STMT_GET_DATABASE_LIST = "/* pgxc_clean */ SELECT DATNAME FROM PG_DATABASE;";

	/*
	 * Get database list
	 */
	res = PQexec(conn, STMT_GET_DATABASE_LIST);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain database list.\n");
		PQclear(res);
		exit (1);
	}
	database_count = PQntuples(res);
	for(ii = 0; ii < database_count; ii++)
	{
		dbname = PQgetvalue(res, ii, 0);
		if (strcmp(dbname, "template0") == 0)
			/* Skip template0 database */
			continue;
		add_database_info(dbname);
	}
	PQclear(res);
}

static int getSvrOptionValue(char *srvoptions, char *key, char *value)
{
	int i = 0;
	char *ret = strstr(srvoptions, key);
	if (ret == NULL)
		return -1;

	char *pStr = ret;
	pStr = pStr + strlen(key) + 1;
	while (*pStr != '\0' && *pStr != ' ' && *pStr != ',' && *pStr != '}')
	{
		value[i++] = *pStr;
		pStr ++;
	}

	return 0;
}

static void
getNodeList(PGconn *conn)
{
	int ii;
	PGresult *res;

	/* SQL Statement */
	static const char *STMT_GET_NODE_INFO = "/* pgxc_clean */ SELECT srvname, srvtype, "
											"srvoptions FROM pg_foreign_server WHERE srvtype IN ('C', 'D');";

	res = PQexec(conn, STMT_GET_NODE_INFO);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain node list.\n");
		PQclear(res);
		exit (1);
	}
	pgxc_clean_node_count = PQntuples(res);
	pgxc_clean_node_info = (node_info *)calloc(pgxc_clean_node_count, sizeof(node_info));
	if (pgxc_clean_node_info == NULL)
	{
		fprintf(stderr, "No more memory.\n");
		exit(1);
	}

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		char *node_name;
		char *node_type_c;
		NODE_TYPE node_type;
		int  port;
		char *host;
		int	 nodeid;
		char *srvoptions;
		char value[256];

		node_name = strdup(PQgetvalue(res, ii, 0));
		node_type_c = strdup(PQgetvalue(res, ii, 1));
		switch (node_type_c[0])
		{
			case 'C':
				/* pgxc_clean has to connect to the Coordinator */
				node_type = NODE_TYPE_COORD;
				if (strcmp(node_name, my_nodename) == 0)
					my_nodeidx = ii;
				break;
			case 'D':
				node_type = NODE_TYPE_DATANODE;
				break;
			case 'G':
				node_type = NODE_TYPE_GTM;
				break;
			default:
				fprintf(stderr, "Invalid catalog data (node_type), node_name: %s, node_type: %s\n", node_name, node_type_c);
				exit(1);
		}
		srvoptions = strdup(PQgetvalue(res, ii, 2));
		memset(value, 0, sizeof(value));
		getSvrOptionValue(srvoptions, "port", value);
		port = atoi(value);

		memset(value, 0, sizeof(value));
		getSvrOptionValue(srvoptions, "host", value);
		host = strdup(value);

		memset(value, 0, sizeof(value));
		getSvrOptionValue(srvoptions, "node_id", value);
		nodeid = atoi(value);

		if (node_type != NODE_TYPE_GTM)
		{
			set_node_info(node_name, port, host, node_type, nodeid, ii);
		}

		if (node_name)
			free(node_name);
		if (node_type_c)
			free(node_type_c);
		if (host)
			free(host);
	}
	/* Check if local Coordinator has been found */
	if (my_nodeidx == -1)
	{
		fprintf(stderr, "Failed to identify the coordinator which %s is connecting to.  ", progname);
		fprintf(stderr, "Connecting to a wrong node.\n");
		exit(1);
	}
}



static void
showVersion(void)
{
	puts("pgxc_clean (Postgres-XC) " PGXC_VERSION);
}

static void
add_to_database_list(char *dbname)
{
	if (head_database_names == NULL)
	{
		head_database_names = last_database_names = (database_names *)malloc(sizeof(database_names));
		if (head_database_names == NULL)
		{
			fprintf(stderr, "No more memory, FILE:%s, LINE:%d.\n", __FILE__, __LINE__);
			exit(1);
		}
	}
	else
	{
		last_database_names->next = (database_names *)malloc(sizeof(database_names));
		if (last_database_names->next == NULL)
		{
			fprintf(stderr, "No more memory, FILE:%s, LINE:%d.\n", __FILE__, __LINE__);
			exit(1);
		}
		last_database_names = last_database_names->next;
	}
	last_database_names->next = NULL;
	last_database_names->database_name = dbname;
}

static void
parse_pgxc_clean_options(int argc, char *argv[])
{
	static struct option long_options[] =
	{
		{"all", no_argument, NULL, 'a'},
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"no-clean", no_argument, NULL, 'N'},
		{"output", required_argument, NULL, 'o'},
		{"port", required_argument, NULL, 'p'},
		{"quiet", no_argument, NULL, 'q'},
		{"username", required_argument, NULL, 'U'},
		{"verbose", no_argument, NULL, 'v'},
		{"version", no_argument, NULL, 'V'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"help", no_argument, NULL, '?'},
		{"status", no_argument, NULL, 's'},
		{"print-prepared-xacts-only", no_argument, NULL, 't'},
		{"min-clean-xact-interval", required_argument, NULL, 'T'},
		{"min-clean-2pcfile-interval", required_argument, NULL, 'y'},
		{"force-clean-2pc", no_argument, NULL, 'f'},
		/* simple-clean-2pc is used for plugin version without HLC. 
		 * And when cleaning up, some HLC specific function will not used, such as polardbx_finish_global_transation etc.
		 * For transactions which are prepared, will abort or commit based on handle-2pc-mode. 
		 * */
        {"simple-clean-2pc", no_argument, NULL, 'S'},
        {"handle-2pc-mode", required_argument, NULL, 'H'},
		{NULL, 0, NULL, 0}
	};

	int optindex;
	extern char *optarg;
	extern int optind;
	int c;

	progname = get_progname(argv[0]);		/* Should be more fancy */

	while ((c = getopt_long(argc, argv, "ad:h:T:y:No:p:qU:vVwWstfSH:?", long_options, &optindex)) != -1)
	{
		switch(c)
		{
			case 'a':
				clean_all_databases = true;
				break;
			case 'd':
				add_to_database_list(optarg);
				break;
			case 'h':
				coordinator_host = optarg;
				break;
			case 'N':
				no_clean_opt = true;
				break;
			case 'o':
				output_filename = optarg;
				break;
			case 'p':
				coordinator_port = atoi(optarg);
				break;
			case 'q':
				verbose_opt = false;
				break;
			case 'U':
				username = optarg;
				break;
			case 'V':
				version_opt = 0;
				break;
			case 'v':
				verbose_opt = true;
				break;
			case 'w':
				try_password_opt = TRI_NO;
				break;
			case 'W':
				try_password_opt = TRI_YES;
				break;
			case 's':
				status_opt = true;
				break;
			case 't':
				print_prepared_xact_only = true;
				break;
			case 'f':
				force_clean_2pc = true;
				break;
            case 'S':
                simple_clean_2pc = true;
                break;
		    case 'H':
		        handle_2pc_mode = atoi(optarg);
		        if (handle_2pc_mode != ROLLBACK_ALL_PREPARED_TXN && handle_2pc_mode != COMMIT_ALL_PREPARED_TXN)
                {
                    fprintf(stderr, "Invalid mode. only 0 or 1 is valid. 0 for rollback and 1 for commit.\n");
                    exit(1);
                }
		        break;
			case 'T':
				min_clean_xact_interval = atoi(optarg);
				fprintf(stdout, "get min_clean_xact_interval:%d(s) from user input.\n",
						min_clean_xact_interval);
				break;
			case 'y':
				min_clean_file_interval = atoi(optarg);
				fprintf(stdout, "get min_clean_file_interval:%d(s) from user input.\n",
						min_clean_file_interval);
				break;
			case '?':
				if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0)
				{
					usage();
					exit(0);
				}
				else
				{
					fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
					exit(1);
				}
				break;
			default:
				fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
				exit(1);
				break;
		}
	}

	while (argc - optind >= 1)
	{
		if (head_database_names == NULL)
		{
			if (strcmp(argv[optind], "template0") == 0)
			{
				fprintf(stderr, "%s: You should not clean template0 database.\n", progname);
				exit(1);
			}
			add_to_database_list(argv[optind]);
		}
		if (username == NULL)
			username = argv[optind];
		else
			fprintf(stderr, "%s: warning: extra command-line argument \"%s\" ignored\n",
					progname, argv[optind]);
		optind++;
	}

	if (!clean_all_databases && head_database_names == NULL)
	{
		fprintf(stderr, "Please specify at least one database or -a for all\n");
		exit(1);
	}
	
	if (simple_clean_2pc && force_clean_2pc)
    {
        fprintf(stderr, "simple-clean-2pc is not compatible with force-clean-2pc. Consider to disable either.\n");
        exit(1);
    }
}

static char *GetUserName(void)
{
	struct passwd *passwd;

	passwd = getpwuid(getuid());
	if (passwd)
		return(strdup(passwd->pw_name));
	else
	{
		fprintf(stderr, "%s: could not get current user name: %s\n", progname, strerror(errno));
		exit(1);
	}
	return NULL;
}

static void usage(void)
{
	char *env;
	char *user;

	user = getenv("PGUSER");
	if (!user)
		user = GetUserName();

	printf("pgxc_clean cleans up outstanding 2PCs after failed node is recovered.\n"
		   "Usage:\n"
		   "pgxc_clean [OPTION ...] [DBNAME [USERNAME]]\n\n"
		   "Options:\n");

	env = getenv("PGDATABASE");
	if (!env)
		env = user;
	printf("  -a, --all                cleanup all the databases available.\n");
	printf("  -d, --dbname=DBNAME      database name to clean up (default: \"%s\")\n", env);
	env = getenv("PGHOST");
	printf("  -h, --host=HOSTNAME      target coordinator host address, (default: \"%s\")\n", env ? env : "local socket");
	printf("  -N, no-clean             only collect 2PC information.  Do not recover them\n");
	printf("  -o, --output=FILENAME    output file name.\n");
	env = getenv("PGPORT");
	printf("  -p, --port=PORT          port number of the coordinator (default: \"%s\")\n", env ? env : DEF_PGPORT_STR);
	printf("  -q, --quiet              quiet mode.  do not print anything but error information.\n");
	printf("  -s, --status             prints out 2PC status\n");
	env = getenv("PGUSER");
	if (!env)
		env = user;
	printf("  -U, --username=USERNAME  database user name (default: \"%s\")\n", env);
	printf("  -v, --verbose            print recovery information.\n");
	printf("  -V, --version            prints out the version.\n");
	printf("  -w, --no-password        never prompt for the password.\n");
	printf("  -W, --password           prompt for the password.\n");
	printf("  -?, --help               print this message.\n");
}

/*
 * This function clean cluster-wide obsolete twophase files which exist in each node's pg_twophase dir.
 * 1. Get twophase file list on each node.
 * 2. Get participate node and prepare time on each node.
 * 3. Classify twophase files by gid.
 * 4. Check all other node's transaction status.
 * 5. Determine the global transaction status based on all node's status.
 * 6. Delete obsolete twophase files.
 * 		Only twophase files which already prepared for min_clean_file_interval seconds will be cleaned.
 * */
static void cleanObsoleteTwoPhaseFiles(void)
{
	PGconn 		*coord_conn;
#define DEFAULT_DATABASE "postgres"
	coord_conn = loginDatabase(coordinator_host, coordinator_port, username, password, "postgres",
							   progname, "auto", password_prompt);
	if (coord_conn == NULL)
	{
		fprintf(errf, "Could not connect to the database %s.\n", DEFAULT_DATABASE);
		return;
	}

	getTwoPhaseFileListOnDisk(coord_conn);

	getFileStatusOnOtherNodesAll(coord_conn);

	checkFileGlobalStatus(coord_conn);

	printTwoPhaseFileList();

	deleteTwoPhaseFile(coord_conn);

	PQfinish(coord_conn);

}

/* parse string, set value in TwoPhaseFileDetail */
bool parseTwoPhaseFileDetail(char *detail, TwoPhaseFileDetail *twoPhaseFileDetail)
{
#define  MAX_PARTICIPATE_NODE_LEN 33280 // (PGXC_NODENAME_LENGTH+1) * (POLARX_MAX_COORDINATOR_NUMBER + POLARX_MAX_DATANODE_NUMBER)
	char magic[11];
	int total_len;
	int xid;
	int database;
	long long int prepared_at;
	int owner;
	int nsubxacts;
	int ncommitrels;
	int nabortrels;
	int ninvalmsgs;
	int initfileinval;
	int gidlen;
	unsigned long long origin_lsn;
	long long int origin_timestamp;
	long long int commit_timestamp;
	int nparticipatenodes_len;
	char gid[200];
	char participate_nodes[MAX_PARTICIPATE_NODE_LEN];

	if (!detail)
	{
		fprintf(errf, "parseTwoPhaseFileDetail: NULL parameter for parse detail.");
		exit(1);
	}

	sscanf(detail, "magic:%s\n"
				   "total_len:%d\n"
				   "xid:%d\n"
				   "database:%d\n"
				   "prepared_at:%lld\n"
				   "owner:%d\n"
				   "nsubxacts:%d\n"
				   "ncommitrels:%d\n"
				   "nabortrels:%d\n"
				   "ninvalmsgs:%d\n"
				   "initfileinval:%d\n"
				   "gidlen:%d\n"
				   "origin_lsn:%llu\n"
				   "origin_timestamp:%lld\n"
				   "commit_timestamp:%lld\n"
				   "nparticipatenodes_len:%d\n"
				   "gid:%s\n"
				   "participate_nodes:%s\n",
		   magic,
		   &total_len,
		   &xid,
		   &database,
		   &prepared_at, &owner, &nsubxacts, &ncommitrels, &nabortrels, &ninvalmsgs, &initfileinval, &gidlen, &origin_lsn, &origin_timestamp,
		   &commit_timestamp, &nparticipatenodes_len, gid, participate_nodes);

	twoPhaseFileDetail->xid = xid;
	twoPhaseFileDetail->prepared_at = prepared_at;
	twoPhaseFileDetail->commit_timestamp = commit_timestamp;
	twoPhaseFileDetail->gid = strdup(gid);
	twoPhaseFileDetail->participate_nodes = strdup(participate_nodes);

	return true;
}

/* connect to node, get select polardbx_parse_2pc_file output raw string. */
char* getTwoPhaseFileDetail(PGconn *conn, int idx, char* filename)
{
	int res_count;
	int ii;
	PGresult *res;
	ExecStatusType pq_status;
	char* twophasefile_detail = NULL;
	static const char *STMT_GET_TWOPHASE_FILE_DETAIL_ON_NODE = "EXECUTE DIRECT ON (%s) '/*pgxc_clean*/ SELECT polardbx_parse_2pc_file(''%s'');'";
	char stmt[MAX_STMT_LEN];

	sprintf(stmt, STMT_GET_TWOPHASE_FILE_DETAIL_ON_NODE, pgxc_clean_node_info[idx].node_name, filename);
	res = PQexec(conn, stmt);
	if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "ERROR: Could not obtain twophase file %s detail on node %s. errmsg:(%s)\n",
				filename, pgxc_clean_node_info[idx].node_name, res ? PQresultErrorMessage(res) : "");
		PQclear(res);
		return NULL;
	}

	res_count = PQntuples(res);
	if (res_count > 1)
	{
		fprintf(stderr, "ERROR: failed to get twophase file detail for node %s, filename:%s. get incorrect row cnt:%d.\n",
				pgxc_clean_node_info[idx].node_name, filename, res_count);
		exit(1);
	}
	for (ii = 0; ii < res_count; ii++)
	{
		twophasefile_detail = strdup(PQgetvalue(res, ii, 0));
	}
	PQclear(res);

	fprintf(outf, "Get twophase file detail for node %s, filename:%s. twophasefile_detail:%s.\n",
			pgxc_clean_node_info[idx].node_name, filename, twophasefile_detail ? twophasefile_detail : "NULL");
	return twophasefile_detail;
}

TwoPhaseFileInfo *find_file_info(char* gid)
{
	TwoPhaseFileInfo *current = head_file_info;
	for (; current; current = current->next)
	{
		if (strcmp(gid, current->gid) == 0)
		{
			return current;
		}
	}
	return NULL;
}

TwoPhaseFileInfo* make_file_info(TwoPhaseFileDetail *detail)
{
	char *tmp_participate_node = NULL;
	char *name = NULL;
	int nodeidx = -1;

	TwoPhaseFileInfo *fileinfo = (TwoPhaseFileInfo*) malloc(sizeof(TwoPhaseFileInfo));
	memset(fileinfo, 0, sizeof(TwoPhaseFileInfo));

	fileinfo->is_participate = (bool*)malloc(sizeof(bool) * pgxc_clean_node_count);
	memset(fileinfo->is_participate, 0, sizeof(bool) * pgxc_clean_node_count);

	fileinfo->is_fileexist = (bool*)malloc(sizeof(bool) * pgxc_clean_node_count);
	memset(fileinfo->is_fileexist, 0, sizeof(bool) * pgxc_clean_node_count);

	fileinfo->status = (TXN_STATUS*)malloc(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	memset(fileinfo->status, 0, sizeof(TXN_STATUS) * pgxc_clean_node_count);

	fileinfo->gid = strdup(detail->gid);
	fileinfo->participate_node = strdup(detail->participate_nodes);
	fileinfo->prepare_time = detail->prepared_at;
	fileinfo->commit_time = detail->commit_timestamp;
	fileinfo->detail_head = fileinfo->detail_last = detail;
	fileinfo->is_fileexist[detail->nodeidx] = true;

	/* assign fileinfo->is_participate array. */
	tmp_participate_node = strdup(fileinfo->participate_node);
	name = strtok(tmp_participate_node,",");
	while (name)
	{
		nodeidx = find_node_index(name);
		if (nodeidx == -1)
		{
			fprintf(errf, "Invalid nodeidx:-1 for node:%s\n", name);
			exit(1);
		}
		fileinfo->is_participate[nodeidx] = true;

		name = strtok(NULL, ",");
	}

	if (tmp_participate_node)
	{
		free(tmp_participate_node);
	}

	/* append new file info to linklist */
	if (NULL == head_file_info)
	{
		head_file_info = last_file_info = fileinfo;
	}
	else
	{
		last_file_info->next = fileinfo;
		last_file_info = fileinfo;
	}

	return fileinfo;
}

static void
addTwoPhasefileInfo(int node_idx, TwoPhaseFileDetail *detail)
{
	TwoPhaseFileInfo *fileinfo = NULL;
	detail->nodename = strdup(pgxc_clean_node_info[node_idx].node_name);
	detail->nodeidx = node_idx;

	fileinfo = find_file_info(detail->gid);

	/* first filedetail to gid organized filedetail list */
	if (!fileinfo)
	{
		make_file_info(detail);
		return;
	}

	if (detail->prepared_at <= 0)
	{
		fprintf(errf, "ERROR: invalid prepared time in detail:"INT64_FORMAT"\n", detail->prepared_at);
		exit(1);
	}
	fileinfo->prepare_time = Min(fileinfo->prepare_time, detail->prepared_at);

	/* some check. */
	if (strcmp(fileinfo->participate_node, detail->participate_nodes) != 0)
	{
		fprintf(errf, "ERROR: participate_node check failed. In fileinfo:%s, in detail:%s\n", fileinfo->participate_node, detail->participate_nodes);
		exit(1);
	}

	if (strcmp(fileinfo->gid, detail->gid) != 0)
	{
		fprintf(errf, "ERROR: gid check failed. In fileinfo:%s, in detail:%s\n", fileinfo->gid, detail->gid);
		exit(1);
	}

	if (fileinfo->commit_time != detail->commit_timestamp)
	{
		fprintf(errf, "ERROR: commit_time check failed. In fileinfo:"INT64_FORMAT", in detail:"INT64_FORMAT"\n", fileinfo->commit_time, detail->commit_timestamp);
		exit(1);
	}

	// put detail into gid file list
	fileinfo->detail_last->next = detail;
	fileinfo->detail_last = detail;
	fileinfo->is_fileexist[detail->nodeidx] = true;

}

/* get 2pc file list and file detail on each cluster node. */
static void getTwoPhaseFileOfNode(PGconn *conn, int idx)
{
	int res_count;
	int ii;
	PGresult *res;
	ExecStatusType pq_status;
	char* filename = NULL;
	char* filelist = NULL;
	char *		   filedetail_str = NULL;
	char stmt[MAX_STMT_LEN];

	/* SQL Statement */
	static const char *STMT_GET_TWOPHASE_FILE_ON_NODE = "EXECUTE DIRECT ON (%s) '/*pgxc_clean*/ SELECT polardbx_get_2pc_filelist();'";

	sprintf(stmt, STMT_GET_TWOPHASE_FILE_ON_NODE, pgxc_clean_node_info[idx].node_name);

	res = PQexec(conn, stmt);
	if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Could not obtain twophase file list for node %s. errmsg:(%s)\n",
				pgxc_clean_node_info[idx].node_name, res ? PQresultErrorMessage(res) : "");
		PQclear(res);
		return;
	}

	res_count = PQntuples(res);
	if (res_count > 1)
	{
		fprintf(stderr, "ERROR: failed to get twophase file list for node %s. get incorrect row cnt:%d.\n",
				pgxc_clean_node_info[idx].node_name, res_count);
		exit(1);
	}
	for (ii = 0; ii < res_count; ii++)
	{
		filelist = strdup(PQgetvalue(res, ii, 0));
	}
	PQclear(res);

	/* get all file list on single node */
	int filecnt = getFileCount(filelist);
	char *filename_array[filecnt];
	filename = strtok(filelist,",");
	ii = 0;
	while (filename)
	{
		fprintf(outf, "DEBUG: filename:%s of filelist:%s\n", filename, filelist);
		filename_array[ii] = strdup(filename);
		ii++;
		filename = strtok(NULL,",");
	}
	if (ii != filecnt)
	{
		fprintf(outf, "ERROR: filecnt:%d maybe wrong. ii:%d, filelist:%s\n", filecnt, ii, filelist);
		exit(1);
	}

	for (ii = 0; ii < filecnt; ii++)
	{
		char* file = filename_array[ii];
		fprintf(outf, "DEBUG: file:%s \n", file);
		filedetail_str			   = getTwoPhaseFileDetail(conn, idx, file);
		TwoPhaseFileDetail *detail = (TwoPhaseFileDetail*)malloc(sizeof(TwoPhaseFileDetail));
		memset(detail, 0, sizeof(TwoPhaseFileDetail));
		detail->filename = strdup(file);
		parseTwoPhaseFileDetail(filedetail_str, detail);

		/* add single 2pc file to coresponding gid organized TwoPhaseFileInfo */
		addTwoPhasefileInfo(idx, detail);
	}

	if (filelist)
		free(filelist);
}

static void printSingleTwoPhase(TwoPhaseFileInfo* fileinfo)
{
	int i;

	fprintf(outf, "gid:%s\n", fileinfo->gid);
	fprintf(outf, "	global_status:%s\n", str_txn_stat(fileinfo->global_status));
	fprintf(outf, "	participate_node:%s\n", fileinfo->participate_node);
	fprintf(outf, "	is_participate list:\n");
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (fileinfo->is_participate[i])
		{
			fprintf(outf, "nodename:%s	status:%s\n", pgxc_clean_node_info[i].node_name, str_txn_stat(fileinfo->status[i]));
		}
	}
	fprintf(outf, "	is_fileexist list:\n");
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (fileinfo->is_fileexist[i])
		{
			fprintf(outf, "		%s\n", pgxc_clean_node_info[i].node_name);
		}
	}

	fprintf(outf, "	prepare_time:"INT64_FORMAT"\n", fileinfo->prepare_time);
	fprintf(outf, "	commit_time:"INT64_FORMAT"\n", fileinfo->commit_time);
}


static void printTwoPhaseFileList(void)
{
	TwoPhaseFileInfo *curr = head_file_info;
	fprintf(outf, "TwoPhaseFileList:\n");
	for (; curr ;curr = curr->next)
	{
		printSingleTwoPhase(curr);
	}
}

static TXN_STATUS
getTxnStatusOnNode(PGconn *conn, char* gid, int node_idx, Timestamp* commit_timestamp)
{
	char *node_name;
	char stmt[1024];
	PGresult *res;
	char *res_s;

	static const char *STMT_FORM = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ SELECT pgxc_is_committed(''%s'');'";
	static const char *STMT_FORM_RUNNING = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ SELECT polardbx_get_transaction_status(''%s'');'";

	node_name = pgxc_clean_node_info[node_idx].node_name;
	sprintf(stmt, STMT_FORM, node_name, gid);

	res = PQexec(conn, stmt);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK ||
		PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		sprintf(stmt, STMT_FORM_RUNNING, node_name, gid);
		res = PQexec(conn, stmt);
		if (res == NULL)
		{
			fprintf(errf, "Failed to exec stmt:%s, res is NULL. set txn status to TXN_STATUS_UNKNOWN.\n",
					stmt);
			return TXN_STATUS_UNKNOWN;
		}

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(errf, "Failed to exec stmt:%s, errmsg:%s. set txn status to TXN_STATUS_UNKNOWN.\n",
					stmt,
					res == NULL ? "res is null" : PQresultErrorMessage(res));
			if (strstr(PQresultErrorMessage(res), twophase_file_not_found_str))
			{
				fprintf(outf, "no 2pc file found, node hasn't received prepare cmd. set txn status to TXN_STATUS_ABORTED.\n");
				return TXN_STATUS_ABORTED;
			}

			return TXN_STATUS_UNKNOWN;
		}

		if (PQgetisnull(res, 0, 0))
		{
			fprintf(errf, "get null output of exec stmt:%s, set txn status to TXN_STATUS_UNKNOWN.\n",
					STMT_FORM_RUNNING);
			return TXN_STATUS_UNKNOWN;
		}

		res_s = PQgetvalue(res, 0, 0);
		if (strcmp(res_s, "t") == 0)
		{
			return TXN_STATUS_INPROGRESS;
		}
		else
		{
			fprintf(errf, "gid:%s on node:%s status invalid.\n", gid, node_name);
			exit(-1);
		}

		fprintf(errf, "set txn status to TXN_STATUS_UNKNOWN.\n");
		return TXN_STATUS_UNKNOWN;
	}

	res_s = PQgetvalue(res, 0, 0);
	if (strcmp(res_s, "t") == 0)
	{
		// If status is commited, there must have valid commit timestamp on datanode.
		if (commit_timestamp && (*commit_timestamp == InvalidGlobalTimestamp))
		{
			*commit_timestamp = getCommitTimestamp(conn, gid, node_name);
		}
		return TXN_STATUS_COMMITTED;
	}
	else if (strcmp(res_s, "f") == 0)
	{
		return TXN_STATUS_ABORTED;
	}
	else
	{
		fprintf(errf, "gid:%s on node:%s status invalid.", gid, node_name);
		exit(-1);
	}

	return TXN_STATUS_UNKNOWN;
}


static void getSingleFileStatusOnOtherNodes(PGconn *conn, TwoPhaseFileInfo* fileinfo)
{
	int i;

	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (fileinfo->is_participate[i])
		{
			fileinfo->status[i] = getTxnStatusOnNode(conn, fileinfo->gid, i, NULL);
		}
	}
}


static void getFileStatusOnOtherNodesAll(PGconn *conn)
{
	TwoPhaseFileInfo *curr = head_file_info;
	for(; curr; curr = curr->next)
	{
		getSingleFileStatusOnOtherNodes(conn, curr);
	}
}

/*
 * Scenario which twophase file residue may happen.
 * 1. When commit 2pc transaction, some nodes or all nodes failed to execute polardbx_finish_global_transation operation.
 * 2. When rollback 2pc transaction, some nodes or all nodes failed tp delete 2pc file.
 * 3. Some 2pc file may responding to on-going transaction, it is normal case.
 * */
static TXN_STATUS checkSingleFileGlobalStatus(TwoPhaseFileInfo* fileinfo)
{
	int check_flag = 0;
	int nodeindx;

	if (fileinfo == NULL)
		return TXN_STATUS_INITIAL;

	for (nodeindx = 0; nodeindx < pgxc_clean_node_count; nodeindx++)
	{
		if (fileinfo->is_participate[nodeindx] == 0)
		{
			fprintf(outf, "Txn gid:%s, node:%s is not participated.\n", fileinfo->gid, pgxc_clean_node_info[nodeindx].node_name);
			continue;
		}

		if (fileinfo->status[nodeindx] == TXN_STATUS_INITIAL ||
			fileinfo->status[nodeindx] == TXN_STATUS_UNKNOWN)
		{
			fprintf(errf, "Txn gid:%s has invalid txn_stat:%s on node:%s\n", fileinfo->gid, str_txn_stat(fileinfo->status[nodeindx]), pgxc_clean_node_info[nodeindx].node_name);
			exit(-1);
		}
		else if (fileinfo->status[nodeindx] == TXN_STATUS_COMMITTED)
			check_flag |= TXN_COMMITTED;
		else if (fileinfo->status[nodeindx] == TXN_STATUS_ABORTED)
			check_flag |= TXN_ABORTED;
		else if (fileinfo->status[nodeindx] == TXN_STATUS_INPROGRESS)
			check_flag |= TXN_INPROGRESS;
		else
		{
			fprintf(errf, "Txn gid:%s has invalid txn_stat:%s on node:%s\n", fileinfo->gid, str_txn_stat(fileinfo->status[nodeindx]), pgxc_clean_node_info[nodeindx].node_name);
			exit(-1);
			return TXN_STATUS_FAILED;
		}

	}

	if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
	{
		fprintf(errf, "FATAL: Txn gid:%s has both committed and aborted txn on all node.\n", fileinfo->gid);
		exit(-1);
		/* Mix of committed and aborted. This should not happen. */
		return TXN_STATUS_FAILED;
	}

	if ((check_flag & TXN_COMMITTED))
	{
		/* Some 2PC transactions are committed.  Need to commit others. */
		return TXN_STATUS_COMMITTED;
	}
	if (check_flag & TXN_ABORTED)
		/* Some 2PC transactions are aborted.  Need to abort others. */
		return TXN_STATUS_ABORTED;

	if (check_flag & TXN_INPROGRESS)
	{
		fprintf(outf, "Txn gid:%s doesn't have committed or aborted xact on all nodes. Can't decide next action.\n", fileinfo->gid);
		return TXN_STATUS_INPROGRESS;
	}

	return TXN_STATUS_UNKNOWN;
}

static void checkFileGlobalStatus(PGconn *conn)
{
	TwoPhaseFileInfo *curr = head_file_info;
	for(; curr; curr = curr->next)
	{
		curr->global_status = checkSingleFileGlobalStatus(curr);
	}
}

static void doDeleteTwoPhaseFile(PGconn *conn, char *gid, int nodeidx)
{
	char* EXEC_DIRECT_CLEANUP_STMT = "EXECUTE DIRECT ON (%s) '/* pgxc_clean */ select polardbx_finish_global_transation(''%s'');'";

	char *stmt = (char *) malloc (128 + strlen(gid));
	PGresult *res;
	char* res_s;
	ExecStatusType res_status;

	if (verbose_opt)
		fprintf(outf, "    cleaning2pcfile... \n");

	sprintf(stmt, EXEC_DIRECT_CLEANUP_STMT,
			pgxc_clean_node_info[nodeidx].node_name,
			gid);
	res = PQexec(conn, stmt);
	res_status = PQresultStatus(res);
	if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
	{
		fprintf(outf, "exec stmt:%s failed on node:%s, errmsg:%s\n",
				stmt, pgxc_clean_node_info[nodeidx].node_name, PQresultErrorMessage(res));
		return;
	}
	res_s = PQgetvalue(res, 0, 0);
	fprintf(outf, "exec stmt:%s on node:%s, get return value:%s\n", stmt, pgxc_clean_node_info[nodeidx].node_name, res_s);

	PQclear(res);
}

static void deleteSingleTwoPhaseFile(PGconn *conn, TwoPhaseFileInfo *fileinfo)
{
	int i;
	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		if (fileinfo->is_fileexist[i])
		{
			doDeleteTwoPhaseFile(conn, fileinfo->gid, i);
		}
	}
}

static bool reachTwoPhaseFileCleanInterval(Timestamp prepare_time)
{
	Timestamp prepare = prepare_time / 1000000 + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);; // cast to second from 19700101.

	struct timespec wall;
	Timestamp now_time;
	Timestamp diff_time;
	/* Use monotonic clock to avoid clock jump back */
	clock_gettime(CLOCK_REALTIME, &wall);
	now_time = wall.tv_sec;

	diff_time = now_time - prepare;
	if (diff_time > min_clean_file_interval)
	{
		fprintf(outf, "now_time:"INT64_FORMAT ", prepare:"INT64_FORMAT", diff_time:"INT64_FORMAT
					  ", min_clean_file_interval:%d, should clean 2pc file.\n",
			now_time,prepare, diff_time, min_clean_file_interval);
		return true;
	}
	else
	{
		fprintf(outf, "now_time:"INT64_FORMAT ", prepare:"INT64_FORMAT", diff_time:"INT64_FORMAT
		", min_clean_file_interval:%d, should NOT clean 2pc file.\n",
			now_time,prepare, diff_time, min_clean_file_interval);
		return false;
	}

	return true;
}


static void deleteTwoPhaseFile(PGconn *conn)
{
	TwoPhaseFileInfo *curr = head_file_info;
	for (; curr; curr = curr->next)
	{
		fprintf(outf, "process gid:%s", curr->gid);
		if (reachTwoPhaseFileCleanInterval(curr->prepare_time) && (curr->global_status == TXN_STATUS_ABORTED ||
			curr->global_status == TXN_STATUS_COMMITTED))
		{
			deleteSingleTwoPhaseFile(conn, curr);
		}

	}
}

static void getTwoPhaseFileListOnDisk(PGconn *coord_conn)
{
	int ii;
	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		getTwoPhaseFileOfNode(coord_conn, ii);
	}
}

static int getFileCount(char* filelist)
{
	char *p = filelist;
	int filecnt = 1;

	if (filelist == NULL || filelist[0] == '\0')
	{
		filecnt = 0;
		return filecnt;
	}

	for (; *p != '\0'; p++)
	{
		if (*p == ',')
		{
			filecnt++;
		}
	}

	return filecnt;
}


static void
do_commit_abort_simple_clean_mode(PGconn *conn, txn_info *txn, char *database_name, bool is_commit)
{
    int ii;
    bool has_error = false;

    char* EXEC_DIRECT_COMMIT_STMT = "/* pgxc_clean simple_clean_mode */ COMMIT PREPARED '%s';";
    char* EXEC_DIRECT_ABORT_STMT  = "/* pgxc_clean simple_clean_mode */ ROLLBACK PREPARED '%s';";

    char *stmt = (char *) malloc (128 + strlen(txn->gid));
    PGresult *res;
    ExecStatusType res_status;
    

    if (verbose_opt)
        fprintf(outf, "    %s... \n", is_commit ? "committing" : "aborting");
    for (ii = 0; ii < pgxc_clean_node_count; ii++)
    {
        memset(stmt, 0, 128 + strlen(txn->gid));
        if (txn->txn_stat[ii] == TXN_STATUS_PREPARED)
        {
            PGconn *node_conn = loginDatabase(pgxc_clean_node_info[ii].host, pgxc_clean_node_info[ii].port, username, password,
                                              database_name,
                                              progname, "auto", password_prompt);
            if (txn->nodeparts[ii] == 0)
            {
                fprintf(errf, "Txn gid:%s node:%s is expected to participate in 2pc.\n", txn->gid, pgxc_clean_node_info[ii].node_name);
                exit(-1);
            }
            if (txn->commit_timestamp == InvalidGlobalTimestamp && is_commit)
            {
                fprintf(errf, "Txn gid:%s has no commit timestamp.\n", txn->gid);
                exit(-1);
            }

            if (is_commit)
            {
                sprintf(stmt, EXEC_DIRECT_COMMIT_STMT, txn->gid);
            }
            else
            {
                sprintf(stmt, EXEC_DIRECT_ABORT_STMT, txn->gid);
            }

            res = PQexec(node_conn, stmt);
            res_status = PQresultStatus(res);
            if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
            {
                fprintf(errf, "exec stmt:%s failed (%s), errmsg:%s, has_error set to true.\n",
                        stmt, PQresultErrorMessage(res), pgxc_clean_node_info[ii].node_name);
                has_error = true;
            }
            if (verbose_opt)
            {
                if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
                    fprintf(outf, "exec stmt:%s succeeded (%s).\n", stmt, pgxc_clean_node_info[ii].node_name);
                else
                    fprintf(outf, "exec stmt:%s failed (%s: %s).\n",
                            stmt,
                            pgxc_clean_node_info[ii].node_name,
                            PQresultErrorMessage(res));
            }
            else
            {
                if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK)
                {
                    fprintf(errf, "Failed to recover TXN, gid: %s, owner: \"%s\", node: \"%s\", stmt:%s, (%s)\n",
                            txn->gid, txn->owner, pgxc_clean_node_info[ii].node_name,
                            stmt, PQresultErrorMessage(res));
                }
            }
            PQclear(res);
            PQfinish(node_conn);
        }
    }

    if (has_error)
    {
        fprintf(errf, "has_error when %s txn, gid:%s. so skip clear 2pc files.\n", is_commit ? "commit" : "abort", txn->gid);
        free(stmt);
        return;
    }
    
    free(stmt);
}


/* This func is called in simple clean mode. Since it use pg_prepared_xacts instead of POLARDBX_PREPARED_XACTS */
static void
getPreparedTxnListOfNodeSimpleCleanMode(PGconn *conn, int idx)
{
    int prep_txn_count;
    int ii;
    PGresult *res;
    ExecStatusType pq_status;

#define MAX_STMT_LEN 1024

    /* SQL Statement */
    static const char *STMT_GET_PREP_TXN_ON_NODE
            = "/* pgxc_clean simple_clean_mode */ SELECT TRANSACTION::text, GID::text, OWNER::text, DATABASE::text, "
              " prepared::text, now()::text FROM PG_PREPARED_XACTS;";
    char stmt[MAX_STMT_LEN];

    strcpy(stmt, STMT_GET_PREP_TXN_ON_NODE);

    PGconn *node_conn = loginDatabase(pgxc_clean_node_info[idx].host, pgxc_clean_node_info[idx].port, username, password,
                                      clean_all_databases ? "postgres" : head_database_names->database_name,
                                      progname, "auto", password_prompt);


    res = PQexec(node_conn, stmt);
    if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Could not obtain prepared transaction list for node %s.(%s)\n",
                pgxc_clean_node_info[idx].node_name, res ? PQresultErrorMessage(res) : "");
        PQclear(res);
        //exit (1);
        return;
    }
    prep_txn_count = PQntuples(res);
    total_prep_txn_count += prep_txn_count;
    for (ii = 0; ii < prep_txn_count; ii++)
    {
        TransactionId xid;
        char *gid;
        char *owner;
        char *database_name;
        char *prepare_time;
        char *now_time;

        xid = atoi(PQgetvalue(res, ii, 0));
        gid = strdup(PQgetvalue(res, ii, 1));
        owner = strdup(PQgetvalue(res, ii, 2));
        database_name = strdup(PQgetvalue(res, ii, 3));
        prepare_time = strdup(PQgetvalue(res, ii, 4));
        now_time = strdup(PQgetvalue(res, ii, 5));

        fprintf(outf, "nodename:%s, get gid:%s, xid:%d, owner:%s, database_name:%s, prepare_time:%s, now_time:%s.\n",
                pgxc_clean_node_info[idx].node_name, gid, xid, owner, database_name, prepare_time, now_time);
        add_txn_info(database_name, pgxc_clean_node_info[idx].node_name, gid, xid, owner, NULL,
                     INVALID_INT_VALUE,
                     TXN_STATUS_PREPARED);

        if (gid)
            free(gid);
        if (owner)
            free(owner);
        if (database_name)
            free(database_name);
        if (prepare_time)
            free(prepare_time);
        if (now_time)
            free(now_time);
    }
    PQclear(res);
    PQfinish(node_conn);
}