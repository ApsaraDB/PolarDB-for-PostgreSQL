#include "pgreplay.h"
#include "uthash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#ifdef HAVE_SYS_SELECT_H
#	include <sys/select.h>
#else
#	include <sys/types.h>
#	include <unistd.h>
#endif
#ifdef TIME_WITH_SYS_TIME
#	include <sys/time.h>
#	include <time.h>
#else
#	ifdef HAVE_SYS_TIME_H
#		include <sys/time.h>
#	else
#		include <time.h>
#	endif
#endif
#ifdef WINDOWS
#	include <windows.h>
#endif

/*
 * Utility macros to calculate with struct timeval.
 * These are already defined on BSD type systems.
 */

#ifndef timeradd
#	define timeradd(a, b, result)  \
	do {  \
		(result)->tv_sec = (a)->tv_sec + (b)->tv_sec;  \
		(result)->tv_usec = (a)->tv_usec + (b)->tv_usec;  \
		if ((result)->tv_usec >= 1000000) {  \
			++(result)->tv_sec;  \
			(result)->tv_usec -= 1000000;  \
		}  \
	} while (0)
#endif

#ifndef timersub
#	define timersub(a, b, result)  \
	do {  \
		(result)->tv_sec = (a)->tv_sec - (b)->tv_sec;  \
		(result)->tv_usec = (a)->tv_usec - (b)->tv_usec;  \
		if ((result)->tv_usec < 0) {  \
			--(result)->tv_sec;  \
			(result)->tv_usec += 1000000;  \
		}  \
	} while (0)
#endif

extern int monitor_connect_finish(void);

/* connect string */
static char *conn_string;

/* speed factor for replay */
static double replay_factor;

/* possible stati a connection can have */
typedef enum {
	idle = 0,
	conn_wait_write,
	conn_wait_read,
	wait_write,
	wait_read,
	closed
} connstatus;

/* prepare hash table */
typedef struct prepare_item{
    char *name;          /* key */
    int id;
    UT_hash_handle hh;         /* makes this structure hashable */
}prepare_item;

/* linked list element for list of open connections */
struct dbconn {
	uint64_t       session_id;
	PGconn         *db_conn;
	int            socket;
	connstatus     status;
	struct timeval session_start;
	struct timeval stmt_start;
	char           *errmsg;
	char		   *search_path;
	prepare_item   *prepare_hash;
	struct dbconn  *next;
};
typedef struct dbconn dbconn;

/* linked list of open connections */
static struct dbconn *connections = NULL;

/* linked list of open connections */
PGconn *monitor_conn = NULL;

/* remember timestamp of program start and stop */
static struct timeval start_time;
static struct timeval stop_time;

/* remember timestamp of first statement */
static struct timeval first_stmt_time;
static struct timeval last_stmt_time;

/* maximum seconds behind schedule */
static time_t secs_behind = 0;

/* time skipped instead of sleeping through it */
static struct timeval jump_total = {0, 0};

/* statistics */
static struct timeval stat_exec = {0, 0};     /* SQL statement execution time */
static struct timeval stat_session = {0, 0};  /* session duration total */
static struct timeval stat_longstmt = {0, 0}; /* session duration total */
static unsigned long stat_stmt = 0;           /* number of SQL statements */
static unsigned long stat_prep = 0;           /* number of preparations */
static unsigned long stat_errors = 0;         /* unsuccessful SQL statements and preparations */
static unsigned long stat_actions = 0;        /* client-server interactions */
static unsigned long stat_statements = 0;     /* number of concurrent statements */
static unsigned long stat_stmtmax = 0;        /* maximum concurrent statements */
static unsigned long stat_sesscnt = 0;        /* total number of sessions */
static unsigned long stat_sessions = 0;       /* number of concurrent sessions */
static unsigned long stat_sessmax = 0;        /* maximum concurrent sessions */
static unsigned long stat_hist[5] = {0, 0, 0, 0, 0};  /* duration histogram */
static unsigned long old_stat_hist[5] = {0, 0, 0, 0, 0};  /* segment duration histogram */

static PGresult* old_result = NULL;
static unsigned long old_stat_stmt = 0;
static unsigned long old_stat_errors = 0;

#define NUM_DELAY_STEPS 11

/* steps for execution delay reports */
static struct {
	int seconds;
	char *display;
	short int shown;
} delay_steps[NUM_DELAY_STEPS] = {
	{10, "10 seconds", 0},
	{30, "30 seconds", 0},
	{60, "1 minute", 0},
	{180, "3 minutes", 0},
	{600, "10 minutes", 0},
	{1800, "30 minutes", 0},
	{3600, "1 hour", 0},
	{7200, "2 hours", 0},
	{21600, "6 hours", 0},
	{43200, "12 hours", 0},
	{86400, "1 day", 0}
};

/* processes (ignores) notices from the server */
static void ignore_notices(void *arg, const PGresult *res) {
}

/* encapsulates "select" call and error handling */

static int do_select(int n, fd_set *rfds, fd_set *wfds, fd_set *xfds, struct timeval *timeout) {
	int rc;

	rc = select(n, rfds, wfds, xfds, timeout);
#ifdef WINDOWS
	if (SOCKET_ERROR == rc) {
		win_perror("Error in select()", 1);
		rc = -1;
	}
#else
	if (-1 == rc) {
		perror("Error in select()");
	}
#endif

	return rc;
}

/* checks if a certain socket can be read or written without blocking */

static int poll_socket(int socket, int for_read, char * const errmsg_prefix) {
	fd_set fds;
	struct timeval zero = { 0, 0 };

	FD_ZERO(&fds);
	FD_SET(socket, &fds);
	return do_select(socket + 1, for_read ? &fds : NULL, for_read ? NULL : &fds, NULL, &zero);
}

/* sleep routine that should work on all platforms */

static int do_sleep(struct timeval *delta) {
	debug(2, "Napping for %lu.%06lu seconds\n", (unsigned long)delta->tv_sec, (unsigned long)delta->tv_usec);
#ifdef WINDOWS
	Sleep((DWORD)delta->tv_sec * 1000 + (DWORD)(delta->tv_usec / 1000));
	return 0;
#else
	return do_select(0, NULL, NULL, NULL, delta);
#endif
}

/* 
set search_path of the connection
 */
static int set_search_path(const char * search_path, PGconn * conn){
	char * set_path = malloc(strlen(search_path) + strlen("set search_path = ;") + 1);
	PGresult* res;
	sprintf(set_path,"set search_path = %s;",search_path);
	res = PQexec(conn, set_path);
	if(PQresultStatus(res) != PGRES_COMMAND_OK){
		fprintf(stderr, "set_search_path: Query execution failed search: %s\n", PQerrorMessage(conn));
	}
	PQclear(res);
	free(set_path);
	return 0;
}

/* add prepare cmd */
static int set_prepare_cmd(const char * prepare_cmd, PGconn * conn){
	PGresult* res = PQexec(conn, prepare_cmd);
	if(PQresultStatus(res) != PGRES_COMMAND_OK){
		fprintf(stderr, "set_prepare_cmd: Query execution failed search: %s\n", PQerrorMessage(conn));
	}
	PQclear(res);
	return 0;
}

static void print_replay_statistics(int dry_run) {
	int hours, minutes;
	double seconds, runtime, session_time, busy_time;
	struct timeval delta;
	unsigned long histtotal =
		stat_hist[0] + stat_hist[1] + stat_hist[2] + stat_hist[3] + stat_hist[4];

	if (dry_run) {
		fprintf(sf, "\nReplay statistics (dry run)\n");
		fprintf(sf, "===========================\n\n");

		/* calculate lengh of the recorded workload */
		timersub(&last_stmt_time, &first_stmt_time, &delta);
		hours = delta.tv_sec / 3600;
		delta.tv_sec -= hours * 3600;
		minutes = delta.tv_sec / 60;
		delta.tv_sec -= minutes * 60;
		seconds = delta.tv_usec / 1000000.0 + delta.tv_sec;

		fprintf(sf, "Duration of recorded workload:");
		if (hours > 0) {
			fprintf(sf, " %d hours", hours);
		}
		if (minutes > 0) {
			fprintf(sf, " %d minutes", minutes);
		}
		fprintf(sf, " %.3f seconds\n", seconds);
		fprintf(sf, "Calls to the server: %lu\n", stat_actions);
	} else {
		fprintf(sf, "\nReplay statistics\n");
		fprintf(sf, "=================\n\n");

		/* calculate total run time */
		timersub(&stop_time, &start_time, &delta);
		runtime = delta.tv_usec / 1000000.0 + delta.tv_sec;
		/* calculate hours and minutes, subtract from delta */
		hours = delta.tv_sec / 3600;
		delta.tv_sec -= hours * 3600;
		minutes = delta.tv_sec / 60;
		delta.tv_sec -= minutes * 60;
		seconds = delta.tv_usec / 1000000.0 + delta.tv_sec;
		/* calculate total busy time */
		busy_time = stat_exec.tv_usec / 1000000.0 + stat_exec.tv_sec;
		/* calculate total session time */
		session_time = stat_session.tv_usec / 1000000.0 + stat_session.tv_sec;

		fprintf(sf, "Speed factor for replay: %.3f\n", replay_factor);
		fprintf(sf, "Total run time:");
		if (hours > 0) {
			fprintf(sf, " %d hours", hours);
		}
		if (minutes > 0) {
			fprintf(sf, " %d minutes", minutes);
		}
		fprintf(sf, " %.3f seconds\n", seconds);
		fprintf(sf, "Maximum lag behind schedule: %lu seconds\n", (unsigned long) secs_behind);
		fprintf(sf, "Calls to the server: %lu\n", stat_actions);
		if (runtime > 0.0) {
			fprintf(sf, "(%.3f calls per second)\n", stat_actions / runtime);
		}
	}

	fprintf(sf, "Total number of connections: %lu\n", stat_sesscnt);
	fprintf(sf, "Maximum number of concurrent connections: %lu\n", stat_sessmax);
	if (!dry_run && runtime > 0.0) {
		fprintf(sf, "Average number of concurrent connections: %.3f\n", session_time / runtime);
	}
	if (!dry_run && session_time > 0.0) {
		fprintf(sf, "Average session idle percentage: %.3f%%\n", 100.0 * (session_time - busy_time) / session_time);
	}

	fprintf(sf, "SQL statements executed: %lu\n", stat_stmt - stat_prep);
	if (!dry_run && stat_stmt > stat_prep) {
		fprintf(sf, "(%lu or %.3f%% of these completed with error)\n",
			stat_errors, (100.0 * stat_errors) / (stat_stmt - stat_prep));
		fprintf(sf, "Maximum number of concurrent SQL statements: %lu\n", stat_stmtmax);
		if (runtime > 0.0) {
			fprintf(sf, "Average number of concurrent SQL statements: %.3f\n", busy_time / runtime);
		}
		fprintf(sf, "Average SQL statement duration: %.3f seconds\n", busy_time / stat_stmt);
		fprintf(sf, "Maximum SQL statement duration: %.3f seconds\n",
			stat_longstmt.tv_sec + stat_longstmt.tv_usec / 1000000.0);
		fprintf(sf, "Statement duration histogram:\n");
		fprintf(sf, "  0    to 0.02 seconds: %.3f%%\n", 100.0 * stat_hist[0] / histtotal);
		fprintf(sf, "  0.02 to 0.1  seconds: %.3f%%\n", 100.0 * stat_hist[1] / histtotal);
		fprintf(sf, "  0.1  to 0.5  seconds: %.3f%%\n", 100.0 * stat_hist[2] / histtotal);
		fprintf(sf, "  0.5  to 2    seconds: %.3f%%\n", 100.0 * stat_hist[3] / histtotal);
		fprintf(sf, "     over 2    seconds: %.3f%%\n", 100.0 * stat_hist[4] / histtotal);
	}
}

int database_consumer_init(const char *ignore, const char *host, int port, const char *passwd, double factor) {
	int conn_string_len = 12;  /* port and '\0' */
	const char *p;
	char *p1;

	debug(3, "Entering database_consumer_init%s\n", "");

	/* get time of program start */
	if (-1 == gettimeofday(&start_time, NULL)) {
		perror("Error calling gettimeofday");
		return 0;
	}

	replay_factor = factor;

	/* calculate length of connect string */
	if (host) {
		conn_string_len += 8;
		for (p=host; '\0'!=*p; ++p) {
			if (('\'' == *p) || ('\\' == *p)) {
				conn_string_len += 2;
			} else {
				++conn_string_len;
			}
		}
	}
	if (passwd) {
		conn_string_len += 12;
		for (p=passwd; '\0'!=*p; ++p) {
			if (('\'' == *p) || ('\\' == *p)) {
				conn_string_len += 2;
			} else {
				++conn_string_len;
			}
		}
	}

	if (extra_connstr)
		conn_string_len += strlen(extra_connstr);

	if (NULL == (conn_string = malloc(conn_string_len))) {
		fprintf(stderr, "Cannot allocate %d bytes of memory\n", conn_string_len);
		return 0;
	}
	/* write the port to the connection string if it is set */
	if (-1 == port) {
		conn_string[0] = '\0';
	} else {
		if (sprintf(conn_string, "port=%d", port) < 0) {
			perror("Error writing connect string:");
			free(conn_string);
			return 0;
		}
	}
	for (p1=conn_string; '\0'!=*p1; ++p1) {
		/* places p1 at the end of the string */
	}

	/* append host if necessary */
	if (host) {
		*(p1++) = ' ';
		*(p1++) = 'h';
		*(p1++) = 'o';
		*(p1++) = 's';
		*(p1++) = 't';
		*(p1++) = '=';
		*(p1++) = '\'';
		for (p=host; '\0'!=*p; ++p) {
			if (('\'' == *p) || ('\\' == *p)) {
				*(p1++) = '\\';
			}
			*(p1++) = *p;
		}
		*(p1++) = '\'';
		*p1 = '\0';
	}

	/* append password if necessary */
	if (passwd) {
		*(p1++) = ' ';
		*(p1++) = 'p';
		*(p1++) = 'a';
		*(p1++) = 's';
		*(p1++) = 's';
		*(p1++) = 'w';
		*(p1++) = 'o';
		*(p1++) = 'r';
		*(p1++) = 'd';
		*(p1++) = '=';
		*(p1++) = '\'';
		for (p=passwd; '\0'!=*p; ++p) {
			if (('\'' == *p) || ('\\' == *p)) {
				*(p1++) = '\\';
			}
			*(p1++) = *p;
		}
		*(p1++) = '\'';
		*p1 = '\0';
	}

	if (extra_connstr) {
		*(p1++) = ' ';
		strcpy(p1, extra_connstr);
	}

	debug(2, "Database connect string: \"%s\"\n", conn_string);

	debug(3, "Leaving database_consumer_init%s\n", "");
	return 1;
}

void database_consumer_finish(int dry_run) {
	debug(3, "Entering database_consumer_finish%s\n", "");

	free(conn_string);

	if (NULL != connections) {
		fprintf(stderr, "Error: not all database connections closed\n");
	}

	if (-1 == gettimeofday(&stop_time, NULL)) {
		perror("Error calling gettimeofday");
	} else if (sf) {
		print_replay_statistics(dry_run);
	}

	debug(3, "Leaving database_consumer_finish%s\n", "");
}

int database_consumer(replay_item *item) {
	const uint64_t session_id = replay_get_session_id(item);
	const replay_type type = replay_get_type(item);
	int all_idle = 1, rc = 0, j;
	struct dbconn *conn = connections, *found_conn = NULL, *prev_conn = NULL;
	struct timeval target_time, now, delta;
	const struct timeval *stmt_time;
	static int fstmtm_set = 0;  /* have we already collected first_statement_time */
	double d;
	time_t i;
	char *connstr, *p1, errbuf[256];
	const char *user, *database, *p;
	PGcancel *cancel_request;
	PGresult *result;
	ExecStatusType result_status;
	// const char* search_path,*params_typename,*source_text;

	debug(3, "Entering database_consumer%s\n", "");

	/* loop through open connections and do what can be done */
	while ((-1 != rc) && (NULL != conn)) {
		/* if we find the connection for the current statement, remember it */
		if (session_id == conn->session_id) {
			found_conn = conn;
		}

		/* handle each connection according to status */
		switch(conn->status) {
			case idle:
			case closed:
				break;  /* nothing to do */

			case conn_wait_read:
			case conn_wait_write:
				/* in connection process */
				/* check if socket is still busy */
				switch (poll_socket(conn->socket, (conn_wait_read == conn->status), "Error polling socket during connect")) {
					case 0:
						/* socket still busy */
						debug(2, "Socket for session 0x" UINT64_FORMAT " busy for %s during connect\n", conn->session_id, (conn_wait_write == conn->status) ? "write" : "read");
						all_idle = 0;
						break;
					case 1:
						/* socket not busy, continue connect process */
						switch(PQconnectPoll(conn->db_conn)) {
							case PGRES_POLLING_WRITING:
								conn->status = conn_wait_write;
								all_idle = 0;
								break;
							case PGRES_POLLING_READING:
								conn->status = conn_wait_read;
								all_idle = 0;
								break;
							case PGRES_POLLING_OK:
								debug(2, "Connection for session 0x" UINT64_FORMAT " established\n", conn->session_id);
								conn->status = idle;

								/* get session start time */
								if (-1 == gettimeofday(&(conn->session_start), NULL)) {
									perror("Error calling gettimeofday");
									rc = -1;
								}

								/* count total and concurrent sessions */
								++stat_sesscnt;
								if (++stat_sessions > stat_sessmax) {
									stat_sessmax = stat_sessions;
								}

								break;
							case PGRES_POLLING_FAILED:
								/* If the connection fails because of a
								   FATAL error from the server, mark
								   connection "closed" and keep going.
								   The same thing probably happened in the
								   original run.
								   PostgreSQL logs no disconnection for this.
								*/
								p1 = PQerrorMessage(conn->db_conn);
								if (0 == strncmp(p1, "FATAL:  ", 8)) {
									p1 += 8;
									if (NULL == (conn->errmsg = malloc(strlen(p1) + 1))) {
										fprintf(stderr, "Cannot allocate %lu bytes of memory\n", (unsigned long)(strlen(p1) + 1));
										rc = -1;
									} else {
										debug(2, "Connection for session 0x" UINT64_FORMAT " failed with FATAL error: %s\n",
											conn->session_id, p1);
										strcpy(conn->errmsg, p1);
										conn->status = closed;
										PQfinish(conn->db_conn);
									}

									break;
								}
								/* else fall through */
							default:
								fprintf(stderr, "Connection for session 0x" UINT64_FORMAT " failed: %s\n", conn->session_id, PQerrorMessage(conn->db_conn));
								rc = -1;
								PQfinish(conn->db_conn);
						}
						break;
					default:
						/* error happened in select() */
						rc = -1;
				}
				break;

			case wait_write:
				/* check if the socket is writable */
				switch (poll_socket(conn->socket, 0, "Error polling socket for write")) {
					case 0:
						/* socket still busy */
						debug(2, "Session 0x" UINT64_FORMAT " busy writing data\n", conn->session_id);
						all_idle = 0;
						break;
					case 1:
						/* try PQflush again */
						debug(2, "Session 0x" UINT64_FORMAT " flushing data\n", conn->session_id);
						switch (PQflush(conn->db_conn)) {
							case 0:
								/* finished flushing all data */
								conn->status = wait_read;
								all_idle = 0;
								break;
							case 1:
								/* more data to flush */
								all_idle = 0;
								break;
							default:
								fprintf(stderr, "Error flushing to database: %s\n", PQerrorMessage(conn->db_conn));
								rc = -1;
						}
						break;
					default:
						/* error in select() */
						rc = -1;
				}
				break;

			case wait_read:
				/* check if the socket is readable */
				switch (poll_socket(conn->socket, 1, "Error polling socket for read")) {
					case 0:
						/* socket still busy */
						debug(2, "Session 0x" UINT64_FORMAT " waiting for data\n", conn->session_id);
						all_idle = 0;
						break;
					case 1:
						/* read input from connection */
						if (! PQconsumeInput(conn->db_conn)) {
							fprintf(stderr, "Error reading from database: %s\n", PQerrorMessage(conn->db_conn));
							rc = -1;
						} else {
							/* check if we are done reading */
							if (PQisBusy(conn->db_conn)) {
								/* more to read */
								all_idle = 0;
							} else {
								/* read and discard all results */
								while (NULL != (result = PQgetResult(conn->db_conn))) {
									/* count statements and errors for statistics */
									++stat_stmt;
									result_status = PQresultStatus(result);
									debug(2, "Session 0x" UINT64_FORMAT " got got query response (%s)\n",
										conn->session_id,
										(PGRES_TUPLES_OK == result_status) ? "PGRES_TUPLES_OK" :
										((PGRES_COMMAND_OK == result_status) ? "PGRES_COMMAND_OK" :
										((PGRES_FATAL_ERROR == result_status) ? "PGRES_FATAL_ERROR" :
										((PGRES_NONFATAL_ERROR == result_status) ? "PGRES_NONFATAL_ERROR" :
										((PGRES_EMPTY_QUERY == result_status) ? "PGRES_EMPTY_QUERY" : "unexpected status")))));

									if ((PGRES_EMPTY_QUERY != result_status)
										&& (PGRES_COMMAND_OK != result_status)
										&& (PGRES_TUPLES_OK != result_status)
										&& (PGRES_NONFATAL_ERROR != result_status))
									{
										++stat_errors;
									}

									PQclear(result);
								}

								/* one less concurrent statement */
								--stat_statements;

								conn->status = idle;

								/* remember execution time for statistics */
								if (-1 == gettimeofday(&delta, NULL)) {
									perror("Error calling gettimeofday");
									rc = -1;
								} else {
									/* subtract statement start time */
									timersub(&delta, &(conn->stmt_start), &delta);

									/* add to duration histogram */
									if (0 == delta.tv_sec) {
										if (20000 >= delta.tv_usec) {
											++stat_hist[0];
										} else if (100000 >= delta.tv_usec) {
											++stat_hist[1];
										} else if (500000 >= delta.tv_usec) {
											++stat_hist[2];
										} else {
											++stat_hist[3];
										}
									} else if (2 > delta.tv_sec) {
										++stat_hist[3];
									} else {
										++stat_hist[4];
									}

									/* remember longest statement */
									if ((delta.tv_sec > stat_longstmt.tv_sec)
										|| ((delta.tv_sec == stat_longstmt.tv_sec)
											&& (delta.tv_usec > stat_longstmt.tv_usec)))
									{
										stat_longstmt.tv_sec = delta.tv_sec;
										stat_longstmt.tv_usec = delta.tv_usec;
									}

									/* add to total */
									timeradd(&stat_exec, &delta, &stat_exec);
								}
							}
						}
						break;
					default:
						/* error during select() */
						rc = -1;
				}
				break;
		}

		if (! found_conn) {
			/* remember previous item in list, useful for removing an item */
			prev_conn = conn;
		}

		conn = conn->next;
	}

	/* make sure we found a connection above (except for connect items) */
	if (1 == rc) {
		if ((pg_connect == type) && (NULL != found_conn)) {
			fprintf(stderr, "Error: connection for session 0x" UINT64_FORMAT " already exists\n", replay_get_session_id(item));
			rc = -1;
		} else if ((pg_connect != type) && (NULL == found_conn)) {
			fprintf(stderr, "Error: no connection found for session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));
			rc = -1;
		}
	}

	/* time when the statement originally ran */
	stmt_time = replay_get_time(item);
	last_stmt_time.tv_sec = stmt_time->tv_sec;
	last_stmt_time.tv_usec = stmt_time->tv_usec;

	/* set first_stmt_time if it is not yet set */
	if (! fstmtm_set) {
		first_stmt_time.tv_sec = stmt_time->tv_sec;
		first_stmt_time.tv_usec = stmt_time->tv_usec;

		fstmtm_set = 1;
	}

	/* get current time */
	if (-1 != rc) {
		if (-1 == gettimeofday(&now, NULL)) {
			fprintf(stderr, "Error: gettimeofday failed\n");
			rc = -1;
		}
	}

	/* determine if statement should already be consumed, sleep if necessary */
	if (-1 != rc) {
		/* calculate "target time" when item should be replayed:
		                                       statement time - first statement time
		   program start time - skipped time + -------------------------------------
		                                                   replay factor            */

		/* timestamp of the statement */
		target_time.tv_sec = stmt_time->tv_sec;
		target_time.tv_usec = stmt_time->tv_usec;

		/* subtract time of first statement */
		timersub(&target_time, &first_stmt_time, &target_time);

		/* subtract skipped time */
		if (jump_enabled) {
			timersub(&target_time, &jump_total, &target_time);
		}

		/* divide by replay_factor */
		if (replay_factor != 1.0) {
			/* - divide the seconds part by the factor
			   - divide the microsecond part by the factor and add the
			     fractional part (times 10^6) of the previous division
			   - if the result exceeds 10^6, subtract the excess and
			     add its 10^6th to the seconds part. */
			d = target_time.tv_sec / replay_factor;
			target_time.tv_sec = d;
			target_time.tv_usec = target_time.tv_usec / replay_factor +
				(d - target_time.tv_sec) * 1000000.0;
			i = target_time.tv_usec / 1000000;
			target_time.tv_usec -= i * 1000000;
			target_time.tv_sec += i;
		}

		/* add program start time */
		timeradd(&target_time, &start_time, &target_time);

		/* warn if we fall behind too much */
		if (secs_behind < now.tv_sec - target_time.tv_sec) {
			secs_behind = now.tv_sec - target_time.tv_sec;
			for (j=0; j<NUM_DELAY_STEPS; ++j) {
				if (! delay_steps[j].shown && delay_steps[j].seconds <= secs_behind) {
					printf("Execution is %s behind schedule\n", delay_steps[j].display);
					fflush(stdout);
					delay_steps[j].shown = 1;
				}
			}
		}

		if (((target_time.tv_sec > now.tv_sec) ||
				((target_time.tv_sec == now.tv_sec) && (target_time.tv_usec > now.tv_usec))) &&
				all_idle) {
			/* sleep or jump if all is idle and the target time is in the future */

			/* calculate time to sleep or jump (delta = target_time - now) */
			timersub(&target_time, &now, &delta);

			if (jump_enabled) {
				/* add the sleep time to jump_total */
				timeradd(&jump_total, &delta, &jump_total);
				debug(2, "Skipping %lu.%06lu seconds\n", (unsigned long)delta.tv_sec, (unsigned long)delta.tv_usec);
				/* then consume item */
				rc = 1;
			} else {
				/* sleep */
				if (-1 == do_sleep(&delta)) {
					rc = -1;
				} else {
					/* then consume item */
					rc = 1;
				}
			}
		} else if (((target_time.tv_sec < now.tv_sec) ||
				((target_time.tv_sec == now.tv_sec) && (target_time.tv_usec <= now.tv_usec))) &&
				((pg_connect == type) ||
				((pg_disconnect == type) && (closed == found_conn->status)) ||
				((pg_cancel == type) && (wait_read == found_conn->status)) ||
				(idle == found_conn->status))) {
			/* if the item is due and its connection is idle, consume it */
			/* cancel items will also be consumed if the connection is waiting for a resonse */
			rc = 1;
		} else if (found_conn && (closed == found_conn->status)) {
			fprintf(stderr, "Connection 0x" UINT64_FORMAT " failed with FATAL error: %s\n",
				found_conn->session_id, found_conn->errmsg);
			rc = -1;
		}
	}

	/* send statement */
	if (1 == rc) {
		/* count for statistics */
		++stat_actions;

		switch (type) {
			case pg_connect:
				debug(2, "Starting database connection for session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));

				/* allocate a connect string */
				user = replay_get_user(item);
				database = replay_get_database(item);
				if (NULL == (connstr = malloc(strlen(conn_string) + 2 * strlen(user) + 2 * strlen(database) + 18))) {
					fprintf(stderr, "Cannot allocate %lu bytes of memory\n", (unsigned long)strlen(conn_string) + 2 * strlen(user) + 2 * strlen(database) + 18);
					rc = -1;
				} else {
					/* append user and password */
					strcpy(connstr, conn_string);
					p1 = connstr + strlen(connstr);
					*(p1++) = ' ';
					*(p1++) = 'u';
					*(p1++) = 's';
					*(p1++) = 'e';
					*(p1++) = 'r';
					*(p1++) = '=';
					*(p1++) = '\'';
					for (p=user; '\0'!=*p; ++p) {
						if (('\'' == *p) || ('\\' == *p)) {
							*(p1++) = '\\';
						}
						*(p1++) = *p;
					}
					*(p1++) = '\'';
					*(p1++) = ' ';
					*(p1++) = 'd';
					*(p1++) = 'b';
					*(p1++) = 'n';
					*(p1++) = 'a';
					*(p1++) = 'm';
					*(p1++) = 'e';
					*(p1++) = '=';
					*(p1++) = '\'';
					for (p=database; '\0'!=*p; ++p) {
						if (('\'' == *p) || ('\\' == *p)) {
							*(p1++) = '\\';
						}
						*(p1++) = *p;
					}
					*(p1++) = '\'';
					*p1 = '\0';

					/* allocate a struct dbconn */
					if (NULL == (found_conn = malloc(sizeof(struct dbconn)))) {
						fprintf(stderr, "Cannot allocate %lu bytes of memory\n", (unsigned long)sizeof(struct dbconn));
						rc = -1;
					} else {
						/* initialize a connection */
						if (NULL == (found_conn->db_conn = PQconnectStart(connstr))) {
							fprintf(stderr, "Cannot allocate memory for database connection\n");
							rc = -1;
							free(found_conn);
						} else {
							if (CONNECTION_BAD == PQstatus(found_conn->db_conn)) {
								fprintf(stderr, "Error: connection to database failed: %s\n", PQerrorMessage(found_conn->db_conn));
								rc = -1;
								PQfinish(found_conn->db_conn);
								free(found_conn);
							} else {
								if (-1 == (found_conn->socket = PQsocket(found_conn->db_conn))) {
									fprintf(stderr, "Error: cannot get socket for database connection\n");
									rc = -1;
									PQfinish(found_conn->db_conn);
									free(found_conn);
								} else {
									/* set values in struct dbconn */

									found_conn->session_id = replay_get_session_id(item);
									found_conn->status = conn_wait_write;
									found_conn->errmsg = NULL;
									found_conn->next = connections;
									found_conn->search_path = malloc(POLARDBlEN);
									found_conn->prepare_hash = NULL;
									connections = found_conn;

									/* do not display notices */
									PQsetNoticeReceiver(found_conn->db_conn, ignore_notices, NULL);
								}
							}
						}
					}

					/* free connection sting */
					free(connstr);
				}
				break;
			case pg_disconnect:
				/* dead connections need not be closed */
				if (closed == found_conn->status) {
					debug(2, "Removing closed session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));
				} else {
					debug(2, "Disconnecting database connection for session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));

					PQfinish(found_conn->db_conn);

					/* remember session duration for statistics */
					if (-1 == gettimeofday(&delta, NULL)) {
						perror("Error calling gettimeofday");
						rc = -1;
					} else {
						/* subtract session start time */
						timersub(&delta, &(found_conn->session_start), &delta);

						/* add to total */
						timeradd(&stat_session, &delta, &stat_session);
					}

					/* one less concurrent session */
					--stat_sessions;
				}

				/* remove struct dbconn from linked list */
				if (prev_conn) {
					prev_conn->next = found_conn->next;
				} else {
					connections = found_conn->next;
				}
				if (found_conn->errmsg) {
					free(found_conn->errmsg);
				}
				if (found_conn->search_path) {
					free(found_conn->search_path);
				}
				free(found_conn);

				break;
			case pg_execute:
				debug(2, "Sending simple statement on session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));
				if(polardb_audit){
					/* set search before execute sql every times */
					char * search_path;
					search_path = replay_get_search_path(item);
					if(!search_path) {
						fprintf(stderr, "Error not have search_path statement: %s\n", search_path);
					}else{
						if(strcmp(search_path, found_conn->search_path) != 0){
							set_search_path(search_path, found_conn->db_conn);
							strcpy(found_conn->search_path, search_path);
						}
					}
					debug(1,"search_path is %s\n",search_path);
					if(search_path) free(search_path);

					/* check out whether prepare exist,create it if not exist */
					char * params_typename;
					params_typename = replay_get_prepare_params_typename(item);
					if(params_typename){
						prepare_item * s;
						char * tmp = strchr(params_typename,',');
						*tmp = '\0';
						debug(1,"params_typename is %s\n",params_typename);
						HASH_FIND_STR(found_conn->prepare_hash, params_typename, s);
						if(!s){
							char* source_text;
							source_text = replay_get_prepare_source_text(item);
							if(!source_text){
								fprintf(stderr, "prepare cmd is err in statement: %s\n", source_text);
							}
							debug(1,"source_text is %s\n",source_text);
 							set_prepare_cmd(source_text, found_conn->db_conn);
							s = (prepare_item*)malloc(sizeof(prepare_item));
							s->name = (char*)malloc(strlen(params_typename)+1);
							// s->id = 1;
							strcpy(s->name, params_typename);
							HASH_ADD_KEYPTR(hh, found_conn->prepare_hash, s->name, strlen(s->name), s);
							if(source_text) free(source_text);
						}
					}
					if(params_typename) free(params_typename);
				}
				debug(1,"replay_get_statement(item) is %s\n",replay_get_statement(item));

				if (! PQsendQuery(found_conn->db_conn, replay_get_statement(item))) {
					fprintf(stderr, "Error sending simple statement: %s\n", PQerrorMessage(found_conn->db_conn));
					rc = -1;
				} else {
					found_conn->status = wait_write;
				}
				break;
			case pg_prepare:
				debug(2, "Sending prepare request on session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));

				/* count preparations for statistics */
				++stat_prep;

				if (! PQsendPrepare(
						found_conn->db_conn,
						replay_get_name(item),
						replay_get_statement(item),
						0,
						NULL)) {
					fprintf(stderr, "Error sending prepare request: %s\n", PQerrorMessage(found_conn->db_conn));
					rc = -1;
				} else {
					found_conn->status = wait_write;
				}
				break;
			case pg_exec_prepared:
				debug(2, "Sending prepared statement execution on session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));

				if (! PQsendQueryPrepared(
						found_conn->db_conn,
						replay_get_name(item),
						replay_get_valuecount(item),
						replay_get_values(item),
						NULL,
						NULL,
						0)) {
					fprintf(stderr, "Error sending prepared statement execution: %s\n", PQerrorMessage(found_conn->db_conn));
					rc = -1;
				} else {
					found_conn->status = wait_write;
				}
				break;
			case pg_cancel:
				debug(2, "Sending cancel request on session 0x" UINT64_FORMAT "\n", replay_get_session_id(item));

				if (NULL == (cancel_request = PQgetCancel(found_conn->db_conn))) {
					fprintf(stderr, "Error creating cancel request\n");
					rc = -1;
				} else {
					if (! PQcancel(cancel_request, errbuf, 256)) {
						fprintf(stderr, "Error sending cancel request: %s\n", errbuf);
						rc = -1;
					}
					/* free cancel request */
					PQfreeCancel(cancel_request);
				}
				/* status remains unchanged */
				break;
		}

		replay_free(item);
	}

	/* try to flush the statement if necessary */
	if ((1 == rc) && (pg_disconnect != type) && (wait_write == found_conn->status)) {
		switch (PQflush(found_conn->db_conn)) {
			case 0:
				/* complete request sent */
				found_conn->status = wait_read;
				break;
			case 1:
				debug(2, "Session 0x" UINT64_FORMAT " needs to flush again\n", found_conn->session_id);
				break;
			default:
				fprintf(stderr, "Error flushing to database: %s\n", PQerrorMessage(found_conn->db_conn));
				rc = -1;
		}

		/* get statement start time */
		if (-1 == gettimeofday(&(found_conn->stmt_start), NULL)) {
			perror("Error calling gettimeofday");
			rc = -1;
		}

		/* count concurrent statements */
		if (++stat_statements > stat_stmtmax) {
			stat_stmtmax = stat_statements;
		}
	}

	debug(3, "Leaving database_consumer%s\n", "");
	return rc;
}

int database_consumer_dry_run(replay_item *item) {
	const replay_type type = replay_get_type(item);
	const struct timeval *stmt_time;
	static int fstmt_set_dr = 0;

	debug(3, "Entering database_consumer_dry_run%s\n", "");

	/* time when the statement originally ran */
	stmt_time = replay_get_time(item);
	last_stmt_time.tv_sec = stmt_time->tv_sec;
	last_stmt_time.tv_usec = stmt_time->tv_usec;

	/* set first_stmt_time if it is not yet set */
	if (! fstmt_set_dr) {
		first_stmt_time.tv_sec = stmt_time->tv_sec;
		first_stmt_time.tv_usec = stmt_time->tv_usec;

		fstmt_set_dr = 1;
	}

	/* gather statistics */
	++stat_actions;

	switch (type) {
		case pg_connect:
			++stat_sesscnt;
			if (++stat_sessions > stat_sessmax) {
				stat_sessmax = stat_sessions;
			}
			break;
		case pg_disconnect:
			--stat_sessions;
			break;
		case pg_execute:
		case pg_exec_prepared:
			++stat_stmt;
			break;
		case pg_prepare:
			++stat_prep;
			break;
		case pg_cancel:
			break;
	}

	replay_free(item);
	debug(3, "Leaving database_consumer_dry_run%s\n", "");

	return 1;
}

/* 
initailize connect for getting monitor info
 */
int monitor_connect_init(const char *host, int port, const char *passwd) {
    // 建立连接
	// create connect to db server
	char conn_info[100] = {'\0'};
	sprintf(conn_info,"dbname=postgres user=polardb password=%s hostaddr=%s port=%d ", passwd, host, port);
	
    monitor_conn = PQconnectdb(conn_info);

    // 检查连接是否成功
	// check if connect was success.
    if (PQstatus(monitor_conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(monitor_conn));
        PQfinish(monitor_conn);
        return 1;
    }
    return 0;
}

/* 
get once monitor info 
 */
int monitor_connect_execute(const char* sql) {
	
	// execute sql
	static int print_title = 0;
	PGresult *result = PQexec(monitor_conn, sql);
	if(!print_title){
		printf("cpu_use mem_use read_count write_count read_bytes write_bytes disk_space active_conn total_conn load5 load10 tps qps stat_correct stat_errors time0 time1 time2 time3 time4\n");
		print_title++;
	}

	// check if sql execution was success 
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query execution failed: %s", PQerrorMessage(monitor_conn));
        PQclear(result);
        PQfinish(monitor_conn);
        return 1;
    }

	if(old_result){
		printf("%s ",PQgetvalue(result, 0, 2));
		printf("%s ",PQgetvalue(result, 1, 2));
		printf("%ld ",atol(PQgetvalue(result, 2, 2)) - atol(PQgetvalue(old_result, 2, 2)));
		printf("%ld ",atol(PQgetvalue(result, 3, 2)) - atol(PQgetvalue(old_result, 3, 2)));
		printf("%ld ",atol(PQgetvalue(result, 4, 2)) - atol(PQgetvalue(old_result, 4, 2)));
		printf("%ld ",atol(PQgetvalue(result, 5, 2)) - atol(PQgetvalue(old_result, 5, 2)));
		printf("%s ",PQgetvalue(result, 6, 2));
		printf("%s ",PQgetvalue(result, 7, 2));
		printf("%s ",PQgetvalue(result, 8, 2));
		printf("%s ",PQgetvalue(result, 9, 2));
		printf("%s ",PQgetvalue(result, 10, 2));
		printf("%ld ",atol(PQgetvalue(result, 11, 2)) - atol(PQgetvalue(old_result, 11, 2)));
		printf("%ld ",atol(PQgetvalue(result, 12, 2)) - atol(PQgetvalue(old_result, 12, 2)));

		printf("%lu ", (stat_stmt - stat_errors) - (old_stat_stmt - old_stat_errors) );
		printf("%lu ", stat_errors - old_stat_errors);
		printf("%lu ", stat_hist[0] - old_stat_hist[0]);
		printf("%lu ", stat_hist[1] - old_stat_hist[1]);
		printf("%lu ", stat_hist[2] - old_stat_hist[2]);
		printf("%lu ", stat_hist[3] - old_stat_hist[3]);
		printf("%lu ", stat_hist[4] - old_stat_hist[4]);
		printf("\n");
	}

	if(old_result){
		PQclear(old_result);
	}
	old_result = result;
	old_stat_errors = stat_errors;
	old_stat_stmt = stat_stmt;
	memcpy(old_stat_hist, stat_hist, sizeof(stat_hist));

    return 0;
}

// close connect of monitor info
int monitor_connect_finish() {
	// Release resources
	PQclear(old_result);
    PQfinish(monitor_conn);

    return 0;
}
