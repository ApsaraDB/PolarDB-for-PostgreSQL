/*-------------------------------------------------------------------------
 * pgut-fe.c
 *
 * Portions Copyright (c) 2008-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2011, Itagaki Takahiro
 * Portions Copyright (c) 2012-2020, The Reorg Development Team
 *-------------------------------------------------------------------------
 */

#define FRONTEND
#include "pgut-fe.h"
#include "common/username.h"

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include <getopt_long.h>
#endif

const char *dbname = NULL;
char	   *host = NULL;
char	   *port = NULL;
char	   *username = NULL;
char	   *password = NULL;
YesNo		prompt_password = DEFAULT;

PGconn	   *connection = NULL;
PGconn     *conn2      = NULL;

worker_conns workers   = {
	.num_workers     = 0,
	.conns           = NULL
};


static bool parse_pair(const char buffer[], char key[], char value[]);

/*
 * Set up worker conns which will be used for concurrent index rebuilds.
 * 'num_workers' is the desired number of worker connections, i.e. from
 * --jobs flag. Due to max_connections we might not actually be able to
 * set up that many workers, but don't treat that as a fatal error.
 */
void
setup_workers(int num_workers)
{
 	int i;
 	PGconn *conn;

 	elog(DEBUG2, "In setup_workers(), target num_workers = %d", num_workers);

 	if (num_workers > 1 && num_workers > workers.num_workers)
 	{
#define PARAMS_ARRAY_SIZE	6

		const char *keywords[PARAMS_ARRAY_SIZE];
		const char *values[PARAMS_ARRAY_SIZE];

		keywords[0] = "host";
		values[0] = host;
		keywords[1] = "port";
		values[1] = port;
		keywords[2] = "user";
		values[2] = username;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = dbname;
		keywords[5] = NULL;
		values[5] = NULL;

 		if (workers.conns == NULL)
 		{
 			elog(NOTICE, "Setting up workers.conns");
 			workers.conns = (PGconn **) pgut_malloc(sizeof(PGconn *) * num_workers);
 		}
 		else
 		{
 			elog(ERROR, "TODO: Implement pool resizing.");
 		}

 		for (i = 0; i < num_workers; i++)
 		{
 			/* Don't prompt for password again; we should have gotten
 			 * it already from reconnect().
 			 */
 			elog(DEBUG2, "Setting up worker conn %d", i);

 			/* Don't confuse pgut_connections by using pgut_connect()
			 *
			 * XXX: could use PQconnectStart() and PQconnectPoll() to
			 * open these connections in non-blocking manner.
			 */
			conn = PQconnectdbParams(keywords, values, true);
			if (PQstatus(conn) == CONNECTION_OK)
			{
				workers.conns[i] = conn;
			}
			else
			{
				elog(WARNING, "Unable to set up worker conn #%d: %s", i,
					 PQerrorMessage(conn));
				break;
			}

			/* Hardcode a search path to avoid injections into public or pg_temp */
			pgut_command(conn, "SET search_path TO pg_catalog, pg_temp, public", 0, NULL);

            /* Make sure each worker connection can work in non-blocking
             * mode.
             */
            if (PQsetnonblocking(workers.conns[i], 1))
			{
				elog(ERROR, "Unable to set worker connection %d "
					 "non-blocking.", i);
			}

			/* POLAR: init guc values for for parallel worker connection. */
			polar_init_guc_for_conn(workers.conns[i]);
 		}
		/* In case we bailed out of setting up all workers, record
		 * how many successful worker conns we actually have.
		 */
		workers.num_workers = i;
	}
}

/* Disconnect all our worker conns. */
void disconnect_workers(void)
{
 	int i;

 	if (!(workers.num_workers))
 		elog(DEBUG2, "No workers to disconnect.");
 	else
 	{
 		for (i = 0; i < workers.num_workers; i++)
 		{
 			if (workers.conns[i])
 			{
 				elog(DEBUG2, "Disconnecting worker %d.", i);
 				PQfinish(workers.conns[i]);
 				workers.conns[i] = NULL;
 			}
 			else
 			{
 				elog(NOTICE, "Worker %d already disconnected?", i);
 			}
 		}
 		workers.num_workers = 0;
 		free(workers.conns);
		workers.conns = NULL;
 	}
}


/*
 * the result is also available with the global variable 'connection'.
 */
void
reconnect(int elevel)
{
	char		   *new_password;

	disconnect();

	connection = pgut_connect(dbname, host, port, username, password, prompt_password, elevel);
	conn2      = pgut_connect(dbname, host, port, username, password, prompt_password, elevel);

	/* update password */
	if (connection)
	{
		new_password = PQpass(connection);
		if (new_password && new_password[0] &&
			(password == NULL || strcmp(new_password, password) != 0))
		{
			free(password);
			password = pgut_strdup(new_password);
		}
	}

	/* POLAR: init guc values for primary connection and conn2. */
	polar_init_guc_for_conn(connection);
	polar_init_guc_for_conn(conn2);
}

void
disconnect(void)
{
	if (connection)
	{
		pgut_disconnect(connection);
		connection = NULL;
	}
	if (conn2)
	{
		pgut_disconnect(conn2);
		conn2 = NULL;
	}
	disconnect_workers();
}

static void
option_from_env(pgut_option options[])
{
	size_t	i;

	for (i = 0; options && options[i].type; i++)
	{
		pgut_option	   *opt = &options[i];
		char			name[256];
		size_t			j;
		const char	   *s;
		const char	   *value;

		if (opt->source > SOURCE_ENV ||
			opt->allowed == SOURCE_DEFAULT || opt->allowed > SOURCE_ENV)
			continue;

		for (s = opt->lname, j = 0; *s && j < lengthof(name) - 1; s++, j++)
		{
			if (strchr("-_ ", *s))
				name[j] = '_';	/* - to _ */
			else
				name[j] = toupper(*s);
		}
		name[j] = '\0';

		if ((value = getenv(name)) != NULL)
			pgut_setopt(opt, value, SOURCE_ENV);
	}
}

/* compare two strings ignore cases and ignore -_ */
bool
pgut_keyeq(const char *lhs, const char *rhs)
{
	for (; *lhs && *rhs; lhs++, rhs++)
	{
		if (strchr("-_ ", *lhs))
		{
			if (!strchr("-_ ", *rhs))
				return false;
		}
		else if (ToLower(*lhs) != ToLower(*rhs))
			return false;
	}

	return *lhs == '\0' && *rhs == '\0';
}

void
pgut_setopt(pgut_option *opt, const char *optarg, pgut_optsrc src)
{
	const char	  *message;

	if (opt == NULL)
	{
		fprintf(stderr, "Try \"%s --help\" for more information.\n", PROGRAM_NAME);
		exit(EINVAL);
	}

	if (opt->source > src)
	{
		/* high prior value has been set already. */
		return;
	}
	else if (src >= SOURCE_CMDLINE && opt->source >= src && opt->type != 'l')
	{
		/* duplicated option in command line -- don't worry if the option
		 * type is 'l' i.e. SimpleStringList, since we are allowed to have
		 * multiples of these.
		 */
		message = "specified only once";
	}
	else
	{
		/* can be overwritten if non-command line source */
		opt->source = src;

		switch (opt->type)
		{
			case 'b':
			case 'B':
				if (optarg == NULL)
				{
					*((bool *) opt->var) = (opt->type == 'b');
					return;
				}
				else if (parse_bool(optarg, (bool *) opt->var))
				{
					return;
				}
				message = "a boolean";
				break;
			case 'f':
				((pgut_optfn) opt->var)(opt, optarg);
				return;
			case 'i':
				if (parse_int32(optarg, opt->var))
					return;
				message = "a 32bit signed integer";
				break;
			case 'l':
				message = "a List";
				simple_string_list_append(opt->var, optarg);
				return;
			case 'u':
				if (parse_uint32(optarg, opt->var))
					return;
				message = "a 32bit unsigned integer";
				break;
			case 'I':
				if (parse_int64(optarg, opt->var))
					return;
				message = "a 64bit signed integer";
				break;
			case 'U':
				if (parse_uint64(optarg, opt->var))
					return;
				message = "a 64bit unsigned integer";
				break;
			case 's':
				if (opt->source != SOURCE_DEFAULT)
					free(*(char **) opt->var);
				*(char **) opt->var = pgut_strdup(optarg);
				return;
			case 't':
				if (parse_time(optarg, opt->var))
					return;
				message = "a time";
				break;
			case 'y':
			case 'Y':
				if (optarg == NULL)
				{
					*(YesNo *) opt->var = (opt->type == 'y' ? YES : NO);
					return;
				}
				else
				{
					bool	value;
					if (parse_bool(optarg, &value))
					{
						*(YesNo *) opt->var = (value ? YES : NO);
						return;
					}
				}
				message = "a boolean";
				break;
			default:
				ereport(ERROR,
					(errcode(EINVAL),
					 errmsg("invalid option type: %c", opt->type)));
				return;	/* keep compiler quiet */
		}
	}

	if (isprint(opt->sname))
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("option -%c, --%s should be %s: '%s'",
				opt->sname, opt->lname, message, optarg)));
	else
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("option --%s should be %s: '%s'",
				opt->lname, message, optarg)));
}

/*
 * Get configuration from configuration file.
 */
void
pgut_readopt(const char *path, pgut_option options[], int elevel)
{
	FILE   *fp;
	char	buf[1024];
	char	key[1024];
	char	value[1024];

	if (!options)
		return;

	if ((fp = pgut_fopen(path, "Rt")) == NULL)
		return;

	while (fgets(buf, lengthof(buf), fp))
	{
		size_t		i;

		for (i = strlen(buf); i > 0 && IsSpace(buf[i - 1]); i--)
			buf[i - 1] = '\0';

		if (parse_pair(buf, key, value))
		{
			for (i = 0; options[i].type; i++)
			{
				pgut_option *opt = &options[i];

				if (pgut_keyeq(key, opt->lname))
				{
					if (opt->allowed == SOURCE_DEFAULT ||
						opt->allowed > SOURCE_FILE)
						elog(elevel, "option %s cannot specified in file", opt->lname);
					else if (opt->source <= SOURCE_FILE)
						pgut_setopt(opt, value, SOURCE_FILE);
					break;
				}
			}
			if (!options[i].type)
				elog(elevel, "invalid option \"%s\"", key);
		}
	}

	fclose(fp);
}

static const char *
skip_space(const char *str, const char *line)
{
	while (IsSpace(*str)) { str++; }
	return str;
}

static const char *
get_next_token(const char *src, char *dst, const char *line)
{
	const char *s;
	size_t		i;
	size_t		j;

	if ((s = skip_space(src, line)) == NULL)
		return NULL;

	/* parse quoted string */
	if (*s == '\'')
	{
		s++;
		for (i = 0, j = 0; s[i] != '\0'; i++)
		{
			if (s[i] == '\\')
			{
				i++;
				switch (s[i])
				{
					case 'b':
						dst[j] = '\b';
						break;
					case 'f':
						dst[j] = '\f';
						break;
					case 'n':
						dst[j] = '\n';
						break;
					case 'r':
						dst[j] = '\r';
						break;
					case 't':
						dst[j] = '\t';
						break;
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							int			k;
							long		octVal = 0;

							for (k = 0;
								 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
									 k++)
								octVal = (octVal << 3) + (s[i + k] - '0');
							i += k - 1;
							dst[j] = ((char) octVal);
						}
						break;
					default:
						dst[j] = s[i];
						break;
				}
			}
			else if (s[i] == '\'')
			{
				i++;
				/* doubled quote becomes just one quote */
				if (s[i] == '\'')
					dst[j] = s[i];
				else
					break;
			}
			else
				dst[j] = s[i];
			j++;
		}
	}
	else
	{
		i = j = strcspn(s, "# \n\r\t\v");
		memcpy(dst, s, j);
	}

	dst[j] = '\0';
	return s + i;
}

static bool
parse_pair(const char buffer[], char key[], char value[])
{
	const char *start;
	const char *end;

	key[0] = value[0] = '\0';

	/*
	 * parse key
	 */
	start = buffer;
	if ((start = skip_space(start, buffer)) == NULL)
		return false;

	end = start + strcspn(start, "=# \n\r\t\v");

	/* skip blank buffer */
	if (end - start <= 0)
	{
		if (*start == '=')
			elog(WARNING, "syntax error in \"%s\"", buffer);
		return false;
	}

	/* key found */
	strncpy(key, start, end - start);
	key[end - start] = '\0';

	/* find key and value split char */
	if ((start = skip_space(end, buffer)) == NULL)
		return false;

	if (*start != '=')
	{
		elog(WARNING, "syntax error in \"%s\"", buffer);
		return false;
	}

	start++;

	/*
	 * parse value
	 */
	if ((end = get_next_token(start, value, buffer)) == NULL)
		return false;

	if ((start = skip_space(end, buffer)) == NULL)
		return false;

	if (*start != '\0' && *start != '#')
	{
		elog(WARNING, "syntax error in \"%s\"", buffer);
		return false;
	}

	return true;
}

/*
 * execute - Execute a SQL and return the result.
 */
PGresult *
execute(const char *query, int nParams, const char **params)
{
	return pgut_execute(connection, query, nParams, params);
}

PGresult *
execute_elevel(const char *query, int nParams, const char **params, int elevel)
{
	return pgut_execute_elevel(connection, query, nParams, params, elevel);
}

/*
 * command - Execute a SQL and discard the result.
 */
ExecStatusType
command(const char *query, int nParams, const char **params)
{
	return pgut_command(connection, query, nParams, params);
}

static void
set_elevel(pgut_option *opt, const char *arg)
{
	pgut_log_level = parse_elevel(arg);
}

static pgut_option default_options[] =
{
	{ 'b', 'e', "echo"			, &pgut_echo },
	{ 'f', 'E', "elevel"		, set_elevel },
	{ 's', 'd', "dbname"		, &dbname },
	{ 's', 'h', "host"			, &host },
	{ 's', 'p', "port"			, &port },
	{ 's', 'U', "username"		, &username },
	{ 'Y', 'w', "no-password"	, &prompt_password },
	{ 'y', 'W', "password"		, &prompt_password },
	{ 0 }
};

static size_t
option_length(const pgut_option opts[])
{
	size_t	len;
	for (len = 0; opts && opts[len].type; len++) { }
	return len;
}

static pgut_option *
option_find(int c, pgut_option opts1[], pgut_option opts2[])
{
	size_t	i;

	for (i = 0; opts1 && opts1[i].type; i++)
		if (opts1[i].sname == c)
			return &opts1[i];
	for (i = 0; opts2 && opts2[i].type; i++)
		if (opts2[i].sname == c)
			return &opts2[i];

	return NULL;	/* not found */
}

static int
option_has_arg(char type)
{
	switch (type)
	{
		case 'b':
		case 'B':
		case 'y':
		case 'Y':
			return no_argument;
		default:
			return required_argument;
	}
}

static void
option_copy(struct option dst[], const pgut_option opts[], size_t len)
{
	size_t	i;

	for (i = 0; i < len; i++)
	{
		dst[i].name = opts[i].lname;
		dst[i].has_arg = option_has_arg(opts[i].type);
		dst[i].flag = NULL;
		dst[i].val = opts[i].sname;
	}
}

static struct option *
option_merge(const pgut_option opts1[], const pgut_option opts2[])
{
	struct option *result;
	size_t	len1 = option_length(opts1);
	size_t	len2 = option_length(opts2);
	size_t	n = len1 + len2;

	result = pgut_newarray(struct option, n + 1);
	option_copy(result, opts1, len1);
	option_copy(result + len1, opts2, len2);
	memset(&result[n], 0, sizeof(pgut_option));

	return result;
}

static char *
longopts_to_optstring(const struct option opts[])
{
	size_t	len;
	char   *result;
	char   *s;

	for (len = 0; opts[len].name; len++) { }
	result = pgut_malloc(len * 2 + 1);

	s = result;
	for (len = 0; opts[len].name; len++)
	{
		if (!isprint(opts[len].val))
			continue;
		*s++ = opts[len].val;
		if (opts[len].has_arg != no_argument)
			*s++ = ':';
	}
	*s = '\0';

	return result;
}

int
pgut_getopt(int argc, char **argv, pgut_option options[])
{
	int					c;
	int					optindex = 0;
	char			   *optstring;
	struct option	   *longopts;
	pgut_option		   *opt;

	pgut_init(argc, argv);

	/* Help message and version are handled at first. */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(true);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("%s %s\n", PROGRAM_NAME, PROGRAM_VERSION);
			exit(0);
		}
		if (strcmp(argv[1], "--configuration") == 0)
		{
			printf("%s\n", PG_VERSION_STR);
			exit(0);
		}
	}

	/* Merge default and user options. */
	longopts = option_merge(default_options, options);
	optstring = longopts_to_optstring(longopts);

	/* Assign named options */
	while ((c = getopt_long(argc, argv, optstring, longopts, &optindex)) != -1)
	{
		opt = option_find(c, default_options, options);
		pgut_setopt(opt, optarg, SOURCE_CMDLINE);
	}

	/* Read environment variables */
	option_from_env(options);
	(void) (dbname ||
	(dbname = getenv("PGDATABASE")) ||
	(dbname = getenv("PGUSER")) ||
	(dbname = get_user_name_or_exit(PROGRAM_NAME)));

	return optind;
}

/*
 * POLAR: set initial guc values in connection. We don't even have to
 * reset it after repacking because the connection will be closed after
 * repacking.
 * 
 * No need to set statement_timeout here because it's set in preliminary_checks().
 * 
 * No need to set default_transaction_isolation because isolation level is set
 * in each transaction by BEGIN ISOLATION LEVEL xx xx.
 * 
 * We cannot set zero_damaged_pages even if we want to do it because it can
 * only be set by superuser and pg_repack user is not superuser.
 */
void
polar_init_guc_for_conn(PGconn *conn)
{
	/*
	 * Set idle_session_timeout to 0. We don't know whether this kind timeout
	 * could happen in pg_repack. But we just set it for better experience.
	 * 
	 * polar_idle_session_timeout is the same as idle_session_timeout
	 * but the name is different in PG11. We set it in all versions for less
	 * code conflict.
	 */
	polar_set_guc_for_conn(conn, "idle_session_timeout", "0");
	polar_set_guc_for_conn(conn, "polar_idle_session_timeout", "0");

	/*
	 * Force settable timeouts off to avoid letting these settings prevent
	 * regular maintenance from being executed.
	 * polar_transaction_timeout and transaction_timeout is the same. The
	 * one with polar_ prefix exists in old versions before PG17.
	 */
	polar_set_guc_for_conn(conn, "transaction_timeout", "0");
	polar_set_guc_for_conn(conn, "polar_transaction_timeout", "0");

	/*
	 * Set idle_in_transaction_session_timeout to 0. One conn could be in
	 * idle-in-txn state for long time because it holds the lock and another
	 * conn is building index.
	 */
	polar_set_guc_for_conn(conn, "idle_in_transaction_session_timeout", "0");

	/*
	 * Turn off the polar_restrict_duplicate_index option to allow
	 * duplicate index on the table because it's a by-design behavior of
	 * pg_repack.  Non-pg_repack connections
	 * can use the duplicate index restriction feature as usual because we
	 * don't change the system level value of the option.
	 */
	polar_set_guc_for_conn(conn, "polar_restrict_duplicate_index", "off");
}

/*
 * POLAR: set session level guc value for specified connection.
 * Other connections will not be affected.
 */
void
polar_set_guc_for_conn(PGconn *conn, const char *guc_name, const char *guc_value)
{
	ExecStatusType code;
	const char	   *params[2];

	params[0] = guc_name;
	params[1] = guc_value;
	code = pgut_command(conn, "SELECT pg_catalog.set_config($1, $2, 'f') "
						" FROM pg_settings WHERE name = $1", 2, params);
	if (code != PGRES_TUPLES_OK)
		ereport(ERROR,
			(errcode(ERROR), errmsg("set %s to %s failed for connection %s with code %d.",
			guc_name, guc_value, conn->appname, code)));
	elog(DEBUG2, "set %s to %s ok for connection %s.", guc_name, guc_value, conn->appname);
}

void
help(bool details)
{
	pgut_help(details);

	if (details)
	{
		printf("\nConnection options:\n");
		printf("  -d, --dbname=DBNAME       database to connect\n");
		printf("  -h, --host=HOSTNAME       database server host or socket directory\n");
		printf("  -p, --port=PORT           database server port\n");
		printf("  -U, --username=USERNAME   user name to connect as\n");
		printf("  -w, --no-password         never prompt for password\n");
		printf("  -W, --password            force password prompt\n");
	}

	printf("\nGeneric options:\n");
	if (details)
	{
		printf("  -e, --echo                echo queries\n");
		printf("  -E, --elevel=LEVEL        set output message level\n");
	}
	printf("  --help                    show this help, then exit\n");
	printf("  --version                 output version information, then exit\n");

	if (details && (PROGRAM_URL || PROGRAM_ISSUES))
	{
		printf("\n");
		if (PROGRAM_URL)
			printf("Read the website for details: <%s>.\n", PROGRAM_URL);
		if (PROGRAM_ISSUES)
			printf("Report bugs to <%s>.\n", PROGRAM_ISSUES);
	}
}
