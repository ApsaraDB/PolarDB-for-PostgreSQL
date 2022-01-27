/*--------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration scheme, including SET
 * command, configuration file, and
 command line options.
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *      src/backend/utils/misc/guc.c
 *
 *--------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#include "gtm/gtm.h"
#include "gtm/path.h"
#include "gtm/gtm_opt_tables.h"
#include "gtm/gtm_opt.h"
#include "gtm/gtm_standby.h"


#define CONFIG_FILENAME "gtm.conf"
const char *config_filename = CONFIG_FILENAME;

/*
 * Variables declared elsewhere for gtm, mainly option variables.
 */

extern char *NodeName;
extern char *ListenAddresses;
extern bool Backup_synchronously;
extern int GTMPortNumber;
extern char *active_addr;
extern int active_port;
extern int GTM_StandbyMode;
extern char *error_reporter;
extern char *status_reader;
extern int log_min_messages;
extern int tcp_keepalives_idle;
extern int tcp_keepalives_count;
extern int tcp_keepalives_interval;
extern char *GTMDataDir;
extern int            scale_factor_threads;
extern int            worker_thread_number;
#ifdef POLARDB_X
extern bool    enable_gtm_sequence_debug;
extern int      wal_writer_delay;
extern int      checkpoint_interval;
extern char     *archive_command;
extern bool     archive_mode;
extern int      max_reserved_wal_number;
extern int      max_wal_sender;
extern char     *synchronous_standby_names;
extern char     *application_name;
extern bool      enalbe_gtm_xlog_debug;
extern char     *recovery_command;
extern char     *recovery_target_timestamp;

extern char*   GTMStartupGTSSet;
extern bool    GTMClusterReadOnly;
extern int     GTMStartupGTSDelta;
extern int     GTMGTSFreezeLimit;

#endif


/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level)
 */

Server_Message_Level_Options();
Gtm_Startup_Mode_Options();

/*
 * GTM option variables that are exported from this module
 */
char       *data_directory;
char       *GTMConfigFileName;

/*
 * Displayable names for context types (enum GtmContext)
 */
gtmOptContext_Names();

/*
 * Displayable names for source types (enum GtmSource)
 */
gtmOptSource_Names();


/*
 * Displayable names for GTM variable types (enum config_type)
 */
Config_Type_Names();

/*
 * Contents of GTM tables
 *
 * See src/backend/utils/misc/README for design notes.
 *
 * TO ADD AN OPTION:
 *
 * 1. Declare a global variable of type bool, int, double, or char*
 *      and make use of it.
 *
 * 2. Decide at what times it's safe to set the option. See guc.h for
 *      details.
 *
 * 3. Decide on a name, a default value, upper and lower bounds (if
 *      applicable), etc.
 *
 * 4. Add a record below.
 *
 * 5. Add it to src/backend/utils/misc/postgresql.conf.sample, if
 *      appropriate.
 *
 * 6. Don't forget to document the option (at least in config.sgml).
 *
 * 7. If it's a new GTMOPT_LIST option you must edit pg_dumpall.c to ensure
 *      it is not single quoted at dump time.
 */

/*
 * Definition of option name strings are given in gtm_opt.h, which are shared
 * with command line option handling.  Naming is GTM_OPTNAME_*.
 */


/******** option records follow ********/

struct config_bool ConfigureNamesBool[] =
{
    {
        {GTM_OPTNAME_SYNCHRONOUS_BACKUP, GTMC_STARTUP,
           gettext_noop("Specifies if backup to GTM-Standby is taken in synchronous manner."),
           gettext_noop("Default value is off."),
           0
        },
        &Backup_synchronously,
        false, false, NULL
    },
#ifdef POLARDB_X    
    {
        {GTM_OPTNAME_SYNCHRONOUS_COMMIT, GTMC_SIGHUP,
           gettext_noop("enable GTM synchronous commit."),
           gettext_noop("Standby must be connected when set."),
           0
        },
        &enable_sync_commit,
        false, false, NULL
    },
    {
        {GTM_OPTNAME_ENABLE_DEBUG, GTMC_SIGHUP,
           gettext_noop("enable GTM debug print."),
           gettext_noop("Default value is off."),
           0
        },
        &enable_gtm_debug,
        false, false, NULL
    },
#ifdef POLARDB_X
    {
        {GTM_OPTNAME_ARCHIVE_MODE, GTMC_SIGHUP,
           gettext_noop("enable archive."),
           gettext_noop("Default value is off."),
           0
        },
        &archive_mode,
        false, false, NULL
    },
    {
        {GTM_OPTNAME_ENABLE_XLOG_DEBUG, GTMC_SIGHUP,
           gettext_noop("enable GTM xlog debug print."),
           gettext_noop("Default value is off."),
           0
        },
        &enalbe_gtm_xlog_debug,
        false, false, NULL
    },
#endif
    /* Set it as a GUC only if we are running regression. */
    {
        {GTM_OPTNAME_ENABLE_SEQ_DEBUG, GTMC_SIGHUP,
           gettext_noop("enable GTM sequence debug."),
           gettext_noop("Default value is off."),
           0
        },
        &enable_gtm_sequence_debug,
#ifdef _PG_REGRESS_
        true, false, NULL
#else
        false, false, NULL
#endif
    },
#endif
    {
            {GTM_OPTNAME_CLUSTER_READ_ONLY, GTMC_SIGHUP,
             gettext_noop("Nodes connected with gtm will be readonly."),
             gettext_noop("Default value is off."),
             0
            },
            &GTMClusterReadOnly,
            false, false, NULL
    },

    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL, 0}, NULL, false, false, NULL
    }
};


struct config_int ConfigureNamesInt[] =
{
    {
        {GTM_OPTNAME_PORT, GTMC_STARTUP,
            gettext_noop("Listen Port of GTM or GTM standby server."),
            NULL,
            0
        },
        &GTMPortNumber,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {GTM_OPTNAME_ACTIVE_PORT, GTMC_SIGHUP,
            gettext_noop("GTM server port number when it works as GTM-Standby."),
            NULL,
            0
        },
        &active_port,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {GTM_OPTNAME_KEEPALIVES_IDLE, GTMC_STARTUP,
            gettext_noop("Sets \"keepalives_idle\" option for the connection to GTM."),
             gettext_noop("This option is effective only when it runs as GTM-Standby."),
            GTMOPT_UNIT_TIME
        },
        &tcp_keepalives_idle,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {GTM_OPTNAME_KEEPALIVES_INTERVAL, GTMC_STARTUP,
            gettext_noop("Sets \"keepalives_interval\" option fo the connetion to GTM."),
            gettext_noop("This option is effective only when it runs as GTM-Standby."),
            GTMOPT_UNIT_TIME
        },
        &tcp_keepalives_interval,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {GTM_OPTNAME_KEEPALIVES_COUNT, GTMC_STARTUP,
            gettext_noop("Sets \"keepalives_count\" option to the connection to GTM."),
            gettext_noop("This option is effective only when it runs as GTM-Standby."),
            0
        },
        &tcp_keepalives_count,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {
            GTM_OPTNAME_SCALE_FACTOR_THREADS, GTMC_STARTUP,
            gettext_noop("The scale factor of the number of worker thread, zero means disabled."),
            NULL,
            0
        },
        &scale_factor_threads,
        1, 0, INT_MAX,
        0, NULL
    },
    {
        {
            GTM_OPTNAME_WORKER_THREADS_NUMBER, GTMC_STARTUP,
            gettext_noop("The number of worker thread, zero means disabled."),
            NULL,
            0
        },
        &worker_thread_number,
        2, 0, INT_MAX,
        0, NULL
    },            
#ifdef POLARDB_X
    {
        {
            GTM_OPTNAME_WAL_WRITER_DELAY, GTMC_SIGHUP,
            gettext_noop("Wal_writer will flush xlog every wal_writer_delay ms."),
            NULL,
            0
        },
        &wal_writer_delay,
        100, 10, INT_MAX,
        0, NULL
    },
    {
        {
            GTM_OPTNAME_CHECKPOINT_INTERVAL, GTMC_SIGHUP,
            gettext_noop("Checkpointer will do checkpoint every checkpoint_interval minute."),
            NULL,
            0
        },
        &checkpoint_interval,
        30, 1, INT_MAX,
        0, NULL
    },
    {
        {
            GTM_OPTNAME_MAX_RESERVED_WAL_NUMBER, GTMC_STARTUP,
            gettext_noop("Max number of reserved xlog segments."),
            NULL,
            0
        },
        &max_reserved_wal_number,
        0, 0, INT_MAX,
        0, NULL
    },
    {
        {
            GTM_OPTNAME_MAX_WAL_SENDER, GTMC_STARTUP,
            gettext_noop("Max number of wal senders."),
            NULL,
            0
        },
        &max_wal_sender,
        3, 0, 100,
        0, NULL
    },
#endif
    {
            {
                    GTM_OPTNAME_GTS_FREEZE_TIME_LIMIT, GTMC_STARTUP,
                    gettext_noop("refuse to start gtm before GTS has n days left,default 100 years"),
                    NULL,
                    0
            },
            &GTMGTSFreezeLimit,
            365 * 100, 0, INT_MAX,
            0, NULL
    },
    {
            {
                    GTM_OPTNAME_STARTUP_GTS_DELTA, GTMC_STARTUP,
                    gettext_noop("Add -d seconds to GTS when started"),
                    NULL,
                    0
            },
            &GTMStartupGTSDelta,
            300 , 0, INT_MAX,
            0, NULL
    },

    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL, 0}, NULL, 0, 0, 0, 0, NULL
    }
};


struct config_real ConfigureNamesReal[] =
{
    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL, 0}, NULL, 0.0, 0.0, 0.0, 0.0, NULL
    }
};

struct config_string ConfigureNamesString[] =
{
    {
        {GTM_OPTNAME_DATA_DIR, GTMC_STARTUP,
            gettext_noop("Work directory."),
            NULL,
            0
        },
        &GTMDataDir,
        NULL,
        NULL,
        NULL
    },

    {
        {GTM_OPTNAME_CONFIG_FILE, GTMC_SIGHUP,
             gettext_noop("Configuration file name."),
             NULL,
             0
        },
        &GTMConfigFileName,
        CONFIG_FILENAME,
        NULL,
        NULL
    },

    {
        {GTM_OPTNAME_NODENAME, GTMC_STARTUP,
            gettext_noop("Name of this GTM/GTM-Standby."),
            NULL,
            0
        },
        &NodeName,
        "gtm",
        NULL,
        NULL
    },

    {
        {GTM_OPTNAME_LISTEN_ADDRESSES, GTMC_STARTUP,
            gettext_noop("Listen address."),
            NULL,
            0
        },
        &ListenAddresses,
        "*",
        NULL, NULL
    },

    {
        {GTM_OPTNAME_ACTIVE_HOST, GTMC_SIGHUP,
            gettext_noop("Address of target GTM ACT."),
            gettext_noop("This parameter is effective only when it runs as GTM-Standby"),
            0
        },
        &active_addr,
        NULL,
        NULL, NULL
    },

    {
        {GTM_OPTNAME_LOG_FILE, GTMC_SIGHUP,
            gettext_noop("Log file name."),
            NULL,
            0
        },
        &GTMLogFile,
        "gtm.log",
        NULL, NULL
    },

    {
        {GTM_OPTNAME_ERROR_REPORTER, GTMC_SIGHUP,
            gettext_noop("Command to report various errors."),
            NULL,
            0
        },
        &error_reporter,
        NULL,
        NULL, NULL
    },

    {
        {GTM_OPTNAME_STATUS_READER, GTMC_SIGHUP,
            gettext_noop("Command to get status of global XC node status."),
            gettext_noop("Runs when configuration file is read by SIGHUP"),
            0
        },
        &status_reader,
        NULL,
        NULL, NULL
    },
#ifdef POLARDB_X
    {
        {GTM_OPTNAME_ARCHIVE_COMMAND, GTMC_SIGHUP,
            gettext_noop("Archive use this command to backup xlog."),
            NULL,
            0
        },
        &archive_command,
        NULL,
        NULL, NULL
    },

    {
        {GTM_OPTNAME_SYNCHRONOUS_STANDBY_NAMES, GTMC_SIGHUP,
            gettext_noop("to indicate which are synchronous slaves."),
            NULL,
            0
        },
        &synchronous_standby_names,
        NULL,
        NULL, NULL
    },
    
    {
        {GTM_OPTNAME_APPLICATION_NAME, GTMC_STARTUP,
            gettext_noop("application name used in sync replication indication"),
            NULL,
            0
        },
        &application_name,
        "",
        NULL, NULL
    },
    {
        {GTM_OPTNAME_RECOVERY_COMMAND, GTMC_STARTUP,
             gettext_noop("Point in time recovery,recovery command"),
             NULL,
             0
        },
        &recovery_command,
        NULL,
        NULL, NULL
    },
    {
        {GTM_OPTNAME_RECOVERY_TARGET_GLOBALTIMESTAMP, GTMC_STARTUP,
         gettext_noop("Point in time recovery,recovery timestamp"),
         NULL,
         0
        },
        &recovery_target_timestamp,
        NULL,
        NULL, NULL
    },
#endif
    {
            {GTM_OPTNAME_STARTUP_GTS_SET, GTMC_STARTUP,
             gettext_noop("Force start GTM with this GTS"),
             NULL,
             0
            },
            &GTMStartupGTSSet,
            NULL,
            NULL, NULL
    },

    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL}, NULL, NULL, NULL, NULL
    }
};


struct config_enum ConfigureNamesEnum[] =
{
    {
        {GTM_OPTNAME_LOG_MIN_MESSAGES, GTMC_SIGHUP,
            gettext_noop("Minimum message level to write to the log file."),
            NULL,
             0
        },
        &log_min_messages,
        WARNING,
        server_message_level_options,
        WARNING, NULL
    },

    {
        {GTM_OPTNAME_STARTUP, GTMC_SIGHUP,
            gettext_noop("Specifies startup mode, act or standby."),
            NULL,
            0
        },
        &GTM_StandbyMode,
        GTM_ACT_MODE,
        gtm_startup_mode_options,
        GTM_ACT_MODE, NULL
    },

    /* End-of-list marker */
    {
        {NULL, 0, NULL, NULL, 0}, NULL, 0, NULL, 0, NULL
    }
};

/******** end of options list ********/

/*
 * Actual lookup of variables is done through this single, sorted array.
 */
struct config_generic **gtm_opt_variables;

/* Current number of variables contained in the vector */
int    num_gtm_opt_variables;

/* Vector capacity */
int    size_gtm_opt_variables;


bool reporting_enabled;    /* TRUE to enable GTMOPT_REPORT */

int    GTMOptUpdateCount = 0; /* Indicates when specific option is updated */
