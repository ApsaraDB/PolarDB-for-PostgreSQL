/*--------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to backend/utils/misc/guc.c and
 * backend/utils/misc/guc-file.l
 *
 * Copyright (c) 2000-2018, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc.h
 *--------------------------------------------------------------------
 */
#ifndef GUC_H
#define GUC_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/array.h"

/* POLAR px */
#include "px/px_vars.h"

/* upper limit for GUC variables measured in kilobytes of memory */
/* note that various places assume the byte size fits in a "long" variable */
#if SIZEOF_SIZE_T > 4 && SIZEOF_LONG > 4
#define MAX_KILOBYTES	INT_MAX
#else
#define MAX_KILOBYTES	(INT_MAX / 1024)
#endif

/*
 * Automatic configuration file name for ALTER SYSTEM.
 * This file will be used to store values of configuration parameters
 * set by ALTER SYSTEM command.
 */
#define PG_AUTOCONF_FILENAME		"postgresql.auto.conf"
#define PLOAR_NODE_CONF_FILENAME 	"polar_node_static.conf"

/*
 * Certain options can only be set at certain times. The rules are
 * like this:
 *
 * INTERNAL options cannot be set by the user at all, but only through
 * internal processes ("server_version" is an example).  These are GUC
 * variables only so they can be shown by SHOW, etc.
 *
 * POSTMASTER options can only be set when the postmaster starts,
 * either from the configuration file or the command line.
 *
 * SIGHUP options can only be set at postmaster startup or by changing
 * the configuration file and sending the HUP signal to the postmaster
 * or a backend process. (Notice that the signal receipt will not be
 * evaluated immediately. The postmaster and the backend check it at a
 * certain point in their main loop. It's safer to wait than to read a
 * file asynchronously.)
 *
 * BACKEND and SU_BACKEND options can only be set at postmaster startup,
 * from the configuration file, or by client request in the connection
 * startup packet (e.g., from libpq's PGOPTIONS variable).  SU_BACKEND
 * options can be set from the startup packet only when the user is a
 * superuser.  Furthermore, an already-started backend will ignore changes
 * to such an option in the configuration file.  The idea is that these
 * options are fixed for a given backend once it's started, but they can
 * vary across backends.
 *
 * SUSET options can be set at postmaster startup, with the SIGHUP
 * mechanism, or from the startup packet or SQL if you're a superuser.
 *
 * USERSET options can be set by anyone any time.
 */
typedef enum
{
	PGC_INTERNAL,
	PGC_POSTMASTER,
	PGC_SIGHUP,
	PGC_SU_BACKEND,
	PGC_BACKEND,
	PGC_SUSET,
	PGC_USERSET
} GucContext;

/*
 * The following type records the source of the current setting.  A
 * new setting can only take effect if the previous setting had the
 * same or lower level.  (E.g, changing the config file doesn't
 * override the postmaster command line.)  Tracking the source allows us
 * to process sources in any convenient order without affecting results.
 * Sources <= PGC_S_OVERRIDE will set the default used by RESET, as well
 * as the current value.  Note that source == PGC_S_OVERRIDE should be
 * used when setting a PGC_INTERNAL option.
 *
 * PGC_S_INTERACTIVE isn't actually a source value, but is the
 * dividing line between "interactive" and "non-interactive" sources for
 * error reporting purposes.
 *
 * PGC_S_TEST is used when testing values to be used later ("doit" will always
 * be false, so this never gets stored as the actual source of any value).
 * For example, ALTER DATABASE/ROLE tests proposed per-database or per-user
 * defaults this way, and CREATE FUNCTION tests proposed function SET clauses
 * this way.  This is an interactive case, but it needs its own source value
 * because some assign hooks need to make different validity checks in this
 * case.  In particular, references to nonexistent database objects generally
 * shouldn't throw hard errors in this case, at most NOTICEs, since the
 * objects might exist by the time the setting is used for real.
 *
 * NB: see GucSource_Names in guc.c if you change this.
 */
typedef enum
{
	PGC_S_DEFAULT,				/* hard-wired default ("boot_val") */
	PGC_S_DYNAMIC_DEFAULT,		/* default computed during initialization */
	PGC_S_ENV_VAR,				/* postmaster environment variable */
	PGC_S_FILE,					/* postgresql.conf */
	PGC_S_ARGV,					/* postmaster command line */
	PGC_S_GLOBAL,				/* global in-database setting */
	PGC_S_DATABASE,				/* per-database setting */
	PGC_S_USER,					/* per-user setting */
	PGC_S_DATABASE_USER,		/* per-user-and-database setting */
	PGC_S_CLIENT,				/* from client connection request */
	PGC_S_OVERRIDE,				/* special case to forcibly set default */
	PGC_S_INTERACTIVE,			/* dividing line for error reporting */
	PGC_S_TEST,					/* test per-database or per-user setting */
	PGC_S_SESSION				/* SET command */
} GucSource;

/*
 * Parsing the configuration file(s) will return a list of name-value pairs
 * with source location info.  We also abuse this data structure to carry
 * error reports about the config files.  An entry reporting an error will
 * have errmsg != NULL, and might have NULLs for name, value, and/or filename.
 *
 * If "ignore" is true, don't attempt to apply the item (it might be an error
 * report, or an item we determined to be duplicate).  "applied" is set true
 * if we successfully applied, or could have applied, the setting.
 */
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *errmsg;
	char	   *filename;
	int			sourceline;
	bool		ignore;
	bool		applied;
	struct ConfigVariable *next;
} ConfigVariable;

extern bool ParseConfigFile(const char *config_file, bool strict,
				const char *calling_file, int calling_lineno,
				int depth, int elevel,
				ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigFp(FILE *fp, const char *config_file,
			  int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigDirectory(const char *includedir,
					 const char *calling_file, int calling_lineno,
					 int depth, int elevel,
					 ConfigVariable **head_p,
					 ConfigVariable **tail_p);
extern void FreeConfigVariables(ConfigVariable *list);

/*
 * The possible values of an enum variable are specified by an array of
 * name-value pairs.  The "hidden" flag means the value is accepted but
 * won't be displayed when guc.c is asked for a list of acceptable values.
 */
struct config_enum_entry
{
	const char *name;
	int			val;
	bool		hidden;
};

/*
 * Signatures for per-variable check/assign/show hook functions
 */
typedef bool (*GucBoolCheckHook) (bool *newval, void **extra, GucSource source);
typedef bool (*GucIntCheckHook) (int *newval, void **extra, GucSource source);
typedef bool (*GucRealCheckHook) (double *newval, void **extra, GucSource source);
typedef bool (*GucStringCheckHook) (char **newval, void **extra, GucSource source);
typedef bool (*GucEnumCheckHook) (int *newval, void **extra, GucSource source);


typedef void (*GucBoolAssignHook) (bool newval, void *extra);
typedef void (*GucIntAssignHook) (int newval, void *extra);
typedef void (*GucRealAssignHook) (double newval, void *extra);
typedef void (*GucStringAssignHook) (const char *newval, void *extra);
typedef void (*GucEnumAssignHook) (int newval, void *extra);

typedef const char *(*GucShowHook) (void);

/*
 * Miscellaneous
 */
typedef enum
{
	/* Types of set_config_option actions */
	GUC_ACTION_SET,				/* regular SET command */
	GUC_ACTION_LOCAL,			/* SET LOCAL command */
	GUC_ACTION_SAVE				/* function SET option, or temp assignment */
} GucAction;

#define GUC_QUALIFIER_SEPARATOR '.'

/*
 * bit values in "flags" of a GUC variable
 */
#define GUC_LIST_INPUT			0x0001	/* input can be list format */
#define GUC_LIST_QUOTE			0x0002	/* double-quote list elements */
#define GUC_NO_SHOW_ALL			0x0004	/* exclude from SHOW ALL */
#define GUC_NO_RESET_ALL		0x0008	/* exclude from RESET ALL */
#define GUC_REPORT				0x0010	/* auto-report changes to client */
#define GUC_NOT_IN_SAMPLE		0x0020	/* not in postgresql.conf.sample */
#define GUC_DISALLOW_IN_FILE	0x0040	/* can't set in postgresql.conf */
#define GUC_CUSTOM_PLACEHOLDER	0x0080	/* placeholder for custom variable */
#define GUC_SUPERUSER_ONLY		0x0100	/* show only to superusers */
#define GUC_IS_NAME				0x0200	/* limit string to NAMEDATALEN-1 */
#define GUC_NOT_WHILE_SEC_REST	0x0400	/* can't set if security restricted */
#define GUC_DISALLOW_IN_AUTO_FILE 0x0800	/* can't set in
											 * PG_AUTOCONF_FILENAME */

#define GUC_UNIT_KB				0x1000	/* value is in kilobytes */
#define GUC_UNIT_BLOCKS			0x2000	/* value is in blocks */
#define GUC_UNIT_XBLOCKS		0x3000	/* value is in xlog blocks */
#define GUC_UNIT_MB				0x4000	/* value is in megabytes */
#define GUC_UNIT_BYTE			0x8000	/* value is in bytes */
#define GUC_UNIT_MEMORY			0xF000	/* mask for size-related units */

#define GUC_UNIT_MS			   0x10000	/* value is in milliseconds */
#define GUC_UNIT_S			   0x20000	/* value is in seconds */
#define GUC_UNIT_MIN		   0x30000	/* value is in minutes */
#define GUC_UNIT_TIME		   0xF0000	/* mask for time-related units */

#define GUC_UNIT				(GUC_UNIT_MEMORY | GUC_UNIT_TIME)

/* POLAR: Shared Server */
#define GUC_SESSION_DEDICATED   0x100000	/* guc need in dedicated mode*/
#define GUC_ASSIGN_IN_TRANS		0x200000	/* assign need in trans */
/* POLAR end */

/* POLAR */
#define MAX_READ_AHEAD_XLOGS	200
#define MAX_NUM_OF_PARALLEL_BGWRITER	16
/* bulk io read/write */
#define POLAR_MAX_BULK_IO_SIZE  64
#define POLAR_DEFAULT_MAX_AUDIT_LOG_LEN 8192
/* POLAR end */

/* POLAR */
#define CORE_DUMP_PRINT_MASK 0x0001
#define CORE_DUMP_CLEAR_MASK 0x0010
#define POLAR_CORE_DUMP_DISABLE 0x0000
#define POLAR_CORE_DUMP_PRINT CORE_DUMP_PRINT_MASK
#define POLAR_CORE_DUMP_CLEAR CORE_DUMP_CLEAR_MASK
#define POLAR_CORE_DUMP_ALL (CORE_DUMP_PRINT_MASK | CORE_DUMP_CLEAR_MASK)

#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))
/* POLAR end */

/* POLAR */
typedef enum
{
	POLAR_DELAY_DML_ONCE,
	POLAR_DELAY_DML_MULTI,
	POLAR_DELAY_DML_OFF
} PolarDelayDmlType;

typedef enum
{
	POLAR_NBLOCKS_CACHE_ALL_MODE,
	POLAR_NBLOCKS_CACHE_BITMAPSCAN_MODE,
	POLAR_NBLOCKS_CACHE_SCAN_MODE,
	POLAR_NBLOCKS_CACHE_OFF_MODE
} PolarBlockCacheType;
/* POLAR end */

/* POLAR px */
typedef struct PxFunctionOidArray {
	int	count;
	int	oid[FLEXIBLE_ARRAY_MEMBER];
} PxFunctionOidArray;

/* GUC vars that are actually declared in guc.c, rather than elsewhere */
extern bool log_duration;
extern bool Debug_print_plan;
extern bool Debug_print_parse;
extern bool Debug_print_rewritten;
extern bool Debug_pretty_print;

extern bool log_parser_stats;
extern bool log_planner_stats;
extern bool log_executor_stats;
extern bool log_statement_stats;
extern bool log_btree_build_stats;

extern PGDLLIMPORT bool check_function_bodies;
extern bool default_with_oids;
extern bool session_auth_is_superuser;

extern int	log_min_error_statement;
extern PGDLLIMPORT int log_min_messages;
extern PGDLLIMPORT int client_min_messages;
extern int	log_min_duration_statement;
extern int	log_temp_files;

extern int	temp_file_limit;

extern int	num_temp_buffers;

extern char *cluster_name;
extern PGDLLIMPORT char *ConfigFileName;
extern char *HbaFileName;
extern char *IdentFileName;
extern char *external_pid_file;

extern char *polar_internal_allowed_roles;

extern PGDLLIMPORT char *application_name;

extern int	tcp_keepalives_idle;
extern int	tcp_keepalives_interval;
extern int	tcp_keepalives_count;

#ifdef TRACE_SORT
extern bool trace_sort;
#endif

/* POLAR GUCs */
extern bool polar_force_unlogged_to_logged_table;
extern bool polar_allow_huge_alloc;
extern bool polar_super_run_as_secdef;
extern bool polar_hold_truncate_interrupt;
extern bool polar_replay_fpi_check_lsn;

extern char *polar_available_extensions;
extern char *polar_forbidden_extensions;
extern char *polar_internal_allowed_extensions;
extern char *polar_auto_cascade_extensions;
extern bool polar_enable_polar_superuser;
extern bool polar_apply_global_guc_for_super;
extern bool polar_enable_inline_cte;
/* dbaas manager */
extern bool polar_force_trans_ro_non_sup;
extern int     polar_max_non_super_conns;
extern int     polar_max_super_conns;
extern int     polar_reserved_polar_super_conns;

extern char	   *polar_release_date;
extern char	   *polar_version;
extern char	   *polar_instance_name;

/* POLAR */
extern double	polar_max_normal_backends_factor;

extern bool	polar_suppress_preload_error;
extern bool polar_log_statement_with_duration;
extern int	polar_max_log_files;
extern int  polar_max_auditlog_files;
extern int  polar_max_slowlog_files;
extern int  polar_max_logindex_files;
extern bool	polar_suppress_preload_error;
/* POLAR: timeout for sync replication */
extern int  polar_sync_replication_timeout;
extern int  polar_sync_rep_timeout_break_lsn_lag;
extern int  polar_semi_sync_observation_window;
extern int  polar_semi_sync_max_backoff_window;
extern int  polar_semi_sync_min_backoff_window;
extern bool polar_enable_semi_sync_optimization;

extern int		polar_hostid;
extern int		polar_bulk_extend_size;
extern int		polar_min_bulk_extend_table_size;
extern int		polar_bulk_read_size;
extern int		polar_recovery_bulk_extend_size;
extern bool 	polar_enable_master_recovery_bulk_extend;
extern int		polar_index_create_bulk_extend_size;
extern int		polar_index_bulk_extend_size;
extern int		polar_csnlog_slot_size;
extern int		polar_clog_slot_size;
extern int     	polar_committs_buffer_slot_size;
extern int		polar_mxact_offset_buffer_slot_size;
extern int		polar_mxact_member_buffer_slot_size;
extern int 		polar_subtrans_buffer_slot_size;
extern int		polar_async_buffer_slot_size;
extern int		polar_oldserxid_buffer_slot_size;
extern char		*polar_datadir;
extern char		*polar_disk_name;
extern char		*polar_storage_cluster_name;
extern bool		polar_enable_shared_storage_mode;
extern bool		polar_enable_ddl_sync_mode;
extern bool		polar_enable_transaction_sync_mode;
extern bool		polar_use_statistical_relpages;
extern bool		polar_enable_debug;
extern bool		polar_enable_pwrite;
extern bool		polar_enable_pread;
extern bool		polar_enable_parallel_replay_standby_mode;
extern bool		polar_enable_fallocate_walfile;
extern bool		polar_skip_fill_walfile_zero_page;
extern int 		polar_auditlog_max_query_length;
extern int		polar_audit_log_flush_timeout;
extern int 		polar_clog_max_local_cache_segments;
extern int 		polar_logindex_max_local_cache_segments;
extern int 		polar_trace_logindex_messages;
extern int 		polar_commit_ts_max_local_cache_segments;
extern int 		polar_multixact_max_local_cache_segments;
extern int 		polar_csnlog_max_local_cache_segments;
extern int 		polar_unit_test_mem_size;
extern int 		polar_parallel_replay_proc_num;
extern int 		polar_parallel_replay_task_queue_depth;
/* POLAR end */

extern int		polar_check_checkpoint_legal_interval;
extern int		polar_copy_buffers;
extern bool		polar_enable_copy_buffer;
extern bool		polar_enable_flushlist;
extern int		polar_bgwriter_max_batch_size;
extern int		polar_bgwriter_batch_size_flushlist;
extern bool		polar_force_flush_buffer;
extern int		polar_buffer_copy_min_modified_count;
extern int		polar_buffer_copy_lsn_lag_with_cons_lsn;
extern bool		polar_enable_normal_bgwriter;
extern int		polar_bgwriter_sleep_lsn_lag;
extern bool		polar_enable_control_vm_flush;
extern bool		polar_enable_lazy_checkpoint;
extern int		polar_read_ahead_xlog_num;
extern bool		polar_enable_master_xlog_read_ahead;
extern bool		polar_enable_parallel_bgwriter;
extern int		polar_parallel_bgwriter_workers;
extern int		polar_parallel_bgwriter_delay;
extern int		polar_parallel_bgwriter_check_interval;
extern int		polar_parallel_new_bgwriter_threshold_lag;
extern int		polar_parallel_new_bgwriter_threshold_time;
extern bool		polar_parallel_bgwriter_enable_dynamic;
extern int 		polar_logindex_table_batch_size;
extern bool		polar_force_change_checkpoint;
extern bool		polar_enable_resolve_conflict;
extern int		polar_bg_replay_batch_size;
extern bool		polar_enable_maxscale_support;
extern bool		polar_enable_xlog_buffer;
extern int		polar_enable_coredump_handler;
extern bool		polar_dropdb_write_wal_beforehand;
extern bool		polar_enable_full_page_write_in_backup;
extern bool		polar_enable_lazy_checkpoint_in_backup;
extern bool		polar_enable_checkpoint_in_backup;
extern bool		polar_enable_switch_wal_in_backup;
extern bool		polar_enable_create_backup_history_file_in_backup;
extern bool 	polar_enable_persisted_logical_slot;
extern bool		polar_enable_persisted_physical_slot;
extern bool		polar_enable_persisted_spill_file;
extern bool		polar_enable_early_launch_checkpointer;
extern bool 	polar_enable_keep_wal_ready_file;
extern bool		polar_enable_node_static_config;
extern bool 	polar_replica_multi_version_snapshot_enable;
extern int  	polar_replica_multi_version_snapshot_slot_num;
extern int  	polar_replica_multi_version_snapshot_retry_times;
extern bool		polar_enable_simply_redo_error_log;
extern bool		polar_enable_virtual_pid;
extern int  	polar_delay_dml_option;
extern int  	polar_primary_dml_delay;
extern int  	polar_delay_dml_lsn_lag_threshold;
extern bool     polar_enable_slru_hash_index;
extern int		polar_fullpage_snapshot_min_modified_count;
extern int		polar_fullpage_snapshot_replay_delay_threshold;
extern int		polar_fullpage_snapshot_oldest_lsn_delay_threshold;
extern int		polar_wait_old_version_page_timeout;
extern int		polar_write_logindex_active_table_delay;
extern bool		polar_enable_fullpage_snapshot;
extern int		polar_startup_replay_delay_size;
extern int		polar_fullpage_keep_segments;
extern bool 	polar_enable_persisted_buffer_pool;
extern bool 	polar_enable_track_lock_stat;
extern bool     polar_enable_track_lock_timing;
extern bool     polar_enable_track_network_stat;
extern bool 	polar_enable_track_network_timing;
extern bool		polar_enable_early_launch_parallel_bgwriter;
extern bool 	polar_enable_alb_client_address;
extern bool		polar_shutdown_walsnd_wait_non_super;
extern int		polar_shutdown_walsnd_wait_replication_kind;
extern bool		polar_enable_stat_wait_info;
extern bool		polar_super_call_all_trigger_event;
extern bool		polar_enable_lazy_end_of_recovery_checkpoint;
extern bool		polar_enable_async_ddl_lock_replay;
extern int		polar_async_ddl_lock_replay_worker_num;
extern bool    	polar_enable_track_sql_time_stat;
extern bool		polar_create_table_with_full_replica_identity;
extern bool		polar_enable_audit_log_bind_sql_parameter;
extern bool		polar_enable_audit_log_bind_sql_parameter_new;
extern bool     polar_enable_standby_pbp;
extern bool     polar_enable_master_pbp;
extern int		polar_save_stack_info_level;
extern int		polar_replica_redo_gap_limit;
/* POLAR datamax */
extern int		polar_datamax_remove_archivedone_wal_timeout;
extern int		polar_datamax_archive_timeout;
extern int		polar_datamax_save_replication_slots_timeout;
extern int		polar_datamax_prealloc_walfile_timeout;
extern int		polar_datamax_prealloc_walfile_num;
extern bool		polar_enable_replica_use_smgr_cache;
extern bool		polar_enable_standby_use_smgr_cache;
extern int 		polar_nblocks_cache_mode;

/* POLAR */
extern bool		polar_csn_enable;
extern bool 	polar_csn_elog_panic_enable;
extern bool		polar_csnlog_upperbound_enable;
extern bool 	polar_csn_xid_snapshot;
extern bool		polar_droptbl_write_wal_beforehand;
extern bool		px_enable_check_workers;
extern bool		px_info_debug;
extern bool		px_enable_replay_wait;
extern bool 	px_enable_transaction;
extern bool		px_enable_plan_cache;
extern bool		px_enable_sort_distinct;
extern bool		px_enable_join_prefetch_inner;
extern int		px_max_workers_number;
extern bool		px_enable_plpgsql;
extern bool		px_enable_procedure;
extern bool		px_enable_prepare_statement;
extern bool		px_enable_check_csn;
extern bool		px_enable_create_table_as;

extern bool		polar_droptbl_write_wal_beforehand;
extern bool 	polar_enable_localfs_test_mode;
extern bool		polar_enable_convert_or_to_union_all;
extern bool 	polar_publish_via_partition_root;
extern bool		px_enable_sethintbits;
extern bool		polar_enable_buffer_alignment;
extern bool 	polar_enable_ro_prewarm;
extern bool		polar_enable_create_table_as_bulk_insert;
extern int		polar_dma_max_standby_wait_delay_size_mb;
extern char		*polar_partition_recursive_reloptions;
extern bool		polar_enable_dump_incorrect_checksum_xlog;
extern bool 	polar_trace_heap_scan_flow;

/* POLAR end */

/* POLAR */
extern bool 	polar_enable_promote_wait_for_walreceive_done;
extern bool     polar_enable_send_stop;
/* POLAR end */

/* POLAR: GUCs for transaction rw-split begin */
extern bool		polar_enable_xact_split;
extern bool		polar_enable_xact_split_debug;
extern bool		polar_xact_split_enable_sethintbits;
extern char	   *polar_xact_split_xids;
/* POLAR: GUCs for transaction rw-split end */

extern bool		polar_enable_flashback_drop;
extern bool		polar_enable_shm_aset;

/* POLAR wal pipeline */

/*
 * For now, we just use native PG spin lock strategy to tune 
 * wal pipeline spin and wait. 
 */

/* 
 * 1000 spin corresponds to 4us, we should spin at most 1ms 
 * 0 indicates no spin
 */
#define POLAR_MIN_WAIT_SPINS 	0
#define POLAR_MAX_WAIT_SPINS 	250000

/*
 * how long does it take to wake up on timeout, depending on timeout:
 *	1us ->    57us
 *	10us ->   66us
 *	20us ->   76us
 *	50us ->   106us
 *	100us ->  156us
 *	1000us -> 1100us
 *	reference MySQL 8.0
 *  0 indicates no timeout
 */
#define POLAR_MIN_WAIT_TIMEOUT_USEC		0
#define POLAR_MAX_WAIT_TIMEOUT_USEC		100000

extern bool polar_wal_pipeline_enable;
extern int  polar_wal_pipeline_mode;
extern bool polar_wal_pipeline_wait_object_align;
extern int	polar_wal_pipeline_wait_timeout;
extern int	polar_wal_pipeline_flush_event_array_size;
extern int	polar_wal_pipeline_flush_event_slot_size;
extern int  polar_wal_pipeline_unflushed_xlog_array_size;
extern int  polar_wal_pipeline_recent_written_array_size;

extern int polar_wal_pipeline_commit_wait_spin_delay;
extern int polar_wal_pipeline_commit_wait_timeout;

extern int polar_wal_pipeline_advance_worker_spin_delay;
extern int polar_wal_pipeline_advance_worker_timeout;
extern int polar_wal_pipeline_advance_worker_write_max_size;

extern int polar_wal_pipeline_write_worker_spin_delay;
extern int polar_wal_pipeline_write_worker_timeout;

extern int polar_wal_pipeline_flush_worker_spin_delay;
extern int polar_wal_pipeline_flush_worker_timeout;

extern int polar_wal_pipeline_notify_worker_spin_delay;
extern int polar_wal_pipeline_notify_worker_timeout;
#define POLAR_WAL_PIPELINE_NOTIFY_WORKER_NUM_MAX	4
#define POLAR_WAL_PIPELINE_NOTIFY_WORKER_NUM_MIN	1
extern int polar_wal_pipeline_notify_worker_num;

/*
 * POLAR: crash recovery rto optimizer
 */
extern double polar_crash_recovery_rto_threshold;
extern int polar_crash_recovery_rto;
extern int polar_crash_recovery_rto_delay_count;
extern int polar_io_read_throughtput_userset;
extern int polar_crash_recovery_rto_statistics_count;

/*
 * POLAR
 * instance specification for cpu and memory
 */
extern int polar_instance_spec_cpu;
extern int polar_instance_spec_mem;
extern int polar_instance_spec_normal_mem;

/* POLAR: operator level mem limit */
extern int polar_max_dsm_request_size;
extern int polar_max_hashagg_mem;
extern int polar_max_setop_mem;
extern int polar_max_subplan_mem;
extern int polar_max_recursiveunion_mem;
extern bool	polar_enable_operator_mem_limit;
extern bool	polar_enable_operator_mem_limit_by_level;

/* New feature should add macro below */

/* Polar wal pipeline should enable on instanch with cpu vcores >= 8 */
#define POLAR_INSTANCE_SPEC_WAL_PIPELINE_IS_AVAILABLE()	(polar_instance_spec_cpu == 0 || polar_instance_spec_cpu >= 8)

/*
 * Functions exported by guc.c
 */
extern void SetConfigOption(const char *name, const char *value,
				GucContext context, GucSource source);

extern void DefineCustomBoolVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 bool *valueAddr,
						 bool bootValue,
						 GucContext context,
						 int flags,
						 GucBoolCheckHook check_hook,
						 GucBoolAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomIntVariable(
						const char *name,
						const char *short_desc,
						const char *long_desc,
						int *valueAddr,
						int bootValue,
						int minValue,
						int maxValue,
						GucContext context,
						int flags,
						GucIntCheckHook check_hook,
						GucIntAssignHook assign_hook,
						GucShowHook show_hook);

extern void DefineCustomRealVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 double *valueAddr,
						 double bootValue,
						 double minValue,
						 double maxValue,
						 GucContext context,
						 int flags,
						 GucRealCheckHook check_hook,
						 GucRealAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomStringVariable(
						   const char *name,
						   const char *short_desc,
						   const char *long_desc,
						   char **valueAddr,
						   const char *bootValue,
						   GucContext context,
						   int flags,
						   GucStringCheckHook check_hook,
						   GucStringAssignHook assign_hook,
						   GucShowHook show_hook);

extern void DefineCustomEnumVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 int *valueAddr,
						 int bootValue,
						 const struct config_enum_entry *options,
						 GucContext context,
						 int flags,
						 GucEnumCheckHook check_hook,
						 GucEnumAssignHook assign_hook,
						 GucShowHook show_hook);

extern void EmitWarningsOnPlaceholders(const char *className);

extern const char *GetConfigOption(const char *name, bool missing_ok,
				bool restrict_privileged);
extern const char *GetConfigOptionResetString(const char *name);
extern int	GetConfigOptionFlags(const char *name, bool missing_ok);
extern void ProcessConfigFile(GucContext context);
extern void InitializeGUCOptions(void);
extern bool SelectConfigFiles(const char *userDoption, const char *progname);
extern void ResetAllOptions(void);
extern void AtStart_GUC(void);
extern int	NewGUCNestLevel(void);
extern void AtEOXact_GUC(bool isCommit, int nestLevel);
extern void BeginReportingGUCOptions(void);
extern void ParseLongOption(const char *string, char **name, char **value);
extern bool parse_int(const char *value, int *result, int flags,
		  const char **hintmsg);
extern bool parse_real(const char *value, double *result);
extern int set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  GucAction action, bool changeVal, int elevel,
				  bool is_reload);
extern void AlterSystemSetConfigFile(AlterSystemStmt *setstmt);
extern char *GetConfigOptionByName(const char *name, const char **varname,
					  bool missing_ok);
extern void GetConfigOptionByNum(int varnum, const char **values, bool *noshow);
extern int	GetNumConfigOptions(void);

extern void SetPGVariable(const char *name, List *args, bool is_local);
extern void GetPGVariable(const char *name, DestReceiver *dest);
extern TupleDesc GetPGVariableResultDesc(const char *name);

extern void ExecSetVariableStmt(VariableSetStmt *stmt, bool isTopLevel);
extern char *ExtractSetVariableArgs(VariableSetStmt *stmt);

extern void ProcessGUCArray(ArrayType *array,
				GucContext context, GucSource source, GucAction action);
extern ArrayType *GUCArrayAdd(ArrayType *array, const char *name, const char *value);
extern ArrayType *GUCArrayDelete(ArrayType *array, const char *name);
extern ArrayType *GUCArrayReset(ArrayType *array);

#ifdef EXEC_BACKEND
extern void write_nondefault_variables(GucContext context);
extern void read_nondefault_variables(void);
#endif

extern void *guc_malloc(int elevel, size_t size);

/* GUC serialization */
extern Size EstimateGUCStateSpace(void);
extern void SerializeGUCState(Size maxsize, char *start_address);
extern void RestoreGUCState(void *gucstate);

/* Support for messages reported from GUC check hooks */

extern PGDLLIMPORT char *GUC_check_errmsg_string;
extern PGDLLIMPORT char *GUC_check_errdetail_string;
extern PGDLLIMPORT char *GUC_check_errhint_string;

extern void GUC_check_errcode(int sqlerrcode);

#define GUC_check_errmsg \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errmsg_string = format_elog_string

#define GUC_check_errdetail \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errdetail_string = format_elog_string

#define GUC_check_errhint \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errhint_string = format_elog_string


/*
 * The following functions are not in guc.c, but are declared here to avoid
 * having to include guc.h in some widely used headers that it really doesn't
 * belong in.
 */

/* in commands/tablespace.c */
extern bool check_default_tablespace(char **newval, void **extra, GucSource source);
extern bool check_temp_tablespaces(char **newval, void **extra, GucSource source);
extern void assign_temp_tablespaces(const char *newval, void *extra);

/* in catalog/namespace.c */
extern bool check_search_path(char **newval, void **extra, GucSource source);
extern void assign_search_path(const char *newval, void *extra);

/* in access/transam/xlog.c */
extern bool check_wal_buffers(int *newval, void **extra, GucSource source);
extern void assign_xlog_sync_method(int new_sync_method, void *extra);

/* POLAR */
extern bool polar_check_is_forbidden_funcs(List *funcname);
extern void polar_process_polar_node_static_config(void);
extern void polar_create_polar_node_static_config(const char *confpath);
extern struct config_generic *polar_parameter_check_name_internal(const char* guc_name);
extern bool polar_parameter_check_value_internal(const char* guc_name, const char* guc_value);
extern int	guc_name_compare(const char *namea, const char *nameb);

/* POLAR px */
extern bool px_execute_pruned_plan;
extern bool	px_enable_print;
extern bool px_debug_cancel_print;
extern bool px_log_dispatch_stats;
extern bool px_optimizer_enable_relsize_collection;

/* Macros to define the level of memory accounting to show in EXPLAIN ANALYZE */
#define EXPLAIN_MEMORY_VERBOSITY_SUPPRESS	0 /* Suppress memory reporting in explain analyze */
#define EXPLAIN_MEMORY_VERBOSITY_SUMMARY	1 /* Summary of memory usage for each owner in explain analyze */
#define EXPLAIN_MEMORY_VERBOSITY_DETAIL		2 /* Detail memory accounting tree for each slice in explain analyze */

/* Optimizer related gucs */
extern bool	polar_enable_px;
extern bool	px_enable_executor;
extern bool px_enable_join;
extern bool px_enable_window_function;
extern bool px_enable_subquery;
extern int  px_dop_per_node;
extern bool px_enable_cte;
extern bool		px_enable_spi_read_all_namespaces;

/* ORCA related definitions */
#define OPTIMIZER_XFORMS_COUNT	400	/* number of transformation rules */

/* types of optimizer failures */
#define OPTIMIZER_ALL_FAIL 			0	/* all failures */
#define OPTIMIZER_UNEXPECTED_FAIL 	1	/* unexpected failures */
#define OPTIMIZER_EXPECTED_FAIL 	2	/* expected failures */

/* optimizer minidump mode */
#define OPTIMIZER_MINIDUMP_FAIL		0	/* create optimizer minidump on failure */
#define OPTIMIZER_MINIDUMP_ALWAYS 	1	/* always create optimizer minidump */

/* optimizer cost model */
#define OPTIMIZER_GPDB_CALIBRATED	1	/* GPDB's calibrated cost model */
#define OPTIMIZER_POLARDB			2	/* PolarDB's cost model */

/* PX speific */
#define GUC_PX_NEED_SYNC		0x00400000	/* guc value is synced between master and primary */
#define GUC_PX_NO_SYNC			0x00800000  /* guc value is not synced between master and primary */

/* ORCA related definitions */
#define OPTIMIZER_XFORMS_COUNT	400 /* number of transformation rules */

/* array of xforms disable flags */
extern bool optimizer_xforms[OPTIMIZER_XFORMS_COUNT];
extern char *px_optimizer_search_strategy_path;

/* GUCs to tell Optimizer to enable a physical operator */
extern bool px_optimizer_enable_indexjoin;
extern bool px_optimizer_enable_motions_masteronly_queries;
extern bool px_optimizer_enable_motions;
extern bool px_optimizer_enable_motion_broadcast;
extern bool px_optimizer_enable_motion_gather;
extern bool px_optimizer_enable_motion_redistribute;
extern bool px_optimizer_enable_sort;
extern bool px_optimizer_enable_materialize;
extern bool px_optimizer_enable_partition_propagation;
extern bool px_optimizer_enable_partition_selection;
extern bool px_optimizer_enable_outerjoin_rewrite;
extern bool px_optimizer_enable_multiple_distinct_aggs;
extern bool px_optimizer_enable_hashjoin_redistribute_broadcast_children;
extern bool px_optimizer_enable_broadcast_nestloop_outer_child;
extern bool px_optimizer_enable_streaming_material;
extern bool px_optimizer_enable_gather_on_segment_for_dml;
extern bool px_optimizer_enable_assert_maxonerow;
extern bool px_optimizer_enable_constant_expression_evaluation;
extern bool px_optimizer_enable_outerjoin_to_unionall_rewrite;
extern bool px_optimizer_enable_ctas;
extern bool px_optimizer_enable_dml;
extern bool px_optimizer_enable_dml_triggers;
extern bool	px_optimizer_enable_dml_constraints;
extern bool px_optimizer_enable_direct_dispatch;
extern bool px_optimizer_enable_master_only_queries;
extern bool px_optimizer_enable_dynamictablescan;
extern bool px_optimizer_enable_dynamicindexscan;
extern bool px_optimizer_expand_fulljoin;
extern bool px_optimizer_enable_hashagg;
extern bool px_optimizer_enable_groupagg;
extern bool px_optimizer_enable_mergejoin;
extern bool px_optimizer_enable_hashjoin;
extern bool	px_optimizer_enable_seqscan;
extern bool	px_optimizer_enable_seqsharescan;
extern bool	px_optimizer_enable_indexscan;
extern bool	px_optimizer_enable_shareindexscan;
extern bool	px_optimizer_enable_dynamicshareindexscan;
extern bool	px_optimizer_enable_indexonlyscan;
extern bool px_optimizer_enable_brinscan;
extern bool	px_optimizer_enable_bitmapscan;
extern bool px_optimizer_enable_nestloopjoin;
extern bool px_optimizer_enable_lasj_notin;
extern bool px_optimizer_enable_crossproduct;

/* Optimizer related gucs */
extern bool	px_optimizer_log;
extern int  px_optimizer_log_failure;
extern bool	px_optimizer_trace_fallback;
extern int  px_optimizer_cost_model;
extern bool px_optimizer_metadata_caching;
extern int	px_optimizer_mdcache_size;

/* Optimizer debugging GUCs */
extern bool px_optimizer_print_query;
extern bool px_optimizer_print_plan;
extern bool px_optimizer_print_xform;
extern bool	px_optimizer_print_memo_after_exploration;
extern bool	px_optimizer_print_memo_after_implementation;
extern bool	px_optimizer_print_memo_after_optimization;
extern bool	px_optimizer_print_job_scheduler;
extern bool	px_optimizer_print_expression_properties;
extern bool	px_optimizer_print_group_properties;
extern bool	px_optimizer_print_optimization_context;
extern bool px_optimizer_print_optimization_stats;
extern bool px_optimizer_print_xform_results;
extern bool px_optimizer_print_memo_enforcement;
extern bool px_optimizer_print_required_columns;
extern bool px_optimizer_print_equiv_distr_specs;

/* Optimizer plan enumeration related GUCs */
extern bool px_optimizer_enumerate_plans;
extern bool px_optimizer_sample_plans;
extern int	px_optimizer_plan_id;
extern int	px_optimizer_samples_number;

/* Cardinality estimation related GUCs used by the Optimizer */
extern bool px_optimizer_extract_dxl_stats;
extern bool px_optimizer_extract_dxl_stats_all_nodes;
extern bool px_optimizer_print_missing_stats;
extern bool px_optimizer_dpe_stats;
extern bool px_optimizer_enable_derive_stats_all_groups;
extern double px_optimizer_damping_factor_filter;
extern double px_optimizer_damping_factor_join;
extern double px_optimizer_damping_factor_groupby;

/* Costing or tuning related GUCs used by the Optimizer */
extern int px_optimizer_segments;
extern int px_optimizer_penalize_broadcast_threshold;
extern double px_optimizer_cost_threshold;
extern double px_optimizer_nestloop_factor;
extern double px_optimizer_sort_factor;
extern double px_optimizer_share_tablescan_factor;
extern double px_optimizer_share_indexscan_factor;

/* Optimizer hints */
extern int px_optimizer_array_expansion_threshold;
extern int px_optimizer_join_order_threshold;
extern int px_optimizer_join_arity_for_associativity_commutativity;
extern int px_optimizer_cte_inlining_bound;
extern int px_optimizer_push_group_by_below_setop_threshold;
extern bool px_optimizer_force_multistage_agg;
extern bool px_optimizer_force_expanded_distinct_aggs;
extern bool px_optimizer_force_agg_skew_avoidance;
extern bool px_optimizer_penalize_skew;
extern bool px_optimizer_prune_computed_columns;
extern bool px_optimizer_push_requirements_from_consumer_to_producer;
extern bool px_optimizer_enforce_subplans;
extern bool px_optimizer_apply_left_outer_to_union_all_disregarding_stats;
extern bool px_optimizer_use_external_constant_expression_evaluation_for_ints;
extern bool px_optimizer_remove_order_below_dml;
extern bool px_optimizer_multilevel_partitioning;
extern bool px_optimizer_cte_inlining;
extern bool px_optimizer_enable_space_pruning;
extern bool px_optimizer_use_px_allocators;
extern bool px_enable_opfamily_for_distribution;

extern int	px_optimizer_minidump;
extern int	px_optimizer_join_order;
extern bool	px_optimizer_force_three_stage_scalar_dqa;
extern bool	px_optimizer_parallel_union;
extern bool	px_optimizer_array_constraints;
extern bool	px_optimizer_enable_eageragg;
extern bool	px_optimizer_prune_unused_columns;
extern bool	px_optimizer_enable_associativity;

/* GUCs for slice table*/
extern int	px_max_slices;
extern char *polar_px_nodes;
extern char *polar_px_ignore_function;
extern PxFunctionOidArray *px_function_oid_array;
extern bool	px_use_master;
extern bool	px_use_standby;
extern bool	polar_px_ignore_unusable_nodes;
extern bool	polar_enable_send_node_info;
extern bool	polar_enable_send_cluster_info;

/* The number of blocks to scan table */
extern int 	px_scan_unit_size;
extern int 	px_scan_unit_bit;
extern bool px_enable_adaptive_scan;
extern bool px_enable_adps_explain_analyze;

extern bool	px_interconnect_udpic_network_enable_ipv6;
extern bool px_enable_dispatch_async;
extern bool	px_enable_udp_testmode;
extern bool	px_enable_tcp_testmode;
extern bool px_enable_remove_redundant_results;
extern bool px_enable_btbuild;
extern bool px_enable_btbuild_cic_phase2;
extern int 	px_btbuild_batch_size;
extern int	px_btbuild_mem_size;
extern int	px_btbuild_queue_size;
extern bool px_enable_cte_shared_scan;
extern int	polar_bt_write_page_buffer_size;
extern bool px_enable_partition;
extern bool px_enable_partition_hash;
extern bool px_enable_relsize_collection;
extern bool px_enable_tableless_scan;
extern bool px_enable_pre_optimizer_check;
extern bool px_enable_left_index_nestloop_join;
extern bool px_enable_result_hash_filter;
extern bool px_enable_insert_select;
extern bool px_enable_insert_partition_table;
extern int	px_wait_lock_timeout;
extern int	px_insert_dop_num;
extern bool px_enable_insert_from_tableless;
extern bool	px_enable_insert_order_sensitive;
extern bool px_enable_partitionwise_join;
extern bool px_enable_update;
extern int	px_update_dop_num;
extern bool px_enable_delete;
extern int	px_delete_dop_num;
extern bool px_optimizer_remove_superfluous_order;

extern bool	px_allow_strat_seqscan;
extern bool	px_allow_strat_seqscan;
extern bool	px_allow_strat_bitmapscan;
extern bool	px_allow_sync_seqscan;
extern bool	px_allow_sync_bitmapscan;
extern bool	px_allow_pagemode_seqscan;
extern bool	px_allow_pagemode_bitmapscan;

/* optimizer join heuristic models */
#define JOIN_ORDER_IN_QUERY				0
#define JOIN_ORDER_GREEDY_SEARCH		1
#define JOIN_ORDER_EXHAUSTIVE_SEARCH	2
#define JOIN_ORDER_EXHAUSTIVE2_SEARCH	3

/* POLAR end */

#endif							/* GUC_H */
