# src/test/modules/Makefile

subdir = src/test/modules
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		  brin \
		  commit_ts \
		  dummy_seclabel \
		  snapshot_too_old \
		  test_async_ddl_lock_replay \
		  test_bloomfilter \
		  test_buffer \
		  test_cancel_key \
		  test_csn \
		  test_csn_xact \
		  test_ddl_deparse \
		  test_extensions \
		  test_logindex \
		  test_multi_version_snapshot \
		  test_parser \
		  test_pbp \
		  test_pg_dump \
		  test_polar_datamax \
		  test_polar_directio \
		  test_predtest \
		  test_rbtree \
		  test_read_core_pattern \
		  test_rls_hooks \
		  test_shm_mq \
		  test_slru \
		  test_xids_snapshot_mvcc \
		  worker_spi \
		  test_polar_bulk_read \
		  test_px \
		  test_wal_pipeline \
		  test_flashback_log \
		  test_flashback_table \
		  test_dsa	\
		  test_polar_shm_aset

ifeq ($(enable_inject_faults),yes)
SUBDIRS += test_xact_split
endif

$(recurse)
