#!/usr/bin/perl

# 017_startup_bulk_read_wal.pl
#
# Copyright (c) 2024, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# IDENTIFICATION
#	  src/test/polar_pl/t/017_startup_bulk_read_wal.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# init primary
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf', 'wal_buffers = 16kB');
$node_primary->append_conf('postgresql.conf',
	'polar_xlog_queue_buffers = 1MB');
$node_primary->append_conf('postgresql.conf',
	'polar_startup_xlog_bulk_read_size = 128');

# init standby
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->polar_init_standby($node_primary);
$node_standby->append_conf('postgresql.conf',
	'polar_startup_xlog_bulk_read_size = 128');

$node_primary->start;
$node_primary->polar_create_slot($node_standby->name);
$node_standby->start;

$node_primary->pgbench_init(dbname => 'postgres', scale => 10);
$node_primary->safe_psql('postgres', 'CREATE EXTENSION polar_monitor;');
$node_primary->safe_psql('postgres', 'CHECKPOINT;');
my $wal_start_lsn;
my $replica_startup_pid;
my $standby_startup_pid;
my $xlog_read_count;
my $generated_wal_size = 32 * 1024 * 1024;    # 32MB at least
my $latency;

#
# primary/standby crash recovery
#
# Stop primary checkpointer and standby startup to hold WAL on both primary and
# standby. Then stop them immediately and restart. They should both get into
# crash recovery. After they are receiving connections, check their WAL read
# count. Should be greatly fewer than reading in XLOG_BLCKSZ.
#
$wal_start_lsn = $node_primary->lsn('insert');
$node_primary->stop_child('checkpointer');
$node_standby->stop_child('startup');
$latency = $node_primary->generate_wal($wal_start_lsn, $generated_wal_size);
$node_primary->stop('immediate');
$node_standby->stop('immediate');
$node_primary->start;
$xlog_read_count = $node_primary->safe_psql('postgres',
	"SELECT SUM(read_count) FROM polar_stat_io_info() where filetype = 'WAL' AND read_count > 0;"
);
ok( $xlog_read_count < $latency / 8192,
	"Primary crash recovery: read $xlog_read_count times for $latency bytes."
);

$node_standby->start;
$wal_start_lsn = $node_primary->lsn('insert');
$node_primary->wait_for_catchup($node_standby, 'replay', $wal_start_lsn);
$standby_startup_pid = $node_standby->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE backend_type = 'startup';");
$xlog_read_count = $node_standby->safe_psql('postgres',
	"SELECT SUM(read_count) FROM polar_stat_io_info() where filetype = 'WAL' AND pid = $standby_startup_pid;"
);
ok( $xlog_read_count < $latency / 8192,
	"Standby crash recovery: read $xlog_read_count times for $latency bytes."
);

#
# standby streaming
#
# Stop the startup on the standby, but not walreceiver. Then generate enough
# WAL on primary and resume the standby startup.
#
my $old_xlog_read_count = $node_standby->safe_psql('postgres',
	"SELECT SUM(read_count) FROM polar_stat_io_info() where filetype = 'WAL' AND pid = $standby_startup_pid;"
);
$node_standby->stop_child('startup');
$wal_start_lsn = $node_primary->lsn('insert');
$latency = $node_primary->generate_wal($wal_start_lsn, $generated_wal_size);
$node_standby->resume_child('startup');
$wal_start_lsn = $node_primary->lsn('insert');
$node_primary->wait_for_catchup($node_standby, 'replay', $wal_start_lsn);
$xlog_read_count = $node_standby->safe_psql('postgres',
	"SELECT SUM(read_count) FROM polar_stat_io_info() where filetype = 'WAL' AND pid = $standby_startup_pid;"
);
$xlog_read_count = $xlog_read_count - $old_xlog_read_count;
ok($xlog_read_count < $latency / 8192,
	"Standby streaming: read $xlog_read_count times for $latency bytes.");

#
# replica redo
#
# Stop primary checkpointer and hold some WAL. Then add a replica node and it
# will have to read the WAL from the beginning.
#
$node_primary->stop_child('checkpointer');
$wal_start_lsn = $node_primary->lsn('insert');
$latency = $node_primary->generate_wal($wal_start_lsn, $generated_wal_size);

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);
$node_replica->append_conf('postgresql.conf', 'wal_buffers = 16kB');
$node_replica->append_conf('postgresql.conf',
	'polar_xlog_queue_buffers = 1MB');
$node_replica->append_conf('postgresql.conf',
	'polar_startup_xlog_bulk_read_size = 128');
$node_primary->polar_create_slot($node_replica->name,
	immediately_reserve => 'true');
$node_replica->start;

$wal_start_lsn = $node_primary->lsn('insert');
$node_primary->wait_for_catchup($node_replica, 'replay', $wal_start_lsn);
$node_primary->resume_child('checkpointer');
$replica_startup_pid = $node_replica->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE backend_type = 'startup';");
$xlog_read_count = $node_replica->safe_psql('postgres',
	"SELECT SUM(read_count) FROM polar_stat_io_info() where filetype = 'WAL' AND pid = $replica_startup_pid;"
);
ok($xlog_read_count < $latency / 8192,
	"Replica redo: read $xlog_read_count times for $latency bytes.");

$node_primary->stop('fast');
$node_replica->stop('fast');
$node_standby->stop('fast');

done_testing();
