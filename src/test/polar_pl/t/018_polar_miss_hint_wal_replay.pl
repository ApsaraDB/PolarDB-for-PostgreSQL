# 018_polar_miss_hint_wal_replay.pl
#	  Test for replica miss replay of XLOG_FPI_FOR_HINT wal
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
#	  src/test/polar_pl/t/018_polar_miss_hint_wal_replay.pl

use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($Config{osname} eq 'MSWin32')
{
	# some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
elsif ($ENV{enable_injection_points} eq 'no')
{
	plan skip_all => 'Fault injector not supported by this build';
}
else
{
	plan tests => 2;
}

# primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf',
	"polar_lru_works_threshold = 0");

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);
$node_replica->append_conf('postgresql.conf', "logging_collector = off");
$node_replica->append_conf('postgresql.conf',
	"polar_parallel_bgwriter_delay = 1");
$node_replica->append_conf('postgresql.conf', "bgwriter_delay = 10");
$node_replica->append_conf('postgresql.conf', "checkpoint_timeout = 30");
$node_replica->append_conf('postgresql.conf', "full_page_writes = on");
$node_replica->append_conf('postgresql.conf', "log_checkpoints = on");
$node_replica->append_conf('postgresql.conf',
	"polar_enable_incremental_checkpoint = off");

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_replica->start;

$node_primary->safe_psql('postgres', 'create extension injection_points;');
$node_primary->safe_psql('postgres', 'create extension polar_monitor;');
$node_primary->safe_psql('postgres', 'CREATE TABLE test(val integer);');
$node_primary->safe_psql('postgres',
	"INSERT INTO test(val) SELECT generate_series(1,5) as newwal");
$node_primary->safe_psql('postgres', 'checkpoint;');

# inject fault to skip flush buffer to disk
$node_primary->safe_psql('postgres',
	"select injection_points_attach('polar_skip_flushbuffer', 'notice');");
$node_primary->safe_psql('postgres',
	'INSERT INTO test(val) SELECT generate_series(5,10) as newwal;');

$node_primary->wait_for_catchup($node_replica);
my $res = $node_replica->safe_psql('postgres', 'SELECT count(*) FROM test');
ok($res == 11, "replica replay data success");

# stop primary and inject fault to hang logindex dispatch and checkpoint
$node_primary->stop('i');

$node_replica->safe_psql('postgres',
	"select injection_points_attach('polar_delay_wal_replay', 'notice');");
$node_replica->safe_psql('postgres',
	"select injection_points_attach('polar_delay_checkpoint_guts', 'notice');"
);

$node_replica->promote;
$node_replica->polar_wait_for_startup(60);

my $logfile = $node_replica->logfile;
print "replica logfile: $logfile\n";
my $found = 0;
my @result;

while ($found == 0)
{
	sleep 1;
	@result = `grep -nr "checkpoint starting:" $logfile`;
	$found = @result;
}
print "new primary started checkpoint\n";

$node_replica->safe_psql('postgres',
	"select polar_mark_buffer_dirty_hint('test', 0);");
my $insert_ptr =
  $node_replica->safe_psql('postgres', "select pg_current_wal_insert_lsn()");
print "insert lsn after hint: $insert_ptr\n";

$node_replica->safe_psql('postgres',
	"select injection_points_detach('polar_delay_wal_replay');");
$node_replica->polar_drop_slot($node_replica->name);

# create new table to advance consistent_lsn
$node_replica->safe_psql('postgres', 'CREATE TABLE test2(val integer);');
$node_replica->safe_psql('postgres',
	'INSERT INTO test2(val) SELECT generate_series(1,5) as newwal');
my $lsn_diff = 0;

while ($lsn_diff <= 0)
{
	sleep 1;
	my $consistent_lsn =
	  $node_replica->safe_psql('postgres', "select polar_consistent_lsn();");
	$lsn_diff = $node_replica->safe_psql('postgres',
		"select pg_wal_lsn_diff('$consistent_lsn', '$insert_ptr');");
	print
	  "lsn diff between consistent_lsn:$consistent_lsn and hint_lsn:$insert_ptr is:$lsn_diff\n";
}

my $node_replica2 = PostgreSQL::Test::Cluster->new('replica2');
$node_replica2->polar_init_replica($node_replica);
$node_replica->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica2->name . "'");
$node_replica->polar_create_slot($node_replica2->name);

$node_replica2->start;
$node_replica2->polar_wait_for_startup(60);
$node_replica2->wait_walstreaming_establish_timeout(30);
$res = $node_replica2->safe_psql('postgres', 'SELECT count(*) FROM test');
print "new replica select res:$res\n";
ok($res == 11, "replica doesn't miss replay wal of XLOG_FPI_FOR_HINT");

$node_replica->safe_psql('postgres',
	"select injection_points_detach('polar_delay_checkpoint_guts');");
$node_replica->stop;
$node_replica2->stop;
