# 014_polar_fullpage_snapshot.pl
#	  test promote replica without fullpage directory
#
# Copyright (c) 2025, Alibaba Group Holding Limited
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
#	  src/test/polar_consistency/t/014_polar_fullpage_snapshot.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $db = 'postgres';

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf',
	'polar_enable_fullpage_snapshot=on');

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);
$node_replica->append_conf('postgresql.conf', "logging_collector = off");
$node_replica->append_conf('postgresql.conf',
	'polar_enable_fullpage_snapshot=on');
$node_replica->append_conf('postgresql.conf',
	'polar_fullpage_snapshot_replay_delay_threshold=0');
$node_replica->append_conf('postgresql.conf',
	'polar_fullpage_snapshot_oldest_lsn_delay_threshold=0');
$node_replica->append_conf('postgresql.conf',
	'polar_fullpage_snapshot_min_modified_count=0');
$node_replica->append_conf('postgresql.conf',
	'polar_enable_control_vm_flush=off');

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_replica->start;

# check polar_fullpage exists
my $polar_datadir = $node_primary->polar_get_datadir;
ok(-d "$polar_datadir/polar_fullpage", "polar_fullpage directory exists");

# remove polar_fullpage and do promote
$node_primary->stop;
my $output = `rm -rf $polar_datadir/polar_fullpage`;
ok( !-d "$polar_datadir/polar_fullpage",
	"succeed to remove polar_fullpage directory");
ok( !-f "$polar_datadir/polar_fullpage/log_index_meta",
	"log_index_meta of fullpage doesn't exist");

$node_replica->promote;
$node_replica->polar_wait_for_startup(30);

my $logdir = $node_replica->logfile;
print "replica logdir:$logdir\n";
$output =
  `grep -i -n "creating missing directory" $logdir | grep -i "polar_fullpage"`;
ok($output ne '',
	"succeed to create polar_fullpage directory during promote");

ok(-d "$polar_datadir/polar_fullpage",
	"polar_fullpage directory exists after promote");
ok( -f "$polar_datadir/polar_fullpage/log_index_meta",
	"log_index_meta of fullpage exists after promote");

my $node_replica2 = PostgreSQL::Test::Cluster->new('replica2');
$node_replica2->polar_init_replica($node_replica);
$node_replica2->append_conf('postgresql.conf',
	'polar_wait_old_version_page_timeout=30000');

$node_replica->polar_drop_slot($node_replica->name);
$node_replica->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica2->name . "'");
$node_replica->polar_create_slot($node_replica2->name);

$node_replica->safe_psql($db, 'select pg_reload_conf();');

$node_replica2->start;
$node_replica2->polar_wait_for_startup(60);

$node_replica->safe_psql($db, 'create extension polar_monitor;');
$node_replica->safe_psql($db, 'create table test(id int);');
$node_replica->safe_psql($db,
	'insert into test(id) select generate_series(1,10);');
$node_replica->wait_for_catchup($node_replica2);

# pause recovery
$node_replica2->safe_psql($db, 'select pg_wal_replay_pause();');
my $result =
  $node_replica2->safe_psql($db, 'select pg_is_wal_replay_paused();');
ok($result eq 't', "recovery of replica2 is paused");

$node_replica->safe_psql($db, 'update test set id = id + 100;');
$result =
  $node_replica->safe_psql($db, 'select count(*) from test where id > 10;');

$node_replica->safe_psql($db, 'checkpoint;');
my $res = $node_replica->safe_psql($db,
	'select write_fullpage_count from polar_get_fullpage_stat();');
print "write fullpage count:$res\n";
ok($res > 0, "new primary write fullpage successfully");

$res =
  $node_replica2->safe_psql($db, 'select count(*) from test where id > 10;');
print "res:$res\n";
ok($res == 0, "replica2 read and replay future page successfully");

#resume recovery
$node_replica2->safe_psql($db, 'select pg_wal_replay_resume();');
$res = $node_replica2->safe_psql($db, 'select pg_is_wal_replay_paused();');
ok($res eq 'f', "recovery of replica2 is resumed");
$res =
  $node_replica2->safe_psql($db, 'select count(*) from test where id > 10;');
print "new_primary_count:$result, replica2_count:$res\n";
ok($res == $result, "replica2 handle future page successfully");

$res = $node_replica2->safe_psql($db,
	'select read_fullpage_count from polar_get_fullpage_stat();');
ok($res > 0, "replica2 read future page");

$res = $node_replica2->safe_psql($db,
	'select restore_oldpage_count from polar_get_fullpage_stat();');
ok($res > 0, "replica2 restore future page");

$node_replica->stop;
$node_replica2->stop;
done_testing();
