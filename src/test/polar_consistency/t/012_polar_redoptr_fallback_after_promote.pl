# 012_polar_redoptr_fallback_after_promote.pl
#	  Test for checkpoint.redo fallback after online promote
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
#	  src/test/polar_consistency/t/012_polar_redoptr_fallback_after_promote.pl

use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($Config{osname} eq 'MSWin32')
{
	# some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => 'Test fails on Windows perl';
}
elsif ($ENV{enable_injection_points} eq 'no')
{
	plan skip_all => 'Fault injector not supported by this build';
}

# primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

# replica
my $node_replica = PostgreSQL::Test::Cluster->new('replica1');
$node_replica->polar_init_replica($node_primary);

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);

$node_replica->append_conf('postgresql.conf',
	"polar_bg_replay_batch_size=5000");
$node_replica->append_conf('postgresql.conf', "checkpoint_timeout=600");
$node_replica->append_conf('postgresql.conf', "restart_after_crash=true");
$node_replica->append_conf('postgresql.conf', "huge_pages=off");
#disable logging collector, need grep logfile later
$node_replica->append_conf('postgresql.conf', "Logging_collector = false");
$node_replica->start;

my $wait_timeout = 30;
my $walpid =
  $node_replica->wait_walstreaming_establish_timeout($wait_timeout);
print "walreceiver pid of replica: $walpid\n";

my $res =
  $node_primary->safe_psql('postgres', "create extension injection_points;");
print "create injection_points res: $res\n";

$res = $node_replica->safe_psql('postgres',
	"select injection_points_attach('polar_skip_checkpoint_record', 'notice')"
);
print "attach skip checkpoint record fault inject:$res\n";

# create table in rw
$node_primary->safe_psql('postgres', 'create table test(a int)');

# stop ro logindex background worker to hang replay
print "ready to stop replica logindex background worker process\n";
$node_replica->stop_child('background logindex');

# primary insert data and do checkpoint
$node_primary->safe_psql('postgres',
	'insert into test(a) select generate_series(1,500000) as newval');
$node_primary->wait_for_catchup($node_replica);

# load buffer into replica buffer pool, so that older version page will be flushed to disk later
$res = $node_replica->safe_psql('postgres', 'select count(*) from test');
print "replica select res:$res\n";
ok($res == 500000, "replica select success");

# switch wal
$node_primary->safe_psql('postgres', 'select pg_switch_wal();');
$node_primary->safe_psql('postgres', 'update test set a = a + 1');
$node_primary->safe_psql('postgres', 'checkpoint;');

$res = $node_primary->safe_psql('postgres',
	'select count(*) from test where a = 1');
print "primary select res:$res\n";
ok($res == 0, "primary update all data success");

# stop primary
$node_primary->stop;

$res = $node_replica->safe_psql('postgres',
	"select injection_points_attach('polar_inject_panic', 'notice')");
print "inject panic res:$res\n";

print "ready to promote replica\n";
$node_replica->promote_async;
$node_replica->resume_child('background logindex');

$node_replica->polar_wait_for_startup(300, 0);

my $logdir = $node_replica->logfile;
print "replica logdir:$logdir\n";

my @result = `grep -rn "test inject panic after flush all buffers" $logdir`;
my $found = @result;
print "grep res:@result found:$found\n";
ok($found == 0, "start replay from oldest redoptr success");

# wait for crash recovery after panic
$node_replica->polar_wait_for_startup(300, 0);

$res = $node_replica->safe_psql('postgres', 'select count(*) from test');
print "new primary select1 res:$res\n";
ok($res == 500000, "new primary data count is correct after promote");

$res = $node_replica->safe_psql('postgres',
	'select count(*) from test where a >= 1 and a <= 100');
print "new primary select2 res:$res\n";
ok($res == 99, "new primary data is correct after promote");

$res = $node_replica->safe_psql('postgres',
	'select count(*) from test where a >=499900 and a <= 500001');
print "new primary select2 res:$res\n";
ok($res == 102, "new primary don't lose update after promote");

$node_replica->safe_psql('postgres',
	"select injection_points_detach('polar_inject_panic')");
$node_replica->safe_psql('postgres',
	"select injection_points_detach('polar_skip_checkpoint_record')");
my $slot_name = $node_replica->name;
$node_replica->polar_drop_slot($slot_name);
$node_replica->stop;
done_testing();
