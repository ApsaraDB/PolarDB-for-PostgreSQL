#!/usr/bin/perl

# 052_polar_recovery_check_required_params.pl
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
#	  src/test/polar_pl/t/052_polar_recovery_check_required_params.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);

$node_primary->append_conf('postgresql.conf', "max_connections=100");
$node_replica->append_conf('postgresql.conf', "max_connections=100");
$node_replica->append_conf('postgresql.conf',
	"polar_enable_parameters_inconsistency=off");
$node_replica->append_conf('postgresql.conf', 'Logging_collector = false');

$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_replica->start;

my $res = $node_primary->safe_psql('postgres', 'show max_connections;');
ok($res == 100, "max_connections of primary is 100");

$node_primary->safe_psql('postgres', 'CREATE TABLE test(val integer);');
$node_primary->safe_psql('postgres',
	'INSERT INTO test(val) SELECT generate_series(1,10) as newwal;');
$node_primary->wait_for_catchup($node_replica);

$res = $node_replica->safe_psql('postgres', 'SELECT count(*) FROM test');
ok($res == 10, "replica replayed all wal correctly");

$node_primary->append_conf('postgresql.conf', "max_connections=200");
$node_primary->restart;
$res = $node_primary->safe_psql('postgres', 'show max_connections;');
ok($res == 200, "max_connections of primary is increased to 200");

my $logfile = $node_replica->logfile;
print "replica logfile:$logfile\n";
my @result;
my $found = 0;
my $i = 0;

# redo XLOG_PARAMETER_CHANGE wal and recovery will be paused
while ($found == 0 && $i < 30)
{
	@result = `grep -rn "recovery has paused" $logfile`;
	$found = @result;
	$i = $i + 1;
	sleep 1;
}
ok($found == 1, "recovery of replica is paused");

$res = $node_replica->safe_psql('postgres', 'show max_connections;');
ok($res == 100, "max_connections of replica is still 100");

$node_primary->safe_psql('postgres',
	'INSERT INTO test(val) SELECT generate_series(11,20) as newwal;');
$res =
  $node_replica->safe_psql('postgres', 'select pg_is_wal_replay_paused();');
print "recovery of replica is paused: $res\n";
ok($res eq 't', "recovery of replica is paused");

$res = $node_replica->safe_psql('postgres', 'SELECT count(*) FROM test');
ok($res = 10, "replica hasn't replayed wal");

$node_replica->safe_psql('postgres', 'select pg_wal_replay_resume();');

$found = 0;
$i = 0;
while ($found == 0 && $i < 30)
{
	@result =
	  `grep -rn "recovery aborted because of insufficient parameter settings" $logfile`;
	$found = @result;
	$i = $i + 1;
	sleep 1;
}
ok($found == 1,
	"recovery is aborted because of insufficient parameter settings");

# set polar_enable_parameters_inconsistency to true
$node_replica->_update_pid(0);
$node_replica->append_conf('postgresql.conf',
	'polar_enable_parameters_inconsistency = true');
$node_replica->start;
$node_replica->polar_wait_for_startup(30);

$res = $node_replica->safe_psql('postgres', 'show max_connections;');
ok($res == 100, "max_connections of replica is still 100");
$res = $node_replica->safe_psql('postgres', 'SELECT count(*) FROM test');
ok( $res = 20,
	"replica started and replayed wal successfully when polar_enable_parameters_inconsistency is on"
);

$node_primary->stop;
$node_replica->promote_async;

$found = 0;
$i = 0;
while ($found == 0 && $i < 30)
{
	@result =
	  `grep -rn "Insufficient parameter settings are detected during promote" $logfile`;
	$found = @result;
	$i = $i + 1;
	sleep 1;
}
ok($found == 1,
	"insufficient parameter settings are detected during promote");

$node_replica->polar_wait_for_startup(30);
$res = $node_replica->safe_psql('postgres', 'select pg_is_in_recovery();');
ok($res eq 'f', "replica is promoted");

$res = $node_replica->safe_psql('postgres', 'show max_connections;');
ok($res == 100, "max_connections of replica is still 100 after promote");

$node_replica->stop;
done_testing();
