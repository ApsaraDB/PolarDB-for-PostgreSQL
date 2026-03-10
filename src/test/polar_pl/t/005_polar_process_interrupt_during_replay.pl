# 005_polar_process_interrupt_during_replay.pl
#	  Test for process interrupt during replay
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
#	  src/test/polar_pl/t/005_polar_process_interrupt_during_replay.pl

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
else
{
	plan tests => 3;
}

# primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

# replica
my $node_replica = PostgreSQL::Test::Cluster->new('replica1');
$node_replica->polar_init_replica($node_primary);
$node_replica->append_conf('postgresql.conf', 'Logging_collector = false');

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_replica->start;

my $res =
  $node_primary->safe_psql('postgres', "create extension injection_points;");
print "create injection_points res: $res\n";
$node_primary->safe_psql('postgres', 'create extension polar_monitor;');
$node_primary->safe_psql('postgres', 'CREATE TABLE test(val integer);');
$node_primary->safe_psql('postgres', 'CHECKPOINT;');

# inject fault to skip flush buffer to disk
$node_primary->safe_psql('postgres',
	"select injection_points_attach('polar_skip_flushbuffer', 'notice');");
$node_primary->safe_psql('postgres',
	'INSERT INTO test(val) SELECT generate_series(1,10) as newwal;');

$node_primary->wait_for_catchup($node_replica);
my $lsn_diff = $node_primary->safe_psql('postgres',
	"select pg_wal_lsn_diff(cp, ap) from (select polar_consistent_lsn() as cp, polar_oldest_apply_lsn() as ap) as lsn_info;"
);
ok($lsn_diff < 0, "consistent_lsn is smaller than replay_lsn");

my $timeout = 6;
my $psql = {};
$node_replica->psql_connect("postgres", $timeout, _psql => $psql);
my $output =
  $node_replica->psql_execute("select pg_backend_pid();", _psql => $psql);
print "stderr: "
  . $output->{_stderr}
  . "stdout: "
  . $output->{_stdout} . "\n";
my $backend_pid = $output->{_stdout};
print "replica replaying backend pid: $backend_pid\n";

$node_replica->safe_psql('postgres',
	"select injection_points_attach('polar_delay_page_replay', 'notice');");
$node_replica->psql_execute("select count(*) from test;", _psql => $psql);

# terminate backend which is replaying
`kill -SIGTERM $backend_pid`;

my $logfile = $node_replica->logfile;
print "replica logfile:$logfile\n";
my @result;
my $found = 0;
my $i = 0;

while ($found == 0 && $i < 10)
{
	@result = `grep -rn "Abort replaying buf_id" $logfile`;
	$found = @result;
	$i = $i + 1;
	sleep 1;
}
ok($found == 1, "backend processed interrupt during replaying buffer");

$node_replica->safe_psql('postgres',
	"select injection_points_detach('polar_delay_page_replay');");
$res = $node_replica->safe_psql('postgres', "select count(*) from test;");
print "res: $res\n";
ok($res == 10, "other backend replays the aborted page successfully");
$node_replica->psql_close(_psql => $psql);

$node_primary->safe_psql('postgres',
	"select injection_points_detach('polar_skip_flushbuffer');");
$node_primary->stop;
$node_replica->stop;
