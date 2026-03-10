# 028_polar_add_duplicate_2pc_during_recovery.pl
#	  test add duplicate two-phase transaction during recovery
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
#	  src/test/polar_pl/t/028_polar_add_duplicate_2pc_during_recovery.pl

use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} eq 'yes')
{
	plan tests => 3;
}
else
{
	plan skip_all => 'Fault injector not supported by this build';
}

# Setup primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

$node_primary->append_conf(
	'postgresql.conf', qq(
	max_prepared_transactions = 10
	log_checkpoints = true
	checkpoint_timeout = 3000
    Logging_collector = false
	restart_after_crash=on
));
$node_primary->start;

my $res =
  $node_primary->safe_psql('postgres', "create extension injection_points;");
print "create injection_points res: $res\n";
$node_primary->psql('postgres', "create table t_test_2pc(id int, msg text)");
$res = $node_primary->safe_psql('postgres',
	"select injection_points_attach('polar_delay_checkpoint', 'notice')");
print "enable delay fault inject:$res\n";

# start do checkpoint1
my $host = $node_primary->host;
my $port = $node_primary->port;
`nohup psql -h $host -p $port -c 'checkpoint' >tmp_check/test.file 2>&1 &`;

my $logdir = $node_primary->logfile;
print "primary logdir:$logdir\n";
my $found = 0;
my @result;

while ($found == 0)
{
	@result = `grep -rn "inject delay for current checkpoint" $logdir`;
	$found = @result;
}

# prepare transaction during checkpoint1
$node_primary->psql(
	'postgres', "
	BEGIN;
	INSERT INTO t_test_2pc VALUES (1, 'test add duplicate 2pc');
	PREPARE TRANSACTION 'xact_test_1';");

# reset delay flag to finish checkpoint1
$res = $node_primary->safe_psql('postgres',
	"select injection_points_detach('polar_delay_checkpoint')");
print "reset delay fault inject:$res\n";

$found = 0;
my $output;
while ($found < 2)
{
	$output = `grep -rn "checkpoint complete" $logdir | wc -l`;
	chomp($output);
	$found = int($output);
	print "$found\n";
	sleep 1;
}
print "checkpoint1 finished\n";

# inject fatal during checkpoint2
$res = $node_primary->safe_psql('postgres',
	"select injection_points_attach('polar_inject_checkpoint_fatal', 'notice')"
);
print "enable fatal fault inject:$res\n";
$node_primary->psql('postgres', "checkpoint");

$found = 0;
while ($found == 0)
{
	@result =
	  `grep -rn "terminating any other active server processes" $logdir`;
	$found = @result;
}

@result = `grep -rn "inject fatal during checkpoint" $logdir`;
$found = @result;
ok($found == 1, "fatal is injected during checkpoint");

# check crash recovery success
$node_primary->polar_wait_for_startup(30, 0);

$res = $node_primary->safe_psql('postgres',
	"select count(*) from pg_prepared_xacts");
print "xacts count:$res\n";
ok($res == 1, "crash recovery success during test add duplicate 2pc data");

$found = 0;
my $i = 0;
while ($found == 0 && $i < 30)
{
	@result =
	  `grep -rn "could not recover two-phase state file for transaction" $logdir`;
	$found = @result;
	$i = $i + 1;
	sleep 1;
}
ok($found == 1, "duplicate 2pc is not added during crash recovery");
@result = `rm tmp_check/test.file`;
$node_primary->stop;
