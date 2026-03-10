# 029_polar_invalidate_new_slot.pl
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
#	  src/test/polar_pl/t/029_polar_invalidate_new_slot.pl

use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} eq 'yes')
{
	plan tests => 1;
}
else
{
	plan skip_all => 'Fault injector not supported by this build';
}

# Setup primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);

$node_primary->append_conf(
	'postgresql.conf', qq(
	log_checkpoints = true
	checkpoint_timeout = 3000
    Logging_collector = false
));
$node_primary->start;

my $res =
  $node_primary->safe_psql('postgres', "create extension injection_points;");
print "create injection_points res: $res\n";
$res = $node_primary->safe_psql('postgres',
	"select injection_points_attach('polar_delay_slot_reserve_wal', 'notice')"
);
print "enable delay fault inject:$res\n";

# start create slot and reserve wal
my $host = $node_primary->host;
my $port = $node_primary->port;
`nohup psql -h $host -p $port -c "SELECT pg_create_physical_replication_slot('replica1', true)" >tmp_check/test.file 2>&1 &`;

# switch wal and do checkpoint
$res = $node_primary->safe_psql('postgres', "select pg_switch_wal()");
$res = $node_primary->safe_psql('postgres', "create table test(a int)");
$res = $node_primary->safe_psql('postgres',
	"INSERT INTO test(a) SELECT generate_series(1,100) as newwal");

`nohup psql -h $host -p $port -c 'checkpoint' >tmp_check/test.file 2>&1 &`;

$res = $node_primary->safe_psql('postgres',
	"select injection_points_detach('polar_delay_slot_reserve_wal')");
print "reset delay fault inject:$res\n";

my $found = 0;
my $output;
my $i = 0;
my $logdir = $node_primary->logfile;
print "logfile:$logdir\n";

while ($found == 0 && $i < 30)
{
	$output = `grep -rn "checkpoint complete" $logdir | wc -l`;
	chomp($output);
	$found = int($output);
	$i = $i + 1;
	sleep 1;
}
print "checkpoint finished\n";

$res = $node_primary->safe_psql('postgres',
	"select wal_status from pg_replication_slots where slot_name='replica1'");
print "res:$res\n";
is($res, "reserved", "slot is not invalidated by checkpoint");

`rm tmp_check/test.file`;
$node_primary->stop;
