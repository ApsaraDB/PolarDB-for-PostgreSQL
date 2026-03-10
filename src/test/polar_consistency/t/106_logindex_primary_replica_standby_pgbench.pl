# 106_logindex_primary_replica_standby_pgbench.pl
#	  Test case: pgbench with special index on primary, replica and standby
#
# Copyright (c) 2022, Alibaba Group Holding Limited
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
#	  src/test/polar_consistency/t/106_logindex_primary_replica_standby_pgbench.pl
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PolarDB::DCRegression;

my $db = 'postgres';

# skip data consistency check
if (!defined $ENV{DCCHECK_ALL} or $ENV{DCCHECK_ALL} ne 1)
{
	plan skip_all => 'skip because of disabled DCCHECK_ALL.';
}

my @prepared_pgbenches = (
	"pgbench_hash_init.sql,pgbench_hash.sql,pgbench_hash_selectonly.sql,60,dccheck_hash_heap",
	"pgbench_gist_init.sql,pgbench_gist.sql,pgbench_gist_selectonly.sql,120,dccheck_gist"
);
my $pgbench_count = @prepared_pgbenches;

foreach my $pgbench_test (@prepared_pgbenches)
{
	my ($init_file, $rw_file, $select_only_file, $timeout, $target) =
	  ("", "", "", 0);
	$pgbench_test =~ s/^\s+|\s+$//g;
	if ($pgbench_test =~
		qr/\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,\n]+)\s*,\s*([0-9]+)\s*,\s*([^,\n]+)\s*/i
	  )
	{
		$init_file = $ENV{PWD} . "/t/$1";
		$rw_file = $ENV{PWD} . "/t/$2";
		$select_only_file = $ENV{PWD} . "/t/$3";
		$timeout = int($4);
		$target = $5;
	}
	print "pgbench for [$pgbench_test]\n";

	my $node_primary = PostgreSQL::Test::Cluster->new('primary');
	$node_primary->polar_init_primary;
	my $node_replica1 = PostgreSQL::Test::Cluster->new('replica1');
	$node_replica1->polar_init_replica($node_primary);
	my $node_standby1 = PostgreSQL::Test::Cluster->new('standby1');
	$node_standby1->polar_init_standby($node_primary);

	$node_primary->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
	$node_replica1->append_conf('postgresql.conf',
		'wal_receiver_timeout=3600s');
	$node_standby1->append_conf('postgresql.conf',
		'wal_receiver_timeout=3600s');
	$node_standby1->append_conf('postgresql.conf', 'logging_collector=off');

	$node_primary->start;
	$node_primary->polar_create_slot($node_replica1->name);
	$node_primary->polar_create_slot($node_standby1->name);
	$node_replica1->start;
	$node_standby1->start;
	$node_standby1->polar_drop_all_slots;

	my @nodes = ($node_primary, $node_replica1, $node_standby1);
	my $start_time = time();

	# init tables
	$node_primary->safe_psql($db, "\\i $init_file;");
	# Wait for replica/standby to catch up
	$node_primary->wait_for_catchup($node_replica1);
	$node_primary->wait_for_catchup($node_standby1);

	# run pgbench
	for my $node (@nodes)
	{
		$node->pgbench_test(
			dbname => $db,
			client => 2,
			job => 2,
			time => $timeout,
			extra => "-f $select_only_file",
			async => 1,
			script => 'none');
	}

	$node_primary->pgbench_test(
		dbname => $db,
		client => 2,
		job => 2,
		time => $timeout,
		extra => "-f $rw_file",
		async => 0,
		script => 'none');

	# Wait for replica/standby to catch up
	$node_primary->wait_for_catchup($node_replica1);
	$node_primary->wait_for_catchup($node_standby1);

	my $primary_target_count =
	  $node_primary->safe_psql($db, "select count(*) from $target;");
	my $replica1_target_count =
	  $node_replica1->safe_psql($db, "select count(*) from $target;");
	ok( $primary_target_count eq $replica1_target_count,
		"primary and replica1 target count is equal");

	my $standby1_target_count =
	  $node_standby1->safe_psql($db, "select count(*) from $target;");
	ok( $primary_target_count eq $standby1_target_count,
		"primary and standby1 target count is equal");

	if (   $primary_target_count ne $replica1_target_count
		or $primary_target_count ne $standby1_target_count)
	{
		if (-e `printf polar_dump_core`)
		{
			print `ps -ef | grep postgres: | xargs -n 1 -P 0 gcore`;
		}
		sleep(86400);
		die "PolarDB regression test for [$pgbench_test]";
	}
	my $cost_time = time() - $start_time;
	note "Test cost time $cost_time for [$pgbench_test]\n";

	$node_primary->stop;
	$node_replica1->stop;
	$node_standby1->stop;

	$node_primary->clean_node;
	$node_replica1->clean_node;
	$node_standby1->clean_node;
}

done_testing();
