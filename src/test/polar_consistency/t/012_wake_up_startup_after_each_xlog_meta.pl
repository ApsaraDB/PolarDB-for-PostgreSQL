# 012_wake_up_startup_after_each_xlog_meta.pl
#	  Test for wake up startup after each xlog meta.
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
#	  src/test/polar_consistency/t/012_wake_up_startup_after_each_xlog_meta.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
srand();
$ENV{'PG_TEST_NOCLEAN'} = 1;

my $loop_count = 1000;
my $wait_xlog_meta_timeout = 12;
my $db = 'postgres';

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

my $node_replica = PostgreSQL::Test::Cluster->new('replica1');
$node_replica->polar_init_replica($node_primary);

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->append_conf('postgresql.conf', "synchronous_commit=on");
$node_primary->append_conf('postgresql.conf', "checkpoint_timeout=60000");

$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);

$node_replica->append_conf('postgresql.conf',
	"polar_wait_xlog_meta_timeout=${wait_xlog_meta_timeout}s");
$node_replica->start;

$node_primary->safe_psql($db, "create extension polar_monitor;");

sub run_ddl
{
	my ($node) = @_;
	my $sql_loop = int(rand(5)) + 1;
	my $ddl_sql = "
		DO \$\$
		DECLARE
		counter integer := 1;
		BEGIN
		RAISE LOG 'beging tmp test';
		FOR counter IN 1..$sql_loop LOOP
			EXECUTE 'CREATE TEMP TABLE temp_table_' || counter || ' (id serial, name text) ON COMMIT DELETE ROWS';
		END LOOP;
		RAISE LOG 'end tmp test';
		END;
		\$\$;
	";

	my $start_time = time();
	$node->safe_psql($db, $ddl_sql);
	my $end_time = time();
	my $cost_time = $end_time - $start_time;
	if ($cost_time > 0)
	{
		note "Test cost time $cost_time for DDL [$start_time, $end_time]\n";
	}

	return $cost_time;
}

for (my $i = 0; $i < $loop_count; $i++)
{
	my $cost_time = run_ddl($node_primary);
	ok( $cost_time < $wait_xlog_meta_timeout / 2,
		"succ to execute ddl: $cost_time");
}

$node_primary->stop();
$node_replica->stop();

done_testing();
