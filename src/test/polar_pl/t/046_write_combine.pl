#!/usr/bin/perl
# 046_write_combine.pl
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
#	  src/test/polar_pl/t/046_write_combine.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf',
	'polar_force_write_combine = on');

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->polar_init_standby($node_primary);
$node_standby->append_conf('postgresql.conf',
	'polar_force_write_combine = on');

$node_primary->start;
$node_replica->start;
$node_standby->start;

$node_primary->polar_create_slot($node_replica->name);
$node_primary->polar_create_slot($node_standby->name);

$node_primary->safe_psql('postgres', 'create extension polar_monitor');
$node_primary->safe_psql('postgres', 'create table t(id int)');

sub test_write_combine_limit
{
	my $combine_limit = shift;

	$node_primary->safe_psql('postgres',
		'insert into t select generate_series(1, 1000000)');

	$node_primary->safe_psql('postgres', 'checkpoint');
	ok( $node_primary->safe_psql('postgres',
			"select count(*) from polar_write_combine_latency where blocks=$combine_limit"
		) > 0,
		"write combine with limit $combine_limit works");
	$node_primary->safe_psql('postgres',
		"select * from polar_write_combine_stats");
	$node_primary->safe_psql('postgres',
		'select polar_write_combine_stats_reset()');

	is($node_primary->safe_psql('postgres', 'select count(*) from t'),
		1000000, "primary select with write combine limit $combine_limit");

	$node_primary->wait_for_catchup($node_replica);
	$node_primary->wait_for_catchup($node_standby);

	is($node_replica->safe_psql('postgres', 'select count(*) from t'),
		1000000, "replica select with write combine limit $combine_limit");
	is($node_standby->safe_psql('postgres', 'select count(*) from t'),
		1000000, "standby select with write combine limit $combine_limit");

	$node_standby->safe_psql('postgres', 'checkpoint');
	ok( $node_standby->safe_psql('postgres',
			"select count(*) from polar_write_combine_latency where blocks=$combine_limit"
		) > 0,
		"write combine with limit $combine_limit works");
	$node_standby->safe_psql('postgres',
		"select * from polar_write_combine_stats");
	$node_standby->safe_psql('postgres',
		'select polar_write_combine_stats_reset()');

	$node_primary->safe_psql('postgres', 'truncate table t');
}

for my $combine_limit (1, 2, 3, 16)
{
	$node_primary->append_conf('postgresql.conf',
		"polar_write_combine_limit = $combine_limit");
	$node_primary->reload;
	$node_standby->append_conf('postgresql.conf',
		"polar_write_combine_limit = $combine_limit");
	$node_standby->reload;
	test_write_combine_limit $combine_limit;
}

$node_primary->stop;
$node_replica->stop;
$node_standby->stop;

done_testing();
