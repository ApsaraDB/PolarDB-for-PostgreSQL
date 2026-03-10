#!/usr/bin/perl

# 050_calc_rel_size_use_rsc.pl
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
#	  src/test/polar_pl/t/050_calc_rel_size_use_rsc.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(gettimeofday);

# init primary
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf',
	'polar_enable_rel_size_cache = on');
$node_primary->append_conf('postgresql.conf',
	'polar_rsc_shared_relations = 16384');

# init replica
my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);
$node_replica->append_conf('postgresql.conf',
	"polar_enable_rel_size_cache = on");
$node_replica->append_conf('postgresql.conf',
	"polar_enable_replica_rel_size_cache = on");
$node_replica->append_conf('postgresql.conf',
	'polar_rsc_shared_relations = 16384');

# init standby
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->polar_init_standby($node_primary);
$node_standby->append_conf('postgresql.conf',
	"polar_enable_rel_size_cache = on");
$node_standby->append_conf('postgresql.conf',
	"polar_enable_standby_rel_size_cache = on");
$node_standby->append_conf('postgresql.conf',
	'polar_rsc_shared_relations = 16384');

$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_primary->polar_create_slot($node_standby->name);
$node_replica->start;
$node_standby->start;

#
# prepare objects of different relkinds
#

# view
$node_primary->safe_psql(
	'postgres', qq(
	CREATE VIEW test_view AS SELECT oid FROM pg_class
));
# partitioned table / partitioned index
# table / index / toast table
$node_primary->safe_psql(
	'postgres', qq(
	CREATE TABLE test_p (id int primary key, txt text) PARTITION BY RANGE (id);
	CREATE TABLE test_p0 PARTITION OF test_p FOR VALUES FROM (0) TO (2000);
	CREATE TABLE test_p1 PARTITION OF test_p FOR VALUES FROM (2000) TO (12000);
	CREATE TABLE test_p2 PARTITION OF test_p FOR VALUES FROM (12000) TO (20000);
	INSERT INTO test_p (id, txt) SELECT oid, relname FROM pg_class;
));
# sequence
$node_primary->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE test_sequence;
));
# matview
$node_primary->safe_psql(
	'postgres', qq(
	CREATE MATERIALIZED VIEW test_matview AS SELECT oid FROM pg_class;
));

$node_primary->wait_for_catchup($node_replica);
$node_primary->wait_for_catchup($node_standby);

my $round = 50000;
my $time_slow;
my $time_fast;
my $size_slow;
my $size_fast;

$time_slow = gettimeofday();
$size_slow = $node_primary->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO off;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_slow = gettimeofday() - $time_slow;

$time_fast = gettimeofday();
$size_fast = $node_primary->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO on;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_fast = gettimeofday() - $time_fast;

ok( $time_fast < $time_slow,
	"pg_total_relation_size (primary) with RSC uses: $time_fast; without RSC uses: $time_slow."
);
ok( $size_fast == $size_slow,
	"pg_total_relation_size (primary) w/wo RSC is the same: $size_fast/$size_slow."
);

$time_slow = gettimeofday();
$size_slow = $node_replica->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO off;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_slow = gettimeofday() - $time_slow;

$time_fast = gettimeofday();
$size_fast = $node_replica->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO on;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_fast = gettimeofday() - $time_fast;

ok( $time_fast < $time_slow,
	"pg_total_relation_size (replica) with RSC uses: $time_fast; without RSC uses: $time_slow."
);
ok( $size_fast == $size_slow,
	"pg_total_relation_size (replica) w/wo RSC is the same: $size_fast/$size_slow."
);

$time_slow = gettimeofday();
$size_slow = $node_standby->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO off;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_slow = gettimeofday() - $time_slow;

$time_fast = gettimeofday();
$size_fast = $node_standby->safe_psql(
	'postgres', qq(
		SET polar_rsc_optimize_rel_size_udf TO on;
		SELECT SUM(pg_total_relation_size(oid)) FROM pg_class, generate_series(1, $round) WHERE relname LIKE 'test%'
));
$time_fast = gettimeofday() - $time_fast;

ok( $time_fast < $time_slow,
	"pg_total_relation_size (standby) with RSC uses: $time_fast; without RSC uses: $time_slow."
);
ok( $size_fast == $size_slow,
	"pg_total_relation_size (standby) w/wo RSC is the same: $size_fast/$size_slow."
);

$node_primary->stop('fast');
$node_replica->stop('fast');
$node_standby->stop('fast');

done_testing();
