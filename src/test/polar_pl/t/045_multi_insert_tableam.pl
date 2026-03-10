#!/usr/bin/perl

# 045_multi_insert_tableam.pl
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
#	  src/test/polar_pl/t/045_multi_insert_tableam.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

# init primary with wal_level = replica
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->append_conf('postgresql.conf', 'wal_level = replica');
$node_primary->start;

# init standby for checking WAL logging
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->polar_init_standby($node_primary);
$node_primary->polar_create_slot($node_standby->name);
$node_standby->start;

# data preparation
$node_primary->safe_psql('postgres',
	'CREATE TABLE base_data (id int, txt text, val float);');
$node_primary->safe_psql(
	'postgres', qq[
	INSERT INTO base_data SELECT i AS id, md5(i::text) AS txt, i + 0.1 AS val
	FROM generate_series(1,100000) i;
	ANALYZE base_data;]);

sub wal_stats
{
	my ($node, $connect_db) = @_;

	$node->safe_psql($connect_db, "SELECT pg_sleep(2)");

	my %results;
	$results{records} =
	  $node->safe_psql($connect_db, "SELECT wal_records FROM pg_stat_wal");
	$results{bytes} =
	  $node->safe_psql($connect_db, "SELECT wal_bytes FROM pg_stat_wal");
	$results{fpi} =
	  $node->safe_psql($connect_db, "SELECT wal_fpi FROM pg_stat_wal");

	return \%results;
}

#
# CREATE MATERIALIZED VIEW
#
# should use FPI records
#
my $before_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO off;
	CREATE MATERIALIZED VIEW mv_single AS SELECT * FROM base_data;
]);
my $after_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_bulk_write_maxpages TO 128;
	CREATE MATERIALIZED VIEW mv_multi AS SELECT * FROM base_data;
]);
my $after_wal_stats_multi = wal_stats($node_primary, 'postgres');

# check
my $result = $node_primary->safe_psql(
	'postgres', qq[
	(SELECT * FROM mv_single EXCEPT ALL SELECT * FROM mv_multi)
	UNION ALL
	(SELECT * FROM mv_multi EXCEPT ALL SELECT * FROM mv_single);
]);
is($result, '', 'the diff is empty on primary');
$node_primary->wait_for_catchup($node_standby);
$result = $node_standby->safe_psql(
	'postgres', qq[
	(SELECT * FROM mv_single EXCEPT ALL SELECT * FROM mv_multi)
	UNION ALL
	(SELECT * FROM mv_multi EXCEPT ALL SELECT * FROM mv_single);
]);
is($result, '', 'the diff is empty on standby');
my $wal_records_single =
  $after_wal_stats_single->{records} - $before_wal_stats_single->{records};
my $wal_records_multi =
  $after_wal_stats_multi->{records} - $after_wal_stats_single->{records};
my $wal_bytes_single =
  $after_wal_stats_single->{bytes} - $before_wal_stats_single->{bytes};
my $wal_bytes_multi =
  $after_wal_stats_multi->{bytes} - $after_wal_stats_single->{bytes};
my $wal_fpi_single =
  $after_wal_stats_single->{fpi} - $before_wal_stats_single->{fpi};
my $wal_fpi_multi =
  $after_wal_stats_multi->{fpi} - $after_wal_stats_single->{fpi};
ok( $wal_records_multi < $wal_records_single,
	"multi insert generates less WAL records ($wal_records_multi) than single insert ($wal_records_single)"
);
ok( $wal_bytes_multi < $wal_bytes_single,
	"multi insert generates less WAL bytes ($wal_bytes_multi) than single insert ($wal_bytes_single)"
);
ok( $wal_fpi_multi > $wal_fpi_single,
	"multi insert ($wal_fpi_multi) generate more FPI records than single insert ($wal_fpi_single)"
);

#
# REFRESH MATERIALIZED VIEW
#
# should use FPI records
#
$before_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO off;
	REFRESH MATERIALIZED VIEW mv_single;
]);
$after_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_bulk_write_maxpages TO 128;
	REFRESH MATERIALIZED VIEW mv_multi;
]);
$after_wal_stats_multi = wal_stats($node_primary, 'postgres');

# check
$result = $node_primary->safe_psql(
	'postgres', qq[
	(SELECT * FROM mv_single EXCEPT ALL SELECT * FROM mv_multi)
	UNION ALL
	(SELECT * FROM mv_multi EXCEPT ALL SELECT * FROM mv_single);
]);
is($result, '', 'the diff is empty on primary');
$node_primary->wait_for_catchup($node_standby);
$result = $node_standby->safe_psql(
	'postgres', qq[
	(SELECT * FROM mv_single EXCEPT ALL SELECT * FROM mv_multi)
	UNION ALL
	(SELECT * FROM mv_multi EXCEPT ALL SELECT * FROM mv_single);
]);
is($result, '', 'the diff is empty on standby');
$wal_records_single =
  $after_wal_stats_single->{records} - $before_wal_stats_single->{records};
$wal_records_multi =
  $after_wal_stats_multi->{records} - $after_wal_stats_single->{records};
$wal_bytes_single =
  $after_wal_stats_single->{bytes} - $before_wal_stats_single->{bytes};
$wal_bytes_multi =
  $after_wal_stats_multi->{bytes} - $after_wal_stats_single->{bytes};
$wal_fpi_single =
  $after_wal_stats_single->{fpi} - $before_wal_stats_single->{fpi};
$wal_fpi_multi =
  $after_wal_stats_multi->{fpi} - $after_wal_stats_single->{fpi};
ok( $wal_records_multi < $wal_records_single,
	"multi insert generates less WAL records ($wal_records_multi) than single insert ($wal_records_single)"
);
ok( $wal_bytes_multi < $wal_bytes_single,
	"multi insert generates less WAL bytes ($wal_bytes_multi) than single insert ($wal_bytes_single)"
);
ok( $wal_fpi_multi > $wal_fpi_single,
	"multi insert ($wal_fpi_multi) generate more FPI records than single insert ($wal_fpi_single)"
);

#
# CREATE TABLE AS
#
# should use FPI records
#
$before_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO off;
	CREATE TABLE tbl_single AS SELECT * FROM base_data;
]);
$after_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_bulk_write_maxpages TO 128;
	CREATE TABLE tbl_multi AS SELECT * FROM base_data;
]);
$after_wal_stats_multi = wal_stats($node_primary, 'postgres');

# check
$result = $node_primary->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on primary');
$node_primary->wait_for_catchup($node_standby);
$result = $node_standby->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on standby');
$wal_records_single =
  $after_wal_stats_single->{records} - $before_wal_stats_single->{records};
$wal_records_multi =
  $after_wal_stats_multi->{records} - $after_wal_stats_single->{records};
$wal_bytes_single =
  $after_wal_stats_single->{bytes} - $before_wal_stats_single->{bytes};
$wal_bytes_multi =
  $after_wal_stats_multi->{bytes} - $after_wal_stats_single->{bytes};
$wal_fpi_single =
  $after_wal_stats_single->{fpi} - $before_wal_stats_single->{fpi};
$wal_fpi_multi =
  $after_wal_stats_multi->{fpi} - $after_wal_stats_single->{fpi};
ok( $wal_records_multi < $wal_records_single,
	"multi insert generates less WAL records ($wal_records_multi) than single insert ($wal_records_single)"
);
ok( $wal_bytes_multi < $wal_bytes_single,
	"multi insert generates less WAL bytes ($wal_bytes_multi) than single insert ($wal_bytes_single)"
);
ok( $wal_fpi_multi > $wal_fpi_single,
	"multi insert ($wal_fpi_multi) generate more FPI records than single insert ($wal_fpi_single)"
);

#
# INSERT INTO SELECT
#
# should use normal multi-insert
#
$before_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_table_multi_insert_min_rows TO 0;
	TRUNCATE tbl_single;
	INSERT INTO tbl_single SELECT * FROM base_data;
]);
$after_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_table_multi_insert_min_rows TO 20000;
	TRUNCATE tbl_multi;
	INSERT INTO tbl_multi SELECT * FROM base_data;
]);
$after_wal_stats_multi = wal_stats($node_primary, 'postgres');

# check
$result = $node_primary->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on primary');
$node_primary->wait_for_catchup($node_standby);
$result = $node_standby->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on standby');
$wal_records_single =
  $after_wal_stats_single->{records} - $before_wal_stats_single->{records};
$wal_records_multi =
  $after_wal_stats_multi->{records} - $after_wal_stats_single->{records};
$wal_bytes_single =
  $after_wal_stats_single->{bytes} - $before_wal_stats_single->{bytes};
$wal_bytes_multi =
  $after_wal_stats_multi->{bytes} - $after_wal_stats_single->{bytes};
ok( $wal_records_multi < $wal_records_single,
	"multi insert generates less WAL records ($wal_records_multi) than single insert ($wal_records_single)"
);
ok( $wal_bytes_multi < $wal_bytes_single,
	"multi insert generates less WAL bytes ($wal_bytes_multi) than single insert ($wal_bytes_single)"
);

# clean up
$node_primary->safe_psql('postgres', 'DROP MATERIALIZED VIEW mv_single');
$node_primary->safe_psql('postgres', 'DROP MATERIALIZED VIEW mv_multi');
$node_primary->safe_psql('postgres', 'DROP TABLE tbl_single');
$node_primary->safe_psql('postgres', 'DROP TABLE tbl_multi');
$node_primary->stop('fast');

# init primary with wal_level = logical
$node_primary->append_conf('postgresql.conf', 'wal_level = logical');
$node_primary->start;

#
# CREATE TABLE AS
#
# should not use raw bulk write, should use normal multi-insert
#
$before_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO off;
	CREATE TABLE tbl_single AS SELECT * FROM base_data;
]);
$after_wal_stats_single = wal_stats($node_primary, 'postgres');
$node_primary->safe_psql(
	'postgres', qq[
	SET polar_enable_tableam_multi_insert TO on;
	SET polar_bulk_write_maxpages TO 128;
	CREATE TABLE tbl_multi AS SELECT * FROM base_data;
]);
$after_wal_stats_multi = wal_stats($node_primary, 'postgres');

# check
$result = $node_primary->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on primary');
$node_primary->wait_for_catchup($node_standby);
$result = $node_standby->safe_psql(
	'postgres', qq[
	(SELECT * FROM tbl_single EXCEPT ALL SELECT * FROM tbl_multi)
	UNION ALL
	(SELECT * FROM tbl_multi EXCEPT ALL SELECT * FROM tbl_single);
]);
is($result, '', 'the diff is empty on standby');
$wal_records_single =
  $after_wal_stats_single->{records} - $before_wal_stats_single->{records};
$wal_records_multi =
  $after_wal_stats_multi->{records} - $after_wal_stats_single->{records};
$wal_bytes_single =
  $after_wal_stats_single->{bytes} - $before_wal_stats_single->{bytes};
$wal_bytes_multi =
  $after_wal_stats_multi->{bytes} - $after_wal_stats_single->{bytes};
$wal_fpi_single =
  $after_wal_stats_single->{fpi} - $before_wal_stats_single->{fpi};
$wal_fpi_multi =
  $after_wal_stats_multi->{fpi} - $after_wal_stats_single->{fpi};
ok( $wal_records_multi < $wal_records_single,
	"multi insert generates less WAL records ($wal_records_multi) than single insert ($wal_records_single)"
);
ok( $wal_bytes_multi < $wal_bytes_single,
	"multi insert generates less WAL bytes ($wal_bytes_multi) than single insert ($wal_bytes_single)"
);
ok($wal_fpi_single < 100,
	"single insert does not use FPI records ($wal_fpi_single)");
ok( $wal_fpi_multi < 100,
	"multi insert does not use FPI records ($wal_fpi_multi) due to it may be logically replicated"
);

$node_primary->stop('fast');
$node_standby->stop('fast');

done_testing();
