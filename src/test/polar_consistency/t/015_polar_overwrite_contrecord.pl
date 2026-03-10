# 015_polar_overwrite_contrecord.pl
#	  Test for overwrite contrecord during crash recovery.
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
#	  src/test/polar_consistency/t/015_polar_overwrite_contrecord.pl

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->polar_init_primary;

# We need these settings for stability of WAL behavior.
$node->append_conf(
	'postgresql.conf', qq(
autovacuum = off
maintenance_io_concurrency = 1000
recovery_prefetch = on
logging_collector = off
synchronous_commit = on
checkpoint_timeout = 300s
));

$node->start;

# DO checkpoint by manual to make sure the last checkpoint
# is completed during async instant recovery.
$node->safe_psql('postgres', 'checkpoint;');

# Now consume all remaining room in the current WAL block, leaving
# space enough only for the start of a largish record.

$node->safe_psql(
	'postgres', q{
	CREATE OR REPLACE FUNCTION emit_test_messages()
	RETURNS pg_lsn AS $$
	DECLARE
		wal_block_size int := (SELECT setting::int FROM pg_settings WHERE name = 'wal_block_size');
		name text;
		remain int;
		iters int := 0;
		length int := wal_block_size;
		insert_lsn pg_lsn;
	BEGIN
		LOOP
			name := 'test ' || iters;
			PERFORM pg_logical_emit_message(true, name, repeat('A', length));
			length := 123;
			iters := iters + 1;

			SELECT pg_current_wal_insert_lsn() INTO insert_lsn;
			remain := wal_block_size - (insert_lsn - '0/0') % wal_block_size;
			IF remain < wal_block_size / 2 THEN
				RAISE log 'exiting after % iterations, % bytes to end of WAL block at lsn %', iters, remain, insert_lsn;
				EXIT;
			END IF;
		END LOOP;
		RETURN insert_lsn;
	END;
	$$ LANGUAGE plpgsql;
});

$node->safe_psql('postgres', 'create extension pg_walinspect;');

my $start_lsn =
  $node->safe_psql('postgres', 'select pg_current_wal_insert_lsn();');
my $find_sql =
  "select count(*) from pg_get_wal_records_info('$start_lsn', pg_current_wal_insert_lsn()) where resource_manager = 'XLOG' and record_type = 'OVERWRITE_CONTRECORD';";

my $wal_block_size = $node->safe_psql('postgres',
	"SELECT setting FROM pg_settings WHERE name = 'wal_block_size';");

my $insert_lsn = $node->safe_psql('postgres', 'select emit_test_messages();');

for (my $i = 1; $i <= 10; $i++)
{
	my $wal_file = $node->safe_psql('postgres',
		"select file_name from pg_walfile_name_offset('$insert_lsn'::pg_lsn);"
	);
	my $wal_file_path = $node->polar_get_datadir . "/pg_wal/$wal_file";
	my $wal_file_size = -s $wal_file_path;

	my $wal_file_offset = $node->safe_psql('postgres',
		"select file_offset from pg_walfile_name_offset('$insert_lsn'::pg_lsn);"
	);
	my $expected_file_offset =
	  $wal_file_offset - $wal_file_offset % $wal_block_size;

	$node->stop('i');

	PostgreSQL::Test::Utils::system_or_bail('truncate', '-s',
		$expected_file_offset, $wal_file_path);
	PostgreSQL::Test::Utils::system_or_bail('truncate', '-s', $wal_file_size,
		$wal_file_path);

	$node->start;
	$node->polar_wait_for_startup(60);

	# DO checkpoint by manual to make sure the last checkpoint
	# is completed during async instant recovery.
	$node->safe_psql('postgres', 'checkpoint;');

	$insert_lsn =
	  $node->safe_psql('postgres', 'select emit_test_messages();');

	my $cur_insert_lsn =
	  $node->safe_psql('postgres', 'select pg_current_wal_insert_lsn();');
	my $cur_flush_lsn =
	  $node->safe_psql('postgres', 'select pg_current_wal_flush_lsn();');
	my $overwrite_contrecord_count = $node->safe_psql('postgres', $find_sql);

	ok( $overwrite_contrecord_count eq $i,
		"succeed to replay $i overwrite contrecords from $start_lsn: $overwrite_contrecord_count, current insert lsn $cur_insert_lsn, current flush lsn $cur_flush_lsn"
	);

	my $sql_content = $node->safe_psql('postgres',
		"select * from pg_get_wal_records_info('$start_lsn', pg_current_wal_insert_lsn()) where resource_manager = 'XLOG';"
	);
	note "$sql_content\n\n";
}

$node->stop;
done_testing();
