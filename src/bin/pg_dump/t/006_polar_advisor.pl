# 006_polar_advisor.pl
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
#	  src/bin/pg_dump/t/006_polar_advisor.pl

# Test pg_dump behavior of the objects in schema polar_advisor.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

############### define check function ##############
sub init_polar_advisor_objects
{
	my ($node, $db) = @_;
	$node->safe_psql($db, "CREATE SCHEMA polar_advisor;");
	$node->safe_psql($db,
		"CREATE SEQUENCE polar_advisor.advisor_exec_id_seq;");
	$node->safe_psql(
		$db, "CREATE TABLE polar_advisor.blacklist_relations (
			schema_name         NAME,
			relation_name       NAME,
			CONSTRAINT blacklist_schema_table_pk PRIMARY KEY (schema_name, relation_name)
		);");
	$node->safe_psql(
		$db,
		"CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_blacklist (
			schema_name             NAME,
			relation_name           NAME,
			action_type             VARCHAR(100)
		)
		RETURNS BOOLEAN AS \$\$
		DECLARE
			rel_exists    BOOLEAN;
		BEGIN
			SELECT COUNT(*) > 0 INTO rel_exists
			FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = \$1 AND c.relname = \$2;
			IF NOT rel_exists THEN
				RAISE EXCEPTION 'relation \"%.%\" does not exist', \$1, \$2;
			END IF;

			SELECT COUNT(*) > 0 INTO rel_exists FROM polar_advisor.blacklist_relations t
			WHERE t.schema_name = \$1 AND t.relation_name = \$2 AND t.action_type = \$3;
			IF rel_exists THEN
				RAISE WARNING 'relation \"%.%\" already exists in blacklist of type %, skip it', \$1, \$2, \$3;
				RETURN FALSE;
			END IF;

			INSERT INTO polar_advisor.blacklist_relations (schema_name, relation_name, action_type) VALUES (\$1, \$2, \$3);
			RETURN TRUE;
		END;
		\$\$ LANGUAGE PLpgSQL;");
	$node->safe_psql(
		$db, "CREATE TABLE polar_advisor.db_level_advisor_log (
			id                      BIGSERIAL PRIMARY KEY,
			exec_id                 BIGINT,
			start_time              TIMESTAMP WITH TIME ZONE,
			end_time                TIMESTAMP WITH TIME ZONE,
			db_name                 NAME,
			event_type              VARCHAR(100),
			total_relation          BIGINT,
			acted_relation          BIGINT,
			age_before              BIGINT,
			age_after               BIGINT,
			others                  JSONB
		);");
	$node->safe_psql($db,
		"CREATE INDEX db_level_advisor_log_start_time_idx ON polar_advisor.db_level_advisor_log (start_time);"
	);
	$node->safe_psql($db,
		"CREATE INDEX db_level_advisor_log_end_time_idx ON polar_advisor.db_level_advisor_log (end_time);"
	);
}

sub init_test_objects
{
	my ($node, $db) = @_;
	$node->safe_psql($db, "CREATE SCHEMA test_schema;");
	$node->safe_psql($db, "CREATE SEQUENCE test_schema.advisor_exec_id_seq;");
	$node->safe_psql(
		$db, "CREATE TABLE test_schema.blacklist_relations (
			schema_name         NAME,
			relation_name       NAME,
			CONSTRAINT test_blacklist_schema_table_pk PRIMARY KEY (schema_name, relation_name)
		);");
	$node->safe_psql(
		$db,
		"CREATE OR REPLACE FUNCTION test_schema.add_relation_to_blacklist (
			schema_name             NAME,
			relation_name           NAME,
			action_type             VARCHAR(100)
		)
		RETURNS BOOLEAN AS \$\$
		DECLARE
			rel_exists    BOOLEAN;
		BEGIN
			SELECT COUNT(*) > 0 INTO rel_exists
			FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = \$1 AND c.relname = \$2;
			IF NOT rel_exists THEN
				RAISE EXCEPTION 'relation \"%.%\" does not exist', \$1, \$2;
			END IF;

			SELECT COUNT(*) > 0 INTO rel_exists FROM test_schema.blacklist_relations t
			WHERE t.schema_name = \$1 AND t.relation_name = \$2 AND t.action_type = \$3;
			IF rel_exists THEN
				RAISE WARNING 'relation \"%.%\" already exists in blacklist of type %, skip it', \$1, \$2, \$3;
				RETURN FALSE;
			END IF;

			INSERT INTO test_schema.blacklist_relations (schema_name, relation_name, action_type) VALUES (\$1, \$2, \$3);
			RETURN TRUE;
		END;
		\$\$ LANGUAGE PLpgSQL;");
	$node->safe_psql(
		$db, "CREATE TABLE test_schema.test_db_level_advisor_log (
			id                      BIGSERIAL PRIMARY KEY,
			exec_id                 BIGINT,
			start_time              TIMESTAMP WITH TIME ZONE,
			end_time                TIMESTAMP WITH TIME ZONE,
			db_name                 NAME,
			event_type              VARCHAR(100),
			total_relation          BIGINT,
			acted_relation          BIGINT,
			age_before              BIGINT,
			age_after               BIGINT,
			others                  JSONB
		);");
	$node->safe_psql($db,
		"CREATE INDEX test_db_level_advisor_log_start_time_idx ON test_schema.test_db_level_advisor_log (start_time);"
	);
	$node->safe_psql($db,
		"CREATE INDEX test_db_level_advisor_log_end_time_idx ON test_schema.test_db_level_advisor_log (end_time);"
	);
}

sub check_dump_polar_advisor
{
	my ($compatibility_mode) = @_;
	my $db_source = 'postgres';
	my $db_target = 'target';
	my $dump_content;
	my $cnt;
	my $node = PostgreSQL::Test::Cluster->new('main');
	$node->init();
	# $node->init(extra => ["--compatibility_mode=$compatibility_mode"]);
	$node->start;
	my $backupdir = $node->backup_dir;

	# Regression test is run in docker and no super_pg client creates polar_advisor
	# by default, so we create it to simulate the real situation.
	init_polar_advisor_objects($node, $db_source);

	# create normal schema
	init_test_objects($node, $db_source);

	# create target database
	$node->safe_psql($db_source, "CREATE DATABASE $db_target;");

	# 1. polar_advisor not dumped if --exclude-schema specified.
	$node->command_ok(
		[
			'pg_dump', '--exclude-schema',
			'polar_advisor', '-f',
			"$backupdir/dump1", '--dbname',
			$node->connstr($db_source)
		],
		'dump with --exclude-schema');
	$dump_content =
	  PostgreSQL::Test::Utils::slurp_file("$backupdir/dump1", 0);
	ok($dump_content !~ /polar_advisor;/,
		"polar_advisor not dumped with --exclude-schema");
	ok($dump_content =~ /test_schema;/,
		"test_schema dumped when polar_advisor excluded");

	# 2. polar_advisor dumped by default.
	$node->command_ok(
		[
			'pg_dump', '-f',
			"$backupdir/dump2", '--dbname',
			$node->connstr($db_source)
		],
		'default dump');
	$dump_content =
	  PostgreSQL::Test::Utils::slurp_file("$backupdir/dump2", 0);
	ok( $dump_content =~ /polar_advisor;/,
		"polar_advisor dumped with --schema");

	# 3. test dump polar_advisor.
	$node->command_ok(
		[
			'pg_dump', '--schema',
			'polar_advisor', '-Fc',
			'-f', "$backupdir/dump_polar_advisor",
			'--dbname', $node->connstr($db_source)
		],
		'dump with --schema');
	$dump_content =
	  PostgreSQL::Test::Utils::slurp_file("$backupdir/dump_polar_advisor", 0);
	# check the result of polar_advisor schema
	ok($dump_content !~ /test_schema;/,
		"test_schema not dumped when specifying polar_advisor");
	ok($dump_content =~ /CREATE SCHEMA IF NOT EXISTS polar_advisor;/,
		"CREATE SCHEMA IF NOT EXISTS polar_advisor");
	ok( $dump_content =~
		  /CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_blacklist/,
		"CREATE OR REPLACE FUNCTION polar_advisor.add_relation_to_blacklist");
	ok( $dump_content =~
		  /CREATE SEQUENCE IF NOT EXISTS polar_advisor.advisor_exec_id_seq/,
		"CREATE SEQUENCE IF NOT EXISTS polar_advisor.advisor_exec_id_seq");
	ok( $dump_content =~
		  /CREATE TABLE IF NOT EXISTS polar_advisor.blacklist_relations/,
		"CREATE TABLE IF NOT EXISTS polar_advisor.blacklist_relations");
	ok( $dump_content =~
		  /CREATE TABLE IF NOT EXISTS polar_advisor.db_level_advisor_log/,
		"CREATE TABLE IF NOT EXISTS polar_advisor.db_level_advisor_log");
	ok( $dump_content =~
		  /CREATE SEQUENCE IF NOT EXISTS polar_advisor.db_level_advisor_log_id_seq/,
		"CREATE SEQUENCE IF NOT EXISTS polar_advisor.db_level_advisor_log_id_seq"
	);
	ok( $dump_content =~
		  /DROP CONSTRAINT IF EXISTS blacklist_schema_table_pk/,
		"DROP CONSTRAINT IF EXISTS blacklist_schema_table_pk");
	ok( $dump_content =~
		  /ADD CONSTRAINT blacklist_schema_table_pk PRIMARY KEY/,
		"ADD CONSTRAINT blacklist_schema_table_pk PRIMARY KEY");
	ok( $dump_content =~
		  /DROP CONSTRAINT IF EXISTS db_level_advisor_log_pkey/,
		"DROP CONSTRAINT IF EXISTS db_level_advisor_log_pkey");
	ok( $dump_content =~
		  /ADD CONSTRAINT db_level_advisor_log_pkey PRIMARY KEY/,
		"ADD CONSTRAINT db_level_advisor_log_pkey PRIMARY KEY");
	ok( $dump_content =~
		  /DROP INDEX IF EXISTS polar_advisor.db_level_advisor_log_end_time_idx;/,
		"DROP INDEX IF EXISTS polar_advisor.db_level_advisor_log_end_time_idx"
	);
	ok( $dump_content =~ /CREATE INDEX db_level_advisor_log_end_time_idx/,
		"CREATE INDEX db_level_advisor_log_end_time_idx ON polar_advisor.db_level_advisor_log"
	);
	ok( $dump_content =~
		  /DROP INDEX IF EXISTS polar_advisor.db_level_advisor_log_start_time_idx;/,
		"DROP INDEX IF EXISTS polar_advisor.db_level_advisor_log_start_time_idx"
	);
	ok( $dump_content =~ /CREATE INDEX db_level_advisor_log_start_time_idx/,
		"CREATE INDEX db_level_advisor_log_start_time_idx ON polar_advisor.db_level_advisor_log"
	);
	# restore to target db
	$node->command_ok(
		[
			'pg_restore', '--dbname',
			$node->connstr($db_target), "$backupdir/dump_polar_advisor"
		],
		'restore polar_advisor');
	# restore again, this is ok because there's IF NOT EXISTS option in the command
	$node->command_ok(
		[
			'pg_restore', '--dbname',
			$node->connstr($db_target), "$backupdir/dump_polar_advisor"
		],
		'restore polar_advisor');

	# 4. test dump test schema.
	$node->command_ok(
		[
			'pg_dump', '--schema',
			'test_schema', '-Fc',
			'-f', "$backupdir/dump_test_schema",
			'--dbname', $node->connstr($db_source)
		],
		'dump with --schema');
	$dump_content =
	  PostgreSQL::Test::Utils::slurp_file("$backupdir/dump_test_schema", 0);
	# check the result of test schema, no IF NOT EXISTS option
	ok($dump_content !~ /polar_advisor;/,
		"polar_advisor not dumped when specifying test_schema");
	ok($dump_content =~ /CREATE SCHEMA test_schema;/,
		"CREATE SCHEMA test_schema");
	ok( $dump_content =~
		  /CREATE FUNCTION test_schema.add_relation_to_blacklist/,
		"CREATE FUNCTION test_schema.add_relation_to_blacklist");
	ok($dump_content =~ /CREATE SEQUENCE test_schema.advisor_exec_id_seq/,
		"CREATE SEQUENCE test_schema.advisor_exec_id_seq");
	ok($dump_content =~ /CREATE TABLE test_schema.blacklist_relations/,
		"CREATE TABLE test_schema.blacklist_relations");
	ok( $dump_content =~ /CREATE TABLE test_schema.test_db_level_advisor_log/,
		"CREATE TABLE test_schema.test_db_level_advisor_log");
	ok( $dump_content =~
		  /CREATE SEQUENCE test_schema.test_db_level_advisor_log_id_seq/,
		"CREATE SEQUENCE test_schema.test_db_level_advisor_log_id_seq");
	ok( $dump_content !~
		  /DROP CONSTRAINT IF EXISTS test_blacklist_schema_table_pk/,
		"no DROP CONSTRAINT IF EXISTS test_blacklist_schema_table_pk");
	ok( $dump_content =~
		  /ADD CONSTRAINT test_blacklist_schema_table_pk PRIMARY KEY/,
		"ADD CONSTRAINT test_blacklist_schema_table_pk PRIMARY KEY");
	ok( $dump_content !~
		  /DROP CONSTRAINT IF EXISTS test_db_level_advisor_log_pkey/,
		"no DROP CONSTRAINT IF EXISTS test_db_level_advisor_log_pkey");
	ok( $dump_content =~
		  /ADD CONSTRAINT test_db_level_advisor_log_pkey PRIMARY KEY/,
		"ADD CONSTRAINT test_db_level_advisor_log_pkey PRIMARY KEY");
	ok( $dump_content !~
		  /DROP INDEX IF EXISTS test_schema.test_db_level_advisor_log_end_time_idx;/,
		"no DROP INDEX IF EXISTS test_schema.test_db_level_advisor_log_end_time_idx"
	);
	ok( $dump_content =~
		  /CREATE INDEX test_db_level_advisor_log_end_time_idx/,
		"CREATE INDEX test_db_level_advisor_log_end_time_idx ON test_schema.test_db_level_advisor_log"
	);
	ok( $dump_content !~
		  /DROP INDEX IF EXISTS test_schema.test_db_level_advisor_log_start_time_idx;/,
		"no DROP INDEX IF EXISTS test_schema.test_db_level_advisor_log_start_time_idx"
	);
	ok( $dump_content =~
		  /CREATE INDEX test_db_level_advisor_log_start_time_idx/,
		"CREATE INDEX test_db_level_advisor_log_start_time_idx ON test_schema.test_db_level_advisor_log"
	);
	# restore
	$node->command_ok(
		[
			'pg_restore', '--dbname',
			$node->connstr($db_target), "$backupdir/dump_test_schema"
		],
		'restore test schema');

	$node->stop;
	$node->clean_node;
}

############### test for pg and ora mode ##############
check_dump_polar_advisor('pg');
# No ora mode now.
# check_dump_polar_advisor('ora');

done_testing();
