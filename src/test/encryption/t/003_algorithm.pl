use strict;
# 003_algorithm.pl
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
#	  src/test/encryption/t/003_algorithm.pl

use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

if ($ENV{with_tde} ne 'yes')
{
	plan skip_all => 'TDE not supported by this build';
}
else
{
	plan tests => 8;
}

my $keyword = "secret keyword";
my $node = PostgreSQL::Test::Cluster->new('test-aes-128');
$node->init(
	extra => [
		'--cluster-passphrase-command',
		'echo "adfadsfadssssssssfa12312312312312312312312%p123"',
		'-e', 'aes-128',
	]);
$node->start;

# Check is the given relation file is encrypted
sub is_encrypted
{
	my $node = shift;
	my $filepath = shift;
	my $expected = shift;
	my $testname = shift;
	my $pgdata = $node->data_dir;

	open my $file, '<', "$pgdata/$filepath";
	sysread $file, my $buffer, 8192;

	my $ret = $buffer !~ /$keyword/ ? 1 : 0;

	is($ret, $expected, $testname);

	close $file;
}

$node->safe_psql(
	'postgres',
	qq(
				 CREATE TABLE test (a text);
				 INSERT INTO test VALUES ('$keyword');
				 ));
my $table_filepath =
  $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('test')));

# Read encrypted table
my $ret = $node->safe_psql('postgres', 'SELECT a FROM test');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $table_filepath, 1, 'table is encrypted');

# Test sm4
$node = PostgreSQL::Test::Cluster->new('test-sm4');
$node->init(
	extra => [
		'--cluster-passphrase-command',
		'echo "adfadsfadssssssssfa12312312312312312312312%p123"',
		'-e', 'sm4',
	]);
$node->start;

$node->safe_psql(
	'postgres',
	qq(
				 CREATE TABLE test (a text);
				 INSERT INTO test VALUES ('$keyword');
				 ));
$table_filepath =
  $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('test')));

# Read encrypted table
$ret = $node->safe_psql('postgres', 'SELECT a FROM test');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $table_filepath, 1, 'table is encrypted');

# Test command
$node = PostgreSQL::Test::Cluster->new('test-command-1');
$node->init(
	extra => [
		'--cluster-passphrase-command',
		'echo "adfadsfadssssssssfa12312312312312312312312%%123"',
		'-e', 'aes-256',
	]);
$node->start;

$node->safe_psql(
	'postgres',
	qq(
				 CREATE TABLE test (a text);
				 INSERT INTO test VALUES ('$keyword');
				 ));
$table_filepath =
  $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('test')));

# Read encrypted table
$ret = $node->safe_psql('postgres', 'SELECT a FROM test');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $table_filepath, 1, 'table is encrypted');

$node = PostgreSQL::Test::Cluster->new('test-command-2');
$node->init(
	extra => [
		'--cluster-passphrase-command',
		'echo "adfadsfadssssssssfa12312312312312312312312%123"',
		'-e', 'aes-256',
	]);
$node->start;

$node->safe_psql(
	'postgres',
	qq(
				 CREATE TABLE test (a text);
				 INSERT INTO test VALUES ('$keyword');
				 ));
$table_filepath =
  $node->safe_psql('postgres', qq(SELECT pg_relation_filepath('test')));

# Read encrypted table
$ret = $node->safe_psql('postgres', 'SELECT a FROM test');
is($ret, "$keyword", 'Read encrypted table');

# Sync to disk
$node->safe_psql('postgres', 'CHECKPOINT');

# Encrypted table must be encrypted
is_encrypted($node, $table_filepath, 1, 'table is encrypted');
