use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 23;

program_help_ok('reindexdb');
program_version_ok('reindexdb');
program_options_handling_ok('reindexdb');

my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf', 'polar_enable_multi_syslogger = off');
$node->start;

$ENV{PGOPTIONS} = '--client-min-messages=WARNING';

$node->issues_sql_like(
	[ 'reindexdb', 'postgres' ],
	qr/statement: REINDEX DATABASE postgres;/,
	'SQL REINDEX run');

$node->safe_psql('postgres',
	'CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a);');
$node->issues_sql_like(
	[ 'reindexdb', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX TABLE public\.test1;/,
	'reindex specific table');
$node->issues_sql_like(
	[ 'reindexdb', '-i', 'test1x', 'postgres' ],
	qr/statement: REINDEX INDEX public\.test1x;/,
	'reindex specific index');
$node->issues_sql_like(
	[ 'reindexdb', '-S', 'pg_catalog', 'postgres' ],
	qr/statement: REINDEX SCHEMA pg_catalog;/,
	'reindex specific schema');
$node->issues_sql_like(
	[ 'reindexdb', '-s', 'postgres' ],
	qr/statement: REINDEX SYSTEM postgres;/,
	'reindex system tables');
$node->issues_sql_like(
	[ 'reindexdb', '-v', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX \(VERBOSE\) TABLE public\.test1;/,
	'reindex with verbose output');

$node->command_ok([qw(reindexdb --echo --table=pg_am dbname=template1)],
	'reindexdb table with connection string');
$node->command_ok(
	[qw(reindexdb --echo dbname=template1)],
	'reindexdb database with connection string');
$node->command_ok(
	[qw(reindexdb --echo --system dbname=template1)],
	'reindexdb system with connection string');
