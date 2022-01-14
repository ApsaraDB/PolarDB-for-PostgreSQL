use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 11;

program_help_ok('dropdb');
program_version_ok('dropdb');
program_options_handling_ok('dropdb');

my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf', 'polar_enable_multi_syslogger = off');
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE foobar1');
$node->issues_sql_like(
	[ 'dropdb', 'foobar1' ],
	qr/statement: DROP DATABASE foobar1/,
	'SQL DROP DATABASE run');

$node->command_fails([ 'dropdb', 'nonexistent' ],
	'fails with nonexistent database');
