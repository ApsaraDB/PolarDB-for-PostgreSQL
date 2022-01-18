use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 13;

program_help_ok('createuser');
program_version_ok('createuser');
program_options_handling_ok('createuser');

my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf', 'polar_enable_multi_syslogger = off');
$node->start;

# 'Create role' statements does not leave any messages into log files, because of commit #19206238
$node->command_ok(
	[ 'createuser', 'regress_user1' ],
	'SQL CREATE USER run');
$node->command_ok(
	[ 'createuser', '-L', 'regress_role1' ],
	'create a non-login role');
$node->command_ok(
	[ 'createuser', '-r', 'regress_user2' ],
	'create a CREATEROLE user');
$node->command_ok(
	[ 'createuser', '-s', 'regress_user3' ],
	'create a superuser');

$node->command_fails([ 'createuser', 'regress_user1' ],
	'fails if role already exists');
