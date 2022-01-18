use strict;
use warnings;

use PostgresNode;
use Test::More tests => 2;

my $node = get_new_node('main');
$node->init;
$node->append_conf('postgresql.conf', 'polar_enable_multi_syslogger = off');
$node->start;

$node->issues_sql_like(
	[ 'vacuumdb', '-a' ],
	qr/statement: VACUUM.*statement: VACUUM/s,
	'vacuum all databases');
