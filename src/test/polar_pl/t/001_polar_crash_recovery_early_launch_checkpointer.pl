#
# Tests relating to PostgreSQL crash recovery and redo
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Config;
if ($Config{osname} eq 'MSWin32')
{

	# some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
else
{
	plan tests => 11;
}

my $node = get_new_node('master');
$node->init(allows_streaming => 1);
$node->start;

# by default disable early launch checkpointer and bgwriter
$node->safe_psql(
	'postgres',
	q[ALTER SYSTEM SET polar_enable_early_launch_checkpointer = off;]);
# restart db for polar_enable_early_launch_checkpointer
$node->restart;

# and make sure polar_enable_early_launch_checkpointer = off
is($node->safe_psql('postgres', qq[show polar_enable_early_launch_checkpointer;]),
	'off', 'polar_enable_early_launch_checkpointer=off');

my ($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that txid_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
my $tx = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
BEGIN;
CREATE TABLE mine(x integer);
SELECT txid_current();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
my $xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'in progress', 'own xid is in-progres');


# POLAR
# When stop immediate, the xlog does not be flushed in time, sleep to wait
# the xlog flushs.
sleep(1);

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT txid_current()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$tx->kill_kill;

# by default disable early launch checkpointer and bgwriter
$node->safe_psql(
	'postgres',
	q[ALTER SYSTEM SET polar_enable_early_launch_checkpointer = on;]);
# restart db for polar_enable_early_launch_checkpointer
$node->restart;

# and make sure polar_enable_early_launch_checkpointer = on
is($node->safe_psql('postgres', qq[show polar_enable_early_launch_checkpointer;]),
	'on', 'polar_enable_early_launch_checkpointer=on');

($stdin, $stdout, $stderr) = ('', '', '');

# Ensure that txid_status reports 'aborted' for xacts
# that were in-progress during crash. To do that, we need
# an xact to be in-progress when we crash and we need to know
# its xid.
$tx = IPC::Run::start(
	[
		'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres')
	],
	'<',
	\$stdin,
	'>',
	\$stdout,
	'2>',
	\$stderr);
$stdin .= q[
DROP TABLE IF EXISTS mine;
BEGIN;
CREATE TABLE mine(x integer);
SELECT txid_current();
];
$tx->pump until $stdout =~ /[[:digit:]]+[\r\n]$/;

# Status should be in-progress
$xid = $stdout;
chomp($xid);

is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'in progress', 'own xid is in-progres');


# POLAR
# When stop immediate, the xlog does not be flushed in time, sleep to wait
# the xlog flushs.
sleep(1);

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# Make sure we really got a new xid
cmp_ok($node->safe_psql('postgres', 'SELECT txid_current()'),
	'>', $xid, 'new xid after restart is greater');

# and make sure we show the in-progress xact as aborted
is($node->safe_psql('postgres', qq[SELECT txid_status('$xid');]),
	'aborted', 'xid is aborted after crash');

$tx->kill_kill;


# test for table value count
$node->safe_psql(
	'postgres',
	q[CREATE TABLE test_crash_recovery(id int primary key, name text);
		INSERT INTO test_crash_recovery select t,repeat('a',1024) from generate_series(1, 1000000) as t;]);

# POLAR
# When stop immediate, the xlog does not be flushed in time, sleep to wait
# the xlog flushs.
sleep(1);

# Crash and restart the postmaster
$node->stop('immediate');
$node->start;

# and make sure we don't loss data
is($node->safe_psql('postgres', qq[SELECT count(*) from test_crash_recovery;]),
	1000000, 'table test_crash_recovery count is ok');
is($node->safe_psql('postgres', qq[SELECT sum(id) from test_crash_recovery;]),
	500000500000, 'table test_crash_recovery sum is ok');
is($node->safe_psql('postgres', qq[SET enable_seqscan = off;SELECT sum(id) from test_crash_recovery;]),
	500000500000, 'table test_crash_recovery index scan sum is ok');
