#
# Tests flashback log
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
	plan tests => 12;
}

print "Please make check it with configure option --enable-inject-faults\n";
# set polar_enable_pwrite and polar_enable_async_pwrite on to cover the code polar_fill_file_zero
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');
$node_master->append_conf('postgresql.conf', 'polar_enable_flashback_log = on');
$node_master->append_conf('postgresql.conf', 'polar_flashback_log_debug = on');

my $node_replica = get_new_node('replica');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);
$node_replica->append_conf('postgresql.conf', 'polar_enable_flashback_log = on');
$node_replica->append_conf('postgresql.conf', 'polar_flashback_log_debug = on');

my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_standby_set_recovery($node_master);
$node_standby->append_conf('postgresql.conf', 'polar_enable_flashback_log = on');
$node_standby->append_conf('postgresql.conf', 'polar_flashback_log_debug = on');
$node_standby->append_conf('postgresql.conf', 'polar_enable_parallel_replay_standby_mode = on');

$node_master->start;
$node_master->polar_create_slot($node_replica->name);
$node_master->polar_create_slot($node_standby->name);

$node_replica->start;
$node_standby->polar_standby_build_data();
$node_standby->start;

$node_master->safe_psql(
	'postgres',
	q[CREATE extension faultinjector;]);
$node_master->safe_psql(
	'postgres',
	q[CREATE TABLE test_crash_recovery(id int primary key, name text);]);
$node_master->safe_psql(
	'postgres',
	q[INSERT INTO test_crash_recovery select t,'b' from generate_series(1, 1000) as t;]);

$node_master->safe_psql(
	'postgres',
	q[SELECT * from test_crash_recovery;]);

$node_master->safe_psql(
        'postgres',
        q[checkpoint;]);

$node_master->safe_psql(
        'postgres',
        q[INSERT INTO test_crash_recovery select t,'b' from generate_series(1001, 2000) as t;]);

# inject the fault in the standby
$node_standby->safe_psql(
        'postgres',
        q[checkpoint]);
$node_standby->safe_psql(
        'postgres',
        q[select inject_fault('all', 'reset');]);
$node_standby->safe_psql(
        'postgres',
        q[select inject_fault('polar_partial_write_fault', 'enable', '', '', 1, 1, 0);]);
$node_standby->psql('postgres', 'checkpoint;', on_error_stop=> 0);

# inject the fault in the master
$node_master->safe_psql(
	'postgres',
	q[select inject_fault('all', 'reset');]);
$node_master->safe_psql(
	'postgres',
	q[select inject_fault('polar_partial_write_fault', 'enable', '', '', 1, 1, 0);]);
$node_master->psql('postgres', 'checkpoint;', on_error_stop=> 0);

sleep(120);

# and make sure we don't loss data
is($node_master->safe_psql('postgres', qq[SELECT count(*) from test_crash_recovery;]),
	2000, 'master node table test_crash_recovery count is ok');
is($node_master->safe_psql('postgres', qq[SELECT distinct(name) from test_crash_recovery;]),
	'b', 'master node table test_crash_recovery name is ok');
is($node_master->safe_psql('postgres', qq[SELECT sum(id) from test_crash_recovery;]),
	2001000, 'master node table test_crash_recovery sum is ok');
is($node_master->safe_psql('postgres', qq[SET enable_seqscan = off;SELECT sum(id) from test_crash_recovery;]),
	2001000, 'master node table test_crash_recovery index scan sum is ok');

# and make sure we don't loss data
is($node_replica->safe_psql('postgres', qq[SELECT count(*) from test_crash_recovery;]),
	2000, 'replica node table test_crash_recovery count is ok');
is($node_replica->safe_psql('postgres', qq[SELECT distinct(name) from test_crash_recovery;]),
	'b', 'replica node table test_crash_recovery name is ok');
is($node_replica->safe_psql('postgres', qq[SELECT sum(id) from test_crash_recovery;]),
	2001000, 'replica node table test_crash_recovery sum is ok');
is($node_replica->safe_psql('postgres', qq[SET enable_seqscan = off;SELECT sum(id) from test_crash_recovery;]),
	2001000, 'replica node table test_crash_recovery index scan sum is ok');

# and make sure we don't loss data
is($node_standby->safe_psql('postgres', qq[SELECT count(*) from test_crash_recovery;]),
	2000, 'standby node table test_crash_recovery count is ok');
is($node_standby->safe_psql('postgres', qq[SELECT distinct(name) from test_crash_recovery;]),
	'b', 'standby node table test_crash_recovery name is ok');
is($node_standby->safe_psql('postgres', qq[SELECT sum(id) from test_crash_recovery;]),
	2001000, 'standby node table test_crash_recovery sum is ok');
is($node_standby->safe_psql('postgres', qq[SET enable_seqscan = off;SELECT sum(id) from test_crash_recovery;]),
	2001000, 'standby node table test_crash_recovery index scan sum is ok');

# disable flashback log to cover the code polar_flashback_log_remove_all
$node_master->stop;
$node_master->append_conf('postgresql.conf', 'polar_enable_flashback_log = off');
$node_master->start;

$node_standby->stop;
$node_standby->append_conf('postgresql.conf', 'polar_enable_flashback_log = off');
$node_standby->start;

$node_master->stop;
$node_standby->stop;
$node_replica->stop;