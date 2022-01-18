# Promote ro and then remount fs with incorrect mode after oom

use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>4;

my $db = 'postgres';
my $timeout = 1000;

my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica = get_new_node('replica1');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);

$node_master->append_conf('postgresql.conf',
	        "synchronous_standby_names='".$node_replica->name."'");

$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_master->append_conf('postgresql.conf', 'checkpoint_timeout=300s');
$node_master->append_conf('postgresql.conf', 'polar_csn_enable=on');
$node_master->append_conf('postgresql.conf', 'polar_enable_flashback_log = on');
$node_master->append_conf('postgresql.conf', 'polar_flashback_log_debug = on');

$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_replica->append_conf('postgresql.conf', 'checkpoint_timeout=300s');
$node_replica->append_conf('postgresql.conf', 'polar_csn_enable=on');
$node_replica->append_conf('postgresql.conf', 'polar_enable_flashback_log = on');
$node_replica->append_conf('postgresql.conf', 'polar_flashback_log_debug = on');

$node_master->start;

$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

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

# inject the fault in the old master
$node_master->safe_psql(
	'postgres',
	q[select inject_fault('all', 'reset');]);
$node_master->safe_psql(
	'postgres',
	q[select inject_fault('polar_partial_write_fault', 'enable', '', '', 1, 1, 0);]);
$node_master->psql('postgres', 'checkpoint;', on_error_stop=> 0);

$node_master->stop;

$node_replica->promote;

# wait
sleep(60);

$node_replica->polar_drop_slot($node_replica->name);

# inject the fault in the new master
$node_replica->safe_psql(
	'postgres',
	q[select inject_fault('all', 'reset');]);
$node_replica->safe_psql(
	'postgres',
	q[select inject_fault('polar_partial_write_fault', 'enable', '', '', 1, 1, 0);]);
$node_replica->psql('postgres', 'checkpoint;', on_error_stop=> 0);

# wait
sleep(60);

# and make sure we don't loss data
is($node_replica->safe_psql('postgres', qq[SELECT count(*) from test_crash_recovery;]),
	2000, 'replica node online promote table test_crash_recovery count is ok');
is($node_replica->safe_psql('postgres', qq[SELECT distinct(name) from test_crash_recovery;]),
	'b', 'replica node online promote table test_crash_recovery name is ok');
is($node_replica->safe_psql('postgres', qq[SELECT sum(id) from test_crash_recovery;]),
	2001000, 'replica node online promote table test_crash_recovery sum is ok');
is($node_replica->safe_psql('postgres', qq[SET enable_seqscan = off;SELECT sum(id) from test_crash_recovery;]),
	2001000, 'replica node online promote table test_crash_recovery index scan sum is ok');

$node_replica->stop;
