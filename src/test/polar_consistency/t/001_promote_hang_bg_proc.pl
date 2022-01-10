# Stop ro background process and then promote it
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>3;

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
$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_replica->append_conf('postgresql.conf', 'checkpoint_timeout=300s');
$node_replica->append_conf('postgresql.conf', 'polar_csn_enable=on');

$node_master->start;

$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my $result = $node_master->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RW is running");

$result = $node_replica->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RO is running");

$node_master->stop;

$node_replica->stop_child('background logindex writer');
$node_replica->promote_async;
sleep 2;
$node_replica->resume_child('background logindex writer');

# wait for new master to startup
$node_replica->polar_wait_for_startup(30, 0);
$node_replica->polar_drop_all_slots;
$result = $node_replica->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RO is promoted");

$node_replica->polar_drop_slot('replica1');
$node_replica->stop;
