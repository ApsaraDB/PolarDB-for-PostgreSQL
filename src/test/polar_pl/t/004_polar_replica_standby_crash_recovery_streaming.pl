# Test hot_standby_feedback and polar_standby_feedback
# There are six scenarios
# 1. disable hot_standby_feedback on replica
# 2. enable hot_standby_feedback on replica
# 3. disable hot_standby_feedback and polar_standby_feedback on standby
# 4. disable hot_standby_feedback and enable polar_standby_feedback on standby
# 5. enable hot_standby_feedback and enable polar_standby_feedback on standby
# 6. enable hot_standby_feedback and disable polar_standby_feedback on standby
#
use strict;
use warnings;
use PostgresNode;
use Test::More tests=>20;

my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica = get_new_node('replica');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);

my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_standby_set_recovery($node_master);

$node_master->append_conf('postgresql.conf',
	"synchronous_standby_names='".$node_replica->name.",".$node_standby->name."'");
$node_master->append_conf('postgresql.conf', 'polar_enable_xlog_buffer=on');
$node_master->append_conf('postgresql.conf', 'polar_enable_pread=on');
$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_standby->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

$node_master->start;
$node_master->polar_create_slot($node_replica->name);
$node_master->polar_create_slot($node_standby->name);

$node_replica->start;
$node_standby->polar_standby_build_data();
$node_standby->start;

sub wait_replica_slot_ready
{
	my ($node, $replica_slot) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT active = 't'
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$replica_slot';
	]) or die "Timed out waiting for replica slot ready";
}

sub wait_standby_slot_ready
{
	my ($node, $standby_slot) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT active = 't'
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$standby_slot';
	]) or die "Timed out waiting for standby slot ready";
}

sub replay_check
{
	my $newval = $node_master->safe_psql('postgres',
		'INSERT INTO replayed(val) SELECT coalesce(max(val),0) + 1 AS newval FROM replayed RETURNING val'
	);

    $node_master->wait_for_catchup($node_replica, 'replay',
        $node_master->lsn('insert'));
    $node_master->wait_for_catchup($node_standby, 'replay',
        $node_master->lsn('insert'));
    $node_replica->safe_psql('postgres',
		qq[SELECT 1 FROM replayed WHERE val = $newval])
	  or die "replica didn't replay master value $newval";
    $node_standby->safe_psql('postgres',
		qq[SELECT 1 FROM replayed WHERE val = $newval])
	  or die "standby didn't replay master value $newval";
    return;
}

sub slot_check
{
	replay_check();
	my $replica_slot = $node_replica->name;
	my $standby_slot = $node_standby->name;
	my $replica_slot_state = $node_master->safe_psql('postgres',
		"select active from pg_replication_slots where slot_name='$replica_slot';");
	my $standby_slot_state = $node_master->safe_psql('postgres',
		"select active from pg_replication_slots where slot_name='$standby_slot';");
	is($replica_slot_state, 't', 'replica slot active');
	is($standby_slot_state, 't', 'standby slot active');

	# Crash and restart the node_replica and standby
	$node_replica->stop('immediate');
	$node_replica->start;

	$node_standby->stop('immediate');
	$node_standby->start;
}

wait_replica_slot_ready($node_master, $node_replica->name);
wait_standby_slot_ready($node_master, $node_standby->name);

$node_master->safe_psql('postgres', 'CREATE TABLE replayed(val integer);');

for( $a = 0; $a < 10; $a = $a + 1 ){
    slot_check();
}

$node_master->stop;
$node_replica->stop;
$node_standby->stop;