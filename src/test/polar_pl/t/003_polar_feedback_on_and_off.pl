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
use Test::More tests=>6;

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
$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_standby->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

$node_master->start;
$node_master->polar_create_slot($node_replica->name);
$node_master->polar_create_slot($node_standby->name);

$node_replica->start;
$node_standby->polar_standby_build_data();
$node_standby->start;

sub get_slot_xmins
{
	my ($node, $slotname, $check_expr) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT $check_expr
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$slotname';
	]) or die "Timed out waiting for slot xmins to advance";

	my $slotinfo = $node->slot($slotname);
	return ($slotinfo->{'xmin'});
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

sub wait_xmin_update_ready
{
	my ($node, $slotname, $xmin_expected) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT xmin::text::int >= $xmin_expected 
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$slotname';
	]) or die "Timed out waiting for slot xmins ready";
}

$node_master->safe_psql('postgres', 'CREATE TABLE replayed(val integer);');

# 1. disable hot_standby_feedback on replica
$node_replica->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_replica->reload;
replay_check();
my $xmin = get_slot_xmins($node_master, $node_replica->name,
	"xmin IS NULL");
is($xmin, '', 'replica xmin of non-cascaded slot null with hs_feedback off');

# 2. enable hot_standby_feedback on replica
$node_replica->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_replica->reload;
replay_check();
my $replica_ret = $node_replica->safe_psql('postgres',
	"select txid_current_snapshot();");
my ($replica_xmin, $replica_xmax, $replica_xip) = split /:/, $replica_ret;
wait_xmin_update_ready($node_master, $node_replica->name, $replica_xmin);
$xmin = get_slot_xmins($node_master, $node_replica->name,
	"xmin IS NOT NULL");
ok($xmin >= $replica_xmin, 'replica xmin of non-cascaded slot not null with hs_feedback on');

# 3. disable hot_standby_feedback and polar_standby_feedback on standby
note "disable hot_standby_feedback and polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name,
	"xmin IS NULL");
is($xmin, '', 'standby xmin of non-cascaded slot null with off hs_feedback and polar_feedback');

# 4. disable hot_standby_feedback and enable polar_standby_feedback on standby
note "disable hot_standby_feedback and enable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name,
	"xmin IS NULL");
is($xmin, '', 'xmin of non-cascaded slot null with off hs_feedback,on polar_feedback');

# 5. enable hot_standby_feedback and enable polar_standby_feedback on standby
note "enable hot_standby_feedback and enable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
replay_check();
my $standby_ret = $node_standby->safe_psql('postgres',
	"select txid_current_snapshot();");
my ($standby_xmin, $standby_xmax, $standby_xip) = split /:/, $standby_ret;
wait_xmin_update_ready($node_master, $node_standby->name, $standby_xmin);
$xmin = get_slot_xmins($node_master, $node_standby->name,
	"xmin IS NOT NULL");
ok($xmin >= $standby_xmin, 'standby xmin of non-cascaded slot not null with on hs_feedback and polar_feedback');

# 6. enable hot_standby_feedback and disable polar_standby_feedback on standby
note "enable hot_standby_feedback and enable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name,
	"xmin IS NULL");
is($xmin, '', 'standby xmin of non-cascaded slot not null on hs_feedback,off polar_feedback');

$node_master->stop;
$node_replica->stop;
$node_standby->stop;