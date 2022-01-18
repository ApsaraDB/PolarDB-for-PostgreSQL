# Test for datamax feedback standby xmin
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>13;

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');
my $primary_system_identifier = $node_master->polar_get_system_identifier;

# datamax
my $node_datamax = get_new_node('datamax');
$node_datamax->polar_init_datamax($primary_system_identifier,'polar_standby_logindex');

# standby
my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_set_root_node($node_master);
$node_standby->polar_standby_build_data;
$node_standby->polar_standby_set_recovery($node_datamax);

$node_master->append_conf('postgresql.conf', "polar_enable_transaction_sync_mode = on");
$node_master->append_conf('postgresql.conf', "synchronous_commit = on");
$node_master->append_conf('postgresql.conf', "synchronous_standby_names='".$node_datamax->name."'");

$node_master->start;
$node_master->polar_create_slot($node_datamax->name);

$node_datamax->start;
$node_datamax->polar_create_slot($node_standby->name);
$node_datamax->safe_psql('postgres','CREATE EXTENSION polar_monitor;');
$node_datamax->stop;

# set datamax recovery.conf
$node_datamax->polar_datamax_set_recovery($node_master);
$node_datamax->start;
$node_standby->start;

my $regress = PolarRegression->create_new_test($node_master);

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

sub insert_data
{
	my ($count) = @_;
	my $i = 0;
	for($i = 0; $i < $count; $i = $i + 1)
	{
		$regress->replay_check($node_standby);
	}
	return;
}

$node_master->safe_psql('postgres', 'CREATE TABLE replayed(val integer);');

# 1. disable hot_standby_feedback on datamax and standby
note "disable hot_standby_feedback on datamax and standby";
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_datamax->reload;
my $replay_ret = $regress->replay_check($node_standby);
ok($replay_ret == 1, "standby replay data success");
my $datamax_xmin = get_slot_xmins($node_datamax, $node_standby->name, "xmin IS NULL");
my $master_xmin = get_slot_xmins($node_master, $node_datamax->name, "xmin IS NULL");
is($datamax_xmin, '', 'datamax xmin is null when feedback is disabled both on datamax and standby');
is($master_xmin, '', 'master xmin is null when feedback is disabled both on datamax and standby');

# 2. enable hot_standby_feedback on datamax and standby
note "enable hot_standby_feedback on datamax and standby";
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_datamax->reload;
$replay_ret = 0;
$replay_ret = $regress->replay_check($node_standby);
ok($replay_ret == 1, "standby replay data success");
my $standby_ret = $node_standby->safe_psql('postgres',"select txid_current_snapshot();");
my($standby_xmin, $standby_xmax, $standby_xip) = split /:/, $standby_ret;
wait_xmin_update_ready($node_datamax, $node_standby->name, $standby_xmin);
wait_xmin_update_ready($node_master, $node_datamax->name, $standby_xmin);
$datamax_xmin = get_slot_xmins($node_datamax, $node_standby->name, "xmin IS NOT NULL");
$master_xmin = get_slot_xmins($node_master, $node_datamax->name, "xmin IS NOT NULL");
print "standby_xmin: $standby_xmin, datamax_xmin: $datamax_xmin, master_xmin: $master_xmin\n";
ok($datamax_xmin <= $standby_xmin, 'datamax record standby xmin when feedback is enabled both on datamax and standby');
ok($master_xmin >= $standby_xmin, 'datamax feedback standby xmin to master when feedback is enabled both on datamax and standby');

# 3. enable hot_standby_feedback on datamax, disable hot_standby_feedback on standby
note "enable hot_standby_feedback on datamax, disable hot_standby_feedback on standby";
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_datamax->reload;
$replay_ret = 0;
$replay_ret = $regress->replay_check($node_standby);
ok($replay_ret == 1, "standby replay data success");
$datamax_xmin = get_slot_xmins($node_datamax, $node_standby->name, "xmin IS NULL");
$master_xmin = get_slot_xmins($node_master, $node_datamax->name, "xmin IS NULL");
# because datamax feedback standby xmin to master, so it is null when feedback is disabled on standby
is($datamax_xmin, '', 'datamax xmin is null when feedback is enabled on datamax but disabled on standby');
is($master_xmin, '', 'master xmin is null when feedback is enabled on datamax but disabled on standby');

# 4. disable hot_standby_feedback on datamax, enable hot_standby_feedback on standby
note "disable hot_standby_feedback on datamax, enable hot_standby_feedback on standby";
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_datamax->safe_psql('postgres', 'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_datamax->reload;
$replay_ret = 0;
$replay_ret = $regress->replay_check($node_standby);
ok($replay_ret == 1, "standby replay data success");
$standby_ret = $node_standby->safe_psql('postgres',"select txid_current_snapshot();");
($standby_xmin, $standby_xmax, $standby_xip) = split /:/, $standby_ret;
wait_xmin_update_ready($node_datamax, $node_standby->name, $standby_xmin);
$datamax_xmin = get_slot_xmins($node_datamax, $node_standby->name, "xmin IS NOT NULL");
$master_xmin = get_slot_xmins($node_master, $node_datamax->name, "xmin IS NULL");
ok($datamax_xmin <= $standby_xmin, 'datamax record standby xmin when feedback is enabled on standby but disabled on datamax');
is($master_xmin, '', 'master xmin is null when feedback is enabled on standby but disabled on datamax');

# test for datamax starting streaming from last valid lsn
# loop for insert data, so that datamax can update last valid lsn 
$node_master->safe_psql('postgres', "select pg_switch_wal();");	
insert_data(10);

# restart datamax to parse xlog
$node_datamax->restart;
my $write_lsn = $node_master->lsn('write');
print "master write lsn: $write_lsn\n";
my $datamax_lsn = $node_datamax->safe_psql('postgres',
		"SELECT last_valid_received_lsn FROM polar_get_datamax_info()");
chomp($datamax_lsn);
print "datamax_lsn: $datamax_lsn\n";
ok($write_lsn le $datamax_lsn, "last_valid_received_lsn of datamax is eaqul to the write lsn of master");

$node_standby->stop;
$node_datamax->stop;
$node_master->stop;