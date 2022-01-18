use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use TestLib ();
use Test::More tests=>12;

my $regress_db = 'postgres';

# Test rename wal ready file.
sub test_rename_wal_ready
{
	my $node          = shift;
	my $role          = shift;
	my $password      = shift;
	my $wal_name      = shift;
	my $expected_res  = shift;

	$ENV{"PGPASSWORD"} = $password;
	my $res = $node->psql($regress_db, "set polar_rename_wal_ready_file = '$wal_name'\n", extra_params => [ '-U', $role ]);
	print $res;
	print "\n";
	print "rename wal ready file $wal_name for role $role with password $password\n";
	print $node->data_dir();
	print "\n";

	is($res, $expected_res,
		"rename wal ready file $wal_name for $node, role $role");
	print "\n";

	return;
}

my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica = get_new_node('replica1');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);

$node_master->append_conf('postgresql.conf',
	        "synchronous_standby_names='".$node_replica->name."'");

$node_master->append_conf('postgresql.conf', 'archive_mode=on');
$node_master->append_conf('postgresql.conf', "logging_collector=off");
$node_replica->append_conf('postgresql.conf', 'archive_mode=on');
$node_replica->append_conf('postgresql.conf', "logging_collector=off");

$node_master->start;

$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my $result = $node_master->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RW is running");

$result = $node_replica->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RO is running");

my $wal_name_exists = '';

# Make sure that wal_name.ready create successfully.
is($node_master->psql($regress_db, 'SELECT pg_walfile_name(pg_switch_wal())', stdout => \$wal_name_exists), 0, "select pg_switch_wal() to create wal.ready");
is($node_master->psql($regress_db, "CREATE USER normal PASSWORD 'normal';"), 0, "create normal user");


# Start tests.
test_rename_wal_ready($node_replica, 'postgres', '', "$wal_name_exists", '3');
test_rename_wal_ready($node_master, 'normal', 'normal', "$wal_name_exists", '3');

# It will fail in localmode, for mode == ----------.
# REF: polar_touch_file->polar_creat(file, mode = 0).
# It will be success in Polarstore, for there is no mode in Polarstore.
test_rename_wal_ready($node_master, 'postgres', '', "$wal_name_exists", '0');

test_rename_wal_ready($node_master, 'postgres', '', '.', '3');
test_rename_wal_ready($node_master, 'postgres', '', '/', '3');
test_rename_wal_ready($node_master, 'postgres', '', ' ', '3');
test_rename_wal_ready($node_master, 'postgres', '', '', '0');
test_rename_wal_ready($node_master, 'postgres', '', '000000011111111111111111', '3');
# Finish tests.

$node_master->stop;
$node_replica->stop;
