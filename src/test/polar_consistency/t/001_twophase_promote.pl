# Test promote ro when there's twophase 
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>2;

my $sql_file = $ENV{PWD}.'/sql/twophase_promote.sql';
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

open my $fh, '<', $sql_file or die "Failed to open $sql_file: $!";
my $sql='';

while (my $line = <$fh>)
{
 	$sql .= $line;
}

$node_master->safe_psql($db, $sql);
$node_master->stop;

$node_replica->promote;

# wait for new master to startup
$node_replica->polar_wait_for_startup(30, 0);
$node_replica->polar_drop_all_slots;

$node_replica->safe_psql($db, "COMMIT PREPARED 'twophase_test';");

my $result = $node_replica->safe_psql($db, "SELECT MAX(v) FROM twophase;");
chomp($result);
ok($result == 10000, "RO is promoted");

$result = $node_replica->safe_psql($db, "SELECT COUNT(*) FROM twophase;");
chomp($result);
ok($result == 91, "All data are visible");

$node_replica->polar_drop_slot('replica1');
$node_replica->stop;
