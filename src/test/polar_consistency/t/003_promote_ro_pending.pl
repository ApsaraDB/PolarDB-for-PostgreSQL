# Test promote ro when it's in snapshot pending state
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>2;

my $sql_file = $ENV{PWD}.'/sql/uncommit_trans.sql';
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

my $master_psql = $node_master->psql_connect($db, $timeout);
$node_master->set_psql($master_psql);

open my $fh, '<', $sql_file or die "Failed to open $sql_file: $!";
my $sql='';

while (my $line = <$fh>)
{
 	$sql .= $line;
}

my $master_output = $node_master->psql_execute($sql, $timeout, $node_master->get_psql());
my $checkpoint_sql = $node_master->psql_connect($db, $timeout);

$node_master->psql_execute("checkpoint;", $timeout, $checkpoint_sql);
$node_master->psql_close($checkpoint_sql);

my $failed = $node_replica->restart_no_check;
ok($failed != 0, "RO is in snapshot pending");

$node_master->kill9;
$node_replica->promote;

# wait for new master to startup
$node_replica->polar_wait_for_startup(30, 0);
$node_replica->polar_drop_all_slots;

my $result = $node_replica->safe_psql('postgres', "select 1");
chomp($result);
ok($result == 1, "RO is promoted");

$node_replica->polar_drop_slot('replica1');
$node_replica->stop;
