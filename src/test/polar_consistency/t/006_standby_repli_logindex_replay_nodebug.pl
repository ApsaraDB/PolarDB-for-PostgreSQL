# Test streaming replication
# 1. Only enable logindex in replica
# 2. Read transaction from SQL file 
# 3. Execute transaction on master, replica and standby.
# 4. If it's readonly transaction, then compare the stdout and stderr from master, replica and standby
# 4. If it's write transaction then check stderr from replica and standby is not empty
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>1;
my $sql_dir = $ENV{PWD}.'/sql';
my $regress_db = 'polar_regression';
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');
my $node_replica1 = get_new_node('replica1');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);
my $node_standby1 = get_new_node('standby1');
$node_standby1->polar_init(0, 'polar_standby_logindex');
$node_standby1->polar_standby_set_recovery($node_master);
$node_master->append_conf('postgresql.conf', 
	"synchronous_standby_names='".$node_replica1->name.",".$node_standby1->name."'");
$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');

$node_replica1->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

$node_standby1->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

# Close polar_enable_persisted_buffer_pool temporarily.
$node_master->append_conf('postgresql.conf', 'polar_enable_persisted_buffer_pool=false');
$node_replica1->append_conf('postgresql.conf', 'polar_enable_persisted_buffer_pool=false');
$node_standby1->append_conf('postgresql.conf', 'polar_enable_persisted_buffer_pool=false');

$node_master->start;
$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_standby1->name);
$node_replica1->start;
$node_standby1->polar_standby_build_data();
$node_standby1->start;
$node_standby1->polar_drop_all_slots;

my @replicas = ($node_replica1, $node_standby1);
my $regress = PolarRegression->create_new_test($node_master);
$regress->register_random_pg_control('restart: standby1 replica1');
$regress->register_random_pg_control('reload: all');
$regress->register_random_pg_control('polar_promote: standby1');
$regress->register_random_pg_control('promote: replica1');
$regress->register_random_pg_control('promote: standby1');
print "random pg_control commands are: ".$regress->get_random_pg_control."\n";
my $start_time = time();
my $failed = $regress->test('parallel_schedule', $sql_dir, \@replicas);
ok($failed == 0, "Polar regression test");
if ($failed)
{
	if (-e `printf polar_dump_core`)
	{
		print `ps -ef | grep postgres: | xargs -n 1 -P 0 gcore`;
	}
	sleep(86400);
}
my $cost_time = time() - $start_time;
note "Test cost time $cost_time";

my @nodes = ($node_master, $node_replica1, $node_standby1);
$regress->shutdown_all_nodes(\@nodes);
