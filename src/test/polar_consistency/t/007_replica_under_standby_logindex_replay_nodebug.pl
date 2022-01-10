# Test streaming replication
# 1. Enable logindex in master, replica and standby.
# 2. Startup replica under standby.
# 3. Read transaction from SQL file.
# 4. Execute transaction on master, replica and standby.
# 5. If it's readonly transaction, then compare the stdout and stderr from master, replica and standby
# 6. If it's write transaction then check stderr from replica and standby is not empty
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>1;
my $sql_dir = $ENV{PWD}.'/sql';
my $regress_db = 'polar_regression';

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# replica1 under master
my $node_replica1 = get_new_node('replica1');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);

# standby1 under master
my $node_standby1 = get_new_node('standby1');
$node_standby1->polar_init(0, 'polar_standby_logindex');
$node_standby1->polar_standby_set_recovery($node_master);

# replica2 under master
my $node_replica2 = get_new_node('replica2');
$node_replica2->polar_init(0, 'polar_repli_logindex');

# postgresql.conf
$node_master->append_conf('postgresql.conf', 
	"synchronous_standby_names='".$node_replica1->name.",".$node_standby1->name."'");
$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_replica1->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_standby1->append_conf('postgresql.conf', 
	"synchronous_standby_names='".$node_replica2->name."'");
$node_standby1->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_replica2->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

# start server
$node_master->start;
$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_standby1->name);
$node_replica1->start;

$node_standby1->polar_standby_build_data();
$node_standby1->start;
$node_standby1->polar_drop_all_slots;
$node_standby1->polar_create_slot($node_replica2->name);
$node_replica2->polar_set_recovery($node_standby1);
$node_replica2->start;

# start test
my @replicas = ($node_replica1, $node_standby1, $node_replica2);
my $regress = PolarRegression->create_new_test($node_master);
$regress->register_random_pg_control('restart: standby1 replica2');
$regress->register_random_pg_control('reload: all');
$regress->register_random_pg_control('polar_promote: standby1');
$regress->register_random_pg_control('promote: replica1');
# TODO: add standby online promote
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

# stop server
my @nodes = ($node_master, $node_replica1, $node_replica2, $node_standby1);
$regress->shutdown_all_nodes(\@nodes);
