# Test streaming replication
# 1. Enable logindex in master and replica
# 2. Read transaction from SQL file 
# 3. Execute transaction on master and replica.
# 4. If it's readonly transaction, then compare the stdout and stderr from master and replica
# 4. If it's write transaction then check stderr from replica is not empty
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>2;

my $sql_dir = $ENV{PWD}.'/sql';
my $regress_db = 'polar_regression';

my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica = get_new_node('replica1');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);

$node_master->append_conf('postgresql.conf', 
	"synchronous_standby_names='".$node_replica->name."'");

$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');

$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

$node_master->start;

$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my @replicas = ($node_replica);
my $regress = PolarRegression->create_new_test($node_master);
$regress->register_random_pg_control('restart: master replia1');
$regress->register_random_pg_control('reload: all');
$regress->register_random_pg_control('promote: replica1');
print "random pg_control commands are: ".$regress->get_random_pg_control."\n";
$regress->replay_check($node_replica);

is ($node_replica->psql($regress_db, 'INSERT INTO replayed VALUES(1)'), 3,
	'read-only queries on replica');

my $start_time = time();
my $failed = $regress->test('serial_schedule', $sql_dir, \@replicas);

ok($failed == 0, "Polar regression test");

if ($failed)
{
	if (-e `printf polar_dump_core`)
	{
		print `ps -ef | grep postgres: | xargs -n 1 -P 0 gcore`;
	}
	sleep(864000);
}
my $cost_time = time() - $start_time;

note "Test cost time $cost_time";

my @nodes = ($node_master, $node_replica);
$regress->shutdown_all_nodes(\@nodes);
