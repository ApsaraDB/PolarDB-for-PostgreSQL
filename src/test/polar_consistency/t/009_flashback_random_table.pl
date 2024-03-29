# Test flashback table
# 1. Enable logindex in master and replica
# 2. Read transaction from SQL file
# 3. Execute serial_schedule on master and replica.
# 4. If it's readonly transaction, then compare the stdout and stderr from master and replica
# 5. If it's write transaction then check stderr from replica is not empty
# 6. select a random table, copy it and wait 3 seconds to flashback the random table and check
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

$node_master->append_conf('postgresql.conf',
	"synchronous_standby_names='".$node_replica1->name."'");

$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_master->append_conf('postgresql.conf', 'polar_enable_flashback_log=on');
$node_master->append_conf('postgresql.conf', 'polar_enable_fast_recovery_area=on');

$node_replica1->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_replica1->append_conf('postgresql.conf', 'polar_enable_flashback_log=on');
$node_replica1->append_conf('postgresql.conf', 'polar_enable_fast_recovery_area=on');

$node_master->start;

$node_master->polar_create_slot($node_replica1->name);
$node_replica1->start;

my @replicas = ($node_replica1);
my $regress = PolarRegression->create_new_test($node_master);
$regress->enable_random_flashback();

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
my @nodes = ($node_master, $node_replica1);
$regress->shutdown_all_nodes(\@nodes);