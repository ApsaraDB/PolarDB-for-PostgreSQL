# Test px consistency for rw and ro
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

my $node_replica2 = get_new_node('replica2');
$node_replica2->polar_init(0, 'polar_repli_logindex');
$node_replica2->polar_set_recovery($node_master);

sub px_init_conf
{
	my ($node) = @_;
	$node->append_conf('postgresql.conf', "logging_collector=off");
	$node->append_conf('postgresql.conf', 'polar_enable_px=1');
	$node->append_conf('postgresql.conf', 'polar_px_enable_check_workers=0');
	$node->append_conf('postgresql.conf', 'polar_px_enable_replay_wait=1');
	$node->append_conf('postgresql.conf', 'polar_px_optimizer_trace_fallback=1');
	$node->append_conf('postgresql.conf', 'polar_px_dop_per_node=3');
	$node->append_conf('postgresql.conf', 'polar_px_max_workers_number=0');
	$node->append_conf('postgresql.conf', "listen_addresses=localhost");
}

px_init_conf($node_master);
$node_master->start;

$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_replica2->name);

px_init_conf($node_replica1);
px_init_conf($node_replica2);
$node_replica1->start;
$node_replica2->start;

my @replicas = ($node_replica1, $node_replica2);
my $regress = PolarRegression->create_new_test($node_master);
my $start_time = time();
my $failed = $regress->test('px_serial_schedule', $sql_dir, \@replicas);
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
my @nodes = ($node_master, $node_replica1, $node_replica2);
$regress->shutdown_all_nodes(\@nodes);
