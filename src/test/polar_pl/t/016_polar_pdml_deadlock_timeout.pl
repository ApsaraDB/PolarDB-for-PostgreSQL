#
# Test PX PDML deadlock timeout with two sessions.
#   Session 1 (main): execute a parallel delete hang 
#        after qc get lock and before delete worker run (by fault injection).
#   Session 2 (child): execute vacuum full after main hang. deadlock trigger
#   parallel delete is expected to be canceled due to wait lock timeout.
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>1;

# database
my $regress_db = 'postgres';

# RW
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# RO1
my $node_replica1 = get_new_node('replica1');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);

# RO2
my $node_replica2 = get_new_node('replica2');
$node_replica2->polar_init(0, 'polar_repli_logindex');
$node_replica2->polar_set_recovery($node_master);

# RW basic PX configuration
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
    $node->append_conf('postgresql.conf', 'polar_px_enable_delete=1');
	$node->append_conf('postgresql.conf', "listen_addresses=localhost");
}
px_init_conf($node_master);

# cluster map
$node_master->append_conf('postgresql.conf', "polar_cluster_map='node1|127.0.0.1|".$node_replica1->port.", node2|127.0.0.1|".$node_replica2->port."'");
# wait lock timeout
$node_master->append_conf('postgresql.conf', "polar_px_wait_lock_timeout=10000");
$node_master->start;

$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_replica2->name);
$node_replica1->start;
$node_replica2->start;

my $start_time = time();

# enable fault injector extension
$node_master->psql($regress_db, "create extension faultinjector;");

# create table and load data
$node_master->psql($regress_db, "drop table deadlock_table1 cascade;");
$node_master->psql($regress_db, "CREATE TABLE deadlock_table1 (c1 int, c2 int);");
$node_master->psql($regress_db, "insert into deadlock_table1 select generate_series(1,1000),generate_series(1,1000);");

# child session on RW
my $pid = fork();
if( $pid == 0 ) {
    $ENV{"whoami"} = "child";

	# wait for main session to start PX query
	$node_master->psql($regress_db, "select pg_sleep(3);");
	# perform DDL to wait lock
	$node_master->psql($regress_db, "vacuum full;");

    exit 0;
}

my $errmsg = "";
# inject fault once to hang the subsequent PX query
$node_master->psql($regress_db, "select inject_fault('all', 'reset');");
$node_master->psql($regress_db, "select inject_fault('pdml_deadlock_creation', 'enable', '', 'pdml_test_table', 1, 1, 0);");
$node_master->psql($regress_db,
				   "delete from deadlock_table1 where c1 <10;",
				   stderr => \$errmsg);

# PX query is expected to be canceled
is($errmsg,
   "psql:<stdin>:1: ERROR:  canceling statement due to conflict with recovery\nDETAIL:  User was holding a relation lock for too long.",
   'PX query is expected to be canceled by timeout.');

my $cost_time = time() - $start_time;
print "Test cost time $cost_time\n";

# stop the cluster
$node_master->stop;
$node_replica1->stop;
$node_replica2->stop;