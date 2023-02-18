#
# Test PX wait lock timeout with two sessions.
#	Session 1 (main): execute a hanging PX query (by fault injection).
#	Session 2 (child): execute a DDL whose lock mode conflicts with PX query.
#	
#	PX query is expected to be canceled due to wait lock timeout.
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
	$node->append_conf('postgresql.conf', "listen_addresses=localhost");
}
px_init_conf($node_master);

# wait lock timeout
$node_master->append_conf('postgresql.conf', "polar_px_wait_lock_timeout=3000");
$node_master->start;

$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_replica2->name);
$node_replica1->start;
$node_replica2->start;

my $start_time = time();

# enable fault injector extension
$node_master->psql($regress_db, "create extension faultinjector;");

# create table and index
$node_master->psql($regress_db, "drop table if exists test_table;");
$node_master->psql($regress_db, "create table test_table (id int);");
$node_master->psql($regress_db, "insert into test_table(id) select generate_series(1, 1000) as id;");
$node_master->psql($regress_db, "create index i on test_table(id);");

# child session on RW
my $pid = fork();
if( $pid == 0 ) {
	$ENV{"whoami"} = "child";

	# wait for main session to start PX query
	$node_master->psql($regress_db, "select pg_sleep(3);");
	# perform DDL to wait lock
	$node_master->psql($regress_db, "drop index i;");
	
	exit 0;
}

my $errmsg = "";

# inject fault once to hang the subsequent PX query
$node_master->psql($regress_db, "select inject_fault('all', 'reset');");
$node_master->psql($regress_db, "select inject_fault('px_wait_lock_timeout', 'enable', '', 'test_table', 1, 1, 0);");
$node_master->psql($regress_db,
				   "select * from test_table;",
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
