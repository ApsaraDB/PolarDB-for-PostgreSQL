# Test xact split:
#
# Test cases:
#  DML insert delete update
#  DDL create drop rename lock
#  subtransaction savepoint 'autonomous xact'
#  aborted xact
#  pg_export_snapshot

use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>1;
use Expect;

my $regress_db = "polar_regression";
my $timeout = 10;
my $result = 0;
my $sql_dir = $ENV{PWD}.'/xact_split_sql';

my $node_master = get_new_node("master");
$node_master->polar_init(1, "polar_master_logindex");

my $node_replica = get_new_node("replica1");
$node_replica->polar_init(0, "polar_repli_logindex");
$node_replica->polar_set_recovery($node_master);

$node_master->append_conf("postgresql.conf", q[
logging_collector=off
log_statement=all
]);
$node_replica->append_conf("postgresql.conf", q[
logging_collector=off
log_statement=all
]);

$node_master->start;
$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my @replicas = ($node_replica);

my $regress = PolarRegression->create_new_test($node_master);
$regress->set_test_mode("xact_split");
my $failed = $regress->test('xact_split_serial_schedule', $sql_dir, \@replicas);

is($result, 0,  "regression check");

$node_master->stop;
$node_replica->stop;
