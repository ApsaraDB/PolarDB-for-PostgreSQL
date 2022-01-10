# Test async ddl lock replay:
# 1. Test in sync mode
# 2. Test in async mode
#
# Test cases:
# a. lock table
# b. vacuum full table
# c. vacuum full database
# d. alter table
# e. drop table
# f. a lot of blocked ddl, more than workers
# g. auto cancel test

use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use threads;
use Test::More tests=>21;
use feature qw(say);

$ENV{"whoami"} = "parent";
my $regress_db = "postgres";
my ($ret, $stdout, $stderr);

sub test_async_execute
{
	my $node	= shift;
	my $sql		= shift;
	my $timeout	= shift;

	my $pid = fork();

	if( $pid == 0) {
		$ENV{"whoami"} = "child";
		my $start_time = time();
		say "$sql";
		$node->psql($regress_db, $sql);
		my $stop_time = time();
		if ($timeout != 0) {
			say "check time $stop_time - $start_time <= $timeout + 1";
			if ($stop_time - $start_time > $timeout + 1){
				say "failed";
				exit -1;
			} else {
				say "success";
			}
		}
		else {
			print "no check time";
		}
		exit 0;
	}
}


my $node_master = get_new_node("master");
$node_master->polar_init(1, "polar_master_logindex");

my $node_replica = get_new_node("replica1");
$node_replica->polar_init(0, "polar_repli_logindex");
$node_replica->polar_set_recovery($node_master);

# polar_enable_ddl_sync_mode = on
$node_master->append_conf(
	"postgresql.conf", q[
polar_enable_ddl_sync_mode=on
max_prepared_transactions=100
logging_collector=off
]);
$node_replica->append_conf(
	"postgresql.conf", q[
polar_enable_ddl_sync_mode=on
max_prepared_transactions=100
polar_enable_async_ddl_lock_replay=on
polar_async_ddl_lock_replay_worker_num=3
max_standby_archive_delay=3s
max_standby_streaming_delay=3s
logging_collector=off
]);

$node_master->start;

$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my @replicas = ($node_replica);
my $regress = PolarRegression->create_new_test($node_master);
$regress->replay_check($node_replica);

is ($node_replica->psql($regress_db, "INSERT INTO replayed VALUES(1)"), 3,
	"read-only queries on replica");

my $g_start_time = time();

$node_master->psql($regress_db, "create table t1(id int)");

$node_master->psql($regress_db, "create extension polar_monitor");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "begin;lock table t1;end;"), 0, "lock1");

# auto cancel test
test_async_execute($node_replica, "select * from t1, pg_sleep(10)", 3);
sleep 1;
is ($node_master->psql($regress_db, "begin;lock table t1;end;"), 0, "autocancel1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "vacuum full t1"), 0, "vacuumtbl1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "vacuum full"), 0, "vacuumdb1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "alter table t1 add column data text;"), 0, "alter1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "drop table t1"), 0, "drop1");

# a lot of async ddl lock getting
for(my $i = 0; $i < 10; $i = $i + 1 ){
	$node_master->psql($regress_db, "create table t$i(id int)");
	test_async_execute($node_replica, "select * from t$i, pg_sleep(2)", 0);
	test_async_execute($node_master, "vacuum full t$i", 0);
}

($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_worker");
note "$stdout";
is ($ret, 0, "worker view");
($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_transaction");
note "$stdout";
is ($ret, 0, "transaction view");
($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_lock");
note "$stdout";
is ($ret, 0, "lock view");

# check replay
$node_master->psql($regress_db, "create table dummy1(id int)");
$node_master->wait_for_catchup($node_replica, 'replay',
	$node_master->lsn('write'));
is ($node_replica->psql($regress_db, "select * from dummy1"), 0, "test replay1");

$node_master->stop;
$node_replica->stop;

my $g_cost_time = time() - $g_start_time;
note "Test cost time1 $g_cost_time";


# polar_enable_ddl_sync_mode = off
$node_master->append_conf(
	"postgresql.conf", q[
polar_enable_ddl_sync_mode=off
]);
$node_master->start;
$node_replica->start;

my $start_time = time();

$node_master->psql($regress_db, "create table t1(id int)");

$node_master->psql($regress_db, "create extension polar_monitor");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "begin;lock table t1;end;"), 0, "lock1");

# auto cancel test
test_async_execute($node_replica, "select * from t1, pg_sleep(10)", 3);
sleep 1;
is ($node_master->psql($regress_db, "begin;lock table t1;end;"), 0, "autocancel1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "vacuum full t1"), 0, "vacuumtbl1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "vacuum full"), 0, "vacuumdb1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "alter table t1 add column data text;"), 0, "alter1");

test_async_execute($node_replica, "select * from t1, pg_sleep(2)", 3);
sleep 1;
is ($node_master->psql($regress_db, "drop table t1"), 0, "drop1");

# a lot of async ddl lock getting
for(my $i = 0; $i < 10; $i = $i + 1 ){
	$node_master->psql($regress_db, "create table t$i(id int)");
	test_async_execute($node_replica, "select * from t$i, pg_sleep(2)", 0);
	test_async_execute($node_master, "vacuum full t$i", 0);
}

($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_worker");
note "$stdout";
is ($ret, 0, "worker view");
($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_transaction");
note "$stdout";
is ($ret, 0, "transaction view");
($ret, $stdout, $stderr) = $node_replica->psql($regress_db, "select * from polar_stat_async_ddl_lock_replay_lock");
note "$stdout";
is ($ret, 0, "lock view");

# check replay
$node_master->psql($regress_db, "create table dummy2(id int)");
$node_master->wait_for_catchup($node_replica, 'replay',
	$node_master->lsn('write'));
is ($node_replica->psql($regress_db, "select * from dummy2"), 0, "test replay2");

$node_master->stop;
$node_replica->stop;

$g_cost_time = time() - $g_start_time;
note "Test cost time2 $g_cost_time";
