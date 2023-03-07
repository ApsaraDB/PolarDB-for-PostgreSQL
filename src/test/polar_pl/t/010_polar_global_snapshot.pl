# Test streaming replication
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>10;

# init cluster
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica1 = get_new_node('replica1');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);

my $node_replica2 = get_new_node('replica2');
$node_replica2->polar_init(0, 'polar_repli_logindex');
$node_replica2->polar_set_recovery($node_master);

$node_master->append_conf('postgresql.conf', "logging_collector=off");
$node_master->append_conf('postgresql.conf', "polar_enable_px=on");
$node_master->append_conf('postgresql.conf', "listen_addresses=localhost");
$node_master->start;

$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_replica2->name);

$node_replica1->append_conf('postgresql.conf', "logging_collector=off");
$node_replica2->append_conf('postgresql.conf', "logging_collector=off");
$node_replica1->append_conf('postgresql.conf', "listen_addresses=localhost");
$node_replica2->append_conf('postgresql.conf', "listen_addresses=localhost");

$node_replica1->start;
$node_replica2->start;

sub wait_for_catchups
{
	my ($node_master) = shift @_;
	my $node;
	foreach $node (@_) {
		$node_master->wait_for_catchup($node, 'replay', $node_master->lsn('insert'));
	}
}

sub getcount
{
  my $count = 0;
  $node_master->psql('postgres', 'SELECT count(*) from t', stdout => \$count);
  return $count;
}

my $count = 0;

# case 1
$node_master->psql('postgres', 'create table t(id int)');
$node_master->psql('postgres', 'alter table t set (px_workers =100)');
$node_master->psql('postgres', 'insert into t select generate_series(1,10000)');
wait_for_catchups($node_master, $node_replica1, $node_replica2);
is(getcount(), 10000, 'dummy px check');

$node_master->psql('postgres', "insert into t select 10001");
wait_for_catchups($node_master, $node_replica1, $node_replica2);
is(getcount(), 10001, 'px check after insert');

$node_master->psql('postgres', "delete from t where id = 10001");
wait_for_catchups($node_master, $node_replica1, $node_replica2);
is(getcount(), 10000, 'px check after delete');

$node_replica1->psql('postgres', "select pg_wal_replay_pause()");
$node_replica2->psql('postgres', "select pg_wal_replay_pause()");
$node_master->psql('postgres', "insert into t select 10002");
is(getcount(), 10000, 'px check after insert and pause replay');

$node_master->psql('postgres', "delete from t where id = 10002");
$node_master->psql('postgres', "vacuum t");
is(getcount(), 10000, 'px check after delete and pause replay');

$node_master->psql('postgres', "delete from t where id <= 5000");
is(getcount(), 10000, 'px check after delete and pause replay 2');

$node_replica1->psql('postgres', "select pg_wal_replay_resume()");
$node_replica2->psql('postgres', "select pg_wal_replay_resume()");
wait_for_catchups($node_master, $node_replica1, $node_replica2);
is(getcount(), 5000, 'px check after resume replay');

$node_replica1->psql('postgres', "select pg_wal_replay_pause()");
$node_master->psql('postgres', "delete from t");
$node_master->psql('postgres', "vacuum t");
wait_for_catchups($node_master, $node_replica2);
is(getcount(), 5000, 'px check after delete all and pause replay');
$node_replica1->psql('postgres', "select pg_wal_replay_resume()");
wait_for_catchups($node_master, $node_replica1, $node_replica2);
is(getcount(), 0, 'px check after delete all and resume replay');

$node_master->psql('postgres', "drop table t");

# case 2
$node_master->append_conf('postgresql.conf', "polar_enable_ddl_sync_mode=off");
$node_master->teardown_node;
$node_master->start;

$node_master->psql('postgres', 'create table t(id int)');
$node_master->psql('postgres', 'alter table t set (px_workers =100)');
$node_master->psql('postgres', 'insert into t select generate_series(1,10000)');
wait_for_catchups($node_master, $node_replica1, $node_replica2);

$node_replica1->psql('postgres', "select pg_wal_replay_pause()");

$node_master->psql('postgres', 'delete from t');
$node_master->psql('postgres', 'vacuum t');
wait_for_catchups($node_master, $node_replica2);
is(getcount(), 10000, 'px check after node dead');

$node_replica1->psql('postgres', "select pg_wal_replay_resume()");
$node_master->stop;
$node_replica1->stop;
$node_replica2->stop;
