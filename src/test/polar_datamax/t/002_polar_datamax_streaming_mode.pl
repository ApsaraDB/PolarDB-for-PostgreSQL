# Test for datamax straming in max_protection/max_performance/max_availability mode
use strict;
use warnings;
use PostgresNode;
use Test::More tests=>8;

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');
my $primary_system_identifier = $node_master->polar_get_system_identifier;

# datamax
my $node_datamax = get_new_node('datamax');
$node_datamax->polar_init_datamax($primary_system_identifier,'polar_standby_logindex');

# standby
my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_set_root_node($node_master);
$node_standby->polar_standby_build_data;
$node_standby->polar_standby_set_recovery($node_datamax);

$node_master->start;
$node_master->polar_create_slot($node_datamax->name);
$node_master->stop;

$node_datamax->start;
$node_datamax->polar_create_slot($node_standby->name);
$node_datamax->safe_psql('postgres','CREATE EXTENSION polar_monitor;');
$node_datamax->stop;

# set datamax recovery.conf after create extension
$node_datamax->polar_datamax_set_recovery($node_master);

#### 1. datamax streaming in max_performance mode
# start cluster
$node_master->start;
$node_datamax->start;
$node_standby->start;

my $wait_timeout = 40;
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(val integer);');
$node_datamax->wait_walstreaming_establish_timeout($wait_timeout);
# stop walreceiver process of datamax when streaming in max_performance mode
$node_datamax->stop_child('walreceiver');
my $newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
my $result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $newval]);
print "stop walreceiver, insert_result: $result\n";
ok($result == 1, 'insert success when stop datamax and streaming in max_performance mode');

# resume walreceiver process of datamax when streaming in max_performance mode
$node_datamax->resume_child('walreceiver');
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $newval]);
print "resume walrecever, insert result: $result\n";
ok($result == 1, 'insert success when resume datamax');

#### 2. datamax streaming in max_protection mode
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_enable_transaction_sync_mode = on;');
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET synchronous_commit = on;');
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET synchronous_standby_names = '.$node_datamax->name.';');
$node_master->reload;

# stop walreceiver process of datamax when streaming in max_protection mode
$node_datamax->wait_walstreaming_establish_timeout($wait_timeout);
$node_datamax->stop_child('walreceiver');
my $timed_out = 0;
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val',
		timeout => 20, timed_out => \$timed_out);
print "stop walreceiver, timed_out: $timed_out\n";
ok($timed_out == 1, 'insert hang when stop datamax and streaming in max_protection mode');

# resume walreceiver process of datamax when streaming in max_protection mode
$node_datamax->resume_child('walreceiver');
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $newval]);
print "resume walrecever, insert result: $result\n";
ok($result == 1, 'insert success when resume datamax');

#### 3. datamax streaming in max_availability mode
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_sync_replication_timeout = 10000;');
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_sync_rep_timeout_break_lsn_lag = 1024;');
$node_master->reload;

# stop walreceiver process of datamax when streaming in max_availability mode
$node_datamax->wait_walstreaming_establish_timeout($wait_timeout);
$node_datamax->stop_child('walreceiver');
$timed_out = 0;
# test insert within polar_sync_replication_timeout
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val',
		timeout => 9, timed_out => \$timed_out);
print "stop walreceiver in max_availability mode, insert within timeout: $timed_out\n";
ok($timed_out == 1, 'insert within timeout hang when stop datamax and streaming in max_availability mode');
# test insert beyond polar_sync_replication_timeout
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val',
		timeout => 20, timed_out => \$timed_out);
$result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $newval]);
print "stop walreceiver in max_availability mode, insert beyond timed_out: $timed_out\n";
ok($timed_out == 0, 'insert beyond timeout success when stop datamax and streaming in max_availability mode');
ok($result == 1, 'insert beyond timeout success when stop datamax and streaming in max_availability mode');

# resume walreceiver process of datamax when streaming in max_availability mode
$node_datamax->resume_child('walreceiver');
$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $newval]);
print "resume walrecever, insert result: $result\n";
ok($result == 1, 'insert success when resume datamax');

$node_standby->stop;
$node_datamax->stop;
$node_master->stop;


