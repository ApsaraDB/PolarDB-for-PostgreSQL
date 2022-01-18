# Test for create logindex meta file
# create logindex file when current node is promary node, which maybe an original primary node
# or pitr recovery from a backup, or promoted from a standby or replica
use strict;
use warnings;
use PostgresNode;
use TestLib ();
use Test::More tests=>12;

### 1. primary node from pitr, test whether insert is normal
my $wait_time = 30;
# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# replica
my $node_replica = get_new_node('replica');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);

# insert data in master node
$node_master->append_conf('postgresql.conf',
	        "synchronous_standby_names='".$node_replica->name."'");
$node_master->start;
$node_master->polar_wait_for_startup($wait_time, 0);
$node_master->polar_create_slot($node_replica->name);
$node_replica->start;

my $begin_val = 1;
my $end_val = 20;
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(val integer)');
$node_master->safe_psql('postgres',
	"INSERT INTO test_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$node_master->safe_psql('postgres', "checkpoint");	
my $result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result == 1, "master node insert data success");

# get basebackup 
my $backup_name = 'pitr_backup';
my $polar_backup = $node_master->backup_dir;
$node_master->backup($backup_name);
my $node_backup = get_new_node('backup');
$node_backup->init_from_backup($node_master, $backup_name, has_streaming => 1);
# backup polar data, delete pg_logindex dir
$polar_backup = "$polar_backup/polar_backup";
my $polar_datadir = $node_master->polar_get_datadir;
my @sysres = `rm -rf $polar_datadir/pg_logindex`;
@sysres = `mkdir $polar_backup`;
@sysres = `cp -r $polar_datadir/* $polar_backup`;

# insert data after get basebackup
$begin_val = $end_val + 1;
$end_val = $begin_val + 10;
$node_master->safe_psql('postgres',
	"INSERT INTO test_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
# create restore point
$node_master->safe_psql('postgres', "select pg_create_restore_point('restore_point1')");
# insert data after create restore point
$begin_val = $end_val + 1;
$end_val = $begin_val + 10;
$node_master->safe_psql('postgres',
	"INSERT INTO test_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$result = 0;
$result = $node_master->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result == 1, "master node insert data success after create restore point");
$node_master->safe_psql('postgres', "checkpoint");

# stop master and copy wal to polar backup dir
$node_master->stop;
$node_replica->stop;
@sysres = `cp -r $polar_datadir/pg_wal/* $polar_backup/pg_wal`;

# create recovery.conf
my $backup_dir = $node_backup->data_dir;
TestLib::system_or_bail('rm', '-r', "$backup_dir/recovery.conf");
TestLib::system_or_bail('touch', "$backup_dir/recovery.conf");
$node_backup->append_conf('recovery.conf', "restore_command = 'echo not copy'");
$node_backup->append_conf('recovery.conf', "recovery_target_name = 'restore_point1'");
$node_backup->append_conf('recovery.conf', "recovery_target_action = 'promote'");
$node_backup->append_conf('postgresql.conf', "polar_datadir = '$polar_backup'");

# start backup_node
$node_backup->start;
# wait for backup node enter pm_run state
$node_backup->polar_wait_for_startup($wait_time, 0);

$node_replica->polar_set_recovery($node_backup);
$node_replica->append_conf('postgresql.conf', "polar_datadir = '$polar_backup'");
my $replica_dir = $node_replica->data_dir;
@sysres = `rm -f $replica_dir/polar_node_static.conf`;
$node_replica->start;
my $walpid = $node_replica->wait_walstreaming_establish_timeout(40);
ok($walpid != 0, "walstream between node_backup and node_replica is normal");
# backup node can get data which inserted before restore point
$result = 0;
$begin_val = $begin_val -1;
print "begin_val: $begin_val, end_val:$end_val\n";
$result = $node_backup->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $begin_val]);
ok($result == 1, "backup node can get data inserted before restore point");
# backup node can't get data which inserted after restore point
$result = 0;
$result = $node_backup->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result eq "", "backup node can not get data inserted after restore point");

$begin_val = $end_val + 1;
$end_val = $begin_val + 100;
$node_backup->safe_psql('postgres',
	"INSERT INTO test_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$node_backup->safe_psql('postgres', "checkpoint");
$result = 0;
$result = $node_backup->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result == 1, "backup node insert data success after recovery");
$result = 0;
$result = $node_replica->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result == 1, "replica node apply data success");

### 2. promote replica and check whether insert is normal
$node_backup->stop;
$node_replica->promote;
# wait for replica node enter pm_run state
$node_replica->polar_wait_for_startup($wait_time, 0);
$node_replica->polar_drop_slot($node_replica->name);

$begin_val = $end_val + 1;
$end_val = $begin_val + 10;
$node_replica->safe_psql('postgres',
	"INSERT INTO test_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$result = 0;
$result = $node_replica->safe_psql('postgres',
		qq[SELECT 1 FROM test_table WHERE val = $end_val]);
ok($result == 1, "replica node insert data success after promote");
$node_replica->safe_psql('postgres', "checkpoint");
$node_replica->stop;
$node_replica->clean_node;

### 3. promote standby before establish walstreaming, check whether insert data is ok
my $node_master1 = get_new_node('master1');
$node_master1->polar_init(1, 'polar_master_logindex');

# standby
my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_set_root_node($node_master1);
$node_standby->polar_standby_build_data;
$node_standby->polar_standby_set_recovery($node_master1);

# standby1
my $node_standby1 = get_new_node('standby1');
$node_standby1->polar_init(0, 'polar_standby_logindex');
$node_standby1->polar_set_root_node($node_master1);
$node_standby1->polar_standby_build_data;
$node_standby1->polar_standby_set_recovery($node_master1);

$node_master1->start;
$node_master1->polar_create_slot($node_standby->name);
$node_master1->polar_create_slot($node_standby1->name);

# promote standby before establish walstream
$node_master1->stop;
$node_standby->start;
$node_standby->polar_wait_for_startup($wait_time);

$node_standby->promote;
# wait for standby to enter pm_run state
$node_standby->polar_wait_for_startup($wait_time, 0);

$node_standby->safe_psql('postgres', 'CREATE TABLE standby_table(val integer)');
$begin_val = $end_val + 1;
$end_val = $begin_val + 100;
$node_standby->safe_psql('postgres',
	"INSERT INTO standby_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$result = 0;
$result = $node_standby->safe_psql('postgres',
		qq[SELECT 1 FROM standby_table WHERE val = $end_val]);
$node_standby->safe_psql('postgres', "checkpoint");
ok($result == 1, "standby node insert data success after promote");
$node_standby->stop;

### 4. promote standby after walstreaming has been established, which will call polar_logindex_redo_set_valid_info function again
$node_master1->start;
$node_master1->polar_wait_for_startup($wait_time, 0);
$node_standby1->start;
$walpid = 0;
$walpid = $node_standby1->wait_walstreaming_establish_timeout(40);
ok($walpid != 0, "wal streaming between standby1 and master1 is normal");
$node_master1->safe_psql('postgres', 'CREATE TABLE rw_table(val integer)');
$begin_val = $end_val + 1;
$end_val = $begin_val + 10;
$node_master1->safe_psql('postgres',
	"INSERT INTO rw_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$result = 0;
$result = $node_master1->safe_psql('postgres',
		qq[SELECT 1 FROM rw_table WHERE val = $end_val]);
ok($result == 1, "master1 node insert data success");
# wait for standby1 node receive all data
$node_master1->wait_for_catchup($node_standby1, 'replay');

# promote standby1
$node_master1->stop;
$node_standby1->promote;
# wait for standby to enter pm_run state
$node_standby1->polar_wait_for_startup($wait_time, 0);

$begin_val = $end_val + 1;
$end_val = $begin_val + 100;
$node_standby1->safe_psql('postgres',
	"INSERT INTO rw_table(val) SELECT generate_series($begin_val,$end_val) as newwal");
$result = 0;
$result = $node_standby1->safe_psql('postgres',
		qq[SELECT 1 FROM rw_table WHERE val = $end_val]);
$node_standby1->safe_psql('postgres', "checkpoint");
ok($result == 1, "standby1 node insert data success after promote and wal streaming");
$node_standby1->stop;