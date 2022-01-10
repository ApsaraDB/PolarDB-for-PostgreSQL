# Test for moving the process of initing local directory for replica from postmaster to startup
use strict;
use warnings;
use PostgresNode;
use TestLib ();
use Test::More tests=>12;

# judge whether file is the same
sub polar_file_compare
{
	my ($dir1, $dir2) = @_;
	my $ret = 0;
	my $file = readpipe("ls $dir1");
	my @file_array1 = split('\n',$file);
	$file = readpipe("ls $dir2");
	my @file_array2 = split('\n', $file);
	foreach my $i (@file_array1)  
	{ 
		if(grep { $_ eq $i } @file_array2)
		{
			my $file1 = $dir1 . "/" . $i;
			my $md51 = readpipe("md5sum $file1");
			$md51 = (split(" ",$md51))[0];
			$file1 = $dir2 . "/" . $i;
			my $md52 = readpipe("md5sum $file1");
			$md52 = (split(" ",$md52))[0];
			if ($md51 eq $md52)
			{
				$ret = 1;
			}
			else
			{
				$ret = 0;
				last;
			}
		}
		else
		{
			$ret = 0;
			last;
		}
	}
	return $ret;
}

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# replica
my $node_replica = get_new_node('replica1');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_master);
$node_replica->append_conf('postgresql.conf',"log_min_messages = DEBUG1");

$node_master->append_conf('postgresql.conf',"synchronous_standby_names='".$node_replica->name."'");
$node_master->start;
$node_master->polar_create_slot($node_replica->name);

# create file in pg_twophase
my $polar_datadir = $node_master->polar_get_datadir;
my $file = "$polar_datadir/pg_twophase/test";
TestLib::system_or_bail('touch', "$file");
# fallocate the size of test file to 1M
TestLib::system_or_bail('fallocate', '-l', '1048576', "$file");

### 1 newly created replica by initdb, need to copy dirs when first boot, regardless of the redoptr gap
my $datadir = $node_replica->data_dir;
my $master_redoptr = $node_master->polar_get_redo_location($polar_datadir);
my $replica_redoptr = $node_replica->polar_get_redo_location($datadir);
print "master_redoptr: $master_redoptr, replica_redoptr:$replica_redoptr\n";

# start replica
$node_replica->start;
my $res = polar_file_compare("$polar_datadir/pg_twophase", "$datadir/pg_twophase");
ok($res == 1, "copy dirs from shared storage to local when master_redoptr is smaller than replica_redoptr");

# check whether walstreaming between master and replica is normal after start
my $walpid = $node_replica->wait_walstreaming_establish_timeout(20);
print "walreceiver pid of replica: $walpid\n";
ok($walpid != 0, "walstream between master and replica is normal");

### 2 increase gap between master redoptr and replica redoptr, need to copy dir when start replica
# add file
my $file1 = "$polar_datadir/pg_twophase/test1";
TestLib::system_or_bail('touch', "$file1");
# fallocate the size of test file to 4G
TestLib::system_or_bail('fallocate', '-l', '4294967296', "$file1");

# generate wal in master
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(val integer);');
my $count = 0;
my $total_count = 2;
do
{
    $node_master->safe_psql('postgres',
	    'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
	# pg_switch_wal() to generate wal 
	$node_master->safe_psql('postgres', "select pg_switch_wal();");
	$count = $count + 1;
}while($count < $total_count);
my $val_insert = $node_master->safe_psql('postgres',
	'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
# do checkpoint, update redoptr of master, checkpoint can't be created when startup/walreceiver of ro is stopped
$node_master->safe_psql('postgres', "checkpoint");
sleep 5;

# create new replica node, so that there is redoptr gap between replica and master
my $node_replica1 = get_new_node('replica2');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);
$node_master->polar_create_slot($node_replica1->name);

$master_redoptr = $node_master->polar_get_redo_location($polar_datadir);
$replica_redoptr = $node_replica1->polar_get_redo_location($node_replica1->data_dir);
print "new master_redoptr: $master_redoptr, replica_redoptr:$replica_redoptr\n";
ok($master_redoptr gt $replica_redoptr, "master_redoptr is greater than new replica1_redoptr");

# create polar_replica_booted file in replica1 datadir to test copy file when redoptr gap exceeds limit
# in order to generate redoptr gap, we need to create a new replica node after checkpoint of master is done
# otherwise if we suspend replica before we do checkpoint in master, the checkpoint process will keep waiting
# and if we keep replica running when we do checkpoint in master, there will be no redoptr gap
# and replica will execute copy dir process without judging redoptr gap when it is newly created by initdb, via whether polar_replica_booted file exists
# so we need to create it to test the gap judgement process
my $logfile = $node_replica1->logfile;
$datadir = $node_replica1->data_dir;
TestLib::system_or_bail('touch', "$datadir/polar_replica_booted");

# start replica1 in async mode, and startup process will copy file from shared_dir to local_dir due to redoptr gap
my $command = "pg_ctl -D $datadir -l $logfile start -w -c &";
print "Running: $command\n";
system("$command");

# psql when replica1 is starting up
sleep 10;
my($stdout, $stderr);
$node_replica1->psql('postgres', '', stdout => \$stdout, stderr => \$stderr,
                on_error_die => 0, on_error_stop => 1);
print "stdout:$stdout\n";
print "stderr:$stderr\n";
my $error = "the database system is starting up";
ok($stderr =~ $error, "startup state can be detected");

# wait for copy done
my $wait_start = 70;
$node_replica1->polar_wait_for_startup($wait_start);
$res = polar_file_compare("$polar_datadir/pg_twophase", "$datadir/pg_twophase");
ok($res == 1, "copy dirs from shared storage to local for replica1 success");

# update self->pid, need to do this because replica1 is started by command not $node->start
$node_replica1->_update_pid(1);
# check whether walstreaming between master and replica is normal
$walpid = $node_replica1->wait_walstreaming_establish_timeout(20);
print "walreceiver pid of replica: $walpid\n";
ok($walpid != 0, "walstream between master and replica1 is normal");

# stop replica1
$command = "pg_ctl -D $datadir -l $logfile stop";
print "Running: $command\n";
system("$command");
$node_replica1->_update_pid(0);

### 3 no need to copy dirs when polar_replica_redo_gap_limit > 0 and redoptr gap is smaller than polar_replica_redo_gap_limit
# check master_redoptr and replica1_redoptr
$master_redoptr = $node_master->polar_get_redo_location($polar_datadir);
$replica_redoptr = $node_replica1->polar_get_redo_location($node_replica1->data_dir);
print "new master_redoptr: $master_redoptr, replica_redoptr:$replica_redoptr\n";
ok($master_redoptr eq $replica_redoptr, "master_redoptr is eaqul to replica1_redoptr");

# create new file in pg_twophase, and remove file1 to avoid pg_ctl start timeout while copy file1
my $file2 = "$polar_datadir/pg_twophase/test2";
TestLib::system_or_bail('mv', "$file1", "$file2");
TestLib::system_or_bail('truncate', '-s', "1048576", "$file2");

# start replica1
$node_replica1->start;
# no need to copy dirs when polar_replica_redo_gap_limit > 0 and redoptr_gap < polar_replica_redo_gap_limit
$res = polar_file_compare("$polar_datadir/pg_twophase", "$datadir/pg_twophase");
ok($res == 0, "no need to copy dirs from shared storage to local when redoptr gap is smaller than polar_replica_redo_gap_limit");
$node_replica1->stop;

### 4 set polar_replica_redo_gap_limit=0, disable the comparison, need to copy dirs regardless of the redoptr gap
$node_replica1->append_conf('postgresql.conf',"polar_replica_redo_gap_limit = 0");
$node_replica1->start;
# need to copy dirs when polar_replica_redo_gap_limit = 0 
$res = polar_file_compare("$polar_datadir/pg_twophase", "$datadir/pg_twophase");
ok($res == 1, "need to copy dirs from shared storage to local when polar_replica_redo_gap_limit is 0");

### 5 test promote replica and demote master
# drop unnecessary replication slot
$node_replica->stop;
$node_replica1->stop;
$node_master->polar_drop_slot($node_replica->name);
$node_master->polar_drop_slot($node_replica1->name);
$node_master->stop;
$node_replica1->start;
$node_replica1->promote;
$node_replica1->polar_wait_for_startup($wait_start, 0);

# create new data to check data consistency after promote
$val_insert = $node_replica1->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$node_replica1->safe_psql('postgres', "select pg_switch_wal();");
$val_insert = $node_replica1->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$res = $node_replica1->safe_psql('postgres', qq[SELECT 1 FROM test_table WHERE val = $val_insert]);
ok($res == 1, "promoted replica1 insert data success");
$node_replica1->polar_create_slot($node_master->name);
$node_master->polar_set_recovery($node_replica1);
$node_master->start;
# check whether walstreaming between master and replica1 is normal after promote and demote
$walpid = $node_master->wait_walstreaming_establish_timeout(20);
print "walreceiver pid of master: $walpid\n";
ok($walpid != 0, "walstream between master and replica1 is normal");
# check whether master replay the wal successfully
$res = $node_master->safe_psql('postgres', qq[SELECT 1 FROM test_table WHERE val = $val_insert]);
ok($res == 1, "demoted master replay data success");

# stop cluster
$node_replica1->stop;
$node_master->stop;
