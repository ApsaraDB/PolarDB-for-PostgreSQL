# Test for whether replica reaches consistency after replayed all wal of primary 
# when it doesn't copy files from shared storage according to the redogap

use strict;
use warnings;
use PostgresNode;
use TestLib ();
use Test::More tests=>6;

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# standby
my $node_standby = get_new_node('standby');
$node_standby->polar_init(0, 'polar_standby_logindex');
$node_standby->polar_set_root_node($node_master);
$node_standby->polar_standby_build_data;
$node_standby->polar_standby_set_recovery($node_master);

# insert data in master
$node_master->start;
$node_master->polar_create_slot($node_standby->name);
$node_standby->start;
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(val integer);');
my $newval = 0;
my $i = 0;
for($i = 0; $i < 30; $i = $i + 1)
{
	$newval = $node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
}
my $check_val = $newval;

# promote standby, there will be timeline switch
$node_master->stop;
$node_standby->promote;
$newval = $node_standby->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
$node_standby->stop;

# create replica and new standby node
my $node_replica = get_new_node('replica1');
$node_replica->polar_init(0, 'polar_repli_logindex');
$node_replica->polar_set_recovery($node_standby);

my $node_standby1 = get_new_node('standby1');
$node_standby1->polar_init(0, 'polar_standby_logindex');
$node_standby1->polar_set_root_node($node_standby);
$node_standby1->polar_standby_build_data;
$node_standby1->polar_standby_set_recovery($node_standby);
$node_standby1->append_conf('postgresql.conf',"wal_keep_segments= 10");

$node_standby->append_conf('postgresql.conf',"synchronous_standby_names='".$node_replica->name."'");
$node_standby->start;
$node_standby->polar_create_slot($node_replica->name);
$node_standby->polar_create_slot($node_standby1->name);

# start replica, and insert data in standby, suspend startup process of replica at the same time
$node_standby1->start;
$node_replica->start;
$node_replica->stop_child('startup');
# insert data in standby
for($i = 0; $i < 100; $i = $i + 1)
{
	$newval = $node_standby->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
}
my $insert_lsn = $node_standby->lsn('insert');
# wait for standby1 replayed all wal
$node_standby->wait_for_catchup($node_standby1, 'replay', $insert_lsn, 1);
my $res = $node_standby1->safe_psql('postgres', qq[SELECT 1 FROM test_table WHERE val = $newval]);
ok($res == 1, "standby1 has replayed all wal before promote");

# stop standby immediately
$node_standby->stop('i');

# stop replica
$node_replica->resume_child('startup');
$node_replica->stop;

# promote standby1 so that when replica start, there will be wal of different timeline to replay
$node_standby1->promote;

# insert data in new master
for($i = 0; $i < 100; $i = $i + 1)
{
	$newval = $node_standby1->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
}
$insert_lsn = $node_standby1->lsn('insert');
$node_standby1->polar_create_slot($node_replica->name);
$node_standby1->safe_psql('postgres', "ALTER SYSTEM SET synchronous_standby_names='".$node_replica->name."';");
$node_replica->polar_set_recovery($node_standby1);

my $polar_datadir = $node_standby1->polar_get_datadir;
my $replica_data = $node_replica->data_dir;
my $standby1_redo = $node_standby1->polar_get_redo_location($polar_datadir);
my $replica_redo = $node_replica->polar_get_redo_location($replica_data);
print "new master_redoptr: $standby1_redo, replica_redoptr:$replica_redo\n";
ok ($standby1_redo ge $replica_redo || length($standby1_redo) > length($replica_redo), "replica_redoptr is not greater than new master_redoptr");

# get the minrecovery point of replica
my $minrecovery = readpipe("pg_controldata -D $replica_data | grep 'Minimum recovery ending location' | cut -d ':' -f 2");
chomp($minrecovery);
my $minrecovery_tli = readpipe("pg_controldata -D $replica_data | grep 'timeline' | cut -d ':' -f 2");
chomp($minrecovery_tli);

# delete the polar_node_static.conf so that replica1 can access the true polar_datadir
my $command = "rm -f $replica_data/polar_node_static.conf";
system("$command");

# start replica, and startup process will replay the wal
$node_replica->start;

# check whether new minrecoverypoint is greater than insert_lsn, replica should reach consistency after replayed all wal
my $minrecovery_after = readpipe("pg_controldata -D $replica_data | grep 'Minimum recovery ending location' | cut -d ':' -f 2");
chomp($minrecovery_after);
my $new_minrecovery_tli = readpipe("pg_controldata -D $replica_data | grep 'timeline' | cut -d ':' -f 2");
chomp($new_minrecovery_tli);
print "minrecoverypoint: $minrecovery, new minrecoverypoint: $minrecovery_after, insert_lsn: $insert_lsn\n";
print "minrecovery_tli: $minrecovery_tli, new_minrecovery_tli: $new_minrecovery_tli\n";
ok($minrecovery_after gt $minrecovery || length($minrecovery_after) > length($insert_lsn), "replica replayed all wal");
ok($minrecovery_after ge $insert_lsn || length($minrecovery_after) > length($insert_lsn), "replica reach consistency after replayed all wal");
ok($new_minrecovery_tli gt $minrecovery_tli, "replica replayed wal of all timeline");
$res = $node_replica->safe_psql('postgres', qq[SELECT 1 FROM test_table WHERE val = $newval]);
ok($res == 1, "replica has replayed all wal");

# stop cluster
$node_standby1->stop;
$node_replica->stop;
