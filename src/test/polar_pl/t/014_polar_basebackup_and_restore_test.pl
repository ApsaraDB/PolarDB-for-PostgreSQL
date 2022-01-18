# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

sub wait_for_catchup_lsn
{
	my ($self, $target_lsn, $timeout) = @_;
	$timeout = defined($timeout) ? $timeout : 180; #default value, the same as poll_query_until

	my $lsn_expr;
	if (defined($target_lsn))
	{
		$lsn_expr = "'$target_lsn'";
	}
	else
	{
		$lsn_expr = 'pg_last_wal_replay_lsn()';
	}
	print "Waiting for node "
	  . $self->name . "'s "
	  . "replay_lsn to pass "
	  . $lsn_expr . "\n";
	my $query =
	  qq[SELECT $lsn_expr <= pg_last_wal_replay_lsn();];
	if (!$self->poll_query_until('postgres', $query, 't', $timeout))
	{
		print "timed out waiting for catchup\n";
		return 0;
	}
	return 1;
}

$ENV{'PG_TEST_NOCLEAN'} = 1;
# Initialize master node
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

# make polar_datadir inside data_directory
my $master_datadir = $node_master->polar_get_datadir;
my $master_basedir = $node_master->data_dir;
my $new_master_datadir = "$master_basedir/polar_shared_data";
TestLib::system_or_bail('mv', $master_datadir, $new_master_datadir);
$node_master->polar_set_datadir($new_master_datadir);
$node_master->append_conf('postgresql.conf', 'polar_datadir=\'file-dio://./polar_shared_data/\'');
TestLib::system_or_bail('rm', '-f',"$master_basedir/polar_node_static.conf");
$node_master->start;

## Take backup
my $backup_name="polar_backup";
my $polardata = $node_master->backup_dir . '/' . $backup_name . '/' . '/polar_shared_data';
$node_master->polar_backup($backup_name, $polardata);
# create standby
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming=>1);
$node_standby->polar_standby_set_recovery($node_master);
$node_master->polar_create_slot($node_standby->name);
$node_standby->start;
# Create some content on master and check its presence in standby
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");
# Wait for standbys to catch up
$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('insert'));
my $result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby: $result\n";
is($result, qq(1000), 'check streamed content on standby');

$node_standby->stop;
$node_master->polar_drop_slot($node_standby->name);

## self-defined pg_basebackup tool usage test
$backup_name = "self_defined_backup";
my $backup_path = $node_master->backup_dir.'/'.$backup_name;
print "# Taking polar_basebackup $backup_name from node \"$node_master->name\"\n";
TestLib::system_or_bail('polar_basebackup', '--pgdata='.$backup_path, '--host='.$node_master->host, '--port='.$node_master->port,
						'--no-sync', '--write-recovery-conf', '--format=tar', '--gzip', '-X', 'fetch');
TestLib::system_or_bail('tar', '-zxvf', $backup_path.'/base.tar.gz', '-C', $backup_path);
TestLib::system_or_bail('rm', '-f', $backup_path.'/base.tar.gz');
# create standby
$node_standby = get_new_node('standby1');
$node_standby->init_from_backup($node_master, $backup_name, has_streaming=>1);
$node_standby->polar_standby_set_recovery($node_master);
$node_master->polar_create_slot($node_standby->name);
$node_standby->start;
# Create some content on master and check its presence in standby1
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int_1 AS SELECT generate_series(1,1000) AS a");
# Wait for standbys to catch up
$node_master->wait_for_catchup($node_standby, 'replay', $node_master->lsn('insert'));
$result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int_1");
print "standby1: $result\n";
is($result, qq(1000), 'check streamed content on standby1');

$node_standby->stop;
$node_master->stop;

## self-defined PITR usage test
my $pitr_name = "self_defined_pitr";
my $archive_path = $node_master->archive_dir.'/'.$pitr_name;
mkdir $node_master->archive_dir;
mkdir $archive_path;
$backup_path = $node_master->backup_dir.'/'.$pitr_name;
# prepare for pitr
$node_master->append_conf('postgresql.conf', 'archive_mode=on');
$node_master->append_conf('postgresql.conf', 'archive_timeout=300');
$node_master->append_conf('postgresql.conf', 'archive_command = \'cp -i %p '.$archive_path.'/%f\'');
$node_master->start;
print "# Taking polar_basebackup $pitr_name from node \"$node_master->name\"\n";
TestLib::system_or_bail('polar_basebackup', '--pgdata='.$backup_path, '--host='.$node_master->host, '--port='.$node_master->port,
						'--no-sync', '--write-recovery-conf', '--format=tar', '--gzip', '-X', 'fetch');
TestLib::system_or_bail('tar', '-zxvf', $backup_path.'/base.tar.gz', '-C', $backup_path);
TestLib::system_or_bail('rm', '-f', $backup_path.'/base.tar.gz');
# Create some content and more WALs on master
$node_master->safe_psql('postgres', "select pg_switch_wal();");
$node_master->safe_psql('postgres', "CREATE TABLE tab_int_2 AS SELECT generate_series(1,1000) AS a");
$node_master->safe_psql('postgres', "select pg_switch_wal();");
$node_master->safe_psql('postgres', "CREATE TABLE tab_int_3 AS SELECT generate_series(1,1000) AS a");
$node_master->safe_psql('postgres', "select pg_switch_wal();");
$node_master->safe_psql('postgres', "checkpoint;");
my $insert_lsn = $node_master->lsn('insert');
$node_master->stop;
# create standby
$node_standby = get_new_node('standby2');
$node_standby->init_from_backup($node_master, $pitr_name, enable_restoring=>1);
$node_standby->polar_standby_set_recovery($node_master);
$node_standby->append_conf('recovery.conf', 'restore_command = \'cp '.$archive_path.'/%f %p\'');
$node_standby->start;
# test result
wait_for_catchup_lsn($node_standby, $insert_lsn, 600);
$result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int_3;");
print "standby2: $result\n";
is($result, qq(1000), 'check pitr content on standby2');
$node_standby->stop;
