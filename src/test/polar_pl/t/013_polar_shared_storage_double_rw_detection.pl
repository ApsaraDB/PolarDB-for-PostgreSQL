use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use Test::More tests=>20;

sub unsafe_psql
{
	my ($self, $dbname, $sql, %params) = @_;

	my ($stdout, $stderr);
	my $ret = $self->psql(
		$dbname, $sql,
		%params,
		stdout        => \$stdout,
		stderr        => \$stderr);
	# psql can emit stderr from NOTICEs etc
	if ($stderr ne "")
	{
		print "#### Begin standard error\n";
		print $stderr;
		print "\n#### End standard error\n";
	}
	return $stdout;
}

sub wait_for_node_shutdown
{
	my ($self) = @_;
	my $timeout = 10 * 60;

	while ($timeout > 0)
	{
		my $pid = $self->find_child('postgres');
		if ($pid == 0)
		{
			last;
		}
		sleep 1;
		$timeout = $timeout - 1;
	}
}

my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

my $node_replica1 = get_new_node('replica1');
$node_replica1->polar_init(0, 'polar_repli_logindex');
$node_replica1->polar_set_recovery($node_master);

my $node_standby1 = get_new_node('standby1');
$node_standby1->polar_init(0, 'polar_standby_logindex');
$node_standby1->polar_standby_set_recovery($node_master);

$node_master->append_conf('postgresql.conf', 
	"synchronous_standby_names='".$node_replica1->name.",".$node_standby1->name."'");

$node_master->start;
$node_master->polar_create_slot($node_replica1->name);
$node_master->polar_create_slot($node_standby1->name);

$node_replica1->start;

$node_standby1->polar_standby_build_data();
$node_standby1->start;

my ($death, $rwid) = ("DEATH", "RWID");
my $master_polar_datadir = $node_master->polar_get_datadir;
my $result;

$node_replica1->stop;
$node_standby1->stop;
# Firstly, we turn off polar_enable_io_fencing, and then create some chaos to prove this function is turned off.
$node_master->safe_psql('postgres', "alter system set polar_enable_io_fencing to off; select pg_reload_conf();");
$node_master->restart;
# create DEATH to see what could happend
system("touch $master_polar_datadir/$death");
$node_master->safe_psql('postgres', "checkpoint");
$result = $node_master->safe_psql('postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");
# change RWID to see what could happend
system("echo > $master_polar_datadir/$rwid");
$node_master->safe_psql('postgres', "checkpoint");
$result = $node_master->safe_psql('postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");
$node_master->restart;
system("rm $master_polar_datadir/$death");

# Secondly, we turn on polar_enable_io_fencing, and then create some chaos to prove this function is working.
$node_master->safe_psql('postgres', "alter system set polar_enable_io_fencing to on; select pg_reload_conf();");
$node_master->restart;
# create DEATH dir to see what will happend
mkdir "$master_polar_datadir/$death";
$node_master->safe_psql('postgres', "checkpoint");
$result = $node_master->safe_psql('postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");
rmdir "$master_polar_datadir/$death";

# create DEATH to make RW FATAL quit
system("touch $master_polar_datadir/$death");
unsafe_psql($node_master, 'postgres', "checkpoint");
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");

wait_for_node_shutdown($node_master);
$node_master->{_pid} = undef;
$node_master->start(fail_ok => 1);
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");

system("rm $master_polar_datadir/$death");
$node_master->start;
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");

# change RWID to make RW FATAL quit
system("echo > $master_polar_datadir/$rwid");
unsafe_psql($node_master, 'postgres', "checkpoint");
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");

wait_for_node_shutdown($node_master);
$node_master->{_pid} = undef;
$node_master->start(fail_ok => 1);
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");

system("rm $master_polar_datadir/$death");
$node_master->restart;
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");

$node_replica1->start;
$node_standby1->start;
# create another new RW node to use the shared storage of old RW.
my $new_node_master = get_new_node('master2');
$new_node_master->polar_set_datadir($master_polar_datadir."_master2");
$new_node_master->polar_init(1, 'polar_master_logindex');
$new_node_master->polar_set_datadir($master_polar_datadir);
$new_node_master->append_conf('postgresql.conf', "polar_datadir=\'file-dio://$master_polar_datadir\'");
$new_node_master->start(fail_ok => 1);
unsafe_psql($node_master, 'postgres', "checkpoint");
unsafe_psql($new_node_master, 'postgres', "checkpoint");
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "Old RW is not running");
system("rm ".$node_master->data_dir."/core.*");

$result = unsafe_psql($new_node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "New RW is not running");
system("rm ".$new_node_master->data_dir."/core.*");

wait_for_node_shutdown($node_master);
system("rm $master_polar_datadir/$death");
$node_master->{_pid} = undef;
$new_node_master->{_pid} = undef;
$node_master->start;
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");

# standby use the shared storage of RW
my $node_standby1_polar_datadir = $node_standby1->polar_get_datadir;
my $node_standby1_polar_node_static = $node_standby1->data_dir.'/polar_node_static.conf';
$node_standby1->stop;
$node_standby1->append_conf('postgresql.conf', "polar_datadir=\'file-dio://$master_polar_datadir\'");
system("rm $node_standby1_polar_node_static");
$node_standby1->start(fail_ok => 1);
unsafe_psql($node_master, 'postgres', "checkpoint");
unsafe_psql($node_standby1, 'postgres', "checkpoint");
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");

$result = unsafe_psql($node_standby1, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "Standby is not running");
system("rm ".$node_standby1->data_dir."/core.*");

wait_for_node_shutdown($node_master);
system("rm $master_polar_datadir/$death");
$node_master->{_pid} = undef;
$node_standby1->{_pid} = undef;
$node_master->start;
unsafe_psql($node_master, 'postgres', "checkpoint");
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "RW is running");

$node_standby1->append_conf('postgresql.conf', "polar_datadir=\'file-dio://$node_standby1_polar_datadir\'");
system("rm $node_standby1_polar_node_static");
$node_standby1->start;
unsafe_psql($node_standby1, 'postgres', "checkpoint");
$result = unsafe_psql($node_standby1, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "Standby is running");

# promote RO while old RW is still running
$result = unsafe_psql($node_replica1, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "Replica is running");

$node_replica1->promote;

$result = unsafe_psql($node_replica1, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "New RW is not running");
system("rm ".$node_replica1->data_dir."/core.*");
sleep 5;
$result = unsafe_psql($node_master, 'postgres', 'select 1');
chomp($result);
ok((!$result or $result != 1), "RW is not running");
system("rm ".$node_master->data_dir."/core.*");
wait_for_node_shutdown($node_master);
wait_for_node_shutdown($node_replica1);
system("rm $master_polar_datadir/$death");
$node_master->{_pid} = undef;
$node_replica1->{_pid} = undef;

$node_replica1->start;
$result = unsafe_psql($node_replica1, 'postgres', 'select 1');
chomp($result);
ok($result == 1, "New RW is running");

$node_replica1->stop('immediate');
$node_standby1->stop;

$node_replica1->append_conf('postgresql.conf', 'polar_enable_lazy_end_of_recovery_checkpoint=off');
$node_replica1->start;
$node_replica1->stop;
