# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 22;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->append_conf(
	'postgresql.conf', q[
log_statement = all
synchronous_standby_names = 'standby_1'
polar_sync_replication_timeout = 30s
polar_sync_rep_timeout_break_lsn_lag = 100MB
polar_semi_sync_max_backoff_window = 1024s
polar_semi_sync_observation_window = 5min
]);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create streaming standby linking to master
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Create some content on master and check its presence in standby 1
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");

# Wait for standbys to catch up
$node_master->wait_for_catchup($node_standby_1, 'replay',
	$node_master->lsn('insert'));

my $result = $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1000), 'check streamed content on standby 1');

# Simulate a delay scene
$node_standby_1->stop;

# Check synchronous replication timeout
$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a)  SELECT generate_series(1001,2000) AS a");

my $cnt;
my $cnt2;
my $master_log;

open($master_log,  "<",  $node_master->logfile());
$cnt = 0;
$cnt2 = 0;
while (<$master_log>) 
{
	if (/WARNING:  canceling wait for synchronous replication due to timeout/)
	{
		print;
		$cnt = $cnt + 1;
	}
	if (/WARNING:  reset backoff window to 0./)
	{
		print;
		$cnt2 = $cnt2 + 1;
	}
}
print "found synchronous replication timeout warning: $cnt\n";
is($cnt, qq(1), 'check synchronous replication timeout');

print "found reset backoff window: $cnt2\n";
is($cnt, qq(1), 'reset backoff window to 0');

# Check sync replication is back to normal
$node_standby_1->start;

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a)  SELECT generate_series(2001,3000) AS a");

$node_master->wait_for_catchup($node_standby_1);

$result = $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(3000), 'check streamed content on standby 1');

open($master_log,  "<",  $node_master->logfile());
$cnt = 0;
while (<$master_log>) 
{
	if (/WARNING:  the timeout synchronous replication is back to wait for sync/)
	{
		print;
		$cnt = $cnt + 1;
	}
}
print "found sync replication is back to normal: $cnt\n";
is($cnt, qq(1), 'check sync replication is back to normal');

sub check_backoff_window_opened
{ 
	my ($window) = @_;
	$node_master->safe_psql('postgres',
		"INSERT INTO tab_int(a) values(3000+$window)");

	sleep(3);

	open($master_log,  "<",  $node_master->logfile());
	$cnt = 0;
	while (<$master_log>) 
	{
		if (/WARNING:  start a new $window s backoff window./)
		{
			print;
			$cnt = $cnt + 1;
		}
	}
	print "found a new $window s backoff window: $cnt\n";
	is($cnt, qq(1), "check a new $window s backoff window.")
}

sub check_within_backoff_window
{
	my ($target_cnt) = @_;
	$node_master->safe_psql('postgres',
		"INSERT INTO tab_int(a) values(5000+$target_cnt)");

	sleep(3);

	open($master_log,  "<",  $node_master->logfile());
	$cnt = 0;
	while (<$master_log>) 
	{
		if (/WARNING:  semi-sync optimization under network jitter, keep async since we are within a backoff window./)
		{
			print;
			$cnt = $cnt + 1;
		}
	}
	print "found $cnt times within a backoff window: $cnt\n";
	is($cnt, qq($target_cnt), 'check within backoff window.')
}

# Check backoff window from 8s to 256s
$node_standby_1->stop;

check_backoff_window_opened(8);
sleep(4);
check_within_backoff_window(1);
sleep(4);

check_within_backoff_window(1);
check_backoff_window_opened(16);
sleep(8);
check_within_backoff_window(2);
sleep(8);

check_within_backoff_window(2);
check_backoff_window_opened(32);
sleep(16);
check_within_backoff_window(3);
sleep(16);

check_within_backoff_window(3);
check_backoff_window_opened(64);
sleep(32);
check_within_backoff_window(4);
sleep(32);

check_within_backoff_window(4);
check_backoff_window_opened(128);
sleep(64);
check_within_backoff_window(5);
sleep(64);

check_within_backoff_window(5);
check_backoff_window_opened(256);
sleep(10);
check_within_backoff_window(6);