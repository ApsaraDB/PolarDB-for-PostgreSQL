# Minimal test testing streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->append_conf(
	'postgresql.conf', q[
synchronous_standby_names = 'standby_1'
polar_sync_replication_timeout = 30s
polar_sync_rep_timeout_break_lsn_lag = 100MB
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
my $master_log;

open($master_log,  "<",  $node_master->logfile());
$cnt = 0;
while (<$master_log>) 
{
	if (/WARNING:  canceling wait for synchronous replication due to timeout/)
	{
		print;
		$cnt = $cnt + 1;
	}
}
print "found synchronous replication timeout warning: $cnt\n";
is($cnt, qq(1), 'check synchronous replication timeout');

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