# Minimal test testing priority replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->append_conf(
	'postgresql.conf', q[
polar_high_priority_replication_standby_names='standby_1'
restart_after_crash = on
]);
$node_master->start;

$node_master->append_conf(
	'postgresql.conf', q[
polar_low_priority_replication_standby_names='standby_1'
]);
$node_master->reload;

sleep(5);

open(my $master_log,  "<",  $node_master->logfile());
my $cnt = 0;
while (<$master_log>) 
{
	if (/invalid value for parameter "polar_low_priority_replication_standby_names": "standby_1"/)
	{
		print;
		$cnt = $cnt + 1;
	}
	print
}
print "found data lose warning: $cnt\n";
is($cnt, qq(1), 'check data lose warning');

$node_master->stop;