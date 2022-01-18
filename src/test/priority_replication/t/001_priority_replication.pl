# Minimal test testing priority replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 18;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->append_conf(
	'postgresql.conf', q[
polar_priority_replication_mode = any
polar_priority_replication_force_wait = on
polar_high_priority_replication_standby_names='standby_1'
polar_low_priority_replication_standby_names='standby_2'
]);
$node_master->start;
my $backup_name = 'my_backup';

# Take backup
$node_master->backup($backup_name);

# Create streaming standby_1 linking to master
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_1->start;


# Create streaming standby_2 linking to master
my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_2->start;

# Create some content on master and check its presence in standbys
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int(a int)");
$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(1, 1000)");

# Wait for standbys to catch up
$node_master->wait_for_catchup($node_standby_1);
$node_master->wait_for_catchup($node_standby_2);

my $result;
$result = $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_1: $result\n";
is($result, qq(1000), 'check streamed content on standby_1');

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(1000), 'check streamed content on standby_2');

# Check priority replication: polar_priority_replication_force_wait
$node_standby_1->stop();

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(1001, 2000)");

sleep(10);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(1000), 'check streamed content on standby_2');
# double check
$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(1000), 'check streamed content on standby_2');

$node_master->append_conf(
	'postgresql.conf', q[
polar_priority_replication_force_wait = off
]);
$node_master->reload();

$node_master->wait_for_catchup($node_standby_2);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(2000), 'check streamed content on standby_2');

# Check priority replication: polar_priority_replication_mode
$node_master->append_conf(
	'postgresql.conf', q[
polar_priority_replication_force_wait = on
]);
$node_master->reload();

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(2001, 3000)");

sleep(10);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(2000), 'check streamed content on standby_2');
# double check
$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(2000), 'check streamed content on standby_2');

$node_master->append_conf(
	'postgresql.conf', q[
polar_priority_replication_mode = off
]);
$node_master->reload();

$node_master->wait_for_catchup($node_standby_2);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(3000), 'check streamed content on standby_2');

# Check priority replication: polar_high_priority_replication_standby_names
$node_master->append_conf(
	'postgresql.conf', q[
polar_priority_replication_mode = all
]);
$node_master->reload();

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(3001, 4000)");

sleep(10);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(3000), 'check streamed content on standby_2');
# double check
$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(3000), 'check streamed content on standby_2');

$node_master->append_conf(
	'postgresql.conf', q[
polar_high_priority_replication_standby_names = ''
]);
$node_master->reload();

$node_master->wait_for_catchup($node_standby_2);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(4000), 'check streamed content on standby_2');

$node_master->append_conf(
	'postgresql.conf', q[
polar_high_priority_replication_standby_names = 'standby_notexists'
]);
$node_master->reload();

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(4001, 5000)");

sleep(10);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(4000), 'check streamed content on standby_2');
# double check
$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(4000), 'check streamed content on standby_2');

my $node_standby_3 = get_new_node('standby_3');
$node_standby_3->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_3->start;

$node_master->append_conf(
	'postgresql.conf', q[
polar_high_priority_replication_standby_names = 'standby_3,standby_1'
]);
$node_master->reload();

$node_master->wait_for_catchup($node_standby_3);
$node_master->wait_for_catchup($node_standby_2);

$result = $node_standby_3->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_3: $result\n";
is($result, qq(5000), 'check streamed content on standby_3');

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(5000), 'check streamed content on standby_2');

# Check priority replication: polar_low_priority_replication_standby_names

$node_standby_3->stop();

$node_master->safe_psql('postgres',
	"INSERT INTO tab_int(a) SELECT generate_series(5001, 6000)");

sleep(10);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(5000), 'check streamed content on standby_2');
# double check
$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(5000), 'check streamed content on standby_2');

$node_master->append_conf(
	'postgresql.conf', q[
polar_low_priority_replication_standby_names = ''
]);
$node_master->reload();

$node_master->wait_for_catchup($node_standby_2);

$result = $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby_2: $result\n";
is($result, qq(6000), 'check streamed content on standby_2');

$node_standby_2->stop();
$node_master->stop();