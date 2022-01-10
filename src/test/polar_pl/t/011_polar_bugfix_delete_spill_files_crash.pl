# Minimal test testting bugfix for delete spill files crash
# the fix C code has been commited by 34de8be, so this commit only has a test case.
# 34de8be: [Feature] to #1025889 update polardb_build.sh to divide test cases for precheck
#
# The main bugfix code is in reorderbuffer.c
# ReorderBufferIterTXNFinish:
# - FileClose(state->entries[off].fd);
# + CloseTransientFile(state->entries[off].fd);
# 
# ReorderBufferRestoreChanges:
# - FileClose(*fd);
# + CloseTransientFile(*fd);
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 'logical');
$node_master->append_conf(
	'postgresql.conf', q[
log_statement=all
]);
$node_master->start;

# Create logical replication client
my $node_client = get_new_node('client');
$node_client->init(allows_streaming => 'logical');
$node_client->append_conf(
	'postgresql.conf', q[
log_statement=all
]);
$node_client->start;

# Create some content on master and check its presence in standby and client
my $master_connstr = $node_master->connstr . ' dbname=postgres';
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int(a int)");
$node_client->safe_psql('postgres',
	"CREATE TABLE tab_int(a int)");

$node_master->safe_psql('postgres',
	"ALTER TABLE tab_int REPLICA IDENTITY FULL");
$node_master->safe_psql('postgres',
	"CREATE PUBLICATION puball for all tables");

my $client_appname = 'client';
my $slot_name = 'suball';
$node_client->safe_psql('postgres',
	"CREATE SUBSCRIPTION $slot_name CONNECTION '$master_connstr application_name=$client_appname' PUBLICATION puball");
$node_master->wait_for_catchup($client_appname);

my $timeout = 300;
my $master_psql = $node_master->psql_connect('postgres', $timeout);
$node_master->set_psql($master_psql);
$node_master->psql_execute('begin;', $timeout, $node_master->get_psql());

my $i;
for ($i = 0; $i < 50000; $i += 1)
{
	$node_master->psql_execute('insert into tab_int(a) values(1);', $timeout, $node_master->get_psql());
}
$node_master->psql_execute('commit;', $timeout, $node_master->get_psql());

$node_master->wait_for_catchup($client_appname);
my $result = $node_client->safe_psql('postgres', 'select count(*) from tab_int;');

print "result: $result\n";
is($result, qq(50000), '50000 on client ok');

$node_client->stop();
$node_master->stop();