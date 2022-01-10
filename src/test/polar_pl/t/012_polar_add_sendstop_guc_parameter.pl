# Test for add polar_enable_send_stop guc parameter to enable send SIGSTOP to peers when backend exits abnormally
use strict;
use warnings;
use PostgresNode;
use TestLib ();
use Test::More tests=>6;

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');

##### case1: start master without -T specified
$node_master->start;

# get value of sendstop
my $result = $node_master->polar_get_variable_value("SendStop");
ok($result == 0, "SendStop is 0 when -T is not specified and polar_enable_send_stop is off");

# set polar_enable_send_stop = on
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_enable_send_stop = on;');
$node_master->safe_psql('postgres', 'select pg_reload_conf();');
$result = -1;
$result = $node_master->polar_get_variable_value("SendStop");
ok($result == 1, "SendStop is 1 when -T is not specified and polar_enable_send_stop is on");

# set polar_enable_send_stop = off
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_enable_send_stop = off;');
$node_master->safe_psql('postgres', 'select pg_reload_conf();');
$result = -1;
$result = $node_master->polar_get_variable_value("SendStop");
ok($result == 0, "SendStop is 0 when -T is not specified and polar_enable_send_stop is off");

$node_master->stop;

##### case2: start master with -T specified
my $pgdata = $node_master->data_dir;
my $command = "postgres -D $pgdata -T &";
system($command);
sleep 10;
# get value of sendstop
$node_master->_update_pid(1);
$result = $node_master->polar_get_variable_value("SendStop");
ok($result == 1, "SendStop is 1 when -T is specified and polar_enable_send_stop is off");

# set polar_enable_send_stop = on
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_enable_send_stop = on;');
$node_master->safe_psql('postgres', 'select pg_reload_conf();');
$result = -1;
$result = $node_master->polar_get_variable_value("SendStop");
ok($result == 1, "SendStop is 1 when -T is specified and polar_enable_send_stop is on");

# set polar_enable_send_stop = off
$node_master->safe_psql('postgres', 'ALTER SYSTEM SET polar_enable_send_stop = off;');
$node_master->safe_psql('postgres', 'select pg_reload_conf();');
$result = -1;
$result = $node_master->polar_get_variable_value("SendStop");
ok($result == 1, "SendStop is 1 when -T is specified and polar_enable_send_stop is off");

$node_master->stop;
