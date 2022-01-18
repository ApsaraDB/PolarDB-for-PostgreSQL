#
# Tests relating to polardb postgres/polar-postgres's ability for '-C' which fetchs run-time parameter under affect of polar_node_static.conf 
#
use strict;
use warnings;
use PostgresNode;
use PolarRegression;
use TestLib;
use Test::More;
use Config;
if ($Config{osname} eq 'MSWin32')
{

	# some Windows Perls at least don't like IPC::Run's start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
else
{
	plan tests => 1;
}

my $node = get_new_node('master');
$node->polar_init(1, 'polar_default');
$node->start;
$node->stop;
# postgres/polar-postgres
my $postgres;
if (TestLib::system_log([ 'polar-postgres', '--version']) == 0)
{
	$postgres = 'polar-postgres';
}
else
{
	$postgres = 'postgres';
}
my $datadir = $node->data_dir;

# first run '-C polar_datadir' as con_A
my $first_fetch = TestLib::run_log([$postgres, '-C', 'polar_datadir', '-D', $datadir]);
$node->start;

# modify polar_datadir inside postgresql.conf
$node->append_conf('postgresql.conf', "polar_datadir=no_exist_path");

# second run '-C polar_datadir' as con_B
my $second_fetch = TestLib::run_log([$postgres, '-C', 'polar_datadir', '-D', $datadir]);

# compare con_A and con_B, should be same
ok($first_fetch eq $second_fetch, 'After postgresql.conf was modified, the result of \'-C polar_datadir\' remain same.');

$node->stop();
