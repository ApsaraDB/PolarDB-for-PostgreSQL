#
# Tests relating to polardb directio when saving two phase transaction's state.
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

my $master = get_new_node('master');
$master->polar_init(1, 'polar_default');
$master->start;

is($master->safe_psql('postgres',
					  q[select 1 from pg_settings where name = 'polar_datadir' and setting like 'file-dio://%';]),
   1, "polar directio is on.");

$master->safe_psql('postgres',
				  q[DROP TABLE IF EXISTS prepared_transaction; CREATE TABLE prepared_transaction (v int); CHECKPOINT;]);
$master->safe_psql('postgres',
				  q[BEGIN; INSERT INTO prepared_transaction VALUES(1); PREPARE TRANSACTION 'twophase_test';]);
$master->restart();

$master->safe_psql('postgres',
					  q[COMMIT PREPARED 'twophase_test';]);

$master->safe_psql('postgres',
				  q[DROP TABLE IF EXISTS prepared_transaction; CREATE TABLE prepared_transaction (v int); CHECKPOINT;]);
$master->safe_psql('postgres',
				  q[BEGIN; INSERT INTO prepared_transaction VALUES(1); PREPARE TRANSACTION 'twophase_test'; CHECKPOINT;]);

$master->safe_psql('postgres',
					  q[COMMIT PREPARED 'twophase_test';]);
$master->stop();
