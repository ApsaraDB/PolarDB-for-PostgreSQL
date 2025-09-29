
# Copyright (c) 2024-2025, PostgreSQL Global Development Group

# Tests for Custom Cumulative Statistics.

use strict;
use warnings FATAL => 'all';
use locale;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test persistency of statistics generated for injection points.
if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Node initialization
my $node = PostgreSQL::Test::Cluster->new('master');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
shared_preload_libraries = 'injection_points'
injection_points.stats = true
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# This should count for two calls.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('stats-notice', 'notice');");
$node->safe_psql('postgres', "SELECT injection_points_run('stats-notice');");
$node->safe_psql('postgres', "SELECT injection_points_run('stats-notice');");
my $numcalls = $node->safe_psql('postgres',
	"SELECT injection_points_stats_numcalls('stats-notice');");
is($numcalls, '2', 'number of stats calls');
my $entrycount =
  $node->safe_psql('postgres', "SELECT injection_points_stats_count();");
is($entrycount, '1', 'number of entries');
my $fixedstats = $node->safe_psql('postgres',
	"SELECT * FROM injection_points_stats_fixed();");
is($fixedstats, '1|0|2|0|0', 'fixed stats after some calls');

# Loading and caching.
$node->safe_psql(
	'postgres', "
SELECT injection_points_load('stats-notice');
SELECT injection_points_cached('stats-notice');
");
$fixedstats = $node->safe_psql('postgres',
	"SELECT * FROM injection_points_stats_fixed();");
is($fixedstats, '1|0|2|1|1', 'fixed stats after loading and caching');

# Restart the node cleanly, stats should still be around.
$node->restart;
$numcalls = $node->safe_psql('postgres',
	"SELECT injection_points_stats_numcalls('stats-notice');");
is($numcalls, '3', 'number of stats after clean restart');
$entrycount =
  $node->safe_psql('postgres', "SELECT injection_points_stats_count();");
is($entrycount, '1', 'number of entries after clean restart');
$fixedstats = $node->safe_psql('postgres',
	"SELECT * FROM injection_points_stats_fixed();");
is($fixedstats, '1|0|2|1|1', 'fixed stats after clean restart');

# On crash the stats are gone.
$node->stop('immediate');
$node->start;
$numcalls = $node->safe_psql('postgres',
	"SELECT injection_points_stats_numcalls('stats-notice');");
is($numcalls, '', 'number of stats after crash');
$entrycount =
  $node->safe_psql('postgres', "SELECT injection_points_stats_count();");
is($entrycount, '0', 'number of entries after crash');
$fixedstats = $node->safe_psql('postgres',
	"SELECT * FROM injection_points_stats_fixed();");
is($fixedstats, '0|0|0|0|0', 'fixed stats after crash');

# On drop all stats are gone
$node->safe_psql('postgres',
	"SELECT injection_points_attach('stats-notice', 'notice');");
$node->safe_psql('postgres', "SELECT injection_points_run('stats-notice');");
$node->safe_psql('postgres', "SELECT injection_points_run('stats-notice');");
$numcalls = $node->safe_psql('postgres',
	"SELECT injection_points_stats_numcalls('stats-notice');");
is($numcalls, '2', 'number of stats calls');
$node->safe_psql('postgres', "SELECT injection_points_stats_drop();");
$numcalls = $node->safe_psql('postgres',
	"SELECT injection_points_stats_numcalls('stats-notice');");
is($numcalls, '', 'no stats after drop via SQL function');
$entrycount =
  $node->safe_psql('postgres', "SELECT injection_points_stats_count();");
is($entrycount, '0', 'number of entries after drop via SQL function');

# Stop the server, disable the module, then restart.  The server
# should be able to come up.
$node->stop;
$node->adjust_conf('postgresql.conf', 'shared_preload_libraries', "''");
$node->start;

done_testing();
