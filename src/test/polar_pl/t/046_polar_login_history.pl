#!/usr/bin/perl

# 046_polar_login_history.pl
#
# IDENTIFICATION
#	  src/test/polar_pl/t/046_polar_login_history.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->start;

$node->append_conf('postgresql.conf',
    "polar_internal_shared_preload_libraries = 'polar_login_history'");
$node->restart;

$node->append_conf('postgresql.conf',
	"polar_login_history.enable = on");
$node->restart;

is( $node->safe_psql('postgres', 'show polar_login_history.enable;'),
    'on', 'login history function is enabled');

$node->safe_psql('postgres', 'create user zhangsan;');
is( $node->psql('postgres', undef, extra_params => [ '-U', 'zhangsan' ]),
    0, 'login success');
$node->safe_psql('postgres', 'drop user zhangsan;');

$node->stop;

done_testing();
