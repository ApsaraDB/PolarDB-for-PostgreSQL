
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Verify that work items work correctly

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

my $node = PostgreSQL::Test::Cluster->new('polar_shm_limit');
$node->init;
$node->append_conf('postgresql.conf', 'polar_shm_limit=\'512MB\'');
$node->append_conf('postgresql.conf', 'wal_buffers=\'16MB\'');
$node->start;

my $count = $node->safe_psql('postgres',
	"select setting from pg_catalog.pg_settings where name='polar_shm_limit'"
);
is($count, '65536', "polar_shm_limit is correct");

my $buffercount = $node->safe_psql('postgres',
	"select setting from pg_catalog.pg_settings where name='shared_buffers'");
printf "shared_buffers is %d\n", $buffercount;
$node->stop;

done_testing();
