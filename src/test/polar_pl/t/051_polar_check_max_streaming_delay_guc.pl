#!/usr/bin/perl

# 051_polar_check_max_streaming_delay_guc.pl
#
# Copyright (c) 2025, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# IDENTIFICATION
#	  src/test/polar_pl/t/051_polar_check_max_streaming_delay_guc.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# init primary
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;
$node_primary->start;

my $max_standby_archive_delay_value;
my $max_standby_streaming_delay_value;
my $max_replica_archive_delay_value;
my $max_replica_streaming_delay_value;

# polar_max_replica_streaming_delay is not configured, use value of max_replica_streaming_delay
$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
ok($max_replica_archive_delay_value eq "30s",
	"polar_max_replica_archive_delay default value is 30s");

$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
ok($max_replica_streaming_delay_value eq "30s",
	"polar_max_replica_streaming_delay default value is 30s");

$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_archive_delay = '40s';
));
$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_streaming_delay = '50s';
));
$node_primary->safe_psql('postgres', qq(select pg_reload_conf();));

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
ok( $max_standby_archive_delay_value eq "40s",
	"max_standby_archive_delay is 40s");
ok( $max_standby_archive_delay_value eq $max_replica_archive_delay_value,
	"use value of max_standby_archive_delay when polar_max_replica_archive_delay is not configured"
);

$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok( $max_standby_streaming_delay_value eq "50s",
	"max_standby_streaming_delay is 50s");
ok( $max_standby_streaming_delay_value eq $max_replica_streaming_delay_value,
	"use value of max_standby_streaming_delay when polar_max_replica_streaming_delay is not configured"
);

# restart node to check values
$node_primary->stop('fast');
$node_primary->start;

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
ok( $max_standby_archive_delay_value eq "40s",
	"max_standby_archive_delay is 40s");
ok( $max_standby_archive_delay_value eq $max_replica_archive_delay_value,
	"use value of max_standby_archive_delay when polar_max_replica_archive_delay is not configured"
);

$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok( $max_standby_streaming_delay_value eq "50s",
	"max_standby_streaming_delay is 50s");
ok( $max_standby_streaming_delay_value eq $max_replica_streaming_delay_value,
	"use value of max_standby_streaming_delay when polar_max_replica_streaming_delay is not configured"
);

# set polar_max_replica_streaming_delay via alter system command
$node_primary->safe_psql(
	'postgres', qq(
	alter system set polar_max_replica_archive_delay = '35s';
));
$node_primary->safe_psql(
	'postgres', qq(
	alter system set polar_max_replica_streaming_delay = '36s';
));
$node_primary->safe_psql('postgres', qq(select pg_reload_conf();));

$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok($max_replica_archive_delay_value eq "35s",
	"use corresponding value when polar_max_replica_archive_delay is set");
ok($max_replica_streaming_delay_value eq "36s",
	"use corresponding value when polar_max_replica_streaming_delay is set");

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
ok( $max_standby_archive_delay_value eq "40s",
	"max_standby_archive_delay is still 40s");
ok( $max_standby_streaming_delay_value eq "50s",
	"max_standby_streaming_delay is still 50s");

$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_archive_delay = '41s';
));
$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_streaming_delay = '51s';
));
$node_primary->safe_psql('postgres', qq(select pg_reload_conf();));

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
ok( $max_standby_archive_delay_value eq "41s",
	"max_standby_archive_delay is 41s");
ok( $max_standby_streaming_delay_value eq "51s",
	"max_standby_streaming_delay is 51s");

$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok( $max_replica_archive_delay_value eq "35s",
	"polar_max_replica_archive_delay value remians unchanged when it is set");
ok( $max_replica_streaming_delay_value eq "36s",
	"polar_max_replica_streaming_delay value remians unchanged when it is set"
);

# remove configuration and reload
my $primary_basedir = $node_primary->data_dir;
PostgreSQL::Test::Utils::system_or_bail(
	"echo '' > $primary_basedir/postgresql.auto.conf");
$node_primary->reload();

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
ok( $max_standby_archive_delay_value eq "30s",
	"max_standby_archive_delay is 30s");
ok( $max_standby_archive_delay_value eq $max_replica_archive_delay_value,
	"use value of max_standby_archive_delay when polar_max_replica_archive_delay configuration is removed"
);

$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok( $max_standby_streaming_delay_value eq "30s",
	"max_standby_streaming_delay is 30s");
ok( $max_standby_streaming_delay_value eq $max_replica_streaming_delay_value,
	"use value of max_standby_streaming_delay when polar_max_replica_streaming_delay configuration is removed"
);

# set polar_max_replica_streaming_delay via postgresql.conf
$node_primary->append_conf('postgresql.conf',
	"polar_max_replica_archive_delay = 38s");
$node_primary->append_conf('postgresql.conf',
	"polar_max_replica_streaming_delay = 39s");
$node_primary->reload();

$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_archive_delay = '43s';
));
$node_primary->safe_psql(
	'postgres', qq(
	alter system set max_standby_streaming_delay = '53s';
));
$node_primary->safe_psql('postgres', qq(select pg_reload_conf();));

$max_standby_archive_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_archive_delay;));
$max_standby_streaming_delay_value =
  $node_primary->safe_psql('postgres', qq(show max_standby_streaming_delay;));
print "max_standby_archive_delay: $max_standby_archive_delay_value\n";
print "max_standby_streaming_delay: $max_standby_streaming_delay_value\n";
ok( $max_standby_archive_delay_value eq "43s",
	"max_standby_archive_delay is 43s");
ok( $max_standby_streaming_delay_value eq "53s",
	"max_standby_streaming_delay is 53s");

$max_replica_archive_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_archive_delay;));
$max_replica_streaming_delay_value = $node_primary->safe_psql('postgres',
	qq(show polar_max_replica_streaming_delay;));
print "polar_max_replica_archive_delay: $max_replica_archive_delay_value\n";
print
  "polar_max_replica_streaming_delay: $max_replica_streaming_delay_value\n";
ok($max_replica_archive_delay_value eq "38s",
	"use corresponding value when polar_max_replica_archive_delay is set");
ok($max_replica_streaming_delay_value eq "39s",
	"use corresponding value when polar_max_replica_streaming_delay is set");

$node_primary->stop('fast');
done_testing();
