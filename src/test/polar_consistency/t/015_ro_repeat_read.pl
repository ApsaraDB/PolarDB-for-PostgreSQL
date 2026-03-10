# 015_ro_repeat_read.pl
#	  Test for RO repeat read.
#
# Copyright (c) 2024, Alibaba Group Holding Limited
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
#	  src/test/polar_consistency/t/015_ro_repeat_read.pl

use strict;
use warnings;
use Config;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

# replica
my $node_replica = PostgreSQL::Test::Cluster->new('replica1');
$node_replica->polar_init_replica($node_primary);

$node_primary->append_conf('postgresql.conf',
	"synchronous_standby_names='" . $node_replica->name . "'");
$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);

# disable logging collector, need grep logfile later
$node_replica->append_conf('postgresql.conf', "Logging_collector = false");
$node_replica->append_conf('postgresql.conf', "restart_after_crash=off");
$node_replica->start;

# rw create table and insert some data
my $block_size = 8192;
my $init_sql = "
	create table t1(id int);
	insert into t1 select generate_series(0, $block_size);
";

$node_primary->safe_psql('postgres', $init_sql);
my $rw_count =
  $node_primary->safe_psql('postgres', "select count(*) from t1;");

my $polar_datadir = $node_primary->polar_get_datadir;
my $relpath =
  $node_primary->safe_psql('postgres', "select pg_relation_filepath('t1');");
my $rel_file_path;
print "relpath: $relpath\n";
if ($relpath =~ /$polar_datadir/)
{
	$rel_file_path = $polar_datadir . $';
}
else
{
	die "wrong rel path: $relpath, polar_datadir is $polar_datadir\n";
}

# rw shutdown
$node_primary->stop;

# make the second block corrupted
my $second_block_content;
my $page_offset = $block_size * 2;

open my $fh, '+<', "$rel_file_path" or die "open($rel_file_path) failed: $!";
binmode $fh;
sysseek($fh, $page_offset, 0) or die "sysseek failed: $!";
sysread($fh, $second_block_content, $block_size) or die "sysread failed: $!";
# write some error data into the second block
sysseek($fh, $page_offset + $block_size / 2, 0) or die "sysseek failed: $!";
my $zero_str = 'A';
my $error_block = $zero_str * $block_size / 2;
syswrite($fh, $error_block) or die "syswrite failed: $!";
close $fh;
print "succ to corrupt the target relation file.\n";

# access the target block in ro
my $timed_out = 0;
$node_replica->psql(
	'postgres', 'select count(*) from t1;',
	on_error_stop => 0,
	on_error_die => 0,
	timeout => 5,
	timed_out => \$timed_out);
ok($timed_out eq 1, "RO query timed out!");

# restore the second block
open $fh, '+<', "$rel_file_path" or die "open($rel_file_path) failed: $!";
binmode $fh;
sysseek($fh, $page_offset, 0) or die "sysseek failed: $!";
syswrite($fh, $second_block_content) or die "syswrite failed: $!";
close $fh;
print "succ to restore the target relation file.\n";

# access the target block in ro
my $ro_count =
  $node_replica->safe_psql('postgres', 'select count(*) from t1;');
ok($ro_count eq $rw_count,
	"rw/ro query result is consistent: ro $ro_count, rw $rw_count");

# stop ro node
$node_replica->stop;

# check repeat read WARNING log
my $repeat_read_logs = () =
  $node_replica->log_content() =~ /times to repeat read blocks of relation/g;
ok($repeat_read_logs > 1, "found repeat read WARNING logs");

# cleanup nodes
$node_replica->clean_node;
$node_primary->clean_node;

done_testing();
