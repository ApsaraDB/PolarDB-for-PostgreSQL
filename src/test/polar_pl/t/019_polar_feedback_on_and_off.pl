# 019_polar_feedback_on_and_off.pl
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
#	  src/test/polar_pl/t/019_polar_feedback_on_and_off.pl
#
# There are six scenarios
# 1. disable hot_standby_feedback on replica
# 2. enable hot_standby_feedback on replica
# 3. disable hot_standby_feedback and polar_standby_feedback on standby
# 4. disable hot_standby_feedback and enable polar_standby_feedback on standby
# 5. enable hot_standby_feedback and enable polar_standby_feedback on standby
# 6. enable hot_standby_feedback and disable polar_standby_feedback on standby
#
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;
plan tests => 6;

my $node_master = PostgreSQL::Test::Cluster->new('master');
$node_master->polar_init_primary;

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_master);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->polar_init_standby($node_master);

$node_master->append_conf('postgresql.conf',
		"synchronous_standby_names='"
	  . $node_replica->name . ","
	  . $node_standby->name
	  . "'");
$node_master->append_conf('postgresql.conf', 'wal_sender_timeout=3600s');
$node_replica->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');
$node_standby->append_conf('postgresql.conf', 'wal_receiver_timeout=3600s');

$node_master->start;
$node_master->polar_create_slot($node_replica->name);
$node_master->polar_create_slot($node_standby->name);

$node_replica->start;
$node_standby->start;

sub get_slot_xmins
{
	my ($node, $slotname, $check_expr) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT $check_expr
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$slotname';
	]) or die "Timed out waiting for slot xmins to advance";

	my $slotinfo = $node->slot($slotname);
	return ($slotinfo->{'xmin'});
}

sub replay_check
{
	my $newval = $node_master->safe_psql('postgres',
		'INSERT INTO replayed(val) SELECT coalesce(max(val),0) + 1 AS newval FROM replayed RETURNING val'
	);

	$node_master->wait_for_catchup($node_replica, 'replay',
		$node_master->lsn('insert'));
	$node_master->wait_for_catchup($node_standby, 'replay',
		$node_master->lsn('insert'));
	$node_replica->safe_psql('postgres',
		qq[SELECT 1 FROM replayed WHERE val = $newval])
	  or die "replica didn't replay master value $newval";
	$node_standby->safe_psql('postgres',
		qq[SELECT 1 FROM replayed WHERE val = $newval])
	  or die "standby didn't replay master value $newval";
	return;
}

sub wait_xmin_update_ready
{
	my ($node, $slotname, $xmin_expected) = @_;

	$node->poll_query_until(
		'postgres', qq[
		SELECT xmin = '$xmin_expected'
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = '$slotname';
	]) or die "Timed out waiting for slot xmins ready";
}

$node_master->safe_psql('postgres', 'CREATE TABLE replayed(val integer);');

# 1. disable hot_standby_feedback on replica
$node_replica->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_replica->reload;
replay_check();
my $xmin = get_slot_xmins($node_master, $node_replica->name, "xmin IS NULL");
is($xmin, '', 'replica xmin of non-cascaded slot null with hs_feedback off');

# 2. enable hot_standby_feedback on replica
$node_replica->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_replica->reload;
replay_check();
my $replica_ret =
  $node_replica->safe_psql('postgres', "select txid_current_snapshot();");
my ($replica_xmin, $replica_xmax, $replica_xip) = split /:/, $replica_ret;
wait_xmin_update_ready($node_master, $node_replica->name, $replica_xmin);
$xmin = get_slot_xmins($node_master, $node_replica->name, "xmin IS NOT NULL");
is($xmin, $replica_xmin,
	'replica xmin of non-cascaded slot not null with hs_feedback on');

# 3. disable hot_standby_feedback and polar_standby_feedback on standby
note "disable hot_standby_feedback and polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name, "xmin IS NULL");
is($xmin, '',
	'standby xmin of non-cascaded slot null with off hs_feedback and polar_feedback'
);

# 4. disable hot_standby_feedback and enable polar_standby_feedback on standby
note
  "disable hot_standby_feedback and enable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name, "xmin IS NULL");
is($xmin, '',
	'xmin of non-cascaded slot null with off hs_feedback,on polar_feedback');

# 5. enable hot_standby_feedback and enable polar_standby_feedback on standby
note
  "enable hot_standby_feedback and enable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = on;');
$node_standby->reload;
replay_check();
my $standby_ret =
  $node_standby->safe_psql('postgres', "select txid_current_snapshot();");
my ($standby_xmin, $standby_xmax, $standby_xip) = split /:/, $standby_ret;
wait_xmin_update_ready($node_master, $node_standby->name, $standby_xmin);
$xmin = get_slot_xmins($node_master, $node_standby->name, "xmin IS NOT NULL");
is($xmin, $standby_xmin,
	'standby xmin of non-cascaded slot not null with on hs_feedback and polar_feedback'
);

# 6. enable hot_standby_feedback and disable polar_standby_feedback on standby
note
  "enable hot_standby_feedback and disable polar_standby_feedback on standby";
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET polar_standby_feedback = off;');
$node_standby->reload;
replay_check();
$xmin = get_slot_xmins($node_master, $node_standby->name, "xmin IS NULL");
is($xmin, '',
	'standby xmin of non-cascaded slot not null on hs_feedback,off polar_feedback'
);

$node_master->stop;
$node_replica->stop;
$node_standby->stop;
