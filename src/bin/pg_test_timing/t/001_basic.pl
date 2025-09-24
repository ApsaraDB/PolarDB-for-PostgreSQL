
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;

#########################################
# Basic checks

program_help_ok('pg_test_timing');
program_version_ok('pg_test_timing');
program_options_handling_ok('pg_test_timing');

#########################################
# Test invalid option combinations

command_fails_like(
	[ 'pg_test_timing', '--duration' => 'a' ],
	qr/\Qpg_test_timing: invalid argument for option --duration\E/,
	'pg_test_timing: invalid argument for option --duration');
command_fails_like(
	[ 'pg_test_timing', '--duration' => '0' ],
	qr/\Qpg_test_timing: --duration must be in range 1..4294967295\E/,
	'pg_test_timing: --duration must be in range');
command_fails_like(
	[ 'pg_test_timing', '--cutoff' => '101' ],
	qr/\Qpg_test_timing: --cutoff must be in range 0..100\E/,
	'pg_test_timing: --cutoff must be in range');

#########################################
# We obviously can't check for specific output, but we can
# do a simple run and make sure it produces something.
# Also, note the output in the log for data collection purposes.

my $cmd = [ 'pg_test_timing', '--duration' => '1' ];
my ($stdout, $stderr);
print("# Running: " . join(" ", @{$cmd}) . "\n");
my $result = IPC::Run::run $cmd, '>' => \$stdout, '2>' => \$stderr;
is($result, 1, "pg_test_timing: exit code 0");
is($stderr, '', "pg_test_timing: no stderr");
like(
	$stdout,
	qr/
\QTesting timing overhead for 1 second.\E.*
\QHistogram of timing durations:\E.*
\QObserved timing durations up to 99.9900%:\E
/sx,
	'pg_test_timing: stdout passes sanity check');

note "pg_test_timing results:\n$stdout\n";

done_testing();
