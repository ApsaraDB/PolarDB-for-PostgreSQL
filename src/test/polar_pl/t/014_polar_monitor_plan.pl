use strict;
use warnings;

use PostgresNode; 
use TestLib;
use Test::More tests => 2;
use Time::HiRes qw(usleep);

# This perl test case is used to test 'polar_monitor_plan.c'
# Set up node with logging collector
my $node = get_new_node('node');
$node->init();
$node->append_conf(
	'postgresql.conf', qq(
logging_collector = on
lc_messages = 'C'
));

$node->start();

# Verify that log output gets to the file
$node->psql('postgres', 'create extension polar_monitor_preload');

# node1 send signal to node2;
$node->psql('postgres', 'select polar_log_current_plan(pg_backend_pid())');

# might need to retry if logging collector process is slow...
my $max_attempts = 180 * 10;

my $current_logfiles;
for (my $attempts = 0; $attempts < $max_attempts; $attempts++)
{
	eval {
		$current_logfiles = slurp_file($node->data_dir . '/current_logfiles');
	};
	last unless $@;
	usleep(100_000);
}
die $@ if $@;

note "current_logfiles = $current_logfiles";

# We are only interested in error.log
my $error_log_name;
my $is_found = 0;
my @log_names_list = split(/\n/,$current_logfiles);
my $log_pattern = qr|^stderr log/postgresql-.*log$|;
for (my $i = 0 ; $i < scalar @log_names_list; $i = $i + 1)
{
	if ($log_names_list[$i] =~ m/$log_pattern/)
	{
		$error_log_name = $log_names_list[$i];
		$is_found = 1;
		last;
	}
}
is($is_found, 1, 'current_logfiles is sane');

$error_log_name =~ s/^stderr //;
chomp $error_log_name;

my $first_logfile;
my $bt_occurence_count;

# Verify that the backtraces of the processes are logged into logfile.
for (my $attempts = 0; $attempts < $max_attempts; $attempts++)
{
	$first_logfile = $node->data_dir . '/' . $error_log_name;
	chomp $first_logfile;
	print "file is $first_logfile";
	open my $fh, '<', $first_logfile
	  or die "Could not open '$first_logfile' $!";
	while (my $line = <$fh>)
	{
		chomp $line;
		if ($line =~ m/logging the plan of running query on PID*/)
		{
			$bt_occurence_count++;
		}
	}
	last if $bt_occurence_count == 1;
	usleep(100_000);
}

is($bt_occurence_count, 1, 'found expected backtrace in the log file');

$node->stop();