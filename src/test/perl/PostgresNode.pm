
=pod

=head1 NAME

PostgresNode - class representing PostgreSQL server instance

=head1 SYNOPSIS

  use PostgresNode;

  my $node = PostgresNode->get_new_node('mynode');

  # Create a data directory with initdb
  $node->init();

  # Start the PostgreSQL server
  $node->start();

  # Change a setting and restart
  $node->append_conf('postgresql.conf', 'hot_standby = on');
  $node->restart();

  # run a query with psql, like:
  #   echo 'SELECT 1' | psql -qAXt postgres -v ON_ERROR_STOP=1
  $psql_stdout = $node->safe_psql('postgres', 'SELECT 1');

  # Run psql with a timeout, capturing stdout and stderr
  # as well as the psql exit code. Pass some extra psql
  # options. If there's an error from psql raise an exception.
  my ($stdout, $stderr, $timed_out);
  my $cmdret = $node->psql('postgres', 'SELECT pg_sleep(60)',
	  stdout => \$stdout, stderr => \$stderr,
	  timeout => 30, timed_out => \$timed_out,
	  extra_params => ['--single-transaction'],
	  on_error_die => 1)
  print "Sleep timed out" if $timed_out;

  # Similar thing, more convenient in common cases
  my ($cmdret, $stdout, $stderr) =
      $node->psql('postgres', 'SELECT 1');

  # run query every second until it returns 't'
  # or times out
  $node->poll_query_until('postgres', q|SELECT random() < 0.1;|')
    or die "timed out";

  # Do an online pg_basebackup
  my $ret = $node->backup('testbackup1');

  # Take a backup of a running server
  my $ret = $node->backup_fs_hot('testbackup2');

  # Take a backup of a stopped server
  $node->stop;
  my $ret = $node->backup_fs_cold('testbackup3')

  # Restore it to create a new independent node (not a replica)
  my $replica = get_new_node('replica');
  $replica->init_from_backup($node, 'testbackup');
  $replica->start;

  # Stop the server
  $node->stop('fast');

  # Find a free, unprivileged TCP port to bind some other service to
  my $port = get_free_port();

=head1 DESCRIPTION

PostgresNode contains a set of routines able to work on a PostgreSQL node,
allowing to start, stop, backup and initialize it with various options.
The set of nodes managed by a given test is also managed by this module.

In addition to node management, PostgresNode instances have some wrappers
around Test::More functions to run commands with an environment set up to
point to the instance.

The IPC::Run module is required.

=cut

package PostgresNode;

use strict;
use warnings;

use Carp;
use Config;
use Cwd;
use Exporter 'import';
use Fcntl qw(:mode);
use File::Basename;
use File::Path qw(rmtree);
use File::Spec;
use File::stat qw(stat);
use File::Temp ();
use IPC::Run;
use RecursiveCopy;
use Socket;
use Test::More;
use TestLib ();
use Time::HiRes qw(usleep);
use Scalar::Util qw(blessed);

use Expect;

our @EXPORT = qw(
  get_new_node
  get_free_port
);

our ($use_tcp, $test_localhost, $test_pghost, $last_host_assigned,
	$testname, $polar_datadir,
	$last_port_assigned, @all_nodes, $died);

# For backward compatibility only.
our $vfs_path = '';
if ($Config{osname} eq 'msys')
{
	$vfs_path = `cd / && pwd -W`;
	chomp $vfs_path;
}

our ($polar_hostid, $polar_repli_logindex, $polar_master_logindex, $polar_standby_logindex, $polar_standby_no_logindex);

INIT
{

	# Set PGHOST for backward compatibility.  This doesn't work for own_host
	# nodes, so prefer to not rely on this when writing new tests.
	$use_tcp            = $TestLib::windows_os;
	$test_localhost     = "127.0.0.1";
	$last_host_assigned = 1;
	$test_pghost        = $use_tcp ? $test_localhost : TestLib::tempdir_short;
	$ENV{PGHOST}        = $test_pghost;
	$ENV{PGDATABASE}    = 'postgres';
	$ENV{PGUSER} = 'postgres';

	$testname = basename($0);
	$testname =~ s/\.[^.]+$//;

	# Tracking of last port value assigned to accelerate free port lookup.
	$last_port_assigned = int(rand() * 16384) + 49152;

	$polar_hostid=0;

$polar_standby_no_logindex =<<"standby_no_logindex";
	polar_logindex_mem_size=0
	shared_buffers = 512MB

standby_no_logindex

	$polar_repli_logindex=<<"repli_logindex";
	shared_buffers = 512MB
	polar_logindex_mem_size=128MB
	polar_xlog_queue_buffers=256MB
	polar_xlog_page_buffers=128MB

repli_logindex

	$polar_master_logindex=<<"master_logindex";
	shared_buffers = 512MB
	polar_logindex_mem_size=128MB
	polar_xlog_queue_buffers=256MB
	polar_xlog_page_buffers=128MB

master_logindex

$polar_standby_logindex=<<"standby_logindex";
	shared_buffers = 512MB
	polar_logindex_mem_size=128MB
	polar_xlog_queue_buffers=256MB
	polar_xlog_page_buffers=128MB

standby_logindex
}

=pod

=head1 METHODS

=over

=item PostgresNode::new($class, $name, $pghost, $pgport)

Create a new PostgresNode instance. Does not initdb or start it.

You should generally prefer to use get_new_node() instead since it takes care
of finding port numbers, registering instances for cleanup, etc.

=cut

sub new
{
	my ($class, $name, $pghost, $pgport) = @_;
	my $self = {
		_port    => $pgport,
		_host    => $pghost,
		_basedir => "$TestLib::tmp_check/t_${testname}_${name}_data",
		_name    => $name,
		_testname => $testname,
		_logfile_generation => 0,
		_logfile_base       => "$TestLib::log_path/${testname}_${name}",
		_logfile => "$TestLib::log_path/${testname}_${name}.log",
		_valgrind_log => "$TestLib::tmp_check/valgrind",
		_psql	=> undef,
		_polar_root_node	=> undef,
		_polar_node_type => "master",
		_polar_datadir => "$TestLib::tmp_check/t_${testname}_polar_data",
	};

	bless $self, $class;
	mkdir $self->{_basedir}
	  or
	  BAIL_OUT("could not create data directory \"$self->{_basedir}\": $!");

	$self->dump_info;

	$self->{_polar_root_node} = $self;
	$ENV{'polar_datadir'} = $self->polar_get_datadir;

	return $self;
}


=pod

=item $node->port()

Get the port number assigned to the host. This won't necessarily be a TCP port
open on the local host since we prefer to use unix sockets if possible.

Use $node->connstr() if you want a connection string.

=cut

sub port
{
	my ($self) = @_;
	return $self->{_port};
}

sub get_psql
{
	my ($self) = @_;
	return $self->{_psql};
}

sub set_psql
{
	my ($self, $psql) = @_;
	$self->{_psql} = $psql;
}

sub polar_set_root_node
{
	my ($self, $root_node) = @_;
	$self->{_polar_root_node} = $root_node;
}

sub polar_get_root_node
{
	my ($self) = @_;
	return $self->{_polar_root_node};
}

sub polar_get_node_type
{
	my ($self) = @_;
	return $self->{_polar_node_type};
}

=pod

=item $node->host()

Return the host (like PGHOST) for this instance. May be a UNIX socket path.

Use $node->connstr() if you want a connection string.

=cut

sub host
{
	my ($self) = @_;
	return $self->{_host};
}

=pod

=item $node->basedir()

The directory all the node's files will be within - datadir, archive directory,
backups, etc.

=cut

sub basedir
{
	my ($self) = @_;
	return $self->{_basedir};
}

=pod

=item $node->name()

The name assigned to the node at creation time.

=cut

sub name
{
	my ($self) = @_;
	return $self->{_name};
}

=pod

=item $node->polar_set_name($name)

POLAR: Create a new name for this node.

=cut

sub polar_set_name
{
	my ($self, $name) = @_;
	$self->{_name} = $name;
}

=pod

=item $node->logfile()

Path to the PostgreSQL log file for this instance.

=cut

sub logfile
{
	my ($self) = @_;
	return $self->{_logfile};
}

=pod

=item $node->connstr()

Get a libpq connection string that will establish a connection to
this node. Suitable for passing to psql, DBD::Pg, etc.

=cut

sub connstr
{
	my ($self, $dbname) = @_;
	my $pgport = $self->port;
	my $pghost = $self->host;
	if (!defined($dbname))
	{
		return "port=$pgport host=$pghost";
	}

	# Escape properly the database string before using it, only
	# single quotes and backslashes need to be treated this way.
	$dbname =~ s#\\#\\\\#g;
	$dbname =~ s#\'#\\\'#g;

	return "port=$pgport host=$pghost dbname='$dbname'";
}

=pod

=item $node->group_access()

Does the data dir allow group access?

=cut

sub group_access
{
	my ($self) = @_;

	my $dir_stat = stat($self->data_dir);

	defined($dir_stat)
	  or die('unable to stat ' . $self->data_dir);

	return (S_IMODE($dir_stat->mode) == 0750);
}

=pod

=item $node->data_dir()

Returns the path to the data directory. postgresql.conf and pg_hba.conf are
always here.

=cut

sub data_dir
{
	my ($self) = @_;
	my $res = $self->basedir;
	return "$res/pgdata";
}

=pod

=item $node->archive_dir()

If archiving is enabled, WAL files go here.

=cut

sub archive_dir
{
	my ($self) = @_;
	my $basedir = $self->basedir;
	return "$basedir/archives";
}

=pod

=item $node->backup_dir()

The output path for backups taken with $node->backup()

=cut

sub backup_dir
{
	my ($self) = @_;
	my $basedir = $self->basedir;
	return "$basedir/backup";
}

=pod

=item $node->info()

Return a string containing human-readable diagnostic information (paths, etc)
about this node.

=cut

sub info
{
	my ($self) = @_;
	my $_info = '';
	open my $fh, '>', \$_info or die;
	print $fh "Name: " . $self->name . "\n";
	print $fh "Data directory: " . $self->data_dir . "\n";
	print $fh "Backup directory: " . $self->backup_dir . "\n";
	print $fh "Archive directory: " . $self->archive_dir . "\n";
	print $fh "Connection string: " . $self->connstr . "\n";
	print $fh "Log file: " . $self->logfile . "\n";
	close $fh or die;
	return $_info;
}

=pod

=item $node->dump_info()

Print $node->info()

=cut

sub dump_info
{
	my ($self) = @_;
	print $self->info;
	return;
}


# Internal method to set up trusted pg_hba.conf for replication.  Not
# documented because you shouldn't use it, it's called automatically if needed.
sub set_replication_conf
{
	my ($self) = @_;
	my $pgdata = $self->data_dir;

	$self->host eq $test_pghost
	  or croak "set_replication_conf only works with the default host";

	open my $hba, '>>', "$pgdata/pg_hba.conf";
	print $hba "\n# Allow replication (set up by PostgresNode.pm)\n";
	if ($TestLib::windows_os)
	{
		print $hba
		  "host replication all $test_localhost/32 sspi include_realm=1 map=regress\n";
	}
	close $hba;
	return;
}

=pod

=item $node->polar_master_init(...)

Initialize a new master node on polar for testing.

=cut

sub polar_shared_storage_init
{
	my ($self) = @_;
	my $pgdata = $self->data_dir;
	my $port = $self->port;
	my $polar_datadir = $self->polar_get_datadir;

	mkdir $polar_datadir
	  or
	BAIL_OUT("could not create polar data directory $polar_datadir");
	TestLib::system_or_bail('polar-initdb.sh', "$pgdata/",
		"$polar_datadir/", 'localfs');

	$ENV{'polar_datadir'} = $polar_datadir;

  	return;
}

=pod
=item $node->polar_crate_slot
=cut
sub polar_create_slot
{
	my ($self, $slot) = @_;

	$self->psql('postgres', "SELECT * FROM pg_create_physical_replication_slot('$slot')",
		extra_params => ['-U', 'postgres']);
}

=pod

=item $node->polar_drop_slot

=cut

sub polar_drop_slot
{
	my ($self, $slot) = @_;

	$self->psql('postgres', "SELECT pg_drop_replication_slot('$slot')",
		extra_params => ['-U', 'postgres']);
}

=pod
=item $node->polar_drop_all_slots
=cut

sub polar_drop_all_slots
{
	my ($self) = @_;
	my $slots = $self->safe_psql('postgres', qq[select slot_name from pg_replication_slots;]);
	foreach my $slot (split('\n', $slots))
	{
		print "standby delete slot [".$slot."]\n";
		$self->polar_drop_slot($slot);
	}
}

=pod

=item $node->polar_set_recovery

=cut

sub polar_set_recovery
{
	my ($self, $root_node) = @_;
	$self->{_polar_node_type} = "replica";
	$self->{_polar_root_node} = $root_node;
	print "polar_set_recovery set node(".$self->port.") root_node(".$root_node->port.")\n";
	my $root_host = $root_node->host;
	my $root_port = $root_node->port;
	my $pgdata = $self->data_dir;

	open my $conf, '>>', "$pgdata/recovery.conf";

	print $conf "primary_conninfo='host=$root_host port=$root_port user=postgres dbname=postgres application_name=".$self->name."'\n";
	print $conf "polar_replica=on\n";
	print $conf "recovery_target_timeline='latest'\n";
	print $conf "primary_slot_name='".$self->name."'\n";

	close $conf;
	my $polar_datadir = $self->polar_get_datadir;
	print "before set, polar_datadir = $polar_datadir\n";
	$polar_datadir = $root_node->polar_get_datadir;
	print "after set, polar_datadir = $polar_datadir\n";
	$self->append_conf('postgresql.conf', "polar_datadir=\'file-dio://$polar_datadir\'");
	$self->polar_set_datadir($polar_datadir);
}

=pod
=item $node->polar_standby_set_recovery
=cut

sub polar_standby_set_recovery
{
	my ($self, $root_node) = @_;
	$self->{_polar_node_type} = "standby";
	$self->{_polar_root_node} = $root_node;
	print "polar_standby_set_recovery set node(".$self->port.") root_node(".$root_node->port.")\n";
	my $root_host = $root_node->host;
	my $root_port = $root_node->port;
	my $pgdata = $self->data_dir;
	open my $conf, '>>', "$pgdata/recovery.conf";
	print $conf "primary_conninfo='host=$root_host port=$root_port user=postgres dbname=postgres application_name=".$self->name."'\n";
	print $conf "standby_mode=on\n";
	print $conf "recovery_target_timeline='latest'\n";
	print $conf "primary_slot_name='".$self->name."'\n";
	close $conf;
}

=pod
=item $node->polar_datamax_set_recovery(root_node)
=cut

sub polar_datamax_set_recovery
{
	my ($self, $root_node) = @_;
	$self->{_polar_node_type} = "datamax";
	$self->{_polar_root_node} = $root_node;
	print "polar_datamax_set_recovery set node(".$self->port.") root_node(".$root_node->port.")\n";
	my $root_host = $root_node->host;
	my $root_port = $root_node->port;
	my $pgdata = $self->data_dir;
	open my $conf, '>>', "$pgdata/recovery.conf";
	print $conf "primary_conninfo='host=$root_host port=$root_port user=postgres dbname=postgres application_name=".$self->name."'\n";
	print $conf "polar_datamax_mode = standalone\n";
	print $conf "recovery_target_timeline='latest'\n";
	print $conf "primary_slot_name='".$self->name."'\n";
	close $conf;
}

=pod

=item $node->polar_drop_recovery

After recovery file was deleted, we think this node is going to be 'master' node.

=cut

sub polar_drop_recovery
{
	my ($self) = @_;
	print "drop recovery file for node(".$self->port."), root_node is (".$self->polar_get_root_node->port.")\n";
	$self->{_polar_node_type} = "master";
	$self->{_polar_root_node} = $self;
	my $pgdata = $self->data_dir;
	TestLib::system_or_bail('rm', '-f',"$pgdata/recovery.conf");
	TestLib::system_or_bail('rm', '-f',"$pgdata/recovery.done");
	TestLib::system_or_bail('rm', '-f',"$pgdata/polar_node_static.conf");
}

=pod
=item $node->polar_standby_build_data
=cut
sub polar_standby_build_data
{
	my ($self) = @_;
	my $root_node = $self->polar_get_root_node;
	my $src_polar_datadir = $root_node->polar_get_datadir;
	my $dst_polar_datadir = $src_polar_datadir.'_'.$self->name;
	if ($src_polar_datadir ne $dst_polar_datadir)
	{
		if ($root_node->polar_postmaster_is_alive)
		{
			# polar_basebackup build standby's data when root_node is alive.
			my $temp_datadir = $self->data_dir."_tmp";
			TestLib::system_or_bail('polar_basebackup', '-h', $root_node->host, '-p', $root_node->port,
									'-U', 'postgres', "--polardata=$dst_polar_datadir", "-D", $temp_datadir);
			TestLib::system_or_bail('rm', '-rf', $temp_datadir);
		}
		else
		{
			TestLib::system_or_bail('cp', '-frp', "$src_polar_datadir", "$dst_polar_datadir");
		}
		$self->append_conf('postgresql.conf', "polar_datadir=\'file-dio://$dst_polar_datadir\'");
		$self->polar_set_datadir($dst_polar_datadir);
		print "Building standby datadir ($dst_polar_datadir) from master datadir ($src_polar_datadir).";
	}
	else
	{
		die "Can not build standby datadir ($dst_polar_datadir) from master datadir ($src_polar_datadir).";
	}
	return;
}

=pod
=item $node->polar_get_datadir
=cut
sub polar_get_datadir
{
	my ($self) = @_;
	return $self->{_polar_datadir};
}

=pod
=item $node->polar_set_datadir
=cut
sub polar_set_datadir
{
	my ($self, $polar_datadir) = @_;
	$self->{_polar_datadir} = $polar_datadir;
	$ENV{'polar_datadir'} = $polar_datadir;
}

=pod
=item $node->polar_get_system_identifier
=cut
sub polar_get_system_identifier
{
	my ($self) = @_;
	my $polar_datadir = $self->polar_get_datadir;
	my $system_identifier = readpipe("pg_controldata -D $polar_datadir | grep 'system identifier' | cut -d ':' -f 2");
	print "get system_identifier: $system_identifier\n";
	return $system_identifier;
}

=pod
=item $node->polar_get_redo_location($data_dir)
=cut
sub polar_get_redo_location
{
	my($self, $datadir) = @_;
	my $name = $self->name;
	my $redoptr = readpipe("pg_controldata -D $datadir | grep 'REDO location' | cut -d ':' -f 2");
	print "get $name redo location: $redoptr";
	chomp($redoptr);
	return $redoptr;
}

=pod

=item $node->polar_init(...)

Initialize a new cluster base on polar for testing.

=cut

sub polar_init
{
	my ($self, $is_master, $mode) = @_;

	my $host   = $self->host;
	my %modes = (
		'polar_default' => '',
		'polar_repli_logindex' => $polar_repli_logindex,
		'polar_master_logindex' => $polar_master_logindex,
		'polar_standby_logindex' => $polar_standby_logindex,
		'polar_standby_no_logindex' => $polar_standby_no_logindex,
	);
	
	my $port = $self->port;
	my $pgdata = $self->data_dir;
	my $polar_datadir = $self->polar_get_datadir;

	$mode = '<undef>' if !defined($mode);
	croak "unknown mode for 'postgres.conf': '$mode', valid modes are "
	  . join(', ', keys %modes)
	  if !defined($modes{$mode});

	TestLib::system_or_bail('initdb', '-k', '-U', 'postgres', '-D', $pgdata);
	$polar_hostid++;

	open my $conf, '>>', "$pgdata/postgresql.conf";

	my $default_conf =<<"default_conf";

	# Added by PostgresNode.pm for polar test case
	polar_enable_shared_storage_mode = on
	polar_hostid = $polar_hostid
	port=$port
	max_connections=100
	shared_buffers=5GB
	synchronous_commit=off
	full_page_writes=off
	max_wal_size=16GB
	min_wal_size=4GB
	autovacuum_naptime = 10min
	polar_vfs.localfs_mode = true
	shared_preload_libraries = '\$libdir/polar_vfs,\$libdir/polar_worker'
	polar_disk_name='tap_test'
	polar_datadir='file-dio://$polar_datadir'
	$modes{$mode}
	wal_level = replica
	hot_standby=on
	hot_standby_feedback=on
	polar_standby_feedback=on
	logging_collector=on
	max_worker_processes=32
	max_prepared_transactions=10
	polar_csn_enable=on

default_conf

	print $conf $default_conf;

	if ($TestLib::windows_os)
	{
		print $conf "listen_addresses = '$host'\n";
	}
	else
	{
		print $conf "unix_socket_directories = '$host'\n";
		print $conf "listen_addresses = ''\n";
	}

	close $conf;

	chmod($self->group_access ? 0640 : 0600, "$pgdata/postgresql.conf")
	  or die("unable to set permissions for $pgdata/postgresql.conf");
	
  	$self->polar_shared_storage_init if $is_master;

	return;
}

=pod
=item $node->polar_init_datamax(...)
initialize datamax node
=cut

sub polar_init_datamax
{
	my ($self, $primary_system_identifier, $mode) = @_;
	
	# set polar datamax datadir
	my $name = $self->name;
	my $polar_datamax_datadir = "$TestLib::tmp_check/t_${testname}_polar_${name}_data";
	$self->polar_set_datadir($polar_datamax_datadir);

	my $host   = $self->host;
	my %modes = (
		'polar_default' => '',
		'polar_repli_logindex' => $polar_repli_logindex,
		'polar_master_logindex' => $polar_master_logindex,
		'polar_standby_logindex' => $polar_standby_logindex,
		'polar_standby_no_logindex' => $polar_standby_no_logindex,
	);
	
	my $port = $self->port;
	my $pgdata = $self->data_dir;
	my $polar_datadir = $self->polar_get_datadir;

	$mode = '<undef>' if !defined($mode);
	croak "unknown mode for 'postgres.conf': '$mode', valid modes are "
	  . join(', ', keys %modes)
	  if !defined($modes{$mode});

	TestLib::system_or_bail('initdb', '-k', '-U', 'postgres', '-D', $pgdata, '-i', $primary_system_identifier);
	$polar_hostid++;

	open my $conf, '>>', "$pgdata/postgresql.conf";

	my $default_conf =<<"default_conf";

	# Added by PostgresNode.pm for polar test case
	polar_enable_shared_storage_mode = on
	polar_hostid = $polar_hostid
	port=$port
	max_connections=100
	shared_buffers=5GB
	synchronous_commit=off
	full_page_writes=off
	max_wal_size=16GB
	min_wal_size=4GB
	autovacuum_naptime = 10min
	polar_vfs.localfs_test_mode = true
	shared_preload_libraries = '\$libdir/polar_vfs,\$libdir/polar_worker'
	polar_disk_name='tap_test'
	polar_datadir='file-dio://$polar_datadir'
	$modes{$mode}
	wal_level = replica
	hot_standby=on
	hot_standby_feedback=on
	polar_standby_feedback=on
	logging_collector=on
	max_worker_processes=32
	max_prepared_transactions=10

default_conf

	print $conf $default_conf;

	if ($TestLib::windows_os)
	{
		print $conf "listen_addresses = '$host'\n";
	}
	else
	{
		print $conf "unix_socket_directories = '$host'\n";
		print $conf "listen_addresses = ''\n";
	}

	close $conf;

	chmod($self->group_access ? 0640 : 0600, "$pgdata/postgresql.conf")
	  or die("unable to set permissions for $pgdata/postgresql.conf");
	
  	$self->polar_shared_storage_init;

	return;
}


=pod

=item $node->init(...)

Initialize a new cluster for testing.

Authentication is set up so that only the current OS user can access the
cluster. On Unix, we use Unix domain socket connections, with the socket in
a directory that's only accessible to the current user to ensure that.
On Windows, we use SSPI authentication to ensure the same (by pg_regress
--config-auth).

WAL archiving can be enabled on this node by passing the keyword parameter
has_archiving => 1. This is disabled by default.

postgresql.conf can be set up for replication by passing the keyword
parameter allows_streaming => 'logical' or 'physical' (passing 1 will also
suffice for physical replication) depending on type of replication that
should be enabled. This is disabled by default.

The new node is set up in a fast but unsafe configuration where fsync is
disabled.

=cut

sub init
{
	my ($self, %params) = @_;
	my $port   = $self->port;
	my $pgdata = $self->data_dir;
	my $host   = $self->host;

	$params{allows_streaming} = 0 unless defined $params{allows_streaming};
	$params{has_archiving}    = 0 unless defined $params{has_archiving};
  $params{enable_encryption} = 0 unless defined $params{enable_encryption};
  $params{enable_flashback_log} = 0 unless defined $params{enable_flashback_log};

	mkdir $self->backup_dir;
	mkdir $self->archive_dir;

	if ($params{enable_encryption})
	{
		TestLib::system_or_bail('initdb', '-D', $pgdata, '-A', 'trust', '-N',
								'--cluster-passphrase-command', 'echo "adfadsfadssssssssfa12312312312312312312312%p123"',
								'-e', 'aes-256',
								@{ $params{extra} });
	}
	else
	{
		TestLib::system_or_bail('initdb', '-D', $pgdata, '-A', 'trust', '-N',
								@{ $params{extra} });
	}
	TestLib::system_or_bail($ENV{PG_REGRESS}, '--config-auth', $pgdata,
		@{ $params{auth_extra} });

	open my $conf, '>>', "$pgdata/postgresql.conf";
	print $conf "\n# Added by PostgresNode.pm\n";
	print $conf "fsync = off\n";
	print $conf "restart_after_crash = off\n";
	print $conf "log_line_prefix = '%m [%p] [%P] %q%a '\n";
	print $conf "log_statement = all\n";
	print $conf "log_replication_commands = on\n";
	print $conf "wal_retrieve_retry_interval = '500ms'\n";
	print $conf "max_worker_processes = 32\n";

	# If we create a no-polar standby node, polar_standby_feedback
	# should be set to true because of connections on standby.
	print $conf "polar_standby_feedback = on\n";

	# If a setting tends to affect whether tests pass or fail, print it after
	# TEMP_CONFIG.  Otherwise, print it before TEMP_CONFIG, thereby permitting
	# overrides.  Settings that merely improve performance or ease debugging
	# belong before TEMP_CONFIG.
	print $conf TestLib::slurp_file($ENV{TEMP_CONFIG})
	  if defined $ENV{TEMP_CONFIG};

	# XXX Neutralize any stats_temp_directory in TEMP_CONFIG.  Nodes running
	# concurrently must not share a stats_temp_directory.
	print $conf "stats_temp_directory = 'pg_stat_tmp'\n";

	if ($params{allows_streaming})
	{
		if ($params{allows_streaming} eq "logical")
		{
			print $conf "wal_level = logical\n";
		}
		else
		{
			print $conf "wal_level = replica\n";
		}
		print $conf "max_wal_senders = 5\n";
		print $conf "max_replication_slots = 5\n";
		print $conf "polar_enable_shared_storage_mode=off\n";
		# if polar_enable_shared_storage_mode is off, polar_dropdb_write_wal_before_rm_file 
		# should be off too, otherwise no drop db wal will be recorded.
		print $conf "polar_dropdb_write_wal_before_rm_file=off\n";
		print $conf "max_wal_size = 2GB\n";
		print $conf "shared_buffers = 1MB\n";
		print $conf "wal_log_hints = on\n";
		print $conf "hot_standby = on\n";
		print $conf "max_connections = 10\n";
		print $conf "polar_wal_snd_reserved_for_superuser = -1\n";
		print $conf "polar_enable_persisted_logical_slot = off\n";
		print $conf "polar_enable_persisted_physical_slot = off\n";
	}
	else
	{
		print $conf "wal_level = minimal\n";
		print $conf "max_wal_senders = 0\n";
	}

	if ($params{enable_flashback_log})
	{
		print $conf "polar_enable_flashback_log = on\n";
		print $conf "polar_enable_lazy_checkpoint = off\n";
	}

	print $conf "port = $port\n";
	if ($use_tcp)
	{
		print $conf "unix_socket_directories = ''\n";
		print $conf "listen_addresses = '$host'\n";
	}
	else
	{
		print $conf "unix_socket_directories = '$host'\n";
		print $conf "listen_addresses = ''\n";
	}
	close $conf;

	chmod($self->group_access ? 0640 : 0600, "$pgdata/postgresql.conf")
	  or die("unable to set permissions for $pgdata/postgresql.conf");

	$self->set_replication_conf if $params{allows_streaming};
	$self->enable_archiving     if $params{has_archiving};
	return;
}

=pod

=item $node->append_conf(filename, str)

A shortcut method to append to files like pg_hba.conf and postgresql.conf.

Does no validation or sanity checking. Does not reload the configuration
after writing.

A newline is automatically appended to the string.

=cut

sub append_conf
{
	my ($self, $filename, $str) = @_;

	my $conffile = $self->data_dir . '/' . $filename;

	TestLib::append_to_file($conffile, $str . "\n");

	chmod($self->group_access() ? 0640 : 0600, $conffile)
	  or die("unable to set permissions for $conffile");

	return;
}

=pod

=item $node->backup(backup_name)

Create a hot backup with B<pg_basebackup> in subdirectory B<backup_name> of
B<< $node->backup_dir >>, including the WAL. WAL files
fetched at the end of the backup, not streamed.

You'll have to configure a suitable B<max_wal_senders> on the
target server since it isn't done by default.

=cut

sub backup
{
	my ($self, $backup_name) = @_;
	my $backup_path = $self->backup_dir . '/' . $backup_name;
	my $name        = $self->name;

	print "# Taking polar_basebackup $backup_name from node \"$name\"\n";
	TestLib::system_or_bail('polar_basebackup', '-D', $backup_path, '-h',
		$self->host, '-p', $self->port, '--no-sync');
	print "# Backup finished\n";
	return;
}

sub polar_backup
{
	my ($self, $backup_name, $polardata) = @_;
	my $backup_path = $self->backup_dir . '/' . $backup_name;
	my $name        = $self->name;

	print "# Taking polar_basebackup $backup_name from node \"$name\"\n";
	TestLib::system_or_bail('polar_basebackup', '-D', $backup_path, '-h',
		$self->host, '-p', $self->port, '--no-sync', '--polardata='.$polardata, '-v');
	print "# Polar backup finished\n";
	return;
}

=item $node->backup_fs_hot(backup_name)

Create a backup with a filesystem level copy in subdirectory B<backup_name> of
B<< $node->backup_dir >>, including WAL.

Archiving must be enabled, as B<pg_start_backup()> and B<pg_stop_backup()> are
used. This is not checked or enforced.

The backup name is passed as the backup label to B<pg_start_backup()>.

=cut

sub backup_fs_hot
{
	my ($self, $backup_name) = @_;
	$self->_backup_fs($backup_name, 1);
	return;
}

=item $node->backup_fs_cold(backup_name)

Create a backup with a filesystem level copy in subdirectory B<backup_name> of
B<< $node->backup_dir >>, including WAL. The server must be
stopped as no attempt to handle concurrent writes is made.

Use B<backup> or B<backup_fs_hot> if you want to back up a running server.

=cut

sub backup_fs_cold
{
	my ($self, $backup_name) = @_;
	$self->_backup_fs($backup_name, 0);
	return;
}


# Common sub of backup_fs_hot and backup_fs_cold
sub _backup_fs
{
	my ($self, $backup_name, $hot) = @_;
	my $backup_path = $self->backup_dir . '/' . $backup_name;
	my $port        = $self->port;
	my $name        = $self->name;

	print "# Taking filesystem backup $backup_name from node \"$name\"\n";

	if ($hot)
	{
		my $stdout = $self->safe_psql('postgres',
			"SELECT * FROM pg_start_backup('$backup_name');");
		print "# pg_start_backup: $stdout\n";
	}

	RecursiveCopy::copypath(
		$self->data_dir,
		$backup_path,
		filterfn => sub {
			my $src = shift;
			return ($src ne 'log' and $src ne 'postmaster.pid');
		});

	if ($hot)
	{

		# We ignore pg_stop_backup's return value. We also assume archiving
		# is enabled; otherwise the caller will have to copy the remaining
		# segments.
		my $stdout =
		  $self->safe_psql('postgres', 'SELECT * FROM pg_stop_backup();');
		print "# pg_stop_backup: $stdout\n";
	}

	print "# Backup finished\n";
	return;
}



=pod

=item $node->init_from_backup(root_node, backup_name)

Initialize a node from a backup, which may come from this node or a different
node. root_node must be a PostgresNode reference, backup_name the string name
of a backup previously created on that node with $node->backup.

Does not start the node after initializing it.

A recovery.conf is not created.

Streaming replication can be enabled on this node by passing the keyword
parameter has_streaming => 1. This is disabled by default.

Restoring WAL segments from archives using restore_command can be enabled
by passing the keyword parameter has_restoring => 1. This is disabled by
default.

The backup is copied, leaving the original unmodified. pg_hba.conf is
unconditionally set to enable replication connections.

=cut

sub init_from_backup
{
	my ($self, $root_node, $backup_name, %params) = @_;
	my $backup_path = $root_node->backup_dir . '/' . $backup_name;
	my $host        = $self->host;
	my $port        = $self->port;
	my $node_name   = $self->name;
	my $root_name   = $root_node->name;

	$params{has_streaming} = 0 unless defined $params{has_streaming};
	$params{has_restoring} = 0 unless defined $params{has_restoring};

	print
	  "# Initializing node \"$node_name\" from backup \"$backup_name\" of node \"$root_name\"\n";
	croak "Backup \"$backup_name\" does not exist at $backup_path"
	  unless -d $backup_path;

	mkdir $self->backup_dir;
	mkdir $self->archive_dir;

	my $data_path = $self->data_dir;
	rmdir($data_path);
	RecursiveCopy::copypath($backup_path, $data_path);
	chmod(0700, $data_path);

	# Base configuration for this node
	$self->append_conf(
		'postgresql.conf',
		qq(
port = $port
));
	if ($use_tcp)
	{
		$self->append_conf('postgresql.conf', "listen_addresses = '$host'");
	}
	else
	{
		$self->append_conf('postgresql.conf',
			"unix_socket_directories = '$host'");
	}
	$self->enable_streaming($root_node) if $params{has_streaming};
	$self->enable_restoring($root_node) if $params{has_restoring};
	return;
}

=pod

=item $node->rotate_logfile()

Switch to a new PostgreSQL log file.  This does not alter any running
PostgreSQL process.  Subsequent method calls, including pg_ctl invocations,
will use the new name.  Return the new name.

=cut

sub rotate_logfile
{
	my ($self) = @_;
	$self->{_logfile} = sprintf('%s_%d.log',
		$self->{_logfile_base},
		++$self->{_logfile_generation});
	return $self->{_logfile};
}

=pod

=item $node->start(%params) => success_or_failure

Wrapper for pg_ctl start

Start the node and wait until it is ready to accept connections.

=over

=item fail_ok => 1

By default, failure terminates the entire F<prove> invocation.  If given,
instead return a true or false value to indicate success or failure.

=back

=cut

sub start
{
	my ($self, %params) = @_;
	my $port   = $self->port;
	my $pgdata = $self->data_dir;
	my $name   = $self->name;
	$params{use_valgrind} = 0 unless defined $params{use_valgrind};

	BAIL_OUT("node \"$name\" is already running") if defined $self->{_pid};
	print("### Starting node \"$name\"\n");
	my $ret;

	if ($params{use_valgrind})
	{
		my $valgrind_log = $self->{_valgrind_log};
		mkdir $valgrind_log;

		$ret = TestLib::system_log('valgrind', '--leak-check=full', '--gen-suppressions=all',
		   '--suppressions=../../../src/tools/valgrind.supp', '--time-stamp=yes',
		   "--log-file=$valgrind_log/%p.log", '--trace-children=yes',
		   'pg_ctl', '-D', $self->data_dir, '-l', $self->logfile, 'start', '-w', '-c');
	}
	else
	{
		$ret = TestLib::system_log('pg_ctl', '-D', $self->data_dir, '-l',
			$self->logfile, 'start', '-w', '-c');
	}

	if ($ret != 0)
	{
		print "# pg_ctl start failed; logfile:\n";
		print TestLib::slurp_file($self->logfile);
		BAIL_OUT("pg_ctl start failed") unless $params{fail_ok};
		return 0;
	}

	$self->_update_pid(1);
	return 1;
}

=pod

=item $node->kill9()

Send SIGKILL (signal 9) to the postmaster.

Note: if the node is already known stopped, this does nothing.
However, if we think it's running and it's not, it's important for
this to fail.  Otherwise, tests might fail to detect server crashes.

=cut

sub kill9
{
	my ($self) = @_;
	my $name = $self->name;
	return unless defined $self->{_pid};
	print "### Killing node \"$name\" using signal 9\n";
	# kill(9, ...) fails under msys Perl 5.8.8, so fall back on pg_ctl.
	kill(9, $self->{_pid})
	  or TestLib::system_or_bail('pg_ctl', 'kill', 'KILL', $self->{_pid});
	$self->{_pid} = undef;
	return;
}

=pod

=item $node->stop(mode)

Stop the node using pg_ctl -m $mode and wait for it to stop.

Note: if the node is already known stopped, this does nothing.
However, if we think it's running and it's not, it's important for
this to fail.  Otherwise, tests might fail to detect server crashes.

=cut

sub stop
{
	my ($self, $mode) = @_;
	my $port   = $self->port;
	my $pgdata = $self->data_dir;
	my $name   = $self->name;
	$mode = 'fast' unless defined $mode;
	return unless defined $self->{_pid};
	print "### Stopping node \"$name\" using mode $mode\n";
	TestLib::system_or_bail('pg_ctl', '-D', $pgdata, '-m', $mode, 'stop');
	$self->_update_pid(0);
	return;
}

=pod

=item $node->reload()

Reload configuration parameters on the node.

=cut

sub reload
{
	my ($self) = @_;
	my $port   = $self->port;
	my $pgdata = $self->data_dir;
	my $name   = $self->name;
	print "### Reloading node \"$name\"\n";
	TestLib::system_or_bail('pg_ctl', '-D', $pgdata, 'reload');
	return;
}

=pod

=item $node->restart()

Wrapper for pg_ctl restart

=cut

sub restart
{
	my ($self)  = @_;
	my $port    = $self->port;
	my $pgdata  = $self->data_dir;
	my $logfile = $self->logfile;
	my $name    = $self->name;
	print "### Restarting node \"$name\"\n";
	TestLib::system_or_bail('pg_ctl', '-D', $pgdata, '-l', $logfile,
		'restart');
	$self->_update_pid(1);
	return;
}

=pod

=item $node->restart_no_check()
=cut

sub restart_no_check
{
	my ($self) = @_;

	my $port    = $self->port;
	my $pgdata  = $self->data_dir;
	my $logfile = $self->logfile;
	my $name    = $self->name;
	print "### Restarting node \"$name\"\n";

	my $ret = TestLib::system_log('pg_ctl', '-D', $pgdata, '-l', $logfile,
		'restart');

	if ($ret != 0)
	{
		print "# pg_ctl start failed; logfile:\n";
		print TestLib::slurp_file($self->logfile);
	}

	$self->_update_pid(1);

	return $ret;
}

=pod

=item $node->promote()

Wrapper for pg_ctl promote

=cut

sub promote
{
	my ($self)  = @_;
	my $port    = $self->port;
	my $pgdata  = $self->data_dir;
	my $logfile = $self->logfile;
	my $name    = $self->name;
	print "### Promoting node \"$name\"\n";
	TestLib::system_or_bail('pg_ctl', '-D', $pgdata, '-l', $logfile,
		'promote');
	$self->{_polar_node_type} = "master";
	$self->{_polar_root_node} = $self;
	return;
}

=pod

=item $node->promote_async()

Wrapper for pg_ctl promote in async mode

=cut

sub promote_async
{
	my ($self)  = @_;
	my $port    = $self->port;
	my $pgdata  = $self->data_dir;
	my $logfile = $self->logfile;
	my $name    = $self->name;
	print "### Promoting node \"$name\"\n";
	system("pg_ctl promote -D $pgdata -l $logfile &");
	$self->{_polar_node_type} = "master";
	$self->{_polar_root_node} = $self;
	return;
}

=pod
=item $node->promote_constraint(force_promote)
only set root_node and node_type when promote success
=cut

sub promote_constraint
{
	my ($self, $force) = @_;
	my $pgdata  = $self->data_dir;
	my $logfile = $self->logfile;
	my $name    = $self->name; 
	print "### Promoting node \"$name\"\n";
	my $ret;
	if ($force == 0)
	{
		$ret = system("pg_ctl -D $pgdata -l $logfile promote");
	}
	# force promote
	else
	{
		$ret = system("pg_ctl -D $pgdata -l $logfile promote -f");
	}

	# set node_type and root_node when promote success
	if ($ret == 0)
	{
		$self->{_polar_node_type} = "master";
		$self->{_polar_root_node} = $self;
	}
	return $ret;
}

=pod
=item $node->pgbench_test(...)
=cut
sub pgbench_test
{
	my ($self, $init_flag, $init_data, $client_num, $job_num, $test_time, $async_flag) = @_;
	my $host 	= $self->host;
	my $port    = $self->port;
	my $name	= $self->name;
	my $ret		= 0;
	print "### Pgbench test on node \"$name\"\n";
	# init database when init_flag = 1
	if ($init_flag == 1)
	{
		$ret = system("pgbench -n -i -s $init_data -h $host -p $port -U postgres");
	}
	if ($ret == 0)
	{
		# do pgbench_test in async mode
		if ($async_flag == 1)
		{
			$ret = system("pgbench -n -M prepared -r -c $client_num -j $job_num -T $test_time -h $host -p $port -b tpcb-like -U postgres &");	
		}
		else
		{
			$ret = system("pgbench -n -M prepared -r -c $client_num -j $job_num -T $test_time -h $host -p $port -b tpcb-like -U postgres");
		}
	}
	return $ret;
}

=pod

=item $node->
=pod

=item $node->kill()

POLAR: Wrapper for $node->kill9()

We should wait for at least 10 seconds after send KILL signal to
postmaster. Because the backend child process may be trapped in
function backend_read_statsfile which would wait for response of
dead stat collector process at most PGSTAT_MAX_WAIT_TIME milliseconds.

=cut

sub kill
{
	my ($self)  = @_;
	$self->kill9;
	# POLAR: wait for previous postmaster to die.
	usleep(20000_000);
	return;
}

=pod

=item $node->find_child(process_name)
POLAR: Find child process base on process name

=cut

sub find_child
{
	my ($self, $process_name) = @_;
	my $pid=0;
	my @childs=`ps -o pid,cmd --ppid $self->{_pid}` or die "can't run ps! $! \n";

	foreach my $child (@childs)
	{
		$child =~ s/^\s+|\s+$//g;
		my $pos = index($child, $process_name);
		if ($pos > 0)
		{
			$pos = index($child, ' ');
			$pid = substr($child, 0, $pos);
			$pid =~ s/^\s+|\s+$//g;
			print "### Killing child process \"$pid\", \"$child\" using signal 9\n";
			last;
		}
	}

	return $pid;
}

=pod

=item $node->kill_child(process_name)
POLAR: Kill child process base on process name

=cut

sub kill_child
{
	my ($self, $process_name) = @_;
	my $pid = $self->find_child($process_name);

	#TestLib::system_or_bail('pg_ctl', 'kill', 'KILL', $pid);
	TestLib::system_or_bail('kill', '-9', $pid);
}

=pod

=item $node->stop_child(process_name)
POLAR: stop child process base on process name

=cut

sub stop_child
{
	my ($self, $process_name) = @_;
	my $pid = $self->find_child($process_name);

	TestLib::system_or_bail('kill', '-19', $pid);
}

=pod

=item $node->resume_child(process_name)
POLAR: resume child process base on process name

=cut

sub resume_child
{
	my ($self, $process_name) = @_;
	my $pid = $self->find_child($process_name);

	TestLib::system_or_bail('kill', '-18', $pid);
}

=pod
=item $node->wait_walstreaming_establish_timeout(timeout)
wait for walstreaming establish during timeout
=cut
 
sub wait_walstreaming_establish_timeout
{
	my ($self, $timeout) = @_;
	my $walstream_state = "";
	my $walreceiver_pid = 0;
	my $root_node = $self->polar_get_root_node;
	my $name = $self->name;
	my $i = 0;
	for($i = 0; $i < $timeout && $walreceiver_pid == 0; $i = $i + 1)
	{
		$walstream_state = $root_node->safe_psql('postgres',
		qq[select state from pg_stat_replication where application_name = '$name';]);
		if ($walstream_state eq "streaming")
		{
			$walreceiver_pid = $self->find_child('walreceiver');
		}
		sleep 1;
	}
	print "walreceiver_pid: $walreceiver_pid\n";
	if ($walreceiver_pid == 0)
	{
		print "wal streaming haven't been established after $timeout secs\n";
	}	
	return $walreceiver_pid;
}

=pod

=item $node->polar_postmaster_is_alive()

=cut

sub polar_postmaster_is_alive
{
	my ($self) = @_;
	if (defined $self->{_pid})
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

# Internal routine to enable streaming replication on a standby node.
sub enable_streaming
{
	my ($self, $root_node) = @_;
	my $root_connstr = $root_node->connstr;
	my $name         = $self->name;

	print "### Enabling streaming replication for node \"$name\"\n";
	$self->append_conf(
		'recovery.conf', qq(
primary_conninfo='$root_connstr application_name=$name'
standby_mode=on
));
	return;
}

# Internal routine to enable archive recovery command on a standby node
sub enable_restoring
{
	my ($self, $root_node) = @_;
	my $path = TestLib::perl2host($root_node->archive_dir);
	my $name = $self->name;

	print "### Enabling WAL restore for node \"$name\"\n";

	# On Windows, the path specified in the restore command needs to use
	# double back-slashes to work properly and to be able to detect properly
	# the file targeted by the copy command, so the directory value used
	# in this routine, using only one back-slash, need to be properly changed
	# first. Paths also need to be double-quoted to prevent failures where
	# the path contains spaces.
	$path =~ s{\\}{\\\\}g if ($TestLib::windows_os);
	my $copy_command =
	  $TestLib::windows_os
	  ? qq{copy "$path\\\\%f" "%p"}
	  : qq{cp "$path/%f" "%p"};

	$self->append_conf(
		'recovery.conf', qq(
restore_command = '$copy_command'
standby_mode = on
));
	return;
}

# Internal routine to enable archiving
sub enable_archiving
{
	my ($self) = @_;
	my $path   = TestLib::perl2host($self->archive_dir);
	my $name   = $self->name;

	print "### Enabling WAL archiving for node \"$name\"\n";

	# On Windows, the path specified in the restore command needs to use
	# double back-slashes to work properly and to be able to detect properly
	# the file targeted by the copy command, so the directory value used
	# in this routine, using only one back-slash, need to be properly changed
	# first. Paths also need to be double-quoted to prevent failures where
	# the path contains spaces.
	$path =~ s{\\}{\\\\}g if ($TestLib::windows_os);
	my $copy_command =
	  $TestLib::windows_os
	  ? qq{copy "%p" "$path\\\\%f"}
	  : qq{cp "%p" "$path/%f"};

	# Enable archive_mode and archive_command on node
	$self->append_conf(
		'postgresql.conf', qq(
archive_mode = on
archive_command = '$copy_command'
));
	return;
}

# Internal method
sub _update_pid
{
	my ($self, $is_running) = @_;
	my $name = $self->name;

	# If we can open the PID file, read its first line and that's the PID we
	# want.
	if (open my $pidfile, '<', $self->data_dir . "/postmaster.pid")
	{
		chomp($self->{_pid} = <$pidfile>);
		print "# Postmaster PID for node \"$name\" is $self->{_pid}\n";
		close $pidfile;

		# If we found a pidfile when there shouldn't be one, complain.
		BAIL_OUT("postmaster.pid unexpectedly present") unless $is_running;
		return;
	}

	$self->{_pid} = undef;
	print "# No postmaster PID for node \"$name\"\n";

	# Complain if we expected to find a pidfile.
	BAIL_OUT("postmaster.pid unexpectedly not present") if $is_running;
	return;
}

=pod
# get the value of specific variable via gdb postmaster
=item $node->polar_get_variable_value
=cut

sub polar_get_variable_value
{
    my ($self, $variable) = @_;
	my $pid = $self->{_pid};
    my $value = -1;
    my $command = "gdb -p $pid -ex 'set confirm off' -ex 'p $variable' -ex 'quit'";
    my $output = readpipe($command);
	print "gdb output:$output\n";
    my @result = split('\n', $output);
    my $str = "\$1";
    foreach my $i (@result)  
    { 
        if (index($i, $str) != -1)
        {
            $value = (split(" ", $i))[2];
            print "value: $value\n";
            return $value;
        }
    }
    return $value;
}

=pod

=item PostgresNode->get_new_node(node_name, %params)

Build a new object of class C<PostgresNode> (or of a subclass, if you have
one), assigning a free port number.  Remembers the node, to prevent its port
number from being reused for another node, and to ensure that it gets
shut down when the test script exits.

You should generally use this instead of C<PostgresNode::new(...)>.

=over

=item port => [1,65535]

By default, this function assigns a port number to each node.  Specify this to
force a particular port number.  The caller is responsible for evaluating
potential conflicts and privilege requirements.

=item own_host => 1

By default, all nodes use the same PGHOST value.  If specified, generate a
PGHOST specific to this node.  This allows multiple nodes to use the same
port.

=back

For backwards compatibility, it is also exported as a standalone function,
which can only create objects of class C<PostgresNode>.

=cut

sub get_new_node
{
	my $class = 'PostgresNode';
	$class = shift if scalar(@_) % 2 != 1;
	my ($name, %params) = @_;

	# Select a port.
	my $port;
	if (defined $params{port})
	{
		$port = $params{port};
	}
	else
	{
		# When selecting a port, we look for an unassigned TCP port number,
		# even if we intend to use only Unix-domain sockets.  This is clearly
		# necessary on $use_tcp (Windows) configurations, and it seems like a
		# good idea on Unixen as well.
		$port = get_free_port();
	}

	# Select a host.
	my $host = $test_pghost;
	if ($params{own_host})
	{
		if ($use_tcp)
		{
			$last_host_assigned++;
			$last_host_assigned > 254 and BAIL_OUT("too many own_host nodes");
			$host = '127.0.0.' . $last_host_assigned;
		}
		else
		{
			$host = "$test_pghost/$name"; # Assume $name =~ /^[-_a-zA-Z0-9]+$/
			mkdir $host;
		}
	}

	# Lock port number found by creating a new node
	my $node = $class->new($name, $host, $port);

	# Add node to list of nodes
	push(@all_nodes, $node);

	return $node;
}

=pod

=item get_free_port()

Locate an unprivileged (high) TCP port that's not currently bound to
anything.  This is used by get_new_node, and is also exported for use
by test cases that need to start other, non-Postgres servers.

Ports assigned to existing PostgresNode objects are automatically
excluded, even if those servers are not currently running.

XXX A port available now may become unavailable by the time we start
the desired service.

=cut

sub get_free_port
{
	my $found = 0;
	my $port  = $last_port_assigned;

	while ($found == 0)
	{

		# advance $port, wrapping correctly around range end
		$port = 49152 if ++$port >= 65536;
		print "# Checking port $port\n";

		# Check first that candidate port number is not included in
		# the list of already-registered nodes.
		$found = 1;
		foreach my $node (@all_nodes)
		{
			$found = 0 if ($node->port == $port);
		}

		# Check to see if anything else is listening on this TCP port.
		# Seek a port available for all possible listen_addresses values,
		# so callers can harness this port for the widest range of purposes.
		# The 0.0.0.0 test achieves that for post-2006 Cygwin, which
		# automatically sets SO_EXCLUSIVEADDRUSE.  The same holds for MSYS (a
		# Cygwin fork).  Testing 0.0.0.0 is insufficient for Windows native
		# Perl (https://stackoverflow.com/a/14388707), so we also test
		# individual addresses.
		#
		# On non-Linux, non-Windows kernels, binding to 127.0.0/24 addresses
		# other than 127.0.0.1 might fail with EADDRNOTAVAIL.  Binding to
		# 0.0.0.0 is unnecessary on non-Windows systems.
		if ($found == 1)
		{
			foreach my $addr (qw(127.0.0.1),
				$use_tcp ? qw(127.0.0.2 127.0.0.3 0.0.0.0) : ())
			{
				if (!can_bind($addr, $port))
				{
					$found = 0;
					last;
				}
			}
		}
	}

	print "# Found port $port\n";

	# Update port for next time
	$last_port_assigned = $port;

	return $port;
}

# Internal routine to check whether a host:port is available to bind
sub can_bind
{
	my ($host, $port) = @_;
	my $iaddr = inet_aton($host);
	my $paddr = sockaddr_in($port, $iaddr);
	my $proto = getprotobyname("tcp");

	socket(SOCK, PF_INET, SOCK_STREAM, $proto)
	  or die "socket failed: $!";

	# As in postmaster, don't use SO_REUSEADDR on Windows
	setsockopt(SOCK, SOL_SOCKET, SO_REUSEADDR, pack("l", 1))
	  unless $TestLib::windows_os;
	my $ret = bind(SOCK, $paddr) && listen(SOCK, SOMAXCONN);
	close(SOCK);
	return $ret;
}

# Automatically shut down any still-running nodes when the test script exits.
# Note that this just stops the postmasters (in the same order the nodes were
# created in).  Any temporary directories are deleted, in an unspecified
# order, later when the File::Temp objects are destroyed.
END
{
	if ($ENV{'whoami'} and ($ENV{'whoami'} ne 'parent'))
	{
		return $?;
	}
	my $polar_datadir = $ENV{'polar_datadir'};
	TestLib::system_or_bail('pg_verify_checksums', '-D', $polar_datadir)
	  if ($polar_datadir && -e `printf $polar_datadir`);
	# take care not to change the script's exit value
	my $exit_code = $?;

	foreach my $node (@all_nodes)
	{
		$node->teardown_node;

		# skip clean if we are requested to retain the basedir
		next if defined $ENV{'PG_TEST_NOCLEAN'};

		# clean basedir on clean test invocation
		$node->clean_node if $exit_code == 0 && TestLib::all_tests_passing();
	}
	$? = $exit_code;
}

=pod

=item $node->teardown_node()

Do an immediate stop of the node

=cut

sub teardown_node
{
	my $self = shift;

	$self->stop('immediate');
	return;
}

=pod

=item $node->clean_node()

Remove the base directory of the node if the node has been stopped.

=cut

sub clean_node
{
	my $self = shift;

	rmtree $self->{_basedir} unless defined $self->{_pid};
	rmtree $self->{_polar_datadir}
		unless (defined $self->{_pid} || $self->polar_get_node_type eq 'replica');
	return;
}

=pod

=item $node->safe_psql($dbname, $sql) => stdout

Invoke B<psql> to run B<sql> on B<dbname> and return its stdout on success.
Die if the SQL produces an error. Runs with B<ON_ERROR_STOP> set.

Takes optional extra params like timeout and timed_out parameters with the same
options as psql.

=cut

sub safe_psql
{
	my ($self, $dbname, $sql, %params) = @_;

	my ($stdout, $stderr);
	my $ret = $self->psql(
		$dbname, $sql,
		%params,
		stdout        => \$stdout,
		stderr        => \$stderr,
		on_error_die  => 1,
		on_error_stop => 1);

	# psql can emit stderr from NOTICEs etc
	if ($stderr ne "")
	{
		print "#### Begin standard error\n";
		print $stderr;
		print "\n#### End standard error\n";
	}

	return $stdout;
}

=pod

=item $node->psql($dbname, $sql, %params) => psql_retval

Invoke B<psql> to execute B<$sql> on B<$dbname> and return the return value
from B<psql>, which is run with on_error_stop by default so that it will
stop running sql and return 3 if the passed SQL results in an error.

As a convenience, if B<psql> is called in array context it returns an
array containing ($retval, $stdout, $stderr).

psql is invoked in tuples-only unaligned mode with reading of B<.psqlrc>
disabled.  That may be overridden by passing extra psql parameters.

stdout and stderr are transformed to UNIX line endings if on Windows. Any
trailing newline is removed.

Dies on failure to invoke psql but not if psql exits with a nonzero
return code (unless on_error_die specified).

If psql exits because of a signal, an exception is raised.

=over

=item stdout => \$stdout

B<stdout>, if given, must be a scalar reference to which standard output is
written.  If not given, standard output is not redirected and will be printed
unless B<psql> is called in array context, in which case it's captured and
returned.

=item stderr => \$stderr

Same as B<stdout> but gets standard error. If the same scalar is passed for
both B<stdout> and B<stderr> the results may be interleaved unpredictably.

=item on_error_stop => 1

By default, the B<psql> method invokes the B<psql> program with ON_ERROR_STOP=1
set, so SQL execution is stopped at the first error and exit code 2 is
returned.  Set B<on_error_stop> to 0 to ignore errors instead.

=item on_error_die => 0

By default, this method returns psql's result code. Pass on_error_die to
instead die with an informative message.

=item timeout => 'interval'

Set a timeout for the psql call as an interval accepted by B<IPC::Run::timer>
(integer seconds is fine).  This method raises an exception on timeout, unless
the B<timed_out> parameter is also given.

=item timed_out => \$timed_out

If B<timeout> is set and this parameter is given, the scalar it references
is set to true if the psql call times out.

=item extra_params => ['--single-transaction']

If given, it must be an array reference containing additional parameters to B<psql>.

=back

e.g.

	my ($stdout, $stderr, $timed_out);
	my $cmdret = $node->psql('postgres', 'SELECT pg_sleep(60)',
		stdout => \$stdout, stderr => \$stderr,
		timeout => 30, timed_out => \$timed_out,
		extra_params => ['--single-transaction'])

will set $cmdret to undef and $timed_out to a true value.

	$node->psql('postgres', $sql, on_error_die => 1);

dies with an informative message if $sql fails.

=cut

sub psql
{
	my ($self, $dbname, $sql, %params) = @_;

	my $stdout            = $params{stdout};
	my $stderr            = $params{stderr};
	my $timeout           = undef;
	my $timeout_exception = 'psql timed out';
	my @psql_params =
	  ('psql', '-XAtq', '-d', $self->connstr($dbname), '-f', '-');

	# If the caller wants an array and hasn't passed stdout/stderr
	# references, allocate temporary ones to capture them so we
	# can return them. Otherwise we won't redirect them at all.
	if (wantarray)
	{
		if (!defined($stdout))
		{
			my $temp_stdout = "";
			$stdout = \$temp_stdout;
		}
		if (!defined($stderr))
		{
			my $temp_stderr = "";
			$stderr = \$temp_stderr;
		}
	}

	$params{on_error_stop} = 1 unless defined $params{on_error_stop};
	$params{on_error_die}  = 0 unless defined $params{on_error_die};

	push @psql_params, '-v', 'ON_ERROR_STOP=1' if $params{on_error_stop};
	push @psql_params, @{ $params{extra_params} }
	  if defined $params{extra_params};

	$timeout =
	  IPC::Run::timeout($params{timeout}, exception => $timeout_exception)
	  if (defined($params{timeout}));

	${ $params{timed_out} } = 0 if defined $params{timed_out};

	# IPC::Run would otherwise append to existing contents:
	$$stdout = "" if ref($stdout);
	$$stderr = "" if ref($stderr);

	my $ret;
	my $h;
	#try 5 more times after expected error happends.
	my $try_after_expected_error = 5;

	# Run psql and capture any possible exceptions.  If the exception is
	# because of a timeout and the caller requested to handle that, just return
	# and set the flag.  Otherwise, and for any other exception, rethrow.
	#
	# For background, see
	# https://metacpan.org/pod/release/ETHER/Try-Tiny-0.24/lib/Try/Tiny.pm
	try_after_error: do
	{
		local $@;
		eval {
			my @ipcrun_opts = (\@psql_params, '<', \$sql);
			push @ipcrun_opts, '>',  $stdout if defined $stdout;
			push @ipcrun_opts, '2>', $stderr if defined $stderr;
			push @ipcrun_opts, $timeout if defined $timeout;

			IPC::Run::run @ipcrun_opts;
			$ret = $?;
		};
		# try after expected error happends.
		if ($try_after_expected_error > 0 &&
			defined $$stderr &&
			$$stderr =~ m/WARNING:\s+page\s+verification\s+failed.*\n*.*invalid.*page.*in.*block/i)
		{
			print "expected error: $$stderr\n";
			$try_after_expected_error -= 1;
			goto try_after_error;
		}
		my $exc_save = $@;
		if ($exc_save)
		{

			# IPC::Run::run threw an exception. re-throw unless it's a
			# timeout, which we'll handle by testing is_expired
			die $exc_save
			  if (blessed($exc_save)
				|| $exc_save !~ /^\Q$timeout_exception\E/);

			$ret = undef;

			die "Got timeout exception '$exc_save' but timer not expired?!"
			  unless $timeout->is_expired;

			if (defined($params{timed_out}))
			{
				${ $params{timed_out} } = 1;
			}
			else
			{
				die "psql timed out: stderr: '$$stderr'\n"
				  . "while running '@psql_params'";
			}
		}
	};

	# Note: on Windows, IPC::Run seems to convert \r\n to \n in program output
	# if we're using native Perl, but not if we're using MSys Perl.  So do it
	# by hand in the latter case, here and elsewhere.

	if (defined $$stdout)
	{
		$$stdout =~ s/\r\n/\n/g if $Config{osname} eq 'msys';
		chomp $$stdout;
	}

	if (defined $$stderr)
	{
		$$stderr =~ s/\r\n/\n/g if $Config{osname} eq 'msys';
		chomp $$stderr;
	}

	# See http://perldoc.perl.org/perlvar.html#%24CHILD_ERROR
	# We don't use IPC::Run::Simple to limit dependencies.
	#
	# We always die on signal.
	my $core = $ret & 128 ? " (core dumped)" : "";
	die "psql exited with signal "
	  . ($ret & 127)
	  . "$core: '$$stderr' while running '@psql_params'"
	  if $ret & 127;
	$ret = $ret >> 8;

	if ($ret && $params{on_error_die})
	{
		die "psql error: stderr: '$$stderr'\nwhile running '@psql_params'"
		  if $ret == 1;
		die "connection error: '$$stderr'\nwhile running '@psql_params'"
		  if $ret == 2;
		die
		  "error running SQL: '$$stderr'\nwhile running '@psql_params' with sql '$sql'"
		  if $ret == 3;
		die "psql returns $ret: '$$stderr'\nwhile running '@psql_params'";
	}

	if (wantarray)
	{
		return ($ret, $$stdout, $$stderr);
	}
	else
	{
		return $ret;
	}
}

=pod
	create psql command stored in $sql.
=cut

sub sql_cmd
{
	my ($self, $opts, $dbname, $sql, %params) = @_;

	my $stdout            = $params{stdout};
	my $stderr            = $params{stderr};
	my $timeout           = undef;
	my $timeout_exception = 'psql timed out';
	my @psql_params =
	  ('psql', '-XAtq', '-d', $self->connstr($dbname), '-f', '-');

	# If the caller wants an array and hasn't passed stdout/stderr
	# references, allocate temporary ones to capture them so we
	# can return them. Otherwise we won't redirect them at all.
	if (wantarray)
	{
		if (!defined($stdout))
		{
			my $temp_stdout = "";
			$stdout = \$temp_stdout;
		}
		if (!defined($stderr))
		{
			my $temp_stderr = "";
			$stderr = \$temp_stderr;
		}
	}

	$params{on_error_stop} = 1 unless defined $params{on_error_stop};
	$params{on_error_die}  = 0 unless defined $params{on_error_die};

	push @psql_params, '-v', 'ON_ERROR_STOP=1' if $params{on_error_stop};
	push @psql_params, @{ $params{extra_params} }
	  if defined $params{extra_params};

	$timeout =
	  IPC::Run::timeout($params{timeout}, exception => $timeout_exception)
	  if (defined($params{timeout}));

	${ $params{timed_out} } = 0 if defined $params{timed_out};

	# IPC::Run would otherwise append to existing contents:
	$$stdout = "" if ref($stdout);
	$$stderr = "" if ref($stderr);

	@$opts = (\@psql_params, '<', \$sql);
	push @$opts, '>',  $stdout if defined $stdout;
	push @$opts, '2>', $stderr if defined $stderr;
	push @$opts, $timeout if defined $timeout;

	if (defined $$stdout)
	{
		chomp $$stdout;
		$$stdout =~ s/\r//g if $TestLib::windows_os;
	}

	if (defined $$stderr)
	{
		chomp $$stderr;
		$$stderr =~ s/\r//g if $TestLib::windows_os;
	}

}

=pod
	connect postgresql server via psql
=cut

sub psql_connect
{
	my ($self, $database, $timeout) = @_;
	my $host = $self->host;
	my $port = $self->port;
	my $cmd = "psql -XAtq --set=SORT_RESULT=on -h ".$host." -p ".$port." -d ".$database;
	my $psql = Expect->new();
	$psql->log_stdout(0);
	$psql->spawn($cmd) or die "psql can not connect to postgresql server.\n";
	$psql->expect($timeout, $database) or die "timeout!\n";
	if ($psql->match)
	{
		print "psql connect SUCC!\n";
		$psql->send_slow(0, "\\pset pager 0\n");
		$psql->clear_accum();
		return $psql;
	}
	return undef;
}

=pod
	execute SQLs via psql.
=cut

sub psql_execute
{
	my($self, $sql, $timeout, $psql) = @_;
	$psql->clear_accum();
	my $file = "/tmp/$$.sql";
	open(my $INPUT, "+>$file") or return "file $file open failed!";
	my $append = ";\\echo One-Job-Done-for-CDC.\n";
	$sql = $sql.$append;
	print $INPUT $sql;
	close($INPUT);
	my $filter = qr/\nOne-Job-Done-for-CDC\.\s/;
	my $output = "";
	my $execute_sql = "\\i $file;\n";
	$psql->send($execute_sql);
	$psql->expect($timeout, -re=>$filter) or return "Timeout!";
	if ($psql->match)
	{
		$output .= $psql->before.$psql->match;
	}
	$psql->clear_accum();
	my @output_array = split(//, $output);
	$output = "";
	foreach my $i (0..$#output_array)
	{
		if ($output_array[$i] eq "\n" or ($output_array[$i] ge "\x20" and $output_array[$i] le "\x7E"))
		{
			$output .= $output_array[$i];
		}
	}
	unlink($file);
	my $start = rindex($output, $execute_sql);
	my $length = length($output);
	if ($start < 0 or $start >= $length)
	{
		return "\n";
	}
	else
	{
		return substr($output, $start, $length)."\n";
	}
}
=pod
	disconnect postgresql server
=cut

sub psql_close
{
	my ($self, $psql) = @_;
	if ($psql)
	{
		$psql->send("\\q\n");
		$psql->soft_close();
	}
	print "psql close SUCC!\n";
}

=pod

=item $node->poll_query_until($dbname, $query [, $expected, $max_attempts])

Run B<$query> repeatedly, until it returns the B<$expected> result
('t', or SQL boolean true, by default).
Continues polling if B<psql> returns an error result.
Times out after 180 seconds.
Returns 1 if successful, 0 if timed out.

=cut

sub poll_query_until
{
	my ($self, $dbname, $query, $expected, $max_attempts) = @_;

	$expected = 't' unless defined($expected);    # default value
	$max_attempts = defined($max_attempts) ? $max_attempts * 10 : 180 * 10;

	my $cmd = [ 'psql', '-XAt', '-c', $query, '-d', $self->connstr($dbname) ];
	my ($stdout, $stderr);
	my $attempts     = 0;
	while ($attempts < $max_attempts)
	{
		my $result = IPC::Run::run $cmd, '>', \$stdout, '2>', \$stderr;

		$stdout =~ s/\r\n/\n/g if $Config{osname} eq 'msys';
		chomp($stdout);

		if ($stdout eq $expected)
		{
			return 1;
		}

		# Wait 0.1 second before retrying.
		usleep(100_000);

		$attempts++;
	}

	# The query result didn't change in 180 seconds. Give up. Print the
	# output from the last attempt, hopefully that's useful for debugging.
	$stderr =~ s/\r\n/\n/g if $Config{osname} eq 'msys';
	chomp($stderr);
	diag qq(poll_query_until timed out executing this query:
$query
expecting this output:
$expected
last actual query output:
$stdout
with stderr:
$stderr);
	return 0;
}

=pod

=item $node->command_ok(...)

Runs a shell command like TestLib::command_ok, but with PGHOST and PGPORT set
so that the command will default to connecting to this PostgresNode.

=cut

sub command_ok
{
	my $self = shift;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	TestLib::command_ok(@_);
	return;
}

=pod

=item $node->command_fails(...)

TestLib::command_fails with our connection parameters. See command_ok(...)

=cut

sub command_fails
{
	my $self = shift;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	TestLib::command_fails(@_);
	return;
}

=pod

=item $node->command_like(...)

TestLib::command_like with our connection parameters. See command_ok(...)

=cut

sub command_like
{
	my $self = shift;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	TestLib::command_like(@_);
	return;
}

=pod

=item $node->command_checks_all(...)

TestLib::command_checks_all with our connection parameters. See
command_ok(...)

=cut

sub command_checks_all
{
	my $self = shift;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	TestLib::command_checks_all(@_);
	return;
}

=pod

=item $node->issues_sql_like(cmd, expected_sql, test_name)

Run a command on the node, then verify that $expected_sql appears in the
server log file.

Reads the whole log file so be careful when working with large log outputs.
The log file is truncated prior to running the command, however.

=cut

sub issues_sql_like
{
	my ($self, $cmd, $expected_sql, $test_name) = @_;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	truncate $self->logfile, 0;
	my $result = TestLib::run_log($cmd);
	ok($result, "@$cmd exit code 0");
	my $log = TestLib::slurp_file($self->logfile);
	like($log, $expected_sql, "$test_name: SQL found in server log");
	return;
}

=pod

=item $node->run_log(...)

Runs a shell command like TestLib::run_log, but with connection parameters set
so that the command will default to connecting to this PostgresNode.

=cut

sub run_log
{
	my $self = shift;

	local $ENV{PGHOST} = $self->host;
	local $ENV{PGPORT} = $self->port;

	TestLib::run_log(@_);
	return;
}

=pod

=item $node->lsn(mode)

Look up WAL locations on the server:

 * insert location (master only, error on replica)
 * write location (master only, error on replica)
 * flush location (master only, error on replica)
 * receive location (always undef on master)
 * replay location (always undef on master)

mode must be specified.

=cut

sub lsn
{
	my ($self, $mode) = @_;
	my %modes = (
		'insert'  => 'pg_current_wal_insert_lsn()',
		'flush'   => 'pg_current_wal_flush_lsn()',
		'write'   => 'pg_current_wal_lsn()',
		'receive' => 'pg_last_wal_receive_lsn()',
		'replay'  => 'pg_last_wal_replay_lsn()');

	$mode = '<undef>' if !defined($mode);
	croak "unknown mode for 'lsn': '$mode', valid modes are "
	  . join(', ', keys %modes)
	  if !defined($modes{$mode});

	my $result = $self->safe_psql('postgres', "SELECT $modes{$mode}");
	chomp($result);
	if ($result eq '')
	{
		return;
	}
	else
	{
		return $result;
	}
}

=pod

=item $node->wait_for_startup($wait_timeout, $readonly)
Wait for the node finish startup process and enter pm_run state

=cut

sub polar_wait_for_startup
{
	my ($self, $wait_timeout, $readonly) = @_;
	$readonly = defined($readonly) ? $readonly : 1;
	my $result = 0;
	my ($stdout, $stderr);
	for (my $i = 0; $i < $wait_timeout && $result != 1; $i = $i + 1)
	{
		if ($readonly == 1)
		{
			$self->psql('postgres', qq[SELECT 1], 
					 stdout        => \$result,
					 on_error_die  => 0, on_error_stop => 0);
		}
		else
		{
			$result = $self->psql('postgres', qq[CREATE VIEW test_view AS SELECT 1],
					 stdout	=> \$stdout, stderr => \$stderr,
					 on_error_die  => 0, on_error_stop => 0);
			print "result: $result, stderr:$stderr\n";
			$result = (($result == 0 && $stderr eq "")? 1 : -1);
		}
		sleep 1;
	}
	if ($result == 1 && $readonly != 1)
	{
		$self->psql('postgres', qq[DROP VIEW test_view],
					 stdout => \$stdout, stderr => \$stderr,
				     on_error_die  => 0, on_error_stop => 1);
	}
	print "startup result: $result\n";
	return $result;
}

=pod

=item $node->wait_for_catchup(standby_name, mode, target_lsn, $return_failed, $expected_res, $timeout))

Wait for the node with application_name standby_name (usually from node->name,
also works for logical subscriptions)
until its replication location in pg_stat_replication equals or passes the
upstream's WAL insert point at the time this function is called. By default
the replay_lsn is waited for, but 'mode' may be specified to wait for any of
sent|write|flush|replay. The connection catching up must be in a streaming
state.

If there is no active replication connection from this peer, waits until
poll_query_until timeout.

Requires that the 'postgres' db exists and is accessible.

target_lsn may be any arbitrary lsn, but is typically $master_node->lsn('insert').
If omitted, pg_current_wal_lsn() is used.

$expected_res is the expected result of the query, default t

$timeout set the timeout of poll_query_until function

This is not a test. It die()s on failure.

=cut

sub wait_for_catchup
{
	my ($self, $standby_name, $mode, $target_lsn, $return_failed, $expected_res, $timeout) = @_;
	$return_failed ||= 0;
	$mode = defined($mode) ? $mode : 'replay';
	$expected_res = 't' unless defined($expected_res);    # default value
	$timeout = defined($timeout) ? $timeout : 180; #default value, the same as poll_query_until
	my %valid_modes =
	  ('sent' => 1, 'write' => 1, 'flush' => 1, 'replay' => 1);
	croak "unknown mode $mode for 'wait_for_catchup', valid modes are "
	  . join(', ', keys(%valid_modes))
	  unless exists($valid_modes{$mode});

	# Allow passing of a PostgresNode instance as shorthand
	if (blessed($standby_name) && $standby_name->isa("PostgresNode"))
	{
		$standby_name = $standby_name->name;
	}
	my $lsn_expr;
	if (defined($target_lsn))
	{
		$lsn_expr = "'$target_lsn'";
	}
	else
	{
		$lsn_expr = 'pg_current_wal_lsn()';
	}
	print "Waiting for replication conn "
	  . $standby_name . "'s "
	  . $mode
	  . "_lsn to pass "
	  . $lsn_expr . " on "
	  . $self->name . "\n";
	my $query =
	  qq[SELECT $lsn_expr <= ${mode}_lsn AND state = 'streaming' FROM pg_catalog.pg_stat_replication WHERE application_name = '$standby_name';];
	if (!$self->poll_query_until('postgres', $query, $expected_res, $timeout))
	{
		if (!$return_failed)
		{
			croak "timed out waiting for catchup";
		}
		else
		{
			print "timed out waiting for catchup\n";
			return 0;
		}
	}
	print "done\n";
	return 1;
}

=pod

=item $node->wait_for_slot_catchup(slot_name, mode, target_lsn)

Wait for the named replication slot to equal or pass the supplied target_lsn.
The location used is the restart_lsn unless mode is given, in which case it may
be 'restart' or 'confirmed_flush'.

Requires that the 'postgres' db exists and is accessible.

This is not a test. It die()s on failure.

If the slot is not active, will time out after poll_query_until's timeout.

target_lsn may be any arbitrary lsn, but is typically $master_node->lsn('insert').

Note that for logical slots, restart_lsn is held down by the oldest in-progress tx.

=cut

sub wait_for_slot_catchup
{
	my ($self, $slot_name, $mode, $target_lsn) = @_;
	$mode = defined($mode) ? $mode : 'restart';
	if (!($mode eq 'restart' || $mode eq 'confirmed_flush'))
	{
		croak "valid modes are restart, confirmed_flush";
	}
	croak 'target lsn must be specified' unless defined($target_lsn);
	print "Waiting for replication slot "
	  . $slot_name . "'s "
	  . $mode
	  . "_lsn to pass "
	  . $target_lsn . " on "
	  . $self->name . "\n";
	my $query =
	  qq[SELECT '$target_lsn' <= ${mode}_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '$slot_name';];
	$self->poll_query_until('postgres', $query)
	  or croak "timed out waiting for catchup";
	print "done\n";
	return;
}

=pod

=item $node->query_hash($dbname, $query, @columns)

Execute $query on $dbname, replacing any appearance of the string __COLUMNS__
within the query with a comma-separated list of @columns.

If __COLUMNS__ does not appear in the query, its result columns must EXACTLY
match the order and number (but not necessarily alias) of supplied @columns.

The query must return zero or one rows.

Return a hash-ref representation of the results of the query, with any empty
or null results as defined keys with an empty-string value. There is no way
to differentiate between null and empty-string result fields.

If the query returns zero rows, return a hash with all columns empty. There
is no way to differentiate between zero rows returned and a row with only
null columns.

=cut

sub query_hash
{
	my ($self, $dbname, $query, @columns) = @_;
	croak 'calls in array context for multi-row results not supported yet'
	  if (wantarray);

	# Replace __COLUMNS__ if found
	substr($query, index($query, '__COLUMNS__'), length('__COLUMNS__')) =
	  join(', ', @columns)
	  if index($query, '__COLUMNS__') >= 0;
	my $result = $self->safe_psql($dbname, $query);

	# hash slice, see http://stackoverflow.com/a/16755894/398670 .
	#
	# Fills the hash with empty strings produced by x-operator element
	# duplication if result is an empty row
	#
	my %val;
	@val{@columns} =
	  $result ne '' ? split(qr/\|/, $result, -1) : ('',) x scalar(@columns);
	return \%val;
}

=pod

=item $node->slot(slot_name)

Return hash-ref of replication slot data for the named slot, or a hash-ref with
all values '' if not found. Does not differentiate between null and empty string
for fields, no field is ever undef.

The restart_lsn and confirmed_flush_lsn fields are returned verbatim, and also
as a 2-list of [highword, lowword] integer. Since we rely on Perl 5.8.8 we can't
"use bigint", it's from 5.20, and we can't assume we have Math::Bigint from CPAN
either.

=cut

sub slot
{
	my ($self, $slot_name) = @_;
	my @columns = (
		'plugin', 'slot_type',  'datoid', 'database',
		'active', 'active_pid', 'xmin',   'catalog_xmin',
		'restart_lsn');
	return $self->query_hash(
		'postgres',
		"SELECT __COLUMNS__ FROM pg_catalog.pg_replication_slots WHERE slot_name = '$slot_name'",
		@columns);
}

=pod

=item $node->pg_recvlogical_upto(self, dbname, slot_name, endpos, timeout_secs, ...)

Invoke pg_recvlogical to read from slot_name on dbname until LSN endpos, which
corresponds to pg_recvlogical --endpos.  Gives up after timeout (if nonzero).

Disallows pg_recvlogical from internally retrying on error by passing --no-loop.

Plugin options are passed as additional keyword arguments.

If called in scalar context, returns stdout, and die()s on timeout or nonzero return.

If called in array context, returns a tuple of (retval, stdout, stderr, timeout).
timeout is the IPC::Run::Timeout object whose is_expired method can be tested
to check for timeout. retval is undef on timeout.

=cut

sub pg_recvlogical_upto
{
	my ($self, $dbname, $slot_name, $endpos, $timeout_secs, %plugin_options)
	  = @_;
	my ($stdout, $stderr);

	my $timeout_exception = 'pg_recvlogical timed out';

	croak 'slot name must be specified' unless defined($slot_name);
	croak 'endpos must be specified'    unless defined($endpos);

	my @cmd = (
		'pg_recvlogical', '-S', $slot_name, '--dbname',
		$self->connstr($dbname));
	push @cmd, '--endpos', $endpos;
	push @cmd, '-f', '-', '--no-loop', '--start';

	while (my ($k, $v) = each %plugin_options)
	{
		croak "= is not permitted to appear in replication option name"
		  if ($k =~ qr/=/);
		push @cmd, "-o", "$k=$v";
	}

	my $timeout;
	$timeout =
	  IPC::Run::timeout($timeout_secs, exception => $timeout_exception)
	  if $timeout_secs;
	my $ret = 0;

	do
	{
		local $@;
		eval {
			IPC::Run::run(\@cmd, ">", \$stdout, "2>", \$stderr, $timeout);
			$ret = $?;
		};
		my $exc_save = $@;
		if ($exc_save)
		{

			# IPC::Run::run threw an exception. re-throw unless it's a
			# timeout, which we'll handle by testing is_expired
			die $exc_save
			  if (blessed($exc_save) || $exc_save !~ qr/$timeout_exception/);

			$ret = undef;

			die "Got timeout exception '$exc_save' but timer not expired?!"
			  unless $timeout->is_expired;

			die
			  "$exc_save waiting for endpos $endpos with stdout '$stdout', stderr '$stderr'"
			  unless wantarray;
		}
	};

	$stdout =~ s/\r\n/\n/g if $Config{osname} eq 'msys';
	$stderr =~ s/\r\n/\n/g if $Config{osname} eq 'msys';

	if (wantarray)
	{
		return ($ret, $stdout, $stderr, $timeout);
	}
	else
	{
		die
		  "pg_recvlogical exited with code '$ret', stdout '$stdout' and stderr '$stderr'"
		  if $ret;
		return $stdout;
	}
}

=pod

=back

=cut

1;
