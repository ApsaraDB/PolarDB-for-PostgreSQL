package PolarRegression;

use strict;
use warnings;
use File::Basename;
use Test::More;
use PostgresNode;
use TestLib ();

use IO::Pipe;
use Time::HiRes qw( gettimeofday tv_interval time );
use POSIX qw(strftime);

our @EXPORT = qw(
	create_new_test
);

our (@asynchronous_sql, @asynchronous_executor, @asynchronous_stdout_exp, @asynchronous_stderr_exp);

sub new
{
	my ($class, $node_master) = @_;
	my $regress_db = 'polar_regression';
	my $testname = basename($0);
	$testname =~ s/\.[^.]+$//;
	my $test_log_path = $TestLib::tmp_check."/log/$testname";
	mkdir $test_log_path;

	my $self = {
		_regress_db => $regress_db,
		_node_master => $node_master,
		_test_log_path => $test_log_path,
		# execute some random pg_control commands between different test cases.
		# randome pg_control command could be registered by register_random_pg_control.
		_random_pg_control => '',
		_mode => 'default',
		_enable_random_flashback => 0,
	};

	$node_master->safe_psql('postgres', 'CREATE DATABASE '.$regress_db, timeout=>600);
	$node_master->safe_psql($regress_db, 'CREATE extension polar_monitor', timeout=>10);
	$node_master->safe_psql($regress_db, 'CREATE TABLE replayed(val integer unique);');
	# Create a function to copy random rel for flashback test
	$node_master->safe_psql($regress_db, "CREATE OR REPLACE FUNCTION fb_copy_a_random_rel() RETURNS TEXT LANGUAGE plpgsql AS \$\$ DECLARE random_rel TEXT; BEGIN SELECT (n.nspname)::text || '.' || (c.relname)::text into random_rel FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast' AND relispartition = 'f' AND relpersistence = 'p' AND reltoastrelid = 0 AND relhassubclass = 'f' AND pg_catalog.pg_table_is_visible(c.oid) order by random() limit 1; execute 'drop table if exists ' || random_rel || '_fbnew'; execute 'create table ' || random_rel || '_fbnew as select * from ' || random_rel; RETURN random_rel; END \$\$;");
	
	bless $self, $class;

	return $self;
}
sub regress_db
{
	my ($self) = @_;

	return $self->{_regress_db};
}

sub node_master
{
	my ($self) = @_;

	return $self->{_node_master};
}

sub test_log_path
{
	my ($self) = @_;

	return $self->{_test_log_path};
}

sub set_test_mode {
	my ($self, $mode) = @_;
	$self->{_mode} = $mode;
}

sub get_test_mode {
	my ($self) = @_;
	return $self->{_mode};
}

sub register_random_pg_control
{
	my ($self, $pg_control) = @_;
	if ($self->{_random_pg_control} eq '')
	{
		$self->{_random_pg_control} = "$pg_control";
	}
	else
	{
		$self->{_random_pg_control} = $self->{_random_pg_control}.";$pg_control";
	}
}

sub enable_random_flashback
{
	my ($self) = @_;
	$self->{_enable_random_flashback} = 1;
}

sub fb_copy_a_random_rel
{
	my ($self) = @_;
	
	my $node_master = $self->node_master;
	my $random_rel = $node_master->safe_psql($self->regress_db, "select * from fb_copy_a_random_rel()");
	
	return $random_rel;
}

sub get_rel_oid
{
	my ($self, $rel) = @_;
	
	my $relid = 0;
	my $node_master = $self->node_master;
	$relid = $node_master->safe_psql($self->regress_db, "select '$rel'::regclass::oid");
	return $relid;
}

sub fb_rel_and_check
{
	my ($self, $old_rel, $old_rel_oid, $flashback_time) = @_;
	
	my $new_rel = $old_rel."_fbnew";
	my $flashback_rel = '';
	my $rel_exist = 0;
	my $failed = 0;
	
	$rel_exist = $self->node_master->safe_psql($self->regress_db, "select count(1) from pg_class where oid = $old_rel_oid");
		
	if ($rel_exist)
	{
		$self->node_master->safe_psql($self->regress_db,
		"flashback table $old_rel to timestamp '$flashback_time'"
		);
		
		$flashback_rel = 'polar_flashback_'.$old_rel_oid;
		
		# Just count(1) to be quick.
		
		my $count = $self->node_master->safe_psql($self->regress_db,
		"select count(1) from $new_rel"
		);
		
		my $right_count = $self->node_master->safe_psql($self->regress_db,
		"select count(1) from $flashback_rel"
		);
		
		if ($right_count != $count)
		{
			print "flashback: The count in the ".$new_rel." is:".$count.", but is:".$right_count." in the ".$flashback_rel."\n";
			$failed = 1;
		}
	}
	
	return $failed;
}

sub fb_drop_copy_rel
{
	my ($self, $old_rel, $old_rel_oid) = @_;
	
	my $new_rel = $old_rel."_fbnew";
	my $flashback_rel = 'polar_flashback_'.$old_rel_oid;
	
	$self->node_master->safe_psql($self->regress_db,
		"drop table IF EXISTS $new_rel");
		
	$self->node_master->safe_psql($self->regress_db,
		"drop table IF EXISTS $flashback_rel");
}


sub get_random_pg_control
{
	my ($self) = @_;
	return $self->{_random_pg_control};
}

sub get_one_random_pg_control
{
	my ($self) = @_;
	my @pg_control_array = split(';', $self->{_random_pg_control});
	print "the length of pg_control is $#pg_control_array\n";
	srand;
	my $index = int(rand(@pg_control_array));
	print "get index: $index\n";
	return $pg_control_array[$index];
}

sub replay_check
{
	my ($self, $node_replica) = @_;
	my $node_master = $self->node_master;
	my $regress_db = $self->regress_db;
	my $root_node = $node_replica->polar_get_root_node;

	my $newval = $node_master->safe_psql($regress_db,
		'INSERT INTO replayed(val) SELECT coalesce(max(val),0) + 1 AS newval FROM replayed RETURNING val'
	);

	if (!$root_node->wait_for_catchup($node_replica, 'replay',
				$node_master->lsn('insert'), 1))
	{
		return 0;
	}

	if (!$node_replica->safe_psql($regress_db,
			qq[SELECT 1 FROM replayed WHERE val = $newval]))
	{
		print $node_replica->name." didn't replay master value $newval\n";
		return 0;
	}

	return 1;
}

sub replay_check_all
{
	my ($self, $node_replica_ref, $table) = @_;
	my $node_master = $self->node_master;
	my $regress_db = $self->regress_db;
	my $start_time = [gettimeofday];
	my $interval;

	if (!$table or $table eq '')
	{
		$table = "replayed";
	}

	# RO or standby's replay lsn points to end+1 of the last record successfully
	# replayed. But RW's insert lsn points to the begining position of the next
	# avaliable WAL record, excluding all WAL page headers. So the replay lsn of RO
	# or standby may be lower than insert lsn of RW, which could make wait_for_catchup
	# failed for timeout. Current solution is inserting one more data to push forward
	# replay lsn of RO or standby.
	my $newval = $node_master->safe_psql($regress_db,
		"INSERT INTO $table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM $table RETURNING val;"
	);
	my $insert_lsn = $node_master->lsn('insert');
	$node_master->safe_psql($regress_db,
		"INSERT INTO $table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM $table RETURNING val;"
	);
	$interval=tv_interval($start_time,[gettimeofday]);
	print "replay_check_all: Inserting two values take $interval seconds.\n";
	$start_time = [gettimeofday];

	foreach my $node_replica (@{$node_replica_ref})
	{
		my $root_node = $node_replica->polar_get_root_node;
		if ($root_node->polar_get_node_type eq "standby")
		{
			$root_node->safe_psql($regress_db, qq[checkpoint], timeout=>600);
			print "Checkpoint on ".$root_node->name."\n";
		}
		if (!$root_node->wait_for_catchup($node_replica, 'replay', $insert_lsn, 1))
		{
			return 0;
		}
		if (!$node_replica->safe_psql($regress_db,
				qq[SELECT 1 FROM $table WHERE val = $newval]))
		{
			print $node_replica->name." didn't replay master value $newval\n";
			return 0;
		}
		$interval=tv_interval($start_time,[gettimeofday]);
		print "replay_check_all: Replay check for '".$node_replica->name."' take $interval seconds.\n";
		$start_time = [gettimeofday];
	}
	return 1;
}

=pod
=item $self->wait_for_all_node($node_replica_ref)
Wait for all nodes to be ready to accept connections.
=cut

sub wait_for_all_node
{
	my ($self, $node_replica_ref) = @_;
	my $node_master = $self->node_master;
	my $wait_time = 30;

	$node_master->polar_wait_for_startup($wait_time);
	foreach my $node_replica (@{$node_replica_ref})
	{
		$node_replica->polar_wait_for_startup($wait_time);
	}
}

=pod
=item $self->shutdown_all_nodes($node_all_ref)
Shutdown all nodes in a specific order which RO nodes will be shutdown at the end.
=cut

sub shutdown_all_nodes
{
	my ($self, $node_all_ref) = @_;
	foreach my $node (@{$node_all_ref})
	{
		if ($node->polar_get_node_type ne "replica")
		{
			$node->stop;
		}
	}
	foreach my $node (@{$node_all_ref})
	{
		if ($node->polar_get_node_type eq "replica")
		{
			$node->stop;
		}
	}
}

sub master_repli_cmp
{
	my ($sql) = @_;
	$sql =~ s/^\s+//;
	if ($sql =~ /^INSERT/i or $sql =~ /^UPDATE/i or $sql =~ /^CREATE/i
		or $sql =~ /^DELETE/i or $sql =~ /^DROP/i or $sql =~ /^ALTER/i
		or $sql =~ /^NOTIFY/i or $sql =~ /^TRUNCATE/i or $sql =~ /^COMMENT/i
		or $sql =~ /^PREPARE TRANSACTION/i or $sql =~ /^COMMIT PREPARED/i
		or $sql =~ /^ROLLBACK PREPARED/i or $sql =~ /^LISTEN/i
		or $sql =~ /^REINDEX/i or $sql =~ /^(\\){0,1}COPY.*(\s+FROM\s+)+(?![^\(]*\))/i or $sql =~ /^GRANT/i
		or $sql =~ /^REASSIGN/i or $sql =~ /^ANALYZE/i or $sql =~ /^CLUSTER/i
		or $sql =~ /^ALTER/i or $sql =~ /^REVOKE/i
		or $sql =~ /^VACUUM/i or $sql =~ /^SELECT.+brin_summarize_new_values/i
		or $sql =~ /^REFRESH MATERIALIZED VIEW/i or $sql =~ /^IMPORT/i
		or $sql =~ /^SELECT.+(brin_summarize_range|brin_desummarize_range)/i
		or $sql =~ /^SELECT.+pg_switch_wal/i
		or $sql =~ /^select.+gin_clean_pending_list/i
		or $sql =~ /^select.+txid_current\(/i or $sql =~ /^select.+(nextval|currval|setval)/i
		or $sql =~ /^select.+txid_status/i
		or $sql =~ /^select.+pg_notify/i
		or $sql =~ /(^select.+|^\\)(lo_create|lo_unlink|lowrite|lo_truncate|lo_put|lo_import|lo_from_bytea)/i
		or $sql =~ /^select.+for\s+(update|share)\s*;/i
		or $sql =~ /^select(?!\s+format).*\s+into\s+/i
		or $sql =~ /^with(.+?)(insert|update|delete)/si
		)
	{
		return 'REPLICA_ERR';
	}

	return 'EQUAL';
}

sub read_sql
{
	my ($fh, $line_num, $polar_tag, $mode) = @_;
	my $sql = "";
	my $executor = "both";
	my $stdout_exp = "";
	my $stderr_exp = "";
	my $read_stdin = qr/.*COPY.+FROM STDIN/i;
	my $read_stdout = qr/.*COPY.+FROM STDOUT/i;
	my $psql_if = qr/^\\IF/i;
	my $psql_endif = qr/^\\ENDIF/i;
	my $symbol_end_tag = qr/(^\\.+)|(^(?!--).*;)/;
	my $copy_end_tag = qr/(^\\\.$)|(^-- POLAR_END_COPY)/;
	my $func_start_tag = qr/^\s*CREATE\s*(OR\s+REPLACE){0,1} FUNCTION/i;
	my $func_end_tag = qr/^-- POLAR_END_FUNC/;
	my $proc_start_tag = qr/^\s*CREATE\s*(OR\s+REPLACE){0,1} PROCEDURE/i;
	my $proc_end_tag = qr/^-- POLAR_END_PROC/;
	my $do_start_tag = qr/^\s*DO.*\$.*\$/i;
	my $do_end_tag = qr/^-- POLAR_END_DO/;
	my @trans_end_tags = (qr/^\s*END(\s+TRANSACTION|\s+WORK){0,1}\s*;/i,
						  qr/^\s*COMMIT(\s+TRANSACTION|\s+WORK){0,1}\s*;/i,
						  qr/^\s*ROLLBACK(\s+TRANSACTION|\s+WORK){0,1}\s*;/i,
						  qr/^\s*PREPARE\s+TRANSACTION.+;/i,
						  qr/^\s*ABORT(\s+TRANSACTION|\s+WORK){0,1}\s*;/i);
	my $trans_start_tag = qr/^\s*(START\s+TRANSACTION)|BEGIN(\s*\\;|\s*;|\s+TRANSACTION|\s+WORK|\s+ISOLATION)/i;
	my $polar_action_flag = qr/--\s*POLAR_TAG:/i;
	my $polar_session_begin = qr/--\s*POLAR_SESSION_BEGIN/i;
	my $polar_session_end = qr/--\s*POLAR_SESSION_END/i;
	my $polar_asynchronous_begin = qr/--\s*POLAR_ASYNCHRONOUS_BEGIN/i;
	my $polar_asynchronous_end = qr/--\s*POLAR_ASYNCHRONOUS_END/i;
	my $polar_result_stdout = qr/--\s*POLAR_RESULT_STDOUT:/i;
	my $polar_result_stderr = qr/--\s*POLAR_RESULT_STDERR:/i;
	my $comment_tag = qr/^(--|\/\*| \*)/;
	my $end_tag = $symbol_end_tag;
	my $trans = 0;
	my $session = 0;

	$$polar_tag = '';

	LOOP_LINE:
	{
		while (my $line = <$fh>)
		{
			$$line_num++;
			if ($line =~ $comment_tag)
			{
				if ($line =~ $polar_action_flag)
				{
					$$polar_tag = $line;
					$$polar_tag =~ s/--\s*POLAR_TAG://;
					$$polar_tag =~ s/^\s+|\s+$//g;
				}
				elsif ($line =~ $polar_session_begin)
				{
					$session = 1;
					if (!$$polar_tag)
					{
						$$polar_tag = 'EQUAL';
					}
					if (index($', "replica") > -1)
					{
						$executor = "replica";
					}
					elsif (index($', "master") > -1)
					{
						$executor = "master";
					}
					$end_tag = $polar_session_end;
				}
				elsif ($line =~ $polar_asynchronous_begin)
				{
					$$mode = "asynchronous";
					next;
				}
				elsif ($line =~ $polar_asynchronous_end)
				{
					$sql .= $line;
					last LOOP_LINE;
				}
				elsif ($line =~ $polar_result_stdout)
				{
					$stdout_exp = $'; #get char after pattern.
					$stdout_exp =~ s/\s//g;
				}
				elsif ($line =~ $polar_result_stderr)
				{
					$stderr_exp = $'; #get char after pattern.
					$stderr_exp =~ s/\s//g;
				}
			}
			else
			{
				$sql .= $line;
			}

			if ($trans == 0 && $session == 0)
			{
				if ($line =~ $read_stdin or $line =~ $read_stdout)
				{
					$end_tag = $copy_end_tag;
				}
				elsif ($line =~ $func_start_tag)
				{
					$end_tag = $func_end_tag;
				}
				elsif ($line =~ $do_start_tag)
				{
					$end_tag = $do_end_tag;
				}
				elsif ($line =~ $proc_start_tag)
				{
					$end_tag = $proc_end_tag;
				}
				elsif ($line =~ $psql_if)
				{
					$end_tag = $psql_endif;
				}
				elsif ($line =~ $trans_start_tag)
				{
					$trans = 1;
				}
			}
			my $exit = 0;
			if ($trans)
			{
				foreach $end_tag(@trans_end_tags)
				{
					if ($line =~ $end_tag)
					{
						if (!$$polar_tag)
						{
							$$polar_tag = 'REPLICA_ERR';
						}
						$exit = 1;
						last;
					}
				}
			}
			elsif ($line =~ $end_tag)
			{
				$exit = 1;
			}
			if ($exit)
			{
				if ($$mode eq 'asynchronous')
				{
					push @asynchronous_sql, $sql;
					push @asynchronous_executor, $executor;
					push @asynchronous_stdout_exp, $stdout_exp;
					push @asynchronous_stderr_exp, $stderr_exp;
					$sql = "";
					$executor = "both";#default both could execute.
					$stdout_exp = "";
					$stderr_exp = "";
					next;
				}
				else
				{
					last LOOP_LINE;
				}
			}
		}
	}

	if (!$$polar_tag)
	{
		$$polar_tag = master_repli_cmp($sql);
	}
	return $sql;
}

sub convert_sourcefile
{
	my ($self) = @_;
	my $pwd = $ENV{'PWD'};
	my $source_dir = $pwd."/source/*.source";
	my @source_files = glob($source_dir);
	mkdir $pwd."/result";
	while (my $file = <@source_files>)
	{
		my $source_path = $file;
		$file =~ s/source/sql/g;
		my $sqlfile_path = $file;
		unlink($sqlfile_path);
		open(my $sqlfile, ">$sqlfile_path") or die "$sqlfile_path can not be open!\n";
		open(my $source,  "<$source_path") or die "$source_path can not be open!\n";
		while (<$source>)
		{
			my $con = $_;
			$con =~ s/\@PWD\@/$pwd/g;
			print $sqlfile $con;
		}
		close ($sqlfile);
		close ($source);
	}
}

=pod
=item $self->polar_online_promote
Function mush be runing with enabled logindex.
=cut
sub polar_online_promote
{
	my ($self, $node_replica_ref, $name) = @_;
	my $new_master = $self->node_master;
	my $old_master = $self->node_master;
	#make sure that replica has replayed all the logindex needed before master stop.
	$self->replay_check_all($node_replica_ref);

	$old_master->stop;
	foreach my $node_replica (@{$node_replica_ref})
	{
		if ($node_replica->name eq $name)
		{
			$new_master = $node_replica;
		}
		else
		{
			$node_replica->stop;
		}
	}
	return if ($new_master eq $old_master);

	my $new_master_origin_type = $new_master->polar_get_node_type;

	$new_master->promote;

	my $old_master_name = $old_master->name;
	$old_master->polar_set_name($new_master->name);
	if ($new_master_origin_type eq "standby")
	{
		$old_master->polar_standby_set_recovery($new_master);
	}
	elsif ($new_master_origin_type eq "replica")
	{
		$old_master->polar_set_recovery($new_master);
	}
	else 
	{
		return
	}

	foreach my $node_replica (@{$node_replica_ref})
	{
		if ($node_replica->name ne $name)
		{
			if ($node_replica->polar_get_root_node eq $old_master)
			{
				if ($node_replica->polar_get_node_type eq "replica")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_set_recovery($new_master);
				}
				elsif ($node_replica->polar_get_node_type eq "standby")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_standby_set_recovery($new_master);
				}
			}
			elsif ($node_replica->polar_get_root_node eq $new_master)
			{
				if ($node_replica->polar_get_node_type eq "replica")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_set_recovery($old_master);
				}
				elsif ($node_replica->polar_get_node_type eq "standby")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_standby_set_recovery($old_master);
				}
			}
		}
	}
	foreach my $node_replica (@{$node_replica_ref})
	{
		if ($node_replica->name eq $name)
		{
			$node_replica = $old_master;
		}
	}
	$new_master->polar_set_name($old_master_name);
	foreach my $node_replica (@{$node_replica_ref})
	{
		print "node(".$node_replica->name.")'s root node is ".$node_replica->polar_get_root_node->name."\n";
	}
	$self->{_node_master} = $new_master;
	$old_master->start;
	#record old master and new master's slots
	my $old_master_slots = $old_master->safe_psql($self->regress_db, qq[select slot_name from pg_replication_slots;]);
	#delete old master's slot and create it for new master
	foreach my $slot (split('\n', $old_master_slots))
	{
		print "create new master's slot [".$slot."]\n";
		$new_master->polar_create_slot($slot);
		print "drop old master's slot [".$slot."]\n";
		$old_master->polar_drop_slot($slot);
	}
	#start all nodes
	$old_master->stop;
	foreach my $node_replica (@{$node_replica_ref})
	{
		$node_replica->start;
	}
}

sub polar_promote
{
	my ($self, $node_replica_ref, $name) = @_;
	my $new_master = $self->node_master;
	my $old_master = $self->node_master;
	foreach my $node_replica (@{$node_replica_ref})
	{
		if ($node_replica->name eq $name)
		{
			$new_master = $node_replica;
			last;
		}
	}
	return if ($new_master eq $old_master or $new_master->polar_get_node_type ne "standby");
	#make sure that standby should receive all the wal needed before stop.
	$self->replay_check_all($node_replica_ref);

	$self->pg_control("stop: all", $node_replica_ref);
	my $old_master_name = $old_master->name;
	$old_master->polar_set_name($new_master->name);
	$old_master->polar_drop_recovery;
	$old_master->polar_standby_set_recovery($new_master);
	foreach my $node_replica (@{$node_replica_ref})
	{
		if ($node_replica->name ne $name)
		{
			if ($node_replica->polar_get_root_node eq $old_master)
			{
				if ($node_replica->polar_get_node_type eq "replica")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_set_recovery($new_master);
				}
				elsif ($node_replica->polar_get_node_type eq "standby")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_standby_set_recovery($new_master);
				}
			}
			elsif ($node_replica->polar_get_root_node eq $new_master)
			{
				if ($node_replica->polar_get_node_type eq "replica")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_set_recovery($old_master);
				}
				elsif ($node_replica->polar_get_node_type eq "standby")
				{
					$node_replica->polar_drop_recovery;
					$node_replica->polar_standby_set_recovery($old_master);
				}
			}
		}
	}
	foreach my $node_replica (@{$node_replica_ref})
	{
		print "node(".$node_replica->name.")'s root node is ".$node_replica->polar_get_root_node->name."\n";
		if ($node_replica->name eq $name)
		{
			$node_replica = $old_master;
		}
	}
	$new_master->polar_set_name($old_master_name);
	$new_master->polar_drop_recovery;
	$self->{_node_master} = $new_master;
	$old_master->start();
	$new_master->start();
	#record old master and new master's slots
	my $old_master_slots = $old_master->safe_psql($self->regress_db, qq[select slot_name from pg_replication_slots;]);
	my $new_master_slots = $new_master->safe_psql($self->regress_db, qq[select slot_name from pg_replication_slots;]);
	#delete old master's slot and create it for new master
	foreach my $slot (split('\n', $old_master_slots))
	{
		print "old master's slot is [".$slot."]\n";
		$new_master->polar_create_slot($slot);
		$old_master->polar_drop_slot($slot);
	}
	#delete new master's slot and create it for old master
	foreach my $slot (split('\n', $new_master_slots))
	{
		print "new master's slot is [".$slot."]\n";
		$old_master->polar_create_slot($slot);
		$new_master->polar_drop_slot($slot);
	}
	#FIXME: if we don't restart here, some replica nodes can't connect to primary server.
	$self->pg_control("restart: all", $node_replica_ref);
}

sub pg_control
{
	my ($self, $line, $node_replica_ref) = @_;
	my ($action, $params) = split(':', $line);
	return if (!$action or !$params);
	$action =~ s/^\s+|\s+$//g;
	$params =~ s/^\s+|\s+$//g;
	return if ($action eq 'ignore');
	print "#### pg control command: $action $params\n";
	if ($action eq 'polar_promote')
	{
		return $self->polar_promote($node_replica_ref, $params);
	}
	elsif ($action eq 'promote')
	{
		return $self->polar_online_promote($node_replica_ref, $params);
	}
	my $node_master = $self->node_master;
	my $regress_db = $self->regress_db;
	my @nodes = ();
	my @modes = ();
	foreach my $param (split(' ', $params))
	{
		my ($node, $mode) = split('-', $param);
		if (($node_master->name eq $node) or ($node eq 'all'))
		{
			push(@nodes, $node_master);
			push(@modes, $mode);
		}
		foreach my $node_replica (@{$node_replica_ref})
		{
			if (($node_replica->name eq $node) or ($node eq 'all'))
			{
				push(@nodes, $node_replica);
				push(@modes, $mode);
			}
		}
	}
	for (my $i = 0; $i < @nodes; $i += 1)
	{
		if ($modes[$i])
		{
			$nodes[$i]->$action($modes[$i]);
		}
		else
		{
			$nodes[$i]->$action;
		}
	}
}

our $max_live_children = 50;
sub test
{
	my ($self, $schedule_file, $sql_dir, $node_replica_ref) = @_;
	$self->convert_sourcefile();
	open my $schedule, '<', "$schedule_file" or die "Could not open $schedule_file $!";
	my $failed = 0;
	my $task_count = 0;
	while (my $line = <$schedule>)
	{
		my $flag = 'test: ';
		if ((rindex $line, $flag, 0) == 0)
		{
			my @tasks = ();
			my $total_tasks = 0;
			$line = substr($line, length($flag));
			$line =~ s/\s+$//;
			foreach my $_line (split(' ', $line))
			{
				push @tasks, $_line;
				$total_tasks ++;
			}
			$task_count += $total_tasks;
			my $left_tasks = $total_tasks;
			my $live_children = 0;
			my @pip_children = ();
			my @task_children = ();
			my $child_index = 0;
			while($live_children > 0 or $left_tasks > 0)
			{
				while(($live_children >= $max_live_children) or ($left_tasks <= 0 and $live_children > 0))
				{
					print "--->waiting for child...\n";
					my $child_pid = wait();
					print "--->child($child_pid) was exited.\n";
					$live_children --;
				}
				if ($left_tasks > 0)
				{
					my $pip = IO::Pipe->new;
					my $pid = fork();
					if ($pid == 0)
					{
						$pip->writer;
						my $task = $tasks[$child_index];
						print "my index is $child_index, my task is tasks[$child_index]:$task\n";
						$ENV{'whoami'} = 'child';
						if ($self->get_test_mode() eq 'default')
						{
							$failed = $self->test_one_case($node_replica_ref, $sql_dir, $task);
						}
						elsif ($self->get_test_mode() eq 'xact_split')
						{
							$failed = $self->test_xact_split_one_case($node_replica_ref, $sql_dir, $task);
						}
						else
						{
							BAIL_OUT("unsupported test mode");
						}
						print $pip $failed;
						exit;
					}
					elsif ($pid > 0)
					{
						print "--->Created child process($pid)\n";
						$left_tasks --;
						$live_children ++;
						$pip->reader;
						push @pip_children, $pip;
						push @task_children, $tasks[$child_index];
						$child_index ++;
					}
				}
			}
			for (my $i = 0; $i < $child_index; $i ++)
			{
				my $res = "";
				my $pip_child = $pip_children[$i];
				while (my $line = <$pip_child>)
				{
					chomp $line;
					$res .= $line;
				}
				if ($res ne "0")
				{
					$failed = 1;
				}
				print "--->child($i:$task_children[$i]) returns $res\n";
			}
			if ($failed == 0 and $task_count > int(rand(20)))
			{
				$task_count = 0;
				my $pg_control_command = $self->get_one_random_pg_control;
				print "Get one random pg_control command : $pg_control_command\n";
				$self->pg_control($pg_control_command, $node_replica_ref);
				# Sometimes, node will cost more time to accept connections after some pg_ctl commands.
				$self->wait_for_all_node($node_replica_ref);
			}
		}
		else
		{
			$self->pg_control($line, $node_replica_ref);
		}
		if ($failed)
		{
			last;
		}
	}

	close $schedule;
	undef $ENV{'PG_TEST_NOCLEAN'} unless $failed != 0;

	#Make sure that RO's restart lsn could catch up the consistency lsn before shutdown instance.
    if ($failed == 0)
    {
        $self->replay_check_all($node_replica_ref);
    }

	return $failed;
}

our $timeout = 120;

sub test_asynchronous_sql
{
	my ($self, $test_case, $node_replica_ref, $regress_out, $line_num, $table) = @_;
	my $node_master = $self->node_master;
	my $regress_db = $self->regress_db;
	my $failed = 0;	
	my $sql = "";

	$failed = 1 unless $self->replay_check_all($node_replica_ref, $table);
	if ($failed == 1)
	{
		return $failed;
	}
	my @_stdout = ();
	my @_stderr = ();
	my @_stdout_exp = ();
	my @_stderr_exp = ();
	my @_name = ();
	my @_harness = ();
	my $harness_sum = 0;
	for (my $i = 0; $i < @asynchronous_sql; $i ++)
	{
		$sql .= $asynchronous_sql[$i];
		if ($asynchronous_executor[$i] eq "master")
		{
			push @_stdout, "";
			push @_stderr, "";
			my @opts = ();
			$node_master->sql_cmd(\@opts, $regress_db, $asynchronous_sql[$i],
				stdout => \$_stdout[$harness_sum],
				stderr => \$_stderr[$harness_sum],
				extra_params => ['-a'],
				on_error_stop => 0,
				timeout => $timeout);
			push @_harness, (IPC::Run::harness @opts);
			push @_name, "master";
			push @_stdout_exp, $asynchronous_stdout_exp[$i];
			push @_stderr_exp, $asynchronous_stderr_exp[$i];
			$harness_sum ++;
		}
		elsif ($asynchronous_executor[$i] eq "replica")
		{
			foreach my $node_replica (@{$node_replica_ref})
			{
				push @_stdout, "";
				push @_stderr, "";
				my @opts = ();
				$node_replica->sql_cmd(\@opts, $regress_db, $asynchronous_sql[$i],
					stdout => \$_stdout[$harness_sum],
					stderr => \$_stderr[$harness_sum],
					extra_params => ['-a'],
					on_error_stop => 0,
					timeout => $timeout);
				push @_harness, (IPC::Run::harness @opts);
				push @_name, $node_replica->name;
				push @_stdout_exp, $asynchronous_stdout_exp[$i];
				push @_stderr_exp, $asynchronous_stderr_exp[$i];
				$harness_sum += 1;
			}
		}
	}
	for (my $_index = 0; $_index < $harness_sum; $_index ++)
	{
		IPC::Run::pump_nb $_harness[$_index];
	}
	for (my $_index = 0; $_index < $harness_sum; $_index ++)
	{
		IPC::Run::finish $_harness[$_index];
	}

	my $test_name = $test_case.":$line_num:".$sql;
	note $test_name;
	for (my $j = 0; $j < $harness_sum; $j ++)
	{
		print $regress_out "asynchronous_sql $_name[$j] stdout .....................................................................................\n";
		print $regress_out "$_stdout[$j]\n";
		print $regress_out "asynchronous_sql $_name[$j] stderr -------------------------------------------------------------------------------------\n";
		print $regress_out "$_stderr[$j]\n";
		if ($_name[$j] ne "master")
		{
			if (($_stdout_exp[$j] ne "") && ($_stdout[$j] ne "") && ($_stdout[$j] !~ $_stdout_exp[$j]))
			{
				note "_stdout_exp = $_stdout_exp[$j]\nAsynchronous STDOUT FAILED!\n";
				$failed = 1;

			}
			if (($_stderr_exp[$j] ne "") && ($_stderr[$j] ne "") && ($_stderr[$j] !~ $_stderr_exp[$j]))
			{
				note "_stderr_exp=$_stderr_exp[$j]\nAsynchronous STDERR FAILED!\n";
				$failed = 1;
			}
		}
	}
	return $failed;
}

sub test_some_sql
{
	my ($self, $node_master, $node_replica_ref, $test_case, $regress_db, $sql, $line_num, $polar_tag, $regress_out) = @_;
	my $failed = 0;
	my $test_name = $test_case.":$line_num:".$sql;
	my $master_output;
	my $replica_output;
	my ($start_seconds, $start_microseconds) = gettimeofday();
	my $start_time = localtime($start_seconds)." ".$start_microseconds;
	$master_output = $node_master->psql_execute($sql, $timeout, $node_master->get_psql());
	$master_output =~ s/^$regress_db[^\s]* //mg;
	my $note_msg = "";
	$note_msg .= $test_name;
	print $regress_out "$test_name\n";
	print $regress_out "master output .....................................................................................\n";
	print $regress_out "$master_output\n";
	foreach my $node_replica (@{$node_replica_ref})
	{
		my $replica_name = $node_replica->name;
		if ($polar_tag eq 'EQUAL')
		{
			$replica_output = $node_replica->psql_execute($sql, $timeout, $node_replica->get_psql());
			$replica_output =~ s/^$regress_db[^\s]* //mg;
			if ($master_output ne $replica_output)
			{
				$failed = 1;
				$note_msg .= "RESULT EQUAL FAILED\n";
			}
			my $_err_filter = qr/.*ERROR:\s+invalid\s+page\s+in\s+block.*/i;
			if ($master_output =~ $_err_filter || $replica_output =~ $_err_filter)
			{
				$failed = 0;
				$note_msg .= "INVALID PAGE ERROR was cause by local filesystem!\n";
			}
		}
		elsif ($polar_tag eq 'REPLICA_ERR')
		{
			$replica_output = $node_replica->psql_execute($sql, $timeout, $node_replica->get_psql());
			$replica_output =~ s/^$regress_db[^\s]* //mg;
			if (!$replica_output || index($replica_output, "ERROR:") == -1)
			{
				$failed = 1;
				$note_msg .= "REPLICA_ERR FAILED\n";
			}
		}
		elsif ($polar_tag ne 'REPLICA_IGNORE')
		{
			$note_msg .= "POLAR_TAG: $polar_tag is incorrect\n";
			$failed = 1;
		}

		print $regress_out "$replica_name output ---------------------------------------------------------------------------------\n";
		print $regress_out "$replica_output\n";
		if ($failed)
		{
			last;
		}
	}
	my ($end_seconds, $end_microseconds) = gettimeofday();
	my $end_time = localtime($end_seconds)." ".$end_microseconds;
	note $note_msg."[$start_time, $end_time]\n";
	return $failed;
}

sub test_one_case
{
	my ($self, $node_replica_ref, $sql_dir, $test_case) = @_;
	my $sql_file = "$sql_dir/$test_case.sql";
	my $node_master = $self->node_master;
	my $test_log_path = $self->test_log_path;
	my $regress_out_file = "$test_log_path/$test_case.out";
	my $regress_db = $self->regress_db;

	open my $regress_out, '>', $regress_out_file or die "Failed to open $regress_out_file";
	open my $fh, '<', $sql_file or die "Failed to open $sql_file: $!";
	my $line_num = 0;
	my $polar_tag;
	my $failed = 0;
	$ENV{'PG_TEST_NOCLEAN'} = 'Do not clean data directory after error during test.';
	my $table = "replayed_$test_case\_$$";
	
	# something about random flashback
	my $random_rel = '';
	my $flashback_time = '';
	my $flashback_epoc = 0;
	my $random_rel_oid = 0;
	my $flashback_t;

	$node_master->safe_psql($regress_db, "CREATE TABLE $table(val integer unique);");
	$self->replay_check_all($node_replica_ref, $table);
	my $master_psql = $node_master->psql_connect($regress_db, $timeout);
	$node_master->set_psql($master_psql);
	foreach my $node_replica (@{$node_replica_ref})
	{
		my $replica_psql = $node_replica->psql_connect($regress_db, $timeout);
		$node_replica->set_psql($replica_psql);
	}

	my $mode = 'synchronous';
	my $polar_prev_tag = 'UNKNOWN';
	my $some_sql = '';
	while (my $sql = read_sql($fh, \$line_num, \$polar_tag, \$mode))
	{
		$failed = 0;
		my $master_stdout = '';
		my $master_stderr = '';
		my $replica_stdout = '';
		my $replica_stderr = '';
		if ($sql =~ qr/^\s*$/i)
		{
			next;
		}
		if ($mode eq 'asynchronous')
		{
			$failed = $self->test_asynchronous_sql($test_case, $node_replica_ref, $regress_out, $line_num, $table);
			if ($failed == 1)
			{
				last;
			}
			@asynchronous_sql = ();
			@asynchronous_executor = ();
			@asynchronous_stdout_exp = ();
			@asynchronous_stderr_exp = ();
			$mode = 'synchronous';
			next;
		}
		if ($polar_prev_tag eq $polar_tag or $some_sql eq '')
		{
			$some_sql = $some_sql.$sql;
			$polar_prev_tag = $polar_tag;
			next;
		}
		$failed = $self->test_some_sql($node_master,
									   $node_replica_ref,
									   $test_case,
									   $regress_db,
									   $some_sql,
									   $line_num,
									   $polar_prev_tag,
									   $regress_out);
		if ($failed)
		{
			last;
		}
		
		# Get a random relation and copy
		if ($self->{_enable_random_flashback} and $random_rel eq '' and $line_num > 10 and $line_num > int(rand(20)))
		{
			$random_rel = $self->fb_copy_a_random_rel;
			print "flashback: The random relation is ".$random_rel."\n";
			$random_rel_oid = $self->get_rel_oid($random_rel);
			print "flashback: The random relation oid is ".$random_rel_oid."\n";
			
			$flashback_t = time;
			$flashback_time = strftime "%Y-%m-%d %H:%M:%S", gmtime $flashback_t;
			$flashback_time .= sprintf ".%05d", ($flashback_t-int($flashback_t))*100000;
			print "flashback: The flashback time is ".$flashback_time."\n";
			$flashback_epoc = time();
		}
		
		# FLashback the table and check it if it exists
		if ($random_rel ne '' and time() - $flashback_epoc >= 3)
		{
			$failed = $self->fb_rel_and_check($random_rel, $random_rel_oid, $flashback_time);
			print "flashback: The flashback table ".$random_rel." are ".$failed." failed\n";
			
			$random_rel = '';
			
			if ($failed == 1)
			{
				last;
			}
			else
			{
				$self->fb_drop_copy_rel($random_rel, $random_rel_oid);
			}
		}

		if ($polar_prev_tag ne 'EQUAL' and $polar_tag eq 'EQUAL' or
			$polar_prev_tag ne 'REPLICA_IGNORE' and $polar_tag eq 'REPLICA_IGNORE')
		{
			$failed = 1 unless $self->replay_check_all($node_replica_ref, $table);
			if ($failed == 1)
			{
				last;
			}
		}
		$polar_prev_tag = $polar_tag;
		$some_sql = $sql;
	}

	if ($failed == 0 and $some_sql ne '')
	{
		$failed = $self->test_some_sql($node_master,
									   $node_replica_ref,
									   $test_case,
									   $regress_db,
									   $some_sql,
									   $line_num,
									   $polar_prev_tag,
									   $regress_out);
	}

	$node_master->psql_close($node_master->get_psql());
	foreach my $node_replica (@{$node_replica_ref})
	{
		$node_replica->psql_close($node_replica->get_psql());
	}
	
	# Clean the random rel when it isn't flashbacked because it run within 3 second
	if ($random_rel ne '')
	{
		 $self->fb_drop_copy_rel($random_rel, $random_rel_oid);
	}

	close $fh;
	close $regress_out;

	return $failed;
}


sub get_rw_xact_info {
	my ($psql, $regress_db) = @_;
	$psql->send_slow(0, "select * from polar_stat_xact_split_info();\n");
	my $master_out = get_output($psql, $regress_db);
	my @lines = split(/\r\n/, $master_out);
	my @xact_info = split(/\|/, $lines[-1]);
	$psql->clear_accum();
	return @xact_info;
}

sub set_ro_xact_info {
	my ($psql, $regress_db, $send_lsn, @xact_info) = @_;
	$psql->send("set polar_xact_split_xids='" . $xact_info[0] . "';\n");
	print get_output($psql, $regress_db);
	if ($send_lsn != 0) {
		$psql->send("set polar_xact_split_wait_lsn='" . $xact_info[2] . "';\n");
		get_output($psql, $regress_db);
	}
	$psql->clear_accum();
}

sub get_output {
	my ($psql, $regress_db) = @_;
	$psql->expect(10, '-re', "^".$regress_db."(.*)\$", ">> ");
	$psql->clear_accum();
	return $psql->before;
}

sub test_xact_split_one_case {
	my ($self, $node_replica_ref, $sql_dir, $test_case) = @_;
	my $sql_file = "$sql_dir/$test_case.sql";
	my $node_master = $self->node_master;
	my $regress_db = $self->regress_db;

	open my $fh, '<', $sql_file or die "Failed to open $sql_file: $!";
	my $failed = 0;
	my $table = "replayed_$test_case\_$$";

	$node_master->safe_psql($regress_db, "CREATE TABLE $table(val integer unique);");
	$self->replay_check_all($node_replica_ref, $table);

	my $master_psql = $node_master->psql_connect($regress_db, $timeout);
	$node_master->set_psql($master_psql);
	foreach my $node_replica (@{$node_replica_ref})
	{
		my $replica_psql = $node_replica->psql_connect($regress_db, $timeout);
		$node_replica->set_psql($replica_psql);
	}

	while (my $line = <$fh>) {
		if (lc($line) =~ m/(begin|end|commit|abort|rollback);/)
		{
			print "xact control\n";
			$master_psql->send($line);
			print get_output($master_psql, $self->regress_db);
			foreach my $node_replica (@{$node_replica_ref})
			{
				$node_replica->get_psql()->send($line);
				get_output($node_replica->get_psql(), $self->regress_db);
			}
		}
		elsif ($line =~ m/POLAR_XACT_SPLIT/) {
			my @xact_info = get_rw_xact_info($master_psql, $self->regress_db);
			if ($xact_info[0])
			{
				foreach my $node_replica (@{$node_replica_ref})
				{
					set_ro_xact_info($node_replica->get_psql(), $self->regress_db, 0, @xact_info);
					$node_replica->get_psql()->clear_accum();
				}
			}

			$self->replay_check_all($node_replica_ref, $table);

			$master_psql->send($line);
			foreach my $node_replica (@{$node_replica_ref})
			{
				$node_replica->get_psql()->send($line);
			}

			my $master_output = get_output($master_psql, $self->regress_db);
			foreach my $node_replica (@{$node_replica_ref})
			{
				my $replica_output = get_output($node_replica->get_psql(), $self->regress_db);
				if ($master_output =~ m/stdin/ && $replica_output =~ m/stdin/) {
					print("**********************\n");
					print($master_output);
					print("**********************\n");
				}
				elsif ($master_output ne $replica_output) {
					print("<<<<<<<<<<<<<<<<<<<<<<\n");
					print($master_output);
					print("----------------------\n");
					print($replica_output);
					print(">>>>>>>>>>>>>>>>>>>>>>\n");
					$failed = 1;
				}
				else {
					print("======================\n");
					print($master_output);
					print("======================\n");
				}
			}
		}
		else {
			$master_psql->send($line);
			print get_output($master_psql, $self->regress_db);
		}
	}
	return $failed;
}

sub create_new_test
{
	my $class = 'PolarRegression';
	$class = shift if 1 < scalar @_;
	my $node_master = shift;

	my $test = $class->new($node_master);
	$ENV{'whoami'} = 'parent';
	return $test;
}
1;

