# Test for remove datadir protocol of archive command
use strict;
use warnings;
use PostgresNode;
use File::Path 'rmtree';
use Test::More tests=>1;

# master
my $node_master = get_new_node('master');
$node_master->polar_init(1, 'polar_master_logindex');
my $backup_dir = $node_master->archive_dir;
$node_master->append_conf('postgresql.conf', "wal_keep_segments = 5");
$node_master->append_conf('postgresql.conf', "polar_wal_pipeline_enable = false");
$node_master->append_conf('postgresql.conf', "archive_mode = on");
$node_master->append_conf('postgresql.conf', "archive_command = 'cp \%p $backup_dir/\%f'");
$node_master->append_conf('postgresql.conf', "polar_logindex_mem_size = 0");

# judge whether wal file is the same
sub polar_walfile_compare
{
	my ($walfile1, $walfile2, $waldir1, $waldir2, $last_segno) = @_;
	my $ret = 0;
	foreach my $i (@$walfile1)  
	{ 
		if ($i lt $last_segno)
		{
			print "wal: $i\n";
			if(grep { $_ eq $i } @$walfile2)
			{
                my $wal1 = $waldir1 . "/" . $i;
                my $md51 = readpipe("md5sum $wal1");
                $md51 = (split(" ",$md51))[0];
                $wal1 = $waldir2 . "/" . $i;
                my $md52 = readpipe("md5sum $wal1");
                $md52 = (split(" ",$md52))[0];
                if ($md51 eq $md52)
                {
                    $ret = 1;
                }
				else
				{
					$ret = 0;
					last;
				}
			}
			else
			{
				$ret = 0;
				last;
			}
		}
	}
	return $ret;
}

# clear content of archive_test
rmtree($backup_dir) or warn "cannot rmtree '$backup_dir'";
mkdir $backup_dir, 0755 or warn "cannot mkdir '$backup_dir'";

# generate wal
$node_master->start;
$node_master->safe_psql('postgres', 'CREATE TABLE test_table(val integer);');
my $count = 0;
my $total_count = 1;
do
{
	# pg_switch_wal() to generate wal 
	$node_master->safe_psql('postgres', "select pg_switch_wal();");	
	$count = $count + 1;
}while($count < $total_count);
$node_master->safe_psql('postgres',
		'INSERT INTO test_table(val) SELECT coalesce(max(val),0) + 1 AS newval FROM test_table RETURNING val');
my $write_lsn = $node_master->lsn('insert');
my $last_segno = $node_master->safe_psql('postgres', "select pg_walfile_name('$write_lsn');");
print "last_segno: $last_segno\n";

# do checkpoint
$node_master->safe_psql('postgres', "checkpoint");
sleep 10;

### 1. check whether archive succeed
my $master_waldir = $node_master->polar_get_datadir;
$master_waldir .= "/pg_wal";
my $walfile = readpipe("ls $master_waldir");
my @master_walarray = split('\n', $walfile);
$walfile = readpipe("ls $backup_dir");
my @backup_walarray = split('\n', $walfile);
my $result = polar_walfile_compare(\@master_walarray, \@backup_walarray, $master_waldir, $backup_dir, $last_segno);
print "archive result: $result\n";
ok($result == 1, "archive succeed");

$node_master->stop;
