#!/usr/bin/perl

#
# A script to copy the directory structure from sql-directory to results.
# pg_regress fails if all the results directories don't exist.
#

use File::Find;
use File::Path;

sub wanted
{
    my $sqlname = $File::Find::name;

    # Ignore non-directories and CVS directories
    if (-d $sqlname && $sqlname !~ /CVS$/) 
    {
        my $resultsname = substr $sqlname, 4;
        #print "$resultsname\n";
        mkdir "results/$resultsname";
    }
}

find({wanted => \&wanted, no_chdir => 1}, 'sql');

# vpd tests additionally use this directory for some output files.
mkpath(['output/vpd_dumps'], 0, 0755);