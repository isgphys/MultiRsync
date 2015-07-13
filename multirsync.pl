#!/usr/bin/perl
#
# copyright 2012 Patrick Schmid <schmid@phys.ethz.ch>, distributed under
# the terms of the GNU General Public License version 2 or any later
# version.
#
# This is compiled with threading support
#
# 2012.09.07, Patrick Schmid <schmid@phys.ethz.ch>

use strict;
use warnings;

use Getopt::Long;
use threads qw[ yield ];
use threads::shared;
use Thread::Queue;
use List::MoreUtils qw/ uniq /;
use POSIX qw(strftime);

use IPC::Open3;

my $version    = "1.5";
my $verbose_arg      = 0;
my $dryrun_arg;

#my $rsync_cmd = "/usr/bin/rsync";
my $rsync_cmd = "/scratch/rsync/rsync";

my $prefix  = "/opt/MultiRsync";
my $logpath = "$prefix/log";
my $logdate = strftime "%Y-%m", localtime;
my $logfile = "$logpath/$logdate.log";

my $SourceHost = "localhost";
my @msgTyp     = ("INFO");

my $sourcepath;
my $destpath      = '';
my $remotehost    = '';
my $exclude       = '';
my @remotedirlist = '';
my $rsyncdel      = '';
my $sizeonly      = '';
my $nthreads_arg;
my $rsyncdel_arg;
my $sizeonly_arg;

#################################
# Main
#
parse_command_options();

#################################
# eval subfolders for the queue
#
&logit( 0, "BANG", "***Start RSYNC  Sequence -- Debug Mode --***" ) if $verbose_arg;
my @queue;

if ($remotehost) {
    @remotedirlist = `rsh $remotehost ls $sourcepath`;
    $SourceHost    = $remotehost;
    $sourcepath    = "$remotehost:$sourcepath";
    foreach my $remotedata (@remotedirlist) {
        chomp $remotedata;
        $remotedata =~ s| |\\ |g;
        push @queue, $remotedata;
    }
} else {
    opendir( SOURCE, $sourcepath );
    while ( readdir(SOURCE) ) {
        next if ( $_ eq "." || $_ eq ".." || $_ eq ".fsr" );
        push @queue, $_;
    }
    closedir(SOURCE);
}

print "\nRSYNC sequence: @queue\n\n" if $verbose_arg;

start_threads();

exit 0;

#**********
# Threading
#
sub start_threads {

    # define number of threads
    my $nthreads;
    if ($nthreads_arg) {

        # If nthreads was defined by cli argument, use it
        $nthreads = $nthreads_arg;
        print "Using nthreads = $nthreads from command line argument\n" if $verbose_arg;
    } else {
        $nthreads = 1;
    }

    my $Q = Thread::Queue->new;
    my @threads = map threads->create( \&thread_work, $Q ), 1 .. $nthreads;
    $Q->enqueue($_) for sort @queue;
    $Q->enqueue( (undef) x $nthreads );
    $_->join for @threads;

    return 1;
}

sub thread_work {
    my ($Q) = @_;
    my $tid = threads->tid;

    while ( my $syncfolder = $Q->dequeue ) {

        &logit( $SourceHost, $syncfolder, "Initialize rsync sequence" );

        if ($exclude) {
            $exclude = "--exclude-from $exclude";
        }
        if ($rsyncdel_arg) {
            $rsyncdel = "--delete";
        }

        if ($sizeonly_arg) {
            $sizeonly = "--size-only";
        }

        if ($dryrun_arg) {

            # imaginäre arbeit!
            my $wait = ( int rand 10 ) + 1;
            print "\n*****\n";
            print "* start $syncfolder - cycles: $wait \n";

            &logit( $SourceHost, $syncfolder, "dryrun rsync start" );
            print "Do_WORK: $rsync_cmd -aHR $exclude -e rsh --delete '$sourcepath/$syncfolder' $destpath\n" if $verbose_arg;

            for my $i ( 1 .. $wait ) {
                print "$syncfolder $i\n";
                sleep 0.5;
            }
        } else {
            my $rsync_options = "-aH --stats -e rsh --inplace $exclude $rsyncdel $sizeonly";

            &logit( $SourceHost, $syncfolder, "Rsync Command: $rsync_cmd $rsync_options '$sourcepath/$syncfolder' $destpath" );
            &logit( $SourceHost, $syncfolder, "Executing rsync for $sourcepath/$syncfolder" );

            local ( *HIS_IN, *HIS_OUT, *HIS_ERR );
            $rsync_cmd = "echo $rsync_cmd" if $dryrun_arg;
            my $rsyncpid = open3( *HIS_IN, *HIS_OUT, *HIS_ERR, "$rsync_cmd $rsync_options '$sourcepath/$syncfolder' $destpath" );

            &logit( $SourceHost, $syncfolder, "Rsync PID: $rsyncpid for $syncfolder" );

            my @outlines = <HIS_OUT>;
            my @errlines = <HIS_ERR>;
            close HIS_IN;
            close HIS_OUT;
            close HIS_ERR;

            print "STDOUT: @outlines\n" if $verbose_arg;

            if (@errlines) {
                print "STDERR: @errlines\n" if $verbose_arg;
            }

            waitpid( $rsyncpid, 0 );

            if ($?) {
                print "That child exited with wait status of $?\n" if $verbose_arg;
            }

            my $errcode = 0;
            if (@errlines) {
                foreach my $errline (@errlines) {
                    if ( $errline =~ /.* \(code (\d+)/ ) {
                        $errcode = $1;
                        &logit( $SourceHost, $syncfolder, "Error $errcode" );
                    }
                }
            } else {
                &logit( $SourceHost, $syncfolder, "rsync successful!" );
            }
        }
        &logit( $SourceHost, $syncfolder, "rsync sequence done" );
    }
}

###########
# write log
sub logit {
    my $hostname  = $_[0];
    my $folder    = $_[1];
    my $msg       = $_[2];
    my $timestamp = strftime "%b %d %H:%M:%S", localtime;

    open LOG, ">>$logfile" or die "$logfile: $!";
    print LOG "$timestamp $hostname $folder - $msg\n";
    close LOG;
}

#########################
# Command line arguments
#
sub parse_command_options {

    GetOptions(
        'help'         => sub { usage('') },
        'version'      => sub { usage("Current version number: $version") },
        "v|verbose"    => \$verbose_arg,
        'n|dry-run'    => \$dryrun_arg,
        "source=s"     => \$sourcepath,
        "dest=s"       => \$destpath,
        "remotehost=s" => \$remotehost,
        "exclude=s"    => \$exclude,
        "del"          => \$rsyncdel_arg,
        "size-only"    => \$sizeonly_arg,
        "threads|th:i" => \$nthreads_arg
    ) or usage("Invalid commmand line options.");

    usage("The Destination must be specified.")
        unless defined $destpath;

    usage("The Sourcepath must be specified.")
        unless defined $sourcepath;

   $verbose_arg = 1 if ( $dryrun_arg );

    return 1;
}

##############
# Usage
#
sub usage {
    my ($message) = @_;

    if ( defined $message && length $message ) {
        $message .= "\n"
            unless $message =~ /\n$/;
    }

    my $command = $0;
    $command =~ s#^.*/##;

    print <<"EOF";
        $message

        Usage Example:

        $command --source <sourcepath> --dest <destination>
        $command --remotehost <remothostname> --source <sourcepath> --dest <destination>

         --source <sourcepath>      Source path, ex. /export/data
         --dest <destination>       Target path, ex. /export/backup/data

         Optional Arguments:

         --remotehost <hostname>    Hostname of Source-Host...
         --del                      use rsync option --delete
         --size-only                use rsync option --size-only, needed after globus-url-copy tasks
         --exclude <file>           Excludefile path
         --th <nr>                  Number of threads, Default: 1
         -n | --dry-run             dry-run without making changes (implies verbose)
         -v | --verbose             verbose mode
         --version                  see version
         --help                     see this help

EOF

    exit 0;
}
