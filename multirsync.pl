#!/usr/bin/env perl
use strict;
use warnings;

use Getopt::Long qw( :config no_auto_abbrev );
use forks;
use Cwd qw( abs_path );
use File::Basename;
use Thread::Queue;
use List::MoreUtils qw/ uniq /;
use POSIX qw(strftime);

use IPC::Open3;

my $version     = "1.5";
my $verbose_arg = 0;
my $dryrun_arg;

my $rsync_cmd_path = "/usr/bin/rsync";

my $prefix       = dirname( abs_path($0) );
my $logpath      = "$prefix";
my $logdate      = strftime "%Y-%m", localtime;
my $logfile      = "$logpath/$logdate.log";
my $excludespath = "$prefix";

my $SourceHost = "localhost";
my @msgTyp     = ("INFO");

my $source      = '';
my $destination = '';
my $sourcepath  = '';
my $remotehost  = "";
my @subfolders  = '';
my $rsh_arg     = "ssh";
my $nthreads_arg;
my $rsyncdel_arg;
my $inplace_arg;
my $sizeonly_arg;
my $relative_arg;
my $exclude_arg;
my $pattern_arg;
my @queue;

#################################
# Main
#
if (! -x $rsync_cmd_path) {
    print "rsync not found in $rsync_cmd_path\n";
    exit 1;
}

parse_command_options();

#################################
# eval subfolders for the queue
#
if ( $source =~ m/:/ ) {
    ($remotehost, $sourcepath) = split(/:\//, $source);
    $sourcepath = "/$sourcepath";
} else {
    $sourcepath = $source;
}

&logit( 0, "MultiRsync", "***Start RSYNC  Sequence -- Debug Mode --***" ) if $verbose_arg;
print "remotehost: $remotehost SOURCE: $sourcepath DEST: $destination\n" if $verbose_arg;

my $pattern = "";
if ($pattern_arg) {
    $pattern = "-name $pattern_arg";
}

my $find_cmd = "find $sourcepath $pattern -xdev -mindepth 1 -maxdepth 1 -type d -printf '%P\n' | sort";
print "local_find: $find_cmd\n";

if ($remotehost) {
    $find_cmd = "$rsh_arg $remotehost \"$find_cmd\"";
    print "remote_find: $find_cmd\n" if $verbose_arg;
}
    @subfolders = `$find_cmd`;

    # if @subfolders empty (rsh troubles?) then use the $srcfolder
    if ( $#subfolders == -1 ) {
        push( @subfolders, $sourcepath );
        logit( 0, "MultiRsync", "ERROR: eval subfolders failed, use now with:\n @subfolders" );
        print "ERROR: eval subfolders failed, use now with:\n @subfolders\n" if $verbose_arg;
    } else {
        logit( 0, "MultiRsync", "eval subfolders:\n @subfolders" );
        print "eval subfolders:\n @subfolders" if $verbose_arg;
    }
    my $exclsubfolderfile = "$excludespath/generic_excludes";

    print "Excludefile: $exclsubfolderfile\n";
    open(my $fhExcludeFile, '>>', $exclsubfolderfile) unless $dryrun_arg;

    foreach my $subfolder (@subfolders) {
        chomp $subfolder;
        $subfolder =~ s| |\\ |g;
        $subfolder =~ s|\(|\\\(|g;
        $subfolder =~ s|\)|\\\)|g;
        $subfolder =~ s|\'|\\\'|g;
#        $subfolder =~ s|\:|\\\:|g;

        my $job = {
            sourcepath     => "$sourcepath",
            subfolder      => "$subfolder",
            exclsubfolders => 0,
        };
        push( @queue, $job );

        print $fhExcludeFile "- $subfolder/\n" unless $dryrun_arg;
        print "- $subfolder/\n" if $verbose_arg;
    }
    close $fhExcludeFile unless $dryrun_arg;

        my $job = {
            sourcepath     => "$sourcepath",
            exclsubfolders => 1,
        };
        push( @queue, $job );

start_threads();

print "Remove generic exclude file: $exclsubfolderfile\n" if $verbose_arg;
unlink $exclsubfolderfile if ( -e $exclsubfolderfile );
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
    $Q->enqueue($_) for @queue;
    $Q->enqueue( (undef) x $nthreads );
    $_->join for @threads;

    return 1;
}

sub thread_work {
    my ($Q) = @_;
    my $tid = threads->tid;

    while ( my $syncfolder = $Q->dequeue ) {
        my $tid            = threads->tid;
        my $sourcepath     = $syncfolder->{sourcepath};
        my $subfolder      = $syncfolder->{subfolder} || "";
        my $exclsubfolders = $syncfolder->{exclsubfolders} || 0;

        &logit(  $tid, $subfolder, "Initialize rsync sequence" );

        my $exclude = "";
        if ($exclude_arg) {
            $exclude = "--exclude-from=$exclude_arg";
        }
        my $rsyncdel = "";
        if ($rsyncdel_arg) {
            $rsyncdel = "--delete";
        }

        my $inplace = "";
        if ($inplace_arg) {
            $inplace = "--inplace";
        }
        my $sizeonly = "";
        if ($sizeonly_arg) {
            $sizeonly = "--size-only";
        }

        my $relative = "";
        if ($relative_arg) {
            $relative = "-R";
        }

        if ( $remotehost ) {
            $sourcepath = $remotehost .":" . $sourcepath;
        }
        my $rsync_generic_exclude = '';
        if ( $exclsubfolders ) {
            $rsync_generic_exclude = "--exclude-from=$excludespath/generic_excludes";
            &logit( $tid, $subfolder, "Apply subfolder excludelist: $rsync_generic_exclude" );
        }

        my $wait = ( int rand 2 ) + 1;
        logit( $tid, $subfolder, "Thread $tid sleep $wait sec. for $sourcepath/$subfolder" );
        sleep($wait);
        logit( $tid, $subfolder, "Thread $tid  working on $sourcepath/$subfolder" );

        my $rsh = "";
        if ( $remotehost && $rsh_arg eq "rsh" ) {
           $rsh = "-e $rsh_arg";
        }
        my $rsync_options = "-aHx $rsh $inplace $relative $rsync_generic_exclude $exclude $rsyncdel $sizeonly";
        $rsync_options =~ s/\s+$//;

        &logit( $tid, $subfolder, "Rsync Command: $rsync_cmd_path $rsync_options '$sourcepath/$subfolder' '$destination'" );
        &logit( $tid, $subfolder, "Executing rsync for $sourcepath/$subfolder" );

        local ( *HIS_IN, *HIS_OUT, *HIS_ERR );
        my $rsync_cmd = $dryrun_arg ? "echo $rsync_cmd_path" : $rsync_cmd_path;
        my $rsyncpid = open3( *HIS_IN, *HIS_OUT, *HIS_ERR, "$rsync_cmd $rsync_options \"$sourcepath/$subfolder\" \"$destination\"" );

        &logit( $tid, $subfolder, "Rsync PID: $rsyncpid for $subfolder" );

        my @outlines = <HIS_OUT>;
        my @errlines = <HIS_ERR>;
        close HIS_IN;
        close HIS_OUT;
        close HIS_ERR;

        print "STDOUT: $tid: @outlines\n" if $verbose_arg;

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
                    &logit( $tid, $subfolder, "Error $errcode" );
                }
            }
        } else {
            &logit( $tid, $subfolder, "rsync successful!" );
        }
    }
    &logit( $tid , "Thread_Work", "rsync sequence done" );
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
        "e|rsh:s"      => \$rsh_arg,
        "pattern=s"    => \$pattern_arg,
        "exclude=s"    => \$exclude_arg,
        "relative"     => \$relative_arg,
        "delete"       => \$rsyncdel_arg,
        "inplace"      => \$inplace_arg,
        "size-only"    => \$sizeonly_arg,
        "threads|th:i" => \$nthreads_arg
    ) or usage("Invalid commmand line options.");

   $verbose_arg = 1 if ( $dryrun_arg );

   usage("Missing  Arguments!") unless ( ($#ARGV + 1) == 2 );

   $source      = $ARGV[-2];
   $source      =~ s/\/$//;
   $destination = $ARGV[-1];
   $destination =~ s/\/$//;

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

        $command [OPTIONS] <source> <destination>

         Optional Arguments:

         --pattern <string>         use find option -name
         --delete                   use rsync option --delete
         --inplace                  use rsync option --inplace
         --relative                 use rsync option --relative
         --size-only                use rsync option --size-only, needed after globus-url-copy tasks
         --exclude <file>           Excludefile path
         --th <nr>                  Number of threads, Default: 1
         -e | --rsh=<rsh|ssh>"      specify the remote shell to use, default = ssh
         -n | --dry-run             dry-run without making changes (implies verbose)
         -v | --verbose             verbose mode
         --version                  see version
         --help                     see this help

EOF

    exit 0;
}
