#!perl -w
package RunCmd;
use strict;
use warnings;
use File::Temp;
use File::Spec;
use File::Path;
use Data::Dumper;
use Log::Log4perl;
use Storable qw(nstore retrieve);
use Carp qw(carp croak verbose);
use Cwd;

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  cc_run_cmd
  cc_run_cmd_parallel
  cc_run_parallel_cmds
  git_run_cmd
  get_view_root
);

my $UNIX_VIEW_ROOT = '/home/_svcmaven-dev/gitimports/git2cc/snapshots';

sub git_run_cmd {
	my ( $repodir, $cmd, %opts ) = @_;
	return cc_run_cmd( $cmd, gitrepo => $repodir, %opts );
}

# Returns stdout as an array of lines in list context
# Returns the first line of output in scalar context
# opts: view gitrepo nodie nowarn retcoderef stderrref
sub cc_run_cmd {
	my ( $cmd, %opts ) = @_;

	# We look for these values in %opts
	#view, gitrepo, nodie, nowarn, retcoderef, stderrref
	(%opts) or %opts = ();

	my $wantarray = wantarray();

	my $l = Log::Log4perl->get_logger();
	$l->trace("entering cc_run_cmd");

	$opts{view} and $opts{gitrepo} and croak('Cannot specify both view and gitrepo');

	my $cwd = undef;
	if ( $opts{view} ) {    # We need to be in a view - Assuming dynamic view
		my $view_root = get_view_root( $opts{view} );
		$cwd = getcwd();    # Save the current directory
		$l->trace("chdir to: $view_root");
		chdir $view_root or croak("Cannot chdir to: '$view_root'\n");
		( $^O ne 'MSWin32' ) and $cmd =~ s/(\$)/\\$1/g;    # Need to escape $ character in unix because it is interpreted
	} elsif ( $opts{gitrepo} ) {
		$cwd = getcwd();                                   # Save the current directory
		$l->trace("chdir to: $opts{gitrepo}");
		chdir $opts{gitrepo} or croak("Cannot chdir to: $opts{gitrepo}\n$!");
	}

	$l->trace("running command: $cmd");

	my ( $rc, $stdoutref, $stderr ) = _run_cmd($cmd);
	$l->trace("finished with status: $rc");

	# Change back to the original dir
	if ($cwd) {
		$l->trace("chdir to: $cwd");
		chdir $cwd or croak("Cannot chdir to: '$cwd'\n");
	}

	if ($rc) {
		$l->trace("stderr:\n $stderr");
		$l->trace("stdout:\n @$stdoutref");

		$opts{nodie}
		  or croak("The following command failed with exit status $rc\n$cmd\n\n$stderr");

		$opts{nowarn}
		  or carp("The following command failed with exit status $rc\n$cmd\n\n$stderr");
	}

	$opts{retcoderef} and ${ $opts{retcoderef} } = $rc;
	$opts{stderrref}  and ${ $opts{stderrref} }  = $stderr;

	$l->trace("leaving from cc_run_cmd");
	if ($wantarray) {
		chomp @$stdoutref;
		return @$stdoutref;
	} else {
		if ( $opts{gitrepo} ) {
			return join( '', @$stdoutref );
		} else {
			return $stdoutref->[0];
		}
	}
}

sub cc_run_cmd_parallel {
	my ( $cmd, $itemsref, %opts ) = @_;
	(%opts) or %opts = ();

	my $l = Log::Log4perl->get_logger();
	$l->trace("entering cc_run_cmd");

	$l->trace("MAX_CHILD_PROCESSES = $main::MAX_CHILD_PROCESSES");

	my $TMPDIR = File::Temp->newdir();

	my @pids;
	my %ret;
	my %pidhash;
	foreach my $item (@$itemsref) {
		if ( scalar(@pids) >= $main::MAX_CHILD_PROCESSES ) {
			my $pid = shift @pids;
			$ret{ $pidhash{$pid} } = _readoutput( $pid, $TMPDIR );
		}

		my $pid = fork();
		defined($pid) or croak("Cannot fork: $!\n");

		if ($pid) {
			push @pids, $pid;
			$pidhash{$pid} = $item;
		} else {
			$item = '"' . $item . '"';
			my @tmpret = cc_run_cmd( $cmd . " " . $item, %opts );
			nstore( \@tmpret, File::Spec->catfile( $TMPDIR, $$ ) );
			exit 0;
		}
	}
	foreach my $pid (@pids) {
		$ret{ $pidhash{$pid} } = _readoutput( $pid, $TMPDIR );
	}

	#( -d $TMPDIR ) and rmdir($TMPDIR) or croak "Failed to remove $TMPDIR !";
	return \%ret;
}

sub cc_run_parallel_cmds {
	my ( $cmdsref, %opts ) = @_;
	(%opts) or %opts = ();
	my $l = Log::Log4perl->get_logger();
	$l->trace("entering cc_run_cmd");

	$l->trace("MAX_CHILD_PROCESSES = $main::MAX_CHILD_PROCESSES");

	my $TMPDIR = File::Temp->newdir();

	my @pids;
	my @ret;
	my %pidhash;
	my $index = 1;
	foreach my $cmd (@$cmdsref) {
		if ( scalar(@pids) >= $main::MAX_CHILD_PROCESSES ) {
			my $pid = shift @pids;
			$ret[ $pidhash{$pid} - 1 ] = _readoutput( $pid, $TMPDIR );
		}

		my $pid = fork();
		defined($pid) or croak("Cannot fork: $!\n");
		if ($pid) {
			push @pids, $pid;
			$pidhash{$pid} = $index;
			$index++;
		} else {
			my @tmpret = cc_run_cmd( $cmd, %opts );
			nstore( \@tmpret, File::Spec->catfile( $TMPDIR, $$ ) );
			exit 0;
		}
	}
	foreach my $pid (@pids) {
		$ret[ $pidhash{$pid} - 1 ] = _readoutput( $pid, $TMPDIR );
	}

	#( -d $TMPDIR ) and rmdir($TMPDIR) or croak "Failed to remove $TMPDIR !";

	return \@ret;
}

sub _readoutput {
	my ( $pid, $TMPDIR ) = @_;
	waitpid( $pid, 0 );
	my $tmpfile = File::Spec->catfile( $TMPDIR, $pid );
	my $ret = retrieve($tmpfile);
	unlink $tmpfile;
	return $ret;
}

sub _run_cmd {
	my ($cmd) = @_;
	my $l = Log::Log4perl->get_logger();

	my $TMPDIR = File::Temp->newdir( File::Spec->catfile( File::Spec->tmpdir(), 'gimp_XXXXXXXXXX' ));
	my $tmpfile = File::Temp::tempnam( $TMPDIR, 'gimp' );
	my ( $rc, @stdout, $stderr );
	@stdout = qx [2>$tmpfile $cmd];
	$rc     = $?;

	open( my $fh, '<', $tmpfile ) or croak("Failed to open for reading: $tmpfile: $!\n");
	{
		local $/ = undef;
		$stderr = <$fh>;
	}

	close $fh;
	unlink $tmpfile;

	#( -d $TMPDIR ) and rmdir($TMPDIR) or croak "Failed to remove $TMPDIR !";

	return ( $rc, \@stdout, $stderr );
}

sub get_view_root {
	my ($viewtag) = @_;
	( $^O eq 'MSWin32' ) and die '\nnot supported yet\n';
	#return File::Spec->canonpath( File::Spec->catfile( $UNIX_VIEW_ROOT, $viewtag ) );
    return "/home/_svcmaven-dev/gitimports/git2cc/snapshots/${viewtag}";
}

1;
