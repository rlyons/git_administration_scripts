#!perl -w
package RepoLock;
use strict;
use warnings;
use File::Spec;
use Sys::Hostname;
use Log::Log4perl;

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  lock_repo
  unlock_repo
);

my $TIMEOUT = 15 * 60;    # 15 minutes

sub lock_repo {
	my $l          = Log::Log4perl->get_logger();
	my $lockdir    = shift;
	my $hostname   = hostname();
	my $hostdir    = File::Spec->catfile( $lockdir, $hostname );
	my $processdir = File::Spec->catfile( $hostdir, $$ );

	my $dircreated       = 0;
	my $count            = 0;
	my $process_is_alive = 0;
	while (1) {
		if ( mkdir($lockdir) ) {
			mkdir($hostdir)    or next;
			mkdir($processdir) or next;
			last;
		}
		if ( my $ret = check_process_alive( $lockdir, $hostname ) ) {
			if ( $ret == 1 ) {
				$process_is_alive = 1;
			} else {
				$l->debug("Removed Stale Lock: $lockdir");
				next;    # we have successfully removed a stale lock, attempt to lock it now
			}
		}
		sleep 1;
		( $count % 60 ) or $l->debug("Waiting to acquire: $lockdir");
		( $process_is_alive or $count > $TIMEOUT ) and $l->logcroak("Timedout waiting for: $lockdir");
		$count++;
	}
	$l->debug("Acquired: $lockdir after $count seconds.");
}

sub unlock_repo {
	my $lockdir = shift;
	my $l       = Log::Log4perl->get_logger();

	my $hostname   = hostname();
	my $hostdir    = File::Spec->catfile( $lockdir, $hostname );
	my $processdir = File::Spec->catfile( $hostdir, $$ );

	rmdir($processdir) or $l->logcroak("Failed to rmdir: $processdir : $!\n");
	rmdir($hostdir)    or $l->logcroak("Failed to rmdir: $hostdir : $!\n");
	rmdir($lockdir)    or $l->logcroak("Failed to rmdir: $lockdir : $!\n");
	$l->debug("$lockdir released.");
	sleep 1;    # Let other processes acquire the lock
}

sub check_process_alive {
	my ( $lockdir, $hostname ) = @_;
	my $l = Log::Log4perl->get_logger();
	foreach my $host ( get_dir_contents($lockdir) ) {
		my $hdir = File::Spec->catfile( $lockdir, $host );
		foreach my $process ( get_dir_contents($hdir) ) {
			if ( $host ne $hostname ) {
				$l->debug("Cannot check if process is active on different host: $host process: $process");
				next;
			}
			if ( kill( 0, $process ) ) {    # process is alive
				$l->debug("process $process is alive on host: $host");
				return 1;
			}
			my $pdir = File::Spec->catfile( $hdir, $process );
			rmdir($pdir) and rmdir($hdir) and rmdir($lockdir) and return 2;
		}
	}
	return;
}

sub get_dir_contents {
	my $indir = shift;
	opendir( my $dh, $indir ) or return;
	my @dirs = readdir($dh);
	close($dh);
	my @ret;
	foreach my $x (@dirs) {
		next if ( $x eq '.' );
		next if ( $x eq '..' );
		push @ret, $x;
	}
	return @ret;
}
