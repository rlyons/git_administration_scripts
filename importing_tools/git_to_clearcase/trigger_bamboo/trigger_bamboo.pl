#!perl -w 
use strict;
use warnings;
use Getopt::Long;
use Storable qw(dclone);
use Log::Log4perl qw(:easy);
use File::Basename;
use File::Spec;
use File::Copy;
use File::Path qw(make_path);
use Data::Dumper;
use Sys::Hostname;
use LWP::Simple;
use Time::Local;

# Main program
my ( $BASEDIR, $COMMITS_DIR, $MAPPING_FILE, $LOGGER_CONF ) = get_cmdline_options();
my $LOCK_DIR = File::Spec->catfile( $COMMITS_DIR, '.lock' );

initialize_logger($LOGGER_CONF);
check_env_vars();

my $mappingsref = read_mapping_file($MAPPING_FILE);

lock_dir($LOCK_DIR);

my $commitsref = get_commits($COMMITS_DIR);
trigger_builds( $commitsref, $BASEDIR, $COMMITS_DIR );

unlock_dir($LOCK_DIR);

# End of Main program

# Subroutines
sub trigger_builds {
	my ( $commitsref, $basedir, $commitsdir ) = @_;
	my $RUNNING_TIMEOUT = 60 * 60 * 12;                  # 12 hours
	my $QUEUED_TIMEOUT  = 60 * 60 * 4;                   # 4 hours
	my $l               = Log::Log4perl->get_logger();
	foreach my $repo ( sort keys %$commitsref ) {
		foreach my $branch ( sort keys %{ $commitsref->{$repo} } ) {
			my $build_key = $mappingsref->{$repo}{$branch}{build_key};
			unless ($build_key) {
				foreach my $timestamp ( sort keys %{ $commitsref->{$repo}{$branch}{commits} } ) {
					move_to_nomapping( $commitsdir, $repo, $branch, $commitsref->{$repo}{$branch}{commits}{$timestamp}{filename} );
				}
				next;
			}

			$l->debug("====== Repo: '$repo' Branch: '$branch' BuildKey: '$build_key' =========");
			my ( $last_build_started_time, $is_build_running ) = get_bamboo_build_status( $build_key, $basedir );
			$l->debug("last_build_started_time: $last_build_started_time");
			$l->debug("       is_build_running: $is_build_running");
			my $last_build_submit_time = undef;
			foreach my $commit ( sort keys %{ $commitsref->{$repo}{$branch}{commits} } ) {
				$l->debug(" =========== CommitTime: $commit");

				my $commitfile = $commitsref->{$repo}{$branch}{commits}{$commit}{filename};
				if ( $last_build_started_time gt $commit ) {    # Build is completed for this commit.
					$l->debug( "Build is completed for '$repo':'$branch':'$commit'.\n", "Removing $commitfile." );
					unlink $commitfile or $l->logcroak( "Failed to remove: ", $commitfile );
					next;
				}

				my $build_submit_time = $commitsref->{$repo}{$branch}{commits}{$commit}{build_submit_time};
				$last_build_submit_time ||= $build_submit_time;
				if ($is_build_running) {                        # This build is running. Wait for it until timeout
					my $sincecommit;
					if ($last_build_submit_time) {
						$sincecommit = " since the last submit time of $last_build_submit_time";
					} else {
						$sincecommit = " since the last commit time of $commit";
					}
					my $running_time = get_timediff( $last_build_submit_time || $commit );
					if ( $running_time > $RUNNING_TIMEOUT ) {
						$l->error( "Repo: '$repo' Branch: '$branch' BuildKey: '$build_key' has been running for " . $RUNNING_TIMEOUT / 3600 . " hours$sincecommit." );
					} else {
						$l->debug("Repo: '$repo' Branch: '$branch' BuildKey: '$build_key' has been running for $running_time seconds$sincecommit.");
					}
					next;
				}

				if ($build_submit_time) {    # This build has already been submitted. Wait for the build to kick off until timeout
					$l->debug("      build_submit_time: $build_submit_time");
					my $queued_time = get_timediff($build_submit_time);
					if ( $queued_time > $QUEUED_TIMEOUT ) {
						$l->error( "Repo: '$repo' Branch: '$branch' BuildKey: '$build_key' has been in queue for " . $QUEUED_TIMEOUT / 3600 . " hours." );
					} else {
						$l->debug("Repo: '$repo' Branch: '$branch' BuildKey: '$build_key' has been queued for $queued_time seconds.");
					}
					next;
				}

				kick_off_build( $basedir, $build_key );
				write_to_file( $commitfile, "build_submit_time=" . get_timestamp() );    # Record the submit time
				last;                                                                    # Do not process anymore builds for this repo,branch
			}
		}
	}
}

sub kick_off_build {
	my ( $basedir, $build_key ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->debug("Submitting bamboo build for $build_key");
	my $cmd    = get_bamboo_cli_command_str( $basedir, $build_key, 'executeBuild' );
	my @output = qx($cmd);
	my $rc     = $?;
	$rc and $l->logdie( "Failed to run executeBuild command:\n", $!, "\n" );
}

sub get_timediff {
	my ( $start_time, $end_time ) = @_;

	$start_time = parse_date_time($start_time);

	if ($end_time) {
		$end_time = parse_date_time($end_time);
	} else {
		$end_time = timegm( localtime( time() ) );
	}

	my $timediff = $end_time - $start_time;
	return $timediff;
}

sub parse_date_time {
	my $str = shift;
	my $l   = Log::Log4perl->get_logger();

	my ( $year, $mon, $mday, $hour, $min, $sec ) = ( $str =~ m/(\d\d\d\d)(\d\d)(\d\d)_(\d\d)(\d\d)(\d\d)/ )
	  or $l->logcroak( "Failed to parse timestamp: '", $str, "'" );

	my $parsed = timegm( 0 + $sec, 0 + $min, 0 + $hour, 0 + $mday, $mon - 1, $year - 1900 );    # year - just like localtime, month is zero based - like localtime
	$l->trace("Parsed $str to $parsed");
	return $parsed;
}

sub get_bamboo_build_status {
	my ( $build_key, $basedir ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug( "Getting last bamboo build for ", $build_key );
	my $last_build_time = get_last_bamboo_build_time( $build_key, $basedir );

	my $is_running = get_bamboo_build_running_status( get_bamboo_browse_url($build_key) );
	if ( $is_running eq ' was successful ' ) {
		$is_running = 0;
		$l->debug("$build_key is not running.");
	} elsif ( $is_running eq ' is building ' ) {
		$is_running = 1;
		$l->debug("$build_key is running.");
	} else {
		$l->logcroak("Unknown build status for BuildKey: '$build_key'");
	}

	return ( $last_build_time, $is_running );
}

sub move_to_nomapping {
	my ( $commitsdir, $reponame, $branchname, $filename ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $nomappingdir = File::Spec->catfile( $commitsdir, '.nomapping', $reponame, $branchname );
	make_path( $nomappingdir, { verbose => 1 } );
	move( $filename, $nomappingdir ) or $l->logcroak( "Failed to move: ", $filename, " to ", $nomappingdir );
	$l->debug( "Moved '", $filename, "' to ", $nomappingdir );
}

sub get_bamboo_build_running_status {
	my ($url) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->debug( "Getting current bamboo build status for ", $url );

	my $content = LWP::Simple::get($url);
	$content or $l->logcroak( "Failed to get bamboo build status using URL:\n", $url );
	foreach ( split( "\n", $content ) ) {
		/^<div id="buildStatus">/ or next;
		s/.*Latest Status://      or next;
		/<\/a>([^<>]+)<\/p>/ and return $1;
	}
	return;
}

sub get_last_bamboo_build_time {
	my ( $build_key, $basedir ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->debug("Getting build status for $build_key");
	my $cmd    = get_bamboo_cli_command_str( $basedir, $build_key, 'getLatestBuildResults' );
	my @output = qx($cmd);
	my $rc     = $?;
	$rc and $l->logdie( "Failed to run getLatestBuildResults command:\n", $!, "\n" );

	chomp @output;
	$l->trace( "Output of getLatestBuildResults command:\n", Dumper( \@output ) );

	my ($build_time) = grep { s/\s+buildTime[\s\.:]+// } @output;
	$build_time =~ s/[-:]//g;
	$build_time =~ s/ /_/;
	return $build_time;
}

# Initializes the logger
# Returns the directory containing commits and mapping file
sub get_cmdline_options {
	my ( $opt_b, $opt_d, $opt_l, $opt_m );
	GetOptions( 'b=s' => \$opt_b, 'd=s' => \$opt_d, 'm=s' => \$opt_m, 'l=s' => \$opt_l );

	$opt_m or usage();
	$opt_b or usage();
	$opt_d or usage();

	$opt_m = File::Spec->rel2abs($opt_m);
	$opt_b = File::Spec->rel2abs($opt_b);
	$opt_d = File::Spec->rel2abs($opt_d);

	( -f $opt_m ) or die "Mapping File : '$opt_m' does not exist !\n ";
	( -d $opt_b ) or make_path( $opt_b, { verbose => 1 } );
	( -d $opt_d ) or make_path( $opt_d, { verbose => 1 } );

	return ( $opt_b, $opt_d, $opt_m, $opt_l );
}

sub get_timestamp {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
	my $tstamp = sprintf( "%d%02d%02d_%02d%02d%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );
	return $tstamp;
}

# Generates a log file name based on timestamp
sub get_logfile_name {
	my $log_dir = shift;

	# If log dir is not specified, use the script_dir/log as default.
	$log_dir ||= File::Spec->catfile( File::Basename->dirname( File::Spec->rel2abs($0) ), 'log' );
	( -d $log_dir ) or mkdir $log_dir or die "Failed to mkdir: $log_dir\n";

	my $tstamp = get_timestamp() . '.log';
	print "Logfile is: $tstamp\n";
	return File::Spec->catfile( $log_dir, $tstamp );
}

sub usage {
	print STDERR " \nUsage : $0 -b base_dir -d commits_dir -m mapfile [ -l logger_config_file ] \n \n ";
	exit 127;
}

# Reads the 'commit' files in the given directory
sub get_commits {
	my $indir = shift;
	my $l     = Log::Log4perl->get_logger();

	$l->debug( "Looking for commits in directory: ", $indir );

	my @dirs = grep { !/^\./ } get_dir_contents($indir);    # Ignore items beginning with '.'

	$l->debug( "Found ", scalar(@dirs), " commit(s)." );

	my %commits;
	foreach my $x (@dirs) {
		( my ( $reponame, $timestamp, $branch ) = ( $x =~ /^(.*?)_([_0-9\.]+)_(.*?)$/ ) )
		  or $l->logcroak( "Cannot parse filename: ", $x );

		my $filename = File::Spec->catfile( $indir, $x );
		$commits{$reponame}{$branch}{commits}{$timestamp}{filename} = $filename;
		unless ( -z $filename ) {
			open( my $fh, '<', $filename ) or $l->logcroak( "Failed to open for reading: ", $filename, $!, "\n" );
			while ( my $line = <$fh> ) {
				chomp $line;
				my ( $key, $val ) = split( /=/, $line );
				$commits{$reponame}{$branch}{commits}{$timestamp}{$key} = $val;
			}
			close $fh;
		}
	}
	$l->trace( "COMMITS:\n", Dumper( \%commits ) );
	return \%commits;
}

# initialize logger
sub initialize_logger {
	my $opt_l = shift;
	if ( $opt_l and -f $opt_l ) {
		Log::Log4perl->init($opt_l);
	} else {
		Log::Log4perl->easy_init($DEBUG);
	}

	my $l = Log::Log4perl->get_logger();
	$l->debug("Logger initialized.");
	return $l;
}

# Verifies environment variables exist
sub check_env_vars {
	my $l = Log::Log4perl->get_logger();

	$l->debug("Checking environment variables.");
	for my $var (qw(JAVA_EXECUTABLE_PATH BAMBOO_CLI_JAR BAMBOO_URL BAMBOO_USER BAMBOO_PASSWORD )) {
		$ENV{$var} or $l->logcroak("Environment Variable: $var is not defined!");
	}
	$l->debug("Checking environment variables check passed.");
}

# Read the 'mapping' file
sub read_mapping_file {
	my ($mapping_file) = @_;
	my $l = Log::Log4perl->get_logger();

	my %mappings;
	my $count = 0;

	$l->debug( "Reading mapping file: ", $mapping_file );
	open( my $fd, '<', $mapping_file ) or $l->logcroak( "Failed to open for reading: ", $mapping_file, $!, "\n" );
	while ( my $line = <$fd> ) {
		chomp $line;
		$line =~ s/^\s+//;    # Remove blanks at the beginning of the line
		$line =~ s/\s+$//;    # Remove blanks at the end of the line
		next unless $line;    # Ignore blank lines
		next if $line =~ /^#/;    # Ignore comments
		my ( $repo, $branch, $build_key ) = split( /\s+/, $line );
		( $repo and $branch and $build_key ) or $l->logcroak( "Invalid line in mapping file: '", $line, "'" );
		$mappings{$repo}{$branch}{build_key} = $build_key;
		$count++;
	}
	close $fd;

	$l->debug("Found: $count mappings.");
	$l->trace( "MAPPINGS:\n", Dumper( \%mappings ) );
	return \%mappings;
}

# Writes the given lines to the given file
sub write_to_file {
	my ( $file, @lines ) = @_;
	my $l   = Log::Log4perl->get_logger();
	my $dir = dirname($file);
	( -d $dir )
	  or make_path( $dir, { verbose => 1, mode => 0775 } )
	  or $l->logcroak("Failed to make dir: '$dir' : $!");

	open( my $fd, '>', $file ) or $l->logcroak("Failed to write: '$file' : $!");
	foreach my $line (@lines) {
		print $fd $line, "\n";
	}
	close $fd;
}

# Locks a given directory by creating sub-directories
sub lock_dir {
	my $l          = Log::Log4perl->get_logger();
	my $lockdir    = shift;
	my $hostname   = hostname();
	my $hostdir    = File::Spec->catfile( $lockdir, $hostname );
	my $processdir = File::Spec->catfile( $hostdir, $$ );
	my $TIMEOUT    = 15 * 60;                                      # 15 minutes

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
				$process_is_alive = 0;
				next;    # we have successfully removed a stale lock, attempt to lock it now
			}
		}
		sleep 1;
		( $count % 60 ) or $l->debug("Waiting to acquire: $lockdir");
		( $process_is_alive and $count > $TIMEOUT ) and $l->logcroak("Timedout waiting for: $lockdir");
		$count++;
	}
	$l->debug("Acquired: $lockdir after $count seconds.");
}

# Unlocks a given directory
sub unlock_dir {
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

# Checks if the process is still alive on a given host
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

# Returns directory contents
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

sub get_bamboo_cli_command_str {
	my ( $basedir, $build_key, $action ) = @_;

	my $cmd = $ENV{JAVA_EXECUTABLE_PATH};
	$cmd .= ' -jar ' . File::Spec->catfile( $basedir, $ENV{BAMBOO_CLI_JAR} );
	$cmd .= ' --server ' . $ENV{BAMBOO_URL};
	$cmd .= ' --user ' . $ENV{BAMBOO_USER};
	$cmd .= ' --password ' . $ENV{BAMBOO_PASSWORD};
	$cmd .= ' --action ' . $action;
	$cmd .= ' --build ' . $build_key;

	return ($cmd);
}

sub get_bamboo_browse_url {
	my $build_key = shift;
	return $ENV{BAMBOO_URL} . '/browse/' . $build_key;
}
