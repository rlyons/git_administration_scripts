#!perl -w
use strict;
use warnings;
use Getopt::Long;
use XML::Simple;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Storable qw(dclone);
use File::Basename;
use File::Spec;
use CCUtils;
use RunCmd;
use GitImport;
use GitRepo;
use Carp qw(carp croak verbose);
use POSIX ":sys_wait_h";

# global variable. Set by map_env_values from property max_processes
our $MAX_CHILD_PROCESSES = 1;

# global variable. set by process_cmdline. used by get_logfile_name
our $LOG_FILE_NAME;

# Main program
my $cfg    = process_cmdline();
my $logger = Log::Log4perl->get_logger();

# Process PVOBs
if ( $cfg->{pvob} and ( keys %{ $cfg->{pvob} } ) ) {
	foreach my $pvob ( keys %{ $cfg->{pvob} } ) {
		$logger->info("Processing ClearCase UCM vob: $pvob");
		process_pvob( $cfg, $pvob );

	}
}

# Process Base clearcasevobs
if ( $cfg->{vob} and ( keys %{ $cfg->{vob} } ) ) {
	foreach my $vob ( keys %{ $cfg->{vob} } ) {
		$logger->info("Processing Base ClearCase vob: $vob");
		process_base_vob( $cfg->{vob}{$vob}, $vob, $cfg->{property} );
		$logger->info("Completed processing Base ClearCase vob: $vob");
	}
}

# End of Main program

# Subroutines
sub process_base_vob {
	my ( $cfg, $vob, $pref ) = @_;
	my $l             = Log::Log4perl->get_logger();
	my $vob_timestamp = cc_run_cmd("cleartool desc -fmt '%d' vob:$vob");
	$l->debug("Retrieving labels in vob: $vob");
	my @ret = cc_run_cmd( "cleartool lstype -kind lbtype -fmt '%d %n\n' -invob " . $vob );

	my @labels;
	foreach my $line ( sort @ret ) {
		my ( $timestamp, $label ) = split( ' ', $line );
		next if ( $timestamp eq $vob_timestamp );    # Skip default labels
		push @labels, $line;
	}
	$l->info( "Retrieved " . scalar(@labels) . " labels from vob: $vob" );

	foreach my $gitrepo ( sort keys %{ $cfg->{gitrepo} } ) {
		$l->info("Processing Git Repo: $gitrepo");
		process_git_repo( $cfg->{gitrepo}{$gitrepo}, $gitrepo, $vob_timestamp, \@labels, $pref );
	}
}

sub process_git_repo {
	my ( $cfg, $gitrepo, $repo_timestamp, $labelsref, $pref ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $selected_labels_ref = select_labels( $cfg, $labelsref );
	my $total = scalar( keys %$selected_labels_ref );
	$l->info("Selected $total labels to import into Git Repo: $gitrepo");

	# Import selected labels in the order they have been created
	my $index = 0;
	foreach my $line ( sort keys %$selected_labels_ref ) {
		$index++;
		my ( $timestamp, $label ) = split( ' ', $line );
		my $git_branch = $selected_labels_ref->{$line};
		$l->info("importing Label $index of $total in gitrepo: $gitrepo (branch $git_branch): $label ($timestamp)");
		import_label( $gitrepo, $repo_timestamp, $git_branch, $label, $timestamp, $pref, $cfg->{include}, $cfg->{branch}{$git_branch}{start_from} );
	}
	$logger->info("Completed importing selected labels for Git Repo: $gitrepo");
	return 1;
}

sub select_labels {
	my ( $cfg, $labelsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	my %selected_labels;
	my %patterns;

	# Process the labels first
	foreach my $git_branch ( keys %{ $cfg->{branch} } ) {
		if ( $cfg->{branch}{$git_branch}{label} and ( keys %{ $cfg->{branch}{$git_branch}{label} } ) ) {
			foreach my $inlabel ( sort keys %{ $cfg->{branch}{$git_branch}{label} } ) {
				my $found = 0;
				foreach my $item (@$labelsref) {
					my ( $timestamp, $label ) = split( ' ', $item );
					if ( $inlabel eq $label ) {
						$selected_labels{$item}
						  and $selected_labels{$item} ne $git_branch
						  and $l->logcroak("More than one branch is selecting the same label: $label Branch1: $selected_labels{$item} Branch2: $git_branch");
						$selected_labels{$item} = $git_branch;
						$found = 1;
						$l->trace("Selected label using label: $label (branch: $git_branch)");
						last;
					}
				}
				$found or $l->error("Could not find label in clearcase: $inlabel");
			}
		}

		# Collect all the patterns
		if ( $cfg->{branch}{$git_branch}{labelpattern} and ( keys %{ $cfg->{branch}{$git_branch}{labelpattern} } ) ) {
			foreach my $pattern ( sort keys %{ $cfg->{branch}{$git_branch}{labelpattern} } ) {
				$patterns{$pattern}
				  and $patterns{$pattern} ne $git_branch
				  and $l->logcroak("More than one branch is selecting the same labelpattern: $pattern Branch1: $patterns{$pattern} Branch2: $git_branch");
				$patterns{$pattern} = $git_branch;
			}
		}
	}

	# Sort patterns with longest one first, and then in ascending order
	foreach my $pattern ( sort { length($b) <=> length($a) or $a cmp $b } keys %patterns ) {
		$l->trace("Pattern: $pattern (branch: $patterns{$pattern})");
		foreach my $item (@$labelsref) {
			my ( $timestamp, $label ) = split( ' ', $item );
			if ( index( $label, $pattern ) >= 0 ) {
				if ( $selected_labels{$item} ) {    # This label has already been taken, skip it
					$l->trace("Skipping already selected label: $label");
					next;
				}
				$selected_labels{$item} = $patterns{$pattern};
				$l->trace("Selected label: $label using pattern: $pattern (branch: $patterns{$pattern})");
			}
		}
	}
	return \%selected_labels;
}

sub process_pvob {
	my ( $cfg, $pvob ) = @_;
	my $l              = Log::Log4perl->get_logger();
	my %stream_details = ();
	my %all_baselines;
	my %comps_for_stream;
	my %Mainlines;
	foreach my $stream ( keys %{ $cfg->{pvob}{$pvob}{stream} } ) {
		$stream = 'stream:' . $stream . '@' . $pvob;
		my ( $compsref, $ablsref ) = cc_get_components_baselines($stream);
		$comps_for_stream{$stream} = $compsref;
		foreach my $bl ( keys %$ablsref ) {
			$all_baselines{ $ablsref->{$bl}{sortorder} }{$stream} = $ablsref->{$bl};
		}
	}
	$l->trace( "all_baselines:\n" . Dumper( \%all_baselines ) );

	# Special processing for CIT _Mainline_integration streams
	my $Extratagsref = cc_find_cit_build_baselines( \%comps_for_stream );
	$l->trace( "cc_find_cit_build_baselines:\n" . Dumper($Extratagsref) );

	my $i        = 0;
	my $blscount = scalar( keys %all_baselines );

	my %waiting;
	my %pids;    # Each pid in here has 3 entries - component, baseline, and pid
	do {
		foreach my $index ( sort keys %all_baselines ) {
			my ( $stream, $blref ) = %{ $all_baselines{$index} };
			my $git_repo_name = get_git_repo_name( $blref->{component}{root_dir} );
			$l->trace( 'Git Repo is ', $git_repo_name );

			# Create GIT REPO if it doesn't exist
			my $repodir = git_create_repo( $git_repo_name, $cfg->{property}, $blref->{component}{create_timestamp} );
			my $git_tag = shortname( $blref->{baseline} );
			git_check_tag_exists( $repodir, $git_tag ) or next;

			unless ( $blref->{component}{is_composite} ) {
				my $git_branch = undef;
				my $scfg       = $cfg->{pvob}{$pvob}{stream}{ shortname($stream) };
				$scfg and $git_branch = $scfg->{git_branch};
				$git_branch ||= shortname($stream);

				if ( $blref->{is_foundation} and git_check_branch_exists( $repodir, $git_branch ) == 0 ) {
					git_try_branch_checkout( $repodir, $git_branch ); # Try checkout to localize the remote branch if it exists
					if ( git_check_branch_exists( $repodir, $git_branch ) == 0 ) {
						git_create_branch_from_tag( $repodir, $git_branch, $git_tag );
					}
				}
			}
			$i++;
			delete $all_baselines{$index};
			if ($blref->{component}{is_composite}) { 
				$l->info("Skipping baseline on composite component: ($i of $blscount) has already been imported in gitrepo: $git_repo_name");
			}else { 
			$l->info("Tag $git_tag ($i of $blscount) has already been imported in gitrepo: $git_repo_name");
			}
			next;
		}

		foreach my $index ( sort keys %all_baselines ) {
			my ( $stream, $blref ) = %{ $all_baselines{$index} };
			if ( ( scalar( keys %pids ) / 3 ) >= $main::MAX_CHILD_PROCESSES ) {
				waitforone( \%pids );
			}

			if ( $pids{ $blref->{component}{component} } or $pids{ $blref->{baseline} } ) {
				$logger->info("Waiting for one of the running processes to finish.");
				waitforone( \%pids );
				next;
			}

			$i++;
			delete $all_baselines{$index};
			$waiting{$index}{$stream} = $blref;
			$logger->info("Starting import of baseline $i of $blscount: $blref->{baseline}");
			last;
		}

		foreach my $idx ( sort keys %waiting ) {    # Check if we can run any of the waiting ones
			my ( $stream, $blref ) = %{ $waiting{$idx} };
			$logger->trace("Processing: $idx|$stream|$blref->{component}{component}|$blref->{baseline}");

			my @extra_tags = ();
			      ($Extratagsref)
			  and (%$Extratagsref)
			  and $Extratagsref->{$stream}
			  and $Extratagsref->{$stream}{ $blref->{baseline} }
			  and @extra_tags = @{ $Extratagsref->{$stream}{ $blref->{baseline} } };

			my $git_branch = undef;
			my $scfg       = $cfg->{pvob}{$pvob}{stream}{ shortname($stream) };
			$scfg and $git_branch = $scfg->{git_branch};

			(@extra_tags) and $logger->debug( "Extra Tags: ", join( ' ', @extra_tags ) );
			my $pid = start_blimport( $stream, $blref, $cfg->{property}, $git_branch, @extra_tags );
			delete $waiting{$idx};

			$pids{$pid}                             = $blref->{component}{component} . '|' . $blref->{baseline};
			$pids{ $blref->{component}{component} } = $pid;
			$pids{ $blref->{baseline} }             = $pid;
		}
	} while ( ( keys %all_baselines ) or ( keys %waiting ) );

	while ( keys %pids ) {
		waitforone( \%pids );
	}
	$l->info("Completed processing $blscount baselines for $pvob");

	# Get the latest stuff on the stream
	foreach my $stream ( keys %comps_for_stream ) {
		my $git_branch = undef;
		my $scfg       = $cfg->{pvob}{$pvob}{stream}{ shortname($stream) };
		$scfg and $git_branch = $scfg->{git_branch};

		$l->info("Importing latest on the stream $stream");
		import_latest_on_stream( $stream, $comps_for_stream{$stream}, $cfg->{property}, $git_branch );
		$l->info("=== Import is complete for $stream ===");
	}
}

# Start and wait for import
sub start_blimport {
	my (@args) = @_;
	my $pid = fork();
	defined($pid) or croak("Cannot fork: $!\n");

	if ($pid) {
		return $pid;
	} else {
		srand();    #re-initialize random number generator so that different process don't end up with the same tmp files
		eval { import_baseline(@args); } or exit 1;
		exit 0;
	}
	return;
}

# Wait for one of the processes to finish
sub waitforone {
	my ($pidsref) = @_;
	my $l = Log::Log4perl->get_logger('waitforone');
	( $pidsref and keys %$pidsref ) or return;
	while (1) {
		foreach my $pid ( keys %$pidsref ) {
			next if ( $pid =~ /^component:/ or $pid =~ /^baseline:/ );
			$l->trace("Checking: $pid");
			if ( waitpid( $pid, WNOHANG ) > 0 ) {
				my $rc = $?;
				if ( $rc >> 8 ) {
					die "Process $pid died !\n$pidsref->{$pid}";
				}

				$logger->info("Completed: $pidsref->{$pid}");
				foreach my $x ( split( /\|/, $pidsref->{$pid} ) ) {
					delete $pidsref->{$x};
				}
				delete $pidsref->{$pid};
				return;
			}
		}
		sleep 1;    # Let the processes run
	}
}

sub get_logfile_name {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
	my $tstamp = sprintf( ".%d%02d%02d.%02d%02d%02d.log", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );
	$LOG_FILE_NAME =~ s/\.xml$/$tstamp/;
	print "Logfile is: $LOG_FILE_NAME\n";
	return $LOG_FILE_NAME;
}

sub process_cmdline {
	my ( $opt_f, $opt_l, $start, $end );
	GetOptions( 'f=s' => \$opt_f, 'l=s' => \$opt_l, 'start=i' => \$start, 'end=i' => \$end );

	$opt_f or usage();
	( -f $opt_f ) or die " Config File : '$opt_f' does not exist !\n ";

	# initialize logger
	my $l_conf_file = File::Spec->canonpath( File::Spec->catfile( File::Basename::dirname($0), '..', 'config', 'logger.conf' ) );
	( $opt_l and -f $opt_l ) and $l_conf_file = $opt_l;

	if ( -f $l_conf_file ) {
		$LOG_FILE_NAME = File::Basename::basename($opt_f);
		Log::Log4perl->init($l_conf_file);
	} else {
		Log::Log4perl->easy_init($DEBUG);
	}
	my $l = Log::Log4perl->get_logger();

	$l->info( "Reading input file : " . $opt_f );
	my $xs1 = XML::Simple->new();
	my $cfg = $xs1->XMLin( $opt_f, ForceArray => 1 );

	$l->trace( Dumper($cfg) );

	foreach my $pvob ( keys %{ $cfg->{pvob} } ) {
		ref( $cfg->{pvob}{$pvob}{stream} ) eq 'HASH'
		  or Log::Log4perl->get_logger()->logcroak("\nCannot parse: $pvob\n");
	}

	foreach my $item ( keys %{ $cfg->{property} } ) {
		ref( $cfg->{property}{$item} ) eq 'HASH'
		  or Log::Log4perl->get_logger()->logcroak("Cannot parse property: $item");

		if ( $item eq 'max_processes' ) {
			( $cfg->{property}{$item}{value} > 0 )
			  or Log::Log4perl->get_logger()->logcroak("property: $item needs to be positive !");
			$MAX_CHILD_PROCESSES = $cfg->{property}{$item}{value};
		}
	}

	$cfg->{property}{view_prefix}{value} or croak("view_prefix is not defined or is empty !");
	$cfg->{property}{view_host}{value}   or croak("view_host is not defined or is empty !");
	$cfg->{property}{view_stgloc}{value} or croak("view_stgloc is not defined or is empty !");

	$l->debug("Mapping env values ");
	map_env_values( $cfg, $start, $end );
	$l->trace( Dumper($cfg) );

	return $cfg;
}

sub map_env_values {
	my ( $cfg, $start, $end ) = @_;
	my $found;
	my $envref = dclone( \%ENV );
	do {
		$found = 0;
		foreach my $item ( keys %{ $cfg->{property} } ) {
			if ( $cfg->{property}{$item}{value} !~ m/\$\{/ ) {
				$envref->{$item} = $cfg->{property}{$item}{value};
			}
		}
		foreach my $item ( keys %{ $cfg->{property} } ) {
			next if defined( $envref->{$item} );
			foreach my $key ( keys %$envref ) {
				if ( $cfg->{property}{$item}{value} =~ s/\$\{\Q$key\E\}/$envref->{$key}/ ) {
					$found = 1;
					last;
				}
			}
			last if $found;
		}
	} while $found;
	$cfg->{property}{start_baseline}{value} = $start;
	$cfg->{property}{end_baseline}{value}   = $end;
}

sub usage {
	print STDERR " \nUsage : $0 -f <config file> [ -l logger_config_file ] \n \n ";
	exit 127;
}
