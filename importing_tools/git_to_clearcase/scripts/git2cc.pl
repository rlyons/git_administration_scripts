#!perl -w
use strict;
use warnings;
use Getopt::Long;
use XML::Simple;
use Log::Log4perl qw(:easy);
use Storable qw(dclone);
use File::Basename;
use File::Spec;
use Data::Dumper;
use Carp qw(carp croak verbose);
use CCUtils;
use GitRepo;
use CCImport;

# global variable. Set by map_env_values from property max_processes
our $MAX_CHILD_PROCESSES = 1;

# global variable. set by process_cmdline. used by get_logfile_name
our $LOG_FILE_NAME;

# Main program
my $cfg    = process_cmdline();
my $logger = Log::Log4perl->get_logger();
$logger->info("Initialization Completed !");
$logger->info("Parent process is: " . getppid());
if ( $cfg->{pvob} and ( keys %{ $cfg->{pvob} } ) ) {
	foreach my $pvob ( sort keys %{ $cfg->{pvob} } ) {
		$logger->info("Processing PVOB: $pvob");
		process_pvob( $cfg->{pvob}{$pvob}, $pvob, $cfg->{property} );
		$logger->info("Completed Processing PVOB: $pvob");
	}
}

# End of Main program

# Subroutines
sub process_pvob {
	my ( $cfg, $pvob, $pref ) = @_;
	my $l = Log::Log4perl->get_logger();

	return unless $cfg->{gitrepo};
	return unless ( keys %{ $cfg->{gitrepo} } );

	git_clone_repos( $pref->{repodir}{value}, $cfg->{gitrepo} );

	my $tagsref = git_get_repo_tags( $pref->{repodir}{value}, sort keys %{ $cfg->{gitrepo} } );
	$l->trace( 'Tags: ' . Dumper($tagsref) );

	my ( $mappingsref, $componentsref ) = get_branch_mappings( $cfg, $pvob );
	$l->trace( 'Mappings: ' . Dumper($mappingsref) );
	$l->trace( 'Components: ' . Dumper($componentsref) );

	my $root_dirs_ref = cc_get_root_dirs_for_components( keys %$componentsref );
	$l->trace( 'RootDirs: ' . Dumper($root_dirs_ref) );

	my $blsref = cc_get_baselines_for_components( sort keys %$componentsref );
	$l->trace( 'Baselines: ' . Dumper($blsref) );

	my $commitsref = get_tags_to_import( $tagsref, $mappingsref, $root_dirs_ref, $blsref );
	$l->trace( 'TAGSToImport: ' . Dumper($commitsref) );

	my $count = 0;
	my $total = scalar( keys %$commitsref );

	foreach my $timestamp ( sort keys %$commitsref ) {
		$count++;
		my $cref = $commitsref->{$timestamp};

		$l->info("Importing commit $count of $total: from gitrepo:$cref->{gitrepo} to $cref->{component_root} ($cref->{stream})");
		import_commit_to_clearcase( $cref, $pref );
	}
	$total or $l->info("No new tags to import.");
}

sub import_commit_to_clearcase {
	my ( $cref, $pref ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $repodir = File::Spec->catfile( $pref->{repodir}{value}, $cref->{gitrepo} );

	# Prepare git repo
	git_lock_repo($repodir);

	my $branchesref = git_get_branches($repodir);
	$l->trace( Dumper($branchesref) );

	my $git_branch = $pref->{view_prefix}{value} . '_' . shortname( $cref->{stream} );
	my $brsref     = git_get_branches($repodir);
	if ( $brsref->{$git_branch} ) {
		git_checkout_branch( $repodir, $cref->{gitbranch} );
		git_delete_branch( $repodir, $git_branch );
	}

	git_create_branch( $repodir, $git_branch, $cref->{commitsha} );
	git_checkout_branch( $repodir, $git_branch );

	my $view = $pref->{view_prefix}{value} . '_' . shortname( $cref->{stream} );
	cc_remove_view( $view, $pref->{view_stgloc}{value} );
	cc_create_view( $view, $pref->{view_host}{value}, $pref->{view_stgloc}{value}, $cref->{stream} );
	#cc_startview($view);
	cc_mountvobs( cc_get_source_vobs_for_comp_dirs( $cref->{component_root} ) , $view );

	my $activity = $cref->{activity};
	unless ($activity) {
		$activity = $cref->{commitsha};
		if ( cc_check_activity_exists( $view, $activity ) == 0 ) {
			cc_create_activity( $view, $activity );
		}
	}
	cc_set_activity( $view, $activity );

	my $changes = cc_import_from_git( $repodir, $view, $cref->{component_root} );
	foreach my $tag ( @{ $cref->{tags} } ) {
		cc_apply_baseline( $view, $cref->{component}, $tag, $changes );
		$changes = 0;       # The second time - you need to use -identical
	}

	cc_remove_view( $view, $pref->{view_stgloc}{value} );

	git_checkout_branch( $repodir, $cref->{gitbranch} );
	git_delete_branch( $repodir, $git_branch );
	git_unlock_repo($repodir);
}

sub get_tags_to_import {
	my ( $tagsref, $mappingsref, $root_dirs_ref, $blsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	my %ret;
	foreach my $gitrepo ( sort keys %$tagsref ) {
		my $component = $mappingsref->{$gitrepo}{component};
		foreach my $commit ( sort keys %{ $tagsref->{$gitrepo} } ) {
			my ( $authorts, $committs, $commitsha ) = split( '_', $commit );
			my @tags = sort keys %{ $tagsref->{$gitrepo}{$commit} };
			foreach my $tag (@tags) {
				my ( $branch, $stream, $brtag, $activity ) = ( ('') x 4 );
				if ( index( $tag, '!' ) > 0 ) {
					( $branch, $brtag ) = split( '!', $tag );

					      $mappingsref->{$gitrepo}
					  and $mappingsref->{$gitrepo}{branch}
					  and $mappingsref->{$gitrepo}{branch}{$branch}
					  and do {
						$mappingsref->{$gitrepo}{branch}{$branch}{stream}
						  and $stream = $mappingsref->{$gitrepo}{branch}{$branch}{stream};
						$mappingsref->{$gitrepo}{branch}{$branch}{activity}
						  and $activity = $mappingsref->{$gitrepo}{branch}{$branch}{activity};
					  };
				} else {
					$brtag = $tag;
				}

				# Check if baseline already exists for this tag
				if ( $blsref->{$brtag} ) {

					# make sure that the tag's stream and component correspond to
					# clearcase's stream and component for the baseline
					if ( $blsref->{$brtag}{component} ne $component ) {
						my $str = "    Mismatched Components !\n";
						$str .= "In GITREPO: $gitrepo ($component)\n";
						$str .= "    TAG: $tag corresponds to $blsref->{$brtag}{baseline} ($blsref->{$brtag}{component})\n";

						$l->logcroak($str);
					}
					if ( (! $mappingsref->{$gitrepo}{branch}{$branch}{ignore_stream_mismatch}) and $stream and $blsref->{$brtag}{stream} ne $stream ) {
						my $str = "    Mismatched Streams !\n";
						$str .= "In GITREPO: $gitrepo ($stream)\n";
						$str .= "    TAG: $tag corresponds to $blsref->{$brtag}{baseline} ($blsref->{$brtag}{stream})\n";

						$l->logcroak($str);
					}
					next;    # Skip importing this tag since a baseline already exists
				}

				next unless $stream;

				push @{ $ret{$commit}{tags} }, $brtag;
				$ret{$commit}{gitrepo}        = $gitrepo;
				$ret{$commit}{gitbranch}      = $branch;
				$ret{$commit}{commitsha}      = $commitsha;
				$ret{$commit}{stream}         = $stream;
				$ret{$commit}{activity}       = $activity;
				$ret{$commit}{component_root} = $root_dirs_ref->{$component};
				$ret{$commit}{component}      = $component;
			}
		}
	}
	return \%ret;
}

sub get_branch_mappings {
	my ( $cfg, $pvob ) = @_;
	my $l = Log::Log4perl->get_logger();

	my %mappings;
	my %components;

	# Map the branch definitions at the pvob level for all the git repos
	if ( $cfg->{branch} and ( keys %{ $cfg->{branch} } ) ) {
		foreach my $branch ( keys %{ $cfg->{branch} } ) {
			foreach my $gitrepo ( keys %{ $cfg->{gitrepo} } ) {
				my $stream   = $cfg->{branch}{$branch}{stream};
				my $activity = $cfg->{branch}{$branch}{activity};
				$stream ||= $branch;
				$stream                                        = 'stream:' . $stream . '@' . $pvob;
				$mappings{$gitrepo}{branch}{$branch}{stream}   = $stream;
				$mappings{$gitrepo}{branch}{$branch}{activity} = $activity;
				$mappings{$gitrepo}{branch}{$branch}{ignore_stream_mismatch} = $cfg->{branch}{$branch}{ignore_stream_mismatch};
			}
		}
	}

	# Over-ride the branch definitions from individual git repos
	foreach my $gitrepo ( keys %{ $cfg->{gitrepo} } ) {
		my $component = 'component:' . $cfg->{gitrepo}{$gitrepo}{component} . '@' . $pvob;
		$mappings{$gitrepo}{component} = $component;
		$components{$component} = 1;
		if ( $cfg->{gitrepo}{$gitrepo}{branch} and ( keys %{ $cfg->{gitrepo}{$gitrepo}{branch} } ) ) {
			foreach my $branch ( keys %{ $cfg->{gitrepo}{$gitrepo}{branch} } ) {
				my $stream   = $cfg->{gitrepo}{$gitrepo}{branch}{$branch}{stream};
				my $activity = $cfg->{gitrepo}{$gitrepo}{branch}{$branch}{activity};
				my $ignore_stream_mismatch = $cfg->{gitrepo}{$gitrepo}{branch}{$branch}{ignore_stream_mismatch};
				$stream ||= $branch;
				$stream                                        = 'stream:' . $stream . '@' . $pvob;
				$mappings{$gitrepo}{branch}{$branch}{stream}   = $stream;
				$mappings{$gitrepo}{branch}{$branch}{activity} = $activity;
				$ignore_stream_mismatch 
					and $mappings{$gitrepo}{branch}{$branch}{ignore_stream_mismatch} = $ignore_stream_mismatch;
			}
		}
	}

	return ( \%mappings, \%components );
}

sub get_logfile_name {
	my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
	my $tstamp = sprintf( ".%d%02d%02d.%02d%02d%02d.%s.log", $year + 1900, $mon + 1, $mday, $hour, $min, $sec, getppid() );
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

		# Sometimes XML::Simple gives it to us as an array
		if ( ref( $cfg->{pvob}{$pvob}{gitrepo} ) eq 'ARRAY' ) {
			$cfg->{pvob}{$pvob}{gitrepo} = $cfg->{pvob}{$pvob}{gitrepo}->[0];
		}
		ref( $cfg->{pvob}{$pvob}{gitrepo} ) eq 'HASH'
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

sub git_clone_repos {
	my ( $basedir, $reporefs ) = @_;
	my $l = Log::Log4perl->get_logger();

	foreach my $gitrepo ( sort keys %{$reporefs} ) {
		my $repodir = File::Spec->catfile( $basedir, $gitrepo );
		if ( git_check_repo_exists($repodir) ) {
			git_fetch($repodir);
		} else {

			# Repo does not exist. Need to clone it
			$reporefs->{$gitrepo}{url} or $l->logcroak("url: is not defined for $gitrepo!");
			git_clone_repo( $reporefs->{$gitrepo}{url}, $repodir );
		}
	}
}
