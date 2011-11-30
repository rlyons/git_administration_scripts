#!perl -w
package CCUtils;
use strict;
use warnings;
use File::Temp;
use File::Spec;
use File::Path;
use Data::Dumper;
use Log::Log4perl;
use Storable qw(nstore retrieve dclone);
use RunCmd;
use Carp qw(carp croak verbose);

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  cc_create_activity
  cc_create_view
  cc_create_import_streams
  cc_get_components_baselines
  shortname
  cc_mountvobs
  cc_get_source_vobs_for_comp_dirs
  cc_set_configspec
  cc_label_baseline
  cc_remove_stream
  cc_remove_view
  cc_remove_import_stream
  cc_find_cit_build_baselines
  cc_get_root_dirs_for_components
  cc_get_baselines_for_components
  cc_check_activity_exists
  cc_set_activity
  cc_apply_baseline
);
my $UCMUTIL = '/opt/rational/clearcase/etc/utils/ucmutil';

# Finds the baselines in the Manlines
sub cc_find_cit_build_baselines {
	my ($compsforstreamsref) = @_;
	my $l = Log::Log4perl->get_logger();
	my @mainline_streams = grep( /_Mainline_integration\@/, keys %$compsforstreamsref );
	(@mainline_streams) or return;

	my %baselines;
	my %rethash;
	foreach my $stream (@mainline_streams) {
		foreach my $comp ( keys %{ $compsforstreamsref->{$stream} } ) {
			$baselines{$comp}
			  or $baselines{$comp} = cc_get_baselines_for_component($comp);

			my $timelinesref = cc_get_timelines( $stream, $comp );

			my $deliverfrombl = undef;
			my %bblreferenced;
			foreach my $timeline (@$timelinesref) {
				$l->trace("Timeline is: $timeline");
				my ( $timestamp, $bl, $stream, $component, $action, $activity ) = split( /\|/, $timeline );
				next if ( $action eq 'rebase' );
				if ( $action eq 'deliver' ) {
					$deliverfrombl = $bl;
					$l->trace("deliverfrombl: $deliverfrombl");
					next;
				}
				if ( $action ne 'baseline' ) {
					croak("Unknown action: $action\n$timeline");
				}

				if ( $bl =~ /^baseline:build_/ ) {
					$bblreferenced{$bl} = 1;    # This baseline is already a build baseline
					next;
				}

				$deliverfrombl or next;         # No previous deliveries

				if ( $deliverfrombl =~ /^baseline:build_/ ) {    # If the delivered baseline is a build baseline
					if ( $bblreferenced{$deliverfrombl} ) {
						$deliverfrombl = undef;
						$l->trace("deliverfrombl: $deliverfrombl is already referenced");
						next;
					}

					$l->trace("adding deliverfrombl: $deliverfrombl to $stream");
					push @{ $rethash{$stream}{$bl} }, shortname($deliverfrombl);
					$bblreferenced{$deliverfrombl} = 1;
					$deliverfrombl = undef;
					next;
				}

				# find out which stream made the last delivery, and the timestamp of the delivered baseline
				my ( $blstream, $deliverbltimestamp ) = find_bl_stream( $deliverfrombl, $baselines{$component} );
				unless ($blstream) {
					$deliverfrombl = undef;
					next;
				}
				$l->trace("blstream: $blstream");

				# find a build baseline in this stream, component earlier than the delivered baseline
				my $buildbl = find_earlier_build_baseline( $blstream, $deliverbltimestamp, $baselines{$component} );
				unless ($buildbl) {
					$deliverfrombl = undef;
					next;
				}

				$l->trace("buildbl: $buildbl");
				if ( $bblreferenced{$buildbl} ) {
					$deliverfrombl = undef;
					$l->trace("buildbl: $buildbl is already referenced");
					next;
				}

				$l->trace("adding buildbl: $buildbl to $stream");
				push @{ $rethash{$stream}{$bl} }, shortname($buildbl);
				$bblreferenced{$buildbl} = 1;
				$deliverfrombl = undef;

			}
		}
	}
	return \%rethash;
}

sub find_earlier_build_baseline {
	my ( $instream, $intimestamp, $ref ) = @_;

	# sort the lines to be in the order: latest to earliest
	foreach my $line ( reverse sort @$ref ) {
		my ( $timestamp, $bl_stream, $bl ) = split( /\|/, $line );
		$bl_stream or next;
		( $instream eq $bl_stream ) or next;
		( $bl =~ /^baseline:build_/ ) or next;    # We only want build_baselines
		( $timestamp lt $intimestamp ) and return $bl;
	}
	return;
}

sub find_bl_stream {
	my ( $inbl, $ref ) = @_;
	foreach my $line (@$ref) {
		my ( $timestamp, $bl_stream, $bl ) = split( /\|/, $line );
		( $bl eq $inbl ) and return ( $bl_stream, $timestamp );
	}
	return;
}

sub cc_get_baselines_for_component {
	my ($component) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug("Getting all baselines for $component");
	my @ret = cc_run_cmd("cleartool lsbl -fmt '%Nd|%[bl_stream]Xp|%Xn\n' -component $component -obsolete");
	return \@ret;
}

sub cc_get_timelines {
	my ( $stream, $component ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug("Getting timelines for $stream \+ $component");
	my ( $rc, $stderr );
	my $cmd = "$UCMUTIL dump_timeline -act -stream $stream -comp $component";
	my @ret = cc_run_cmd( $cmd, nodie => 1, nowarn => 1, retcoderef => \$rc, stderrref => $stderr );
	$rc and $stderr and $l->logcroak("Error running: $cmd\n$stderr\n");
	my ($pvob) = ( $stream =~ m/\@(.*)$/ );
	my @timelines;

	foreach my $line (@ret) {
		my ( $action, $bl, $mm, $dd, $yyyy, $HH, $MM, $SS, $AMPM, $activity ) = ( $line =~ /^(.*?)\s+(.*?)\s+(\d\d)\/(\d\d)\/(\d\d\d\d) (\d\d):(\d\d):(\d\d) ([AP]M)\s+(.*?)\s*$/ );
		$action or croak "Failed to decode line:\n$line\n";
		( $AMPM eq 'PM' ) and $HH += 12;
		if ($activity) {
			$activity = 'activity:' . $activity . '@' . $pvob;
		} else {
			$activity = '';
		}
		$bl = 'baseline:' . $bl;

		#$yyyy$mm$dd.$HH$MM$SS
		push @timelines, "$yyyy$mm$dd.$HH$MM$SS|$bl|$stream|$component|$action|$activity";
	}
	return \@timelines;
}

sub cc_get_components_baselines {
	my ( $stream, $cached ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->info("Getting list of components and baselines for $stream");
	my $ref = cc_run_parallel_cmds( [ "cleartool describe -fmt \"%[components]Xp\\n\" $stream", "cleartool describe -fmt \"%[mod_comps]Xp\\n\" $stream", "cleartool describe -fmt \"%[found_bls]Xp\\n\" $stream", "cleartool lsbl -fmt \"%d|%Xn\\n\" -obsolete -stream $stream" ] );

	my %components = map { $_ => { 'modifiable' => 0 } } split( ' ', $ref->[0][0] );
	foreach ( split( ' ', $ref->[1][0] ) ) { $components{$_}{modifiable} = 1; }

	$l->info("Getting components details for $stream");
	my $tmpref = cc_run_cmd_parallel( "cleartool desc -fmt \"%d|%[root_dir]p\\n\"", [ keys %components ] );
	foreach my $component ( keys %components ) {
		$components{$component}{component} = $component;
		my $ref = $tmpref->{$component};
		my ( $create_timestamp, $root_dir ) = split( /\|/, $ref->[0] );
		$components{$component}{create_timestamp} = $create_timestamp;
		if ($root_dir) {
			$components{$component}{root_dir}     = $root_dir;
			$components{$component}{is_composite} = 0;
		} else {
			$components{$component}{root_dir}     = undef;
			$components{$component}{is_composite} = 1;
		}
	}
	$l->trace( Dumper( \%components ) );

	$l->info("Getting foundation baseline details for $stream");
	my $fblsref = cc_get_baseline_details( \%components, [ split( ' ', $ref->[2][0] ) ], $cached );
	$l->trace( Dumper($fblsref) );

	my %all_baselines;
	my $num = 1;
	foreach my $line ( sort @{ $ref->[3] } ) {
		my ( $timestamp, $bl ) = split( /\|/, $line );
		$all_baselines{$bl} = { sort_order => $num, create_timestamp => $timestamp };
		$num++;
	}

	$l->info("Getting baseline details for $stream");
	my $ablsref = cc_get_baseline_details( \%components, [ keys %all_baselines ], $cached );
	foreach my $bl ( keys %all_baselines ) {
		$ablsref->{$bl}{sort_order} = $all_baselines{$bl}{sort_order};
	}

	$l->debug( "components: \n   ", join( "\n   ", map { "$_\t\t" . ( $components{$_}{'root_dir'} ? $components{$_}{'root_dir'} : '' ) . "\t" . ( ( $components{$_}{'is_composite'} ) ? '' : ( ( $components{$_}{'modifiable'} ) ? '(modifiable)' : '(read-only)' ) ) } sort keys %components ), "\n" );
	$l->debug( "Foundation baselines:", join( "\n   ", '', sort keys %$fblsref ) );

	validate_foundation_baselines( $stream, $fblsref, $ablsref, \%components );

	my $stream_creation_time = cc_run_cmd("cleartool describe -fmt \"%d\" $stream");
	my %ret_baselines;
	foreach my $bl ( keys %$fblsref ) {
		my $js = '_2_';    # Baselines that are created off of another stream
		$fblsref->{$bl}{bl_stream} or $js = '_0_';    # baselines without a stream go first
		$fblsref->{$bl}{sortorder}     = $fblsref->{$bl}{create_timestamp} . $js . $stream_creation_time;
		$fblsref->{$bl}{is_foundation} = 1;
		$fblsref->{$bl}{importstream}  = $stream;
		$ret_baselines{$bl}            = $fblsref->{$bl};
	}
	foreach my $bl ( keys %$ablsref ) {               # Baselines that are created on the stream
		$ablsref->{$bl}{sortorder}    = $ablsref->{$bl}{create_timestamp} . '_1_' . $stream_creation_time;
		$ablsref->{$bl}{importstream} = $stream;
		$ret_baselines{$bl}           = $ablsref->{$bl};
	}

	return ( \%components, \%ret_baselines );
}

sub cc_get_baseline_details {
	my ( $componentsref, $inblsref, $cached ) = @_;
	my $l = Log::Log4perl->get_logger();
	my $blsserfile = File::Spec->catfile( File::Spec->tmpdir(), '.git_import.baselines.ser' );
	$l->trace( 'baseline details ser file: ', $blsserfile );

	my $blsref = {};
	if ($cached) {
		$l->trace('baseline details caching is turned on');
		if ( -f $blsserfile ) {

			$blsref = retrieve($blsserfile);
			$l->trace( 'Retrieved ', scalar( keys %$blsref ), ' baselines from ser file.' );
		} else {
			$l->trace('baseline cache ser file is missing');
		}
	} else {
		$l->trace('baseline details caching is turned off');
	}

	my $cmd = "cleartool describe -fmt \"component=%[component]Xp|create_timestamp=%d|depends_on=%[depends_on]Xp|bl_stream=%[bl_stream]Xp|predecessor=%[predecessor]Xp\\n\"";
	my @getbls = grep { !$blsref->{$_} } @$inblsref;
	if (@getbls) {
		my $nfromcache = scalar( keys @$inblsref ) - scalar(@getbls);
		$nfromcache and $l->trace( 'Retrieved details for ', $nfromcache, ' baselines from cache.' );

		$l->trace( 'Getting baseline details for ', scalar(@getbls), ' baselines' );
		my $tmpref = cc_run_cmd_parallel( $cmd, \@getbls );

		foreach my $bl (@getbls) {
			foreach my $line ( @{ $tmpref->{$bl} } ) {
				my %hash;
				foreach my $kv ( split( '\|', $line ) ) {
					$kv =~ s/\=$/\=undef/;
					my ( $k, $v ) = split( '=', $kv );
					$hash{$k} = $v;
				}
				while ( my ( $k, $v ) = each(%hash) ) {
					if ( $k eq 'depends_on' and $v ne 'undef' ) {
						foreach ( split( ' ', $v ) ) {
							$blsref->{$bl}{depends_on}{ 'baseline:' . $_ } = undef;
						}
					} elsif ( $k eq 'predecessor' ) {
						$blsref->{$bl}{$k} = 'baseline:' . $v;
					} elsif ( $k eq 'component' ) {
						$blsref->{$bl}{$k} = dclone( $componentsref->{$v} );
					} else {
						$blsref->{$bl}{$k} = ( $v eq 'undef' ) ? undef : $v;
					}
				}
				$blsref->{$bl}{baseline} = $bl;
			}
		}

		if ($cached) {
			nstore( $blsref, $blsserfile );
			$l->trace( 'Saved ', scalar( keys %$blsref ), ' baselines to ser file.' );
		}
	} else {
		$l->trace( 'Retrieved details for ', scalar( keys @$inblsref ), ' baselines from cache.' );
	}

	my %rethash = map { $_ => $blsref->{$_} } @$inblsref;
	return \%rethash;
}

sub cc_remove_import_stream {
	my ( $istream, $pref, $viewtag ) = @_;
	my $l      = Log::Log4perl->get_logger();
	my $prefix = $pref->{view_prefix}{value};
	$prefix or croak("prefix is not defined !");
	my $retstr = cc_run_cmd("cleartool describe -fmt \"%[dstreams]Xp\\n\" $istream");
	foreach my $s ( split( ' ', $retstr ) ) {
		( $s =~ /^stream:$viewtag\@/ )
		  and cc_remove_stream( $s, $pref->{view_stgloc}{value} );
	}
}

sub cc_create_import_streams {
	my ( $istream, $pref, $component, @bls ) = @_;

	my $l = Log::Log4perl->get_logger();
	my ($pvob) = ( $istream =~ m/(\@.*?)$/ );
	my @retviews;
	srand();
	for my $bl (@bls) {
		my $vnum     = int( rand(10000) + 10000 );
		my $cmstream = shortname($istream);
		$cmstream =~ s/_integration//;
		$cmstream = 'stream:' . $pref->{view_prefix}{value} . '_' . $vnum . '_' . shortname($component) . '_' . $cmstream . $pvob;
		my $retstr = cc_run_cmd("cleartool describe -fmt \"%[dstreams]Xp\\n\" $istream");
		foreach my $s ( split( ' ', $retstr ) ) {
			$s eq $cmstream
			  and cc_remove_stream( $cmstream, $pref->{view_stgloc}{value} );
		}

		$l->info("Creating $cmstream");
		cc_run_cmd("cleartool mkstream -in $istream -nc -baseline $bl -readonly $cmstream");

		my $view_tag = shortname($cmstream);
		cc_create_view( $view_tag, $pref->{view_host}{value}, $pref->{view_stgloc}{value}, $cmstream );
		push @retviews, $view_tag;
	}
	return @retviews;
}

sub find_baseline_for_component {
	my ( $stream, $comp, $compsref, $blsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace("Finding baseline for $comp");

	my $bls = cc_get_baseline_details( $compsref, [ keys %$blsref ] );

	foreach my $bl ( keys %$bls ) {
		if ( $bls->{$bl}{component}{component} eq $comp ) {
			$l->trace("Found $bl for $comp");
			return $bl;
		}
	}

	foreach my $bl ( keys %$bls ) {
		next if $bls->{$bl}{component}{root_dir};
		next unless $bls->{$bl}{depends_on};
		my $newbl = find_baseline_for_component( $stream, $comp, $compsref, $bls->{$bl}{depends_on} );
		if ($newbl) {
			$l->trace("Found $newbl for $comp");
			return $newbl;
		}
	}
	$l->trace("No baseline found for $comp");
	return;
}

sub update_foundation_baselines_for_all_components {
	my ( $stream, $fblsref, $compsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace("Updating foundation baselines for all components.");

	my %blforcomp;
	foreach my $fbl ( keys %$fblsref ) {
		$blforcomp{ $fblsref->{$fbl}{component}{component} } = $fbl;
	}

	# For the components for which a foundation is not specified, look for the baseline in the composite baselines
	foreach my $comp ( keys %$compsref ) {
		next if $blforcomp{$comp};
		foreach my $fbl ( keys %$fblsref ) {
			next if $fblsref->{$fbl}{component}{root_dir};
			next unless $fblsref->{$fbl}{depends_on};
			my $bl = find_baseline_for_component( $stream, $comp, $compsref, $fblsref->{$fbl}{depends_on} );
			$bl or croak("Cannot find foundation baseline for component: $comp\n");
			my $blref = cc_get_baseline_details( $compsref, [$bl] );
			$fblsref->{$bl} = $blref->{$bl};
		}
	}
	return;
}

sub validate_foundation_baselines {
	my ( $stream, $fblsref, $allblsref, $compsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->info("Validating foundation baselines.");
	update_foundation_baselines_for_all_components( $stream, $fblsref, $compsref );
	my @sorted_bls = sort { $allblsref->{$a}{sort_order} <=> $allblsref->{$b}{sort_order} } keys %$allblsref;
	my $modified = 0;
	foreach my $comp ( keys %$compsref ) {

		# Find the first baseline on this component
		my $first_bl = undef;
		for (@sorted_bls) {
			next if ( $allblsref->{$_}{component}{component} ne $compsref->{$comp}{component} );
			$first_bl = $_;
			last;
		}
		next unless $first_bl;

		# Find the foundation baseline for this component
		my $fbl = undef;
		for my $x ( keys %$fblsref ) {
			if ( $fblsref->{$x}{component}{component} eq $compsref->{$comp}{component} ) {
				$fbl = $x;
				last;
			}
		}
		next unless $fbl;

		# Make sure the foundation baseline is created on a stream, not just component
		next unless $fblsref->{$fbl}{bl_stream};

		# Check if this stream is based off of another stream
		next if ( $allblsref->{$first_bl}{bl_stream} eq $fblsref->{$fbl}{bl_stream} );

		# Check if this stream's first baseline is newer than the foundation baseline
		next if ( $allblsref->{$first_bl}{create_timestamp} gt $fblsref->{$fbl}{create_timestamp} );

		# The first baseline is older than the foundation baseline, make sure it has a predecessor
		( $allblsref->{$first_bl}{predecessor} )
		  or croak("The first $first_bl is older than the foundation $fbl but it does not have a predecessor.\n");

		my $new_fbl = $allblsref->{$first_bl}{predecessor};
		my $newbls = cc_get_baseline_details( $compsref, [$new_fbl] );
		delete $fblsref->{$fbl};
		$fblsref->{$new_fbl} = $newbls->{$new_fbl};
		$l->info("Reset the foundation baseline of $comp\n");
		$l->info("   From: $fbl");
		$l->info("     To: $new_fbl");
		$modified = 1;
	}
	$modified
	  and $l->debug( "Validated foundation baselines:\n", join( "\n   ", sort keys %$fblsref ), '' );
	return;
}

sub verify_stream_exists {
	my $istream = shift;
	my $retstr  = cc_run_cmd("cleartool lsstream -fmt '%Xn\\n' $istream");
	return $retstr;
}

sub cc_remove_stream {
	my ( $stream, $viewstgloc ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $retstr = cc_run_cmd("cleartool lsstream -fmt \"%[views]p\\n\" $stream");
	my %views = map { $_ => 1 } split( ' ', $retstr );

	my $vw = shortname($stream);
	my $stderr;
	cc_run_cmd( "cleartool lsview -s $vw", nodie => 1, nowarn => 1, stderrref => \$stderr );
	( $stderr =~ /cleartool: Error: No matching entries found for view tag/ ) or $views{$vw} = 1;

	foreach my $view ( keys %views ) {
		cc_remove_view( $view, $viewstgloc );
	}
	$l->trace("Removing $stream");
	cc_run_cmd("cleartool rmstream -force $stream");
	return;
}

sub cc_remove_view {
	my ( $view, $viewstgloc ) = @_;
	my $l = Log::Log4perl->get_logger();
	my $rc;
	cc_run_cmd("cleartool lsview $view", retcoderef => \$rc, nodie => 1, nowarn => 1);
	$rc and return;
	
	$l->trace("Removing view:$view");
	cc_run_cmd("cleartool endview -server $view");

	my $retcode = 0;
	#cc_run_cmd( "cleartool rmview -force -tag $view", nodie => 1, nowarn => 1, retcoderef => \$retcode );
	cc_run_cmd( "cleartool rmview -force  " . get_view_root($view), nodie => 1, nowarn => 1, retcoderef => \$retcode );
	$retcode or return;

	# need to remove the view using uuid
	my @ret = cc_run_cmd("cleartool lsview -long $view");
	foreach my $uuid (@ret) {
		next unless ( $uuid =~ s/^View uuid: // );
		$l->trace("Removing view:$view using uuid $uuid");
		cc_run_cmd( "cleartool rmview -force -uuid $uuid -avob", nodie => 1, nowarn => 1 );
		cc_run_cmd("cleartool unregister -view -uuid $uuid");
		last;
	}
	$l->trace("Removing view tag:$view");
	cc_run_cmd("cleartool rmtag -view -all $view");
	cc_run_cmd("rm -rf /home/_svcmaven-dev/gitimports/git2cc/snapshots/$view");

	my $viewstg = File::Spec->catfile( $viewstgloc, "$view.vws" );
	if ( -d $viewstg ) {
		$l->trace("Removing view storage: $viewstg");
		rmtree($viewstg) or croak("Cannot rmtree dir: $viewstg");
	}
}

sub cc_set_configspec {
	my ( $viewtag, $label, $timestamp ) = @_;
	my $l = Log::Log4perl->get_logger();
	my ( $fh, $filename ) = File::Temp::tempfile();
	$l->trace("Writing configspec into $filename");
	print $fh "element * $label\n";
	print $fh "time $timestamp\n";

	print $fh "element * /main/LATEST\n";
	print $fh "element * /main/0\n";
	close $fh;
	$l->trace("Setting configspec for $viewtag from $filename");
	cc_run_cmd("cleartool setcs -tag $viewtag $filename");
	unlink $filename;
}

sub cc_create_view {
	my ( $view_tag, $view_host, $view_stgloc, $stream ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $retcode;
	cc_run_cmd( "cleartool lsview $view_tag", nodie => 1, nowarn => 1, retcoderef => \$retcode );
	unless ($retcode) {    # The view exists, remove it
		cc_remove_view( $view_tag, $view_stgloc );
	}
	my $cmd = "cleartool mkview -snapshot -tag $view_tag ";
	$stream and $cmd .= "-stream $stream ";
	my $viewstg = File::Spec->catfile( $view_stgloc, "$view_tag.vws" );
	$cmd .= "-vws $viewstg /home/_svcmaven-dev/gitimports/git2cc/snapshots/${view_tag}";

	$l->info( "Creating snapshot view:$view_tag", $stream ? " for $stream" : '' );
	my $retstr = cc_run_cmd($cmd);
	$l->trace($retstr);
	my $vobrule;
	open CATCS, "cleartool catcs -tag $view_tag |";
	while(<CATCS>) {
		if( m#=/(cc/[a-zA-Z0-9_]+)\]# ) {
			$vobrule=$1;
		}
	}
	$l->trace("adding load rule for vob: /$vobrule");
	cc_run_cmd("cd /home/_svcmaven-dev/gitimports/git2cc/snapshots/${view_tag}; cleartool update -force -add_loadrules $vobrule " );
}


sub cc_label_baseline {
	my ( $baseline, $pref ) = @_;
	my $l        = Log::Log4perl->get_logger();
	my $blstatus = cc_run_cmd("cleartool describe -fmt \"%[label_status]p\" $baseline");
	if ( $blstatus eq 'Not Labeled' ) {
		my $viewtag = 'tmpview_' . $$;
		$l->trace("$baseline is not labeled.");
		cc_create_view( $viewtag, $pref->{view_host}{value}, $pref->{view_stgloc}{value} );
		$l->debug("labeling baseline: $baseline");
		cc_run_cmd( "cleartool chbl -incremental $baseline", view => $viewtag );
		cc_remove_view( $viewtag, $pref->{view_stgloc}{value} );
	} else {
		$l->trace("$baseline is already labeled.");
	}
}

sub cc_mountvobs {
	my @vobs = @_;
	my $view = pop(@vobs);
	my $l    = Log::Log4perl->get_logger();
	foreach my $vob (@vobs) {
#		my $retstr = cc_run_cmd("cleartool lsvob $vob");
#		if ( substr( $retstr, 0, 1 ) eq '*' ) {
#			$l->trace("vob:$vob is already mounted.");
#			next;
#		}
#		$l->trace("mounting vob: $vob");
		$l->trace("adding load rule for vob: $vob");
		my $vr = get_view_root($view);
		print "View Root is $vr\n";
		my $stripvob=$vob;
		$stripvob =~ s/^\///;
		print "Stripped VOB is $stripvob\n";
		cc_run_cmd("cd $vr; cleartool update -add_loadrules $stripvob " );
	}
}

sub cc_get_source_vobs_for_comp_dirs {
	my @dirs      = @_;
	my @vobs_list = cc_run_cmd('cleartool lsvob -s');
	my %rethash;
	foreach my $svob (@vobs_list) {
		foreach my $dir (@dirs) {
			if ( ( $dir eq $svob ) or ( $dir =~ /^\Q$svob\E[\/\\]/ ) ) {
				$rethash{$svob} = 1;
			}
		}
		return sort keys %rethash;
	}
}

sub cc_startview {
	my ($view) = @_;
	my $l      = Log::Log4perl->get_logger();
	my $retstr = cc_run_cmd("cleartool lsview $view");
	if ( substr( $retstr, 0, 1 ) eq '*' ) {
		$l->trace("view:$view is already started.");
		return;
	}
	$l->debug("Starting view: $view");
	cc_run_cmd("cleartool startview $view");
}

# remove xxx: and @/cc/xxx from the given string
sub shortname {
	my ($str) = @_;
	$str =~ s/^.*?://;
	$str =~ s/@.*?$//;
	return $str;
}

sub cc_get_root_dirs_for_components {
	my (@components) = @_;
	my $l      = Log::Log4perl->get_logger();
	$l->debug("Getting root_dir for " . scalar(@components) ." components");
	my $ref = cc_run_cmd_parallel("cleartool describe -fmt \"\%[root_dir]p\"", \@components);
	my %ret;
	foreach my $comp (@components) {
		$ret{$comp} = $ref->{$comp}->[0]; 
	}
	return \%ret;
}

sub cc_get_baselines_for_components {
	my (@components) = @_;
	my %baselines;
	my %comp_stream_bls;
	my $l      = Log::Log4perl->get_logger();
	$l->debug("Getting baselines for " . scalar(@components) ." components");
	foreach my $comp (@components) {
		my $blsref = cc_get_baselines_for_component($comp);
		foreach my $line (@$blsref) {
			my ( $timestamp, $stream, $baseline ) = split( '\|', $line );
			next unless $stream;
			my ($bl) = ( $baseline =~ m/baseline:(.*?)\@/ );
			$baselines{$bl}{component} = $comp;
			$baselines{$bl}{stream}    = $stream;
			$baselines{$bl}{baseline}  = $baseline;
		}
	}
	return ( \%baselines );
}

sub cc_create_activity {
	my ($view, $activity) = @_;
	my $l      = Log::Log4perl->get_logger();
	$l->debug("Creating activity: $activity");
	my @ret = cc_run_cmd("cleartool mkact \"$activity\"", view => $view);
	$l->trace(join("\n",'Output: ', @ret));
}

sub cc_set_activity {
	my ($view, $activity) = @_;
	my $l      = Log::Log4perl->get_logger();
	$l->debug("Setting activity: $activity in $view");
	my @ret = cc_run_cmd("cleartool setact \"$activity\"", view => $view);
	$l->trace(join("\n",'Output: ', @ret));
}

sub cc_check_activity_exists{
	my ($view, $activity) = @_;
	my $l      = Log::Log4perl->get_logger();
	my $rc = 0;
	my $retstr = cc_run_cmd("cleartool lsact \"$activity\"", view => $view, retcoderef => \$rc, nowarn => 1, nodie => 1);
	if ( $rc) {
		$l->trace("activity:$activity does not exist");
		return 0; 
	} else {
		$l->trace("activity:$activity exists");
		return 1; 
	}
}

sub cc_apply_baseline {
	my ($view, $component, $baseline, $has_changes) = @_;
	my $l      = Log::Log4perl->get_logger();
	my $cmd = "cleartool mkbl -nc -component $component ";
	$has_changes or $cmd .= "-identical ";
	$cmd .= "-incremental -view $view $baseline";
	$l->info("Applying baseline: $baseline");
	my @ret = cc_run_cmd($cmd);
	$l->trace(join("\n",'Output: ', @ret));
}	

1;
