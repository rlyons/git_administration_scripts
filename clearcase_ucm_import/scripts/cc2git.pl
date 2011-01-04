#!perl -w
use strict;
use warnings;
use Getopt::Long;
use XML::Simple;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Storable qw(nstore retrieve dclone);
use CCUtils;
use GitImport;
use Carp qw(carp croak verbose);

# global variable. Set by map_env_values from property max_processes
our $MAX_CHILD_PROCESSES = 1;
my $logger = Log::Log4perl->get_logger();

# Main program
my $cfg = process_cmdline();
foreach my $pvob ( keys %{ $cfg->{pvob} } ) {
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
	$logger->trace("all_baselines:\n" . Dumper(\%all_baselines));

	# Special processing for CIT _Mainline_integration streams
	my $Extratagsref = cc_find_cit_build_baselines( \%comps_for_stream );
	$logger->trace("cc_find_cit_build_baselines:\n" . Dumper($Extratagsref));

	my $i        = 0;
	my $blscount = scalar( keys %all_baselines );
	foreach my $index ( sort keys %all_baselines ) {
		$i++;
		my ( $stream, $blref ) = %{ $all_baselines{$index} };
		$logger->trace("$i: $index|$stream|$blref->{baseline}");

		my @extra_tags = ();
		($Extratagsref)
		  and (%$Extratagsref)
		  and $Extratagsref->{$stream}
		  and $Extratagsref->{$stream}{ $blref->{baseline} }
		  and @extra_tags = @{ $Extratagsref->{$stream}{ $blref->{baseline} } };

		(@extra_tags) and $logger->debug("Extra Tags: ", join (' ', @extra_tags));
		import_baseline( $i, $blscount, $stream, $blref, $comps_for_stream{$stream}, $cfg->{property}, @extra_tags );
	}
	$logger->info("Completed processing $blscount baselines for $pvob");

	# Get the latest stuff on the stream
	foreach my $stream ( keys %comps_for_stream ) {
		$logger->info("Importing latest on the stream $stream");
		import_latest_on_stream( $stream, $comps_for_stream{$stream}, $cfg->{property} );
		$logger->info("=== Import is complete for $stream ===");
	}
}

# End of Main program

# Subroutines
sub process_cmdline {
	my ( $opt_f, $opt_l, $start, $end );
	GetOptions( 'f=s' => \$opt_f, 'l=s' => \$opt_l, 'start=i' => \$start, 'end=i' => \$end );

	$opt_f or usage();
	( -f $opt_f ) or die " Config File : '$opt_f' does not exist !\n ";

	# initialize logger
	my $l_conf_file = 'config/logger.conf';
	( $opt_l and -f $opt_l ) and $l_conf_file = $opt_l;

	if ( -f $l_conf_file ) {
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
	die " \nUsage : $0 -f <config file> [ -l logger_config_file ] \n \n ";
}
