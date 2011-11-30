package FSUtils;
use strict;
use warnings;
use Data::Dumper;
use File::Find;
use File::Spec;
use File::Copy;
use Socket;
use IO::Handle;
use Digest::SHA1;
use Storable qw(dclone);

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  applydiffs
  copydir
  fsread
  fscomparedirs
  fscomparehashwithdir
  get_sha1_file
);

my $MATCH_AFTER = 100;

# Subroutines
sub fsread {
	my ($indir) = @_;
	my $l = Log::Log4perl->get_logger();
	( -d $indir ) or $l->logcroak( 'Not a directory: ', $indir );
	$l->info( 'Reading Dir: ', map { "==> " . $_ } $indir );
	my ( $pid, $child ) = launch_find_process($indir);
	my %fshash;
	while ( my $line = <$child> ) {
		$l->trace( "Received: ", $line );
		my ( $filename, $href ) = parse_line( $line, $indir );
		$fshash{$filename} = $href;
	}
	close $child;
	waitpid( $pid, 0 );
	return \%fshash;
}

sub fscomparedirs {
	my @indirs = @_;
	my $l      = Log::Log4perl->get_logger();
	$l->info( 'Comparing dirs: ', map { "\n\t==> " . $_ } @indirs );

	my %base_dirs;
	my ( %sockethandles, %pids );
	for my $id ( 1 .. scalar(@indirs) ) {
		$base_dirs{$id} = $indirs[ $id - 1 ];
		my ( $pid, $child ) = launch_find_process( $base_dirs{$id} );
		$pids{$id}          = $pid;
		$sockethandles{$id} = $child;
	}
	$l->trace('Starting diff engine');
	my $diffref = diffdirs( \%base_dirs, %sockethandles );
	$l->info( 'Found ', scalar( keys( %{ $diffref->{0} } ) ), ' differences.' );

	foreach my $id ( keys %pids ) {
		waitpid( $pids{$id}, 0 );
	}
	return $diffref;
}

sub fscomparehashwithdir {
	my ( $indhref, $indir ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->info( 'Reading Dir: ', map { "==> " . $_ } $indir );
	$l->info('Comparing with given hashref');

	my ( $pid, $child ) = launch_find_process($indir);
	my %fshash;

	foreach my $item ( keys %$indhref ) {
		$fshash{0}{$item} = undef;
		$fshash{1}{$item} = dclone( $indhref->{$item} );
	}
	while ( my $line = <$child> ) {
		$l->trace( "Received: ", $line );
		my ( $item, $href ) = parse_line( $line, $indir );
		if ( $indhref->{$item} ) {
			if (    $href->{type} eq $indhref->{$item}{type}
				and $href->{SHA1} eq $indhref->{$item}{SHA1} )
			{
				delete $fshash{0}{$item};
				delete $fshash{1}{$item};
				next;
			}
			$fshash{1}{$item} = dclone( $indhref->{$item} );
		}
		$fshash{0}{$item} = undef;
		$fshash{2}{$item} = $href;
	}
	$l->info( 'Found ', scalar( keys( %{ $fshash{0} } ) ), ' differences.' );
	close $child;
	waitpid( $pid, 0 );
	return \%fshash;
}

sub launch_find_process {
	my ($indir) = @_;
	my $l = Log::Log4perl->get_logger();

	# Set up socket pair
	socketpair( my $parent, my $child, AF_UNIX, SOCK_STREAM, PF_UNSPEC )
	  or $l->logcroak( 'cannot create socketpair: ', $! );
	$child->autoflush(1);
	$parent->autoflush(1);

	# Launch process
	my $pid = fork();
	if ( not defined $pid ) {
		$l->logcroak( 'fork failed: ', $! );
	} elsif ( $pid == 0 ) {

		# Child process
		close $child;
		my $ctx = Digest::SHA1->new;
		$ctx or $l->logcroak( 'Cannot create a SHA1 object !', $! );

		my %opts = (
			no_chdir   => 1,
			wanted     => sub { },
			preprocess => sub {
				return processdirs( $parent, $ctx, @_ );
			}
		);
		find( \%opts, $indir );
		close $parent;
		exit(0);
	}

	# Parent process
	close $parent;
	$l->trace( 'Parent process, pid of child is ', $pid );
	return ( $pid, $child );
}

sub diffdirs {
	my ( $basedirref, %sockethandles ) = @_;
	my $num_dirs = scalar( keys %$basedirref );
	my $l        = Log::Log4perl->get_logger();
	$l->trace( 'num_dirs is ', $num_dirs );
	my %fshash;
	my $count = 0;
	while (%sockethandles) {
		foreach my $id ( keys %sockethandles ) {
			do {
				my $handle = $sockethandles{$id};
				my $line   = undef;
				if ( defined( $line = <$handle> ) ) {
					$l->trace( "Received ", $id, ': ', $line );
					my ( $filename, $href ) = parse_line( $line, $basedirref->{$id} );
					$fshash{$id}{$filename} = $href;
					$fshash{0}{$filename} = undef;
					$count++;
				} else {
					close $handle;
					delete $sockethandles{$id};
					last;
				}
			} while ( $count % $MATCH_AFTER );
		}
		$l->trace( 'Processed ', $count, ' entries.' );
		remove_matched( $num_dirs, \%fshash );
	}
	return \%fshash;
}

sub remove_matched {
	my ( $num_dirs, $hashref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( Dumper($hashref) );

	foreach my $item ( keys %{ $hashref->{0} } ) {
		my ( $matches, $sha1, $type ) = ( 0, undef, undef );
		foreach my $id ( 1 .. $num_dirs ) {
			if ($type) {
				$hashref->{$id}{$item}
				  and ( $type eq $hashref->{$id}{$item}{type} )
				  and ( $sha1 eq $hashref->{$id}{$item}{SHA1} )
				  and $matches++;
			} else {
				$hashref->{$id}{$item}
				  and ( $type, $sha1 ) = ( $hashref->{$id}{$item}{type}, $hashref->{$id}{$item}{SHA1} )
				  and $matches++;
			}
		}
		if ( $matches eq $num_dirs ) {
			$l->trace( 'Matched : ', $item );
			foreach my $id ( 0 .. $num_dirs ) {
				delete $hashref->{$id}{$item};
			}
		}
	}
}

sub parse_line {
	my ( $line, $basedir ) = @_;

	chomp $line;
	my $type = substr( $line, 0, 1,  '' ); # Removes the first 1 char and returns it 
	my $sha1 = substr( $line, 0, 40, '' ); # Removes the first 40 chars and returns it
	$line = File::Spec->abs2rel( $line, $basedir );
	return ( $line, { type => $type, SHA1 => $sha1 } );
}

sub processdirs {
	my $parent  = shift;
	my $ctx     = shift;
	my $l       = Log::Log4perl->get_logger();
	my $dirsha1 = '0' x 40;
	my @retdirs;

	$l->trace( "Processing dir: ", $File::Find::dir );
	for my $item ( sort @_ ) {
		next if ( $item eq '.' or $item eq '..' or $item eq '.git' or $item eq '.gitlock' or $item eq '.gitignore' );
		my $citem = File::Spec->catfile( $File::Find::dir, $item );
		if ( -l $citem ) {
			my $linkval = readlink($citem);
			print $parent 'L', get_sha1_string( $ctx, $linkval ), $citem, "\n";
		} elsif ( -d $citem ) {
			push @retdirs, $item;
			print $parent 'D', $dirsha1, $citem, "\n";
		} elsif ( -f $citem ) {
			print $parent 'F', get_sha1( $ctx, $citem ), $citem, "\n";
		} else {
			$l->logcroak( 'unknown type for ', $citem );
		}
	}
	return @retdirs;
}

sub get_sha1_string {
	my ( $ctx, $string ) = @_;
	my $l = Log::Log4perl->get_logger();
	$ctx->add($string);
	my $digest = $ctx->hexdigest();
	$ctx->reset();
	return $digest;
}

sub get_sha1 {
	my ( $ctx, $filename ) = @_;
	my $l = Log::Log4perl->get_logger();
	open( my $fh, '<', $filename ) or $l->logcroak( "Failed to read : ", $filename );
	$ctx->addfile($fh);
	my $digest = $ctx->hexdigest();
	$ctx->reset();
	close $fh;
	return $digest;
}

sub get_sha1_file {
	my ($filename) = @_;
	my $l          = Log::Log4perl->get_logger();
	my $ctx        = Digest::SHA1->new;
	$ctx or $l->logcroak( 'Cannot create a SHA1 object !', $! );
	open( my $fh, '<', $filename ) or $l->logcroak( "Failed to read : ", $filename );
	$ctx->addfile($fh);
	my $digest = $ctx->hexdigest();
	$ctx->reset();
	close $fh;
	return $digest;
}

sub copydir {
	my ( $srcdir, $tgtdir ) = @_;
	my $l = Log::Log4perl->get_logger();

	if ( -d $tgtdir ) {
		$l->logcroak( 'Target directory exists: ', $tgtdir );
	} else {
		$l->info( 'Target dir: ', $tgtdir, ' does not exist.' );
		$l->info( ' createdir: ', $tgtdir );
		mkdir($tgtdir)
		  or $l->logcroak( 'mkdir failed.', "\n", $tgtdir, "\n", $! );
	}

	my $srcref = fsread($srcdir);

	$l->debug( 'Copying recursively ', 'From: ', $srcdir, ' To: ', $tgtdir );
	for my $item ( sort keys %$srcref ) {
		my $tgt = File::Spec->catfile( $tgtdir, $item );
		if ( $srcref->{$item}{type} eq 'D' ) {
			$l->trace( ' createdir: ', $tgt );
			mkdir($tgt)
			  or $l->logcroak( 'mkdir failed.', "\n", $tgt, "\n", $! );
		} else {
			my $src = File::Spec->catfile( $srcdir, $item );
			$l->trace( 'createfile: ', $tgt );
			copy( $src, $tgt )
			  or $l->logcroak( 'copy failed.', "\n", 'From: ', $src, "\n", '  To: ', $tgt );
		}
	}
	$l->debug( 'Copied ', scalar( keys %$srcref ), ' items' );

	return dclone($srcref);
}

sub applydiffs {
	my ( $srcdir, $tgtdir, $diffref, $functionsref, $coref ) = @_;
	my $l = Log::Log4perl->get_logger('applydiffs');
	$l->trace('applydiffs called');
	$l->trace( 'src dir is ', $srcdir );
	$l->trace( 'tgt dir is ', $tgtdir );
	my $previtemcount = 0;
	while ( keys %{ $diffref->{0} } ) {
		my $itemcount = scalar( keys %{ $diffref->{0} } ) + scalar( keys %{ $diffref->{1} } ) + scalar( keys %{ $diffref->{2} } );
		( $itemcount == $previtemcount ) and $l->logcroak('Internal Error: ItemCount did not change since previous loop !');
		$previtemcount = $itemcount;
		$l->trace( 'there are ', scalar( keys %{ $diffref->{0} } ), ' differences, starting loop' );
		my @deleteitems = ();
		foreach my $item ( sort keys %{ $diffref->{0} } ) {

			# Apply new and modify files first, in ascending order
			$l->trace( '=== Item is: ', $item );
			if ( $diffref->{2}{$item} ) {
				unless ( $diffref->{1}{$item} ) {    # exists in  2 only - DELETE item
					$l->trace( 'Deferring for deletion: ', $item );
					push @deleteitems, $item;
					next;
				}

				# Exists in both - check if they are the same type
				if ( $diffref->{2}{$item}{type} eq 'D' ) {
					if ( $diffref->{1}{$item}{type} eq 'D' ) {
						$l->logcroak( 'Contradiction - both dirs are equal', "\n", $item, "\n" );
					} elsif ( $diffref->{1}{$item}{type} eq 'F' or $diffref->{2}{$item}{type} eq 'L' ) {
						$l->trace( 'Deferring for deletion: ', $item );
						push @deleteitems, $item;
					} else {
						$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
					}
				} elsif ( $diffref->{2}{$item}{type} eq 'F' ) {
					if ( $diffref->{1}{$item}{type} eq 'D' ) {
						$l->trace( 'Deferring for deletion: ', $item );
						push @deleteitems, $item;
						last;    # We need to remove this file pronto because  a directory (and possibly its contents) are going to replace it. If we continue the loop, we cannot copy over the items
					} elsif ( $diffref->{1}{$item}{type} eq 'F' ) {
						$functionsref->{copyfile}->( $item, $srcdir, $tgtdir, $coref );
						delete $diffref->{0}{$item};
						delete $diffref->{1}{$item};
						delete $diffref->{2}{$item};
					} elsif ( $diffref->{1}{$item}{type} eq 'L' ) {
						$l->trace( 'Deferring for deletion: ', $item );
						push @deleteitems, $item;
					} else {
						$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
					}
				} elsif ( $diffref->{2}{$item}{type} eq 'L' ) {
					if ( $diffref->{1}{$item}{type} eq 'D' ) {
						$l->trace( 'Deferring for deletion: ', $item );
						push @deleteitems, $item;
						last;    # We need to remove this symlink pronto because a directory (and possibly its contents) are going to replace it. If we continue the loop, we cannot copy over the items
					} elsif ( $diffref->{1}{$item}{type} eq 'F' or $diffref->{2}{$item}{type} eq 'L' ) {
						$l->trace( 'Deferring for deletion: ', $item );
						push @deleteitems, $item;
					} else {
						$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
					}
				} else {
					$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
				}
			} elsif ( $diffref->{1}{$item} ) {    # Exists in 1 only - NEW item
				if ( $diffref->{1}{$item}{type} eq 'D' ) {
					$functionsref->{createdir}->( $item, $tgtdir, $coref );
				} elsif ( $diffref->{1}{$item}{type} eq 'F' ) {
					$functionsref->{copyfile}->( $item, $srcdir, $tgtdir, $coref );
				} elsif ( $diffref->{1}{$item}{type} eq 'L' ) {
					$functionsref->{createsymlink}->( $item, $srcdir, $tgtdir, $coref );
				} else {
					$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
				}
                delete $diffref->{0}{$item};
                delete $diffref->{1}{$item};
			} else {
				$l->logcroak( 'item exists as diff but is not in either ! ', "\n", $item, "\n" );
			}
		}    # create/modify loop
		(@deleteitems) and $l->trace( 'there are ', scalar(@deleteitems), 'delete items, starting loop' );
		foreach my $item ( reverse sort @deleteitems ) {    # Apply deletes in descending order
			$l->trace( '=== deleteitem: ', $item );
			my @dependents = find_items_starting_with( $item, $diffref->{2} );
			foreach my $depitem ( reverse sort @dependents ) {
				if ( $diffref->{2}{$item}{type} eq 'D' ) {
					$functionsref->{removedir}->( $depitem, $tgtdir, $coref );
				} elsif ( $diffref->{2}{$item}{type} eq 'F' ) {
					$functionsref->{removefile}->( $depitem, $tgtdir, $coref );
				} elsif ( $diffref->{2}{$item}{type} eq 'L' ) {
					$functionsref->{removesymlink}->( $depitem, $tgtdir, $coref );
				} else {
					$l->logcroak( 'Unknown type for item', "\n", $item, "\n" );
				}
				delete $diffref->{2}{$depitem};
				$diffref->{1}{$depitem} and next;
				delete $diffref->{0}{$depitem};
			}
		}    # delete loop
	}    # While loop
}

sub find_items_starting_with {
	my ( $item, $itemsref ) = @_;
	my @ret;
	foreach my $x ( keys %$itemsref ) {
		( $x eq $item or $x =~ /^\Q$item\E[\\\/]/ ) and push @ret, $x;
	}
	return @ret;
}
1;
