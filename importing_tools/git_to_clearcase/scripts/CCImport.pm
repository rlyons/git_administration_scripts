#!perl -w
package CCImport;
use strict;
use warnings;
use File::Path qw(make_path);
use File::Spec;
use File::Basename;
use File::Copy;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Carp qw(carp croak verbose);
use RunCmd;
use FSUtils;

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  cc_import_from_git
);

my $functionsref = bless {
	createdir     => \&_createdir,
	copyfile      => \&_copyfile,
	createsymlink => \&_createsymlink,
	removedir     => \&_removedir,
	removefile    => \&_removefile,
	removesymlink => \&_removesymlink,
};


sub cc_import_from_git {
	my ( $repodir, $view, $component_root ) = @_;
	my $srcdir    = File::Spec->canonpath($repodir);
	my $tgtdir    = File::Spec->canonpath( File::Spec->catfile( get_view_root($view), $component_root ) );
	save_sha1sum_txt($repodir);
	my $diffref   = fscomparedirs( $srcdir, $tgtdir );
	my $diffcount = scalar( keys %{ $diffref->{0} } );
	my %checkouts;
	applydiffs( $srcdir, $tgtdir, $diffref, $functionsref, \%checkouts );
	foreach my $item ( reverse sort keys %checkouts ) {
		_cc_checkin( $item, \%checkouts, '-identical' );
	}
	return $diffcount;
}

sub save_sha1sum_txt {
	my ($indir) = @_;
	my $sha1sumfile = '.git.sha1sum.txt';

	my $href = fsread($indir);
	
	my $outfile = File::Spec->catfile($indir,$sha1sumfile);
	
	my $l = Log::Log4perl->get_logger();
	open my $fh, '>', $outfile or $l->logcroak("Cannot open for writing: $outfile");
	foreach my $item ( sort keys %$href ) {
		next if ($item eq $sha1sumfile); # Do not include the sha1sum for the same file we are generating
		print $fh $href->{$item}{type}, ',', $href->{$item}{SHA1}, ',', $item, "\n";
	}
	close $fh;
}

sub _createdir {
	my ( $item, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_createdir: ', $item );
	my $itemname = File::Spec->catfile( $tgtdir, $item );
	_cc_mkdir( $itemname, $coref );
	return;
}

sub _copyfile {
	my ( $item, $srcdir, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_copyfile: ', $item );
	my $src = File::Spec->catfile( $srcdir, $item );
	my $tgt = File::Spec->catfile( $tgtdir, $item );
	_cc_create_element( $src, $tgt, $coref );
	return;
}

sub _createsymlink {
	my ( $item, $srcdir, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_createsymlink: ', $item );
	my $src = File::Spec->catfile( $srcdir, $item );
	my $tgt = File::Spec->catfile( $tgtdir, $item );
	_cc_create_symlink( $src, $tgt, $coref );
}

sub _removedir {
	my ( $item, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_removedir: ', $item );
	my $itemname = File::Spec->catfile( $tgtdir, $item );
	_cc_rmname_dir( $itemname, $coref );
	return;
}

sub _removefile {
	my ( $item, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_removefile: ', $item );
	my $itemname = File::Spec->catfile( $tgtdir, $item );
	_cc_rmname( $itemname, $coref );
}

sub _removesymlink {
	my ( $item, $tgtdir, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace( '_removesymlink: ', $item );
	my $itemname = File::Spec->catfile( $tgtdir, $item );
	_cc_rmname( $itemname, $coref );
}

sub _cc_mkdir {
	my ( $itemname, $coref ) = @_;
	my $l       = Log::Log4perl->get_logger();
	my $dirname = dirname $itemname;
	_cc_checkout( $dirname, $coref );
	$l->debug("mkdir: $itemname");
	cc_run_cmd("cleartool mkdir -nc \'$itemname\'");
	$coref->{$itemname} = 1;
}

sub _cc_create_element {
	my ( $src, $tgt, $coref ) = @_;
	my $l       = Log::Log4perl->get_logger();
	my $dirname = dirname $tgt;
	if ( -e $tgt ) {

		# Element already exists, copy over
		_cc_checkout( $tgt, $coref );
		$l->debug("loadfile: $tgt");
		copy( $src, $tgt ) or $l->logcroak("Failed to copy from '$src' to '$tgt'");
		return;
	}

	# Create a new element
	_cc_checkout( $dirname, $coref );
	$l->debug("loadfile: $tgt");
	copy( $src, $tgt ) or $l->logcroak("Failed to copy from '$src' to '$tgt'");
	$l->debug("mkelem: $tgt");
	cc_run_cmd("cleartool mkelem -nc -ptime \'$tgt\'");
	$coref->{$tgt} = 1;

	# Special case - when creating zero byte size file
	# clearcase does not let you check-in the file unless you give -identical
	# because by default, won't create version with data identical to predecessor (which is version 0).
	( -f $tgt ) and ( -z $tgt ) and _cc_checkin( $tgt, $coref, '-identical' );
}

sub _cc_create_symlink {
	my ( $src, $tgt, $coref ) = @_;
	my $l       = Log::Log4perl->get_logger();
	my $dirname = dirname $tgt;
	_cc_checkout( $dirname, $coref );
	my $linktgt  = readlink($src);
	my $linkname = basename($tgt);
	$l->debug("mksymlink: $tgt");
	cc_run_cmd( "cleartool ln -s -nc \'$linktgt\' \'$linkname\'", gitrepo => $dirname );
}

sub _cc_rmname_dir {
	my ( $itemname, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();

	# Find checkout items in this directory that we are about to delete
	foreach my $item ( reverse sort keys %$coref ) {
		if ( $item eq $itemname or ( index( $item, $itemname . '/' ) == 0 ) ) {
			_cc_checkin( $item, $coref, '-identical' );
		}
	}

	my $dirname = dirname $itemname;
	_cc_checkout( $dirname, $coref );
	$l->debug("rmname: $itemname");
	cc_run_cmd("cleartool rmname -nc \'$itemname\'");
}

sub _cc_rmname {
	my ( $itemname, $coref ) = @_;
	my $l       = Log::Log4perl->get_logger();
	my $dirname = dirname $itemname;
	_cc_checkout( $dirname, $coref );
	$l->debug("rmname: $itemname");
	cc_run_cmd("cleartool rmname -nc \'$itemname\'");
}

sub _cc_checkout {
	my ( $itemname, $coref ) = @_;
	my $l = Log::Log4perl->get_logger();
	if ( $coref->{$itemname} ) {
		$l->trace("already checkedout: $itemname");
		return;
	}

	$l->debug("checkout: $itemname");
	cc_run_cmd("cleartool checkout -nc \'$itemname\'");
	$coref->{$itemname} = 1;
}

sub _cc_checkin {
	my ( $itemname, $coref, $identical ) = @_;
	$identical ||= '';
	my $l = Log::Log4perl->get_logger();

	unless ( $coref->{$itemname} ) {
		$l->trace("already checkedin: $itemname");
		return;
	}
	$l->debug("checkin: $itemname");
	cc_run_cmd("cleartool checkin -nc $identical \'$itemname\'");
	delete $coref->{$itemname};
}

1;
