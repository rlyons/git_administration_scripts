#!perl -w
package GitImport;
use strict;
use warnings;
use File::Path qw(make_path);
use File::Spec;
use Data::Dumper;
use Log::Log4perl;
use CCUtils;
use RunCmd;
use RepoLock;
use Carp qw(carp croak verbose);

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  import_baseline
  import_latest_on_stream
);

my $GIT_IGNORE_FILENAME              = '.gitignore';
my $GIT_REPO_LOCK_DIR                = '.gitlock';
my $GIT_TAG_SEPARATOR                = '!';
my $GIT_AUTHOR                       = ' "gitadmin <gitadmin@cmegroup.com>" ';
my $GIT_REPO_CREATION_COMMIT_MESSAGE = '"Initial Commit after git repo creation to allow branch creation."';
my $RSYNC_CMD                        = '/usr/bin/rsync --verbose --perms --times --recursive --delete --force --whole-file --exclude .git/ --exclude .gitlock --exclude ".nfs*"';
my $UNIX_VIEW_ROOT                   = '/view';

sub import_latest_on_stream {
	my ( $stream, $compsref, $pref ) = @_;
	my $l              = Log::Log4perl->get_logger();
	my $git_branch     = shortname($stream);
	my $commit_message = "\"import latest\"";
	$l->trace( 'Git Branch is ', $git_branch );

	my $viewtag = $pref->{view_prefix}{value} . '0_' . shortname($stream);
	cc_create_dyn_view( $viewtag, $pref->{view_host}{value}, $pref->{view_stgloc}{value}, $stream );

	foreach my $component ( sort keys %$compsref ) {
		next if $compsref->{$component}{is_composite};
		$l->trace( 'Component root_dir is ', $compsref->{$component}{root_dir} );

		my $git_repo_name = get_git_repo_name( $compsref->{$component}{root_dir} );
		$l->trace( 'Git Repo is ', $git_repo_name );

		# Create GIT REPO if it doesn't exist
		my $repodir = git_create_repo( $git_repo_name, $pref, $compsref->{$component}{create_timestamp} );

		_do_import( $compsref->{$component}{root_dir}, $viewtag, $pref, $repodir, $git_branch, $commit_message );
	}
	cc_remove_import_streams( $stream, $pref );
}

sub import_baseline {
	my ( $num, $totalnum, $stream, $blref, $compsref, $pref, @extratags ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->info("Processing baseline $num of $totalnum: $blref->{baseline}");
	if ( $blref->{component}{is_composite} ) {
		$l->info("Skipping baseline on composite component.");
		return;
	}
	cc_label_baseline( $blref->{baseline}, $pref );

	my $baseline   = shortname( $blref->{baseline} );
	my $git_tag    = $baseline;
	my $git_branch = shortname($stream);
	$l->trace( 'Git Branch is ',         $git_branch );
	$l->trace( 'Baseline is ',           $baseline );
	$l->trace( 'Git tag is ',            $git_tag );
	$l->trace( 'Component root_dir is ', $blref->{component}{root_dir} );

	my $git_repo_name = get_git_repo_name( $blref->{component}{root_dir} );
	$l->trace( 'Git Repo is ', $git_repo_name );

	# Create GIT REPO if it doesn't exist
	my $repodir = git_create_repo( $git_repo_name, $pref, $blref->{component}{create_timestamp} );

	# Check if the tag is already present
	if ( git_check_tag_exists( $repodir, $git_tag ) ) {
		if ( !$blref->{is_foundation} ) {
			$l->info("Already Imported: $git_tag");
			return;
		}

		# This baseline is foundation for this stream
		if ( git_check_branch_exists( $repodir, $git_branch ) ) {
			$l->info("Branch already exists: $git_branch");
			return;
		}

		# Create branch from tag
		git_lock_repo($repodir);
		git_cleanup($repodir);    # Cleanup any junk left over
		git_create_branch( $repodir, $git_branch, $git_tag );
		git_unlock_repo($repodir);
		return;
	}

	$l->info( "Importing baseline: $baseline into " . get_git_repo_name( $blref->{component}{root_dir} ) );

	# Create stream and view
	my ($viewtag) = cc_create_import_streams( $stream, $pref, $blref->{baseline} );
	my $commit_message = "\"import $blref->{baseline}\"";
	my $commit_sha = _do_import( $blref->{component}{root_dir}, $viewtag, $pref, $repodir, $git_branch, $commit_message, $blref->{create_timestamp}, $git_tag );
	cc_remove_import_streams( $stream, $pref );

	if (@extratags) {
		git_lock_repo($repodir);
		git_cleanup($repodir);    # Cleanup any junk left over
		foreach my $tag (@extratags) {
			next if ( git_check_tag_exists( $repodir, $tag ) );
			git_tag_commit( $repodir, $commit_sha, $tag );
		}
		git_unlock_repo($repodir);
	}
	return $commit_sha;
}

sub _do_import {
	my ( $comp_root_dir, $viewtag, $pref, $repodir, $git_branch, $commit_message, $create_timestamp, @git_tags ) = @_;
	my $l = Log::Log4perl->get_logger();

	# Start the view and mount the vob
	cc_startview($viewtag);
	cc_mountvob( cc_get_source_vob_for_comp_dir($comp_root_dir) );

	my $comp_root = get_view_component_root( $viewtag, $comp_root_dir );
	$l->trace( 'Component view_root is ', $comp_root );

	# START importing
	git_lock_repo($repodir);    # Lock the repo

	git_cleanup($repodir);      # Cleanup any junk left over

	git_check_branch_exists( $repodir, $git_branch )
	  or git_create_branch( $repodir, $git_branch );    # Create branch from master if it does not exist

	git_checkout_branch( $repodir, $git_branch );       # check out the branch

	rsync_dir_to_git( $comp_root, $repodir );           # rsync from clearcase view

	my $commit_sha = git_commit_all( $repodir, $commit_message, $create_timestamp );    # add all and commit

	foreach my $git_tag (@git_tags) {
		git_tag_commit( $repodir, $commit_sha, $git_tag );                              # Tag the commit
	}

	git_unlock_repo($repodir);                                                          # Unlock the repo

	return $commit_sha;
}

sub git_cleanup {
	my ($repodir) = @_;
	my $l = Log::Log4perl->get_logger();
	my $ret = git_run_cmd( $repodir, 'git clean -f' );
	$l->trace($ret);
	$ret = git_run_cmd( $repodir, 'git reset --hard' );
	$l->trace($ret);
}

sub git_check_tag_exists {
	my ( $repodir, $git_tag ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->trace("Checking if $git_tag exists");
	my @tags = git_run_cmd( $repodir, 'git tag' );
	foreach my $tag (@tags) {
		if ( $tag eq $git_tag ) {
			$l->trace("$git_tag exists.");
			return 1;
		}
	}
	$l->trace("$git_tag does not exist.");
	return;
}

sub git_tag_commit {
	my ( $repodir, $commitsha1, $tag ) = @_;
	my $l = Log::Log4perl->get_logger();

	$l->info("$repodir: Tagging $commitsha1 with : $tag");
	my $ret = git_run_cmd( $repodir, "git tag $tag $commitsha1" );
	$l->trace($ret);
}

sub rsync_dir_to_git {
	my ( $srcdir, $tgtdir ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug( 'rsyncing from: ', $srcdir );
	$l->trace( 'rsyncing   to: ', $tgtdir );

	my @ret = cc_run_cmd( join( ' ', $RSYNC_CMD, $srcdir, $tgtdir ) );
	foreach (@ret) { $l->trace($_) }
}

sub git_create_repo {
	my ( $git_repo, $pref, $create_timestamp ) = @_;
	my $l = Log::Log4perl->get_logger();
	$pref->{repodir}{value} or croak("repodir is not defined or is empty !");
	my $repodir = File::Spec->catfile( File::Spec->rel2abs( $pref->{repodir}{value} ), $git_repo );
	my $gitdir = File::Spec->catfile( $repodir, '.git' );
	if ( -d $repodir ) {
		if ( -d $gitdir ) {
			$l->trace("git repository already exists: $repodir");
			return $repodir;
		}
	} else {
		make_path( $repodir, { verbose => 1 } ) or croak( "mkpath failed ", $! );
	}

	# Create a new git repository
	$l->info( 'creating new git repository: ', $repodir );
	my $ret = git_run_cmd( $repodir, 'git init' );
	$l->trace($ret);

	$l->debug( 'creating .gitignore file in: ', $repodir );
	create_gitignore_file($repodir);

	git_commit_all( $repodir, $GIT_REPO_CREATION_COMMIT_MESSAGE, $create_timestamp );
	return $repodir;
}

sub git_checkout_branch {
	my ( $repodir, $git_branch ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $gitcmd = "git checkout $git_branch";
	my $ret = git_run_cmd( $repodir, $gitcmd );
	$l->trace($ret);
}

sub git_check_branch_exists {
	my ( $repodir, $git_branch ) = @_;
	my $l = Log::Log4perl->get_logger();
	my @ret = git_run_cmd( $repodir, 'git branch' );
	foreach my $line (@ret) {
		$line =~ s/.*\s+//;
		if ( $line eq $git_branch ) {
			return 1;
		}
	}
	return 0;
}

sub git_create_branch {
	my ( $repodir, $git_branch, $git_tag ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace("$repodir: switching to master");
	my $ret = git_run_cmd( $repodir, 'git checkout master' );
	$l->trace($ret);

	my $cmd = 'git branch --verbose --verbose ' . $git_branch;
	if ($git_tag) {
		$l->debug("$repodir: creating branch $git_branch from $git_tag");
		$cmd .= ' ' . $git_tag;
	} else {
		$l->debug("$repodir: creating branch $git_branch from master");
	}
	my $ret1 = git_run_cmd( $repodir, $cmd );
	$l->trace($ret1);
}
sub git_lock_repo {
	my ($repodir) = @_;
	my $lockdir = File::Spec->catfile( $repodir, $GIT_REPO_LOCK_DIR );
	lock_repo($lockdir);
}

sub git_unlock_repo {
	my ($repodir) = @_;
	my $lockdir = File::Spec->catfile( $repodir, $GIT_REPO_LOCK_DIR );
	unlock_repo($lockdir);
}

sub git_commit_all {
	my ( $repodir, $commit_message, $create_timestamp ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug("$repodir: add all");
	my $ret = git_run_cmd( $repodir, 'git add --all --verbose' );
	$l->trace($ret);

	my @ret = git_run_cmd( $repodir, 'git status --porcelain' );
	if ( scalar(@ret) ) {
		$l->debug("$repodir: committing");
		my $gitcmd = 'git commit --all --verbose --author ' . $GIT_AUTHOR . '--m ' . $commit_message;
		$create_timestamp and $gitcmd .= " --date=$create_timestamp";
		$ret = git_run_cmd( $repodir, $gitcmd );
		$l->trace($ret);
	}

	$l->trace( 'getting commit hash: ', $repodir );
	my $commithash = git_run_cmd( $repodir, 'git log -1 --pretty=format:%H' );
	if ( scalar(@ret) ) {
		$l->debug("$repodir: commit hash: $commithash");
	} else {
		$l->debug("$repodir: previous commit hash: $commithash");
	}
	return $commithash;
}

sub get_view_component_root {
	my ( $viewtag, $comp_root ) = @_;
	( $^O eq 'MSWin32' ) and return;    # Not supported on windows
	return File::Spec->canonpath( File::Spec->catfile( $UNIX_VIEW_ROOT, $viewtag, $comp_root ) );
}

sub create_gitignore_file {
	my ($repodir) = @_;
	my $l = Log::Log4perl->get_logger();

	# Create an empty file and commit
	my $ignorefile = File::Spec->catfile( $repodir, $GIT_IGNORE_FILENAME );
	open( my $fh, '>', $ignorefile ) or croak( "create gitignore failed ", $! );
	print $fh '.nfs*', "\n";
	close $fh;
}

sub get_git_repo_name {
	my ($root_dir) = @_;
	$root_dir =~ s/[\\\/]$//;
	$root_dir =~ s/^[\\\/]//;
	$root_dir =~ s/^cc[\\\/]//;
	$root_dir =~ s/[\\\/]/_/g;
	return $root_dir;
}
1;
