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
use GitRepo;
use Carp qw(carp croak verbose);

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  import_label
  import_baseline
  import_latest_on_stream
  get_git_repo_name
  git_create_repo
  git_check_tag_exists
  git_check_branch_exists
  git_create_branch_from_tag
);

my $GIT_IGNORE_FILENAME              = '.gitignore';
my $GIT_TAG_SEPARATOR                = '!';
my $GIT_AUTHOR                       = ' "gitadmin <gitadmin@cmegroup.com>" ';
my $GIT_REPO_CREATION_COMMIT_MESSAGE = '"Initial Commit after git repo creation to allow branch creation."';
my $RSYNC_CMD                        = '/usr/bin/rsync --links --verbose --perms --times --recursive --delete --force --whole-file --exclude .git/ --exclude .gitlock --exclude ".nfs*" --exclude lost+found/';

sub import_label {
	my ( $git_repo_name, $repo_timestamp, $git_branch, $git_tag, $timestamp, $pref, $includesref, $git_branch_base_tag ) = @_;
	my $l              = Log::Log4perl->get_logger();
	my $commit_message = "\"import label $git_tag\"";

	$l->trace( 'Git Repo is ', $git_repo_name );

	# Create GIT REPO if it doesn't exist
	my $repodir = git_create_repo( $git_repo_name, $pref, $repo_timestamp );

	if ( git_check_tag_exists( $repodir, $git_tag ) ) {
		$l->info("Already Imported: $git_tag");
		return;
	}

	my $viewtag = $pref->{view_prefix}{value} . ( rand($$) + 10000 ) % 10000 . '_' . $git_repo_name;
	cc_create_view( $viewtag, $pref->{view_host}{value}, $pref->{view_stgloc}{value} );

	cc_set_configspec( $viewtag, $git_tag, $timestamp );

	_do_import( $viewtag, $pref, $repodir, $git_branch, $commit_message, $timestamp, [ sort keys %{$includesref} ], $git_tag, $git_branch_base_tag );

	cc_remove_view( $viewtag, $pref->{view_stgloc}{value} );
}

sub import_latest_on_stream {
	my ( $stream, $compsref, $pref, $git_branch ) = @_;
	my $l = Log::Log4perl->get_logger();
	$git_branch ||= shortname($stream);
	my $commit_message = "\"import latest\"";
	$l->trace( 'Git Branch is ', $git_branch );

	my $viewtag = $pref->{view_prefix}{value} . $$ . '_' . shortname($stream);
	cc_create_view( $viewtag, $pref->{view_host}{value}, $pref->{view_stgloc}{value}, $stream );

	foreach my $component ( sort keys %$compsref ) {
		next if $compsref->{$component}{is_composite};
		$l->trace( 'Component root_dir is ', $compsref->{$component}{root_dir} );

		my $git_repo_name = get_git_repo_name( $compsref->{$component}{root_dir} );
		$l->trace( 'Git Repo is ', $git_repo_name );

		# Create GIT REPO if it doesn't exist
		my $repodir = git_create_repo( $git_repo_name, $pref, $compsref->{$component}{create_timestamp} );

		_do_import( $viewtag, $pref, $repodir, $git_branch, $commit_message, undef, [ $compsref->{$component}{root_dir} ] );
	}
	cc_remove_view( $viewtag, $pref->{view_stgloc}{value} );
}

sub git_create_branch_from_tag {
	my ( $repodir, $git_branch, $git_tag ) = @_;

	# Create branch from tag
	git_lock_repo($repodir);
	git_cleanup($repodir);    # Cleanup any junk left over
	git_create_branch( $repodir, $git_branch, $git_tag );
	git_unlock_repo($repodir);
}

sub import_baseline {
	my ( $stream, $blref, $pref, $git_branch, @extratags ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->trace("Processing baseline: $blref->{baseline}");
	if ( $blref->{component}{is_composite} ) {
		$l->info("Skipping baseline on composite component.");
		return 1;
	}
	cc_label_baseline( $blref->{baseline}, $pref );

	my $baseline = shortname( $blref->{baseline} );
	my $git_tag  = $baseline;
	$git_branch ||= shortname($stream);

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
			return 1;
		}

		# This baseline is foundation for this stream
		if ( git_check_branch_exists( $repodir, $git_branch ) ) {
			$l->info("Branch already exists: $git_branch");
			return 1;
		}

		# Create branch from tag
		git_create_branch_from_tag( $repodir, $git_branch, $git_tag );
		return 1;
	}

	$l->info( "Importing baseline: $baseline into " . get_git_repo_name( $blref->{component}{root_dir} ) );

	# Create stream and view
	my ($viewtag) = cc_create_import_streams( $stream, $pref, $blref->{component}{component}, $blref->{baseline} );
	my $commit_message = "\"import $blref->{baseline}\"";
	my $commit_sha = _do_import( $viewtag, $pref, $repodir, $git_branch, $commit_message, $blref->{create_timestamp}, [ $blref->{component}{root_dir} ], $git_tag );
	cc_remove_import_stream( $stream, $pref, $viewtag );

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
	my ( $viewtag, $pref, $repodir, $git_branch, $commit_message, $create_timestamp, $import_dirs_ref, $git_tag, $git_branch_base_tag ) = @_;
	my $l = Log::Log4perl->get_logger();

	# Start the view and mount the vob
	cc_startview($viewtag);
	cc_mountvobs( cc_get_source_vobs_for_comp_dirs(@$import_dirs_ref) );

	my $view_root = get_view_root($viewtag);
	$l->trace( 'view_root is ', $view_root );

	# START importing
	git_lock_repo($repodir);    # Lock the repo

	git_cleanup($repodir);      # Cleanup any junk left over

	git_check_branch_exists( $repodir, $git_branch )
	  or git_create_branch( $repodir, $git_branch, $git_branch_base_tag );    # Create branch from master if it does not exist

	git_checkout_branch( $repodir, $git_branch );                             # check out the branch

	rsync_dirs_to_git( $view_root, $repodir, $import_dirs_ref );              # rsync from clearcase view

	my $commit_sha = git_commit_all( $repodir, $commit_message, $create_timestamp );    # add all and commit

	$git_tag
	  and git_tag_commit( $repodir, $commit_sha, $git_tag );                            # Tag the commit

	git_unlock_repo($repodir);                                                          # Unlock the repo

	return $commit_sha;
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

sub rsync_dirs_to_git {
	my ( $view_root, $tgtdir, $srcdirsref ) = @_;
	my $l = Log::Log4perl->get_logger();
	$l->debug( 'rsyncing   to: ', $tgtdir );

	if ( scalar(@$srcdirsref) == 1 ) {    # Only one directory specified
		my @ret = cc_run_cmd( join( ' ', $RSYNC_CMD, File::Spec->catfile( $view_root, $srcdirsref->[0] ) . '/', $tgtdir ) );
		$l->is_trace() and $l->trace( join( "\n", "", @ret, "" ) );
		return;
	}

	# Multiple source directories
	# need to create a temp file with the list of source items for rsync
	my $srcdir = File::Spec->catfile( $view_root, 'cc' );
	my ( $tmpfh, $tmpfilename ) = File::Temp::tempfile();
	foreach my $item (@$srcdirsref) {
		my $tmpitem = $item;
		$tmpitem =~ s/^\/cc\///;

		# rsync fails if the source item does not exist
		unless ( -e File::Spec->catfile( $view_root, $item ) ) {
			my $tgtitem = File::Spec->catfile( $view_root, $tmpitem );
			if ( -e $tgtitem ) {    # Need to remove from target if not there in source - so we can keep it in sync
				$l->debug("Missing source item: $item, present in target. Removing it.");
				File::Path::remove_tree( $tgtitem, { verbose => 1, error => \my $err } );
				if (@$err) {
					for my $diag (@$err) {
						my ( $file, $message ) = %$diag;
						if ( $file eq '' ) {
							$l->warn("general error: $message\n");
						} else {
							$l->warn("problem unlinking $file: $message\n");
						}
					}
					$l->logcroak("Failed to remove: $tgtitem");
				}
			} else {
				$l->warn("Ignoring missing source item: $item");
			}
			next;
		}
		print $tmpfh $tmpitem, "\n";
	}
	close $tmpfh;
	my @ret = cc_run_cmd( join( ' ', $RSYNC_CMD, '--files-from=' . $tmpfilename, $srcdir . '/', $tgtdir ) );
	unlink $tmpfilename;
	$l->is_trace() and $l->trace( join( "\n", "", @ret, "" ) );
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
	git_lock_repo($repodir);

	# Create a new git repository
	$l->info( 'creating new git repository: ', $repodir );
	my $ret = git_run_cmd( $repodir, 'git init' );
	$l->trace($ret);

	$l->debug( 'creating .gitignore file in: ', $repodir );
	create_gitignore_file($repodir);

	git_commit_all( $repodir, $GIT_REPO_CREATION_COMMIT_MESSAGE, $create_timestamp );
	git_unlock_repo($repodir);
	return $repodir;
}

sub git_check_branch_exists {
	my ( $repodir, $git_branch ) = @_;
	$git_branch ||= 'master';
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

sub create_gitignore_file {
	my ($repodir) = @_;
	my $l = Log::Log4perl->get_logger();

	# Create an empty file and commit
	my $ignorefile = File::Spec->catfile( $repodir, $GIT_IGNORE_FILENAME );
	open( my $fh, '>', $ignorefile ) or croak( "create gitignore failed ", $! );
	print $fh '.nfs*',    "\n";
	print $fh '.gitlock', "\n";
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
