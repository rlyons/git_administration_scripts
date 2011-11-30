#!perl -w
package GitRepo;
use strict;
use warnings;
use File::Path qw(make_path);
use File::Spec;
use Data::Dumper;
use Log::Log4perl qw(:easy);
use Carp qw(carp croak verbose);
use RepoLock;
use RunCmd;

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(
  git_get_repo_tags
  git_lock_repo
  git_unlock_repo
  git_get_branches
  git_checkout_branch
  git_create_branch
  git_delete_branch
  git_try_branch_checkout
  git_check_repo_exists
  git_clone_repo
  git_cleanup
  git_fetch
);

my $GIT_REPO_LOCK_DIR = '.gitlock';

sub git_get_repo_tags {
	my ( $repodir, @gitrepos ) = @_;
	my $l = Log::Log4perl->get_logger();

	my %repo_tags;
	my %duplicate_tags;
	my %all_tags;
	foreach my $gitrepo (@gitrepos) {
		my $gitrepodir = File::Spec->catfile( $repodir, $gitrepo );
		$l->debug("Getting tags for gitrepo: $gitrepodir");
		my @tags = git_run_cmd( $gitrepodir, "git tag -l" );
		my $ref = cc_run_cmd_parallel( "git log -1 --pretty=\"%ai_%ci_%H\"", \@tags, gitrepo => $gitrepodir );
		foreach my $tag (@tags) {
			if ( $all_tags{$tag} ) {
				$duplicate_tags{$tag}{$gitrepo} = 1;
				foreach my $tmprepo ( keys %{ $all_tags{$tag} } ) {
					$duplicate_tags{$tag}{$tmprepo} = 1;
				}
			}
			$all_tags{$tag}{$gitrepo} = 1;

			my $tscommit = $ref->{$tag}->[0];
			$repo_tags{$gitrepo}{$tscommit}{$tag} = 1;
		}
	}

	if ( keys %duplicate_tags ) {
		my $str = "Duplicate tags found:\n";
		foreach my $tag ( sort keys %duplicate_tags ) {
			$str .= "    TAG: $tag exists in gitrepos: " . join( ", ", sort keys %{ $duplicate_tags{$tag} } ) . "\n";
		}
		$l->logdie( "Please make sure there are no duplicate tags in the gitrepos belonging to the same pvob\n" . $str );
	}

	return \%repo_tags;
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

sub git_get_branches {
	my ($repodir) = @_;
	my %ret;
	my @ret1 = git_run_cmd( $repodir, 'git branch' );
	foreach my $item (@ret1) {
		$ret{ substr( $item, 2 ) } = 1;
	}
	return \%ret;
}

sub git_try_branch_checkout {
	eval { git_checkout_branch(@_) };
}

sub git_checkout_branch {
	my ( $repodir, $git_branch ) = @_;
	my $l = Log::Log4perl->get_logger();

	$git_branch ||= 'master';
	my $gitcmd = "git checkout $git_branch";
	my $ret = git_run_cmd( $repodir, $gitcmd );
	$l->trace($ret);
}

sub git_create_branch {
	my ( $repodir, $git_branch, $git_tag ) = @_;
	my $l = Log::Log4perl->get_logger();

	my $cmd = 'git branch --verbose --verbose ' . $git_branch;
	if ($git_tag) {
		$l->debug("$repodir: creating branch $git_branch from $git_tag");
		$cmd .= ' ' . $git_tag;
	} else {
		$l->trace("$repodir: switching to master");
		my $ret = git_run_cmd( $repodir, 'git checkout master' );
		$l->trace($ret);
		$l->debug("$repodir: creating branch $git_branch from master");
	}
	my $ret1 = git_run_cmd( $repodir, $cmd );
	$l->trace($ret1);
}

sub git_delete_branch {
	my ( $repodir, $git_branch ) = @_;
	my $l = Log::Log4perl->get_logger();

	$git_branch ||= 'master';
	my $gitcmd = "git branch -D $git_branch";
	my $ret = git_run_cmd( $repodir, $gitcmd );
	$l->trace($ret);
}

sub git_check_repo_exists {
	my ($repodir) = @_;
	my $l = Log::Log4perl->get_logger();
	my $gitdir = File::Spec->catfile( $repodir, '.git' );
	if ( -d $gitdir ) {
		$l->trace( $gitdir . " exists." );
		return 1;
	} else {
		$l->trace( $gitdir . " does not exist." );
		return 0;
	}
}

sub git_clone_repo {
	my ( $url, $repodir ) = @_;
	my $l = Log::Log4perl->get_logger();
	my $ret = cc_run_cmd( 'git clone ' . $url . ' ' . $repodir );
	$l->trace($ret);
}

sub git_fetch { 
	my ( $repodir ) = @_;
	my $l = Log::Log4perl->get_logger();
	my $ret = git_run_cmd( $repodir, 'git fetch --all' );
	$l->trace($ret);
}

sub git_cleanup {
	my ($repodir) = @_;
	my $l = Log::Log4perl->get_logger();
	my $ret = git_run_cmd( $repodir, 'git clean -f' );
	$l->trace($ret);
	$ret = git_run_cmd( $repodir, 'git reset --hard' );
	$l->trace($ret);
}

1;
