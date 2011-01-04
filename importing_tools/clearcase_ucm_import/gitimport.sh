#!/bin/ksh -x 
BASEDIR=`dirname $0`
export PATH=/home/e15580/ActivePerl-5.12/site/bin:/home/e15580/ActivePerl-5.12/bin:$PATH
export PERL5LIB=$BASEDIR/scripts
perl $DEBUG $BASEDIR/scripts/cc2git.pl $@
