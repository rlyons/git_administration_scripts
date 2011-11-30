#!/bin/ksh
BASEDIR=`dirname $0`
if [ -d /cme/gfix/gitimports/ActivePerl-5.12.1.1201 ]; then
	export PATH=/cme/gfix/gitimports/ActivePerl-5.12.1.1201/site/bin:/cme/gfix/gitimports/ActivePerl-5.12.1.1201/bin:$PATH
else 
	export PATH=/home/e15580/ActivePerl-5.12/site/bin:/home/e15580/ActivePerl-5.12/bin:$PATH
fi
export PERL5LIB=$BASEDIR

# CUSTOM VARIABLES
export JAVA_EXECUTABLE_PATH='/usr/java/jdk1.6.0_24/bin/java'
export BAMBOO_CLI_JAR='bamboo-cli-1.1.0.jar'
export BAMBOO_URL='http://bamboo:8085'
export BAMBOO_USER='bamboo'
export BAMBOO_PASSWORD='bb01Sox!'

exec perl $DEBUG $BASEDIR/trigger_bamboo.pl $@
