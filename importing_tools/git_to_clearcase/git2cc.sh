#!/bin/ksh -x 
BASEDIR=`dirname $0`

#. ~/.profile
#DEBUG="-d"


/usr/sbin/rpcinfo -n 371 -t clccor0101p 390512 2>&1 | grep -q "program 390512 version 3 ready and waiting"
albd=$?

if [[ "$albd" -ne "0" ]]
then
	echo "ClearCase is not responding on clccor0101p"
	echo "Exiting now."
	exit
fi


if [[ -f ${2}.lock ]]
then
	echo "Already processing ${2}"
	exit
fi

touch ${2}.lock

echo $PATH

if [ -d ${BASEDIR}/../../ActivePerl-5.12.1.1201 ]; then
	export PATH=${BASEDIR}/../../ActivePerl-5.12.1.1201/site/bin:${BASEDIR}/../../ActivePerl-5.12.1.1201/bin:$PATH
else 
	echo "We Can't Find ActivePerl!"
	exit 1
fi
export PERL5LIB="$BASEDIR/scripts:${BASEDIR}/../../ActivePerl-5.12.1.1201/lib:$BASEDIR/scripts:${BASEDIR}/../../ActivePerl-5.12.1.1201/site/lib"

# path to the directory containing clearcase magic file (default.magic)
# had to customize it for *.json files     
export MAGIC_PATH=$BASEDIR

perl $DEBUG $BASEDIR/scripts/git2cc.pl $@
retcode=$?
echo $retcode
ls -tr *.$$.log | tail -1 | read logname

if (( retcode > 0 ))
then
	echo "Error:  Return code was $retcode" >> $logname
	mv $logname errorlogs/
else
	gzip -9 $logname
    mv ${logname}.gz archivelogs/
fi

rm ${2}.lock

#grep -q 'Error: ' $logname
#rc=$?

#if [[ "$rc" -eq "0" ]]
#then
#	mv $logname errorlogs/
#else
#	mv $logname archivelogs/
#fi
