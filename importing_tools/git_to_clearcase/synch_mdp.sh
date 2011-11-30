#!/bin/ksh

cd /home/_svcmaven-dev/gitimports/git2cc/cm_git_import

for i in mdp_fix
do
	./git2cc.sh -f config/git2cc/${i}.xml 2>&1 >/dev/null
done
