#!/bin/ksh

cd /home/_svcmaven-dev/gitimports/git2cc/cm_git_import

#for i in etpa_blocks etpa_bobsync etpa_common_bugfix etpa_common etpa_csvtools etpa_demo_maja etpa_harpoon_bugfix etpa_harpoon etpa_icebreaker etpa_integrity_audit etpa_maja_bugfix etpa_maja etpa_skepclientinstaller etpa_skep etpa_template_management etpa_template_packaging etpa_tradeup
for i in etpa_blocks etpa_harpoon_bugfix etpa_harpoon etpa_icebreaker etpa_skep etpa_template_packaging etpa_maja
do
	./git2cc.sh -f config/git2cc/${i}.xml 2>&1 >/dev/null
done
