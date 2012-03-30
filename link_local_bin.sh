#!/bin/bash
# Install utilities by making a link to them in ~/local/bin

if [ $# -lt 1 ]; then 
    TARGET=${HOME}/local/bin
else
    TARGET=$1
fi

if [ ! -d ${TARGET} ]; then
    echo
    echo "  Usage: $0 [A DIRECTORY IN YOUR PATH]"
    echo
    echo " Directory ${TARGET} does not exist. "
    echo
    exit 1
fi

HERE=`pwd`
exes="nice_submit pbs_chain submit_command qdel_range qdel_all qdel_name kill_submitted_jobs pbs_alert"

for exe in $exes; do
    if [ ! -e ${exe}.py ]; then
	echo "WARNING: ${exe}.py does not exist here... skipping it."
	continue
    fi
    if [ -e ${TARGET}/${exe} ]; then
	echo "REPLACING: ${TARGET}/${exe}"
	rm ${TARGET}/${exe}
	if [ -e ${TARGET}/${exe} ]; then
	    echo "FAILED to replace: ${TARGET}/${exe}."
	fi
	ln -fs ${HERE}/${exe}.py ${TARGET}/${exe}
    else
	echo "LINKING ${TARGET}/${exe}"
	ln -fs ${HERE}/${exe}.py ${TARGET}/${exe}
    fi

done

