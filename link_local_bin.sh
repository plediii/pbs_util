#!/bin/bash
# Install utilities by making a link to them in ~/local/bin

TARGET=${HOME}/local/bin
mkdir -p ${TARGET}

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

