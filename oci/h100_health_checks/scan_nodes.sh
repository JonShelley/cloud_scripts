#!/bin/bash

HOSTLIST=${1:-hostlist.txt}
BASEDIR=${2:-~/cloud_scripts/oci/h100_health_checks}
DATE=`date "+%Y%m%d-%H%M%S"`
mkdir -p $DATE
for x in `cat $HOSTLIST`
do
    echo $x
    HID=$(ssh $x sudo /usr/sbin/dmidecode -s system-serial-number)
    NODE_NAME=$(ssh $x hostname)
    ssh $x "cd $BASEDIR;sudo python3 check_h100_setup.py 2>&1 | tee nhc_${HID}_${NODE_NAME}.out"
    scp $x:$BASEDIR/nhc_${HID}_${NODE_NAME}.out $DATE/.
done