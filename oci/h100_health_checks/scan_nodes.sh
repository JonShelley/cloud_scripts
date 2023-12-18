#!/bin/bash

HOSTLIST=${1:-hostlist.txt}
BASEDIR=${2:-~/cloud_scripts/oci/h100_health_checks}
for x in `cat $HOSTLIST`
do
    echo $x
    HID=$(ssh $x sudo /usr/sbin/dmidecode -s system-serial-number)
    ssh $x "cd $BASEDIR;sudo python3 check_h100_setup.py 2>$1 | tee nhc_${HID}_\${HOSTNAME}.out"
    scp $x:$BASEDIR/nhc_${HID}_\${HOSTNAME}.out .
done