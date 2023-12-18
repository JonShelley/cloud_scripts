#!/bin/bash

HOSTLIST=${1:-hostlist.txt}
BASEDIR=${2:-~/cloud_scripts/oci/h100_health_checks}
for x in `cat $HOSTLIST`
do
    echo $x
    HID=$(ssh $x sudo /usr/sbin/dmidecode -s system-serial-number)
    ssh $x "cd jshelley/cloud_scripts/oci/h100_health_checks;sudo python3 check_h100_setup.py | tee nhc_${HID}_\$HOSTNAME_2.out"
done