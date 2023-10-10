#!/bin/bash
set -ex

IF_RENAME_SERVICE=oci_network_interface_naming.service
SBIN_SCRIPT_NAME=/usr/sbin/oci_network_interface_monitor.sh
SERVICE_PATH=/etc/systemd/system
SERVICE_NAME=oci_network_interface_monitor.service

#
# setup systemd service
#

cat <<EOF > $SBIN_SCRIPT_NAME
#!/bin/bash

# monitoring service to check that hca_id's are named correctly
# if incorrect, restart oci_network_interface_naming.service

while true; do

    for device in \$(ibdev2netdev -v | sort -n | cut -f2 -d' '); do

        part_id=\$(ibv_devinfo -d \$device | sed -n 's/^[\t]*link_layer:[\ \t]*\([0-9]*\)\$/\1/p')

        if [[ \$device != *"eth"* && \$device != *"rdma"* ]]; then
            systemctl enable $IF_RENAME_SERVICE
            systemctl restart $IF_RENAME_SERVICE
            sleep 60
            break
        fi

    done

    sleep 60

done
EOF
chmod 755 $SBIN_SCRIPT_NAME

cat <<EOF > $SERVICE_PATH/$SERVICE_NAME
[Unit]
Description=OCI Network Interface Monitor
After=network.target

[Service]
Type=simple
ExecStart=$SBIN_SCRIPT_NAME
RemainAfterExit=true
StandardOutput=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl enable $SERVICE_NAME
systemctl start $SERVICE_NAME
