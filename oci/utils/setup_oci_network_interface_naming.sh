#!/bin/bash
set -ex

#
# install rdma_rename with NAME_FIXED option
# install rdma_rename monitor
#

BASE_DIR=/nfs/scratch/jshelley
SBIN_SCRIPT_NAME=/usr/sbin/oci_network_interface_naming.sh
SERVICE_PATH=/etc/systemd/system
SERVICE_NAME=oci_network_interface_naming.service
MONITOR_SCRIPT=$BASE_DIR/setup_oci_network_interface_monitor.sh

pushd /tmp
rdma_core_branch=stable-v34
git clone -b $rdma_core_branch https://github.com/linux-rdma/rdma-core.git
pushd rdma-core
bash build.sh
cp build/bin/rdma_rename /usr/sbin/rdma_rename_$rdma_core_branch
popd
rm -rf rdma-core
popd

#
# setup systemd service
#


cat <<EOF > $SBIN_SCRIPT_NAME
#!/bin/bash

rdma_rename=/usr/sbin/rdma_rename_${rdma_core_branch}

eth_index=0
rdma_index=0

for old_device in \$(ibdev2netdev -v | sort -n | cut -f2 -d' '); do

        part_id=\$(ibv_devinfo -d \$old_device | sed -n 's/^[\t]*vendor_part_id:[\ \t]*\([0-9]*\)\$/\1/p')

        if [ "\$part_id" = "4129" ]; then
                \$rdma_rename \$old_device NAME_FIXED mlx5_rdma\${rdma_index}
                rdma_index=\$((\$rdma_index + 1))

        elif [ "\$part_id" = "4125" ]; then
                \$rdma_rename \$old_device NAME_FIXED mlx5_eth\${eth_index}
                eth_index=\$((\$eth_index + 1))

        else
                echo "Unknown device type for \$old_device - \$device_type."
        fi

done
EOF
chmod 755 $SBIN_SCRIPT_NAME

cat <<EOF > $SERVICE_PATH/$SERVICE_NAME
[Unit]
Description=OCI Network Interface naming
After=network.target

[Service]
Type=oneshot
ExecStart=$SBIN_SCRIPT_NAME
RemainAfterExit=true
StandardOutput=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl enable $SERVICE_NAME
systemctl start $SERVICE_NAME

$MONITOR_SCRIPT
