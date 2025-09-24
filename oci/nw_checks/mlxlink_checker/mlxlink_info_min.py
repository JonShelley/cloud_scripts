#!/usr/bin/env python3

import json
import os
import sys
import subprocess
import socket
import re
from datetime import datetime

flap_duration_threshold = 86400
flap_startup_wait_time = 1800

data = dict()

# Get the hostname and add it to the data
hostname = socket.gethostname()
data['hostname'] = hostname

# Get the system uptime
cmd = "uptime -s"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
date_str = output.stdout.strip()
uptime_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
data['uptime'] = date_str

# Get the rdma link information
cmd = "rdma link"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
if output.returncode != 0:
    print(f"Error getting rdma info")
    sys.exit(1)

# Define the pattern
pattern = r"(mlx5_\d+)/\d+:? state (\w+) physical_state (\w+) netdev (\w+)"

rdma_dict = {}
for line in output.stdout.split('\n'):
    match = re.search(pattern, line)
    if match:
        print(f"Match: {match.group(1)}")
        rdma_dict[match.group(4)] = match.group(1)
print(rdma_dict)
data['rdma_link'] = rdma_dict

# Get the mlx5 link information
link_dict = {}
cmd = "dmesg -T| grep -E 'mlx5_'"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
for line in output.stdout.split('\n'):
    if "mlx5_" in line and "link down" in line.lower():
        print(f"Link down event: {line}")

        # Define the pattern (Change eth to rdma if your interfaces are named rdma when you run rdma link)
        pattern = r"\[(\w{3} \w{3} {1,2}\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\].*(eth\d+): Link (\w+)"

        # Search for the date, rdma interface, and link status
        match = re.search(pattern, line)

        #print(f"Match: {match}")
        # If a match was found, print it
        if match:
            link_flap_time = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Y")
            mlx_interface = match.group(2)
            link_status = match.group(3)
            mlx_interface = rdma_dict[mlx_interface]
            #print(f"Date and Time: {link_flap_time}, Interface: {mlx_interface}, Link Status: {link_status}")

            # Check to see if the link flap time is within the last x hours
            #print(f"Link flap time: {link_flap_time}, Uptime: {uptime_date}, Diff: {(link_flap_time - uptime_date).total_seconds()}, Duration: {flap_duration_threshold}")
            if (datetime.now() - link_flap_time).total_seconds() < flap_duration_threshold:
                # Check to see if the link_flap_time > than system uptime + 30 minutes
                if (link_flap_time - uptime_date).total_seconds() > flap_startup_wait_time:
                    print(f"Link flap detected within the last hour: {link_flap_time}")
                    if mlx_interface not in link_dict:
                        link_dict[mlx_interface] = {"last_flap_time": link_flap_time.strftime("%Y-%m-%d %H:%M:%S"), "flap_count": 1}
                    else:
                        link_dict[mlx_interface]["flap_count"] += 1
                        link_dict[mlx_interface]["last_flap_time"] = link_flap_time.strftime("%Y-%m-%d %H:%M:%S")


data['link_flaps'] = link_dict

# Get the system serial number
cmd = "dmidecode -s system-serial-number"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
data['serial_number'] = output.stdout.strip()

# Get the mst status
mst_cmd = "mst status -v"
mst_output = subprocess.run(mst_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

# Parse in the mst status and store the PCI and RDMA information
pattern = r".*\s+.*\s+(\S+)\s+(\S+)\s+\S+\s+\d+"
mst_dict = {}
for line in mst_output.stdout.split('\n'):
    match = re.search(pattern, line)
    if match:
        mst_dict[match.group(1)] = match.group(2)
print(mst_dict)

data['mst_status'] = mst_dict

# Get the mlx5 link information for each mlx5 interface
for key in mst_dict:
    print(f"Key: {key}, Value: {mst_dict[key]}")
    mlx5_inter = mst_dict[key]
    cmd = f"mlxlink -d {mlx5_inter} -m -e -c --rx_fec_histogram --show_histogram --json"

    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print(f"Error getting mlxlink info")
    try:
        data[key] = json.loads(output.stdout)
    except json.JSONDecodeError as e:
        print(f"Error decoding json: {e}")
        print(f"Output: {output.stdout}")

# Get the ethtool -S information for each rdma interface
for key in rdma_dict:
    print(f"Key: {key}, Value: {rdma_dict[key]}")
    rdma_inter = key
    cmd = f"ethtool -S {rdma_inter}"

    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print(f"Error getting ethtool info")
    ethtool_dict = {}
    for line in output.stdout.split('\n'):
        if ':' in line:
            parts = line.split(':')
            # Exclude keys that contain tx[0-9]+_ or rx[0-9]+_ or ch[0-9]+_ prefixes or the value equals to zero
            if not re.match(r'^(tx[0-9]+_|rx[0-9]+_|ch[0-9]+_)', parts[0].strip()):
                ethtool_dict[parts[0].strip()] = parts[1].strip()


    data[rdma_inter] = ethtool_dict

# define a variable with the current date and time
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# write data to a json file
with open(f'mlxlink_info_min_{data["hostname"]}_{current_time}.json', 'w') as f:
    json.dump(data, f, indent=4)
