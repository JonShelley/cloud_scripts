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
data['uptime'] = date_str

# Get the rdma link information
cmd = "rdma link"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
if output.returncode != 0:
    print(f"Error getting rdma info")
    sys.exit(1)

# Define the pattern
pattern = r"(mlx5_\d+)/\d+ state (\w+) physical_state (\w+) netdev (\w+)"
    
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
cmd = "sudo dmesg -T| grep -E 'mlx5_'"
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
                        link_dict[mlx_interface] = {"last_flap_time": link_flap_time, "flap_count": 1}
                    else:
                        link_dict[mlx_interface]["flap_count"] += 1
                        link_dict[mlx_interface]["last_flap_time"] = link_flap_time
        

data['link_flaps'] = link_dict

# Get the system serial number
cmd = "sudo dmidecode -s system-serial-number"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
data['serial_number'] = output.stdout.strip()

# Get the mst status
mst_cmd = "sudo mst status -v"
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
    cmd = f"sudo mlxlink -m -e -c -d {mlx5_inter} --rx_fec_histogram --show_histogram --cable --dump --json"
    
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print(f"Error getting mlxlink info")
    data[key] = json.loads(output.stdout)

# write data to a json file

with open(f'mlxlink_info_min_{data["hostname"]}.json', 'w') as f:
    json.dump(data, f, indent=4)