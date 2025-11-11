#!/usr/bin/env python3

import json
import sys
import subprocess
import socket
import re
import argparse
from datetime import datetime

flap_duration_threshold = 86400
flap_startup_wait_time = 1800

parser = argparse.ArgumentParser(description="Collect mlxlink and NIC stats")
parser.add_argument("--IB", action="store_true",
                    help="Parse `rdma link` with IB-capable pattern; if no netdev, use lid_* as key")
args = parser.parse_args()

data = {}

# Hostname
hostname = socket.gethostname()
data['hostname'] = hostname

# Uptime
cmd = "uptime -s"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
date_str = output.stdout.strip()
uptime_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
data['uptime'] = date_str

# RDMA link info
cmd = "rdma link"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
if output.returncode != 0:
    print("Error getting rdma info")
    sys.exit(1)

# Regex selection
if args.IB:
    rdma_link_pat = re.compile(
        r"""^link\s+
            (?P<iface>mlx5_\d+/\d+)
            (?:\s+subnet_prefix\s+(?P<subnet_prefix>[0-9A-Fa-f:]+)
                \s+lid\s+(?P<lid>\d+)
                \s+sm_lid\s+(?P<sm_lid>\d+)
                \s+lmc\s+(?P<lmc>\d+)
            )?
            \s+state\s+(?P<state>\w+)
            \s+physical_state\s+(?P<physical_state>\w+)
            (?:\s+netdev\s+(?P<netdev>\S+))?
            \s*$""",
        re.X
    )
else:
    rdma_link_pat = re.compile(r"(mlx5_\d+)/\d+:? state (\w+) physical_state (\w+) netdev (\w+)")

rdma_dict = {}
for line in output.stdout.splitlines():
    m = rdma_link_pat.search(line)
    if not m:
        continue

    if args.IB:
        iface = m.group("iface")
        netdev = m.group("netdev")
        lid = m.group("lid")
        # Use netdev if present, else lid_* as key
        key = netdev if netdev else (f"lid_{lid}" if lid else None)
        if key:
            rdma_dict[key] = iface
    else:
        rdma_dict[m.group(4)] = m.group(1)

print("=== RDMA LINK MAP ===")
for k, v in rdma_dict.items():
    print(f"{k} -> {v}")
data['rdma_link'] = rdma_dict

# Detect link flaps from dmesg
link_dict = {}
cmd = "dmesg -T | grep -E 'mlx5_'"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
for line in output.stdout.splitlines():
    if "mlx5_" in line and "link down" in line.lower():
        dmesg_pat = re.compile(
            r"\[(\w{3} \w{3}\s+\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\].*?\b(\S+): Link (\w+)"
        )
        m = dmesg_pat.search(line)
        if not m:
            continue

        link_flap_time = datetime.strptime(m.group(1), "%a %b %d %H:%M:%S %Y")
        netdev_name = m.group(2)

        # Only map when we have a netdev-based key
        if netdev_name not in rdma_dict:
            continue

        mlx_interface = rdma_dict[netdev_name]

        if (datetime.now() - link_flap_time).total_seconds() < flap_duration_threshold:
            if (link_flap_time - uptime_date).total_seconds() > flap_startup_wait_time:
                if mlx_interface not in link_dict:
                    link_dict[mlx_interface] = {
                        "last_flap_time": link_flap_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "flap_count": 1
                    }
                else:
                    link_dict[mlx_interface]["flap_count"] += 1
                    link_dict[mlx_interface]["last_flap_time"] = link_flap_time.strftime("%Y-%m-%d %H:%M:%S")

data['link_flaps'] = link_dict

# System serial number
cmd = "dmidecode -s system-serial-number"
output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
data['serial_number'] = output.stdout.strip()

# MST status
mst_cmd = "mst status -v"
mst_output = subprocess.run(mst_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
pattern = r".*\s+.*\s+(\S+)\s+(\S+)\s+\S+\s+\d+"
mst_dict = {}
for line in mst_output.stdout.splitlines():
    m = re.search(pattern, line)
    if m:
        mst_dict[m.group(1)] = m.group(2)
print("=== MST STATUS MAP ===")
for k, v in mst_dict.items():
    print(f"{k} -> {v}")
data['mst_status'] = mst_dict

# mlxlink info
for key in mst_dict:
    print(f"Collecting mlxlink info for {key}: {mst_dict[key]}")
    mlx5_inter = mst_dict[key]
    cmd = f"mlxlink -d {mlx5_inter} -m -e -c --rx_fec_histogram --show_histogram --json"
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print("Error getting mlxlink info")
    try:
        data[key] = json.loads(output.stdout)
    except json.JSONDecodeError as e:
        print(f"Error decoding json: {e}")
        print(f"Output: {output.stdout}")

# ethtool stats: skip IB lid_* entries
print("=== ETHTOOL COLLECTION ===")
for netdev in rdma_dict:
    print(f"Key: {netdev}, Value: {rdma_dict[netdev]}")
    if netdev.startswith("lid_"):
        print(f"Skipping ethtool for {netdev} (IB LID entry)")
        continue
    cmd = f"ethtool -S {netdev}"
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print("Error getting ethtool info")
        continue
    ethtool_dict = {}
    for line in output.stdout.splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            if not re.match(r'^(tx[0-9]+_|rx[0-9]+_|ch[0-9]+_)', k.strip()):
                ethtool_dict[k.strip()] = v.strip()
    data[netdev] = ethtool_dict

# Output JSON
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
outfile = f"mlxlink_info_min_{data['hostname']}_{current_time}.json"
with open(outfile, 'w') as f:
    json.dump(data, f, indent=4)
print(f"Saved: {outfile}")

