#!/usr/bin/env python3

import json
import sys
import subprocess
import socket
import re
import argparse
from datetime import datetime
import urllib.request
import urllib.error

flap_duration_threshold = 86400
flap_startup_wait_time = 1800

parser = argparse.ArgumentParser(description="Collect mlxlink and NIC stats")
parser.add_argument(
    "--IB",
    action="store_true",
    help="Parse `rdma link` with IB-capable pattern; if no netdev, use lid_* as key",
)
parser.add_argument(
    "--debug",
    action="store_true",
    help="Enable debug output for mst parsing and rdma link parsing",
)
args = parser.parse_args()

data = {}

def dprint(msg: str):
    if args.debug:
        print(msg)

# Hostname
hostname = socket.gethostname()
data["hostname"] = hostname

# Uptime
cmd = "uptime -s"
output = subprocess.run(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
)
date_str = output.stdout.strip()
uptime_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
data["uptime"] = date_str
dprint(f"[DEBUG] Uptime: {date_str}")

# Collect IPv4/IPv6 for eth0 (Python equivalent of provided bash)
ipv4_addr = None
ipv6_addr = None
try:
    ip_cmd = "ip addr show dev eth0"
    dprint(f"[DEBUG] Running: {ip_cmd}")
    ip_out = subprocess.run(
        ip_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    for line in ip_out.stdout.splitlines():
        if ipv4_addr is None and "inet " in line:
            m = re.search(r"inet\s+([0-9.]+)/", line)
            if m:
                ipv4_addr = m.group(1)
        if ipv6_addr is None and "inet6" in line:
            m6 = re.search(r"inet6\s+([0-9A-Fa-f:]+)/", line)
            if m6:
                ipv6_addr = m6.group(1)
        if ipv4_addr is not None and ipv6_addr is not None:
            break
except Exception as e:
    dprint(f"[DEBUG] Error collecting IP addresses: {e}")

data["ipv4"] = ipv4_addr
data["ipv6"] = ipv6_addr
dprint(f"[DEBUG] IPv4: {ipv4_addr}, IPv6: {ipv6_addr}")

# Fetch OCI instance metadata and extract instance id
instance_id = None
try:
    req = urllib.request.Request(
        "http://169.254.169.254/opc/v2/instance",
        headers={"Authorization": "Bearer Oracle"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=2) as resp:
        body = resp.read().decode("utf-8")
        meta = json.loads(body)
        instance_id = meta.get("id")
except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, Exception) as e:
    dprint(f"[DEBUG] Error fetching instance metadata: {e}")

data["instance_id"] = instance_id
dprint(f"[DEBUG] Instance ID: {instance_id}")

# RDMA link info
cmd = "rdma link"
dprint(f"[DEBUG] Running: {cmd}")
output = subprocess.run(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
)
if output.returncode != 0:
    print("Error getting rdma info")
    sys.exit(1)

dprint("[DEBUG] rdma link output:")
if args.debug:
    print(output.stdout)

# Regex selection for rdma link
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
        re.X,
    )
    dprint("[DEBUG] Using IB rdma_link pattern")
else:
    rdma_link_pat = re.compile(
        r"(mlx5_\d+)/\d+:? state (\w+) physical_state (\w+) netdev (\w+)"
    )
    dprint("[DEBUG] Using non-IB rdma_link pattern")

rdma_dict = {}
for line in output.stdout.splitlines():
    if args.debug:
        print(f"[DEBUG] rdma line: {line!r}")
    m = rdma_link_pat.search(line)
    if not m:
        continue

    if args.IB:
        iface = m.group("iface")
        iface = iface.split("/")[0]
        netdev = m.group("netdev")
        lid = m.group("lid")
        key = netdev if netdev else (f"lid_{lid}" if lid else None)
        dprint(f"[DEBUG] rdma match IB: iface={iface}, netdev={netdev}, lid={lid}, key={key}")
        if key:
            rdma_dict[key] = iface
    else:
        iface = m.group(1)
        netdev = m.group(4)
        dprint(f"[DEBUG] rdma match non-IB: iface={iface}, netdev={netdev}")
        rdma_dict[netdev] = iface

print("=== RDMA LINK MAP ===")
for k, v in rdma_dict.items():
    print(f"{k} -> {v}")
data["rdma_link"] = rdma_dict

# Detect link flaps from dmesg
link_dict = {}
cmd = "dmesg -T | grep -E 'mlx5_'"
dprint(f"[DEBUG] Running: {cmd}")
output = subprocess.run(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
)
for line in output.stdout.splitlines():
    if "mlx5_" in line and "link down" in line.lower():
        dprint(f"[DEBUG] dmesg link-down line: {line!r}")
        dmesg_pat = re.compile(
            r"\[(\w{3} \w{3}\s+\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\].*?\b(\S+): Link (\w+)"
        )
        m = dmesg_pat.search(line)
        if not m:
            dprint("[DEBUG] dmesg pattern did not match this line")
            continue

        link_flap_time = datetime.strptime(m.group(1), "%a %b %d %H:%M:%S %Y")
        netdev_name = m.group(2)
        link_status = m.group(3)
        dprint(
            f"[DEBUG] Parsed dmesg: time={link_flap_time}, netdev={netdev_name}, status={link_status}"
        )

        if netdev_name not in rdma_dict:
            dprint(f"[DEBUG] netdev {netdev_name} not in rdma_dict, skipping")
            continue

        mlx_interface = rdma_dict[netdev_name]

        if (datetime.now() - link_flap_time).total_seconds() < flap_duration_threshold:
            if (link_flap_time - uptime_date).total_seconds() > flap_startup_wait_time:
                if mlx_interface not in link_dict:
                    link_dict[mlx_interface] = {
                        "last_flap_time": link_flap_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "flap_count": 1,
                    }
                else:
                    link_dict[mlx_interface]["flap_count"] += 1
                    link_dict[mlx_interface]["last_flap_time"] = link_flap_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

data["link_flaps"] = link_dict

# System serial number
cmd = "dmidecode -s system-serial-number"
dprint(f"[DEBUG] Running: {cmd}")
output = subprocess.run(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
)
data["serial_number"] = output.stdout.strip()
dprint(f"[DEBUG] Serial number: {data['serial_number']!r}")

# MST status
mst_cmd = "mst status -v"
dprint(f"[DEBUG] Running: {mst_cmd}")
mst_output = subprocess.run(
    mst_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
)

if args.debug:
    print("=== RAW MST STATUS -V OUTPUT ===")
    print(mst_output.stdout)

mst_dict = {}

# Robust line-based parsing:
#  - find mlx5_N on the line
#  - to the left of it, find nearest PCI-like token:
#      0c:00.0 or 0000:03:00.0
pci_pattern = re.compile(r"(?:[0-9A-Fa-f]{4}:)?[0-9A-Fa-f]{2}:[0-9A-Fa-f]{2}\.[0-7]")
rdma_pattern = re.compile(r"mlx5_\d+")

for line in mst_output.stdout.splitlines():
    if args.debug:
        print(f"[DEBUG] MST line: {line!r}")

    if "mlx5_" not in line:
        dprint("[DEBUG]   -> no mlx5_ in line, skip")
        continue

    rdma_match = rdma_pattern.search(line)
    if not rdma_match:
        dprint("[DEBUG]   -> rdma_pattern did not match, skip")
        continue
    rdma = rdma_match.group(0)
    dprint(f"[DEBUG]   -> rdma_match: {rdma!r} at [{rdma_match.start()}:{rdma_match.end()}]")

    left_text = line[:rdma_match.start()]
    pci_match = None
    for m in pci_pattern.finditer(left_text):
        pci_match = m
    if not pci_match:
        dprint("[DEBUG]   -> no PCI match to the left of rdma, skip")
        continue

    pci = pci_match.group(0)
    dprint(f"[DEBUG]   -> pci_match: {pci!r} at [{pci_match.start()}:{pci_match.end()}]")

    mst_dict[pci] = rdma

print("=== MST STATUS MAP ===")
for k, v in mst_dict.items():
    print(f"{k} -> {v}")
data["mst_status"] = mst_dict

# mlxlink info
for key in mst_dict:
    print(f"Collecting mlxlink info for {key}: {mst_dict[key]}")
    mlx5_inter = mst_dict[key]
    cmd = f"mlxlink -d {mlx5_inter} -m -e -c --rx_fec_histogram --show_histogram --json"
    dprint(f"[DEBUG] Running: {cmd}")
    output = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print("Error getting mlxlink info")
    try:
        data[key] = json.loads(output.stdout)
    except json.JSONDecodeError as e:
        print(f"Error decoding json: {e}")
        print(f"Output: {output.stdout}")

# ETHTOOL stats: skip IB lid_* entries
print("=== ETHTOOL COLLECTION ===")
for netdev in rdma_dict:
    print(f"Key: {netdev}, Value: {rdma_dict[netdev]}")
    if netdev.startswith("lid_"):
        print(f"Skipping ethtool for {netdev} (IB LID entry)")
        continue
    cmd = f"ethtool -S {netdev}"
    dprint(f"[DEBUG] Running: {cmd}")
    output = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if output.returncode != 0:
        print(f"cmd: {cmd}, returncode: {output.returncode}")
        print("Error getting ethtool info")
        continue
    ethtool_dict = {}
    for line in output.stdout.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            if not re.match(r"^(tx[0-9]+_|rx[0-9]+_|ch[0-9]+_)", k.strip()):
                ethtool_dict[k.strip()] = v.strip()
    data[netdev] = ethtool_dict

# Output JSON
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
outfile = f"mlxlink_info_min_{data['hostname']}_{current_time}.json"
with open(outfile, "w") as f:
    json.dump(data, f, indent=4)
print(f"Saved: {outfile}")
