#!/usr/bin/env python3
import subprocess
import argparse
import sys
import json
import re
import os

def get_rdma_ips(node, user=None, ssh_key=None):
    """
    SSH to `node` and return a list of IPv4 addresses (no CIDR) for all rdma[0-9]* interfaces.
    """
    ssh_target = f"{user+'@' if user else ''}{node}"
    ssh_cmd = ["ssh", "-o", "BatchMode=yes"]
    if ssh_key:
        ssh_cmd += ["-i", ssh_key]
    ssh_cmd.append(ssh_target)

    # ask remote for rdma* IPv4 addrs
    remote = r"""ip -o -4 addr show | awk '/ rdma[0-9]+ / {print $4}'"""
    ssh_cmd.append(remote)

    proc = subprocess.run(ssh_cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          text=True)
    if proc.returncode != 0:
        print(f"WARNING: {node}: SSH error: {proc.stderr.strip()}", file=sys.stderr)
        return []

    # strip off any /mask
    addrs = [line.split('/')[0] for line in proc.stdout.splitlines() if line.strip()]
    return addrs

def parse_pattern(pattern):
    """
    Parse a pattern like prefix-[n1,n2,...] or prefix-[start-end]
    into ['prefix-n1', 'prefix-n2', ...] or ['prefix-start', ..., 'prefix-end'].
    """
    m = re.fullmatch(r'(.+)-\[(.+)\]', pattern.strip())
    if not m:
        raise ValueError(f"Invalid pattern format: {pattern!r}. "
                         "Expected something like gpu-[1,2,3] or gpu-[5-7]")
    prefix, body = m.group(1), m.group(2)
    parts = []
    for token in body.split(','):
        token = token.strip()
        if not token:
            continue
        # handle numeric range "start-end"
        rng = re.fullmatch(r'(\d+)\s*-\s*(\d+)', token)
        if rng:
            start, end = map(int, rng.groups())
            if start > end:
                raise ValueError(f"Invalid range {token!r}: start > end")
            parts.extend(str(i) for i in range(start, end + 1))
        else:
            parts.append(token)
    return [f"{prefix}-{p}" for p in parts]



def main():
    parser = argparse.ArgumentParser(
        description="Produce JSON of rdma* interface IPs for each node."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-n", "--nodes-file",
        help="Path to a file with one hostname/IP per line (comments with # OK)."
    )
    group.add_argument(
        "-p", "--pattern",
        help="Hostname pattern, e.g. gpu-[153,335,383]"
    )
    parser.add_argument(
        "-u", "--user",
        help="SSH user (defaults to current user)."
    )
    parser.add_argument(
        "-i", "--ssh-key",
        help="SSH private key (if needed)."
    )
    args = parser.parse_args()

    # build list of nodes from file or pattern
    if args.nodes_file:
        try:
            with open(args.nodes_file) as f:
                nodes = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        except IOError as e:
            print(f"ERROR: cannot read nodes file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        try:
            nodes = parse_pattern(args.pattern)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)

    if not nodes:
        print("ERROR: No nodes to process.", file=sys.stderr)
        sys.exit(1)

    output = {"nodes": []}
    for node in nodes:
        ips = get_rdma_ips(node, user=args.user, ssh_key=args.ssh_key)
        output["nodes"].append({
            "hostname": node,
            "ips": ips
        })

    # emit JSON on stdout
    json.dump(output, sys.stdout, indent=2)
    sys.stdout.write("\n")

if __name__ == "__main__":
    main()