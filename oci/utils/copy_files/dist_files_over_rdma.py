#!/usr/bin/env python3
import os
import json
import argparse
import subprocess
import tempfile
import socket
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

def rsync_chunk(source_dir, rel_paths, remote_user, target_ip, local_ip, dest_dir):
    """
    Rsync a list of relative paths to a single remote IP,
    binding the ssh socket to local_ip.
    """
    # write the list of relative paths to a temp file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tf:
        for p in rel_paths:
            tf.write(p + '\n')
        listfile = tf.name

    ssh_cmd = f"ssh -b {local_ip}"
    cmd = [
        'rsync',
        '-av',
        '-e', ssh_cmd,
        '--files-from=' + listfile,
        source_dir.rstrip('/') + '/',
        f'{remote_user}@{target_ip}:{dest_dir.rstrip("/")}/'
    ]
    print(f"[{target_ip} ← {local_ip}] Starting rsync of {len(rel_paths)} files…")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        print(f"[{target_ip}] ERROR: {proc.stderr.strip()}")
    else:
        print(f"[{target_ip}] Done.")
    os.unlink(listfile)

def split_into_chunks(all_paths, n_chunks):
    """
    Distribute file paths into n nearly‐equal chunks.
    """
    chunks = [[] for _ in range(n_chunks)]
    for idx, p in enumerate(all_paths):
        chunks[idx % n_chunks].append(p)
    return chunks

def main():
    parser = argparse.ArgumentParser(
        description="Parallel directory copy over multi‐rail RDMA with explicit binding."
    )
    parser.add_argument('-c', '--config', required=True,
                        help="JSON file mapping each node to its list of RDMA IPs.")
    parser.add_argument('-s', '--source-dir', required=True,
                        help="Local directory to copy.")
    parser.add_argument('-d', '--dest-dir', required=True,
                        help="Destination directory on remote nodes.")
    parser.add_argument('-u', '--remote-user', default=os.getlogin(),
                        help="SSH user.")
    parser.add_argument('-w', '--max-workers', type=int, default=None,
                        help="Max parallel rsync tasks.")
    args = parser.parse_args()

    # load config
    with open(args.config) as f:
        cfg = json.load(f)
    nodes = cfg.get('nodes', [])
    if not nodes:
        print("ERROR: no nodes in config", file=sys.stderr)
        sys.exit(1)

    # figure out which host this is, so we can get its local IP list
    local_hostnames = {socket.gethostname(), socket.getfqdn()}
    local_entry = next((n for n in nodes
                        if n['hostname'] in local_hostnames), None)
    if not local_entry:
        print("ERROR: this host not found in config; cannot bind locally", file=sys.stderr)
        sys.exit(1)
    local_ips = local_entry.get('ips', [])
    if not local_ips:
        print("ERROR: no local IPs listed for this host in config", file=sys.stderr)
        sys.exit(1)

    # collect files
    all_files = []
    for root, _, files in os.walk(args.source_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, args.source_dir)
            all_files.append(rel)
    if not all_files:
        print("ERROR: no files under", args.source_dir, file=sys.stderr)
        sys.exit(1)

    # build task list
    tasks = []
    for node in nodes:
        hostname = node['hostname']
        # skip sending to self
        if hostname in local_hostnames:
            continue

        remote_ips = node.get('ips', [])
        if not remote_ips:
            continue

        chunks = split_into_chunks(all_files, len(remote_ips))
        for idx, (rip, chunk) in enumerate(zip(remote_ips, chunks)):
            if not chunk:
                continue
            # pick the corresponding local IP
            if idx < len(local_ips):
                lip = local_ips[idx]
            else:
                # fallback if counts mismatch
                lip = local_ips[0]
                print(f"WARNING: fewer local IPs than remote; binding all extras to {lip}",
                      file=sys.stderr)

            tasks.append((rip, lip, chunk))

    if not tasks:
        print("No work to do; all hosts skipped.", file=sys.stderr)
        sys.exit(0)

    max_workers = args.max_workers or len(tasks)
    print(f"Spawning up to {max_workers} rsync jobs across {len(tasks)} streams…")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for rip, lip, chunk in tasks:
            futures.append(pool.submit(
                rsync_chunk,
                args.source_dir,
                chunk,
                args.remote_user,
                rip,
                lip,
                args.dest_dir
            ))
        for f in as_completed(futures):
            f.result()

if __name__ == '__main__':
    main()