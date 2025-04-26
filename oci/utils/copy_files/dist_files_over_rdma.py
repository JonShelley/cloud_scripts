#!/usr/bin/env python3
import os
import json
import argparse
import subprocess
import tempfile
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

def rsync_chunk(source_dir, rel_paths, remote_user, target_ip, dest_dir):
    """
    Rsync a list of relative paths to a single remote IP.
    """
    # write the list of relative paths to a temp file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tf:
        for p in rel_paths:
            tf.write(p + '\n')
        listfile = tf.name

    cmd = [
        'rsync',
        '-av',  # archive + verbose
        '--files-from=' + listfile,
        source_dir.rstrip('/') + '/',  # ensure trailing slash
        f'{remote_user}@{target_ip}:{dest_dir.rstrip("/")}/'
    ]
    print(f"[{target_ip}] Starting rsync of {len(rel_paths)} files…")
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
        description="Parallel directory copy to multi‐NIC nodes using rsync (skips local host)."
    )
    parser.add_argument(
        '--config', '-c', required=True,
        help="JSON file mapping nodes to their 8 IPs."
    )
    parser.add_argument(
        '--source-dir', '-s', required=True,
        help="Local directory to copy (will be recursed)."
    )
    parser.add_argument(
        '--dest-dir', '-d', required=True,
        help="Destination directory path on each remote node."
    )
    parser.add_argument(
        '--remote-user', '-u', default=os.getlogin(),
        help="Username for SSH (must have key‐based auth set up)."
    )
    parser.add_argument(
        '--max-workers', '-w', type=int, default=None,
        help="Max parallel rsync tasks (default: total chunks across all nodes)."
    )
    args = parser.parse_args()

    # determine local hostnames to skip
    local_hostnames = {socket.gethostname(), socket.getfqdn()}

    # load the node→IP mapping
    with open(args.config) as f:
        cfg = json.load(f)
    nodes = cfg.get('nodes', [])
    if not nodes:
        print("No nodes found in config file.")
        return

    # gather all files under source_dir, storing relative paths
    all_files = []
    for root, _, files in os.walk(args.source_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, args.source_dir)
            all_files.append(rel)
    if not all_files:
        print("No files found under", args.source_dir)
        return

    # prepare tasks, skipping any node matching local hostname/FQDN
    tasks = []
    for node in nodes:
        hostname = node['hostname']
        if hostname in local_hostnames:
            print(f"Skipping local host '{hostname}'.")
            continue

        ips = node.get('ips', [])
        if not ips:
            continue

        # split the file list into as many chunks as there are IPs
        chunks = split_into_chunks(all_files, len(ips))
        for ip, chunk in zip(ips, chunks):
            if chunk:
                tasks.append((hostname, ip, chunk))

    if not tasks:
        print("No remote tasks to run. Exiting.")
        return

    max_workers = args.max_workers or len(tasks)
    print(f"Launching up to {max_workers} parallel rsync jobs "
          f"across {len(nodes)} nodes ({len(tasks)} total chunks).")

    # run them in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = []
        for hostname, ip, chunk in tasks:
            futures.append(
                exe.submit(
                    rsync_chunk,
                    args.source_dir,
                    chunk,
                    args.remote_user,
                    ip,
                    args.dest_dir
                )
            )
        for f in as_completed(futures):
            f.result()  # propagate any exceptions

if __name__ == '__main__':
    main()