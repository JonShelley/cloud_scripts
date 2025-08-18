#!/usr/bin/env python3

# Note: sudo pip3 install pandas numpy natsort tabulate

import subprocess
from subprocess import TimeoutExpired
import time
import pandas as pd
import numpy as np
import warnings
import math
import argparse
import concurrent.futures
from tabulate import tabulate
from natsort import index_natsorted
import logging
import sys
from datetime import datetime
import os
from collections import Counter

warnings.simplefilter(action='ignore', category=FutureWarning)

wait = 1
pd.options.display.width = 0

message_columns_max = []
message_columns = []

# --------------------------- Utility helpers ---------------------------

def convert_size(size_bytes):
    """Convert bytes to human-readable format, snapped to common sizes."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 0)
    acceptable_values = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    s = min(acceptable_values, key=lambda x: abs(x - s))
    if s == 1024:
        i += 1
        s = 1
    return "%s %s" % (int(s), size_name[i])

def get_nccl_run_type(args):
    test = args.nccl_test
    if "alltoallv" in test:
        return "A2AV"
    if "alltoall" in test:
        return "A2A"
    if "all_reduce" in test:
        return "AR"
    if "all_gather" in test:
        return "AG"
    if "broadcast" in test:
        return "BC"
    if "gather" in test:
        return "GA"
    if "reduce_scatter" in test:
        return "RS"
    if "reduce" in test:
        return "RE"
    if "scatter" in test:
        return "SC"
    if "sendrecv" in test:
        return "SR"
    if "hypercube" in test:
        return "HC"
    return "Unknown"

def parse_nccl_output(output):
    """Parse nccl-tests text output into msg_size/results/time arrays and extend global msg_size catalog."""
    lines = output.split('\n')
    tmp_data = {'msg_size': [], 'results': [], 'time': []}
    for line in lines:
        line = line.strip()
        columns = line.split()
        # Per-message results (nccl-tests format)
        if len(columns) >= 3 and columns[2] == 'float':
            msg_size = convert_size(int(columns[0]))
            time_stamp = columns[9]
            result = columns[-2]
            tmp_data['msg_size'].append(msg_size)
            tmp_data['results'].append(float(result))
            tmp_data['time'].append(time_stamp)
        # Avg bus BW line
        if len(columns) >= 3 and columns[1] == 'Avg':
            msg_size = "Avg BW"
            result = columns[5]
            tmp_data['msg_size'].append(msg_size)
            tmp_data['results'].append(round(float(result), 2))

    logging.debug(f"Msg Size: {tmp_data['msg_size']}")
    logging.debug(f"Result: {tmp_data['results']}")
    logging.debug(f"Time: {tmp_data['time']}")
    message_columns = tmp_data['msg_size']

    # Extend the union set of seen msg sizes so later rows can be validated for missing data.
    global message_columns_max
    if not message_columns_max:
        message_columns_max = message_columns.copy()
    for msg_size in message_columns:
        if msg_size not in message_columns_max:
            message_columns_max.append(msg_size)

    return tmp_data

def load_hosts(hostfile):
    with open(hostfile, 'r') as f:
        hosts = {
            line.strip()
            for line in f.read().splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
    return sorted(hosts)

def _hosts_from_file(path):
    """Read hosts from a hostfile; ignore blanks/comments; return list of hostnames."""
    try:
        with open(path, 'r') as f:
            hosts = []
            for line in f:
                s = line.strip()
                if not s or s.startswith('#'):
                    continue
                hosts.append(s)
            return hosts
    except Exception as e:
        logging.warning(f"Could not read hostfile {path}: {e}")
        return []

def _write_pruned_hostfile(hosts, args):
    """Write current in-memory hosts list to a pruned hostfile path."""
    pruned_path = args.pruned_hostfile or f"{os.path.splitext(args.hostfile)[0]}_pruned.txt"
    with open(pruned_path, 'w') as f:
        for h in hosts:
            f.write(h + "\n")
    return pruned_path

def _slug(s):
    return "".join(c if (c.isalnum() or c in ('-', '_', '.')) else "_" for c in s)

def kill_on_host(host, nccl_test, ssh_port, timeout=10):
    cmd = ["ssh", "-p", str(ssh_port), host, "pkill", "-f", "--", nccl_test]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        return host, None
    except subprocess.CalledProcessError as e:
        return host, f"exit={e.returncode} stderr={e.stderr.decode(errors='ignore').strip()}"
    except subprocess.TimeoutExpired:
        return host, "timeout"
    except Exception as e:
        return host, f"error={e}"

def cleanup_orphans_parallel(hostfile, nccl_test, args):
    hosts = load_hosts(hostfile)
    if not hosts:
        logging.warning("No hosts found in hostfile.")
        return

    logging.debug(f"Cleaning up orphan processes for '{nccl_test}' on {len(hosts)} hosts (parallel={args.max_workers}).")

    futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        for h in hosts:
            logging.debug(f"Submitting cleanup task for {h}")
            futures[ex.submit(kill_on_host, h, nccl_test, args.ssh_port, args.timeout)] = h

        for fut in concurrent.futures.as_completed(futures):
            host = futures[fut]
            try:
                host, err = fut.result()
                if err is None:
                    logging.debug(f"[OK] {host}")
                else:
                    logging.debug(f"[FAIL] {host}: {err}")
            except Exception as e:
                logging.debug(f"[FAIL] {host}: unexpected error: {e}")

# --------------------------- NCCL runner ---------------------------

def run_mpi_command(args, dargs, hostfile, HPJ, date_stamp):
    logging.debug(f"Running on {HPJ} using mpirun for ({hostfile})")
    NP = int(HPJ) * 8
    logging.debug(f"HPJ: {HPJ}, Running on {NP} GPUs using mpirun for ({hostfile})")

    run_type = get_nccl_run_type(args)

    if args.guidance:
        proto = args.nccl_proto.split(',')
        algo = args.nccl_algo.split(',')
        logging.info(f"Guidance run: proto={proto}, algo={algo}")
    else:
        proto = [args.nccl_proto]
        algo = [args.nccl_algo]
        logging.info(f"Non-guidance run: proto={proto}, algo={algo}")

    all_rows_df = pd.DataFrame()

    # Noisy neighbors special case
    if args.noisy_neighbors and hostfile.find('noisy') != -1:
        nccl_iters = 2000
        nccl_test = "/workspace/nccl-tests/build/alltoall_perf"
    else:
        nccl_iters = args.nccl_iters
        nccl_test = args.nccl_test

    for a in algo:
        if a == "default":
            a = dargs.nccl_algo
            tmp_proto = [dargs.nccl_proto]
        else:
            tmp_proto = proto

        for p in tmp_proto:
            results_df = pd.DataFrame()
            mpirun_command = f"mpirun"
            if args.no_ucx == False:
                mpirun_command += f" -mca pml ucx"
            mpirun_command += f" -mca coll ^hcoll"
            mpirun_command += f" -mca plm_rsh_args '-p {args.ssh_port}'"
            mpirun_command += f" -mca btl_tcp_if_include eth0"
            mpirun_command += f" --allow-run-as-root"
            mpirun_command += f" -N 8"
            mpirun_command += f" -np {NP}"
            mpirun_command += f" -hostfile {hostfile}"
            mpirun_command += f" --bind-to numa"
            mpirun_command += f" -x RX_QUEUE_LEN=8192"
            mpirun_command += f" -x IB_RX_QUEUE_LEN=8192"
            mpirun_command += f" -x UCX_TLS=tcp"
            mpirun_command += f" -x UCX_NET_DEVICES=eth0"
            mpirun_command += f" -x HCOLL_ENABLE_MCAST_ALL=0"
            mpirun_command += f" -x coll_hcoll_enable=0"
            mpirun_command += f" -x NCCL_CUMEM_ENABLE=0"
            mpirun_command += f" -x NCCL_IB_TIMEOUT=22"
            mpirun_command += f" -x NCCL_IB_SL=0"
            mpirun_command += f" -x NCCL_IB_TC=41"
            mpirun_command += f" -x NCCL_IB_GID_INDEX=3"
            if args.nccl_debug:
                mpirun_command += f" -x NCCL_DEBUG=INFO"
            else:
                mpirun_command += f" -x NCCL_DEBUG=WARN"
            if args.nccl_qps_per_connection:
                mpirun_command += f" -x NCCL_IB_QPS_PER_CONNECTION={args.nccl_qps_per_connection}"
            else:
                mpirun_command += f" -x NCCL_IB_QPS_PER_CONNECTION=1"
            mpirun_command += f" -x NCCL_IB_SPLIT_DATA_ON_QPS=0"
            if args.nccl_nchannels:
                mpirun_command += f" -x NCCL_MIN_NCHANNELS={args.nccl_nchannels}"
                mpirun_command += f" -x NCCL_MAX_NCHANNELS={args.nccl_nchannels}"
            if args.nccl_proto != "default":
                mpirun_command += f" -x NCCL_PROTO={p}"
            if args.nccl_algo != "default":
                mpirun_command += f" -x NCCL_ALGO={a}"
            # HCA maps by node shape
            if args.node_shape == "h200":
                mpirun_command += f" -x NCCL_IB_HCA='=mlx5_0,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_9,mlx5_10,mlx5_11'"
            elif args.node_shape == "mi300x":
                mpirun_command += f" -x NCCL_IB_HCA='=mlx5_0,mlx5_2,mlx5_3,mlx5_4,mlx5_5,mlx5_7,mlx5_8,mlx5_9'"
                mpirun_command += f" -x NCCL_PXN_DISABLE=0"
            elif args.node_shape == "h100":
                mpirun_command += f" -x NCCL_IB_HCA='=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17'"
            else:
                logging.error(f"Unknown node shape: {args.node_shape}")
                sys.exit(1)
            mpirun_command += f" -x NCCL_NET_PLUGIN=none"
            mpirun_command += f" -x LD_LIBRARY_PATH"
            mpirun_command += f" {nccl_test} -b {args.begin_size} -e {args.end_size} -f 2 -g 1 -n {nccl_iters}"
            logging.info(mpirun_command)

            time_taken = 0.0
            try:
                stime = datetime.now()
                run_count = 0
                outfile_name = f'output_{hostfile[:-4]}_{run_type}_{a}_{p}_{date_stamp}_run_{run_count}.log'
                while outfile_name in os.listdir():
                    run_count += 1
                    outfile_name = f'output_{hostfile[:-4]}_{run_type}_{a}_{p}_{date_stamp}_run_{run_count}.log'

                if args.noisy_neighbors and hostfile.find('noisy') != -1:
                    with open("noisy_" + outfile_name, 'w') as f:
                        proc1 = subprocess.Popen(mpirun_command, shell=True, stderr=f, stdout=f, universal_newlines=True)
                    return proc1
                else:
                    with open(outfile_name, 'w') as f:
                        subprocess.run(mpirun_command, shell=True, stderr=f, stdout=f, universal_newlines=True, timeout=args.timeout)

                time_taken = (datetime.now() - stime).total_seconds()

                with open(outfile_name, 'r') as f:
                    output = f.read()

                tmp_data = parse_nccl_output(output)
                row_data = {'HostSet': [hostfile], "Nodes": [HPJ], "GPUs": [NP], 'algo': [a], 'proto': [p]}
                for msg_size, result in zip(tmp_data['msg_size'], tmp_data['results']):
                    row_data[str(msg_size)] = result
                for msg_size, mtime in zip(tmp_data['msg_size'], tmp_data['time']):
                    row_data[f"time_{msg_size}"] = mtime

            except TimeoutExpired:
                logging.info(f"Command timed out after {args.timeout // 60} minutes on {hostfile}.")
                row_data = {'HostSet': [hostfile], "Nodes": [HPJ], "GPUs": [NP], 'algo': [a], 'proto': [p], 'Status': 'Timeout'}
            except subprocess.CalledProcessError as e:
                logging.info(f"Error executing command for {hostfile} using mpirun: {e}")
                row_data = {'HostSet': [hostfile], "Nodes": [HPJ], "GPUs": [NP], 'algo': [a], 'proto': [p], 'Status': 'Failed - Command Error'}
            except Exception as e:
                logging.info(f"An unexpected error occurred for job set {hostfile} --- {e}")
                row_data = {'HostSet': [hostfile], "Nodes": [HPJ], "GPUs": [NP], 'algo': [a], 'proto': [p], 'Status': 'Failed - Unexpected Error'}

            time.sleep(wait)

            # Optional Avg BW threshold checks by node shape and run type
            if 'Avg BW' in row_data:
                avg_bw = {}
                if args.node_shape in ['h100', 'h200', 'b200']:
                    avg_bw = {
                        "A2A": {"1": 220.0, "2": 42.0, "4": 42, "8": 37.5, "16": 34, "32": 32, "64": 27, "96": 23, "128": 20, "256": 18, "512": 18},
                        "AR":  {"1": 295.0, "2": 252, "4": 175, "8": 175, "16": 165, "32": 165, "64": 160, "96": 150, "128": 120, "256": 105, "512": 100}
                    }
                if args.node_shape in ['mi300x']:
                    avg_bw = {
                        "A2A": {"1": 210.0, "2": 50.0, "4": 37, "8": 29, "16": 24, "32": 20, "64": 15, "96": 12, "128": 15, "256": 11, "512": 10},
                        "A2AV": {},
                        "AR":  {"1": 230.0, "2": 190, "4": 171, "8": 155, "16": 145, "32": 135, "64": 120, "96": 115, "128": 110, "256": 100, "512": 100}
                    }

                rtype = get_nccl_run_type(args)
                if rtype in avg_bw and str(HPJ) in avg_bw[rtype] and 'Avg BW' in row_data:
                    logging.info(f"Checking Avg BW for {rtype} with {HPJ} nodes")
                    try:
                        if float(row_data['Avg BW']) < avg_bw[rtype][str(HPJ)]:
                            logging.info(f"Avg BW {row_data['Avg BW']} below threshold {avg_bw[rtype][str(HPJ)]} for {rtype} with {HPJ} nodes")
                            row_data['Status'] = 'Failed - Below Avg BW'
                        else:
                            if 'Status' not in row_data:
                                row_data['Status'] = 'Success'
                    except Exception as e:
                        logging.debug(f"Avg BW check error: {e}")

            if 'Status' not in row_data:
                row_data['Status'] = 'Success'

            row_data['RunTime'] = time_taken
            results_df = pd.concat([results_df, pd.DataFrame(row_data)], ignore_index=True)
            all_rows_df = pd.concat([all_rows_df, results_df], ignore_index=True)

    # Normalize Status as string and mark missing-data runs
    if 'Status' in all_rows_df.columns:
        all_rows_df['Status'] = all_rows_df['Status'].astype(str)

    metric_cols = [c for c in message_columns_max if c in all_rows_df.columns]
    for idx, row in all_rows_df.iterrows():
        status = str(row['Status'])
        # If not already a "hard" fail or timeout or below-threshold, ensure data exists
        if status not in ('Failed - Command Error', 'Timeout', 'Failed - Below Avg BW'):
            if (not metric_cols) or row[metric_cols].isna().any():
                all_rows_df.at[idx, 'Status'] = 'Failed - Missing Data'
                logging.warning(f"Missing data in row {idx}. Setting Status to Failed - Missing Data")

    return all_rows_df

# --------------------------- Hostfile generation ---------------------------

def create_circular_pairs(lst):
    return [[lst[i], lst[(i + 1) % len(lst)]] for i in range(len(lst))]

def create_n_minus_1_host_list(lst):
    return [[x for x in lst if x != y] for y in lst]

def generate_host_files(args, hosts, hosts_per_job, bad_hosts):
    HPJ = int(hosts_per_job)
    if len(hosts) < HPJ:
        print("Not enough hosts to perform the command between {} nodes.".format(HPJ))
        return None

    total = math.floor((len(hosts) / HPJ))
    print(f"Total number of tests to run for hosts/job = {HPJ}: {total}")

    hostfiles = []
    if args.find_waldo:
        tmp_list = create_n_minus_1_host_list(hosts)
        for i in range(0, len(tmp_list)):
            file_num = '{0:03d}'.format(i)
            node_num = '{0:03d}'.format(HPJ)
            hosts_filename = f'hostfile-{node_num}n-{file_num}.txt'
            with open(hosts_filename, 'w') as f:
                for host in tmp_list[i]:
                    f.write(f'{host}\n')
            hostfiles.append(hosts_filename)
        hostfile_list = [hostfiles]
        logging.info(f"Hostfiles: {hostfiles}")

    elif HPJ == 2:
        host_pairs = create_circular_pairs(hosts)
        for i in range(0, len(host_pairs)):
            host1, host2 = host_pairs[i]
            idx1 = '{0:03d}'.format(hosts.index(host1))
            idx2 = '{0:03d}'.format(hosts.index(host2))
            node_num = '{0:03d}'.format(HPJ)
            hosts_filename = f'hostfile-{node_num}n-{idx1}-{idx2}.txt'
            with open(hosts_filename, 'w') as f:
                for h in (host1, host2):
                    f.write(f'{h}\n')
            hostfiles.append(hosts_filename)

        # Group into rounds to cap concurrency; this doesn't affect counting logic.
        hostfile_list = []
        if len(hostfiles) == 1:
            hostfile_list.append([hostfiles])
        elif len(hostfiles) % 2 == 0:
            even_index_list = hostfiles[0::2]
            odd_index_list  = hostfiles[1::2]
            hostfile_list.append(even_index_list)   # round 1
            hostfile_list.append(odd_index_list)    # round 2
        else:
            if len(hostfiles) == 3:
                hostfile_list.append([hostfiles[0]])
                hostfile_list.append([hostfiles[1]])
                hostfile_list.append([hostfiles[2]])
            else:
                even_index_list = hostfiles[0:-1:2]
                odd_index_list  = hostfiles[1:-1:2]
                hostfile_list.append(even_index_list)         # round 1
                hostfile_list.append(odd_index_list)          # round 2
                hostfile_list.append([hostfiles[-1]])         # round 3 (leftover)
    else:
        for i in range(0, len(hosts), HPJ):
            if (i + HPJ) <= len(hosts):
                job_hosts = hosts[i:i + HPJ]
                file_num = '{0:03d}'.format(i)
                node_num = '{0:03d}'.format(HPJ)
                hosts_filename = f'hostfile-{node_num}n-{file_num}.txt'
                with open(hosts_filename, 'w') as f:
                    for h in job_hosts:
                        f.write(f'{h}\n')
                hostfiles.append(hosts_filename)
            else:
                logging.info("Skipping remaining hosts as there are not enough hosts to run the test.")
                logging.info(f"Remaining hosts: {hosts[i:]}")
                break
        hostfile_list = [hostfiles]

    logging.debug(f"Hostfile List: {hostfile_list}")
    return hostfile_list

# --------------------------- Bad-host detection (HPJ=1/2) ---------------------------

def check_for_bad_hosts(HPJ, all_results_df_for_hpj, min_appearances=2):
    """
    Across ALL rounds for this HPJ:
      - Take rows whose Status contains 'Failed' or 'Timeout' (substring match).
      - Read hosts from each bad run's HostSet, and tally appearances.
      - Flag any host with count >= min_appearances.
    Works for HPJ=1 (single host per file) and HPJ=2 (two hosts per file).
    """
    bhosts = []
    if all_results_df_for_hpj is None or all_results_df_for_hpj.empty:
        return bhosts

    req = {'Status', 'HostSet', 'Nodes'}
    if not req.issubset(all_results_df_for_hpj.columns):
        logging.warning("Results missing Status/HostSet/Nodes; skipping bad-host detection.")
        return bhosts

    df = all_results_df_for_hpj.copy()
    df['Status'] = df['Status'].astype(str)

    # Only HPJ rows and only Failed/Timeout
    df = df[(df['Nodes'] == HPJ) &
            (df['Status'].str.contains('Failed', na=False) | df['Status'].str.contains('Timeout', na=False))]
    if df.empty:
        return bhosts

    counts = Counter()
    for hostfile in df['HostSet']:
        for h in _hosts_from_file(hostfile):
            counts[h] += 1

    bhosts = sorted([h for h, c in counts.items() if c >= min_appearances])

    # Diagnostics
    if counts:
        diag = ", ".join(f"{h}:{counts[h]}" for h in sorted(counts, key=counts.get, reverse=True))
        logging.info(f"HPJ={HPJ} bad-run host tallies: {diag}")
        if bhosts:
            logging.info(f"HPJ={HPJ} bad hosts (>= {min_appearances} appearances): {bhosts}")
        else:
            logging.info(f"HPJ={HPJ}: no host met the {min_appearances} appearances threshold.")

    return bhosts

# --------------------------- Triage for HPJ>2 ---------------------------

def _pick_good_hostfile_for_triage(args, hpj_results_df, HPJ):
    """Pick a 'good' hostfile from this HPJ batch. Prefer explicit arg; else first Success."""
    if args.triage_good_hostfile:
        return args.triage_good_hostfile
    cand = hpj_results_df[(hpj_results_df['Nodes'] == HPJ) &
                          (hpj_results_df['Status'].astype(str) == 'Success')]
    if cand.empty:
        return None
    # Just take the first successful HostSet
    return cand.iloc[0]['HostSet']

def _triage_failed_hostfile(args, dargs, HPJ, good_hostfile, failed_hostfile, date_stamp):
    """
    Combine (HPJ-1) hosts from good_hostfile with each candidate host from failed_hostfile.
    Run tests and mark any candidate host that causes a failure as underperforming.
    Returns (underperformers, triage_results_df).
    """
    good_all = _hosts_from_file(good_hostfile)
    bad_all  = _hosts_from_file(failed_hostfile)
    underperformers = []
    triage_results = pd.DataFrame()

    if len(good_all) < HPJ:
        logging.warning(f"Triage: good hostfile {good_hostfile} has fewer than HPJ={HPJ} hosts; skipping triage for {failed_hostfile}.")
        return underperformers, triage_results

    # Use a smaller iteration count for triage if provided
    orig_iters = args.nccl_iters
    if args.triage_iters:
        args.nccl_iters = args.triage_iters

    for cand in bad_all:
        # Build a base of (HPJ-1) good hosts excluding 'cand' if it overlaps with the good set
        base_good = [x for x in good_all if x != cand][:HPJ-1]
        if len(base_good) < HPJ - 1:
            # fill in from the rest (ensuring uniqueness)
            extras = [x for x in good_all if (x not in base_good and x != cand)]
            need = (HPJ - 1) - len(base_good)
            base_good.extend(extras[:need])

        if len(base_good) < HPJ - 1:
            logging.warning(f"Triage: cannot form base_good of size {HPJ-1} from {good_hostfile}; skipping cand {cand}")
            continue

        triage_name = f"triage-{HPJ:03d}n-{_slug(os.path.basename(good_hostfile)[:-4])}-plus-{_slug(cand)}.txt"
        try:
            with open(triage_name, 'w') as f:
                for h in base_good + [cand]:
                    f.write(h + "\n")
        except Exception as e:
            logging.warning(f"Triage: failed to write {triage_name}: {e}")
            continue

        logging.info(f"Triage run: {triage_name} (base_good from {good_hostfile} + cand {cand})")
        df = run_mpi_command(args, dargs, triage_name, HPJ, date_stamp)
        triage_results = pd.concat([triage_results, df], ignore_index=True)

        # Decide pass/fail: ALL rows should be 'Success' to pass
        statuses = df['Status'].astype(str)
        if not (len(statuses) > 0 and statuses.eq('Success').all()):
            logging.info(f"Triage: candidate host {cand} deemed UNDERPERFORMING based on {triage_name}")
            underperformers.append(cand)
        else:
            logging.info(f"Triage: candidate host {cand} passed with {triage_name}")

    # restore
    args.nccl_iters = orig_iters
    return sorted(set(underperformers)), triage_results

# --------------------------- Main batch executor ---------------------------

def execute_command_in_sets_of_hosts_with_mpirun(args, dargs, date_stamp):
    # Load initial host list
    with open(args.hostfile, 'r') as file:
        hosts = [h.strip() for h in file if h.strip() and not h.strip().startswith('#')]

    all_results_df = pd.DataFrame()
    bad_hosts_overall = []  # across all HPJ values

    for HPJ in args.hosts_per_job:
        HPJ = int(HPJ)

        # Generate hostfiles for this HPJ with the current (possibly pruned) host list
        hostfile_list = generate_host_files(args, hosts, HPJ, bad_hosts_overall)
        if hostfile_list is None:
            logging.error("No hostfiles generated. Exiting.")
            return None

        # If guidance, only run the first set of hostfiles
        if args.guidance:
            hostfile_list = [[hostfile_list[0][0]]]
            logging.debug(f"Guidance Hostfiles: {hostfile_list}")

        if args.runs_per_node_count > 1:
            tmp_hostfile_list = []
            for _ in range(args.runs_per_node_count):
                tmp_hostfile_list.append([hostfile_list[0][0]])
            hostfile_list = tmp_hostfile_list
            logging.debug(f"{args.runs_per_node_count} Runs per node count: {hostfile_list}")

#        # Collect results only for this HPJ
#        hpj_results_df = pd.DataFrame()
#
#        for hostfiles in hostfile_list:
#            logging.info(f"HPJ: {HPJ}, Hostfiles: {hostfiles}")
#            with concurrent.futures.ThreadPoolExecutor(args.max_workers) as executor:
#                future_to_task = {executor.submit(run_mpi_command, args, dargs, hostfile, HPJ, date_stamp): hostfile for hostfile in hostfiles}
#                for future in concurrent.futures.as_completed(future_to_task):
#                    results_df = future.result()
#                    if 'Status' in results_df.columns:
#                        results_df['Status'] = results_df['Status'].astype(str)
#                    hpj_results_df = pd.concat([hpj_results_df, results_df], ignore_index=True)
#                    all_results_df = pd.concat([all_results_df, results_df], ignore_index=True)

            # Clean up orphan processes after each batch of hostfiles
#            cleanup_orphans_parallel(args.hostfile, args.nccl_test, args)

            # NEW: if this HPJ is large, wait 60s before the NEXT batch starts
#            if HPJ >= 64 and batch_idx < total_batches:
#                logging.info(f"HPJ={HPJ} >= 64 — sleeping 60 seconds before next batch ({batch_idx+1}/{total_batches})")
#                time.sleep(60)
        # Collect results only for this HPJ
        hpj_results_df = pd.DataFrame()

        # NEW: we need the batch index and total for conditional sleeping
        total_batches = len(hostfile_list)
        for batch_idx, hostfiles in enumerate(hostfile_list, start=1):
            logging.info(f"HPJ: {HPJ}, Batch {batch_idx}/{total_batches}, Hostfiles: {hostfiles}")
            with concurrent.futures.ThreadPoolExecutor(args.max_workers) as executor:
                future_to_task = {executor.submit(run_mpi_command, args, dargs, hostfile, HPJ, date_stamp): hostfile for hostfile in hostfiles}
                for future in concurrent.futures.as_completed(future_to_task):
                    results_df = future.result()
                    if 'Status' in results_df.columns:
                        results_df['Status'] = results_df['Status'].astype(str)
                    hpj_results_df = pd.concat([hpj_results_df, results_df], ignore_index=True)
                    all_results_df = pd.concat([all_results_df, results_df], ignore_index=True)

            # Clean up orphan processes after each batch
            cleanup_orphans_parallel(args.hostfile, args.nccl_test, args)

            # NEW: if this HPJ is large, wait 60s before the NEXT batch starts
            if HPJ >= 64 and batch_idx < total_batches:
                logging.info(f"HPJ={HPJ} >= 64 — sleeping 60 seconds before next batch ({batch_idx+1}/{total_batches})")
                time.sleep(60)


        # --------------------------------------
        # HPJ-specific post-processing:
        # --------------------------------------
        if HPJ in (1, 2):
            # Identify bad hosts across ALL rounds we just ran (HPJ=1/2)
            thr2 = args.bad_min_appearances if args.bad_min_appearances is not None else args.bad_h2_min_appearances
            hpj_threshold = args.bad_h1_min_appearances if HPJ == 1 else (thr2 if HPJ == 2 else 1)

            bhosts = check_for_bad_hosts(HPJ, hpj_results_df, min_appearances=hpj_threshold)
            if bhosts:
                outname = f"bad_nodes_hpj{HPJ}.txt"
                with open(outname, "w") as f:
                    for h in bhosts:
                        f.write(h + "\n")
                logging.info(f"Wrote {len(bhosts)} bad hosts to {outname}")
                bad_hosts_overall.extend(bhosts)
            else:
                logging.info(f"No bad hosts detected for HPJ={HPJ} using min_appearances={hpj_threshold}.")

            # Prune for HPJ=1/2 if enabled
            if args.prune_bad_hosts:
                if bhosts:
                    bad_set = set(bhosts)
                    before = len(hosts)
                    hosts = [h for h in hosts if h not in bad_set]
                    after = len(hosts)
                    try:
                        pruned_path = _write_pruned_hostfile(hosts, args)
                        logging.info(f"Pruned {before - after} hosts ({sorted(bad_set)}) from list; "
                                     f"{after} remain. Wrote pruned hostfile: {pruned_path}")
                    except Exception as e:
                        logging.warning(f"Failed to write pruned hostfile: {e}")

                    # STDOUT report for this HPJ batch
                    print(f"=== PRUNE REPORT (HPJ={HPJ}) ===", flush=True)
                    print(f"Pruned hosts ({len(bhosts)}): {', '.join(sorted(bhosts))}", flush=True)
                    print(f"Remaining healthy hosts: {after}", flush=True)
                else:
                    print(f"=== PRUNE REPORT (HPJ={HPJ}) ===", flush=True)
                    print("Pruned hosts: none", flush=True)
                    print(f"Remaining healthy hosts: {len(hosts)}", flush=True)

            # Warn if the next HPJ won’t have enough hosts
            hpj_list_int = list(map(int, args.hosts_per_job))
            next_idx = hpj_list_int.index(HPJ) + 1
            if next_idx < len(hpj_list_int):
                next_hpj = hpj_list_int[next_idx]
                if len(hosts) < next_hpj:
                    logging.warning(f"Only {len(hosts)} hosts remain; insufficient for upcoming HPJ={next_hpj}. Subsequent batches may be skipped.")

        else:
            # HPJ > 2: DO NOT PRUNE automatically.
            # 1) Report the file names for each host set that failed in this HPJ.
            failed_mask = (hpj_results_df['Nodes'] == HPJ) & (
                hpj_results_df['Status'].str.contains('Failed', na=False) |
                hpj_results_df['Status'].str.contains('Timeout', na=False)
            )
            failed_sets = sorted(set(hpj_results_df.loc[failed_mask, 'HostSet'].tolist()))
            if failed_sets:
                print(f"=== FAILED HOSTSETS (HPJ={HPJ}) ===", flush=True)
                for hs in failed_sets:
                    print(hs, flush=True)
            else:
                print(f"=== FAILED HOSTSETS (HPJ={HPJ}) ===", flush=True)
                print("None", flush=True)

            # 2) Optional TRIAGE to isolate underperforming host(s) using a good hostfile from the same batch.
            if args.triage_hpj_gt2 and failed_sets:
                good_hostfile = _pick_good_hostfile_for_triage(args, hpj_results_df, HPJ)
                if not good_hostfile:
                    logging.warning(f"HPJ={HPJ}: No successful HostSet found and no --triage_good_hostfile provided; skipping triage.")
                else:
                    print(f"=== TRIAGE (HPJ={HPJ}) using good hostfile: {good_hostfile} ===", flush=True)
                    triage_all_bad = set()
                    triage_all_results = pd.DataFrame()
                    for failed_hs in failed_sets:
                        bads, tri_df = _triage_failed_hostfile(args, dargs, HPJ, good_hostfile, failed_hs, date_stamp)
                        triage_all_bad.update(bads)
                        triage_all_results = pd.concat([triage_all_results, tri_df], ignore_index=True)
                        if bads:
                            print(f"Underperformers from {failed_hs}: {', '.join(bads)}", flush=True)
                        else:
                            print(f"{failed_hs}: no single-node culprit found via triage.", flush=True)

                    # Write triage reports
                    tri_csv = f"triage_report_hpj{HPJ}_{date_stamp}.csv"
                    triage_all_results.to_csv(tri_csv, index=False)
                    tri_txt = f"bad_nodes_hpj{HPJ}_triage_{date_stamp}.txt"
                    with open(tri_txt, 'w') as f:
                        for h in sorted(triage_all_bad):
                            f.write(h + "\n")
                    logging.info(f"HPJ={HPJ} triage written: {tri_csv}, {tri_txt}")

                    # If pruning is enabled, prune ONLY triage-confirmed bad hosts for HPJ>2
                    if args.prune_bad_hosts and triage_all_bad:
                        bad_set = set(triage_all_bad)
                        before = len(hosts)
                        hosts = [h for h in hosts if h not in bad_set]
                        after = len(hosts)
                        try:
                            pruned_path = _write_pruned_hostfile(hosts, args)
                            logging.info(f"(HPJ>{2}) Pruned {before - after} triage-confirmed hosts ({sorted(bad_set)}); "
                                         f"{after} remain. Wrote pruned hostfile: {pruned_path}")
                        except Exception as e:
                            logging.warning(f"Failed to write pruned hostfile after triage: {e}")

                        print(f"=== PRUNE REPORT (HPJ={HPJ}, TRIAGE) ===", flush=True)
                        print(f"Pruned hosts ({len(bad_set)}): {', '.join(sorted(bad_set))}", flush=True)
                        print(f"Remaining healthy hosts: {after}", flush=True)

            # No prune summary here unless triage pruned above.

    # Sort the global dataframe for the report
    all_results_df = all_results_df.sort_values(
        by="HostSet",
        key=lambda x: np.argsort(index_natsorted(all_results_df["HostSet"]))
    )

    # Use the script-start timestamp for report naming to keep all outputs aligned in time.
    run_type = get_nccl_run_type(args)
    report_name = f'report_{run_type}_{date_stamp}.csv'
    if args.guidance:
        report_name = f'guidance_report_{run_type}_{date_stamp}.csv'

    if args.output_dir:
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        all_results_df.to_json(os.path.join(args.output_dir, f'{report_name[:-4]}.json'), orient='records', lines=False)
        all_results_df.to_csv(os.path.join(args.output_dir, report_name), index=False)
    else:
        all_results_df.to_csv(report_name, index=False)

    tmp_results_df = all_results_df.loc[:, ~all_results_df.columns.str.startswith('time_')]

    logging.info("\nResults Report:")
    logging.info(f"\n{tabulate(tmp_results_df, headers='keys', tablefmt='simple_outline')}")

    failed_hosts = all_results_df[all_results_df['Status'].astype(str).str.contains('Failed', na=False)]
    failed_hosts = failed_hosts.loc[:, ~all_results_df.columns.str.startswith('time_')]
    if not failed_hosts.empty:
        logging.info("\nFailed Hosts:")
        logging.info(f"\n{tabulate(failed_hosts, headers='keys', tablefmt='simple_outline')}")

    if args.guidance:
        logging.info("\nGuidance Report:")
        guidance_df = tmp_results_df.groupby(['Nodes']).max(numeric_only=True)
        for col in ['HostSet', 'algo', 'proto', 'Status']:
            if col in guidance_df.columns:
                guidance_df = guidance_df.drop(columns=[col])
        logging.info(f"\n{tabulate(guidance_df, headers='keys', tablefmt='simple_outline')}")

    # --- FINAL HEALTHY HOSTS FILE + STDOUT ---
    # Default name: healthy_hosts_<number_of_nodes>_<date>.txt
    # - number_of_nodes: count of hosts actually written
    # - date: script-start timestamp (date_stamp)
    healthy_count = len(hosts)
    default_healthy_name = f"healthy_hosts_{healthy_count}_{date_stamp}.txt"
    healthy_path = args.healthy_hostfile or default_healthy_name
    try:
        with open(healthy_path, 'w') as f:
            for h in hosts:
                f.write(h + "\n")
        print(f"=== FINAL HEALTHY HOSTS ===", flush=True)
        print(f"Wrote {healthy_count} healthy hosts to: {healthy_path}", flush=True)
    except Exception as e:
        logging.warning(f"Failed to write healthy hosts file {healthy_path}: {e}")

def identify_suspect_hosts(df):
    # Placeholder for future heuristics (e.g., z-score on per-size BW).
    pass

def check_mpirun_exists():
    try:
        subprocess.run(['mpirun', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    except FileNotFoundError:
        logging.error("mpirun not found in the system. Please load a module or source a setup file to use mpirun.")
        return False
    return True

# --------------------------- Entrypoint ---------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run set of NCCL tests")

    parser.add_argument('--hostfile', type=str, help='The hostfile to use')
    parser.add_argument('--hosts_per_job', nargs='+', required=False, default=[16], help='Number of hosts per job. Can specify 1 or more values separated by spaces')
    parser.add_argument('--begin_size', type=str, required=False, default='512KB', help='Beginning size for the NCCL test')
    parser.add_argument('--end_size', type=str, required=False, default='8G', help='End size for the NCCL test')
    parser.add_argument('--nccl_test', type=str, required=False, default="/data/launches/xsun/nccl-tests/build/alltoall_perf", help='NCCL test to run')
    parser.add_argument('--nccl_topo_file', type=str, required=False, default="/data/nccl-topology.xml", help='NCCL topology file to use')
    parser.add_argument('--nccl_algo', type=str, required=False, default="default", help='NCCL algorithm to use')
    parser.add_argument('--nccl_proto', type=str, required=False, default="default", help='NCCL Proto(s) to use')
    parser.add_argument('--nccl_ib_hca', type=str, required=False, default="mlx5", help='NCCL IB HCA interfaces to use')
    parser.add_argument('--quiet', action='store_true', help='If set, write logs to a file instead of the console')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--max_workers', type=int, required=False, default=64, help='Maximum number of workers to use for parallel execution')
    parser.add_argument('--guidance', action='store_true', help='Guidance run')
    parser.add_argument('--nccl_iters', type=int, required=False, default=50, help='Number of nccl iterations to run')
    parser.add_argument('--iterations', type=int, required=False, default=1, help='Number of iterations to run per HPJ')
    parser.add_argument('--runs_per_node_count', type=int, required=False, default=1, help='Number of runs per node count')
    parser.add_argument('--nccl_nchannels', type=str, required=False, help='NCCL nchannels to run')
    parser.add_argument('--noisy_neighbors', action='store_true', help='Run noisy neighbors test')
    parser.add_argument('--find_waldo', action='store_true', help='Find Waldo takes the hosts list and runs set of tests for all but one host')
    parser.add_argument('--ssh_port', type=int, default=22, help="port for ssh to use")
    parser.add_argument('--no_ucx', action='store_true', help='Do not use UCX')
    parser.add_argument('--good_hosts', type=str, help='List of good hosts')
    parser.add_argument('--nccl_qps_per_connection', type=int, required=False, help='NCCL IB QPS per connection')
    parser.add_argument('--output_dir', type=str, required=False, help='Output directory for the results')
    parser.add_argument('--node_shape', type=str, required=False, default='h100', help='Node shape (h200 or h100)')
    parser.add_argument('--net_plugin', type=str, required=False, default='none', help='path to the NCCL net plugin to use')
    parser.add_argument('--timeout', type=int, required=False, default=300, help='Timeout for the command in seconds')
    parser.add_argument('--rocm', action='store_true', help='Use ROCm for the NCCL tests')
    parser.add_argument('--nccl_debug', action='store_true', help='Enable NCCL debug mode')

    # --- HPJ-specific thresholds for bad-host detection (HPJ=1/2 only) ---
    parser.add_argument('--bad_h1_min_appearances', type=int, default=1,
                        help='HPJ=1: mark a host bad if it appears in Failed/Timeout runs at least this many times (default: 1)')
    parser.add_argument('--bad_h2_min_appearances', type=int, default=2,
                        help='HPJ=2: mark a host bad if it appears in Failed/Timeout pairs at least this many times (default: 2)')
    parser.add_argument('--bad_min_appearances', type=int, default=None,
                        help='[deprecated] alias for --bad_h2_min_appearances')

    # --- Pruning controls ---
    parser.add_argument('--prune_bad_hosts', action='store_true',
                        help='For HPJ=1/2 (and for HPJ>2 only after triage), remove bad hosts from the in-memory host list and write a pruned hostfile.')
    parser.add_argument('--pruned_hostfile', type=str, default=None,
                        help='Output path for the pruned hostfile. Default: <input hostfile basename>_pruned.txt')
    parser.add_argument('--healthy_hostfile', type=str, default=None,
                        help='Output path for the final healthy host list. Default: healthy_hosts_<num>_<date>.txt')

    # --- Triage options for HPJ>2 ---
    parser.add_argument('--triage_hpj_gt2', action='store_true',
                        help='For HPJ>2, after reporting failed hostsets, try to isolate bad hosts by combining (HPJ-1) from a good hostfile with each candidate.')
    parser.add_argument('--triage_good_hostfile', type=str, default=None,
                        help='Known-good hostfile to use as the base for HPJ>2 triage. If omitted, a successful HostSet from the same batch is auto-selected.')
    parser.add_argument('--triage_iters', type=int, default=None,
                        help='Override --nccl_iters just for triage runs (smaller saves time).')

    args = parser.parse_args()
    dargs = parser.parse_args([])

    if args.find_waldo:
        args.max_workers = 1
        nhosts = len(open(args.hostfile).readlines())
        args.hosts_per_job = [nhosts - 1]

    # Script-start timestamp (used for all outputs, including healthy hosts filename default)
    date_stamp = datetime.now().strftime('%Y%m%d%H%M%S')

    if args.quiet:
        logging.basicConfig(filename=f'/home/ubuntu/jshelley/nccl_tests/nccl_tests_logfile_{date_stamp}.log',
                            level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        if args.debug:
            logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        else:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if args.iterations > 1:
        logging.info(f"Setting HPJ to {args.hosts_per_job} for {args.iterations} iterations.")
        args.hosts_per_job = args.hosts_per_job * args.iterations
        logging.info(f"New HPJ: {args.hosts_per_job}")

    if not check_mpirun_exists():
        sys.exit(1)

    execute_command_in_sets_of_hosts_with_mpirun(args, dargs, date_stamp)

