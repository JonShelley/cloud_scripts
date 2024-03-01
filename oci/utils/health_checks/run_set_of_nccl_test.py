#!/usr/bin/env python3

import subprocess
from subprocess import TimeoutExpired
import time
from itertools import combinations
import pandas as pd
import numpy as np
import warnings
import math
import argparse
import concurrent.futures
from  tabulate import tabulate
from natsort import index_natsorted
import logging

warnings.simplefilter(action='ignore', category=FutureWarning)
 
wait = 1
pd.options.display.width = 0

def run_mpi_command(args, hostfile, HPJ, timeout=300):
    logging.debug(f"Running on {HPJ} using mpirun for ({hostfile})")
    NP = HPJ * 8
    results_df = pd.DataFrame() 
    mpirun_command = f"mpirun --allow-run-as-root"
    mpirun_command += f" --mca coll ^hcoll -mca coll_hcoll_enable 0 -np {NP} -npernode 8 --bind-to numa"
    mpirun_command += f" -hostfile {hostfile} -x NCCL_ALGO=auto -x NCCL_CROSS_NIC=0 -x NCCL_SOCKET_NTHREADS=16"
    mpirun_command += f" -x NCCL_DEBUG=WARN -x NCCL_CUMEM_ENABLE=0 -x NCCL_IB_SPLIT_DATA_ON_QPS=0"
    mpirun_command += f" -x NCCL_IB_QPS_PER_CONNECTION=16 -x NCCL_IB_GID_INDEX=3 -x NCCL_IB_HCA=mlx5"
    mpirun_command += f" -x NCCL_IB_TC=41 -x NCCL_IB_SL=0 -x NCCL_IB_TIMEOUT=22 -x NCCL_NET_PLUGIN=none"
    mpirun_command += f" -x HCOLL_ENABLE_MCAST_ALL=0 -x coll_hcoll_enable=0 -x UCX_TLS=tcp -x UCX_NET_DEVICES=eth0"
    mpirun_command += f" -x RX_QUEUE_LEN=8192 -x IB_RX_QUEUE_LEN=8192 -x NCCL_TOPO_FILE={args.nccl_topo_file} {args.nccl_test}"
    mpirun_command += f" -b {args.begin_size} -f 2 -g 1 -e {args.end_size} -c 1"
    logging.info(mpirun_command)
    try:
        output = subprocess.run(mpirun_command, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, universal_newlines=True, timeout=timeout)
        lines = output.stdout.split('\n')
        tmp_data = {'msg_size': [], 'results': []}
        for line in lines:
            line = line.strip()
            columns = line.split()
            if len(columns) >= 3 and columns[2] == 'float':
                msg_size = columns[0] # Column 12 (0-based index)
                result = columns[11]  # Column 12 (0-based index)
                tmp_data['msg_size'].append(int(msg_size))
                tmp_data['results'].append(float(result))

        logging.debug(f"Test executed on host set {hostfile} ran successfully. Msg Size: {tmp_data['msg_size']}, Result: {tmp_data['results']}")
        
        row_data = {'HostSet': [hostfile]}
        for msg_size, result in zip(tmp_data['msg_size'], tmp_data['results']):
            row_data[str(msg_size)] = result
        
    except TimeoutExpired:
        logging.info("Command timed out after 5 minutes.")   
        row_data = {'HostSet': [hostfile]} 
    except subprocess.CalledProcessError as e:
        logging.info(f"Error executing command for {hostfile} using mpirun: {e}")
    except Exception as e:
        logging.info(f"An unexpected error occurred for job set {hostfile} --- {e}  ")
    time.sleep(wait)
    results_df = pd.concat([results_df, pd.DataFrame(row_data)], ignore_index=True)
    logging.debug(f"Results: {results_df}")
    return results_df

def execute_command_in_sets_of_hosts_with_mpirun(args):
    with open(args.hostfile, 'r') as file:
        hosts = file.readlines()

    hosts = [h.strip() for h in hosts]
    all_results_df = pd.DataFrame()
    for value in args.hosts_per_job:
        # Check if there are enough hosts to perform the command
        HPJ = int(value)
        if len(hosts) < HPJ:
            print("Not enough hosts to perform the command between {} nodes.".format(HPJ))
            return
    
        # Create an empty DataFrame to store results
        results_df = pd.DataFrame()
        total=math.floor((len(hosts)/HPJ))
        print(f"Total number of tests to run for hosts/job = {HPJ}: {total}")

        #for host1, host2 in host_pairs:
        hostfiles = []
        if HPJ == 2:
            host_pairs = create_circular_pairs(hosts)
            logging.debug(f"Hosts: {host_pairs}")
            for i in range(0, len(host_pairs)):
                logging.debug(f"Hosts: {host_pairs[i]}")
                host1 = host_pairs[i][0]
                host2 = host_pairs[i][1]

                idx1 = hosts.index(host1)
                idx2 = hosts.index(host2)
                
                # Format the filename with leading zeros
                idx1 = '{0:03d}'.format(idx1)
                idx2 = '{0:03d}'.format(idx2)
                node_num = '{0:03d}'.format(HPJ)
                hosts_filename = f'hostfile-{node_num}n-{idx1}-{idx2}.txt'
                with open(hosts_filename, 'w') as f:
                    for host in host_pairs[i]:
                        f.write(f'{host}\n')
                    hostfiles.append(hosts_filename)
            hostfile_list = []
            if len(hostfiles) == 1:
                hostfile_list.append(hostfiles)
                logging.debug(f"{hostfiles}")
            elif len(hostfiles)%2 == 0:
                even_index_list = []
                odd_index_list = []
                for i in range(0, len(hostfiles), 2):
                    even_index_list.append(hostfiles[i])
                for i in range(1, len(hostfiles), 2):
                    odd_index_list.append(hostfiles[i])
                hostfile_list.append(even_index_list)
                hostfile_list.append(odd_index_list)
                logging.debug(f"{len(hostfile_list)}")
            else:
                if len(hostfiles) == 3:
                    hostfile_list.append(hostfiles[0])
                    hostfile_list.append(hostfiles[1])
                    hostfile_list.append(hostfiles[2])
                else:
                    even_index_list = []
                    odd_index_list = []
                    for i in range(0, len(hostfiles)-1, 2):
                        even_index_list.append(hostfiles[i])
                    for i in range(1, len(hostfiles)-1, 2):
                        odd_index_list.append(hostfiles[i])
                    hostfile_list.append(even_index_list)
                    hostfile_list.append(odd_index_list)
                    hostfile_list.append(hostfiles[-1])
                    logging.debug(f"{len(hostfile_list)}, {hostfile_list}")
        else:
            for i in range(0, len(hosts), HPJ):
                if (i + HPJ) <= len(hosts):
                    # Generate sets of hosts to run the test on
                    job_hosts = hosts[i:i+HPJ]
                    # Format the filename with leading zeros
                    file_num = '{0:03d}'.format(i)
                    node_num = '{0:03d}'.format(HPJ)
                    hosts_filename = f'hostfile-{node_num}n-{file_num}.txt'
                    with open(hosts_filename, 'w') as f:
                        for host in job_hosts:
                            f.write(f'{host}\n')
                    hostfiles.append(hosts_filename)
                else:
                    logging.info(f"Skipping remaining hosts as there are not enough hosts to run the test.")
                    logging.info(f"Remaining hosts: {hosts[i:]}")
                    break
            hostfile_list = [hostfiles]

        
        # Run through the host files in parallel
        for hostfiles in hostfile_list:
            logging.debug(f"Hostfiles: {hostfiles}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
                future_to_task = {executor.submit(run_mpi_command, args, hostfile, HPJ): hostfile for hostfile in hostfiles}
                for future in concurrent.futures.as_completed(future_to_task):
                    results_df = future.result()

                    # Append the results to the all_results_df
                    all_results_df = pd.concat([all_results_df, results_df], ignore_index=True)

    # Sort the dataframe by interface
    all_results_df.sort_values(by=['HostSet'])

    # Sort the dataframe by interface
    all_results_df = all_results_df.sort_values(
        by="HostSet",
        key=lambda x: np.argsort(index_natsorted(all_results_df["HostSet"]))
    )

    # Generate a table report at the end using pandas
    logging.info("\nResults Report:")
    logging.info(f"\n{tabulate(all_results_df, headers='keys', tablefmt='simple_outline')}")
    
    all_results_df.to_csv('report.csv')

def create_circular_pairs(lst):
    return [[lst[i], lst[(i+1)%len(lst)]] for i in range(len(lst))]

 
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Run set of NCCL tests")

    # Add the arguments
    parser.add_argument('--hostfile', type=str, help='The hostfile to use')
    parser.add_argument('--hosts_per_job', nargs='+', required=False, default=[16], help='Number of hosts per job. Can specify 1 or more values separated by spaces')
    parser.add_argument('--begin_size', type=str, required=False, default='1G', help='Beginning size for the NCCL test')
    parser.add_argument('--end_size', type=str, required=False, default='2G', help='End size for the NCCL test')
    parser.add_argument('--nccl_test', type=str, required=False, default="/data/launches/xsun/nccl-tests/build/alltoall_perf", help='NCCL test to run')
    parser.add_argument('--nccl_topo_file', type=str, required=False, default="/data/nccl-topology.xml", help='NCCL topology file to use')
    parser.add_argument('--nccl_algo', type=str, required=False, default="all", help='NCCL algorithm to use')
    parser.add_argument('--nccl_ib_hca', type=str, required=False, default="mlx5", help='NCCL IB HCA interfaces to use')
    parser.add_argument('--msg_sizes', nargs='+', required=False, default=[2147483648], help='NCCL message sizes to use')
    parser.add_argument('--quiet', action='store_true', help='If set, write logs to a file instead of the console')

    # Parse the arguments
    args = parser.parse_args()

    # Set up logging
    if args.quiet:
        logging.basicConfig(filename='logfile.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO)

    # Run the command
    execute_command_in_sets_of_hosts_with_mpirun(args)