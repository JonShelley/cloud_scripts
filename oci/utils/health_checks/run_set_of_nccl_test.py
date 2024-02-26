#!/usr/bin/env python3

import subprocess
from subprocess import TimeoutExpired
import time
from itertools import combinations
import pandas as pd
import warnings
import math
import argparse
import concurrent.futures
from  tabulate import tabulate
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
    mpirun_command += f" -x RX_QUEUE_LEN=8192 -x IB_RX_QUEUE_LEN=8192 -x NCCL_TOPO_FILE=/h100/topo.xml {args.nccl_test}"
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
                for msg_size in args.msg_sizes:
                    #print("QQQQ", columns)
                    if int(msg_size) == int(columns[0]):
                        result = columns[11]  # Column 12 (0-based index)
                        tmp_data['msg_size'].append(int(msg_size))
                        tmp_data['results'].append(float(result))
                        break

        logging.debug(f"Test executed on host set {hostfile} ran successfully. Msg Size: {tmp_data['msg_size']}, Result: {tmp_data['results']}")
        
        row_data = {'HostSet': hostfile}
        for msg_size, result in zip(tmp_data['msg_size'], tmp_data['results']):
            row_data[str(msg_size)] = result
        results_df = results_df.append(row_data, ignore_index=True)
    except TimeoutExpired:
        logging.info("Command timed out after 5 minutes.")    
    except subprocess.CalledProcessError as e:
        logging.info(f"Error executing command for {hostfile} using mpirun: {e}")
    except Exception as e:
        logging.info(f"An unexpected error occurred for job set {hostfile} --- {e}  ")
    time.sleep(wait)
    logging.debug(f"Results: {results_df}")
    return results_df

def execute_command_in_sets_of_hosts_with_mpirun(args):
    with open(args.hostfile, 'r') as file:
        hosts = file.readlines()

    hosts = [h.strip() for h in hosts]
 
    # Define variables
    HPJ = args.hosts_per_job


    # Check if there are enough hosts to perform the command
    if len(hosts) < HPJ:
        print("Not enough hosts to perform the command between {} nodes.".format(HPJ))
        return
 
    # Create an empty DataFrame to store results
    results_df = pd.DataFrame()
    #total = len(host_pairs)
    total=math.floor((len(hosts)/HPJ))
    print(f"Total number of tests to run: {total}")

    #for host1, host2 in host_pairs:
    hostfiles = []
    for i in range(0, len(hosts), HPJ):
        if (i + HPJ) <= len(hosts):
            # Generate sets of hosts to run the test on
            job_hosts = hosts[i:i+HPJ]
            # Format the filename with leading zeros
            file_num = '{0:03d}'.format(i)
            hosts_filename = f'hostfile-{HPJ}n-{file_num}.txt'
            with open(hosts_filename, 'w') as f:
                for host in job_hosts:
                    f.write(f'{host}\n')
            hostfiles.append(hosts_filename)
        else:
            print(f"Skipping remaining hosts as there are not enough hosts to run the test.")
            print(f"Remaining hosts: {hosts[i:]}")
            break
    
    # Run through the host files in parallel
    all_results_df = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_task = {executor.submit(run_mpi_command, args, hostfile, HPJ): hostfile for hostfile in hostfiles}
        for future in concurrent.futures.as_completed(future_to_task):
            results_df = future.result()

            # Append the results to the all_results_df
            all_results_df = pd.concat([all_results_df, results_df], ignore_index=True)

    # Generate a table report at the end using pandas
    logging.info("\nResults Report:")
    logging.info(f"\n{tabulate(all_results_df, headers='keys', tablefmt='simple_outline')}")
    
    all_results_df.to_csv('report.csv')
 
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Run set of NCCL tests")

    # Add the arguments
    parser.add_argument('--hostfile', type=str, help='The hostfile to use')
    parser.add_argument('--hosts_per_job', type=int, required=False, default=16, help='Number of hosts per job')
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