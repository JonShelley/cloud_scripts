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
from  tabulate import tabulate
from natsort import index_natsorted
import logging
import sys
from datetime import datetime
import os

warnings.simplefilter(action='ignore', category=FutureWarning)
 
wait = 1
pd.options.display.width = 0

message_columns = []

# Convert message sizes to human readable format
def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 0)

    # modify s so that is matches the closest value in the acceptable_values list
    acceptable_values = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    s = min(acceptable_values, key=lambda x:abs(x-s))

    return "%s %s" % (int(s), size_name[i])

def get_nccl_run_type(args):
    # get run type name
    if "alltoall" in args.nccl_test:
        run_type = "A2A"  # All to All
    elif "all_reduce" in args.nccl_test:
        run_type = "AR"
    elif "all_gather" in args.nccl_test:
        run_type = "AG"
    elif "broadcast" in args.nccl_test:
        run_type = "BC"
    elif "gather" in args.nccl_test:
        run_type = "GA"
    elif "reduce_scatter" in args.nccl_test:
        run_type = "RS"
    elif "reduce" in args.nccl_test:
        run_type = "RE"
    elif "scatter" in args.nccl_test:
        run_type = "SC"
    elif "sendrecv" in args.nccl_test:
        run_type = "SR"
    elif "hypercube" in args.nccl_test:
        run_type = "HC"
    else:
        run_type = "Unknown"
    
    return run_type

def parse_nccl_output(output):
    # Parse the output
    lines = output.split('\n')
    tmp_data = {'msg_size': [], 'results': [], 'time': []}
    for line in lines:
        line = line.strip()
        columns = line.split()
        if len(columns) >= 3 and columns[2] == 'float':
            msg_size = columns[0] # Column 12 (0-based index)
            msg_size = convert_size(int(msg_size))
            time_stamp = columns[9] # Column 10 (0-based index)
            result = columns[11]  # Column 12 (0-based index)
            tmp_data['msg_size'].append(msg_size)
            tmp_data['results'].append(float(result))
            tmp_data['time'].append(time_stamp)

    logging.debug(f"Msg Size: {tmp_data['msg_size']}")
    logging.debug(f"Result: {tmp_data['results']}")
    logging.debug(f"Time: {tmp_data['time']}")
    message_columns = tmp_data['msg_size']

    return tmp_data

def run_mpi_command(args, dargs, hostfile, HPJ, date_stamp, timeout=300):
    logging.debug(f"Running on {HPJ} using mpirun for ({hostfile})")
    NP = HPJ * 8

    # get run type name
    run_type = get_nccl_run_type(args)

    # check if this is a guidance run
    proto = []
    algo = []
    if args.guidance:
        proto = args.nccl_proto.split(',')
        algo = args.nccl_algo.split(',')
        logging.info(f"Guidance run: proto={proto}, algo={algo}")
    else:
        proto = [args.nccl_proto]
        algo = [args.nccl_algo]
        logging.info(f"Non-guidance run: proto={proto}, algo={algo}")

    # Create an empty DataFrame to store results
    all_rows_df = pd.DataFrame()

    # Change args if noisy neighbors run
    if args.noisy_neighbors and hostfile.find('noisy') != -1:
        nccl_iters = 2000
        nccl_test = "/workspace/nccl-tests/build/alltoall_perf"
    else:
        nccl_iters = args.nccl_iters
        nccl_test = args.nccl_test
    
    # Loop through the specified protocol and algorithm
    for a in algo:
        if a == "default":
            a = dargs.nccl_algo
            tmp_proto = [dargs.nccl_proto]
        else:
            tmp_proto = proto


        for p in tmp_proto:
            results_df = pd.DataFrame()
            mpirun_command = f"ulimit -n 1000000 && mpirun"
            if args.no_ucx == False:
                mpirun_command += f" -mca pml ucx"
            mpirun_command += f" -mca coll ^hcoll"
            mpirun_command += f" -mca plm_rsh_args '-p {args.ssh_port}'"
            mpirun_command += f" -mca btl_tcp_if_include eth0  "
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
            mpirun_command += f" -x NCCL_DEBUG=warn"
            mpirun_command += f" -x NCCL_CUMEM_ENABLE=0"
            mpirun_command += f" -x NCCL_IB_TIMEOUT=22"
            mpirun_command += f" -x NCCL_IB_SL=0"
            mpirun_command += f" -x NCCL_IB_TC=41"
            mpirun_command += f" -x NCCL_IB_GID_INDEX=3"
            #mpirun_command += f" -x NCCL_BUFFSIZE=16777216"
            if args.nccl_qps_per_connection:
                mpirun_command += f" -x NCCL_IB_QPS_PER_CONNECTION={args.nccl_qps_per_connection}"
            mpirun_command += f" -x NCCL_IB_QPS_PER_CONNECTION=16"
            mpirun_command += f" -x NCCL_IB_SPLIT_DATA_ON_QPS=0"
            if args.nccl_nchannels:
                mpirun_command += f" -x NCCL_MIN_NCHANNELS={args.nccl_nchannels}"
                mpirun_command += f" -x NCCL_MAX_NCHANNELS={args.nccl_nchannels}"
            if args.nccl_proto !=  "default":
                mpirun_command += f" -x NCCL_PROTO={p}"
            if args.nccl_algo != "default":
                mpirun_command += f" -x NCCL_ALGO={a}"
            #mpirun_command += f" -x NCCL_IB_HCA=mlx5"
            mpirun_command += f" -x NCCL_IB_HCA='=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17'"
            mpirun_command += f" -x NCCL_NET_PLUGIN=none"
            mpirun_command += f" -x LD_LIBRARY_PATH"
            mpirun_command += f" {nccl_test} -b {args.begin_size} -e {args.end_size} -f 2 -g 1 -n {nccl_iters}"
            logging.info(mpirun_command)
            time_taken = 0
            try:
                # capture the time it takes to run the command
                stime = datetime.now()

                run_count = 0
                outfile_name = f'output_{hostfile[:-4]}_{run_type}_{a}_{p}_{date_stamp}_run_{run_count}.log'
                
                #check to see if the file exists. If it does, increment the run_count and try again
                while outfile_name in os.listdir():
                    run_count += 1
                    outfile_name = f'output_{hostfile[:-4]}_{run_type}_{a}_{p}_{date_stamp}_run_{run_count}.log'

                if args.noisy_neighbors and hostfile.find('noisy') != -1:
                    # Write output to a file
                    with open("noisy_" + outfile_name, 'w') as f:
                        proc1 = subprocess.Popen(mpirun_command, shell=True, stderr=f, stdout=f, universal_newlines=True)
                    return proc1
                else:
                    with open(outfile_name, 'w') as f:
                        output = subprocess.run(mpirun_command, shell=True, stderr=f, stdout=f, universal_newlines=True, timeout=timeout)
                    
                time_taken = datetime.now() - stime
                time_taken = time_taken.total_seconds()
                # Read the output file
                with open(outfile_name, 'r') as f:
                    output = f.read()

                # Parse the output
                tmp_data = parse_nccl_output(output)
                
                row_data = {'HostSet': [hostfile], "Nodes": [HPJ], "GPUs": [NP], 'algo': [a], 'proto': [p]}
                for msg_size, result in zip(tmp_data['msg_size'], tmp_data['results']):
                    row_data[str(msg_size)] = result
                for msg_size, mtime in zip(tmp_data['msg_size'], tmp_data['time']):
                    row_data[f"time_{msg_size}"] = mtime 
                
            except TimeoutExpired:
                logging.info(f"Command timed out after 5 minutes on {hostfile}.")   
                row_data = {'HostSet': [hostfile], 'Status': ['Timeout']}
            except subprocess.CalledProcessError as e:
                logging.info(f"Error executing command for {hostfile} using mpirun: {e}")
                row_data = {'HostSet': [hostfile], 'Status': ['Error executing command']}
            except Exception as e:
                logging.info(f"An unexpected error occurred for job set {hostfile} --- {e}  ")
                row_data = {'HostSet': [hostfile], 'Status': ['Unexpected Error']}
            time.sleep(wait)

            if 'Status' not in row_data:
                row_data['Status'] = ['Success']
            # Add the timeing data to the row_data
            row_data['RunTime'] = time_taken

            # Append the results to the results_df
            results_df = pd.concat([results_df, pd.DataFrame(row_data)], ignore_index=True)
            logging.debug(f"Results: {results_df}")
            all_rows_df = pd.concat([all_rows_df, results_df], ignore_index=True)

    logging.debug(f"All Results: {all_rows_df}")
    return all_rows_df

def execute_noisy_neighbor_runs(args, dargs, date_stamp):
    with open(args.hostfile, 'r') as file:
        hosts = file.readlines()

    hosts = [h.strip() for h in hosts]
    all_results_df = pd.DataFrame()
    for value in args.hosts_per_job:
        # Check if there are enough hosts to perform the command
        HPJ = int(value)
        if len(hosts) < HPJ:
            logging.error(f"Not enough hosts to perform the command between {HPJ} nodes.")
            return
        else:
            job1_hosts = hosts[:HPJ]
            job2_hosts = hosts[HPJ:]
            if len(job2_hosts) == 0:
                logging.warning(f"No extra hosts to perform the 2nd job")
                return
    
        # Create an empty DataFrame to store results
        results_df = pd.DataFrame()
        logging.info(f"Running 2 jobs. Job 1 has {HPJ} and Job 2 has {len(job2_hosts)} nodes.")

        # write out host files for job1_hosts and job2_hosts
        # Format the filename with leading zeros
        node_num = '{0:03d}'.format(len(job1_hosts))
        node_noisy_num = '{0:03d}'.format(len(job2_hosts))
        hosts_filename = f'hostfile-{node_num}n-test.txt'
        hosts_noisy_filename = f'hostfile-{node_noisy_num}n-noisy.txt'

        with open(hosts_noisy_filename, 'w') as f:
            for host in job2_hosts:
                f.write(f'{host}\n')
        with open(hosts_filename, 'w') as f:
            for host in job1_hosts:
                f.write(f'{host}\n')

        for count in range(args.runs_per_node_count):
            # Start the noisy neigbors run in the background
            noisy_proc = run_mpi_command(args, dargs, hosts_noisy_filename, len(job2_hosts), date_stamp)

            # Sleep for 5 seconds to allow the noisy neighbors run to start
            time.sleep(5)

            # Start the test run
            test_results_df = run_mpi_command(args, dargs, hosts_filename, len(job1_hosts), date_stamp)

            # Stop the noisy neighbors run
            noisy_proc.terminate()
            
            # Append the results to the all_results_df
            all_results_df = pd.concat([all_results_df, test_results_df], ignore_index=True)

    # Sort the dataframe by interface
    all_results_df.sort_values(by=['HostSet'])

    # Sort the dataframe by interface
    all_results_df = all_results_df.sort_values(
        by="HostSet",
        key=lambda x: np.argsort(index_natsorted(all_results_df["HostSet"]))
    )

    # Generate a table report at the end using pandas
    date_stamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # get run type name
    run_type = get_nccl_run_type(args)

    # Check to see if this is a noisy neighbors run
    report_name = f'noisy_report_{run_type}_{date_stamp}.csv'
    
    # Write the results to a CSV file
    all_results_df.to_csv(report_name)

    # remove any columns that starts with 'time_'
    tmp_results_df = all_results_df.loc[:, ~all_results_df.columns.str.startswith('time_')]
    logging.info("\nNoisy Results Report:")
    logging.info(f"\n{tabulate(tmp_results_df, headers='keys', tablefmt='simple_outline')}")


def execute_command_in_sets_of_hosts_with_mpirun(args, dargs, date_stamp):
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
        if args.find_waldo:
            tmp_list = create_n_minus_1_host_list(hosts)
            for i in range(0, len(tmp_list)):
                logging.debug(f"Hosts: {tmp_list[i]}")
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
                hostfile_list.append([hostfiles])
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
                    hostfile_list.append([hostfiles[0]])
                    hostfile_list.append([hostfiles[1]])
                    hostfile_list.append([hostfiles[2]])
                    logging.debug(f"{len(hostfile_list)}, {hostfile_list}")
                else:
                    even_index_list = []
                    odd_index_list = []
                    for i in range(0, len(hostfiles)-1, 2):
                        even_index_list.append(hostfiles[i])
                    for i in range(1, len(hostfiles)-1, 2):
                        odd_index_list.append(hostfiles[i])
                    hostfile_list.append(even_index_list)
                    hostfile_list.append(odd_index_list)
                    hostfile_list.append([hostfiles[-1]])
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
        # If guidance run, run through all the protocols and algorithms on only one set of hosts
        if args.guidance:
            hostfile_list = [[hostfile_list[0][0]]]
            logging.debug(f"Guidance Hostfiles: {hostfiles}")

        if args.runs_per_node_count > 1:
            tmp_hostfile_list = []
            for i in range(args.runs_per_node_count):
                tmp_hostfile_list.append([hostfile_list[0][0]])
            hostfile_list = tmp_hostfile_list
            logging.debug(f"{args.runs_per_node_count} Runs per node count: {hostfile_list}")

        for hostfiles in hostfile_list:
            logging.debug(f"Hostfiles: {hostfiles}")
            with concurrent.futures.ThreadPoolExecutor(args.max_workers) as executor:
                future_to_task = {executor.submit(run_mpi_command, args, dargs, hostfile, HPJ, date_stamp): hostfile for hostfile in hostfiles}
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
    date_stamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # get run type name
    run_type = get_nccl_run_type(args)

    # Check to see if this is a guidance run
    report_name = f'report_{run_type}_{date_stamp}.csv'
    if args.guidance:
        report_name = f'guidance_report_{run_type}_{date_stamp}.csv'
    
    # Write the results to a CSV file
    all_results_df.to_csv(report_name)

    # remove any columns that starts with 'time_'
    tmp_results_df = all_results_df.loc[:, ~all_results_df.columns.str.startswith('time_')]
    logging.info("\nResults Report:")
    logging.info(f"\n{tabulate(tmp_results_df, headers='keys', tablefmt='simple_outline')}")

    # if guidance run, report the maximum value in each column in a single row for each node count
    if args.guidance:
        logging.info("\nGuidance Report:")
        guidance_df = tmp_results_df.groupby(['Nodes']).max()
        guidance_df = guidance_df.drop(columns=['HostSet', 'algo', 'proto', 'Status'])
        logging.info(f"\n{tabulate(guidance_df, headers='keys', tablefmt='simple_outline')}")

def identify_suspect_hosts(df):
    # Identify the suspect hosts by looking at average value of each message size
    # and identifying the hosts that are significantly different from the average
    pass

def create_circular_pairs(lst):
    return [[lst[i], lst[(i+1)%len(lst)]] for i in range(len(lst))]

def create_n_minus_1_host_list(lst):
    # Create a list of lists with n-1 hosts
    return [[x for x in lst if x != y] for y in lst]

def check_mpirun_exists():
    try:
        subprocess.run(['mpirun', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    except FileNotFoundError:
        logging.error("mpirun not found in the system. Please load a module or source a setup file to use mpirun.")
        return False
    return True

 
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Run set of NCCL tests")

    # Add the arguments
    parser.add_argument('--hostfile', type=str, help='The hostfile to use')
    parser.add_argument('--hosts_per_job', nargs='+', required=False, default=[16], help='Number of hosts per job. Can specify 1 or more values separated by spaces')
    parser.add_argument('--begin_size', type=str, required=False, default='512KB', help='Beginning size for the NCCL test')
    parser.add_argument('--end_size', type=str, required=False, default='8G', help='End size for the NCCL test')
    parser.add_argument('--nccl_test', type=str, required=False, default="/data/launches/xsun/nccl-tests/build/alltoall_perf", help='NCCL test to run')
    parser.add_argument('--nccl_topo_file', type=str, required=False, default="/data/nccl-topology.xml", help='NCCL topology file to use')
    parser.add_argument('--nccl_algo', type=str, required=False, default="default", help='NCCL algorithm to use')
    parser.add_argument('--nccl_proto', type=str, required=False, default="default", help='NCCL Proto(s) to use')
    parser.add_argument('--nccl_ib_hca', type=str, required=False, default="mlx5", help='NCCL IB HCA interfaces to use')
#    parser.add_argument('--msg_sizes', nargs='+', required=False, default=[2147483648], help='NCCL message sizes to use')
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
    parser.add_argument('--nccl_qps_per_connection', type=int, required=False, default=2, help='NCCL IB QPS per connection')


    # Parse the arguments
    args = parser.parse_args()
    dargs = parser.parse_args([])

    if args.find_waldo:
        args.max_workers = 1
        nhosts = len(open(args.hostfile).readlines())
        args.hosts_per_job = [nhosts-1]

    # Get date
    date_stamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Set up logging
    if args.quiet:
        logging.basicConfig(filename=f'/shared/jshelley/nccl_tests_logfile_{date_stamp}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    # If runs_per_node_count is 1 and iterations is greater than 1, multiply HPJ
    if args.iterations > 1:
        logging.info(f"Setting HPJ to {args.hosts_per_job} for {args.iterations} iterations.")
        args.hosts_per_job = args.hosts_per_job * args.iterations
        logging.info(f"New HPJ: {args.hosts_per_job}")


    # Check if mpirun exists
    if not check_mpirun_exists():
        sys.exit(1)

    # Run the command
    if args.noisy_neighbors:
        execute_noisy_neighbor_runs(args, dargs, date_stamp)
    else:
        execute_command_in_sets_of_hosts_with_mpirun(args, dargs, date_stamp)