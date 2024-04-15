#!/usr/bin/env python3

# Note: sudo pip3 install pandas numpy natsort tabulate

import os, sys
import subprocess
import pandas as pd
import numpy as np
import logging
import re
import concurrent.futures
from  tabulate import tabulate
from natsort import index_natsorted
import argparse
import socket
import logging.config

logging.config.fileConfig('logging.conf')

# create logger
logger = logging.getLogger('simpleExample')

base_dir = os.getcwd()

# Get host info number
def get_host_info():
    host_info = {}
    try:
        
        # If root remove sudo from the command
        if os.geteuid() == 0:
            # Check to see if dmidecode command is available
            result = subprocess.run(['which', 'dmidecode'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                logging.error("dmidecode command not found")
                result2 = subprocess.run(['chroot', '/host', 'dmidecode', '-s', 'system-serial-number'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                if result2.returncode != 0:
                    logging.error("Error getting host serial")
                    host_info['serial'] = 'Unknown'
                else:
                    output = result2.stdout.decode('utf-8')
                    host_info['serial'] = output.strip()

            result = subprocess.run(['dmidecode', '-s', 'system-serial-number'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        else:
            result = subprocess.run(['sudo', 'dmidecode', '-s', 'system-serial-number'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        
        # Decode the output from bytes to string
        if 'serial' not in host_info:
            output = result.stdout.decode('utf-8')
            host_info['serial'] = output.strip()
    except Exception as e:
        logging.info(f"Error getting host serial: {e}")
        host_info['serial'] = 'Unknown'
    
    # Get the hostname and add it to the data
    hostname = socket.gethostname()
    host_info['hostname'] = hostname

    return host_info

def install_gpu_burn():
    try:
        # Install GPU burn
        logging.info("Installing GPU burn")
        cmd = "git clone https://github.com/wilicc/gpu-burn"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        os.chdir("gpu-burn")
        cmd = "make"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        
    except Exception as e:
        logging.error(f"Error installing GPU burn: {e}")
        return False
    
    return True

def run_gpu_burn(gpu_burn_dir, gpu_id):
    # Run GPU burn
    logging.info(f"Running GPU burn on GPU {gpu_id} in {gpu_burn_dir}")
    #os.chdir(gpu_burn_dir)
    cmd = f"{gpu_burn_dir}/gpu_burn -i {gpu_id} -d -stts 1 -c {gpu_burn_dir}/compare.ptx 15"
    logging.debug(cmd)
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    # Write the output to a file
    with open(f"gpu_burn_{gpu_id}.log", "w") as f:
        f.write(output.stdout)

    if output.returncode != 0:
        logging.error(f"Error running GPU burn on GPU {gpu_id}")
        logging.error(f"stdout: {output.stdout}")
        logging.error(f"stderr: {output.stderr}")
        return False
    else:
        return {"gpu_id": gpu_id, "output": output.stdout}
    
def parse_gpu_burn_output(input, host_info):
    # Parse GPU burn output
    #logging.debug(f"output: {input['output']}")

    # Get the GPU ID
    gpu_id = input["gpu_id"]

    # Get the output
    output = input["output"]

    # Find all of the Gflops and temperatures values
    pattern = r"(\d+.\d+)%.+?(\d+) Gflop/s.+?temps: (\d+) C"

    matches = re.findall(pattern, output)

    # Create a dictionary to store the values
    results = {"host": host_info["serial"], "hostname": host_info["hostname"],"gpu_id": [gpu_id]}

    # Loop through the matches and store the values
    gflops = []
    temps = []
    for match in matches:
        gflops.append(float(match[1]))
        temps.append(int(match[2]))
    
    results["max_gflops"] = np.max(gflops)
    results["max_temp"] = np.max(temps)
    results["status"] = "Passed"

    logging.debug(f"results: {results}")
    df = pd.DataFrame(results)
    return df

def execute_gpu_burn(gpu_burn_dir, host_info):
    # Get the number of GPUs from NVIDIA SMI
    cmd = "nvidia-smi --query-gpu=count --format=csv,noheader"
    output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    if output.returncode != 0:
        logging.error(f"Error getting GPU count")
        logging.error(f"stdout: {output.stdout}")
        logging.error(f"stderr: {output.stderr}")
        return False
    
    # Get the GPU count
    gpu_count = int(output.stdout.strip().count('\n') + 1)

    # run GPU burn on all GPUs in parallel
    results = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=gpu_count) as executor:
        futures = {executor.submit(run_gpu_burn, gpu_burn_dir, i): i for i in range(gpu_count)}

        results = pd.DataFrame()
        for future in concurrent.futures.as_completed(futures):
            data = parse_gpu_burn_output(future.result(), host_info)
            #logging.debug(f"data: {data}")
            results = pd.concat([results,data], ignore_index=True)

    # Sort the results by GPU ID
    results = results.sort_values(by="gpu_id")

    logging.debug(f"\n{tabulate(results, headers='keys', tablefmt='simple_outline')}")

    return results

def check_gpu_burn_results(df):

    max_gflops_threshold = 40000
    avg_temp_threshold = 80

    # Check GPU burn results
    df.loc[df['max_gflops'] < max_gflops_threshold, 'status'] = f'Failed - GFlops > {max_gflops_threshold}'
    df.loc[df['max_temp'] > avg_temp_threshold, 'status'] = f'Failed - Temp > {avg_temp_threshold}'

    return df

def main(args,gpu_burn_dir,host_info):
    # Execute GPU burn
    results = execute_gpu_burn(gpu_burn_dir, host_info)

    # Check GPU burn results
    df = check_gpu_burn_results(results)

    # Tabulate the df
    # Filter the dataframe
    fail_df = df[df['status'].str.contains('Failed')]

    if args.error and not fail_df.empty:
        # Print the filtered dataframe if not empty
        logging.error(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
    else:
        logging.info(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")
    
    if not fail_df.empty:
        logging.error(f"GPU BURN Test: Failed")
        #logging.error(f"fail_df: {fail_df}")
    else:
        logging.info(f"GPU BURN Test: Passed")

    if args.file_format == 'csv':
        # Write the dataframe to a CSV file
        df.to_csv(f"gpu_burn_{host_info['hostname']}_results_{args.date_stamp}.csv", index=False)
    elif args.file_format == 'json':
        # Write the dataframe to a JSON file
        df.to_json(f"gpu_burn_{host_info['hostname']}_results_{args.date_stamp}.json", orient='records')
    else:
        logging.error(f"Invalid file format: {args.file_format}")

    return results

if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(description="Gather gpu_burn info")

    # Add the logging level argument
    parser.add_argument('-l', '--log', default='info', help='Set the logging level (default: %(default)s)')
    parser.add_argument('-e', '--error', action='store_true', help='Error reporting (default: %(default)s')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to the console (default: %(default)s)')
    parser.add_argument('--gpu_burn_dir', default='/opt/oci-hpc/gpu-burn', help='Set the GPU burn directory (default: %(default)s)')
    parser.add_argument('--file_format', default='json', help='Set the output file format: csv,json (default: %(default)s')
    parser.add_argument('--gflops_threshold', type=int, default=40000, help='Set the GFlops threshold (default: %(default)s)')
    parser.add_argument('--date_stamp', type=str, help='The date stamp to use')

    # Execute the parse_args() method
    args = parser.parse_args()

    # Get the host info
    host_info = get_host_info()
    
    # Set the logging level
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {args.log}')
    
    # Set the log file
    if args.quiet:
        log_filename = f"{host_info['hostname']}_gpu_burn_info.log"
        logging.basicConfig(filename=log_filename, 
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=numeric_level)
    else:
        logging.basicConfig(level=numeric_level)

    # Install GPU burn if not found
    if not os.path.exists(args.gpu_burn_dir):
        test_dir = "/tmp"
        os.chdir(test_dir)
        gpu_burn_dir = f"{test_dir}/gpu-burn"
        if not install_gpu_burn():
            logging.error("Error installing GPU burn")
            sys.exit(-1)
        else:
            logging.info("GPU burn installed successfully")
        os.chdir(base_dir)

    main(args,gpu_burn_dir,host_info)