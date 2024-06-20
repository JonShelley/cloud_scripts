#!/usr/bin/env python3

# Note: sudo pip3 install tabulate pandas numpy Pyarrow

import argparse
import concurrent.futures
import datetime
import os
import glob
import pandas as pd
from  tabulate import tabulate
import logging
import subprocess
import logging.config

logging.config.fileConfig('logging.conf')

# create logger
logger = logging.getLogger('simpleExample')

class run_gpu_burn:
    def __init__(self, args):
        self.status_df = pd.DataFrame()
        self.results_df = pd.DataFrame()
        if args.date_stamp is None:
            self.date_stamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        else:
            self.date_stamp = args.date_stamp
        self.nfs = args.nfs
        self.venv = args.venv
        self.gflops_threshold = args.gflops_threshold
        self.script_directory = args.script_directory
        self.hostfile = args.hostfile
        self.exe_file = args.exe_file
        self.user = args.user
        self.max_workers = args.max_workers
        self.port = args.port 


    def get_date_stamp(self):
        return self.date_stamp


    def setup_host(self, host):
        if self.nfs:
            logging.debug(f'Setting up {host} for nfs')
            # if self.script_directory does not exist, create it
            if not os.path.exists(self.script_directory):
                status = os.mkdir(self.script_directory)
                if status != 0:
                    logging.debug(f'Error setting up nfs {host}')
                    return {'host': host, 'cmd': ['setup_host'], 'status': 'Fail', 'output': f'Error setting up {host}'}
                else:
                    logging.debug(f'Successfully set up nfs {host}')
                    return {'host': host, 'cmd': ['setup_host'], 'status': 'Pass', 'output': f'Successfully set up {host}'}
        else:
            logging.debug(f'Setting up {host}')
            cmd = f'ssh -p {self.port} {self.user}@{host} "mkdir -p {self.script_directory}"'
            logging.debug(cmd)
            output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if output.returncode != 0:
                logging.debug(f'Error setting up {host}')
                return {'host': host, 'cmd': ['setup_host'], 'status': 'Fail', 'output': output.stderr}
            else:
                logging.debug(f'Successfully set up {host}')
                return {'host': host, 'cmd': ['setup_host'], 'status': 'Pass', 'output': output.stdout}


    def setup_python_on_host(self, host):
        cmd_py_setup = "sudo apt install -y python3-pip python3-venv"
        if self.nfs:
            logging.debug(f'Setting up Python on {host} for nfs')
            if self.venv:
                # Check to see if the venv exists
                test_venv = f'test -d {self.venv}'
                if test_venv.returncode != 0:
                    cmd_py_setup = f'{cmd_py_setup}; python3 -m venv {self.venv}'
                    cmd_py_setup = f'{cmd_py_setup}; source {self.venv}/bin/activate'
                    cmd_py_setup = f'{cmd_py_setup}; pip3 install pandas numpy natsort Pyarrow tabulate'
        logging.debug(f'Setting up Python on {host}')
        cmd = f'ssh -p {self.port} {self.user}@{host} "{cmd_py_setup}"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        cmd = f'ssh -p {self.port} {self.user}@{host} "sudo pip3 install pandas numpy natsort Pyarrow tabulate"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error setting up Python on {host}')
            return {'host': host, 'cmd': ['setup_python_on_host'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully set up Python on {host}')
            return {'host': host, 'cmd': ['setup_python_on_host'], 'status': 'Pass', 'output': output.stdout}


    def distribute_file_to_host(self, host):
        logging.debug(f'Distributing {self.exe_file} to {host}')
        cmd = f'scp -P {self.port} {self.exe_file} {self.user}@{host}:{self.script_directory}'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error distributing {self.exe_file} to {host}')
            return {'host': host, 'cmd': ['distribute_file_to_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully distributed {self.exe_file} to {host}')
            return {'host': host, 'cmd': ['distribute_file_to_hosts'], 'status': 'Pass', 'output': output.stdout}


    def execute_file_on_host(self, host):
        logging.debug(f'Executing {self.exe_file} on {host}')
        cmd = f'ssh -p {self.port} {self.user}@{host} "cd {self.script_directory}; python3 {self.exe_file} --date_stamp {self.date_stamp} --gflops_threshold {self.gflops_threshold}"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error executing {self.exe_file} on {host}')
            return {'host': host, 'cmd': ['execute_file_on_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully executed {self.exe_file} on {host}')
            return {'host': host, 'cmd': ['execute_file_on_hosts'], 'status': 'Pass', 'output': output.stdout}
        

    def collect_results_from_host(self, host):
        logging.debug(f'Collecting results from {host}')
        filename = f'gpu_burn_*_{self.date_stamp}.json'
        cmd = f'scp -P {self.port} {self.user}@{host}:{self.script_directory}/{filename} .'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error collecting results from {host}')
            return {'host': host, 'cmd': ['collect_results_from_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully collected results from {host}')
            return {'host': host, 'cmd': ['collect_results_from_hosts'], 'status': 'Pass', 'output': output.stdout}

    def run_executable_on_hosts(self, task, hosts):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(task, host): host for host in hosts}
            for future in concurrent.futures.as_completed(future_to_task):
                data = future.result()
                logging.debug(f"Data: {data}")
                self.status_df = pd.concat([self.status_df, pd.DataFrame(data)], ignore_index=True)

    def process_results(self):
        # print the cwd
        logging.debug(f'cwd: {os.getcwd()}')

        # list the current directory
        logging.debug(f'ls: {os.listdir()}')

        files = glob.glob('gpu_burn_*.json')

        # Create an empty DataFrame to store results
        df = pd.DataFrame()

        # Read in the files
        for file in files:
            new_df = pd.read_json(file)
            df = pd.concat([df, new_df])
        
        # Print out results that failed
        fail_df = df[df['status'].str.contains('Failed')]

        # Print out the results
        if not fail_df.empty:
            logging.info('The following hosts have issues')
            logging.info(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
            # Write out the failed hosts to a file
            fail_df.to_csv(f'failed_hosts_{self.date_stamp}.csv', index=False)
            fail_df.to_json(f'failed_hosts_{self.date_stamp}.json', orient='records')
        else:
            logging.info('All tests passed')
            logging.debug(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")
        
        # Write out the results to a file
        df.to_csv(f'run_gpu_burn_{self.date_stamp}.csv', index=False)
        df.to_json(f'run_gpu_burn_{self.date_stamp}.json', orient='records')


if __name__ == '__main__':
    # Create the parser
    parser = argparse.ArgumentParser(description='Process some integers.')

    # Add the arguments
    parser.add_argument('--hostfile', type=str, default='hostfile.txt', help='the hostfile name')
    parser.add_argument('-f', '--exe_file', type=str, default='gpu_burn_checker.py', help='the executable file')
    parser.add_argument('--script_directory', type=str, default='cloud_scripts/oci/h100_health_checks', help='the script directory')
    parser.add_argument('-s', '--setup_host', action='store_true', help='setup the host to run gpu burn check')
    parser.add_argument('-d', '--distribute', action='store_true', help='distribute the executable file to the remote hosts')
    parser.add_argument('-e', '--execute', action='store_true', help='execute the executable file on the remote hosts')
    parser.add_argument('-c', '--collect', action='store_true', help='collect the results from the remote hosts')
    parser.add_argument('-u', '--user', default="ubuntu", type=str, help='the user name')
    parser.add_argument('--date_stamp', default=None, type=str, help='the date stamp')
    parser.add_argument('--nfs', action='store_true', help='script directory is NFS mounted (default: %(default)s)')
    parser.add_argument('--venv', type=str, default='', help='specify the python virtual environment to use')
    parser.add_argument('--gflops_threshold', type=str, default='42000', help='specify the GFlops threshold')
    parser.add_argument('--max_workers', type=int, default=32, help='specify the maximum number of workers (default: %(default)s)')
    parser.add_argument('-p', '--port', type=int, default=22, help='specify the ssh port number (default: %(default)s)')

    # Execute the parse_args() method
    args = parser.parse_args()

    hostfile = args.hostfile

    rmi = run_gpu_burn(args)

    # Read the hostfile
    with open(hostfile, 'r') as f:
        hosts = f.readlines()
        # Remove the newline character
        hosts = [x.strip() for x in hosts]

    logging.debug(f'Hosts: {hosts}')

    # Make results directory
    results_directory = f'results_{rmi.get_date_stamp()}'
    os.mkdir(results_directory)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        if args.setup_host:
            logging.debug('Setting up the hosts')
            rmi.run_executable_on_hosts(rmi.setup_host, hosts)
            logging.debug('Setting up Python on the hosts')
            rmi.run_executable_on_hosts(rmi.setup_python_on_host, hosts)
        if args.distribute:
            logging.debug('Distributing the executable to the hosts')
            rmi.run_executable_on_hosts(rmi.distribute_file_to_host, hosts)
        if args.execute:
            # Make results directory
            results_directory = f'results_{rmi.get_date_stamp()}'
            if not os.path.exists(results_directory):
                os.mkdir(results_directory)

            logging.debug('Executing the executable on the hosts')
            rmi.run_executable_on_hosts(rmi.execute_file_on_host, hosts)

            logging.debug('Collecting the results from the hosts')
            os.chdir(results_directory)
            rmi.run_executable_on_hosts(rmi.collect_results_from_host, hosts)

            # Process the results
            print('Processing the results')
            rmi.process_results()    
