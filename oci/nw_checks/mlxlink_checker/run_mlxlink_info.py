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

logging.basicConfig(level=logging.DEBUG)

class run_mlxlink_info:
    def __init__(self, date_stamp=None):
        self.status_df = pd.DataFrame()
        self.results_df = pd.DataFrame()
        if date_stamp is None:
            self.date_stamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        else:
            self.date_stamp = date_stamp

    def get_date_stamp(self):
        return self.date_stamp

    def setup_host(self, host, exe_file, script_directory, user):
        logging.debug(f'Setting up {host}')
        cmd = f'ssh {user}@{host} "mkdir -p {script_directory}"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error setting up {host}')
            return {'host': host, 'cmd': ['setup_host'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully set up {host}')
            return {'host': host, 'cmd': ['setup_host'], 'status': 'Pass', 'output': output.stdout}
        
    def setup_python_on_host(self, host, exe_file, script_directory, user):
        logging.debug(f'Setting up Python on {host}')
        cmd = f'ssh {user}@{host} "sudo pip3 install pandas numpy natsort Pyarrow tabulate"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error setting up Python on {host}')
            return {'host': host, 'cmd': ['setup_python_on_host'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully set up Python on {host}')
            return {'host': host, 'cmd': ['setup_python_on_host'], 'status': 'Pass', 'output': output.stdout}
    
    def distribute_file_to_host(self, host, exe_file, script_directory, user):
        logging.debug(f'Distributing {exe_file} to {host}')
        cmd = f'scp {exe_file} {user}@{host}:{script_directory}'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error distributing {exe_file} to {host}')
            return {'host': host, 'cmd': ['distribute_file_to_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully distributed {exe_file} to {host}')
            return {'host': host, 'cmd': ['distribute_file_to_hosts'], 'status': 'Pass', 'output': output.stdout}

    def execute_file_on_host(self, host, exe_file, script_directory, user):
        logging.debug(f'Executing {exe_file} on {host}')
        cmd = f'ssh {user}@{host} "cd {script_directory}; python3 ./{exe_file} --date_stamp {self.date_stamp} -a {host}"'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error executing {exe_file} on {host}')
            return {'host': host, 'cmd': ['execute_file_on_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully executed {exe_file} on {host}')
            return {'host': host, 'cmd': ['execute_file_on_hosts'], 'status': 'Pass', 'output': output.stdout}
        

    def collect_results_from_host(self, host, exe_file, script_directory, user):
        logging.debug(f'Collecting results from {host}')
        csv_filename = f'mlxlink_info_{host}_{self.date_stamp}.csv'
        cmd = f'scp {user}@{host}:{script_directory}/{csv_filename} .'
        logging.debug(cmd)
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.debug(f'Error collecting results from {host}')
            return {'host': host, 'cmd': ['collect_results_from_hosts'], 'status': 'Fail', 'output': output.stderr}
        else:
            logging.debug(f'Successfully collected results from {host}')
            return {'host': host, 'cmd': ['collect_results_from_hosts'], 'status': 'Pass', 'output': output.stdout}

    def run_executable_on_hosts(self, task, hosts, exe_file, script_directory, user):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_task = {executor.submit(task, host, exe_file, script_directory, user): host for host in hosts}
            for future in concurrent.futures.as_completed(future_to_task):
                data = future.result()
                logging.debug(f"Data: {data}")
                self.status_df = pd.concat([self.status_df, pd.DataFrame(data)], ignore_index=True)

    def process_results(self):
        # print the cwd
        logging.debug(f'cwd: {os.getcwd()}')

        # list the current directory
        logging.debug(f'ls: {os.listdir()}')

        files = glob.glob('mlxlink_info_*.csv')

        # Create an empty DataFrame to store results
        df = pd.DataFrame()

        # Read in the files
        for file in files:
            new_df = pd.read_csv(file)
            df = pd.concat([df, new_df])
        
        # Print out results that failed
        fail_df = df[df['Status'].str.contains('Fail')]

        # Print out the results
        if not fail_df.empty:
            logging.info('The following hosts have issues')
            logging.info(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
        else:
            logging.info('All tests passed')
            logging.info(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")

if __name__ == '__main__':
    # Create the parser
    parser = argparse.ArgumentParser(description='Process some integers.')

    # Add the arguments
    parser.add_argument('--hostfile', type=str, help='the hostfile name')
    parser.add_argument('-f', '--exe_file', type=str, help='the executable file')
    parser.add_argument('--script_directory', type=str, help='the script directory')
    parser.add_argument('-s', '--setup_host', action='store_true', help='setup the host to run mlxlink_info')
    parser.add_argument('-d', '--distribute', action='store_true', help='distribute the executable file to the remote hosts')
    parser.add_argument('-e', '--execute', action='store_true', help='execute the executable file on the remote hosts')
    parser.add_argument('-c', '--collect', action='store_true', help='collect the results from the remote hosts')
    parser.add_argument('-u', '--user', type=str, help='the user name')
    parser.add_argument('--date_stamp', default=None, type=str, help='the date stamp')

    # Execute the parse_args() method
    args = parser.parse_args()

    hostfile = args.hostfile
    exe_file = args.exe_file
    script_directory = args.script_directory

    rmi = run_mlxlink_info(args.date_stamp)

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
            rmi.run_executable_on_hosts(rmi.setup_host, hosts, args.exe_file, args.script_directory, args.user)
            rmi.run_executable_on_hosts(rmi.setup_python_on_host, hosts, args.exe_file, args.script_directory, args.user)
        if args.distribute:
            logging.debug('Distributing the executable to the hosts')
            rmi.run_executable_on_hosts(rmi.distribute_file_to_host, hosts, exe_file, script_directory, args.user)
        if args.execute:
            # Make results directory
            results_directory = f'results_{rmi.get_date_stamp()}'
            if not os.path.exists(results_directory):
                os.mkdir(results_directory)

            logging.debug('Executing the executable on the hosts')
            rmi.run_executable_on_hosts(rmi.execute_file_on_host, hosts, exe_file, script_directory, args.user)

            logging.debug('Collecting the results from the hosts')
            os.chdir(results_directory)
            rmi.run_executable_on_hosts(rmi.collect_results_from_host, hosts, exe_file, script_directory, args.user)

            # Process the results
            print('Processing the results')
            rmi.process_results()    
