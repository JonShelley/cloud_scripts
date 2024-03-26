#!/usr/bin/env python3

# Note: sudo pip3 install pandas numpy natsort Pyarrow tabulate

import subprocess
import pandas as pd
import numpy as np
from natsort import index_natsorted
import json
import logging
import concurrent.futures
import socket
import argparse
from datetime import datetime
from  tabulate import tabulate
import sys
import time
import os


class MlxlinkInfo:
    def __init__(self, args):
        # Set the timestamp
        if args.date_stamp:
            self.date_stamp = args.date_stamp
        else:
            self.date_stamp = datetime.now().strftime('%Y%m%d%H%M%S')
        if args.address:
            self.address = args.address
        else:
            self.address = None
        self.ber_threshold = args.ber_threshold
        self.eff_threshold = args.eff_threshold

        self.mlx5_interfaces = [0,1,3,4,5,6,7,8,9,10,12,13,14,15,16,17]
        #self.mlx5_interfaces = [15,17]
        self.timeout = 60
        self.host_info = {}

        self._collect_host_info()

    # Get host info number
    def _collect_host_info(self):
        try:
            # Run the shell command
            result = subprocess.run(['sudo', 'dmidecode', '-s', 'system-serial-number'], stdout=subprocess.PIPE)

            # Decode the output from bytes to string
            output = result.stdout.decode('utf-8')
            self.host_info['serial'] = output.strip()
        except Exception as e:
            logging.info(f"Error getting host serial: {e}")
            self.host_info['serial'] = 'Unknown'
        
        # Get the hostname and add it to the data
        hostname = socket.gethostname()
        self.host_info['hostname'] = hostname
    
    def get_host_info(self):
        return self.host_info

    # Retrieve a single page and report the URL and contents
    def get_mlxlink_info(self, mlx5_inter, timeout):
        cmd = f"sudo mlxlink -m -e -c -d mlx5_{mlx5_inter} --rx_fec_histogram --show_histogram --json"
        logging.debug(f"Running command: {cmd}")

        # Run the command and capture the output
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Check for errors
        if output.returncode != 0:
            logging.debug(f"Error running command: {cmd}")
            logging.debug(f"Error message: {output.stderr}")
            stdout = output.stdout
            stderr_status = output.stderr.find("No such file or directory")
            logging.debug(f"{output.returncode}, Output: {output.stderr}")
            logging.debug(f"StdErr: {stderr_status}, {output.stderr}")

            if output.returncode == 1 and stderr_status != -1:
                logging.debug("output.stdout is not json format")
                data = {'status': {}, }
                data['status']['code'] = output.returncode
                data['status']['message'] = 'fail'
            else:
                logging.debug("output.stdout is json format")
                data = json.loads(output.stdout)
        else:
            logging.debug(f"Command output: {output.stdout}")
            # Parse the output as JSON and convert it to a dataframe
            data = json.loads(output.stdout)
            
        data['mlx5_interface'] = f"{mlx5_inter}"
        data['ip_address'] = self.address

        # Add the hostname to the data
        data['hostname'] = self.host_info['hostname']

        logging.debug(f"Data: {type(data)} - {data}")

        return data
    
    def get_date_stamp(self):
        return self.date_stamp
    
    def check_mlxlink_info(self, df):
        # Check to see if the link state is up
        df.loc[df['LinkState'] != 'Active', 'Status'] = 'Fail - LinkState = {}'.format(df['LinkState'])
        
        # loop through FecBin 7-15 and verify that they are all 0
        df.loc[df['FecBin0'] == -1 , 'Status'] = 'Fail - Check interface mapping'
        df.loc[df['FecBin7'] > 0, 'Status'] = 'Watch - FecBin7 > 0'
        df.loc[df['FecBin8'] > 0, 'Status'] = 'Watch - FecBin8 > 0'
        df.loc[df['FecBin9'] > 0, 'Status'] = 'Watch - FecBin9 > 0'

        # Check to see if the raw physical BER is lower than 1E-9
        df.loc[df['RawPhyBER'] > float(self.ber_threshold), 'Status'] = 'Fail - RawPhyBER > {}'.format(self.ber_threshold) 

        # Check to see if the effective physical errors are greather than 0
        df.loc[df['EffPhyErrs'] > int(self.eff_threshold), 'Status'] = 'Fail - EffPhyErrs > {}'.format(self.eff_threshold)

        return df    

    def gather_mlxlink_info(self):
        # Create an empty dataframe
        df = pd.DataFrame()

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its mlx5 interface
            future_to_mlxlink = {executor.submit(self.get_mlxlink_info, mlx5_interface, 60): mlx5_interface for mlx5_interface in self.mlx5_interfaces}
            for future in concurrent.futures.as_completed(future_to_mlxlink):
                mlx5_interface = future_to_mlxlink[future]
                try:
                    data = future.result()
                    logging.debug(data)
                    CMD_Status = data['status']['code']
                    CMD_Status_msg = data['status']['message']
                    if CMD_Status == 0:
                        RawPhysicalErrorsPerLane = data['result']['output']['Physical Counters and BER Info']['Raw Physical Errors Per Lane']['values']
                        RawPhysicalErrorsPerLane0 = RawPhysicalErrorsPerLane[0]
                        RawPhysicalErrorsPerLane1 = RawPhysicalErrorsPerLane[1]
                        RawPhysicalErrorsPerLane2 = RawPhysicalErrorsPerLane[2]
                        RawPhysicalErrorsPerLane3 = RawPhysicalErrorsPerLane[3]
                        RawPhysicalBER = data['result']['output']['Physical Counters and BER Info']['Raw Physical BER']
                        EffectivePhysicalErrors = data['result']['output']['Physical Counters and BER Info']['Effective Physical Errors']
                        EffectivePhysicalBER = data['result']['output']['Physical Counters and BER Info']['Effective Physical BER']
                        VendorName = data['result']['output']['Module Info']['Vendor Name']
                        VendorSerialNumber = data['result']['output']['Module Info']['Vendor Serial Number']
                        Recommended = data['result']['output']['Troubleshooting Info']['Recommendation']
                        LinkState = data['result']['output']['Operational Info']['State']
                        FecBin0 = data['result']['output']['Histogram of FEC Errors']['Bin 0']['values'][1]
                        FecBin1 = data['result']['output']['Histogram of FEC Errors']['Bin 1']['values'][1]
                        FecBin2 = data['result']['output']['Histogram of FEC Errors']['Bin 2']['values'][1]
                        FecBin3 = data['result']['output']['Histogram of FEC Errors']['Bin 3']['values'][1]
                        FecBin4 = data['result']['output']['Histogram of FEC Errors']['Bin 4']['values'][1]
                        FecBin5 = data['result']['output']['Histogram of FEC Errors']['Bin 5']['values'][1]
                        FecBin6 = data['result']['output']['Histogram of FEC Errors']['Bin 6']['values'][1]
                        FecBin7 = data['result']['output']['Histogram of FEC Errors']['Bin 7']['values'][1]
                        FecBin8 = data['result']['output']['Histogram of FEC Errors']['Bin 8']['values'][1]
                        FecBin9 = data['result']['output']['Histogram of FEC Errors']['Bin 9']['values'][1]
                        FecBin10 = data['result']['output']['Histogram of FEC Errors']['Bin 10']['values'][1]
                        FecBin11 = data['result']['output']['Histogram of FEC Errors']['Bin 11']['values'][1]
                        FecBin12 = data['result']['output']['Histogram of FEC Errors']['Bin 12']['values'][1]
                        FecBin13 = data['result']['output']['Histogram of FEC Errors']['Bin 13']['values'][1]
                        FecBin14 = data['result']['output']['Histogram of FEC Errors']['Bin 14']['values'][1]
                        FecBin15 = data['result']['output']['Histogram of FEC Errors']['Bin 15']['values'][1]
                    else:
                        RawPhysicalErrorsPerLane0 = '-1'
                        RawPhysicalErrorsPerLane1 = '-1'
                        RawPhysicalErrorsPerLane2 = '-1'
                        RawPhysicalErrorsPerLane3 = '-1'
                        EffectivePhysicalErrors = '-1'
                        EffectivePhysicalBER = '-1'
                        RawPhysicalBER = '1e-99'
                        LinkState = 'Unknown'
                        Recommended = 'Unknown'
                        VendorSerialNumber = 'Unknown'
                        if 'result' in data:
                            RawPhysicalErrorsPerLane = data['result']['output']['Physical Counters and BER Info']['Raw Physical Errors Per Lane']['values']
                            RawPhysicalErrorsPerLane0 = RawPhysicalErrorsPerLane[0]
                            RawPhysicalErrorsPerLane1 = RawPhysicalErrorsPerLane[1]
                            RawPhysicalErrorsPerLane2 = RawPhysicalErrorsPerLane[2]
                            RawPhysicalErrorsPerLane3 = RawPhysicalErrorsPerLane[3]
                            RawPhysicalBER = data['result']['output']['Physical Counters and BER Info']['Raw Physical BER']
                            EffectivePhysicalErrors = data['result']['output']['Physical Counters and BER Info']['Effective Physical Errors']
                            EffectivePhysicalBER = data['result']['output']['Physical Counters and BER Info']['Effective Physical BER']
                            VendorName = data['result']['output']['Module Info']['Vendor Name']
                            VendorSerialNumber = data['result']['output']['Module Info']['Vendor Serial Number']
                            Recommended = data['result']['output']['Troubleshooting Info']['Recommendation']
                            LinkState = data['result']['output']['Operational Info']['State']
                        if 'result' in data and 'Histogram of FEC Errors' in data['result']['output']:
                            FecBin0 = data['result']['output']['Histogram of FEC Errors']['Bin 0']['values'][1]
                            FecBin1 = data['result']['output']['Histogram of FEC Errors']['Bin 1']['values'][1]
                            FecBin2 = data['result']['output']['Histogram of FEC Errors']['Bin 2']['values'][1]
                            FecBin3 = data['result']['output']['Histogram of FEC Errors']['Bin 3']['values'][1]
                            FecBin4 = data['result']['output']['Histogram of FEC Errors']['Bin 4']['values'][1]
                            FecBin5 = data['result']['output']['Histogram of FEC Errors']['Bin 5']['values'][1]
                            FecBin6 = data['result']['output']['Histogram of FEC Errors']['Bin 6']['values'][1]
                            FecBin7 = data['result']['output']['Histogram of FEC Errors']['Bin 7']['values'][1]
                            FecBin8 = data['result']['output']['Histogram of FEC Errors']['Bin 8']['values'][1]
                            FecBin9 = data['result']['output']['Histogram of FEC Errors']['Bin 9']['values'][1]
                            FecBin10 = data['result']['output']['Histogram of FEC Errors']['Bin 10']['values'][1]
                            FecBin11 = data['result']['output']['Histogram of FEC Errors']['Bin 11']['values'][1]
                            FecBin12 = data['result']['output']['Histogram of FEC Errors']['Bin 12']['values'][1]
                            FecBin13 = data['result']['output']['Histogram of FEC Errors']['Bin 13']['values'][1]
                            FecBin14 = data['result']['output']['Histogram of FEC Errors']['Bin 14']['values'][1]
                            FecBin15 = data['result']['output']['Histogram of FEC Errors']['Bin 15']['values'][1]
                        else:
                            FecBin0 = '-1'
                            FecBin1 = '-1'
                            FecBin2 = '-1'
                            FecBin3 = '-1'
                            FecBin4 = '-1'
                            FecBin5 = '-1'
                            FecBin6 = '-1'
                            FecBin7 = '-1'
                            FecBin8 = '-1'
                            FecBin9 = '-1'
                            FecBin10 = '-1'
                            FecBin11 = '-1'
                            FecBin12 = '-1'
                            FecBin13 = '-1'
                            FecBin14 = '-1'
                            FecBin15 = '-1'

                    # Set the dataframe vars
                    mlx5_interface = data['mlx5_interface']
                    host = self.host_info['hostname']
                    host_serial = self.host_info['serial']

                    temp_df = pd.DataFrame({
                                            'hostname': host,
                                            'ip_addr': data['ip_address'],
                                            'LinkState': LinkState,
                                            'HostSerial': host_serial,
                                            'CableSerial': VendorSerialNumber,
                                            'mlx5_': mlx5_interface,
                                            'EffPhyErrs': [int(EffectivePhysicalErrors)],
                                            'EffPhyBER': float(EffectivePhysicalBER),
                                            'RawPhyBER': float(RawPhysicalBER),
                                            'FecBin0': int(FecBin0),
                                            'FecBin6': int(FecBin6),
                                            'FecBin7': int(FecBin7),
                                            'FecBin8': int(FecBin8),
                                            'FecBin9': int(FecBin9),
                                            'FecBin10': int(FecBin10),
                                            'FecBin11': int(FecBin11),
                                            'FecBin12': int(FecBin12),
                                            'FecBin13': int(FecBin13),
                                            'FecBin14': int(FecBin14),
                                            'FecBin15': int(FecBin15),
                                            'Recommended': Recommended,
                                            'Status': 'Pass'
                                            })

#                    # Append the dataframe to the main dataframe
                    df = pd.concat([df, temp_df], ignore_index=True)
                    logging.debug(f"Appended data for {mlx5_interface} to dataframe")
                except Exception as exc:
                    logging.info('%r generated an exception: %s' % (mlx5_interface, exc))
                else:
                    logging.debug('mlx5_%r data collected' % (mlx5_interface))

        # Sort the dataframe by interface
        df = df.sort_values(
            by="mlx5_",
            key=lambda x: np.argsort(index_natsorted(df["mlx5_"]))
        )

        # Print the dataframe
        logging.debug(df.to_string(index=False))
        return df

if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Gather mlxlink info")

    # Add the logging level argument
    parser.add_argument('-l', '--log', default='info', help='Set the logging level (default: %(default)s)')
    parser.add_argument('-e', '--error', action='store_true', help='Set the error reporting')
    parser.add_argument('--date_stamp', type=str, help='The data file to use')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to the console (default: %(default)s)')
    parser.add_argument('-a', '--address', type=str, help='The ip address of the remote host')
    parser.add_argument('--ber_threshold', type=str, default='1e-9', help='specify the Raw Physical BER threshold')
    parser.add_argument('--eff_threshold', type=str, default='0', help='specify the Effective Physical Error threshold')

    # Parse the arguments
    args = parser.parse_args()

    # Create the MlxlinkInfo object
    mlxlink_info = MlxlinkInfo(args)

    # Get the host info
    host_info = mlxlink_info.get_host_info()

    # Set the logging level
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {args.log}')
    
    # Set the log file
    if args.quiet:
        log_filename = f"{host_info['hostname']}_mlxlink_info.log"
        logging.basicConfig(filename=log_filename, 
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=numeric_level)
    else:
        logging.basicConfig(level=numeric_level)

    # Collect the mlxlink info
    df = mlxlink_info.gather_mlxlink_info()

    # Check the mlxlink info
    df = mlxlink_info.check_mlxlink_info(df)

    # Sort the dataframe by interface
    df.sort_values(by=['hostname', 'mlx5_'])

    # Tabulate the df
    if args.error:
        # Filter the dataframe
        fail_df = df[df['Status'].str.contains('Fail')]

        # Print the filtered dataframe if not empty
        if not fail_df.empty:
            logging.info(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
        else:
            logging.info(f"No errors found in the dataframe")
    else:
        logging.info(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")

    # Log that we are saving the dataframe to a CSV file
    csv_filename = f'mlxlink_info_{args.address}_{mlxlink_info.get_date_stamp()}.csv'
    df.to_csv(csv_filename, index=False)
    logging.debug(f"Dataframe saved to {csv_filename}")