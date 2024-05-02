#!/usr/bin/env python3

# Note: sudo pip3 install pandas numpy natsort Pyarrow tabulate

import traceback
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
import os
import re

import logging.config

logging.config.fileConfig('logging.conf')

# create logger
logger = logging.getLogger('simpleExample')

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
        self.flap_duration_threshold = 3600*6
        self.flap_startup_wait_time = 1800

        self._collect_host_info()

    def check_for_flaps(self):
        # Check system uptime
        cmd = "uptime -s"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        logging.debug(f"Uptime: {output.stdout}")
        date_str = output.stdout.strip()
        uptime_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        # Get rdma info to see how the interface names are mapped
        cmd = "chroot /host rdma link show"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.error(f"Error getting rdma info")
            return {}
        # Define the pattern
        pattern = r"(mlx5_\d+)/\d+ state (\w+) physical_state (\w+) netdev (\w+)"

        rdma_dict = {}
        for line in output.stdout.split('\n'):
            match = re.search(pattern, line)
            if match:
                logging.debug(f"Match: {match.group(1)}")
                rdma_dict[match.group(4)] = match.group(1)
        logging.info(f"rdma_dict: {rdma_dict}")

        # Check to see if dmesg command is available
        cmd = "chroot /host dmesg -T| grep -E 'mlx5_'"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.error(f"Error getting dmesg info")
            return {}
        
        logging.debug(f"dmesg: {output.stdout}")
        # Check for link down events
        link_dict = {}
        for line in output.stdout.split('\n'):
            if "mlx5_" in line and "link down" in line.lower():
                logging.error(f"Link down event: {line}")

                # Define the pattern
                pattern = r"\[(\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})\].*(rdma\d+): Link (\w+)"

                # Search for the date, rdma interface, and link status
                match = re.search(pattern, line)

                # If a match was found, print it
                if match:
                    link_flap_time = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Y")
                    mlx_interface = match.group(2)
                    link_status = match.group(3)
                    mlx_interface = rdma_dict[mlx_interface]
                    logging.info(f"Date and Time: {link_flap_time}, Interface: {mlx_interface}, Link Status: {link_status}")
                    
                    # Check to see if the link flap time is within the last x hours
                    if (datetime.now() - link_flap_time).total_seconds() < self.flap_duration_threshold:
                        # Check to see if the link_flap_time > than system uptime + 30 minutes
                        if (link_flap_time - uptime_date).total_seconds() > self.flap_startup_wait_time:
                            logging.debug(f"Link flap detected within the last hour: {link_flap_time}")
                            if mlx_interface not in link_dict:
                                link_dict[mlx_interface] = {"last_flap_time": link_flap_time, "flap_count": 1}
                            else:
                                link_dict[mlx_interface]["flap_count"] += 1
                                link_dict[mlx_interface]["last_flap_time"] = link_flap_time
        
        logging.info(f"Link flaps: {link_dict}")
        return link_dict

    # Get host info number
    def _collect_host_info(self):
        try:
            # Run the shell command
            # Check to see if dmidecode command is available
            cmd = "/usr/bin/which dmidecode"
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                logging.error(f"dmidecode command not found {cmd}")
                cmd = "chroot /host dmidecode -s system-serial-number"
                result2 = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                if result2.returncode != 0:
                    logging.error(f"Error getting host serial: {cmd}")
                    self.host_info['serial'] = 'Unknown'
                else:
                    output = result2.stdout
                    logging.info(f"Host serial: {output}")
                    self.host_info['serial'] = output.strip()
            else:
                # If user is root, remove sudo from the command
                if os.geteuid() == 0:
                    cmd = "dmidecode -s system-serial-number"
                else:
                    cmd = "sudo dmidecode -s system-serial-number"
                result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

                # Decode the output from bytes to string
                output = result.stdout
                self.host_info['serial'] = output.strip()
        except Exception as e:
            logging.info(f"Error getting host serial: {e}")
            logging.info(traceback.format_exc())
            self.host_info['serial'] = 'Unknown'

        # Get the hostname and add it to the data
        hostname = socket.gethostname()
        self.host_info['hostname'] = hostname
    
    def get_host_info(self):
        return self.host_info

    # Retrieve a single page and report the URL and contents
    def get_mlxlink_info(self, mlx5_inter, timeout):
        # Check if mlxlink is installed
        cmd = "mlxlink --version"
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if output.returncode != 0:
            logging.error(f"mlxlink command not found")
            cmd = "chroot /host mlxlink --version"
            output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if output.returncode != 0:
                logging.error(f"mlxlink command not found")
                sys.exit(1)
            else:
                cmd = f"chroot /host mlxlink -m -e -c -d mlx5_{mlx5_inter} --rx_fec_histogram --show_histogram --json"
        else:
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
                data['status']['message'] = 'Failed'
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
        df.loc[df['LinkState'] != 'Active', 'Status'] = 'Failed - LinkState != Active'
        
        # Check to see if the raw physical BER is lower than 1E-9
        df.loc[df['RawPhyBER'] > float(self.ber_threshold), 'Status'] = 'Failed - RawPhyBER > {}'.format(self.ber_threshold) 

        # Check to see if the effective physical errors are greather than 0
        df.loc[df['EffPhyErrs'] > int(self.eff_threshold), 'Status'] = 'Failed - EffPhyErrs > {}'.format(self.eff_threshold)

        # Check to see if the link has flapped
        df.loc[df['flap_count'] > 0, 'Status'] = 'Failed - Link Flap Detected'

        return df    

    def gather_mlxlink_info(self):
        # Create an empty dataframe
        df = pd.DataFrame()

        # Check for link flaps
        link_flaps = self.check_for_flaps()

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
                    logging.info(f"{mlx5_interface} - CMD_Status: {CMD_Status}, CMD_Status_msg: {CMD_Status_msg}")
                    if CMD_Status == 0:
                        logging.info(f"{mlx5_interface} - CMD_Status: {CMD_Status}, CMD_Status_msg: {CMD_Status_msg}")
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
                        NicFWVersion = data['result']['output']['Tool Information']['Firmware Version']
                        LinkState = data['result']['output']['Operational Info']['State']
                        if 'result' in data and 'Histogram of FEC Errors' in data['result']['output']:
                            FecBin0 = data['result']['output']['Histogram of FEC Errors']['Bin 0']['values'][1]
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
                            FecBin0 = "-1"
                            FecBin1 = "-1"
                            FecBin2 = "-1"
                            FecBin3 = "-1"
                            FecBin4 = "-1"
                            FecBin5 = "-1"
                            FecBin6 = "-1"
                            FecBin7 = "-1"
                            FecBin8 = "-1"
                            FecBin9 = "-1"
                            FecBin10 = "-1"
                            FecBin11 = "-1"
                            FecBin12 = "-1"
                            FecBin13 = "-1"
                            FecBin14 = "-1"
                            FecBin15 = "-1"
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
                        NicFWVersion = 'Unknown'
                        if 'result' in data:
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

                    try:
                        int(EffectivePhysicalErrors)
                    except:
                        EffectivePhysicalErrors = -1
                    
                    try:
                        float(EffectivePhysicalBER)
                    except:
                        EffectivePhysicalBER = -1.0
                    
                    try:
                        float(RawPhysicalBER)
                    except:
                        RawPhysicalBER = -1.0

                    tmp_name = f"mlx5_{mlx5_interface}"
                    if tmp_name in link_flaps:
                        logging.debug(f"Link flap detected: {mlx5_interface}")
                        flap_count = link_flaps[tmp_name]['flap_count']
                        last_flap_time = link_flaps[tmp_name]['last_flap_time']
                    else:
                        flap_count = 0
                        last_flap_time = None

                    temp_df = pd.DataFrame({
                                            'hostname': host,
                                            'ip_addr': data['ip_address'],
                                            'LinkState': LinkState,
                                            'HostSerial': host_serial,
                                            'CableSerial': VendorSerialNumber,
                                            'mlx5_': mlx5_interface,
                                            'nic_fw_version': NicFWVersion,
                                            'EffPhyErrs': [int(EffectivePhysicalErrors)],
                                            'EffPhyBER': float(EffectivePhysicalBER),
                                            'RawPhyBER': float(RawPhysicalBER),
                                            'flap_count': flap_count,
                                            'last_flap_time': last_flap_time,
                                            'Recommended': Recommended,
                                            'Status': 'Passed'
                                            })

#                    # Append the dataframe to the main dataframe
                    df = pd.concat([df, temp_df], ignore_index=True)
                    logging.debug(f"Appended data for {mlx5_interface} to dataframe")
                except Exception as exc:
                    logging.info('%r generated an exception: %s' % (mlx5_interface, exc))
                    logging.info(traceback.format_exc())
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
    parser.add_argument('-l', '--log', default='critical', help='Set the logging level (default: %(default)s)')
    parser.add_argument('-e', '--error', action='store_true', help='Set the error reporting')
    parser.add_argument('--date_stamp', type=str, help='The data file to use')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to the console (default: %(default)s)')
    parser.add_argument('-a', '--address', type=str, help='The ip address of the remote host')
    parser.add_argument('--ber_threshold', type=str, default='1e-7', help='specify the Raw Physical BER threshold')
    parser.add_argument('--eff_threshold', type=str, default='0', help='specify the Effective Physical Error threshold')
    parser.add_argument('--file_format', type=str, default='json', help='specify the output file format: csv,json (default: %(default)s')

    # Parse the arguments
    args = parser.parse_args()

    # Set the log level to one of the following: DEBUG, INFO, WARNING, ERROR, CRITICAL
    logging.getLogger().setLevel(args.log.upper())

    # Create the MlxlinkInfo object
    mlxlink_info = MlxlinkInfo(args)

    # Get the host info
    host_info = mlxlink_info.get_host_info()

    # Collect the mlxlink info
    df = mlxlink_info.gather_mlxlink_info()

    # Check the mlxlink info
    df = mlxlink_info.check_mlxlink_info(df)

    # Sort the dataframe by interface
    df.sort_values(by=['hostname', 'mlx5_'])

    # Set the logging level to INFO
    logging.getLogger().setLevel('INFO')

    # Tabulate the df
    if args.error:
        # Filter the dataframe
        fail_df = df[df['Status'].str.contains('Failed')]

        # Print the filtered dataframe if not empty
        if not fail_df.empty:
            logging.info(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
        else:
            logging.info(f"No errors found in the dataframe")
    else:
        logging.info(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")

    if args.file_format == 'json':
        # Write the dataframe to a JSON file
        json_filename = f'mlxlink_info_{args.address}_{mlxlink_info.get_date_stamp()}.json'
        df.to_json(json_filename, orient='records')
        logging.debug(f"Dataframe saved to {json_filename}")
    elif args.file_format == 'csv':
        # Log that we are saving the dataframe to a CSV file
        csv_filename = f'mlxlink_info_{args.address}_{mlxlink_info.get_date_stamp()}.csv'
        df.to_csv(csv_filename, index=False)
        logging.debug(f"Dataframe saved to {csv_filename}")
    else:
        logging.error(f"Invalid file format: {args.file_format}")
