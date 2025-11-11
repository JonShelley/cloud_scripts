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
from glob import glob

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

        self.mlx5_interfaces = args.mlx_interfaces
        
        #self.mlx5_interfaces = [15,17]
        self.timeout = 60
        self.host_info = {}
        self.host_info['hostname'] = 'Unknown'
        self.host_info['serial'] = 'Unknown'
        if args.flap_duration_threshold:
            self.flap_duration_threshold = args.flap_duration_threshold
        else:
            self.flap_duration_threshold = 3600*6
        self.flap_startup_wait_time = 1800
        self.args = args

        self.mst_mapping = {"H100": {
                "d5:00.1": "mlx5_17",
                "d5:00.0": "mlx5_16",
                "bd:00.1": "mlx5_15",
                "bd:00.0": "mlx5_14",
                "a5:00.1": "mlx5_13",
                "a5:00.0": "mlx5_12",
                "9a:00.0": "mlx5_11",
                "86:00.1": "mlx5_10",
                "86:00.0": "mlx5_9",
                "58:00.1": "mlx5_8",
                "58:00.0": "mlx5_7",
                "41:00.1": "mlx5_6",
                "41:00.0": "mlx5_5",
                "2a:00.1": "mlx5_4",
                "2a:00.0": "mlx5_3",
                "1f:00.0": "mlx5_2",
                "0c:00.1": "mlx5_1",
                "0c:00.0": "mlx5_0"
            },
            "A100": {
                "c3:00.0": "mlx5_9",
                "0c:00.0": "mlx5_5",
                "d1:00.1": "mlx5_12",
                "16:00.1": "mlx5_8",
                "89:00.0": "mlx5_14",
                "4b:00.0": "mlx5_3",
                "c3:00.1": "mlx5_10",
                "0c:00.1": "mlx5_6",
                "47:00.0": "mlx5_1",
                "93:00.0": "mlx5_16",
                "d1:00.0": "mlx5_11",
                "89:00.1": "mlx5_15",
                "4b:00.1": "mlx5_4",
                "aa:00.0": "mlx5_13",
                "16:00.0": "mlx5_7",
                "6b:00.0": "mlx5_0",
                "47:00.1": "mlx5_2",
                "93:00.1": "mlx5_17"
            },
            "GB200": {
                "0000:03:00.0": "mlx5_0",
                "0002:03:00.0": "mlx5_1",
                "0010:03:00.0": "mlx5_3",
                "0012:03:00.0": "mlx5_4",
                "0006:09:00.0": "mlx5_2",
                "0016:0b:00.0": "mlx5_5"
            },
            "B200": {
                "0c:00.0": "mlx5_0",
                "a5:00.0": "mlx5_9",
                "41:00.0": "mlx5_4",
                "bd:00.0": "mlx5_10",
                "58:00.0": "mlx5_5",
                "86:00.0": "mlx5_6",
                "9a:00.0": "mlx5_7",
                "1f:00.0": "mlx5_1",
                "2a:00.0": "mlx5_3",
                "d5:00.0": "mlx5_11"
            }
        }

        if not args.read_json_files:
            self._collect_host_info()
        else:
            logging.info("Reading JSON files")

    def check_for_flaps(self):

        if args.read_json_files:
            return {}

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
            cmd = "rdma link show"
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
            logging.error(f"Error getting dmesg info using chroot: {cmd}")
            cmd = "dmesg -T| grep -E 'mlx5_'"
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
                pattern = r"\[(\w{3} \w{3} {1,2}\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\].*(rdma\d+): Link (\w+)"

                # Search for the date, rdma interface, and link status
                match = re.search(pattern, line)

                logging.debug(f"Match: {match}")
                # If a match was found, print it
                if match:
                    link_flap_time = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Y")
                    mlx_interface = match.group(2)
                    link_status = match.group(3)
                    mlx_interface = rdma_dict[mlx_interface]
                    logging.info(f"Date and Time: {link_flap_time}, Interface: {mlx_interface}, Link Status: {link_status}")
                    
                    # Check to see if the link flap time is within the last x hours
                    logging.debug(f"Link flap time: {link_flap_time}, Uptime: {uptime_date}, Diff: {(link_flap_time - uptime_date).total_seconds()}, Duration: {self.flap_duration_threshold}")
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
                logging.debug(f"Running command: {cmd}")
                result2 = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                logging.debug(f"Result: {result2}")
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
                logging.debug(f"Root: Running command: {cmd}")
                result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                logging.debug(f"Result: {result}")
                # Decode the output from bytes to string
                output = result.stdout
                logging.info(f"Host serial: {output}")
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
            mst_cmd = "chroot /host mst status -v"
            output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if output.returncode != 0:
                logging.error(f"mlxlink command not found")
                sys.exit(1)
            else:
                cmd = f"chroot /host mlxlink -d mlx5_{mlx5_inter} -m -e -c --rx_fec_histogram --show_histogram --json"
        else:
            cmd = f"sudo mlxlink -d mlx5_{mlx5_inter} -m -e -c --rx_fec_histogram --show_histogram --json"
            mst_cmd = "mst status -v"
        
        logging.debug(f"Running command: {cmd}")

        # Run the command and capture the output
        output = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Make results directory if one does not exist
        results_dir = 'mlxlink_files'
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)

        # write the output to a file
        filename = f"{results_dir}/{self.host_info['hostname']}_mlx5_{mlx5_inter}.json"
        with open(filename, 'w') as outfile:
            outfile.write(output.stdout)

        # Get the mst status for the host
        mst_cmd = "mst status -v"

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

        # Print the column names
        logging.debug("DF Columns: ")
        logging.debug(df.columns)

        # Check to see if the FW is older than 28.39.2500
        df.loc[df['nic_fw_version'] < '28.39.2500', 'Status'] = 'Warning - FW < 28.39.2500'

        # Check to see if bad signal integrity is in the recommended message
        df.loc[df['Recommended'].str.contains('Bad signal integrity', case=False), 'Status'] = 'Failed - Bad Signal Integrity'
        
        # Check to see if the raw physical BER is lower than 1E-9
        df.loc[df['RawPhyBER'] > float(self.ber_threshold), 'Status'] = 'Failed - RawPhyBER > {}'.format(self.ber_threshold) 

        # Check to see if the effective physical errors are greather than 0
        df.loc[df['EffPhyErrs'] > int(self.eff_threshold), 'Status'] = 'Failed - EffPhyErrs > {}'.format(self.eff_threshold)

        # Check to see if the link has flapped
        df.loc[df['flap_count'] > 0, 'Status'] = 'Failed - Link Flap Detected'

        # Check to see if the link state is up
        df.loc[df['LinkState'] != 'Active', 'Status'] = 'Failed - LinkState != Active'

        # Check the FEC bins. If FEC Bin7 or higher is greater than 0, then set the status to Failed
        # FEC Bin0-6 are normal, FEC Bin7-15 are errors
        # FEC Bin values need to be converted to integers
        for i in range(0, 16):
            df[f'FecBin{i}'] = df[f'FecBin{i}'].astype(int)

        for i in range(7, 16):
            if df[f'FecBin{i}'].gt(0).any():
                df.loc[df[f'FecBin{i}'] > 100, 'Status'] = f'Failed - FEC Bin{i} > 0'

        return df    

    def gather_mlxlink_info(self):
        # Create an empty dataframe
        all_df = pd.DataFrame()

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its mlx5 interface
            future_to_mlxlink = {executor.submit(self.get_mlxlink_info, mlx5_interface, 60): mlx5_interface for mlx5_interface in self.mlx5_interfaces}
            for future in concurrent.futures.as_completed(future_to_mlxlink):
                mlx5_interface = future_to_mlxlink[future]
                data = future.result()
                df = self.process_mlxlink_info(data, mlx5_interface)
                all_df = pd.concat([all_df, df], ignore_index=True)

        # Sort the dataframe by interface
        all_df = all_df.sort_values(
            by="mlx5_",
            key=lambda x: np.argsort(index_natsorted(all_df["mlx5_"]))
        )

        if self.args.read_json_files:
            link_flaps = {}
        else:
            link_flaps = self.check_for_flaps()

        for interface in self.mlx5_interfaces:
            tmp_name = f"mlx5_{interface}"

            # Print the condition
            logging.debug(f"Condition (all_df['mlx5_'] == tmp_name): {interface} - {tmp_name}")
            logging.debug(all_df['mlx5_'] == tmp_name)

            if tmp_name in link_flaps:
                logging.debug(f"Link flap detected: mlx5_{interface}")
                logging.debug(f"Flap count: {link_flaps[tmp_name]['flap_count']}")
                logging.debug(f"Last flap time: {link_flaps[tmp_name]['last_flap_time']}")
                flap_count = link_flaps[tmp_name]['flap_count']
                last_flap_time = link_flaps[tmp_name]['last_flap_time']
                all_df.loc[all_df['mlx5_'] == str(interface), 'flap_count'] = flap_count
                all_df.loc[all_df['mlx5_'] == str(interface), 'last_flap_time'] = last_flap_time

        # Print the dataframe
        logging.debug(all_df.to_string(index=False))
        return all_df
    
    def process_mlxlink_info(self, data, mlx5_interface):
        df = pd.DataFrame()
        try:
            logging.debug(data)
            logging.debug(data['status'])
            
            CMD_Status = data['status']['code']
            CMD_Status_msg = data['status']['message']
            logging.debug(f"{mlx5_interface} - CMD_Status: {CMD_Status}, CMD_Status_msg: {CMD_Status_msg}")
            if CMD_Status == 0 or CMD_Status == 1 and CMD_Status_msg.find("FEC Histogram is not supported for the current device") != -1 :
                logging.debug(f"{mlx5_interface} - CMD_Status: {CMD_Status}, CMD_Status_msg: {CMD_Status_msg}")
                try:
                    RawPhysicalErrorsPerLane = data['result']['output']['Physical Counters and BER Info']['Raw Physical Errors Per Lane']['values']
                except:
                    RawPhysicalErrorsPerLane = [-1,-1,-1,-1]
                RawPhysicalBER = data['result']['output']['Physical Counters and BER Info']['Raw Physical BER']
                EffectivePhysicalErrors = data['result']['output']['Physical Counters and BER Info']['Effective Physical Errors']
                EffectivePhysicalBER = data['result']['output']['Physical Counters and BER Info']['Effective Physical BER']
                VendorName = data['result']['output']['Module Info']['Vendor Name']
                VendorSerialNumber = data['result']['output']['Module Info']['Vendor Serial Number']
                Recommended = data['result']['output']['Troubleshooting Info']['Recommendation']
                NicFWVersion = data['result']['output']['Tool Information']['Firmware Version']
                LinkState = data['result']['output']['Operational Info']['State']
                # Initialize FEC bins 0–15
                fec_bins = {}
                if 'result' in data and 'Histogram of FEC Errors' in data['result']['output']:
                    fec_data = data['result']['output']['Histogram of FEC Errors']
                    for i in range(16):
                        key = f"Bin {i}"
                        try:
                            fec_bins[i] = fec_data[key]['values'][1]
                        except Exception:
                            fec_bins[i] = "-1"
                            logging.debug(f"Missing FEC Bin{i} in {data.get('mlx5_interface', 'unknown')} — set to -1")
                else:
                    for i in range(16):
                        fec_bins[i] = "-1"
                        logging.debug(f"No Histogram of FEC Errors present — FEC Bin{i} set to -1")

                # Unpack fec_bins into named variables
                for i in range(16):
                    globals()[f"FecBin{i}"] = fec_bins[i]
            else:
                RawPhysicalErrorsPerLane = [-1,-1,-1,-1]
                EffectivePhysicalErrors = '-1'
                EffectivePhysicalBER = '-1'
                RawPhysicalBER = '1e-99'
                LinkState = 'Unknown'
                Recommended = 'Unknown'
                VendorSerialNumber = 'Unknown'
                NicFWVersion = 'Unknown'
                if 'result' in data:
                    logging.debug(f"Results: {data['results']}")
                    
                    RawPhysicalBER = data['result']['output']['Physical Counters and BER Info']['Raw Physical BER']
                    RawPhysicalErrorsPerLane = data['result']['output']['Physical Counters and BER Info']['Raw Physical Errors Per Lane']['values']
                    EffectivePhysicalErrors = data['result']['output']['Physical Counters and BER Info']['Effective Physical Errors']
                    EffectivePhysicalBER = data['result']['output']['Physical Counters and BER Info']['Effective Physical BER']
                    VendorName = data['result']['output']['Module Info']['Vendor Name']
                    VendorSerialNumber = data['result']['output']['Module Info']['Vendor Serial Number']
                    Recommended = data['result']['output']['Troubleshooting Info']['Recommendation']
                    LinkState = data['result']['output']['Operational Info']['State']
                # Initialize FEC bins 0–15
                fec_bins = {}
                if 'result' in data and 'Histogram of FEC Errors' in data['result']['output']:
                    fec_data = data['result']['output']['Histogram of FEC Errors']
                    for i in range(16):
                        key = f"Bin {i}"
                        try:
                            fec_bins[i] = fec_data[key]['values'][1]
                        except Exception:
                            fec_bins[i] = "-1"
                            logging.debug(f"Missing FEC Bin{i} in {data.get('mlx5_interface', 'unknown')} — set to -1")
                else:
                    for i in range(16):
                        fec_bins[i] = "-1"
                        logging.debug(f"No Histogram of FEC Errors present — FEC Bin{i} set to -1")

                # Unpack fec_bins into named variables
                for i in range(16):
                    globals()[f"FecBin{i}"] = fec_bins[i]

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

            try:
                # Convert the list of strings to a list of integers
                RawPhysicalErrorsPerLane = [int(i) for i in RawPhysicalErrorsPerLane]
                #print(f"RawPhysicalErrorsPerLane: {RawPhysicalErrorsPerLane}")
                RawPhyErrPerLaneStdev = np.std(RawPhysicalErrorsPerLane)
                # Convert the standard deviation to a float
                RawPhyErrPerLaneStdev = float(RawPhyErrPerLaneStdev)
            except:
                RawPhyErrPerLaneStdev = 0.0

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
                                    'RawPhyErrStdev': RawPhyErrPerLaneStdev,
                                    'flap_count': 0,
                                    'last_flap_time': None,
                                    'Recommended': Recommended,
                                    # Return of list of all of the Fec bins
                                    'FecBin0': FecBin0,
                                    'FecBin1': FecBin1,
                                    'FecBin2': FecBin2,
                                    'FecBin3': FecBin3,
                                    'FecBin4': FecBin4,
                                    'FecBin5': FecBin5,
                                    'FecBin6': FecBin6,
                                    'FecBin7': FecBin7,
                                    'FecBin8': FecBin8,
                                    'FecBin9': FecBin9,
                                    'FecBin10': FecBin10,
                                    'FecBin11': FecBin11,
                                    'FecBin12': FecBin12,
                                    'FecBin13': FecBin13,
                                    'FecBin14': FecBin14,
                                    'FecBin15': FecBin15,
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

        return df
    
    def read_json_files(self):
        logging.debug("Reading JSON files")
        # Get the list of JSON files
        json_files = glob('*_mlx5_*.json')
        

        all_df = pd.DataFrame()

        # Loop through the JSON files
        for file in json_files:
            # Read the JSON file
            with open(file, 'r') as infile:
                data = json.load(infile)

            mlx5_inter = file.split('_')[2].split('.')[0]
            if self.args.process_min_files:
                hostname = file.split('_')[3]
            else:
                hostname = file.split('_')[0]

            data['mlx5_interface'] = f"{mlx5_inter}"
            data['ip_address'] = self.address

            logging.debug(f"Hostname: {hostname}, mlx5_interface: {mlx5_inter}")

            # Add the hostname to the data
            self.host_info['hostname'] = hostname
            self.host_info['serial'] = 'Unknown'
            data['hostname'] = self.host_info['hostname']

            logging.debug(f"Data: {type(data)} - {data}")

            #print("Data: ", data)

            df = self.process_mlxlink_info(data, mlx5_inter)

            # Append the dataframe to the main dataframe
            all_df = pd.concat([all_df, df], ignore_index=True)
            
        return all_df

    def read_min_json_files(self):
        logging.debug("Reading Min JSON files")

        if self.args.process_min_files == 'CWD':
            # Get the current working directory
            files_dir = os.getcwd()
        else:
            files_dir = self.args.process_min_files

        # Check to see if the directory exists
        if not os.path.exists(files_dir):
            logging.error(f"Min JSON files directory not found: {files_dir}")
            sys.exit(1)

        # Get the list of JSON files
        logging.debug(f"Files dir: {files_dir}")
        logging.debug(f"{files_dir}/*mlxlink_info_min*.json")
        json_files = glob(f'{files_dir}/*mlxlink_info_min*.json')
        json_files += glob(f'{files_dir}/*test_min.json')

        logger.debug(f"JSON Files: {json_files}")

        all_df = pd.DataFrame()

        # Loop through the JSON files
        for file in json_files:
            logging.debug(f"File: {file}")
            # Try to read the JSON file. If it fails, skip the file and print an error message
            try:
                with open(file, 'r') as infile:
                    data = json.load(infile)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
                continue

            hostname = data['hostname']
            logging.debug(f"Hostname: {hostname}")

            for key in data['mst_status']:
                logging.debug(f"Key: {key}, Data: {data['mst_status'][key]}")
                std_mlx_interface = self.convert_mst_status_to_standard_mlx5(key)
                logging.debug(f"Standard mlx5 interface: {std_mlx_interface}")

                data['mlx5_interface'] = f"{std_mlx_interface}"
                data['ip_address'] = None
                if self.args.process_min_files:
                    mlx5_inter = std_mlx_interface
                else:
                    mlx5_inter = data['mst_status'][key][5:]

                # Add the hostname to the data
                self.host_info['hostname'] = hostname
                self.host_info['serial'] = data['serial_number']
                data['hostname'] = self.host_info['hostname']

                #logging.debug(f"Data: {type(data)} - {data[key]}")
                #logging.debug(f"Data: {type(data)} - {data}")
                logging.debug(f"key: {key} - {mlx5_inter}")
                try:
                    data[key]['mlx5_interface'] = f"{mlx5_inter}"
                    data[key]['ip_address'] = self.address
                except:
                    logging.error(f"Error processing data key: {key}")
                    continue

                df = self.process_mlxlink_info(data[key], mlx5_inter)
                
                # Check to see if the link has flapped
                if data['mst_status'][key] in data['link_flaps']:
                    link_key = data['mst_status'][key]
                    logging.debug(f"Key: {link_key}, Data: {data['link_flaps'][link_key]}")
                    # Map the link key to the correct interface by using the rdma link keys that map to the mst status keys
                    logging.debug(f"Link flap detected: {link_key}")
                    logging.debug(f"Flap count: {data['link_flaps'][link_key]['flap_count']}")
                    logging.debug(f"Last flap time: {data['link_flaps'][link_key]['last_flap_time']}")
                    flap_count = data['link_flaps'][link_key]['flap_count']
                    last_flap_time = data['link_flaps'][link_key]['last_flap_time']
                    df.loc[df['mlx5_'] == str(mlx5_inter), 'flap_count'] = flap_count
                    df.loc[df['mlx5_'] == str(mlx5_inter), 'last_flap_time'] = last_flap_time

                # Append the dataframe to the main dataframe
                all_df = pd.concat([all_df, df], ignore_index=True)
            
        return all_df        

    def display_mlxlink_info_json(self):
        # Read the data from the JSON files
        if self.args.process_min_files:
            df = self.read_min_json_files()
        else:
            df = self.read_json_files()

        # Print the dataframe
        logging.debug(f"Dataframe: {df}")
        logging.debug(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")

        # Check the mlxlink info
        df = self.check_mlxlink_info(df)

        # Sort the dataframe by hostname and interface
        df = df.sort_values(
            by=["hostname", "mlx5_"],
            key=lambda x: np.argsort(index_natsorted(df["hostname"]))
        )

        # If args.full is not set, then only display a subset of the columns
        if not self.args.full:
            # Remove the FecBin columns
            df = df.drop(columns=['FecBin0', 'FecBin1', 'FecBin2', 'FecBin3', 'FecBin4', 'FecBin5', 'FecBin6', 'FecBin7', 'FecBin8', 'FecBin9', 'FecBin10', 'FecBin11', 'FecBin12', 'FecBin13', 'FecBin14', 'FecBin15'])

        # Remove any rows where mlx5_ is set to None
        df = df[df['mlx5_'].notna()]
        df = df[df['mlx5_'] != 'None']

        # Tabulate the df
        logging.debug(f"self.args.error: {self.args.error}")
        if self.args.error:
            logging.debug("Checking for errors")
            # Filter the dataframe
            fail_df = df[df['Status'].str.contains('Failed')]

            # Print the filtered dataframe if not empty
            if not fail_df.empty:
                logging.info(f"\n{tabulate(fail_df, headers='keys', tablefmt='simple_outline')}")
            else:
                logging.info(f"No errors found in the dataframe")
        else:
            logging.info(f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}")

        # Save the dataframe to a file
        if self.args.output_dir:
            if not os.path.exists(self.args.output_dir):
                os.makedirs(self.args.output_dir)
            os.chdir(self.args.output_dir)

        if self.args.file_format == 'csv':
            # Write the dataframe to a CSV file
            csv_filename = f'mlxlink_info_{self.args.address}_{self.get_date_stamp()}.csv'
            df.to_csv(csv_filename, index=False)
            logging.debug(f"Dataframe saved to {csv_filename}")
        elif self.args.file_format == 'json':
            # Write the dataframe to a JSON file
            json_filename = f'mlxlink_info_{self.args.address}_{self.get_date_stamp()}.json'
            df.to_json(json_filename, orient='records')
            logging.debug(f"Dataframe saved to {json_filename}")

        return df
    
    def convert_mst_status_to_standard_mlx5(self, interface):
        # Convert the mst status to standard mlx5 interface
        try:
            mlx5_interface = self.mst_mapping[self.args.shape][interface]
        except:
            print(f"Error converting mst status to standard mlx5 interface: {interface}")
            mlx5_interface = None

        logging.debug(f"Interface: {interface}, mlx5_interface: {mlx5_interface}")
        return mlx5_interface

if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Gather mlxlink info")

    # Define the custom type function before it's used
    def list_of_strings(arg):
        return arg.split(',')
    
    # Add the logging level argument
    parser.add_argument('-l', '--log', default='INFO', help='Set the logging level (default: %(default)s)')
    parser.add_argument('-e', '--error', action='store_true', help='Set the error reporting')
    parser.add_argument('-w', '--warning', action='store_true', help='Add warnings to the error reporting')
    parser.add_argument('--date_stamp', type=str, help='The data file to use')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output to the console (default: %(default)s)')
    parser.add_argument('-a', '--address', type=str, help='The ip address of the remote host')
    parser.add_argument('--ber_threshold', type=str, default='1e-7', help='specify the Raw Physical BER threshold')
    parser.add_argument('--eff_threshold', type=str, default='100000', help='specify the Effective Physical Error threshold')
    parser.add_argument('--file_format', type=str, default='json', help='specify the output file format: csv,json (default: %(default)s')
    parser.add_argument('--output_dir', type=str, help='specify the output dir name')
    parser.add_argument('--read_json_files', action='store_true', help='Load json files')
    parser.add_argument('--flap_duration_threshold', type=int, help='specify the flap duration threshold in seconds')
    parser.add_argument('--mlx_interfaces', type=list_of_strings, default="0,1,3,4,5,6,7,8,9,10,12,13,14,15,16,17", help='specify the mlx interfaces to check %(default)s')
    parser.add_argument('--process_min_files', type=str, help='specify the the directory where the mlxlink_info_min files are located: "CWD" or "path to the results dir"')
    parser.add_argument('--rdma_prefix', type=str, default='rdma', help='specify the rdma prefix (default: %(default)s)')
    parser.add_argument('-s', '--shape', type=str, default='H100', help='specify the compute shape. (A100, H100) (default: %(default)s)')
    parser.add_argument('-f', '--full', action='store_true', help='Enable full output')

    # Parse the arguments
    args = parser.parse_args()

    # Set the log level to one of the following: DEBUG, INFO, WARNING, ERROR, CRITICAL
    logging.getLogger().setLevel(args.log.upper())

    # print the arguments
    logging.debug(f"Arguments: {args}")

    # Create the MlxlinkInfo object
    mlxlink_info = MlxlinkInfo(args)

    if args.read_json_files:
        mlxlink_info.display_mlxlink_info_json()
        sys.exit(0)

    if args.process_min_files:
        mlxlink_info.display_mlxlink_info_json()
        sys.exit(0)

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

    # Remove any rows where mlx5_ is set to None
    df = df[df['mlx5_'].notna()]
    df = df[df['mlx5_'] != 'None']

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

    # Save the dataframe to a file
    if args.output_dir:
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        os.chdir(args.output_dir)
    
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

