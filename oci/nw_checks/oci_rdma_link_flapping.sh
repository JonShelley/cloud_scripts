#!/usr/bin/env python3

import os
import sys
import time
import datetime
import logging
import re
import argparse
import socket

def parse_args():
    parser = argparse.ArgumentParser(description="Process RDMA link flapping data")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="ERROR", help="Set the logging level")
    return parser.parse_args()

def setup_logging(log_level):
#    filename = 'oci_rdma_link_flapping.log'
#    if os.path.exists(filename):
#        os.remove(filename)
#    open(filename, 'w').close()
#    logging.basicConfig(filename=filename, level=log_level, format='%(asctime)s %(levelname)s %(message)s')
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s %(message)s')

    hostname = socket.gethostname()
    logging.info(f"Hostname: {hostname}")


def get_rdma_link_failures(log_file):

    pattern  = r"(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+\S+\s+wpa_supplicant(?:\[\d+\])?: (\w+): CTRL-EVENT-EAP-FAILURE EAP authentication failed"
    pattern2 = r"(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+\S+\s+kernel: (?:\[\d+\.\d+\]\s)?mlx5_core \S+ (\w+): Link down"
    
    link_data = {}
    with open(log_file, "r") as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                time_str = match.group(1)
                interface = match.group(2)
                logging.debug(f"time: {time_str}, interface: {interface}")
                if interface not in link_data:
                    link_data[interface] = {"failures": [time_str], "link_down": []}
                else:
                    link_data[interface]["failures"].append(time_str)

            
            match = re.search(pattern2, line)
            if match:
                time_str = match.group(1)
                interface = match.group(2)
                logging.debug(f"time: {time_str}, interface: {interface}")
                if interface not in link_data:
                    link_data[interface] = {"failures": [], "link_down": [time_str]}
                else:
                    link_data[interface]["link_down"].append(time_str)
                    
    logging.info("Link Data: {}".format(link_data))
    return link_data

def process_rdma_link_flapping(link_data, time_interval_hours):
    status = 0
    if len(link_data) >= 1:
        current_date = datetime.datetime.now()
        current_date_str = current_date.strftime("%b %d %H:%M:%S")
        current_date_sec = int(time.mktime(datetime.datetime.strptime(current_date_str, "%b %d %H:%M:%S").timetuple()))
        
        link_failures = False
        for interface in link_data:
            if len(link_data[interface]["failures"]) > 0:
                link_failures = True
                logging.error(f"{interface}: {len(link_data[interface]['failures'])} RDMA link failure entries in messages or syslog")        
            last_date_failure_str = None

            if len(link_data[interface]["failures"]) > 0:
                last_date_failure_str = link_data[interface]["failures"][-1]
                last_date_failure_sec = int(time.mktime(datetime.datetime.strptime(last_date_failure_str, "%b %d %H:%M:%S").timetuple()))
            
            if last_date_failure_str != None and last_date_failure_str != current_date_str:
                diff_secs = current_date_sec - last_date_failure_sec
                diff_hours = diff_secs // (60 * 60)
                logging.info(f"RDMA link ({interface}) failed  {diff_hours} hours ago")

                if diff_hours < time_interval_hours:
                    logging.error(f"{interface}: one or more RDMA link flapping events within {time_interval_hours} hours ({current_date_str}, {last_date_failure_str})")
                    status = -1
        if link_failures:
            logging.error("########################################")
        for interface in link_data:
            if len(link_data[interface]["link_down"]) > 0:
                logging.error(f"{interface}: {len(link_data[interface]['failures'])} RDMA link down entries in messages or syslog")
            last_date_down_str = None

            if len(link_data[interface]["link_down"]) > 0:
                    last_date_down_str = link_data[interface]["link_down"][-1]
                    last_date_down_sec = int(time.mktime(datetime.datetime.strptime(last_date_down_str, "%b %d %H:%M:%S").timetuple()))


            if last_date_down_str != None and last_date_down_str != current_date_str:
                diff_secs = current_date_sec - last_date_down_sec
                diff_hours = diff_secs // (60 * 60)
                logging.info(f"RDMA link ({interface}) down  {diff_hours} hours ago")

                if diff_hours < time_interval_hours:
                    logging.error(f"{interface}, one or more RDMA link down events within {time_interval_hours} hours ({current_date_str}, {last_date_down_str})")
                    status = -2
        if status == -1:
            logging.error(f"{process_rdma_link_flapping.__name__}: one or more RDMA link flapping events within the past {time_interval_hours} hours")
        if status == -2:
            logging.error(f"{process_rdma_link_flapping.__name__}: one or more RDMA link down events within the past {time_interval_hours} hours")
        if status < 0:
            sys.exit(-1)    
    else:
        logging.info("No RDMA link failures entry in /var/log/messages")
        return 0


if __name__ == "__main__":
    args = parse_args()
    setup_logging(args.log_level)
    auth_failure_file = "/tmp/last_auth_failure_date"
    msg_file = "/var/log/messages"
    if not os.path.exists(msg_file):
        msg_file = "/var/log/syslog"
    time_interval_hours = 6
    link_data = get_rdma_link_failures(msg_file)
    process_rdma_link_flapping(link_data, time_interval_hours)