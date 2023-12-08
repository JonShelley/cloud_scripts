#!/usr/bin/env python3

import os
import sys
import time
import datetime
import logging
import re
import argparse
import socket

# expect to not have any RDMA link flaps within a given time interval (in hours)
RDMA_FLAPPING_LINK_TEST = "RDMA link failures detected"
INTERFACES = ["enp12s0f0", "enp12s0f1", "enp42s0f0", "enp42s0f1", "enp65s0f0", "enp65s0f1", "enp88s0f0", "enp88s0f1", "enp134s0f0", "enp134s0f1", "enp165s0f0", "enp165s0f1", "enp189s0f0", "enp189s0f1", "enp213s0f0", "enp213s0f1"]


def parse_args():
    parser = argparse.ArgumentParser(description="Process RDMA link flapping data")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level")
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

def die(exit_code, message):
    logging.error(message)
    sys.exit(exit_code)

def get_rdma_link_failures(log_file):

    pattern = r"(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+\S+\s+wpa_supplicant(?:\[\d+\])?: (\w+): CTRL-EVENT-EAP-FAILURE EAP authentication failed"

    link_data = {}
    with open(log_file, "r") as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                time_str = match.group(1)
                interface = match.group(2)
                logging.debug(f"time: {time_str}, interface: {interface}")
                if interface not in link_data:
                    link_data[interface] = {"failures": [time_str]}
                else:
                    link_data[interface]["failures"].append(time_str)
    logging.info("Link Data: {}".format(link_data))
    #logging.info("Link Data: {}".format(link_data.keys()))
    return link_data

def process_rdma_link_flapping(link_data, time_interval_hours):
    status = 0
    if len(link_data) >= 1:
        for interface in link_data:
            logging.info(f"{len(link_data[interface]['failures'])} RDMA link failure entries in /var/log/messages for interface: {interface}")
            
            last_date_str = link_data[interface]["failures"][-1]
            current_date = datetime.datetime.now()
            current_date_str = current_date.strftime("%b %d %H:%M:%S")

            last_date_sec = int(time.mktime(datetime.datetime.strptime(last_date_str, "%b %d %H:%M:%S").timetuple()))
            current_date_sec = int(time.mktime(datetime.datetime.strptime(current_date_str, "%b %d %H:%M:%S").timetuple()))
            
            if last_date_str != current_date_str:
                diff_secs = current_date_sec - last_date_sec
                logging.info(f"RDMA link ({interface}) failed {diff_secs} seconds ago")
                diff_hours = diff_secs // (60 * 60)
                logging.info(f"RDMA link ({interface}) failed  {diff_hours} hours ago")

                if diff_hours < time_interval_hours:
                    logging.error(f"{RDMA_FLAPPING_LINK_TEST}, multiple RDMA link flapping events within {time_interval_hours} hours ({current_date_str}, {last_date_str})")
                    status = -1
        if status == -1:
            die(-1, f"{process_rdma_link_flapping.__name__}: {RDMA_FLAPPING_LINK_TEST}, multiple RDMA link flapping events within the past {time_interval_hours} hours")
    else:
        logging.info("No RDMA link failures entry in /var/log/messages")
        return 0


if __name__ == "__main__":
    args = parse_args()
    setup_logging(args.log_level)
    auth_failure_file = "/tmp/last_auth_failure_date"
    msg_file = "/var/log/messages"
    time_interval_hours = 12 
    link_data = get_rdma_link_failures(msg_file)
    process_rdma_link_flapping(link_data, time_interval_hours)