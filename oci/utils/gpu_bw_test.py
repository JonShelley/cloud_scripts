#!/usr/bin/env python3

import argparse
import subprocess
import os
import json
import logging
import socket
import time


bw_test_exe = "bandwidthTest"


parser = argparse.ArgumentParser(description='Run GPU bandwidth test')
parser.add_argument('--log-level', dest='log_level', default='NONE', help='Logging level (default: INFO)')
parser.add_argument('-g', dest='gpus', default='NONE', help='Number of GPUs on the node Ex. -g 8')
parser.add_argument('-i', dest='iterations', default='NONE', help='Number of iterations to run Ex. -i 3')
parser.add_argument('-n', dest='numa_nodes', default='NONE', help='Number of numa nodes Ex. -n 2')
parser.add_argument('-s', dest='size', default='NONE', type=str, help='Size value Ex. -s 32000000')
args = parser.parse_args()

# If running on A100 systems in OCI change numas to 8.
numas = 2
gpus = 8
iterations = 1
size = "32000000"

if args.log_level != 'NONE':
    logging.basicConfig(level=args.log_level, format='%(asctime)s - %(levelname)s - %(message)s')
if args.gpus != 'NONE':
    gpus = int(args.gpus)
if args.numa_nodes != 'NONE':
    numas = int(args.numa_nodes)
if args.iterations != 'NONE':
    iterations = int(args.iterations)
if args.size != 'NONE':
    size = args.size

# Calculate the numbers of GPUs in a numa domain
gpus_per_numa = gpus // numas

logging.debug("Iteration: Device: DtoH : HtoD")
hostname = socket.gethostname()
results = {"gpus": {}, "host": hostname}
for i in range(iterations):
    for device in range(gpus):
        os.environ["CUDA_VISIBLE_DEVICES"] = str(device)
        logging.debug("ENV: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))
        result = subprocess.run(["numactl", "-N" + str(device // gpus_per_numa), "-m" + str(device // gpus_per_numa), bw_test_exe, "-dtoh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        logging.debug("Output: {}".format(result.stdout))
        #if result.stdout.find("cudaGetDeviceProperties") == -1 and result.stdout.find("cudaErrorSystemNotReady") == -1:
        if result.stdout.find(size) != -1:
            result = result.stdout.split("\n")
            tmp = [x for x in result if size in x]
            tmp = tmp[0].split()
            dtoh = tmp[1]

            result = subprocess.run(["numactl", "-N" + str(device // gpus_per_numa), "-m" + str(device // gpus_per_numa), bw_test_exe, "-htod"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            result = result.stdout.split("\n")
            #print("{} {}".format(type(size), size))
            tmp = [x for x in result if size in x]
            tmp = tmp[0].split()
            htod = tmp[1]
        else:
            dtoh = -1.0
            htod = -1.0

        if device not in results["gpus"]:
            results["gpus"][device] = {"dtoh": [dtoh], "htod": [htod]}
        else:
            results["gpus"][device]["dtoh"].append(dtoh)
            results["gpus"][device]["htod"].append(htod)

        logging.debug(str(i) + " : " +str(device) + " : " + str(dtoh) + " : " + str(htod))
    # Sleep for 5 seconds and rerun
    time.sleep(5)
    
print(json.dumps(results))
