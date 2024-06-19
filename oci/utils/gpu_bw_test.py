#!/usr/bin/env python3

import argparse
import subprocess
import os
import socket
import time
import json
from shared_logging import logger


class BandwidthTest:
    def __init__(self, iteration=1, size=32000000, bw_test_exe="/opt/oci-hpc/cuda-samples/bin/x86_64/linux/release/bandwidthTest"):
        self.iteration = iteration
        self.size = size
        self.bw_test_exe = bw_test_exe
        self.results = None
        self.dtoh_threshold = 52.0
        self.htod_threshold = 52.0

    def get_numa_nodes(self):
        result = subprocess.run(['numactl', '-H'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        filtered_output = [line for line in output.split('\n') if line.startswith('available:')]
        return int(filtered_output[0].split()[1].strip())

    def get_gpus(self):
        result = subprocess.run(['nvidia-smi', '-L'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        filtered_output = [line for line in output.split('\n') if line.startswith('GPU')]
        return len(filtered_output)

    def measure_gpu_bw(self):
        numas = 2
        gpus = 8
        iterations = 1
        size = "32000000"

        gpus = self.get_gpus()
        numas = self.get_numa_nodes()
        gpus_per_numa = gpus // numas

        logger.debug("GPUs: {}".format(gpus))
        logger.debug("NUMAs: {}".format(numas))
        logger.debug("GPUs per NUMA: {}".format(gpus_per_numa))

        logger.debug("Iteration: Device: DtoH : HtoD")
        hostname = socket.gethostname()
        results = {"gpus": {}, "host": hostname}
        for i in range(iterations):
            for device in range(gpus):
                os.environ["CUDA_VISIBLE_DEVICES"] = str(device)
                logger.debug("ENV: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))
                logger.debug("Iteration: {} Device: {} gpus_per_numa: {}".format(i, device, gpus_per_numa))
                logger.debug("CMD: {}".format(["numactl", "-N" + str(device // gpus_per_numa), "-m" + str(device // gpus_per_numa), self.bw_test_exe, "-dtoh"]))
                result = subprocess.run(["numactl", "-N" + str(device // gpus_per_numa), "-m" + str(device // gpus_per_numa), self.bw_test_exe, "-dtoh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                logger.debug("Output: {}".format(result.stdout))
                logger.debug("Error: {}".format(result.stderr))
                if result.stdout.find(size) != -1:
                    result = result.stdout.split("\n")
                    tmp = [x for x in result if size in x]
                    tmp = tmp[0].split()
                    dtoh = float(tmp[1])

                    result = subprocess.run(["numactl", "-N" + str(device // gpus_per_numa), "-m" + str(device // gpus_per_numa), self.bw_test_exe, "-htod"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                    result = result.stdout.split("\n")
                    tmp = [x for x in result if size in x]
                    tmp = tmp[0].split()
                    htod = float(tmp[1])
                else:
                    dtoh = -1.0
                    htod = -1.0

                if device not in results["gpus"]:
                    results["gpus"][device] = {"dtoh": [dtoh], "htod": [htod]}
                else:
                    results["gpus"][device]["dtoh"].append(dtoh)
                    results["gpus"][device]["htod"].append(htod)

            logger.debug(str(i) + " : " +str(device) + " : " + str(dtoh) + " : " + str(htod))
        
            if i > 1 and i != iterations - 1:
                 # Sleep for 5 seconds and rerun
                 time.sleep(5)
        
        logger.debug(json.dumps(results))
        self.results = results

    def validate_results(self):
        status = True
        for device in self.results["gpus"]:
            dtoh = self.results["gpus"][device]["dtoh"]
            htod = self.results["gpus"][device]["htod"]
            dtoh_avg = sum(dtoh) / len(dtoh)
            htod_avg = sum(htod) / len(htod)
            logger.debug("Device: {} DtoH: {} HtoD: {}".format(device, dtoh_avg, htod_avg))
            if dtoh_avg < self.dtoh_threshold:
                logger.error("Device: {} DtoH: {} is below threshold: {}".format(device, dtoh_avg, self.dtoh_threshold))
                status = False
            if htod_avg < self.htod_threshold:
                logger.error("Device: {} HtoD: {} is below threshold: {}".format(device, htod_avg, self.htod_threshold))
                status = False
        if status == True:
            logger.info("GPU bandwidth test passed")
        return status
            

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run GPU bandwidth test')
    parser.add_argument('--log-level', dest='log_level', default='NONE', help='Logging level (default: INFO)')
    parser.add_argument('-i', dest='iterations', default='1', help='Number of iterations to run Ex. -i 3')
    parser.add_argument('-s', dest='size', default='32000000', help='Message size to run Ex. -s 32000000')
    parser.add_argument('--bw-test-exe', dest='bw_test_exe', default='/opt/oci-hpc/cuda-samples/bin/x86_64/linux/release/bandwidthTest', help='Path to the bw_test executable')
    args = parser.parse_args()

    logger.setLevel(args.log_level)
    if args.iterations != 'NONE':
        iterations = int(args.iterations)
    if args.size != 'NONE':
        size = args.size
    if args.bw_test_exe != 'NONE':
        bw_test_exe = args.bw_test_exe

    bwt = BandwidthTest(iteration=iterations, size=size, bw_test_exe=bw_test_exe)
    bwt.measure_gpu_bw()
    bwt.validate_results()