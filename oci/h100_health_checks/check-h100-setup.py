i#!/usr/bin/env python3

import subprocess
import re
import argparse
from datetime import datetime
from shared_logging import logger
from gpu_bw_test import BandwidthTest
from rdma_link_flapping import LinkFlappingTest
from xid_checker import XidChecker


def get_oca_version():
    # Run the shell command
    result = subprocess.run(['rpm', '-qa'], stdout=subprocess.PIPE)

    # Decode the output from bytes to string
    output = result.stdout.decode('utf-8')

    # Filter the output for lines containing 'oracle-cloud-agent'
    filtered_output = [line for line in output.split('\n') if 'oracle-cloud-agent' in line]

    # Define the regular expression pattern for the version
    pattern = r'\d+\.\d+\.\d+'

    # Log the filtered output and capture the version
    for line in filtered_output:
        logger.info(line)
        match = re.search(pattern, line)
        if match:
            version = match.group()
            if version < "1.37.2":
                logger.error(f"Oracle Cloud Agent: {version} needs to be updated to 1.37.2 or higher")
            else:
                logger.info(f"Oracle Cloud Agent: {version}")

    # Return the version
    return version

def check_rttcc_status():
    devices = ["mlx5_0", "mlx5_1", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_12", "mlx5_13", "mlx5_14", "mlx5_15", "mlx5_16", "mlx5_17"]
    status = "disabled"
    for device in devices:
        command = ['sudo', 'mlxreg', '-d', device, '-y', '--set', 'cmd_type=3', '--reg_name=PPCC', '--indexes=local_port=1,pnat=0,lp_msb=0,algo_slot=0,algo_param_index=0']
        result = subprocess.run(command, stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        filtered_output = [line for line in output.split('\n') if line.startswith('value')]
        for line in filtered_output:
            logger.debug(line)
            if "0x00000001" in line:
                status = "enabled"
    
    if status == "enabled":
        logger.error(f"RTTCC status: {status}")
    return status

def check_ecc_errors():
    # Run the nvidia-smi -q command
    result = subprocess.run(['nvidia-smi', '-q'], stdout=subprocess.PIPE)

    # Decode the output from bytes to string
    output = result.stdout.decode('utf-8')

    # Find the lines containing "SRAM Correctable" and "DRAM Correctable"
    sram_line = re.search(r'SRAM Correctable.*', output)
    dram_line = re.search(r'DRAM Correctable.*', output)

    # Extract the fourth field from these lines and remove any whitespace
    sram_errors = sram_line.group().split()[3].strip() if sram_line else None
    dram_errors = dram_line.group().split()[3].strip() if dram_line else None

    # Check if the extracted values are equal to "0000000000000000" and log the appropriate message
    if sram_errors == "0000000000000000" or sram_errors == "0":
        logger.info("SRAM ECC Test: Passed")
    else:
        logger.error("SRAM ECC Test: Failed - {sram_errors}")

    if dram_errors == "0000000000000000" or dram_errors == "0":
        logger.info("DRAM ECC Test: Passed")
    else:
        logger.error("DRAM ECC Test: Failed - {dram_errors}")

def check_rdma_link_status():
    status = "True"
    devices = ["mlx5_0", "mlx5_1", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_12", "mlx5_13", "mlx5_14", "mlx5_15", "mlx5_16", "mlx5_17"]

    for device in devices:
        # Run the mlxlink command
        command = ['sudo', 'mlxlink', '-d', device, '-m', '-c', '-e']
        result = subprocess.run(command, stdout=subprocess.PIPE)

        # Decode the output from bytes to string
        output = result.stdout.decode('utf-8')

        # Find the line containing "Recommendation"
        recommendation_line = re.search(r'Recommendation.*', output)

        # Extract the part after the ":" and print it along with the device name
        if recommendation_line:
            recommendation = recommendation_line.group().split(":")[1].strip()
            pattern = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
            recommendation = re.sub(pattern, '', recommendation)
            if recommendation != "No issue was observed":
                logger.error(f"{device}: {recommendation}")
                status = "False"
            else:
                logger.debug(f"{device}: {recommendation}")

    if status:
        logger.info(f"RDMA Link Status Check: Passed")
    return status

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check H100 setup')
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level default: INFO")
    parser.add_argument('--bw-test', dest='bw_test', default=False, help='Run GPU bandwidth test (default: False)')
    parser.add_argument('--lf-interval', dest='lf_interval', default=6, help='Link flapping interval with no flapping or link down events (default: 6 (hours))')
    parser.add_argument('-a','--all', dest='run_all', action='store_true', default=False, help='Run all checks (default: False)')
    args = parser.parse_args()

    logger.setLevel(args.log_level)

    datetime_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    logger.info(f"Started H100 setup check at: {datetime_str}")
    oca_version = get_oca_version()
    status = check_rttcc_status()
    check_ecc_errors()
    check_rdma_link_status()
    
    # Check for RDMA link flapping
    lft = LinkFlappingTest(time_interval=args.lf_interval)
    lft.get_rdma_link_failures()
    lft.process_rdma_link_flapping()

    # Check for GPU Xid errors
    xc = XidChecker()
    results = xc.check_gpu_xid()

    # Check GPU bandwidth
    if args.bw_test == True or args.run_all == True:
        bwt = BandwidthTest()
        bwt.measure_gpu_bw()
        bwt.validate_results()

    datetime_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    logger.info(f"Finished H100 setup check at: {datetime_str}")
    
