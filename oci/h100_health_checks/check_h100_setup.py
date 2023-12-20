#!/usr/bin/env python3

import subprocess
import re
import argparse
from datetime import datetime
from shared_logging import logger
from gpu_bw_test import BandwidthTest
from rdma_link_flapping import LinkFlappingTest
from xid_checker import XidChecker
import platform


def get_oca_version():
    # Run the shell command
    os_name = platform.system()

    
    if os_name == 'Linux':
        try:
            distro = platform.linux_distribution()[0]
        except:
            import distro
            distro = distro.name()

        if 'Ubuntu' in distro:
            result = subprocess.run(['sudo', 'snap', 'info', 'oracle-cloud-agent'], stdout=subprocess.PIPE)
            
            # Decode the output from bytes to string
            output = result.stdout.decode('utf-8')

            # Define the regular expression pattern for the version
            pattern = r'installed:\s+(\d+\.\d+\.\d+)'
            match = re.search(pattern, output)
            if match:
                version = match.group(1)

        elif 'Oracle' in distro:
            result = subprocess.run(['rpm', '-qa'], stdout=subprocess.PIPE)
        
            # Decode the output from bytes to string
            output = result.stdout.decode('utf-8')

            # Define the regular expression pattern for the version
            pattern = r'oracle-cloud-agent-(\d+\.\d+\.\d+)'
            match = re.search(pattern, output)
            if match:
                version = match.group(1)

   
        if version < "1.37.2":
            logger.error(f"Oracle Cloud Agent: {version} needs to be updated to 1.37.2 or higher")
        else:
            logger.info(f"Oracle Cloud Agent: {version}")

        # Return the version
        return version

def check_rttcc_status():
    link_status = []
    devices = ["mlx5_0", "mlx5_1", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_12", "mlx5_13", "mlx5_14", "mlx5_15", "mlx5_16", "mlx5_17"]
    status = "disabled"
    status_dict = {"devices": {}}
    for device in devices:
        command = ['sudo', 'mlxreg', '-d', device, '-y', '--set', 'cmd_type=3', '--reg_name=PPCC', '--indexes=local_port=1,pnat=0,lp_msb=0,algo_slot=0,algo_param_index=0']
        result = subprocess.run(command, stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')
        filtered_output = [line for line in output.split('\n') if line.startswith('value')]
        for line in filtered_output:
            logger.debug(line)
            if "0x00000001" in line:
                status_dict["devices"][device] = "enabled"
    
    for device in status_dict["devices"]:
        if status_dict["devices"][device] == "enabled":
            logger.warning(f"RTTCC enabled on {device}")
            status = "enabled"
            link_status.append(f"RTTCC enabled on: {device}")
        else:
            logger.info(f"RTTCC status for {device}: disabled")
    if status == "disabled":
        logger.info(f"RTTCC disabled check: Passed")
    else:
        logger.error(f"RTTCC disabled check: Failed")

    return link_status

def check_ecc_errors():
    ecc_issues = []
    try:
        # Run the nvidia-smi -q command
        result = subprocess.run(['nvidia-smi', '-q'], stdout=subprocess.PIPE)
    except FileNotFoundError:
        logger.warning("Skipping SRAM/DRAM ECC Test: nvidia-smi command not found")
        return ["Skipped SRAM/DRAM ECC Test: nvidia-smi command not found"]
    
    # Decode the output from bytes to string
    output = result.stdout.decode('utf-8')

    # Find the lines containing "SRAM Correctable" and "DRAM Correctable"
    sram_matches = re.findall(r'SRAM Uncorrectable\s+:\s+(\d+)', output)
    dram_matches = re.findall(r'DRAM Uncorrectable\s+:\s+(\d+)', output)
    gpu_matches = re.findall(r'\nGPU\s+(.*)\n', output)
    vol_sram_line = sram_matches[0::2]
    vol_dram_line = dram_matches[0::2]
    agg_sram_line = sram_matches[1::2]
    agg_dram_line = dram_matches[1::2]

    for i, gpu in enumerate(gpu_matches):
        logger.debug(f"GPU: {gpu}")
        if vol_sram_line[i] != "0":
            logger.debug(f"Volatile SRAM Uncorrectable: {vol_sram_line[i]}")
            ecc_issues.append(f"{gpu_matches[i]} - Volatile SRAM Uncorrectable: {vol_sram_line[i]}")
        if vol_dram_line[i] != "0":
            logger.debug(f"Volatile DRAM Uncorrectable: {vol_dram_line[i]}")
            ecc_issues.append(f"{gpu_matches[i]} - Volatile DRAM Uncorrectable: {vol_dram_line[i]}")
        if agg_sram_line[i] != "0":
            logger.debug(f"Aggregate SRAM Uncorrectable: {agg_sram_line[i]}")
            ecc_issues.append(f"{gpu_matches[i]} - Aggregate SRAM Uncorrectable: {agg_sram_line[i]}")
        if agg_dram_line[i] != "0":
            logger.debug(f"Aggregate DRAM Uncorrectable: {agg_dram_line[i]}")
            ecc_issues.append(f"{gpu_matches[i]} - Aggregate DRAM Uncorrectable: {agg_dram_line[i]}")


    # Check if the extracted values are equal to "0000000000000000" and log the appropriate message
    if len(ecc_issues) == 0:
        logger.info("GPU ECC Test: Passed")
    else:
        logger.error("GPU ECC Test: Failed")
    
    return ecc_issues

def check_rdma_link_status():
    status = True
    devices = ["mlx5_0", "mlx5_1", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_12", "mlx5_13", "mlx5_14", "mlx5_15", "mlx5_16", "mlx5_17"]
    
    link_issues = []
    for device in devices:
        # Run the mlxlink command
        command = ['sudo', 'mlxlink', '-d', device, '-m', '-c', '-e']
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Decode the output from bytes to string
        output = result.stdout.decode('utf-8')
        stderr = result.stderr.decode('utf-8')

        if stderr and stderr.find("-E-") != -1:
            stderr = stderr.split("\n")
            stderr_line = ", ".join(stderr)
            logger.debug(f"{device}: {stderr_line}")
            link_issues.append(f"{device}: {stderr[0]}")
            status = "False"
            continue

        # Find the line containing "Recommendation"
        recommendation_line = re.search(r'Recommendation.*', output)
        vendor_serial_num_line = re.search(r'Vendor Serial Number.*', output)
        vendor_serial_num = vendor_serial_num_line.group().split(":")[1].strip()
        logger.info(f"{device}: {vendor_serial_num}")

        # Extract the part after the ":" and print it along with the device name
        if recommendation_line:
            recommendation = recommendation_line.group().split(":")[1].strip()
            pattern = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
            recommendation = re.sub(pattern, '', recommendation)
            if recommendation != "No issue was observed":
                logger.debug(f"{device}: {recommendation}")
                link_issues.append(f"{device}: {recommendation}")
                status = False
            else:
                logger.debug(f"{device}: {recommendation}")

    if status:
        logger.info(f"RDMA Link Status Check: Passed")
    else:
        logger.warning(f"RDMA Link Status Check: Failed")
    return link_issues

def get_host_serial():
    # Run the shell command
    result = subprocess.run(['sudo', 'dmidecode', '-s', 'system-serial-number'], stdout=subprocess.PIPE)

    # Decode the output from bytes to string
    output = result.stdout.decode('utf-8')

    # Return the serial number
    return output.strip()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check H100 setup')
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Set the logging level default: INFO")
    parser.add_argument('--bw-test', dest='bw_test', action='store_true', default=False, help='Run GPU bandwidth test (default: False)')
    parser.add_argument('--bw-test-exe', dest='bw_test_exe', help='Location to cuda-sampels bandwidthTest')
    parser.add_argument('--lf-interval', dest='lf_interval', default=6, type=int, help='Link flapping interval with no flapping or link down events (default: 6 (hours))')
    parser.add_argument('-a','--all', dest='run_all', action='store_true', default=False, help='Run all checks (default: False)')
    args = parser.parse_args()

    logger.setLevel(args.log_level)

    datetime_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    logger.info(f"Started H100 setup check at: {datetime_str}")
    oca_version = get_oca_version()
    rttcc_issues = check_rttcc_status()
    ecc_issues = check_ecc_errors()
    rdma_link_issues = check_rdma_link_status()
    
    # Check for RDMA link flapping
    lft = LinkFlappingTest(time_interval=args.lf_interval)
    lft.get_rdma_link_failures()
    lft_issues = lft.process_rdma_link_flapping()

    # Check for GPU Xid errors
    xc = XidChecker()
    xid_results = xc.check_gpu_xid()

    # Check GPU bandwidth
    bwt_results = None
    if args.bw_test == True or args.run_all == True:
        if args.bw_test_exe:
            bwt = BandwidthTest(bw_test_exe=args.bw_test_exe)
        else:
            bwt = BandwidthTest()
        bwt.measure_gpu_bw()
        bwt_results = bwt.validate_results()

    # Summarize the results
    host_serial = get_host_serial()
    logger.info(f"--------- Summary of H100 setup check for {host_serial} ---------")
    if oca_version < "1.37.2":
        logger.error(f"Oracle Cloud Agent: {oca_version} needs to be updated to 1.37.2 or higher")
    if len(rttcc_issues) > 0:
        logger.error(f"RTTCC issues: {rttcc_issues}")
    if len(ecc_issues) > 0:
        for issue in ecc_issues:
            if "Skipped" in issue:
                logger.warning(f"{host_serial} - {issue}")
            else:
                if "Aggregate" in issue:
                    logger.warning(f"{host_serial} - ECC issues: {issue}")
                else:
                    logger.error(f"{host_serial} - ECC issues: {issue}")
    if xid_results["status"] == "Failed":
        for xid in xid_results["results"]:
            for pci in xid_results["results"][xid]["results"]:
                logger.error(f"{host_serial} - GPU Xid {xid} device: {pci}, {xid_results['results'][xid]['description']}")
    if len(rdma_link_issues) > 0:
        for issue in rdma_link_issues:
            logger.error(f"{host_serial} - RDMA link issues: {issue}")
    if len(lft_issues["failures"]) > 0 or len(lft_issues["link_down"]) > 0:
        if len(lft_issues["failures"]) > 0:
            for issue in lft_issues["failures"]:
                logger.error(f"{host_serial} - RDMA link flapping issues: {issue}")
        if len(lft_issues["link_down"]) > 0:
            for issue in lft_issues["link_down"]:
                logger.error(f"{host_serial} - RDMA link down issues: {issue}")
    if bwt_results != None:
        if bwt_results["status"] == "Failed":
            for issue in bwt_results["issues"]:
                logger.error(f"{host_serial} - GPU bandwidth issues: {issue}")
    datetime_str = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    logger.info(f"Finished H100 setup check at: {datetime_str}")
    
