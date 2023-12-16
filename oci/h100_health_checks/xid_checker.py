#!/usr/bin/env python3

import argparse
from shared_logging import logger
import subprocess
import sys
import re

class XidChecker:
    def __init__(self, dmesg_cmd="dmesg", time_interval=60):
        self.dmesg_cmd = dmesg_cmd
        self.results = {}


        # Check for the following GPU Xid errors in dmesg
        self.XID_EC = {"48": {"description": "Double Bit ECC Error", "severity": "Critical"}, 
                "56": {"description": "Display Engine error", "severity": "Critical"},
                "57": {"description": "Error programming video memory interface", "severity": "Critical"},
                "58": {"description": "Unstable video memory interface detected", "severity": "Critical"},
                "62": {"description": "Internal micro-controller halt", "severity": "Critical"},
                "63": {"description": "ECC page retirement or row remapping recording event", "severity": "Critical"},
                "64": {"description": "ECC page retirement or row remapper recording failure", "severity": "Critical"},
                "65": {"description": "Video processor exception", "severity": "Critical"},
                "68": {"description": "NVDEC0 Exception", "severity": "Critical"},
                "69": {"description": "Graphics Engine class error", "severity": "Critical"},
                "73": {"description": "NVENC2 Error", "severity": "Critical"},
                "74": {"description": "NVLINK Error", "severity": "Critical"},
                "79": {"description": "GPU has fallen off the bus", "severity": "Critical"},
                "80": {"description": "Corrupted data sent to GPU", "severity": "Critical"},
                "81": {"description": "VGA Subsystem Error", "severity": "Critical"},
                "82": {"description": "NVJPGO Error", "severity": "Warn"},
                "83": {"description": "NVDEC1 Error", "severity": "Warn"},
                "84": {"description": "NVDEC2 Error", "severity": "Warn"},
                "86": {"description": "OFA Exception", "severity": "Warn"},
                "88": {"description": "NVDEC3 Error", "severity": "Warn"},
                "89": {"description": "NVDEC4 Error", "severity": "Warn"},
                "92": {"description": "High single-bit ECC error rate", "severity": "Critical"},
                "94": {"description": "Contained ECC error", "severity": "Critical"},
                "95": {"description": "Uncontained ECC error", "severity": "Critical"},
                "96": {"description": "NVDEC5 Error", "severity": "Warn"},
                "97": {"description": "NVDEC6 Error", "severity": "Warn"},
                "98": {"description": "NVDEC7 Error", "severity": "Warn"},
                "99": {"description": "NVJPG1 Error", "severity": "Warn"},
                "100": {"description": "NVJPG2 Error", "severity": "Warn"},
                "101": {"description": "NVJPG3 Error", "severity": "Warn"},
                "102": {"description": "NVJPG4 Error", "severity": "Warn"},
                "103": {"description": "NVJPG5 Error", "severity": "Warn"},
                "104": {"description": "NVJPG6 Error", "severity": "Warn"},
                "105": {"description": "NVJPG7 Error", "severity": "Warn"},
                "110": {"description": "Security Fault Error", "severity": "Warn"},
                "111": {"description": "Display Bundle Error Event", "severity": "Warn"},
                "112": {"description": "Display Supervisor Error", "severity": "Warn"},
                "113": {"description": "DP Link Training Error", "severity": "Warn"},
                "114": {"description": "Display Pipeline Underflow Error", "severity": "Warn"},
                "115": {"description": "Display Core Channel Error", "severity": "Warn"},
                "116": {"description": "Display Window Channel Error", "severity": "Warn"},
                "117": {"description": "Display Cursor Channel Error", "severity": "Warn"},
                "118": {"description": "Display Pixel Pipeline Error", "severity": "Warn"},
                "119": {"description": "GSP RPC Timeout", "severity": "Critical"},
                "120": {"description": "GSP Error", "severity": "Critical"},
                "122": {"description": "SPI PMU RPC Read Failure", "severity": "Warn"},
                "123": {"description": "SPI PMU RPC Write Failure", "severity": "Warn"},
                "124": {"description": "SPI PMU RPC Erase Failure", "severity": "Warn"},
                "125": {"description": "Inforom FS Failure", "severity": "Warn"}
                }

    def check_gpu_xid(self):
        status = "Pass"
        dmesg_output = subprocess.check_output([self.dmesg_cmd]).decode("utf-8")
        if "NVRM: Xid" in dmesg_output:
            for XID in self.XID_EC.keys():
                logger.debug(f"Checking for GPU Xid {XID} error in dmesg")
                
                matches = re.findall(f"NVRM: Xid \(PCI:(.*?): {XID},", dmesg_output)
                tmp_dict = {}
                for match in matches:
                    if match not in tmp_dict:
                        tmp_dict[match] = 1
                    else:
                        tmp_dict[match] = tmp_dict[match] + 1
                for x in tmp_dict.keys():
                    logger.info(f"{XID} : count: {tmp_dict[x]}, {self.XID_EC[XID]['description']} - PCI: {x}")
                if not matches:
                    logger.debug(f"No GPU Xid {XID} error found in dmesg")
                if tmp_dict != {}:
                    if XID_EC[XID]['severity'] == "Critical":
                        status = "Fail"
                    self.results[XID] = {"results": tmp_dict, "description": self.XID_EC[XID]['description']}
        else:
            logger.info("Xid Check: Passed")
        return {"status": status, "results": self.results}


if __name__ == '__main__':
    # Argument parsing
    parser = argparse.ArgumentParser(description='Check for GPU Xid errors.')
    parser.add_argument('--dmesg_cmd', default='dmesg', help='Dmesg file to check. Default is dmesg.')
    args = parser.parse_args()


    logger.debug(f"Using dmesg command: {args.dmesg_cmd}")
    
    xc = XidChecker(dmesg_cmd=args.dmesg_cmd)
    results = xc.check_gpu_xid()
    logger.debug("Status: {}, Results: {}".format(results["status"], results["results"]))
