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
from datetime import datetime, timedelta
from tabulate import tabulate
import sys
import os
import re
from glob import glob

import logging.config
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

logging.config.fileConfig('logging.conf')

logger = logging.getLogger('simpleExample')


class MlxlinkInfo:
    def __init__(self, args):
        if args.date_stamp:
            self.date_stamp = args.date_stamp
        else:
            self.date_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.address = args.address if args.address else None
        self.ber_threshold = args.ber_threshold
        self.eff_threshold = args.eff_threshold
        self.dataset_id = args.dataset_id if args.dataset_id else self.date_stamp
        self.prev_failures_file = args.prev_failures_file

        self.mlx5_interfaces = args.mlx_interfaces

        self.timeout = 60
        self.host_info = {"hostname": "Unknown", "serial": "Unknown"}
        self.flap_duration_threshold = (
            args.flap_duration_threshold if args.flap_duration_threshold else 3600 * 6
        )
        self.flap_startup_wait_time = 1800
        self.args = args

        self.mst_mapping = {
            "H100": {
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
                "0c:00.0": "mlx5_0",
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
                "93:00.1": "mlx5_17",
            },
            "GB200": {
                "0000:03:00.0": "mlx5_0",
                "0002:03:00.0": "mlx5_1",
                "0010:03:00.0": "mlx5_3",
                "0012:03:00.0": "mlx5_4",
                "0006:09:00.0": "mlx5_2",
                "0016:0b:00.0": "mlx5_5",
            },
            "GB200v3": {
                "0000:03:00.0": "mlx5_0",
                "0002:03:00.0": "mlx5_2",
                "0010:03:00.0": "mlx5_5",
                "0012:03:00.0": "mlx5_7",
                "0000:03:00.1": "mlx5_1",
                "0006:09:00.0": "mlx5_4",
                "0002:03:00.1": "mlx5_3",
                "0010:03:00.1": "mlx5_6",
                "0016:0b:00.0": "mlx5_9",
                "0012:03:00.1": "mlx5_8"
            },
            "GB300": {
                "0000:03:00.0": "mlx5_0",
                "0002:03:00.0": "mlx5_2",
                "0010:03:00.0": "mlx5_5",
                "0007:01:00.0": "mlx5_4",
                "0012:03:00.0": "mlx5_7",
                "0000:03:00.1": "mlx5_1",
                "0017:01:00.0": "mlx5_9",
                "0002:03:00.1": "mlx5_3",
                "0010:03:00.1": "mlx5_6",
                "0012:03:00.1": "mlx5_8"
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
                "d5:00.0": "mlx5_11",
            },
        }

        if not args.read_json_files:
            self._collect_host_info()
        else:
            logging.info("Reading JSON files")

    def check_for_flaps(self):

        if self.args.read_json_files:
            return {}

        cmd = "uptime -s"
        output = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        date_str = output.stdout.strip()
        uptime_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        cmd = "chroot /host rdma link show"
        output = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        if output.returncode != 0:
            cmd = "rdma link show"
            output = subprocess.run(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
            )
            if output.returncode != 0:
                return {}

        pattern = r"(mlx5_\d+)/\d+ state (\w+) physical_state (\w+) netdev (\w+)"
        rdma_dict = {}
        for line in output.stdout.split("\n"):
            match = re.search(pattern, line)
            if match:
                rdma_dict[match.group(4)] = match.group(1)

        cmd = "chroot /host dmesg -T| grep -E 'mlx5_'"
        output = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        if output.returncode != 0:
            cmd = "dmesg -T| grep -E 'mlx5_'"
            output = subprocess.run(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
            )
            if output.returncode != 0:
                return {}

        link_dict = {}
        for line in output.stdout.split("\n"):
            if "mlx5_" in line and "link down" in line.lower():
                pattern = (
                    r"\[(\w{3} \w{3} {1,2}\d{1,2} \d{2}:\d{2}:\d{2} \d{4})\].*(rdma\d+): Link (\w+)"
                )
                match = re.search(pattern, line)
                if match:
                    link_flap_time = datetime.strptime(
                        match.group(1), "%a %b %d %H:%M:%S %Y"
                    )
                    mlx_interface = match.group(2)
                    mlx_interface = rdma_dict.get(mlx_interface, mlx_interface)

                    if (datetime.now() - link_flap_time).total_seconds() < self.flap_duration_threshold:
                        if (link_flap_time - uptime_date).total_seconds() > self.flap_startup_wait_time:
                            if mlx_interface not in link_dict:
                                link_dict[mlx_interface] = {
                                    "last_flap_time": link_flap_time,
                                    "flap_count": 1,
                                }
                            else:
                                link_dict[mlx_interface]["flap_count"] += 1
                                link_dict[mlx_interface]["last_flap_time"] = link_flap_time

        return link_dict

    def _collect_host_info(self):
        try:
            cmd = "/usr/bin/which dmidecode"
            result = subprocess.run(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
            )
            if result.returncode != 0:
                cmd = "chroot /host dmidecode -s system-serial-number"
                result2 = subprocess.run(
                    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                )
                if result2.returncode != 0:
                    self.host_info["serial"] = "Unknown"
                else:
                    self.host_info["serial"] = result2.stdout.strip()
            else:
                if os.geteuid() == 0:
                    cmd = "dmidecode -s system-serial-number"
                else:
                    cmd = "sudo dmidecode -s system-serial-number"
                result = subprocess.run(
                    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
                )
                self.host_info["serial"] = result.stdout.strip()
        except Exception:
            self.host_info["serial"] = "Unknown"

        self.host_info["hostname"] = socket.gethostname()

    def get_host_info(self):
        return self.host_info

    def get_mlxlink_info(self, mlx5_inter, timeout):
        cmd = "mlxlink --version"
        output = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        if output.returncode != 0:
            cmd = "chroot /host mlxlink --version"
            output = subprocess.run(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
            )
            if output.returncode != 0:
                sys.exit(1)
            else:
                cmd = (
                    f"chroot /host mlxlink -d mlx5_{mlx5_inter} -m -e -c "
                    f"--rx_fec_histogram --show_histogram --json"
                )
        else:
            cmd = (
                f"sudo mlxlink -d mlx5_{mlx5_inter} -m -e -c "
                f"--rx_fec_histogram --show_histogram --json"
            )

        output = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )

        results_dir = "mlxlink_files"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)

        filename = f"{results_dir}/{self.host_info['hostname']}_mlx5_{mlx5_inter}.json"
        with open(filename, "w") as outfile:
            outfile.write(output.stdout)

        if output.returncode != 0:
            stderr_status = output.stderr.find("No such file or directory")
            if output.returncode == 1 and stderr_status != -1:
                data = {"status": {"code": output.returncode, "message": "Failed"}}
            else:
                data = json.loads(output.stdout)
        else:
            data = json.loads(output.stdout)

        data["mlx5_interface"] = f"{mlx5_inter}"
        data["ip_address"] = self.address
        data["hostname"] = self.host_info["hostname"]

        return data

    def get_date_stamp(self):
        return self.date_stamp

    def check_mlxlink_info(self, df):
        # Normalize dtypes for numeric comparisons
        try:
            df["flap_count"] = pd.to_numeric(df["flap_count"], errors="coerce").fillna(0).astype(int)
        except Exception:
            df["flap_count"] = 0
        try:
            df["EffPhyErrs"] = pd.to_numeric(df["EffPhyErrs"], errors="coerce").fillna(-1).astype(int)
        except Exception:
            pass
        try:
            df["RawPhyBER"] = pd.to_numeric(df["RawPhyBER"], errors="coerce")
        except Exception:
            pass

        df.loc[df["nic_fw_version"] < "28.39.2500", "Status"] = (
            "Warning - FW < 28.39.2500"
        )
        df.loc[
            df["Recommended"].str.contains("Bad signal integrity", case=False),
            "Status",
        ] = "Failed - Bad Signal Integrity"
        df.loc[df["RawPhyBER"] > float(self.ber_threshold), "Status"] = (
            f"Failed - RawPhyBER > {self.ber_threshold}"
        )
        df.loc[df["EffPhyErrs"] > int(self.eff_threshold), "Status"] = (
            f"Failed - EffPhyErrs > {self.eff_threshold}"
        )
        df.loc[df["flap_count"] > 0, "Status"] = "Failed - Link Flap Detected"
        df.loc[df["LinkState"] != "Active", "Status"] = "Failed - LinkState != Active"

        for i in range(16):
            df[f"FecBin{i}"] = df[f"FecBin{i}"].astype(int)
        for i in range(7, 16):
            if df[f"FecBin{i}"].gt(0).any():
                df.loc[df[f"FecBin{i}"] > 10000, "Status"] = f"Failed - FEC Bin{i} > 0"

        return df

    @staticmethod
    def classify_failure_reason(status: str) -> str:
        if not isinstance(status, str):
            return "Other"
        if "RawPhyBER" in status:
            return "RawPhyBER"
        if "FEC Bin" in status:
            return "FECBin"
        if "Bad Signal" in status:
            return "Bad Signal"
        if "EffPhyErrs" in status:
            return "EffPhyErrs"
        if "Link Flap" in status:
            return "LinkFlap"
        if "LinkState" in status:
            return "LinkState"
        if "FW <" in status:
            return "FW"
        if status.startswith("Warning"):
            return "Warning"
        return "Other"

    def summarize_failures(self, df, label_prefix=""):
        if "Status" not in df.columns:
            return pd.DataFrame()

        fail_df = df[df["Status"].str.startswith("Failed", na=False)].copy()
        total = len(fail_df)
        logging.info(f"{label_prefix}Failure Summary:")
        logging.info(f"Total: {total}")
        if total == 0:
            return fail_df

        fail_df["FailureReason"] = fail_df["Status"].apply(self.classify_failure_reason)

        for reason in ["RawPhyBER", "FECBin", "Bad Signal", "EffPhyErrs", "LinkFlap", "LinkState", "FW", "Other"]:
            count = int((fail_df["FailureReason"] == reason).sum())
            if count > 0:
                logging.info(f"* {count:4d} {reason}")

        # ICMD semaphore summary across all rows (not only failed)
        try:
            if "CMD_Status" in df.columns and "CMD_Status_msg" in df.columns:
                sem_mask = (pd.to_numeric(df["CMD_Status"], errors="coerce") == 1) & df["CMD_Status_msg"].astype(str).str.contains("ICMD semaphore", case=False, na=False)
                sem_count = int(sem_mask.sum())
                if sem_count > 0:
                    logging.info(f"* {sem_count:4d} ICMD semaphore")
                    # Save HostSerial and port (mlx5_) for these rows
                    out_dir = self.args.output_dir or "."
                    os.makedirs(out_dir, exist_ok=True)
                    sem_df = df[sem_mask].copy()
                    cols_to_save = [c for c in ["HostSerial", "mlx5_"] if c in sem_df.columns]
                    if cols_to_save:
                        # Name icmd semaphore issues file; include process_min_files dir if provided
                        if self.args.process_min_files:
                            pmf = self.args.process_min_files
                            dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
                        else:
                            dir_token = self.dataset_id
                        sem_out_path = os.path.join(out_dir, f"icmd_semaphore_issues_{dir_token}.csv")
                        sem_df[cols_to_save].to_csv(sem_out_path, index=False)
                        logging.info(f"ICMD semaphore details saved to {sem_out_path}")
        except Exception:
            # If any issue occurs, do not interrupt main flow
            pass

        return fail_df

    def write_failure_csv_and_compare(self, fail_df: pd.DataFrame, full_df: pd.DataFrame):
        if fail_df.empty:
            return

        out_dir = self.args.output_dir or "."
        os.makedirs(out_dir, exist_ok=True)
        dataset_id = self.dataset_id

        fail_df = fail_df.copy()
        fail_df["FailureReason"] = fail_df["FailureReason"].apply(
            lambda r: r if isinstance(r, str) else self.classify_failure_reason(r)
        )
        fail_df["dataset_id"] = dataset_id

        # comparison key: HostSerial + mlx5_
        for d in (fail_df, full_df):
            d["key"] = d["HostSerial"].astype(str) + ":" + d["mlx5_"].astype(str)

        # Name failed links file; if processing min files, include that dir name in filename
        if self.args.process_min_files:
            pmf = self.args.process_min_files
            dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
            fail_name = f"failed_links_{dir_token}.csv"
        else:
            fail_name = f"failed_links_{dataset_id}.csv"
        fail_csv = os.path.join(out_dir, fail_name)
        # Drop internal comparison key from failed_links CSV output
        fail_to_save = fail_df.drop(columns=["key"]) if "key" in fail_df.columns else fail_df
        fail_to_save.to_csv(fail_csv, index=False)
        logging.info(f"Failure details saved to {fail_csv}")

        if not self.prev_failures_file:
            return

        try:
            prev_df = pd.read_csv(self.prev_failures_file)
        except Exception as e:
            logging.error(f"Error reading previous failures file {self.prev_failures_file}: {e}")
            return

        if "HostSerial" not in prev_df.columns or "mlx5_" not in prev_df.columns:
            logging.error(
                f"Previous failures file {self.prev_failures_file} does not contain HostSerial/mlx5_ columns"
            )
            return

        prev_df["key"] = prev_df["HostSerial"].astype(str) + ":" + prev_df["mlx5_"].astype(str)

        prev_dataset = (
            prev_df["dataset_id"].iloc[0]
            if "dataset_id" in prev_df.columns and len(prev_df) > 0
            else "previous"
        )

        curr_fail_keys = set(fail_df["key"])
        prev_fail_keys = set(prev_df["key"])
        curr_all_keys = set(full_df["key"])

        overlap_failed = len(curr_fail_keys & prev_fail_keys)

        prev_only_keys = prev_fail_keys - curr_fail_keys
        recovered_keys = prev_only_keys & curr_all_keys  # previously failed, now present and not failing

        curr_unique = len(curr_fail_keys)
        prev_unique = len(prev_fail_keys)
        new_only = curr_unique - overlap_failed

        logging.info("Failure overlap with previous dataset:")
        logging.info(f"Current dataset id:  {dataset_id}")
        logging.info(f"Previous dataset id: {prev_dataset}")
        logging.info(f"Unique failed cables this dataset: {curr_unique}")
        logging.info(f"Unique failed cables previous    : {prev_unique}")
        logging.info(f"Common failed cables             : {overlap_failed}")
        logging.info(f"New failed cables (not previous) : {new_only}")
        logging.info(f"Recovered cables (were bad, now good): {len(recovered_keys)}")

        if recovered_keys:
            prev_rec = prev_df[prev_df["key"].isin(recovered_keys)].copy()
            curr_rec = full_df[full_df["key"].isin(recovered_keys)].copy()

            # ensure FailureReason exists in prev_rec
            if "FailureReason" not in prev_rec.columns:
                prev_rec["FailureReason"] = prev_rec["Status"].apply(self.classify_failure_reason)
            prev_rec = prev_rec.rename(
                columns={
                    "Status": "PrevStatus",
                    "FailureReason": "PrevFailureReason",
                    "dataset_id": "PrevDatasetId",
                }
            )
            curr_rec = curr_rec.rename(columns={"Status": "CurrStatus"})
            # merge on HostSerial + mlx5_ key
            rec_merged = prev_rec.merge(
                curr_rec[
                    ["key", "hostname", "HostSerial", "CableSerial", "mlx5_", "CurrStatus"]
                ],
                on="key",
                how="left",
                suffixes=("_prev", "_curr"),
            )
            rec_merged["CurrDatasetId"] = dataset_id

            recovered_csv = os.path.join(
                out_dir, f"recovered_links_{prev_dataset}_to_{dataset_id}.csv"
            )
            rec_merged.to_csv(recovered_csv, index=False)
            logging.info(f"Recovered cable details saved to {recovered_csv}")

    def gather_mlxlink_info(self):
        all_df = pd.DataFrame()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_mlxlink = {
                executor.submit(self.get_mlxlink_info, mlx5_interface, 60): mlx5_interface
                for mlx5_interface in self.mlx5_interfaces
            }
            for future in concurrent.futures.as_completed(future_to_mlxlink):
                mlx5_interface = future_to_mlxlink[future]
                data = future.result()
                df = self.process_mlxlink_info(data, mlx5_interface, "None")
                if not df.empty:
                    all_df = df if all_df.empty else pd.concat([all_df, df], ignore_index=True)

        all_df = all_df.sort_values(
            by="mlx5_",
            key=lambda x: np.argsort(index_natsorted(all_df["mlx5_"])),
        )

        if self.args.read_json_files:
            link_flaps = {}
        else:
            link_flaps = self.check_for_flaps()

        for interface in self.mlx5_interfaces:
            tmp_name = f"mlx5_{interface}"
            if tmp_name in link_flaps:
                flap_count = link_flaps[tmp_name]["flap_count"]
                last_flap_time = link_flaps[tmp_name]["last_flap_time"]
                all_df.loc[all_df["mlx5_"] == str(interface), "flap_count"] = flap_count
                all_df.loc[
                    all_df["mlx5_"] == str(interface), "last_flap_time"
                ] = last_flap_time

        return all_df

    def process_mlxlink_info(self, data, mlx5_interface, file):
        df = pd.DataFrame()
        try:
            CMD_Status = data["status"]["code"]
            CMD_Status_msg = data["status"]["message"]
            if CMD_Status == 0 or \
               (CMD_Status == 1 and "FEC Histogram is not supported for the current device" in CMD_Status_msg) or \
               (CMD_Status == 1 and "ICMD semaphore" in CMD_Status_msg):
                try:
                    RawPhysicalErrorsPerLane = data["result"]["output"][
                        "Physical Counters and BER Info"
                    ]["Raw Physical Errors Per Lane"]["values"]
                except Exception:
                    RawPhysicalErrorsPerLane = [-1, -1, -1, -1]
                try:
                    RawPhysicalBER = data["result"]["output"]["Physical Counters and BER Info"]["Raw Physical BER"]
                except:
                    RawPhysicalBER = -1
                    logging.error(f"No RawPyhsicalBER found in {file} {mlx5_interface}")
                EffectivePhysicalErrors = data["result"]["output"][
                    "Physical Counters and BER Info"
                ]["Effective Physical Errors"]
                EffectivePhysicalBER = data["result"]["output"][
                    "Physical Counters and BER Info"
                ]["Effective Physical BER"]
                VendorSerialNumber = data["result"]["output"]["Module Info"][
                    "Vendor Serial Number"
                ]
                Recommended = data["result"]["output"]["Troubleshooting Info"][
                    "Recommendation"
                ]
                NicFWVersion = data["result"]["output"]["Tool Information"][
                    "Firmware Version"
                ]
                LinkState = data["result"]["output"]["Operational Info"]["State"]

                fec_bins = {}
                if "result" in data and "Histogram of FEC Errors" in data["result"]["output"]:
                    fec_data = data["result"]["output"]["Histogram of FEC Errors"]
                    for i in range(16):
                        key = f"Bin {i}"
                        try:
                            fec_bins[i] = fec_data[key]["values"][1]
                        except Exception:
                            fec_bins[i] = "-1"
                else:
                    for i in range(16):
                        fec_bins[i] = "-1"

                for i in range(16):
                    globals()[f"FecBin{i}"] = fec_bins[i]
            else:
                RawPhysicalErrorsPerLane = [-1, -1, -1, -1]
                EffectivePhysicalErrors = "-1"
                EffectivePhysicalBER = "-1"
                RawPhysicalBER = "1e-99"
                LinkState = "Unknown"
                Recommended = "Unknown"
                VendorSerialNumber = "Unknown"
                NicFWVersion = "Unknown"

            mlx5_interface = data["mlx5_interface"]
            host = self.host_info["hostname"]
            host_serial = self.host_info["serial"]

            try:
                int(EffectivePhysicalErrors)
            except Exception:
                EffectivePhysicalErrors = -1

            try:
                float(EffectivePhysicalBER)
            except Exception:
                EffectivePhysicalBER = -1.0

            try:
                float(RawPhysicalBER)
            except Exception:
                RawPhysicalBER = -1.0

            try:
                RawPhysicalErrorsPerLane = [int(i) for i in RawPhysicalErrorsPerLane]
                RawPhyErrPerLaneStdev = float(np.std(RawPhysicalErrorsPerLane))
            except Exception:
                RawPhyErrPerLaneStdev = 0.0

            # Parse system uptime into minutes if provided
            uptime_min_val = None
            try:
                uptime_str = data.get("uptime") if isinstance(data, dict) else None
                if uptime_str:
                    boot_time = datetime.strptime(uptime_str, "%Y-%m-%d %H:%M:%S")
                    uptime_min_val = int(max(0, (datetime.now() - boot_time).total_seconds() // 60))
            except Exception:
                uptime_min_val = None

            # When processing minimal IB JSON files, capture flap metrics from counters
            flap_count_val = 0
            last_flap_time_val = None
            if self.args.process_min_files and self.args.IB:
                try:
                    if "Link Down Counter" in data["result"]["output"][
                        "Physical Counters and BER Info"]:
                        try:
                            flap_count_val = data["result"]["output"][
                        "Physical Counters and BER Info"]["Link Down Counter"]
                        except Exception:
                            flap_count_val = 0
                    if "Time Since Last Clear [Min]" in data["result"]["output"][
                        "Physical Counters and BER Info"]:
                        try:
                            last_flap_time_val = float(data["result"]["output"][
                        "Physical Counters and BER Info"]["Time Since Last Clear [Min]"] or 0.0)
                        except Exception:
                            last_flap_time_val = None

                        ts_val = last_flap_time_val
                        ld_val = flap_count_val
                except Exception:
                    pass

            temp_df = pd.DataFrame(
                {
                    "hostname": host,
                    "ip_addr": data["ip_address"],
                    "LinkState": LinkState,
                    "HostSerial": host_serial,
                    "CableSerial": VendorSerialNumber,
                    "mlx5_": mlx5_interface,
                    "nic_fw_version": NicFWVersion,
                    "EffPhyErrs": [int(EffectivePhysicalErrors)],
                    "EffPhyBER": float(EffectivePhysicalBER),
                    "RawPhyBER": float(RawPhysicalBER),
                    "RawPhyErrStdev": RawPhyErrPerLaneStdev,
                    "CMD_Status": int(CMD_Status),
                    "CMD_Status_msg": str(CMD_Status_msg),
                    "UptimeMin": uptime_min_val,
                    "flap_count": flap_count_val,
                    "last_flap_time": last_flap_time_val,
                    "Recommended": Recommended,
                    "FecBin0": FecBin0,
                    "FecBin1": FecBin1,
                    "FecBin2": FecBin2,
                    "FecBin3": FecBin3,
                    "FecBin4": FecBin4,
                    "FecBin5": FecBin5,
                    "FecBin6": FecBin6,
                    "FecBin7": FecBin7,
                    "FecBin8": FecBin8,
                    "FecBin9": FecBin9,
                    "FecBin10": FecBin10,
                    "FecBin11": FecBin11,
                    "FecBin12": FecBin12,
                    "FecBin13": FecBin13,
                    "FecBin14": FecBin14,
                    "FecBin15": FecBin15,
                    "Status": "Passed",
                    
                }
            )

            df = pd.concat([df, temp_df], ignore_index=True)
        except Exception as exc:
            logging.info("%r generated an exception: %s" % (mlx5_interface, exc))
            logging.info(traceback.format_exc())

        return df

    def read_json_files(self):
        json_files = glob("*_mlx5_*.json")
        all_df = pd.DataFrame()
        for file in json_files:
            with open(file, "r") as infile:
                data = json.load(infile)
            mlx5_inter = file.split("_")[2].split(".")[0]
            hostname = file.split("_")[3] if self.args.process_min_files else file.split("_")[0]
            data["mlx5_interface"] = f"{mlx5_inter}"
            data["ip_address"] = self.address
            self.host_info["hostname"] = hostname
            self.host_info["serial"] = "Unknown"
            data["hostname"] = self.host_info["hostname"]
            df = self.process_mlxlink_info(data, mlx5_inter,file)
            if not df.empty:
                all_df = df if all_df.empty else pd.concat([all_df, df], ignore_index=True)
        return all_df

    def read_min_json_files(self):
        if self.args.process_min_files == "CWD":
            files_dir = os.getcwd()
        else:
            files_dir = self.args.process_min_files

        if not os.path.exists(files_dir):
            logging.error(f"Min JSON files directory not found: {files_dir}")
            sys.exit(1)

        json_files = glob(f"{files_dir}/*mlxlink_info_min*.json")
        json_files += glob(f"{files_dir}/*test_min.json")

        all_df = pd.DataFrame()

        for file in json_files:
            logging.info(f"Processing JSON file: {file}")
            try:
                with open(file, "r") as infile:
                    data = json.load(infile)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
                continue

            hostname = data["hostname"]
            for key in data["mst_status"]:
                std_mlx_interface = self.convert_mst_status_to_standard_mlx5(key)
                data["mlx5_interface"] = f"{std_mlx_interface}"
                data["ip_address"] = None
                mlx5_inter = std_mlx_interface if self.args.process_min_files else data["mst_status"][key][5:]
                self.host_info["hostname"] = hostname
                self.host_info["serial"] = data["serial_number"]
                data["hostname"] = self.host_info["hostname"]
                try:
                    data[key]["mlx5_interface"] = f"{mlx5_inter}"
                    data[key]["ip_address"] = self.address
                    # Propagate top-level uptime string into per-port JSON for processing
                    data[key]["uptime"] = data.get("uptime")
                except Exception:
                    logging.error(f"Error processing data key: {key}")
                    continue

                df = self.process_mlxlink_info(data[key], mlx5_inter, file)
                if (not self.args.IB) and ("link_flaps" in data) and (data["mst_status"][key] in data["link_flaps"]):
                    link_key = data["mst_status"][key]
                    flap_count = data["link_flaps"][link_key]["flap_count"]
                    last_flap_time = data["link_flaps"][link_key]["last_flap_time"]

                    # If --recent-flap-hours is set, only report flaps within that window based on top-level 'time'
                    try:
                        recent_hours = getattr(self.args, "recent_flap_hours", None)
                        if recent_hours:
                            ref_time_str = data.get("time")
                            ref_time = datetime.strptime(ref_time_str, "%Y-%m-%d %H:%M:%SZ") if ref_time_str else None
                            last_dt = datetime.strptime(last_flap_time, "%Y-%m-%d %H:%M:%S") if last_flap_time else None
                            if ref_time and last_dt and (ref_time - last_dt) > timedelta(hours=int(recent_hours)):
                                flap_count = 0
                                last_flap_time = None
                    except Exception:
                        # On any parsing/logic error, fall back to original values
                        pass

                    # Apply flap metrics to DataFrame, guard against missing columns
                    try:
                        if (df is not None) and (not df.empty) and ("mlx5_" in df.columns):
                            df.loc[df["mlx5_"] == str(mlx5_inter), "flap_count"] = flap_count
                            df.loc[df["mlx5_"] == str(mlx5_inter), "last_flap_time"] = last_flap_time
                        else:
                            logging.error(
                                f"Missing 'mlx5_' column or empty DataFrame when updating flaps for file {file}, "
                                f"mlx5_inter={mlx5_inter}. DF columns={list(df.columns) if df is not None else 'None'}"
                            )
                    except KeyError as e:
                        logging.error(
                            f"KeyError updating flap fields for file {file}, mlx5_inter={mlx5_inter}: {e}. "
                            f"DF columns={list(df.columns) if df is not None else 'None'}"
                        )
                    except Exception as e:
                        logging.error(
                            f"Unexpected error updating flap fields for file {file}, mlx5_inter={mlx5_inter}: {e}"
                        )

                if not df.empty:
                    all_df = df if all_df.empty else pd.concat([all_df, df], ignore_index=True)

        return all_df

    def display_mlxlink_info_json(self):
        if self.args.process_min_files:
            df = self.read_min_json_files()
        else:
            df = self.read_json_files()

        df = self.check_mlxlink_info(df)

        df = df.sort_values(
            by=["hostname", "mlx5_"],
            key=lambda x: np.argsort(index_natsorted(df["hostname"])),
        )

        if not self.args.full:
            fec_columns = [f"FecBin{i}" for i in range(16)]
            df = df.drop(columns=fec_columns)
        else:
            if self.args.IB:
                high_fec_columns = [f"FecBin{i}" for i in range(8, 16)]
                df = df.drop(columns=high_fec_columns)

        df = df[df["mlx5_"].notna()]
        df = df[df["mlx5_"] != "None"]

        logging.info(
            f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}"
        )

        fail_df = self.summarize_failures(df)
        self.write_failure_csv_and_compare(fail_df, df)

        if self.args.plot_histograms:
            self.plot_histograms(df)

        if self.args.output_dir:
            if not os.path.exists(self.args.output_dir):
                os.makedirs(self.args.output_dir)
            os.chdir(self.args.output_dir)

        if self.args.file_format == "csv":
            # Build sorted DataFrame for CSV output:
            # Always list FAILED rows first, then non-failed. Within FAILED, apply --failed-sort-by.
            df_to_save = df.copy()
            try:
                df_to_save["Status"] = df_to_save["Status"].astype(str)
                failed_mask = df_to_save["Status"].str.startswith("Failed", na=False)

                failed_rows = df_to_save[failed_mask].copy()
                ok_rows = df_to_save[~failed_mask].copy()

                secondary = self.args.failed_sort_by

                if secondary == "raw_ber":
                    failed_rows = failed_rows.sort_values("RawPhyBER", ascending=False, na_position="last")
                elif secondary == "fec7":
                    if "FecBin7" not in failed_rows.columns:
                        failed_rows["FecBin7"] = -1
                    failed_rows = failed_rows.sort_values("FecBin7", ascending=False, na_position="last")
                elif secondary == "eff_phy":
                    failed_rows = failed_rows.sort_values("EffPhyErrs", ascending=False, na_position="last")
                elif secondary == "bad_signal":
                    failed_rows = (
                        failed_rows.assign(
                            __bad=failed_rows["Recommended"].astype(str).str.contains(
                                "Bad signal integrity", case=False, na=False
                            )
                        )
                        .sort_values(["__bad", "hostname", "mlx5_"], ascending=[False, True, True])
                        .drop(columns="__bad")
                    )
                elif secondary == "link_state":
                    failed_rows = (
                        failed_rows.assign(__badstate=failed_rows["LinkState"].astype(str).ne("Active"))
                        .sort_values(["__badstate", "hostname", "mlx5_"], ascending=[False, True, True])
                        .drop(columns="__badstate")
                    )

                # Keep non-failed ordered by hostname, mlx5_
                ok_rows = ok_rows.sort_values(["hostname", "mlx5_"], ascending=[True, True])

                df_to_save = pd.concat([failed_rows, ok_rows], ignore_index=True)
            except Exception as _:
                # On any issue, fall back to hostname/mlx5_ ordering
                df_to_save = df.sort_values(["hostname", "mlx5_"], ascending=[True, True])

            # Place UptimeMin immediately after Status in CSV output if present
            if "UptimeMin" in df_to_save.columns and "Status" in df_to_save.columns:
                cols = [c for c in df_to_save.columns if c != "UptimeMin"]
                try:
                    status_idx = cols.index("Status")
                    cols.insert(status_idx + 1, "UptimeMin")
                    df_to_save = df_to_save[cols]
                except ValueError:
                    pass

            # Drop comparison key column from final CSV if present
            if "key" in df_to_save.columns:
                df_to_save = df_to_save.drop(columns=["key"])

            # Ensure hostname column is string and first
            if "hostname" in df_to_save.columns:
                df_to_save["hostname"] = df_to_save["hostname"].astype(str)
                _cols = list(df_to_save.columns)
                _cols = ["hostname"] + [c for c in _cols if c != "hostname"]
                df_to_save = df_to_save[_cols]

            # Name CSV; if processing min files, include that dir name in filename
            if self.args.process_min_files:
                pmf = self.args.process_min_files
                dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
                csv_filename = f"mlxlink_info_{dir_token}_{self.get_date_stamp()}.csv"
            else:
                csv_filename = f"mlxlink_info_{self.args.address}_{self.get_date_stamp()}.csv"

            df_to_save.to_csv(csv_filename, index=False)
        elif self.args.file_format == "json":
            json_filename = (
                f"mlxlink_info_{self.args.address}_{self.get_date_stamp()}.json"
            )
            df.to_json(json_filename, orient="records")
        else:
            logging.error(f"Invalid file format: {self.args.file_format}")

        return df

    def plot_histograms(self, df):
        output_dir = self.args.output_dir or "."
        timestamp = self.get_date_stamp()

        fec_columns = [f"FecBin{i}" for i in range(16)]
        for col in fec_columns:
            if col not in df.columns:
                df[col] = -1

        valid_df = df[df["mlx5_"].notna() & (df["mlx5_"] != "None")]

        if valid_df.empty:
            logging.warning("No valid data for plotting")
            return

        # Raw Physical BER histogram: 1e-8 .. 1e-5, log x, report under/over, plot only in-range
        raw_ber_data = valid_df[valid_df["RawPhyBER"] > 0]["RawPhyBER"]
        if not raw_ber_data.empty:
            min_val = 1e-7
            max_val = 1e-5
            total = len(raw_ber_data)
            below_min = int((raw_ber_data <= min_val).sum())
            above_max = int((raw_ber_data >= max_val).sum())
            in_range = raw_ber_data[(raw_ber_data > min_val) & (raw_ber_data < max_val)]

            # Bin edges at mantissa 1..9 times powers of 10 between min and max (e.g., 1e-7,2e-7,...,9e-7,1e-6,2e-6,...)
            edges = []
            for exp in range(int(np.floor(np.log10(min_val))), int(np.ceil(np.log10(max_val))) + 1):
                for m in range(1, 10):
                    val = m * (10.0 ** exp)
                    if val >= min_val and val <= max_val:
                        edges.append(val)
            # Ensure edges are unique and sorted
            bins = np.array(sorted(set(edges)))

            plt.figure(figsize=(10, 6))
            if not in_range.empty:
                plt.hist(in_range, bins=bins, edgecolor="black")
            else:
                plt.hist([], bins=bins, edgecolor="black")
            plt.xscale("log")
            plt.xlim(min_val, max_val)
            plt.ylim(0, self.args.max_y_raw_ber)
            plt.title("Raw Physical BER Histogram")
            plt.xlabel("Raw Physical BER")
            plt.ylabel("Number of Cables")

            ax = plt.gca()
            ax.xaxis.set_major_formatter(
                FuncFormatter(lambda x, pos: f"{x:.0e}")
            )

            annotation = (
                f"Total cables: {total:,}\n"
                f"<= {min_val:.0e}: {below_min:,}\n"
                f"> {max_val:.0e}: {above_max:,}"
            )
            plt.text(
                0.98,
                0.95,
                annotation,
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox=dict(facecolor="white", alpha=0.7, edgecolor="none"),
            )

            plt.xticks(rotation=45)
            plt.tight_layout()
            # Name raw BER plot; include process_min_files dir if provided
            if self.args.process_min_files:
                pmf = self.args.process_min_files
                dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
                out_name = f"{output_dir}/raw_ber_histogram_{dir_token}_{timestamp}.png"
            else:
                out_name = f"{output_dir}/raw_ber_histogram_{timestamp}.png"
            plt.savefig(out_name)
            plt.close()
        else:
            logging.warning("No valid RawPhyBER data for histogram")

        # Effective Physical Errors histogram with fixed range 0–100,000
        eff_err_data = valid_df[valid_df["EffPhyErrs"] >= 0]["EffPhyErrs"]
        if not eff_err_data.empty:
            total = len(eff_err_data)
            below_2000 = int((eff_err_data < 2000).sum())
            above_cap = int((eff_err_data > 100000).sum())

            nonzero_eff = eff_err_data[eff_err_data >= 2000]
            bins = np.linspace(2000, 100000, 51)
            eff_inrange = nonzero_eff[nonzero_eff <= 100000]

            plt.figure(figsize=(10, 6))
            if not nonzero_eff.empty:
                n, bin_edges, _ = plt.hist(eff_inrange, bins=bins, edgecolor="black")
            else:
                n, bin_edges, _ = plt.hist([], bins=bins, edgecolor="black")

            plt.title("Effective Physical Errors Histogram (2,000–100,000)")
            plt.xlabel("Effective Physical Errors")
            plt.ylabel("Number of Cables")

            ax = plt.gca()
            ax.xaxis.set_major_formatter(
                FuncFormatter(lambda x, pos: f"{int(x):,}" if x >= 0 else f"{int(x)}")
            )
            plt.xticks(np.arange(0, 100001, 10000), rotation=45)
            plt.xlim(0, 100000)
            plt.ylim(0, self.args.max_y_eff_phy)

            annotation = (
                f"Total cables: {total:,}\n"
                f"< 2,000: {below_2000:,}\n"
                f"> 100,000: {above_cap:,}"
            )
            plt.text(
                0.98,
                0.95,
                annotation,
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox=dict(facecolor="white", alpha=0.7, edgecolor="none"),
            )

            plt.tight_layout()
            # Name eff phy errors plot; include process_min_files dir if provided
            if self.args.process_min_files:
                pmf = self.args.process_min_files
                dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
                out_name = f"{output_dir}/eff_phy_errors_histogram_{dir_token}_{timestamp}.png"
            else:
                out_name = f"{output_dir}/eff_phy_errors_histogram_{timestamp}.png"
            plt.savefig(out_name)
            plt.close()
        else:
            logging.warning("No valid EffPhyErrs data for histogram")

        # FEC Bins histograms
        for i in range(16):
            fec_col = f"FecBin{i}"
            fec_data = valid_df[valid_df[fec_col] >= 0][fec_col].astype(int)
            if fec_data.empty:
                logging.warning(f"No valid {fec_col} data for histogram")
                continue

            total = len(fec_data)
            zeros = int((fec_data == 0).sum())
            nonzero = fec_data[fec_data > 0]
            annotation_zeros_label = "Zeros"
            annotation_zeros_count = zeros

            plt.figure(figsize=(10, 6))

            if nonzero.empty:
                bins = [-0.5, 0.5]
                n, bin_edges, _ = plt.hist(fec_data, bins=bins, edgecolor="black")
                cutoff = 0
                above_cutoff = 0
            else:
                if i == 7:
                    # Fixed range for FEC Bin 7: 2,000–10,000 with 50 bins (exclude values < 2,000)
                    cutoff = 10000
                    min_cut = 2000
                    bins = np.linspace(min_cut, cutoff, 51)
                    fec_nonmin = fec_data[fec_data >= min_cut]
                    fec_inrange = fec_nonmin[fec_nonmin <= cutoff]
                    n, bin_edges, _ = plt.hist(fec_inrange, bins=bins, edgecolor="black")
                    above_cutoff = int((fec_data > cutoff).sum())
                    plt.ylim(0, self.args.max_y_fec7)
                    annotation_zeros_label = "< 2,000"
                    annotation_zeros_count = int((fec_data < min_cut).sum())
                else:
                    cutoff = float(np.quantile(nonzero, 0.99))
                    max_val = int(nonzero.max())
                    if cutoff <= 0 or cutoff > max_val:
                        cutoff = max_val

                    bins = np.linspace(0, cutoff, 51)
                    fec_inrange = fec_data[fec_data <= cutoff]
                    n, bin_edges, _ = plt.hist(fec_inrange, bins=bins, edgecolor="black")

                    above_cutoff = int((fec_data > cutoff).sum())
                    if len(n) > 1:
                        nonzero_bins_max = n[1:].max()
                        if nonzero_bins_max > 0:
                            plt.ylim(0, nonzero_bins_max * 1.1)

            plt.title(f"FEC Bin {i} Histogram")
            plt.xlabel(f"FEC Bin {i} Count")
            plt.ylabel("Number of Cables")

            ax = plt.gca()
            ax.xaxis.set_major_formatter(
                FuncFormatter(lambda x, pos: f"{int(x):,}" if x >= 0 else f"{int(x)}")
            )
            plt.xticks(rotation=45)

            annotation = (
                f"Total cables: {total:,}\n"
                f"{annotation_zeros_label}: {annotation_zeros_count:,}\n"
                f"> {int(cutoff):,}: {above_cutoff:,}"
            )
            plt.text(
                0.98,
                0.95,
                annotation,
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox=dict(facecolor="white", alpha=0.7, edgecolor="none"),
            )

            plt.tight_layout()
            # Name fec bin plot; if processing min files, include that dir name in filename
            if self.args.process_min_files:
                pmf = self.args.process_min_files
                dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
                out_name = f"{output_dir}/fec_bin_{i}_histogram_{dir_token}_{timestamp}.png"
            else:
                out_name = f"{output_dir}/fec_bin_{i}_histogram_{timestamp}.png"
            plt.savefig(out_name)
            plt.close()

    def convert_mst_status_to_standard_mlx5(self, interface):
        try:
            mlx5_interface = self.mst_mapping[self.args.shape][interface]
        except Exception:
            print(
                f"Error converting mst status to standard mlx5 interface: {interface}"
            )
            mlx5_interface = None
        return mlx5_interface


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gather mlxlink info")

    def list_of_strings(arg):
        return arg.split(",")

    parser.add_argument(
        "-l", "--log", default="INFO", help="Set the logging level (default: %(default)s)"
    )
    parser.add_argument("-e", "--error", action="store_true", help="Set the error reporting")
    parser.add_argument(
        "-w",
        "--warning",
        action="store_true",
        help="Add warnings to the error reporting",
    )
    parser.add_argument("--date_stamp", type=str, help="The data file to use")
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress output to the console (default: %(default)s)",
    )
    parser.add_argument("-a", "--address", type=str, help="The ip address of the remote host")
    parser.add_argument(
        "--ber_threshold",
        type=str,
        default="1e-7",
        help="specify the Raw Physical BER threshold",
    )
    parser.add_argument(
        "--eff_threshold",
        type=str,
        default="100000",
        help="specify the Effective Physical Error threshold",
    )
    parser.add_argument(
        "--file_format",
        type=str,
        default="json",
        help="specify the output file format: csv,json (default: %(default)s",
    )
    parser.add_argument("--output_dir", type=str, help="specify the output dir name")
    parser.add_argument("--read_json_files", action="store_true", help="Load json files")
    parser.add_argument(
        "--flap_duration_threshold",
        type=int,
        help="specify the flap duration threshold in seconds",
    )
    parser.add_argument(
        "--mlx_interfaces",
        type=list_of_strings,
        default="0,1,3,4,5,6,7,8,9,10,12,13,14,15,16,17",
        help="specify the mlx interfaces to check %(default)s",
    )
    parser.add_argument(
        "--process_min_files",
        type=str,
        help='specify the directory where the mlxlink_info_min files are located: "CWD" or "path to the results dir"',
    )
    parser.add_argument(
        "--rdma_prefix",
        type=str,
        default="rdma",
        help="specify the rdma prefix (default: %(default)s)",
    )
    parser.add_argument(
        "-s",
        "--shape",
        type=str,
        default="H100",
        help="specify the compute shape. (A100, H100, GB200, B200) (default: %(default)s)",
    )
    parser.add_argument("-f", "--full", action="store_true", help="Enable full output")
    parser.add_argument("--IB", action="store_true", help="Limit FecBins to 0-7")
    parser.add_argument(
        "--plot_histograms",
        action="store_true",
        help="Generate and save histograms for RawPhyBER, EffPhyErrs, and each FEC bin",
    )
    # Plot scaling controls
    parser.add_argument(
        "--max-y-raw-ber",
        type=float,
        default=100.0,
        help="Max y-axis for Raw Physical BER histogram (default: %(default)s)",
    )
    parser.add_argument(
        "--max-y-fec7",
        type=float,
        default=25.0,
        help="Max y-axis for FEC Bin 7 histogram (default: %(default)s)",
    )
    parser.add_argument(
        "--max-y-eff-phy",
        type=float,
        default=250.0,
        help="Max y-axis for Effective Physical Errors histogram (default: %(default)s)",
    )
    parser.add_argument(
        "--failed-sort-by",
        type=str,
        choices=["raw_ber", "fec7", "eff_phy", "bad_signal", "link_state"],
        default="raw_ber",
        help="Secondary sort within FAILED cables: raw_ber, fec7, eff_phy, bad_signal, link_state (default: %(default)s). Non-failed rows are listed after failed.",
    )
    parser.add_argument(
        "--dataset-id",
        type=str,
        help="Identifier for this dataset/run (used in failure CSV name)",
    )
    parser.add_argument(
        "--prev-failures-file",
        type=str,
        help="Path to a previous failed_links_*.csv to compare against",
    )
    parser.add_argument(
        "--recent-flap-hours",
        type=int,
        help="Only report link_flaps whose last_flap_time is within the past N hours based on top-level 'time' in ingested JSON",
    )

    args = parser.parse_args()

    logging.getLogger().setLevel(args.log.upper())

    mlxlink_info = MlxlinkInfo(args)

    if args.read_json_files or args.process_min_files:
        mlxlink_info.display_mlxlink_info_json()
        sys.exit(0)

    host_info = mlxlink_info.get_host_info()

    df = mlxlink_info.gather_mlxlink_info()
    df = mlxlink_info.check_mlxlink_info(df)

    df.sort_values(by=["hostname", "mlx5_"])

    logging.getLogger().setLevel("INFO")

    df = df[df["mlx5_"].notna()]
    df = df[df["mlx5_"] != "None"]

    logging.info(
        f"\n{tabulate(df, headers='keys', tablefmt='simple_outline')}"
    )

    fail_df = mlxlink_info.summarize_failures(df)
    mlxlink_info.write_failure_csv_and_compare(fail_df, df)

    if args.plot_histograms:
        mlxlink_info.plot_histograms(df)

    if args.output_dir:
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        os.chdir(args.output_dir)

    if args.file_format == "json":
        json_filename = (
            f"mlxlink_info_{args.address}_{mlxlink_info.get_date_stamp()}.json"
        )
        df.to_json(json_filename, orient="records")
    elif args.file_format == "csv":
        if args.process_min_files:
            pmf = args.process_min_files
            dir_token = "CWD" if pmf == "CWD" else os.path.basename(os.path.normpath(pmf))
            csv_filename = f"mlxlink_info_{dir_token}_{mlxlink_info.get_date_stamp()}.csv"
        else:
            csv_filename = f"mlxlink_info_{args.address}_{mlxlink_info.get_date_stamp()}.csv"
        if "key" in df.columns:
            df = df.drop(columns=["key"])
        # Ensure hostname column is string and first
        if "hostname" in df.columns:
            df["hostname"] = df["hostname"].astype(str)
            _cols = list(df.columns)
            _cols = ["hostname"] + [c for c in _cols if c != "hostname"]
            df = df[_cols]
        df.to_csv(csv_filename, index=False)
    else:
        logging.error(f"Invalid file format: {args.file_format}")
