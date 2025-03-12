#!/usr/bin/env python3

import json
import pandas as pd

import argparse

# collect the arguments
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-f', '--file', type=str, help='input file')
parser.add_argument('-s', '--shape', type=str, default="h100t", help='What compute shape are you using?')
parser.add_argument('-c', '--cut_sheet', type=str, help='Path to the cut sheet you using')

args = parser.parse_args()

mst_status_h100t = {
        "d5:00.1": "mlx5_19",
        "d5:00.0": "mlx5_18",
        "bd:00.1": "mlx5_17",
        "bd:00.0": "mlx5_16",
        "a5:00.1": "mlx5_15",
        "a5:00.0": "mlx5_14",
        "9a:00.1": "mlx5_13",
        "9a:00.0": "mlx5_12",
        "86:00.1": "mlx5_11",
        "86:00.0": "mlx5_10",
        "58:00.1": "mlx5_9",
        "58:00.0": "mlx5_8",
        "41:00.1": "mlx5_7",
        "41:00.0": "mlx5_6",
        "2a:00.1": "mlx5_5",
        "2a:00.0": "mlx5_4",
        "1f:00.1": "mlx5_3",
        "1f:00.0": "mlx5_2",
        "0c:00.1": "mlx5_1",
        "0c:00.0": "mlx5_0"
    }

mst_status_h100 = {
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
    }

mst_status_h200 = {
        "0c:00.0": "mlx5_0",
        "1f:00.0": "mlx5_1",
        "bd:00.0": "mlx5_10",
        "d5:00.0": "mlx5_11",
        "1f:00.1": "mlx5_2",
        "2a:00.0": "mlx5_3",
        "41:00.0": "mlx5_4",
        "58:00.0": "mlx5_5",
        "86:00.0": "mlx5_6",
        "9a:00.0": "mlx5_7",
        "9a:00.1": "mlx5_8",
        "a5:00.0": "mlx5_9"
    }

mst_h100t_to_h100 = {}
for key, value in mst_status_h100t.items():
    if key in mst_status_h100:
        mst_h100t_to_h100[value] = mst_status_h100[key]
    else:
        print(f"Key {key} not found in mst_status_h100")
        mst_h100t_to_h100[value] = "mlx5_-99"


def convert_mlx5_interface(shape, interface):
    interface = str(interface)
    if interface.find("mlx5_") == -1:
        interface = "mlx5_" + str(interface)
     
    if shape == "h100t":
        #print(f"Interface {interface}")
        interface = mst_h100t_to_h100[interface]
        #print(f"New interface {interface}")
    elif shape == "h100":
        return interface.replace("mlx5_", "")
    else:
        print("Shape not supported")
        exit(1)

    return interface.replace("mlx5_", "")

if args.cut_sheet:
    print(f"Cut sheet provided {args.cut_sheet}")
    cut_sheet = pd.read_csv(args.cut_sheet)
    print(cut_sheet)

# read the file
with open(args.file) as f:
    data = json.load(f)

    # convert mlx5 interface to standard mlx5 numbering
    for item in data:
        if "mlx5_" in item:
            item["mlx5_"] = convert_mlx5_interface(args.shape, item["mlx5_"])
    
    # Get file name without the path or extension
    file_name = args.file.split("/")[-1].split(".")[0]
    print(f"File name: {file_name}")

    # write out the file
    with open("mod_"+file_name, 'w') as f:
        json.dump(data, f, indent=4)

    # write out a csv file
    pd.DataFrame(data).to_csv("mod_" + file_name + ".csv", index=False)
    
    # Load in mod file
    mod_file = pd.read_csv("mod_" + file_name + ".csv")

    # Add mlx5_ to the values in mlx5_ column
    mod_file["mlx5_"] = "mlx5_" + mod_file["mlx5_"].astype(str)

    # Add the information from the cut sheet to the mod file based on "HOST_SERIAL and "MLX_DEVICE"
    mod_file = pd.merge(mod_file, cut_sheet, left_on=["HostSerial", "mlx5_"], right_on=["HOST_SERIAL", "MLX DEVICE"])

    # Select only the rows that contain Failed the "Status" column
    mod_file_fail = mod_file[mod_file['Status'].str.contains('Failed')]

    # Write out the new file for the following columns HostSerial, RACK NUMBER, PCIE SLOT, GPU_EXPANDER SWITCH_RACK, SWITCH_RACK_ELEVATION, SWITCH_NAME, SWITCH_PORT, PCI BDF, MLX DEVICE
    mod_file_fail[["HostSerial", "RACK NUMBER", "PCIE SLOT_PORT", "GPU_EXPANDER", "SWITCH_RACK", "SWITCH_RACK_ELEVATION", "SWITCH_NAME", "SWITCH_PORT", "PCI BDF", "MLX DEVICE"]].to_csv("mod_" + file_name + "_final.csv", index=False)

