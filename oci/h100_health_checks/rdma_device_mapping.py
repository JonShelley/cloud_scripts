#!/usr/bin/env python3

import subprocess

expected_vals = {'0000:0c:00.0': ['mlx5_0','mlx5_18'],
                 '0000:0c:00.1': ['mlx5_1','mlx5_19'],
                 '0000:1f:00.0': ['mlx5_2'],
                 '0000:2a:00.0': ['mlx5_3','mlx5_26'],
                 '0000:2a:00.1': ['mlx5_4','mlx5_27'],
                 '0000:41:00.0': ['mlx5_5','mlx5_28'],
                 '0000:41:00.1': ['mlx5_6','mlx5_29'],
                 '0000:58:00.0': ['mlx5_7','mlx5_30'],
                 '0000:58:00.1': ['mlx5_8','mlx5_31'],
                 '0000:86:00.0': ['mlx5_9','mlx5_32'],
                 '0000:86:00.1': ['mlx5_10','mlx5_33'],
                 '0000:9a:00.0': ['mlx5_11'],
                 '0000:a5:00.0': ['mlx5_12','mlx5_20'],
                 '0000:a5:00.1': ['mlx5_13','mlx5_21'],
                 '0000:bd:00.0': ['mlx5_14','mlx5_22'],
                 '0000:bd:00.1': ['mlx5_15','mlx5_23'],
                 '0000:d5:00.0': ['mlx5_16','mlx5_24'],
                 '0000:d5:00.1': ['mlx5_17','mlx5_25']
                 }

def run_ibdev2netdev():
    result = subprocess.run(['ibdev2netdev', '-v'], stdout=subprocess.PIPE, universal_newlines=True)
    lines = result.stdout.split('\n')

    data = {}
    for line in lines[1:]:  # Skip the header line
        if line:  # Skip empty lines
            parts = line.split()
            data[parts[0]] = parts[1]  # Map mlx to interface

    return data

data = run_ibdev2netdev()
#print(data)

errors = False
for key in data:
    if data[key] not in expected_vals[key]:
        print("Failed: Expected {} to be {}, not {}".format(key, expected_vals[key], data[key]))
        errors = True
if not errors:
    print("Passed: RDMA devices are mapped correctly")