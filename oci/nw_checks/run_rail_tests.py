#!/usr/bin/env python3

from subprocess import run
import re
import os
import sys

cluster="H100"
host1="compute-permanent-node-933"
host2="compute-permanent-node-420"
msg_sizes="4:8"

cmd="mpirun -n 1 --host {} -x UCX_NET_DEVICES=mlx5_{}:1  --map-by node -x LD_LIBRARY_PATH numactl -N {} {}/osu_latency -m {} : -n 1 --host {} -x UCX_NET_DEVICES=mlx5_{}:1 --map-by node -x LD_LIBRARY_PATH numactl -N {} {}/osu_latency -m {}"
rdma={
      "H100": { "rail_opt": True,
                "subnet": "single",
                "rail_pairs": [[0,1],[3,4],[5,6],[7,8],[9,10],[12,13],[14,15],[16,17]],
#                "rail_pairs": [[0,1],[2,3],[4,5],[6,7],[8,9],[10,11],[12,13],[14,15]],
                "inter_numa": {"0": [0,1,3,4,5,6,7,8], "1": [9,10,12,13,14,15,16,17]},
                #"inter_numa": {"0": [0,1,2,3,4,5,6,7], "1": [8,9,10,11,12,13,14,15]},
                "lat_cutoff": 3.2
              },
      "A100": {"rail_opt": False, "subnet": "multiple","rail_pairs": [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[14],[15],[16],[17]] },
}

exec(open('/usr/share/modules/init/python.py').read())
module('purge')
module('load','/home/ubuntu/mpi/hpcx-v2.16-gcc-mlnx_ofed-ubuntu20.04-cuda12-gdrcopy2-nccl2.18-x86_64/modulefiles/hpcx')
module('list')

osu_path = os.environ['HPCX_OSU_CUDA_DIR']
#os.environ['coll_hcoll_enable'] = "0"
#os.environ['HCOLL_ENABLE_MCAST_ALL'] = "0"
#os.environ['RX_QUEUE_LEN'] = "8192"
#os.environ['IB_RX_QUEUE'] = "8192"
#os.environ['UCX_TLS'] = "tcp"
#os.environ['UCX_NET_DEVICES'] = "eth0"

str_p = re.compile(r"^8\s+(.*)", flags=re.MULTILINE)

c_type = rdma[cluster]

print("Cluster: {}".format(cluster))
print("{} -> {} ".format(host1,host2))
print("_____________________")
print("{:10s}{:10s}{:8s}".format("interface","interface","time(us)"))
for devices in rdma[cluster]["rail_pairs"]:
    #print("Devices: {}".format(devices))
    device_test = []
    for d,device in enumerate(devices):
        device_test.append([device, device])
    if len(devices) == 2:
        device_test.append([devices[0], devices[1]])
        

    #print("Tests: {}".format(device_test))
    for d1,d2 in device_test:
        #print("Device: {}".format(device))
        # Check to see which numa the interface is in.
        numa_id_1 = 0
        numa_id_2 = 0
        for nid in rdma[cluster]["inter_numa"]:
            if d1 in rdma[cluster]["inter_numa"][nid]:
                numa_id_1 = nid
            if d2 in rdma[cluster]["inter_numa"][nid]:
                numa_id_2 = nid
        cmd_line = cmd.format(host1,d1,numa_id_1,osu_path,msg_sizes,host2,d2,numa_id_2,osu_path,msg_sizes)
#        print(cmd_line)

        p = run (cmd_line.split(), capture_output=True)
        if p.returncode == 0:
            output=p.stdout.decode()
            #print(output)
            result=str_p.search(output)
            lat = float(result.group(1))
            print("mlx5_{:<4d} mlx5_{:<4d} {:>8.2f}".format(d1,d2,lat))
        else:
            print( 'Failed! exit status: ', p.returncode )
            print( "CMD: {}".format(cmd_line) )
            print( "Error: {}".format(p.stderr.decode()))

