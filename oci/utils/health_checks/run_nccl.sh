#!/bin/bash

hostfile=$1

file_dir="$(date +%Y%m%d-%H%M%S)"
mkdir $file_dir
cd $file_dir

#python3 ../run_set_of_nccl_tests.py --hostfile $hostfile --hosts_per_job 128  --nccl_test /opt/oci-hpc/nccl-tests/build/all_reduce_perf  --node_shape gb200v3 --timeout 180 --max_workers 8
for x in all_reduce_perf all_gather_perf gather_perf reduce_perf reduce_scatter_perf broadcast_perf  alltoall_perf
do
    python3 ../run_set_of_nccl_tests.py --hostfile $hostfile --hosts_per_job 16 32 48 64 128  --nccl_test /opt/oci-hpc/nccl-tests/build/$x  --node_shape gb200v3 --timeout 180 --max_workers 8
done 

