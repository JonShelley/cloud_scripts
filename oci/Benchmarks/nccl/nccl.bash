#!/bin/bash
#

host1=$1
host2=$2

module load /home/ubuntu/mpi/hpcx*/modulefiles/hpcx

export NCCL_IB_HCA="=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17" 
#/usr/mpi/gcc/openmpi-4.1.5a1/bin/mpirun \
CMD="mpirun \
    -np 64 \
    -N 8 \
    --hostfile ~/cloud_scripts/oci/benchmarks/nccl/hosts_8.txt \
    -x LD_LIBRARY_PATH \
    -mca pml ucx \
    -mca coll ^hcoll \
    --bind-to numa \
    -x RX_QUEUE_LEN=8192 \
    -x IB_RX_QUEUE_LEN=8192 \
    -x UCX_TLS=tcp \
    -x UCX_NET_DEVICES=eth0 \
    -x HCOLL_ENABLE_MCAST_ALL=0 \
    -x coll_hcoll_enable=0 \
    -x NCCL_DEBUG_FILE=~/cloud_scripts/oci/benchmarks/nccl/nccl_debug.log \
    -x NCCL_DEBUG=WARN \
    -x NCCL_IB_TIMEOUT=16 \
    -x NCCL_IB_SL=0 \
    -x NCCL_IB_TC=41 \
    -x NCCL_IB_GID_INDEX=3 \
    -x NCCL_IB_QPS_PER_CONNECTION=4 \
    -x NCCL_IB_SPLIT_DATA_ON_QPS=0 \
    -x NCCL_TOPO_FILE=~/cloud_scripts/oci/utils/h100_topo_rdma.xml \
    /opt/oci-hpc/nccl-test/build/all_reduce_perf -b 8 -e 16G -f 2 -g 1 "

#    -x NCCL_IB_HCA="=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17" \

date
echo ${CMD}
${CMD}
