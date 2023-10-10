#!/bin/bash

CONT="/home/ubuntu/sqsh/pytorch-23.08.sqsh"
MOUNT="/home/ubuntu/utils:/utils,/home/ubuntu/Benchmarks/nccl:/nccl,/home/ubuntu/mpi/hpcx-v2.16-gcc-mlnx_ofed-ubuntu20.04-cuda12-gdrcopy2-nccl2.18-x86_64:/opt/hpcx"

  
export RX_QUEUE_LEN=8192 \
       IB_RX_QUEUE_LEN=8192 \
       UCX_TLS=tcp \
       HCOLL_ENABLE_MCAST_ALL=0 \
       coll_hcoll_enable=0 \
       UCX_NET_DEVICES=eth0 \
       NCCL_SOCKET_IFNAME=eth0 \
       NCCL_IB_TIMEOUT=16 \
       NCCL_IB_SL=0 \
       NCCL_IB_TC=41 \
       NCCL_IB_GID_INDEX=3 \
       NCCL_IB_QPS_PER_CONNECTION=4 \
       NCCL_IB_SPLIT_DATA_ON_QPS=0 \
       NCCL_TOPO_FILE=/utils/h100_topo_rdma.xml \
       NCCL_DEBUG_LOG=/nccl/nccl_debug.log \
       OMPI_MCA_pml=ucx \
       OMPI_MCA_btl=^openib

export NCCL_IB_HCA="=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17"

env | grep "SLURMD_NODENAME="
env | grep "SLURM_NODELIST="

srun \
    --mpi=pmix_v3 \
    --gpus-per-node=8 \
    --ntasks-per-node=8 \
    --container-image "${CONT}" \
    --container-name=nccl \
    --container-mounts "${MOUNT}" \
    bash -c 'source /opt/hpcx/hpcx-init.sh && hpcx_load && /nccl/nccl-tests/build/all_reduce_perf -b8 -f 2 -g 1 -e 16G'
