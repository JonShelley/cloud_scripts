#!/bin/bash
  
CONT="/home/ubuntu/sqsh/pytorch-23.08.sqsh"
MOUNT="/home/ubuntu/Benchmarks/nccl:/nccl,/home/ubuntu/mpi/hpcx-v2.16-gcc-mlnx_ofed-ubuntu20.04-cuda12-gdrcopy2-nccl2.18-x86_64:/opt/hpcx"

export OMPI_MCA_pml=ucx
export OMPI_MCA_btl=^openib

srun --ntasks=$SLURM_JOB_NUM_NODES \
    --container-image "${CONT}" \
    --container-name=nccl \
    --container-mounts="${MOUNT}" \
    --ntasks-per-node=1 \
    bash -c 'cd /nccl && git clone https://github.com/NVIDIA/nccl-tests.git && source /opt/hpcx/hpcx-init.sh && hpcx_load && cd nccl-tests && make MPI=1'
