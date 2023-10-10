#!/bin/bash

container="nvcr.io/nvidia/hpc-benchmarks:23.5"
#docker run --runtime nvidia --shm-size=1g \

GPUS=8
#DAT_FILE=/hpl_data/HPL-dgx-1N-4.dat # Use for 2 or 4 GPUs
DAT_FILE=hpl-linux-x86_64/sample-dat/HPL-dgx-1N.dat #Use for 1 or 8 GPUs


docker run --gpus all --shm-size=1g \
	-v $(pwd):/hpl_data \
	--ulimit memlock=-1 -t "$container" \
	mpirun --allow-run-as-root -np $GPUS \
	--mca pml ucx --mca btl ^openib,smcuda \
	--mca coll_hcoll_enable 0 \
	-x coll_hcoll_np=0 --bind-to none \
	./hpl.sh --no-multinode --dat $DAT_FILE
