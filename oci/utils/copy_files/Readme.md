# Get a list of all of the slurm nodes
scontrol show topology | grep Level=0 | rev | awk '{print $1}' | rev | cut -d "=" -f2 | cluset -f
sinfo | grep -E 'down|drain' | rev | awk '{print $1}' | rev | cluset -f

# Find the information of the RDMA interfaces on the nodes
python3 ./fetch_rdma_to_json.py -p gpu-[153,335,383,733,740,928,959,999] > rdma_config.json

# Distribute files using RDMA interfaces to local NVMe disks
python3 distribute_dirs.py -c rdma_config.json --source-dir /mnt/resource_nvme/lora_data --dest-dir /mnt/resource_nvme/lora_data --max-workers 32
