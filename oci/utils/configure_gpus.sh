#!/bin/bash

nvidia-smi -pm 1
nvidia-smi -acp UNRESTRICTED

for x in 0 1 2 3 4 5 6 7 
do
    memory=`nvidia-smi --query-gpu=clocks.max.mem -i $x --format=csv,noheader,nounits`
    graphics=`nvidia-smi --query-gpu=clocks.max.graphics -i $x --format=csv,noheader,nounits`
    #graphics=1605
    echo "GPU Memory: $memory,   GPU Graphics: $graphics"
    nvidia-smi -i $x --auto-boost-default=0
    nvidia-smi -i $x -ac $memory,$graphics
done
