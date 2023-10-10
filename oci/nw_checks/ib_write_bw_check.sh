#!/bin/bash

n1=$1
n2=$2

# Srv
ssh $n1 "for x in 0 1 3 4 5 6 7 8 9 10 12 13 14 15 16 17;do ib_write_bw -a -d mlx5_${x} -F &";done

# Client
sleep 3
ssh $n2 "for x in 0 1 3 4 5 6 7 8 9 10 12 13 14 15 16 17;do ib_write_bw -a -d mlx5_${x} -F $n2";sleep 3;done
