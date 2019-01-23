#!/bin/bash

EXP_NODES=(`grep -v "^#" experiment-nodes`)

pssh -H "${EXP_NODES[*]}"  -l ubuntu -i  sudo umount /mnt/cephfs
