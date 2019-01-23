#!/usr/bin/env bash
MDS_LIST=(`grep -v "^#" mds-nodes`)
MDS_NODES=${MDS_LIST[*]}

for n in ${MDS_NODES[@]}; do
  connectStr="ubuntu@${n}"
   echo "drop caches for ${n}"
   ssh $connectStr "sudo ceph daemon mds.${n} cache drop"
done
