#!/bin/bash

EXP_NODES=(`grep -v "^#" experiment-nodes | shuf`)

MDS_SIZE=$1

baseDir="/mnt/cephfs"

let i=0
for n in ${EXP_NODES[@]}; do
   echo "pin dir ${n} to rank ${i}"
   dir="${baseDir}/${n}"
   sudo mkdir ${dir}
   sudo setfattr -n ceph.dir.pin -v ${i} ${dir}
   let i++
   if [ $i -ge ${MDS_SIZE} ]
   then
      let i=0
   fi
done
