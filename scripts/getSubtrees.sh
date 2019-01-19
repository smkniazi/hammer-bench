#!/usr/bin/env bash
MDS_LIST=(`grep -v "^#" mds-nodes`)
MDS_NODES=${MDS_LIST[*]}

dir=$1/mds
mkdir -p $dir
sudo ceph fs status > $dir/activeMDSs

for n in ${MDS_NODES[@]}; do
  connectStr="ubuntu@${n}"
   echo "get Subtrees for ${n}"
   #ssh $connectStr "sudo ceph daemon mds.${n} get subtrees | jq '.[] | [.dir.path, .auth_first]' > /tmp/${n}-subtrees"
   ssh $connectStr "sudo ceph daemon mds.${n} get subtrees  > /tmp/${n}-subtrees"
   ssh $connectStr "sudo ceph daemon mds.${n} perf dump  > /tmp/${n}-perf"
   ssh $connectStr "sudo ceph daemon mds.${n} dump_mempools > /tmp/${n}-mempool"
   #ssh $connectStr "sudo ceph daemon mds.${n} perf dump | jq '.mds_mem.rss'; sudo ceph daemon mds.${n} dump_mempools | grep -A 3 mds_co; > /tmp/${n}-memgrep"
   scp $connectStr:"/tmp/${n}-*" $dir
done
