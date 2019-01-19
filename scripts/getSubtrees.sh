#!/usr/bin/env bash
MDS_LIST=(`grep -v "^#" mds-nodes`)
MDS_NODES=${MDS_LIST[*]}

sudo ceph fs status > $1

for n in ${MDS_NODES[@]}; do
  connectStr="ubuntu@${n}"
   echo "get Subtrees for ${n}"
   ssh $connectStr "sudo ceph daemon mds.${n} get subtrees | jq '.[] | [.dir.path, .auth_first]' > /tmp/${n}-subtrees"
   ssh $connectStr "do ceph daemon mds.${n} perf dump | jq '.mds_mem.rss'; ceph daemon mds.${n} dump_mempools | grep -A 3 mds_co; > /tmp/${n}-memory"
   scp $connectStr:"/tmp/${n}-*" $1
done
