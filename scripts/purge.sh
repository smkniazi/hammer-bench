OSD_LIST=(`grep -v "^#" osd-nodes`)
MDS_LIST=(`grep -v "^#" mds-nodes`)
MON_LIST=(`grep -v "^#" mon-nodes`)
EXP_LIST=(`grep -v "^#" experiment-nodes`)

All_Hosts="${OSD_LIST[*]} ${MDS_LIST[*]} ${MON_LIST[*]} ${EXP_LIST[*]}"
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

MON=${MON_LIST[*]}
CEPH_NODES=${All_Unique_Hosts[*]}
OSD_NODES=${OSD_LIST[*]}
MDS_NODES=${MDS_LIST[*]}

ceph-deploy purge ${CEPH_NODES}
ceph-deploy purgedata ${CEPH_NODES}
ceph-deploy forgetkeys
rm cluster/ceph.*
