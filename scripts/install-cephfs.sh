#!/usr/bin/env bash

OSD_DISK="/dev/sdb"

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

echo "**** Ceph MON ${MON} ****"
echo "**** Ceph OSDS ${OSD_NODES} ****"
echo "**** Ceph MDSs ${MDS_NODES} ****"
echo "**** Ceph All ${CEPH_NODES} ****"

pssh -H "${CEPH_NODES}"  -l ubuntu -i 'sudo yum install ntp ntpdate ntp-doc -y'

sudo rpm -Uhv http://download.ceph.com/rpm-luminous/el7/noarch/ceph-deploy-2.0.1-0.noarch.rpm
sudo yum update -y && sudo yum install ceph-deploy -y

mkdir cluster
cd cluster

echo "**** Ceph add MON ${MON} ****"
ceph-deploy new ${MON}

echo "**** Ceph install on ${CEPH_NODES} ****"

ceph-deploy install ${CEPH_NODES}
ceph-deploy mon create-initial

ceph-deploy admin ${CEPH_NODES}

ceph-deploy mgr create ${MON}

for n in ${OSD_NODES[@]}; do
 echo "**** Ceph create osd on ${n} ****"
 ceph-deploy osd create --data ${OSD_DISK} ${n}
done

ceph-deploy mds create ${MDS_NODES}

ceph-deploy pkg --install cephfs-java ${CEPH_NODES}

pssh -H "${CEPH_NODES}"  -l ubuntu -i  'sudo ln -s /lib64/libcephfs_jni.so.1.0.0 /lib64/libcephfs_jni.so'

echo "Create CephFS"

sudo ceph osd pool create cephfs_data 64
sudo ceph osd pool create cephfs_metadata 128
sudo ceph osd pool set cephfs_metadata size 2

sudo ceph fs new cephfs cephfs_metadata cephfs_data
