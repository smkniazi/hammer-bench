#!/usr/bin/env bash

OSD_DISK="/dev/sdb"
MON="gcemaism-cephfs-m-1"
CEPH_NODES="gcemaism-cephfs-m-1 gcemaism-cephfs-s-1 gcemaism-cephfs-s-2 gcemaism-cephfs-s-3 gcemaism-cephfs-s-4  gcemaism-cephfs-s-5"
OSD_NODES=(gcemaism-cephfs-s-1 gcemaism-cephfs-s-2 gcemaism-cephfs-s-3)
MDS_NODES="gcemaism-cephfs-m-1"

pssh -H "${CEPH_NODES}"  -l ubuntu -i 'sudo yum install ntp ntpdate ntp-doc'

sudo rpm -Uhv http://download.ceph.com/rpm-luminous/el7/noarch/ceph-deploy-2.0.1-0.noarch.rpm
sudo yum update -y && sudo yum install ceph-deploy -y

mkdir cluster
cd cluster

ceph-deploy new ${MON}
ceph-deploy install ${CEPH_NODES}
ceph-deploy mon create-initial

ceph-deploy admin ${CEPH_NODES}

ceph-deploy mgr create ${MON}

for n in ${OSD_NODES[@]}; do
 ceph-deploy osd create --data ${OSD_DISK} ${n}
done

ceph-deploy mds create ${MDS_NODES}

ceph-deploy pkg --install cephfs-java ${CEPH_NODES}

pssh -H "${CEPH_NODES}"  -l ubuntu -i  'sudo ln -s /lib64/libcephfs_jni.so.1.0.0 /lib64/libcephfs_jni.so'

echo "Create CephFS"

sudo ceph osd pool create cephfs_data 100
sudo ceph osd pool create cephfs_metadata 100
sudo ceph osd pool set cephfs_metadata size 2

sudo ceph fs new cephfs cephfs_metadata cephfs_data



