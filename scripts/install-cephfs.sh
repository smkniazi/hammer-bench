#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
mount_cephfs="$DIR/mount-cephfs.sh"

OSD_DISK="/dev/sdb"
#MDS_CACHE_SIZE="2073741824"
MDS_CACHE_SIZE="17179869184"

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

date1=$(date +"%s")

echo "**** Ceph MON ${MON} ****"
echo "**** Ceph OSDS ${OSD_NODES} ****"
echo "**** Ceph MDSs ${MDS_NODES} ****"
echo "**** Ceph All ${CEPH_NODES} ****"

pssh -H "${CEPH_NODES}"  -l ubuntu -i 'sudo yum install ntp ntpdate ntp-doc jq -y'

sudo rpm -Uhv http://download.ceph.com/rpm-luminous/el7/noarch/ceph-deploy-2.0.1-0.noarch.rpm
sudo yum update -y && sudo yum install ceph-deploy -y

mkdir cluster
cd cluster

echo "**** Ceph add MON ${MON} ****"
ceph-deploy new ${MON}

echo "**** Ceph install on ${CEPH_NODES} ****"

#ceph-deploy install ${CEPH_NODES}
let i=0
pids=()
for n in ${CEPH_NODES[*]}; do
   echo "install on  ${n} -- ${i}"
   ceph-deploy install ${n} > "/tmp/ceph-deploy-${n}" &
   pids[${i}]=$!
   let i++
done

for pid in ${pids[*]}; do
    echo "Waiting for ${pid}"
    wait $pid
done

ceph-deploy mon create-initial

ceph-deploy admin ${CEPH_NODES}

ceph-deploy mgr create ${MON}

let i=0
pids=()
for n in ${OSD_NODES[@]}; do
 echo "**** Ceph create osd on ${n} ****"
 #ceph-deploy osd create --data ${OSD_DISK} ${n}
 ceph-deploy osd create --data ${OSD_DISK} ${n} > "/tmp/ceph-deploy-osd-${n}" &
 pids[${i}]=$!
 let i++
done

for pid in ${pids[*]}; do
    echo "Waiting for ${pid}"
    wait $pid
done

echo "Change crush map across 3 zones for 12 OSDs"

if [ ${#OSD_LIST[@]} -eq 12 ];
then
  sudo ceph osd crush set osd.0 1.0 root=default rack=zone1
  sudo ceph osd crush set osd.1 1.0 root=default rack=zone1
  sudo ceph osd crush set osd.2 1.0 root=default rack=zone1
  sudo ceph osd crush set osd.3 1.0 root=default rack=zone1
  sudo ceph osd crush set osd.4 1.0 root=default rack=zone2
  sudo ceph osd crush set osd.5 1.0 root=default rack=zone2
  sudo ceph osd crush set osd.6 1.0 root=default rack=zone2
  sudo ceph osd crush set osd.7 1.0 root=default rack=zone2
  sudo ceph osd crush set osd.8 1.0 root=default rack=zone3
  sudo ceph osd crush set osd.9 1.0 root=default rack=zone3
  sudo ceph osd crush set osd.10 1.0 root=default rack=zone3
  sudo ceph osd crush set osd.11 1.0 root=default rack=zone3

  sudo ceph osd crush rule create-replicated osd-rule default rack
fi

ceph-deploy mds create ${MDS_NODES}

#ceph-deploy pkg --install cephfs-java ${CEPH_NODES}

let i=0
pids=()
for n in ${CEPH_NODES[*]}; do
   echo "install cephfs-java on ${n} -- ${i}"
   ceph-deploy pkg --install cephfs-java ${n} > "/tmp/ceph-deploy-pkg-${n}" &
   pids[${i}]=$!
   let i++
done

for pid in ${pids[*]}; do
    echo "Waiting for ${pid}"
    wait $pid
done

pssh -H "${CEPH_NODES}"  -l ubuntu -i  'sudo ln -s /lib64/libcephfs_jni.so.1.0.0 /lib64/libcephfs_jni.so'

for n in ${MDS_NODES[@]}; do
   echo "Increase Cache size for ${n} to ${MDS_CACHE_SIZE}"
   ssh "ubuntu@${n}" "sudo ceph daemon mds.${n} config set mds_cache_memory_limit ${MDS_CACHE_SIZE}"
done


echo "Create CephFS"

if [ ${#OSD_LIST[@]} -eq 12 ];
then
  sudo ceph osd pool create cephfs_data 128 128 replicated osd-rule
  sudo ceph osd pool create cephfs_metadata 256 256 replicated osd-rule
else
  sudo ceph osd pool create cephfs_data 128
  sudo ceph osd pool create cephfs_metadata 256
fi


#sudo ceph osd pool set cephfs_metadata size 2

sudo ceph fs new cephfs cephfs_metadata cephfs_data


date2=$(date +"%s")
diff=$(($date2-$date1))

source $mount_cephfs

echo "CephFS installed in $(($diff / 60)) minutes and $(($diff % 60)) seconds."
