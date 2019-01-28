#!/bin/bash
set -e
echo "unmount"
./umount-cephfs.sh
echo "stop mdss"
./stop-mdss.sh
echo "fail mdss"
./fail-mdss.sh 59
echo "delete cephfs"
./delete-cephfs.sh
echo "create cephfs"
./create-cephfs.sh
echo "set active mdss $1"
./set-active-mdss.sh $1
echo "reset failed"
./reset-failed.sh
echo "start mdss"
./start-mdss.sh
sleep 20
echo "mount"
./mount-cephfs.sh
#sudo ceph fs reset cephfs --yes-i-really-mean-it
