#!/usr/bin/env bash
EXP_NODES=(`grep -v "^#" experiment-nodes`)
MON_NODES=(`grep -v "^#" mon-nodes`)

pssh -H "${EXP_NODES[*]}"  -l ubuntu -i  "sudo mkdir /mnt/cephfs; sudo mount -t ceph ${MON_NODES[0]}:/ /mnt/cephfs -o name=admin,noshare,noasyncreaddir,secret=`sudo grep "key" /etc/ceph/ceph.client.admin.keyring | awk '{print $3}'`"
