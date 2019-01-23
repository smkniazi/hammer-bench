#!/bin/bash

sudo ceph tell mon.\* injectargs '--mon-allow-pool-delete=true' 

sudo ceph fs rm cephfs --yes-i-really-mean-it
sudo ceph osd pool delete  cephfs_data cephfs_data --yes-i-really-really-mean-it
sudo ceph osd pool delete  cephfs_metadata cephfs_metadata --yes-i-really-really-mean-it
