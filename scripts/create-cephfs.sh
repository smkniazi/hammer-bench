#!/bin/bash

sudo ceph osd pool create cephfs_data 128 128 replicated osd-rule
sudo ceph osd pool create cephfs_metadata 256 256 replicated osd-rule
sudo ceph fs new cephfs cephfs_metadata cephfs_data 
