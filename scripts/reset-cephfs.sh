#!/bin/bash
./stop-mdss.sh
sudo ceph fs reset cephfs --yes-i-really-mean-it
