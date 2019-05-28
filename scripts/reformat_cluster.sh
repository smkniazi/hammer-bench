#!/bin/bash
# This script formats the NN, DN, removes data dirs, and restarts the services.

echo "1. Format namenode"
/mnt/hadoop/sbin/format-nn.sh

echo "2. Delete Datanode directories and runtime config"
rm -rf /mnt/hopsdata/hdfs/dn/current/
rm -rf /mnt/hopsdata/hdfs/nn/current/
rm -rf /mnt/hadoop/logs/hadoop-*-datanode-ip-*.log
rm -rf /mnt/hadoop/logs/hadoop-*-namenode-ip-*.log

echo "3. reboot namenode"
sudo service namenode restart

echo "4. reboot datanode"
sudo service datanode restart

