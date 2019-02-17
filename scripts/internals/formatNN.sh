#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters


truncateTable() {
  tableName=$1
  echo "Truncating $tableName table"
  /home/tester/.mysql/mysql/bin/mysql -u root hop_salman -e "truncate table $tableName"
}


#truncate speeds up the format if there is lot of data the following tables
truncateTable hdfs_inodes
truncateTable hdfs_block_infos
truncateTable hdfs_block_lookup_table
truncateTable hdfs_replicas
truncateTable hdfs_inmemory_file_inode_data
truncateTable hdfs_ondisk_small_file_inode_data
truncateTable hdfs_ondisk_medium_file_inode_data
truncateTable hdfs_ondisk_large_file_inode_data
truncateTable bench_blockreporting_datanodes


connectStr="$HopsFS_User@$Current_Leader_NN"
echo "Formatting ...  on $Current_Leader_NN"
ssh $connectStr $HopsFS_Remote_Dist_Folder/bin/hdfs namenode -format 


echo "Deleting datanode data dirs"
source ./internals/wipe-datanode-data-dir.sh

echo "Deleting log files"
source ./internals/wipe-nn-logs.sh
