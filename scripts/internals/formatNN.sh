#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters

/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_inodes"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_block_infos"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_block_lookup_table"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_replicas"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_inmemory_file_inode_data"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_ondisk_small_file_inode_data"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_ondisk_medium_file_inode_data"
/srv/hops/mysql-cluster/ndb/scripts/mysql-client.sh hops -e "truncate table hdfs_ondisk_large_file_inode_data"
#/home/tester/.mysql/mysql-cluster-gpl-7.5.5-linux-glibc2.5-x86_64/bin/mysql -uroot hop_salman_sf -e "truncate table bench_blockreporting_datanodes"


connectStr="$HopsFS_User@$Current_Leader_NN"
#echo "Drop Schema and Recreate  ...  on $Current_Leader_NN"
#ssh $connectStr $HopsFS_Remote_Dist_Folder/bin/hdfs namenode -dropAndCreateDB
#ssh $connectStr $HopsFS_Remote_Dist_Folder/sbin/drop-and-recreate-hops-db.sh
echo "Formatting ...  on $Current_Leader_NN"
ssh $connectStr $HopsFS_Remote_Dist_Folder/bin/hdfs namenode -format 


echo "Deleting datanode data dirs"
source ./internals/wipe-datanode-data-dir.sh

echo "Deleting log files"
source ./internals/wipe-nn-logs.sh




