#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters


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




