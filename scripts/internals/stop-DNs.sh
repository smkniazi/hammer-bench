#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script




#All Unique Hosts
All_Hosts=${DNS_FullList[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')


for i in ${All_Unique_Hosts[@]}
do
	connectStr="$HopsFS_User@$i"
	echo "Stopping DN on $i"
	ssh $connectStr  $HopsFS_Remote_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs stop datanode &
done





