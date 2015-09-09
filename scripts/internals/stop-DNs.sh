#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script




#All Unique Hosts
All_Hosts=${DNS_FullList[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')



if [ ${#errors[@]} -eq 0 ]; then
    echo "No datanodes to stop"
else
    echo "Starting DNs on ${All_Unique_Hosts[*]}"
    parallel-ssh -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  $HopsFS_Remote_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs stop datanode 
fi

