#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script



#All Unique Hosts
All_Hosts=${All_NNs_In_Current_Exp[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Stopping NN on ${All_Unique_Hosts[*]}"
parallel-ssh -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  $HopsFS_Remote_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs stop namenode




