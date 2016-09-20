#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

#All Unique Hosts
All_Hosts=${All_NNs_In_Current_Exp[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Starting NN on ${All_Unique_Hosts[*]}"
$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  $HopsFS_Remote_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs start namenode

$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  $HopsFS_Remote_Dist_Folder/hop_conf/scripts/set-nn-cpu-affinity.sh                                                                                                                     


