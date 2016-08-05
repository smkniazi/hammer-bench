#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

PSSH=
PRSYNC=
OS=$(./os-type.sh)
if [ $OS == "Ubuntu" ] ; then
   PRSYNC="/usr/bin/parallel-rsync"
   PSSH="/usr/bin/parallel-ssh"
elif [ $OS == "CentOS" ] ; then
   PRSYNC="/usr/bin/prsync"
   PSSH="/usr/bin/pssh"
fi



#All Unique Hosts
All_Hosts=${All_NNs_In_Current_Exp[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Stopping NN on ${All_Unique_Hosts[*]}"
$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  $HopsFS_Remote_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs stop namenode




