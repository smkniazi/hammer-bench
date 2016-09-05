#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PSSH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

#All Unique Hosts
All_Hosts_To_Kill=${NNS_FullList[*]}" "${DNS_FullList[*]}" "${BM_Machines_FullList[*]}
All_Unique_Hosts_To_Kill=$(echo "${All_Hosts_To_Kill[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')



echo "Killing java on ${All_Unique_Hosts_To_Kill[*]}"
$PSSH -H "${All_Unique_Hosts_To_Kill[*]}"  -l $HopsFS_User -i  pkill .*java

