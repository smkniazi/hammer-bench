#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

PSSH=
PRSYNC=
OS=$(lsb_release -is)
if [ $OS == "Ubuntu" ] ; then
   PRSYNC="/usr/bin/parallel-rsync"
   PSSH="/usr/bin/parallel-ssh"
elif [ $OS == "CentOS" ] ; then
   PRSYNC="/usr/bin/prsync"
   PSSH="/usr/bin/pssh"
fi

if [ -z $NNS_FullList ]; then
  DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  echo "Loading params "
  source $DIR/../experiment-env.sh
fi



#All Unique Hosts
All_Hosts_To_Kill=${NNS_FullList[*]}" "${DNS_FullList[*]}" "${BM_Machines_FullList[*]}
All_Unique_Hosts_To_Kill=$(echo "${All_Hosts_To_Kill[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')



echo "Killing java on ${All_Unique_Hosts_To_Kill[*]}"
$PSSH -H "${All_Unique_Hosts_To_Kill[*]}"  -l $HopsFS_User -i  pkill .*java

