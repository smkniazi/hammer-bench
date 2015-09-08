#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


if [ -z $1 ]; then
	echo "please, specify the process name i.e. .*java"
	exit
fi


if [ -z $DIR ]; then
  DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  echo "Loading params "
  source $DIR/../experiment-env.sh
fi



#All Unique Hosts
All_Hosts_To_Kill=${NNS_FullList[*]}" "${DNS_FullList[*]}" "${BM_Machines_FullList[*]}
All_Unique_Hosts_To_Kill=$(echo "${All_Hosts_To_Kill[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')


echo "*** Going to kill $1 on ${All_Unique_Hosts_To_Kill[@]}"

for i in ${All_Unique_Hosts_To_Kill[@]}
do
	      connectStr="$HopsFS_User@$i"

        pids=""
        pids=`ssh $connectStr pgrep -u $HopsFS_User $1`
	
	if [ -z "${pids}" ]; then
		echo "There is no process named $1 running on $i" 
	else
        	echo "Killing $1 on $i. PIDS to kill "$pids
		ssh $connectStr  kill -9 $pids
	fi

done

