#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script



if [ -z $1 ]; then
	echo "please, specify the user name. usage [uername] [process name] i.e. root java"
	exit
fi

if [ -z $2 ]; then
        echo "please, specify the process name. usage [uername] [process name] i.e. root java"
        exit
fi

#All Unique Hosts
All_Hosts_To_Purge=(cloud1 cloud2 cloud3 cloud4 cloud5 cloud6 cloud7 cloud8 cloud9 cloud10 cloud11 cloud12 
#cloud13
 cloud14 cloud15 cloud16 cloud17 
  bbc1 bbc2 bbc3 bbc4 bbc5 bbc6 bbc7 salman2 hawtaky snurran maismdell)


for i in ${All_Hosts_To_Purge[@]}
do
	connectStr="$1@$i"
#	ssh $connectStr "ps -ax | grep "java""
        pids=""
        pids=`ssh $connectStr pgrep -u $1 $2`
	
	if [ -z "${pids}" ]; then
		echo "There is no process named $2 running on $i" 
	else
        	echo "Killing $2 on $i. PIDS to kill "$pids
		ssh $connectStr  kill -9 $pids
	fi
done

