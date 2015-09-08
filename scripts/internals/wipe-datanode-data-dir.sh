#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#All Unique Hosts

for i in ${DNS_FullList[@]}
do
	connectStr="$HopsFS_User@$i"
	ssh $connectStr 'rm -rf ' $Datanode_Data_Dir
done





