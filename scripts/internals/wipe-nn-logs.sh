#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

for i in ${NNS_FullList[@]}
do
	connectStr="$HopsFS_User@$i"
	ssh $connectStr rm -rf  "$HopsFS_Remote_Dist_Folder/logs/*"
done





