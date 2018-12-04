#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#All Unique Hosts
All_Hosts_To_Purge=(`grep -v "^#" experiment-nodes`)
pssh -H "${All_Hosts_To_Purge[*]}"  -l ubuntu -i   pkill -9 java
