#!/bin/bash
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
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



  echo "Going to kill Master/Slave process on all experiment machines ${BM_Machines_FullList[*]}"
	#All Unique Hosts
	All_Hosts=${BM_Machines_FullList[*]}
	All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')


  echo "Stopping Slaves on ${BM_Machines_FullList[*]}"
  $PSSH -H "${BM_Machines_FullList[*]}"  -l $HopsFS_User -i  $HopsFS_Experiments_Remote_Dist_Folder/kill-slave.sh
