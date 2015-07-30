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


#load config parameters
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/exp-deployment.properties

if test -z "$1"
then
	echo "Provide host name where master will be started"
	exit 0
fi


connectStr="$HOP_User@$1"
echo "loading new master properties files on $1"
scp $DIR/hop_exp_scripts/master.properties $connectStr:$HOP_Experiments_Dist_Folder
echo "Starting Experiment Master on $1"
ssh $connectStr $HOP_Experiments_Dist_Folder/start-master.sh 






