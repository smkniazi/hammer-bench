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

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PSSH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

#upload Experiments

 echo "***   Copying the Experiment to $HopsFS_Experiments_Remote_Dist_Folder  on ${BM_Machines_FullList[*]}***"
        $PSSH -H "${BM_Machines_FullList[*]}"  -l $HopsFS_User -i  'mkdir -p '$HopsFS_Experiments_Remote_Dist_Folder

	JarFileName=hop-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
	temp_folder=/tmp/hop_exp_distro
	rm -rf $temp_folder
	mkdir -p $temp_folder	
	cp $HopsFS_Src_Folder/target/$JarFileName $temp_folder/
	cp ./internals/HopsFS_Exp_Remote_Scripts/* $temp_folder/
	cp ./master.properties $temp_folder/
	cp ./slave.properties $temp_folder/

        sed -i 's|JAVA_BIN|'$JAVA_BIN'|g' $temp_folder/*.sh
        sed -i 's|CPU_AFFINITY=.*|CPU_AFFINITY='$CPU_AFFINITY'|g' $temp_folder/*.sh

	$PRSYNC -arzv -H "${BM_Machines_FullList[*]}" --user $HopsFS_User     $temp_folder/   $HopsFS_Experiments_Remote_Dist_Folder  
