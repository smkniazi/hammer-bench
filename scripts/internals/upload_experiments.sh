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
source $DIR/../exp-deployment.properties

#upload Experiments

     echo "***   Copying the Experiment to $HOP_Experiments_Dist_Folder  on ${HOP_Experiments_Machine_List[*]}***"
	for machine in ${HOP_Experiments_Machine_List[*]}
	do
		 connectStr="$HOP_User@$machine"
		 ssh $connectStr 'mkdir -p '$HOP_Experiments_Dist_Folder
	done

	JarFileName=hop-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
	temp_folder=/tmp/hop_exp_distro
	rm -rf $temp_folder
	mkdir -p $temp_folder	
	cp $HOP_Experiments_Folder/target/$JarFileName $temp_folder/
	cp $DIR/hop_exp_scripts/* $temp_folder/

	parallel-rsync -arzv -H "${HOP_Experiments_Machine_List[*]}" --user $HOP_User     $temp_folder/   $HOP_Experiments_Dist_Folder  






