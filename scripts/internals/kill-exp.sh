#!/bin/bash
# Copyright (C) 2022 HopsWorks.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PSSH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

echo "Going to kill Master/Slave process on all experiment machines ${BM_Machines_FullList[*]}"
#All Unique Hosts
All_Hosts=${BM_Machines_FullList[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Stopping Slaves on ${BM_Machines_FullList[*]}"
$PSSH -H "${BM_Machines_FullList[*]}"  -l $HopsFS_User -i  $HopsFS_Experiments_Remote_Dist_Folder/kill-slave.sh
