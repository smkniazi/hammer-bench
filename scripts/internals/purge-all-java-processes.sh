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



if [ -z $1 ]; then
  echo "please, specify the user name. usage [uername] [process name] i.e. root java"
  exit
fi

if [ -z $2 ]; then
  echo "please, specify the process name. usage [uername] [process name] i.e. root java"
  exit
fi

#All Unique Hosts
All_Hosts_To_Purge=(hostname1 hostname2) 

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

