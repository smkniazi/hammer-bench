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


#echo "host1 host2"
nodes=()
for i in $(seq 0 99); do
  ping -c 1 $i.tester.service.consul > /dev/null
  if [ "$?" -eq "0" ]; then
    nodes+="$i.tester.service.consul"
  else
    break 
  fi
done

echo ${nodes[@]}
exit 0
