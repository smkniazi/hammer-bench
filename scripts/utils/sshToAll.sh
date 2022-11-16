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

# a script to ssh multiple servers over multiple tmux panes

if [ "$#" -ne 2 ]; then
     echo "Usage {file that contains the hosts names} {user name}"
     echo "i.e. sshToAll.sh namenodes ubuntu"
     exit
fi

user=$2
machines=$1

AllDeployedMachines=(`cat $machines`)

HOSTS=${AllDeployedMachines[*]}

echo "Machines are ${HOSTS[@]}"

starttmux() {
    if [ -z "$HOSTS" ]; then
       echo -n "Please provide of list of hosts separated by spaces [ENTER]: "
       read HOSTS
    fi

    local hosts=( $HOSTS )

    echo "tmux new-window "ssh $user@${hosts[0]}""
    
    tmux new-window "ssh $user@${hosts[0]}"
    unset hosts[0];
    for i in "${hosts[@]}"; do
        tmux split-window -h  "ssh $user@$i"
        tmux select-layout tiled > /dev/null
    done
    tmux select-pane -t 0
    tmux set-window-option synchronize-panes on > /dev/null

}

HOSTS=${HOSTS:=$*}

starttmux

