#!/bin/bash
# ssh-multi
# D.Kovalov
# Based on http://linuxpixies.blogspot.jp/2011/06/tmux-copy-mode-and-how-to-control.html

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

