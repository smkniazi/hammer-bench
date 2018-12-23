#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

#All Unique Hosts
All_Hosts="${OSD_FullList[*]} ${MDS_FullList[*]} ${MON_FullList[*]} ${BM_Machines_FullList[*]}"
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Running NMON to collect stats on ${All_Unique_Hosts[*]}"
$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  "pkill nmon; cd /tmp; rm *.nmon; nmon -f -s 1 -c 1000 -T"