#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SH=$($DIR/psshcmd.sh)
PRSYNC=$($DIR/prsynccmd.sh)

#All Unique Hosts
All_Hosts="${OSD_FullList[*]} ${MDS_FullList[*]} ${MON_FullList[*]} ${BM_Machines_FullList[*]}"
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Stopping NMON on ${All_Unique_Hosts[*]}"
$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  pkill nmon

for i in ${All_Unique_Hosts[@]}
do
	connectStr="$HopsFS_User@$i"

	scp $connectStr:/tmp/*.nmon $1

done

$PSSH -H "${All_Unique_Hosts[*]}"  -l $HopsFS_User -i  "rm /tmp/*.nmon"
